"""Where per-subject data keys live, and how they are destroyed.

The vault is the whole erasure guarantee, so it has three properties that matter more than its
implementation:

1. destroying a key is a real delete, not a soft delete and not a status flag. A row that can be
   un-deleted is not erasure.
2. it is never backed up in a form that can be restored. A nightly export of the vault quietly
   undoes every erasure it contains.
3. every destruction is recorded in an audit table that holds no personal data, only the subject id
   already known to the requester, so you can prove when it happened without keeping the data.

DuckDB backs the local demo and BigQuery the deployed one. Both implement the same small interface,
so the erasure service does not care which it is talking to.
"""

from __future__ import annotations

import datetime as dt
from typing import Protocol

from privacy.crypto import ShreddedKeyError, new_data_key, unwrap, wrap

KEY_TABLE = "subject_keys"
AUDIT_TABLE = "erasure_audit"


class KeyVault(Protocol):
    def create_key(self, subject_id: str) -> bytes: ...
    def data_key(self, subject_id: str) -> bytes: ...
    def destroy_key(self, subject_id: str, reason: str) -> bool: ...
    def subject_count(self) -> int: ...


class DuckDBKeyVault:
    """Local vault. One row per subject, deleted outright on erasure."""

    def __init__(self, connection, schema: str = "privacy") -> None:
        self.connection = connection
        self.schema = schema
        self._create_tables()

    def _create_tables(self) -> None:
        self.connection.execute(f"create schema if not exists {self.schema}")
        self.connection.execute(f"""
            create table if not exists {self.schema}.{KEY_TABLE} (
                subject_id  varchar primary key,
                wrapped_key varchar not null,
                created_at  timestamp not null
            )
        """)
        # No personal data here on purpose. It records that a subject id was erased and when, which
        # is what a regulator asks for, and nothing that would recreate what was erased.
        self.connection.execute(f"""
            create table if not exists {self.schema}.{AUDIT_TABLE} (
                subject_id   varchar not null,
                erased_at    timestamp not null,
                reason       varchar,
                key_existed  boolean not null
            )
        """)

    def create_key(self, subject_id: str) -> bytes:
        existing = self.connection.execute(
            f"select wrapped_key from {self.schema}.{KEY_TABLE} where subject_id = ?", [subject_id]
        ).fetchone()
        if existing:
            return unwrap(existing[0])
        data_key = new_data_key()
        self.connection.execute(
            f"insert into {self.schema}.{KEY_TABLE} values (?, ?, ?)",
            [subject_id, wrap(data_key), dt.datetime.now(dt.UTC)],
        )
        return data_key

    def data_key(self, subject_id: str) -> bytes:
        row = self.connection.execute(
            f"select wrapped_key from {self.schema}.{KEY_TABLE} where subject_id = ?", [subject_id]
        ).fetchone()
        if row is None:
            raise ShreddedKeyError(f"no key for {subject_id}: erased, or never encrypted")
        return unwrap(row[0])

    def destroy_key(self, subject_id: str, reason: str = "erasure_request") -> bool:
        existed = self.connection.execute(
            f"select count(*) from {self.schema}.{KEY_TABLE} where subject_id = ?", [subject_id]
        ).fetchone()[0] > 0
        self.connection.execute(
            f"delete from {self.schema}.{KEY_TABLE} where subject_id = ?", [subject_id]
        )
        self.connection.execute(
            f"insert into {self.schema}.{AUDIT_TABLE} values (?, ?, ?, ?)",
            [subject_id, dt.datetime.now(dt.UTC), reason, existed],
        )
        return existed

    def subject_count(self) -> int:
        return self.connection.execute(
            f"select count(*) from {self.schema}.{KEY_TABLE}"
        ).fetchone()[0]


class BigQueryKeyVault:
    """The deployed vault.

    Two deployment notes that are easy to get wrong. The dataset holding these tables must be
    excluded from any table copy or export job, or the erasure is undone by the backup. And the
    wrapped key should be encrypted by a Cloud KMS KEK rather than the local one, so the plaintext
    DEK exists only in the memory of whichever process is decrypting a field.
    """

    def __init__(self, client, project: str, dataset: str = "privacy") -> None:
        self.client = client
        self.project = project
        self.dataset = dataset

    @property
    def _keys(self) -> str:
        return f"`{self.project}.{self.dataset}.{KEY_TABLE}`"

    @property
    def _audit(self) -> str:
        return f"`{self.project}.{self.dataset}.{AUDIT_TABLE}`"

    def create_key(self, subject_id: str) -> bytes:
        rows = list(self.client.query(
            f"select wrapped_key from {self._keys} where subject_id = @s",
            job_config=_params(s=subject_id),
        ).result())
        if rows:
            return unwrap(rows[0]["wrapped_key"])
        data_key = new_data_key()
        self.client.query(
            f"insert into {self._keys} (subject_id, wrapped_key, created_at) "
            "values (@s, @k, current_timestamp())",
            job_config=_params(s=subject_id, k=wrap(data_key)),
        ).result()
        return data_key

    def data_key(self, subject_id: str) -> bytes:
        rows = list(self.client.query(
            f"select wrapped_key from {self._keys} where subject_id = @s",
            job_config=_params(s=subject_id),
        ).result())
        if not rows:
            raise ShreddedKeyError(f"no key for {subject_id}: erased, or never encrypted")
        return unwrap(rows[0]["wrapped_key"])

    def destroy_key(self, subject_id: str, reason: str = "erasure_request") -> bool:
        job = self.client.query(
            f"delete from {self._keys} where subject_id = @s", job_config=_params(s=subject_id)
        )
        job.result()
        existed = bool(job.num_dml_affected_rows)
        self.client.query(
            f"insert into {self._audit} (subject_id, erased_at, reason, key_existed) "
            "values (@s, current_timestamp(), @r, @e)",
            job_config=_params(s=subject_id, r=reason, e=existed),
        ).result()
        return existed

    def subject_count(self) -> int:
        return list(self.client.query(f"select count(*) as n from {self._keys}").result())[0]["n"]


def _params(**kwargs):
    from google.cloud import bigquery

    types = {bool: "BOOL", str: "STRING"}
    return bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter(name, types[type(value)], value)
        for name, value in kwargs.items()
    ])
