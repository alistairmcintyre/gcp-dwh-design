"""The erasure sweep: turn a queue of requests into deletions, shredded keys and an audit trail.

The sweep is deliberately a scheduled batch rather than an online handler. Erasure touches every
layer, some of it behind a compaction cycle it cannot control, so the honest design is a job that
runs often, reports where each request has got to, and shouts before the deadline rather than after.

Order matters. The key is destroyed first: if the process dies halfway through, a subject whose key
is gone is unreadable everywhere, which is the safe failure. Doing the deletes first and dying
before the shred leaves readable ciphertext with the key still present.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import pathlib
from typing import Protocol

import yaml

TARGETS_FILE = pathlib.Path(__file__).with_name("erasure_targets.yaml")
DEADLINE_DAYS = 30  # Article 12(3): one month, extendable by two in limited cases


@dataclasses.dataclass(frozen=True)
class Target:
    table: str
    action: str
    key_column: str
    lawful_basis: str | None = None
    note: str | None = None


@dataclasses.dataclass
class ErasureResult:
    subject_id: str
    key_destroyed: bool
    rows_deleted: dict[str, int]
    tombstones: list[str]
    completed_at: dt.datetime

    @property
    def total_rows_deleted(self) -> int:
        return sum(self.rows_deleted.values())


class Warehouse(Protocol):
    def delete_rows(self, table: str, key_column: str, subject_id: str) -> int: ...
    def count_rows(self, table: str, key_column: str, subject_id: str) -> int: ...
    def pending_requests(self) -> list[tuple[str, dt.datetime]]: ...
    def completed_requests(
        self, since_days: int | None = None
    ) -> list[tuple[str, dt.datetime]]: ...
    def mark_complete(self, subject_id: str, result: ErasureResult) -> None: ...


def load_targets(path: pathlib.Path = TARGETS_FILE) -> list[Target]:
    spec = yaml.safe_load(path.read_text(encoding="utf-8"))
    default_key = spec["subject_key"]
    return [
        Target(
            table=entry["table"],
            action=entry["action"],
            key_column=entry.get("key_column", default_key),
            lawful_basis=entry.get("lawful_basis"),
            note=entry.get("note"),
        )
        for entry in spec["targets"]
    ]


class ErasureService:
    def __init__(self, vault, warehouse: Warehouse, targets: list[Target] | None = None,
                 tombstone_planner=None) -> None:
        self.vault = vault
        self.warehouse = warehouse
        self.targets = targets if targets is not None else load_targets()
        self.tombstone_planner = tombstone_planner

    def erase(self, subject_id: str, reason: str = "erasure_request") -> ErasureResult:
        # 1. Shred first. Everything after this is cleanup of readable copies; if it fails, the data
        #    that survives is already unreadable.
        key_destroyed = self.vault.destroy_key(subject_id, reason)

        # 2. Delete the rows that exist only because the person does.
        deleted: dict[str, int] = {}
        for target in self.targets:
            if target.action != "delete":
                continue
            deleted[target.table] = self.warehouse.delete_rows(
                target.table, target.key_column, subject_id
            )

        # 3. Tombstone the keyed topics so the log forgets them too, and so every other consumer of
        #    those topics is told to delete rather than having to be emailed.
        tombstones = list(self.tombstone_planner(subject_id)) if self.tombstone_planner else []

        result = ErasureResult(
            subject_id=subject_id,
            key_destroyed=key_destroyed,
            rows_deleted=deleted,
            tombstones=tombstones,
            completed_at=dt.datetime.now(dt.UTC),
        )
        self.warehouse.mark_complete(subject_id, result)
        return result

    def verify(self, subject_id: str) -> dict[str, int]:
        """Rows still present in delete targets. Anything but zero is an incomplete erasure.

        Run after the sweep rather than trusting the delete counts: a table added to the inventory
        after the request was processed shows up here, and a delete that silently matched nothing
        because someone renamed the key column does too.
        """
        return {
            target.table: self.warehouse.count_rows(target.table, target.key_column, subject_id)
            for target in self.targets
            if target.action == "delete"
        }

    def sweep(self, deadline_days: int = DEADLINE_DAYS) -> tuple[list[ErasureResult], list[str]]:
        """Process every pending request, returning the results and anything past the deadline."""
        results, overdue = [], []
        now = dt.datetime.now(dt.UTC)
        for subject_id, requested_at in self.warehouse.pending_requests():
            if requested_at.tzinfo is None:
                requested_at = requested_at.replace(tzinfo=dt.UTC)
            age_days = (now - requested_at).days
            if age_days >= deadline_days:
                overdue.append(subject_id)
            results.append(self.erase(subject_id))
        return results, overdue


class DuckDBWarehouse:
    """Local implementation, used by the demo and the tests."""

    REQUESTS_TABLE = "raw.erasure_requests"

    def __init__(self, connection) -> None:
        self.connection = connection

    def delete_rows(self, table: str, key_column: str, subject_id: str) -> int:
        if not self._exists(table):
            return 0
        before = self.count_rows(table, key_column, subject_id)
        self.connection.execute(f"delete from {table} where {key_column} = ?", [subject_id])
        return before

    def count_rows(self, table: str, key_column: str, subject_id: str) -> int:
        if not self._exists(table):
            return 0
        return self.connection.execute(
            f"select count(*) from {table} where {key_column} = ?", [subject_id]
        ).fetchone()[0]

    def pending_requests(self) -> list[tuple[str, dt.datetime]]:
        return self.connection.execute(
            f"select client_id, requested_at from {self.REQUESTS_TABLE} "
            "where completed_at is null order by requested_at"
        ).fetchall()

    def completed_requests(self, since_days: int | None = None) -> list[tuple[str, dt.datetime]]:
        sql = (f"select client_id, completed_at from {self.REQUESTS_TABLE} "
               "where completed_at is not null")
        params: list = []
        if since_days is not None:
            sql += " and completed_at >= ?"
            params.append(dt.datetime.now(dt.UTC) - dt.timedelta(days=since_days))
        return self.connection.execute(sql + " order by completed_at", params).fetchall()

    def mark_complete(self, subject_id: str, result: ErasureResult) -> None:
        self.connection.execute(
            f"update {self.REQUESTS_TABLE} set completed_at = ?, rows_deleted = ? "
            "where client_id = ? and completed_at is null",
            [result.completed_at, result.total_rows_deleted, subject_id],
        )

    def _exists(self, table: str) -> bool:
        schema, _, name = table.partition(".")
        return bool(self.connection.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = ? and table_name = ?", [schema, name]
        ).fetchone()[0])


class BigQueryWarehouse:
    """Deployed implementation.

    One BigQuery constraint: DML against a table with a streaming insert in the last 30 minutes
    used to be refused. Rows written by the Storage Write API can be modified
    straight away, so a warehouse fed by the current write path can erase without waiting.
    """

    def __init__(self, client, project: str, requests_table: str = "raw.erasure_requests") -> None:
        self.client = client
        self.project = project
        self.requests_table = f"`{project}.{requests_table}`"

    def _fq(self, table: str) -> str:
        return f"`{self.project}.{table}`"

    def delete_rows(self, table: str, key_column: str, subject_id: str) -> int:
        job = self.client.query(
            f"delete from {self._fq(table)} where {key_column} = @s",
            job_config=_string_param(subject_id),
        )
        job.result()
        return int(job.num_dml_affected_rows or 0)

    def count_rows(self, table: str, key_column: str, subject_id: str) -> int:
        rows = list(self.client.query(
            f"select count(*) as n from {self._fq(table)} where {key_column} = @s",
            job_config=_string_param(subject_id),
        ).result())
        return int(rows[0]["n"])

    def pending_requests(self) -> list[tuple[str, dt.datetime]]:
        rows = self.client.query(
            f"select client_id, requested_at from {self.requests_table} "
            "where completed_at is null order by requested_at"
        ).result()
        return [(row["client_id"], row["requested_at"]) for row in rows]

    def completed_requests(self, since_days: int | None = None) -> list[tuple[str, dt.datetime]]:
        window = (
            "and completed_at >= timestamp_sub(current_timestamp(), "
            f"interval {int(since_days)} day)"
            if since_days is not None else ""
        )
        rows = self.client.query(
            f"select client_id, completed_at from {self.requests_table} "
            f"where completed_at is not null {window} order by completed_at"
        ).result()
        return [(row["client_id"], row["completed_at"]) for row in rows]

    def mark_complete(self, subject_id: str, result: ErasureResult) -> None:
        self.client.query(
            f"update {self.requests_table} set completed_at = current_timestamp(), "
            "rows_deleted = @n where client_id = @s and completed_at is null",
            job_config=_complete_params(subject_id, result.total_rows_deleted),
        ).result()


def _string_param(value: str):
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("s", "STRING", value)]
    )


def _complete_params(subject_id: str, rows: int):
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("s", "STRING", subject_id),
        bigquery.ScalarQueryParameter("n", "INT64", rows),
    ])
