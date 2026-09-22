"""The sweep, end to end against a real DuckDB warehouse."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from privacy.erasure import DuckDBWarehouse, ErasureService, Target, load_targets
from privacy.vault import DuckDBKeyVault


@pytest.fixture
def warehouse():
    connection = duckdb.connect(":memory:")
    connection.execute("create schema raw")
    connection.execute("create schema marts")
    connection.execute("create table raw.clients (client_id varchar, email varchar)")
    connection.execute("create table raw.trades (trade_id varchar, client_id varchar)")
    connection.execute("create table marts.dim_client (client_id varchar, trading_region varchar)")
    connection.execute("""
        create table raw.erasure_requests (
            client_id varchar, requested_at timestamp, completed_at timestamp, rows_deleted bigint
        )
    """)
    for client in ("cli-1", "cli-2"):
        connection.execute(
            "insert into raw.clients values (?, ?)", [client, f"{client}@example.com"]
        )
        connection.execute("insert into marts.dim_client values (?, 'UK')", [client])
        for n in range(3):
            connection.execute("insert into raw.trades values (?, ?)", [f"t-{client}-{n}", client])
    return connection


@pytest.fixture
def targets():
    return [
        Target(table="raw.clients", action="delete", key_column="client_id"),
        Target(table="marts.dim_client", action="delete", key_column="client_id"),
        Target(table="raw.trades", action="retain", key_column="client_id",
               lawful_basis="Article 17(3)(b)"),
    ]


@pytest.fixture
def service(warehouse, targets):
    return ErasureService(
        vault=DuckDBKeyVault(warehouse),
        warehouse=DuckDBWarehouse(warehouse),
        targets=targets,
        tombstone_planner=lambda subject: [f"client.onboarding.v2 key={subject}"],
    )


def test_erase_removes_the_subject_and_leaves_everyone_else(service, warehouse):
    result = service.erase("cli-1")

    assert result.rows_deleted == {"raw.clients": 1, "marts.dim_client": 1}
    assert warehouse.execute("select count(*) from raw.clients").fetchone()[0] == 1
    assert warehouse.execute(
        "select count(*) from marts.dim_client where client_id = 'cli-2'"
    ).fetchone()[0] == 1


def test_retained_tables_are_not_touched(service, warehouse):
    """A trade is kept under a record-keeping obligation. The erasure must not quietly delete it."""
    service.erase("cli-1")
    assert warehouse.execute(
        "select count(*) from raw.trades where client_id = 'cli-1'"
    ).fetchone()[0] == 3


def test_verify_reports_zero_when_the_erasure_is_complete(service):
    service.erase("cli-1")
    assert set(service.verify("cli-1").values()) == {0}


def test_verify_catches_a_table_the_sweep_missed(service, warehouse):
    """The reason verify re-queries instead of trusting the delete counts."""
    service.erase("cli-1")
    warehouse.execute("insert into raw.clients values ('cli-1', 'came-back@example.com')")
    assert service.verify("cli-1")["raw.clients"] == 1


def test_tombstones_are_planned_for_the_subject(service):
    assert service.erase("cli-1").tombstones == ["client.onboarding.v2 key=cli-1"]


def test_sweep_processes_the_queue_and_marks_it_complete(service, warehouse):
    warehouse.execute(
        "insert into raw.erasure_requests values ('cli-1', ?, null, null)",
        [dt.datetime.now(dt.UTC) - dt.timedelta(days=2)],
    )
    results, overdue = service.sweep()

    assert [r.subject_id for r in results] == ["cli-1"]
    assert overdue == []
    completed, rows = warehouse.execute(
        "select completed_at, rows_deleted from raw.erasure_requests where client_id = 'cli-1'"
    ).fetchone()
    assert completed is not None
    assert rows == 2


def test_sweep_flags_a_request_past_the_deadline(service, warehouse):
    warehouse.execute(
        "insert into raw.erasure_requests values ('cli-2', ?, null, null)",
        [dt.datetime.now(dt.UTC) - dt.timedelta(days=31)],
    )
    _, overdue = service.sweep()
    assert overdue == ["cli-2"]


def test_key_is_destroyed_before_any_deletion(warehouse, targets):
    """If the run dies mid-sweep, the safe half is the one that already happened.

    The delete is made to fail, and the key must already be gone.
    """
    class ExplodingWarehouse(DuckDBWarehouse):
        def delete_rows(self, table, key_column, subject_id):
            raise RuntimeError("warehouse unavailable")

    vault = DuckDBKeyVault(warehouse)
    vault.create_key("cli-1")
    service = ErasureService(vault, ExplodingWarehouse(warehouse), targets)

    with pytest.raises(RuntimeError):
        service.erase("cli-1")

    assert warehouse.execute(
        "select count(*) from privacy.subject_keys where subject_id = 'cli-1'"
    ).fetchone()[0] == 0


def test_shipped_inventory_parses_and_every_retain_states_its_basis():
    """The inventory is a legal document as much as a config file."""
    for target in load_targets():
        assert target.action in {"delete", "shred", "retain", "none"}
        if target.action == "retain":
            assert target.lawful_basis, f"{target.table} retains data without naming a lawful basis"


def test_lake_tables_are_left_to_the_spark_job(warehouse):
    """The warehouse sweep can't rewrite Parquet, so it must not try, but it must still know."""
    targets = [
        Target(table="raw.clients", action="delete", key_column="client_id"),
        Target(table="lake.crm.clients", action="delete", key_column="client_id",
               engine="iceberg", location="s3://example-lake/warehouse/crm/clients"),
    ]
    service = ErasureService(DuckDBKeyVault(warehouse), DuckDBWarehouse(warehouse), targets)

    assert [t.table for t in service.targets] == ["raw.clients"]
    assert [t.table for t in service.lake_targets] == ["lake.crm.clients"]
    assert set(service.erase("cli-1").rows_deleted) == {"raw.clients"}


def test_shipped_lake_targets_say_where_they_live():
    for target in load_targets():
        if target.engine == "iceberg":
            assert target.location, f"{target.table} is a lake table with no storage location"
