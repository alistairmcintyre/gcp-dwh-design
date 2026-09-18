"""Tests for the readiness gate.

Run with the Dagster venv (the code location's environment), from the `dagster/` directory:

    DBT_TARGET=dev DUCKDB_PATH=../data/dev.duckdb .venv/bin/python -m pytest tests -q

The third test is the one that matters. `raw_partitions_ready_sensor` is a generator, so a skip path
that `return`s a SkipReason instead of `yield`ing one emits nothing at all: the sensor ticks, shows no
reason in the UI, and never launches, a gate that is silently always closed. That is close to
undetectable by reading the code, so it is asserted here instead.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from dagster import RunRequest, SkipReason, build_sensor_context

from dwh_dagster import gates


def _yesterday() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


@pytest.fixture
def all_sources_present(monkeypatch):
    monkeypatch.setattr(gates, "_partition_row_count", lambda table, col, day: 100)


@pytest.fixture
def trades_missing(monkeypatch):
    monkeypatch.setattr(
        gates, "_partition_row_count",
        lambda table, col, day: 0 if table == "trades" else 100,
    )


def test_requests_a_run_when_every_source_has_rows(all_sources_present):
    results = list(gates.raw_partitions_ready_sensor(build_sensor_context(sensor_name="s")))
    assert len(results) == 1
    assert isinstance(results[0], RunRequest)
    assert results[0].partition_key == _yesterday()


def test_skips_when_a_source_is_empty(trades_missing):
    results = list(gates.raw_partitions_ready_sensor(build_sensor_context(sensor_name="s")))
    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    # The reason must name the offending source and carry the counts -- a bare "not ready" tells
    # whoever is on call nothing.
    assert "trades" in results[0].skip_message
    assert "Counts:" in results[0].skip_message


def test_every_skip_path_yields_rather_than_returns(trades_missing):
    """A generator that `return`s a SkipReason emits nothing; the gate would be silently always shut."""
    results = list(gates.raw_partitions_ready_sensor(build_sensor_context(sensor_name="s")))
    assert results, "sensor produced no result at all -- a skip path returned instead of yielding"


def test_cursor_prevents_re_requesting_the_same_partition(all_sources_present):
    ctx = build_sensor_context(sensor_name="s", cursor=_yesterday())
    results = list(gates.raw_partitions_ready_sensor(ctx))
    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert "already requested" in results[0].skip_message


def test_unreachable_warehouse_skips_rather_than_crashes(monkeypatch):
    """Upstream being unreadable is 'not ready', not a page. The next tick retries."""
    def boom(table, col, day):
        raise ConnectionError("warehouse unreachable")

    monkeypatch.setattr(gates, "_partition_row_count", boom)
    results = list(gates.raw_partitions_ready_sensor(build_sensor_context(sensor_name="s")))
    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert "Could not read" in results[0].skip_message
