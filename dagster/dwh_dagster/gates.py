"""Readiness gates: the Dagster equivalent of the Airflow partition sensors.

WHY THIS FILE LOOKS DIFFERENT FROM THE AIRFLOW DAG
---------------------------------------------------
Airflow gates with a *blocking wait*: a schedule fires, a sensor occupies (or defers) until the
partition appears, then the build runs. The unit of thought is a task that waits.

Dagster has no equivalent and does not need one, because assets carry state. The idiomatic gate is
not "wait inside the run", it is **do not launch the run until the data is there**: the sensor
evaluates cheaply on a tick, and either requests a run or skips with a reason. Nothing is held open,
so there is no worker slot to starve and no `deferrable=True` to remember.

That is the real difference to articulate: Airflow waits, Dagster re-evaluates.

Two gates here, matching the two in `airflow/dags/dbt_incremental_dag.py`:

* ``raw_partitions_ready_sensor``: "should we start?" Only requests a run for a partition once every
  required raw source holds rows for that day. Replaces the blocking sensors.
* ``build_raw_freshness_checks``: "is the pipe alive?" Freshness checks on the raw assets, the
  analogue of `dbt source freshness` demoted to a report (they surface as asset checks rather than
  gating the build).

The *third* gate, "is this window safe to overwrite?", deliberately stays in dbt as the
`rows_for_window` source test, for the same reason as in Airflow: it has to travel with the project so
it also protects backfills and CI, neither of which run this sensor.

FULLY DECLARATIVE ALTERNATIVE
-----------------------------
Dagster's most native answer removes the schedule altogether: put an ``AutomationCondition`` on the
dbt assets (``AutomationCondition.eager()``) and let a partition materialise when its parents have.
That is strictly better where Dagster *owns* the upstream assets. It is not used here because in prod
the raw tables are populated by ingestion Dagster does not run, so something still has to observe
that the external data landed, which is exactly what this sensor does.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from dagster import (
    AssetKey,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    build_last_update_freshness_checks,
    sensor,
)

from .jobs import incremental_dbt_job
from .partitions import daily_partitions

# Raw source -> the event-time column that decides which day a row belongs to. Mirrors the
# `rows_for_window` test configuration in dbt/models/staging/_sources.yml, deliberately: if the two
# ever disagree, the gate and the assertion are guarding different things.
REQUIRED_SOURCES: dict[str, str] = {
    "trades": "closed_at",
    "account_transactions": "created_at",
}

RAW_ASSET_KEYS = [AssetKey(["raw", table]) for table in REQUIRED_SOURCES]


def _partition_row_count(table: str, date_column: str, day: str) -> int:
    """Rows in `table` whose event date is `day`. Warehouse-agnostic, mirroring the dbt targets.

    Cheap by construction: a count over one partition, not a scan of history. On BigQuery the raw
    tables are date-partitioned, so this prunes to a single partition.
    """
    target = os.getenv("DBT_TARGET", "dev")

    if target == "dev":
        import duckdb

        path = os.environ["DUCKDB_PATH"]
        con = duckdb.connect(path, read_only=True)
        try:
            sql = (
                f"select count(*) from raw.{table} "  # noqa: S608, table names are from a fixed dict
                f"where cast({date_column} as date) = date '{day}'"
            )
            return int(con.sql(sql).fetchone()[0])
        finally:
            con.close()

    from google.cloud import bigquery

    project = os.environ["GCP_PROJECT"]
    dataset = os.getenv("BQ_RAW_DATASET", "raw")
    client = bigquery.Client(project=project)
    sql = (
        f"select count(*) as n from `{project}.{dataset}.{table}` "  # noqa: S608
        f"where date({date_column}) = @day"
    )
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
        ),
    )
    return int(next(iter(job.result())).n)


@sensor(
    name="raw_partitions_ready_sensor",
    job=incremental_dbt_job,
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Request the incremental dbt run for a day only once every required raw source holds rows "
        "for it. The Dagster analogue of the Airflow partition sensors, it re-evaluates rather "
        "than waiting, so it holds no slot."
    ),
)
def raw_partitions_ready_sensor(context: SensorEvaluationContext):
    """Evaluate the most recent complete day; request a run when all sources are present.

    NOTE: this function is a generator (it yields a RunRequest), so every skip path must **yield**
    its SkipReason rather than return it. `return SkipReason(...)` inside a generator emits nothing:
    the sensor appears to tick with no result and the UI shows no reason, which is a silent gate
    failure and very hard to spot after the fact.
    """
    # The day the Airflow DAG would call `data_interval_start`: yesterday, UTC.
    day = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

    if day not in daily_partitions.get_partition_keys():
        yield SkipReason(f"{day} is outside the partition range; nothing to do.")
        return

    # The cursor stops a satisfied day being requested on every tick. Dagster deduplicates by run key
    # as well, so this is belt-and-braces, but it keeps the tick log readable.
    if context.cursor == day:
        yield SkipReason(f"Partition {day} already requested.")
        return

    missing: list[str] = []
    counts: dict[str, int] = {}
    for table, date_column in REQUIRED_SOURCES.items():
        try:
            counts[table] = _partition_row_count(table, date_column, day)
        except Exception as exc:  # noqa: BLE001, an unreachable warehouse is "not ready", not a crash
            yield SkipReason(f"Could not read raw.{table}: {exc}")
            return
        if counts[table] == 0:
            missing.append(table)

    if missing:
        # Skipping rather than failing is the point: upstream being late is normal, and the next tick
        # will pick it up. A hard failure here would page someone for a pipeline working as designed.
        yield SkipReason(
            f"Partition {day} not ready, no rows yet in: {', '.join(sorted(missing))}. "
            f"Counts: {counts}"
        )
        return

    context.update_cursor(day)
    context.log.info(f"Partition {day} ready: {counts}. Requesting incremental dbt run.")
    yield RunRequest(run_key=f"incremental-{day}", partition_key=day)


def build_raw_freshness_checks():
    """Freshness checks on the raw assets, the analogue of `dbt source freshness` as a report.

    These surface as asset checks in the UI rather than gating the build, matching the Airflow DAG
    where freshness was demoted from gate to report once the partition gate existed.
    """
    return build_last_update_freshness_checks(
        assets=RAW_ASSET_KEYS,
        lower_bound_delta=timedelta(hours=26),  # daily sources, with headroom for a late run
    )
