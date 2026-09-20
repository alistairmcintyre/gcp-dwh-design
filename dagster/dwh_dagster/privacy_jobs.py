"""The erasure sweep as Dagster ops, the counterpart to airflow/dags/gdpr_erasure_dag.py.

Ops rather than assets, deliberately. An asset describes a thing that exists and can be rebuilt from
its inputs; erasure is the opposite, an action whose whole purpose is that something stops existing
and cannot come back. Modelling it as an asset would put a "materialize" button next to it, which is
the wrong verb entirely.

Three ops, in the same order as the Airflow DAG: report the queue, do the work, then check the work
independently of the thing that did it.
"""

import pathlib
import sys

from dagster import OpExecutionContext, ScheduleDefinition, job, op

# The privacy package lives at the repo root, alongside this code location rather than inside it.
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DUCKDB_PATH = str(REPO_ROOT / "data" / "dev.duckdb")


def _service(read_only: bool = False):
    import duckdb

    from privacy.erasure import DuckDBWarehouse, ErasureService
    from privacy.tombstones import plan
    from privacy.vault import DuckDBKeyVault

    connection = duckdb.connect(DUCKDB_PATH, read_only=read_only)
    vault = DuckDBKeyVault(connection) if not read_only else None
    service = ErasureService(
        vault,
        DuckDBWarehouse(connection),
        tombstone_planner=lambda subject: [t.describe() for t in plan(subject)],
    )
    return service, connection


@op(description="Report how long each open erasure request has been waiting.")
def erasure_deadlines_op(context: OpExecutionContext) -> int:
    import datetime as dt

    from privacy.erasure import DEADLINE_DAYS

    service, connection = _service(read_only=True)
    try:
        now = dt.datetime.now(dt.UTC)
        at_risk = []
        for subject_id, requested_at in service.warehouse.pending_requests():
            if requested_at.tzinfo is None:
                requested_at = requested_at.replace(tzinfo=dt.UTC)
            age = (now - requested_at).days
            context.log.info(f"{subject_id}: open {age} day(s) of {DEADLINE_DAYS}")
            if age >= DEADLINE_DAYS - 9:
                at_risk.append(subject_id)
        if at_risk:
            # A warning rather than a failure: the sweep that follows is what fixes it, and failing
            # here would stop the very thing that clears the backlog.
            context.log.warning(f"close to the deadline: {', '.join(at_risk)}")
        return len(at_risk)
    finally:
        connection.close()


@op(description="Shred keys, delete rows and plan tombstones for every pending request.")
def erasure_sweep_op(context: OpExecutionContext, at_risk: int):
    service, connection = _service()
    try:
        results, overdue = service.sweep()
        for result in results:
            context.log.info(
                f"{result.subject_id}: key destroyed={result.key_destroyed}, "
                f"{result.total_rows_deleted} row(s) deleted, "
                f"{len(result.tombstones)} tombstone(s) planned"
            )
        if overdue:
            raise RuntimeError(f"processed past the deadline: {', '.join(overdue)}")
        if at_risk and not results:
            raise RuntimeError("requests are near the deadline but the sweep found nothing to do")
        return [result.subject_id for result in results]
    finally:
        connection.close()


@op(description="Re-query every completed request to prove the data is actually gone.")
def erasure_verify_op(context: OpExecutionContext, swept):  # noqa: ARG001
    service, connection = _service(read_only=True)
    try:
        problems = {}
        for subject_id, _ in service.warehouse.completed_requests():
            remaining = {t: n for t, n in service.verify(subject_id).items() if n}
            if remaining:
                problems[subject_id] = remaining
        if problems:
            raise RuntimeError(f"erased subjects still present: {problems}")
        context.log.info("every completed request verified clean")
    finally:
        connection.close()


@job(description="Daily GDPR erasure sweep: shred, delete, tombstone, verify.")
def gdpr_erasure_job():
    erasure_verify_op(erasure_sweep_op(erasure_deadlines_op()))


daily_erasure_schedule = ScheduleDefinition(
    job=gdpr_erasure_job,
    cron_schedule="0 2 * * *",
    description="Runs before the business day, so a failure has a working day to be dealt with.",
)
