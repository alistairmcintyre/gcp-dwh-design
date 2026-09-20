"""Run the erasure sweep, or inspect what it would do.

    python -m privacy.cli sweep --dry-run           what is pending, and what it would touch
    python -m privacy.cli sweep                     process the queue (DuckDB by default)
    python -m privacy.cli sweep --target bigquery   the same, against the warehouse
    python -m privacy.cli verify --subject cli-42   rows still present for one subject
    python -m privacy.cli verify-all                the same for every request already completed
    python -m privacy.cli deadlines --warn-days 21  non-zero exit while a request is still fixable
    python -m privacy.cli tombstones --bootstrap …  produce the tombstones for recent erasures
    python -m privacy.cli check-topics              CI gate on the registry's erasure config
    python -m privacy.cli targets                   print the erasure inventory

Exit codes matter here: every command returns non-zero when a human needs to look, so the same
commands work as Airflow tasks, Dagster ops and a CI step without a wrapper.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

from privacy import tombstones as tombstone_module
from privacy.erasure import (
    DEADLINE_DAYS,
    BigQueryWarehouse,
    DuckDBWarehouse,
    ErasureService,
    load_targets,
)
from privacy.vault import BigQueryKeyVault, DuckDBKeyVault

DEFAULT_DB = "data/dev.duckdb"


class _ReadOnlyVault:
    """Stands in for the vault in a dry run, so nothing is destroyed while you are looking."""

    def destroy_key(self, subject_id: str, reason: str = "") -> bool:  # noqa: ARG002
        return False


def _service(args) -> tuple[ErasureService, object]:
    """Build the service for whichever backend, and whatever needs closing afterwards."""
    if args.target == "bigquery":
        from google.cloud import bigquery

        project = args.project or os.environ.get("GCP_PROJECT")
        if not project:
            raise SystemExit("set GCP_PROJECT or pass --project for the bigquery target")
        client = bigquery.Client(project=project)
        dry = getattr(args, "dry_run", False)
        vault = _ReadOnlyVault() if dry else BigQueryKeyVault(client, project)
        warehouse = BigQueryWarehouse(client, project)
        closer = None
    else:
        import duckdb

        dry = getattr(args, "dry_run", False)
        connection = duckdb.connect(args.database, read_only=dry)
        vault = _ReadOnlyVault() if dry else DuckDBKeyVault(connection)
        warehouse = DuckDBWarehouse(connection)
        closer = connection

    service = ErasureService(
        vault,
        warehouse,
        tombstone_planner=lambda subject: [t.describe() for t in tombstone_module.plan(subject)],
    )
    return service, closer


def _close(closer) -> None:
    if closer is not None:
        closer.close()


def sweep(args) -> int:
    service, closer = _service(args)
    try:
        pending = service.warehouse.pending_requests()
        if not pending:
            print("no pending erasure requests")
            return 0
        suffix = " (dry run, nothing will change)" if args.dry_run else ""
        print(f"{len(pending)} pending request(s){suffix}")

        if args.dry_run:
            for subject_id, requested_at in pending:
                print(f"\n  {subject_id}  requested {requested_at:%Y-%m-%d}")
                for table, rows in service.verify(subject_id).items():
                    if rows:
                        print(f"    would delete {rows:>6} row(s) from {table}")
                for tombstone in tombstone_module.plan(subject_id):
                    print(f"    would tombstone {tombstone.describe()}")
            return 0

        failed = False
        results, overdue = service.sweep()
        for result in results:
            print(f"\n  {result.subject_id}: key destroyed={result.key_destroyed}, "
                  f"{result.total_rows_deleted} row(s) deleted")
            for table, rows in result.rows_deleted.items():
                if rows:
                    print(f"    {rows:>6}  {table}")
            for tombstone in result.tombstones:
                print(f"    tombstone  {tombstone}")
            remaining = {t: n for t, n in service.verify(result.subject_id).items() if n}
            if remaining:
                print(f"    INCOMPLETE, rows remain: {remaining}")
                failed = True
        if overdue:
            print(f"\nPAST THE {DEADLINE_DAYS}-DAY DEADLINE: {', '.join(overdue)}")
            failed = True
        return 1 if failed else 0
    finally:
        _close(closer)


def verify(args) -> int:
    args.dry_run = True
    service, closer = _service(args)
    try:
        remaining = service.verify(args.subject)
        for table, rows in remaining.items():
            print(f"  {rows:>6}  {table}")
        total = sum(remaining.values())
        print(f"\n{args.subject}: {total} row(s) remain in delete targets")
        return 1 if total else 0
    finally:
        _close(closer)


def verify_all(args) -> int:
    """Re-check every completed request.

    Worth running on a schedule rather than only after a sweep: a restore from backup, a backfill
    that replays an old extract, or a new model reading an older source can all put an erased
    subject back, and none of them announce themselves.
    """
    args.dry_run = True
    service, closer = _service(args)
    try:
        completed = service.warehouse.completed_requests()
        if not completed:
            print("no completed erasure requests to re-check")
            return 0
        problems = 0
        for subject_id, completed_at in completed:
            remaining = {t: n for t, n in service.verify(subject_id).items() if n}
            if remaining:
                problems += 1
                print(f"  {subject_id} (erased {completed_at:%Y-%m-%d}) is back: {remaining}")
        print(f"\nre-checked {len(completed)} completed request(s), {problems} with data present")
        return 1 if problems else 0
    finally:
        _close(closer)


def deadlines(args) -> int:
    service, closer = _service(args)
    try:
        now = dt.datetime.now(dt.UTC)
        breaching = []
        for subject_id, requested_at in service.warehouse.pending_requests():
            if requested_at.tzinfo is None:
                requested_at = requested_at.replace(tzinfo=dt.UTC)
            age = (now - requested_at).days
            marker = "  <-- act now" if age >= args.warn_days else ""
            print(f"  {subject_id}  open {age:>3} day(s){marker}")
            if age >= args.warn_days:
                breaching.append(subject_id)
        if breaching:
            print(f"\n{len(breaching)} request(s) at or past {args.warn_days} days, "
                  f"deadline is {DEADLINE_DAYS}")
            return 1
        print("\nevery open request is inside the deadline")
        return 0
    finally:
        _close(closer)


def emit_tombstones(args) -> int:
    """Produce tombstones for requests completed recently.

    Driven off completed requests rather than pending ones so it cannot run ahead of the sweep, and
    re-running is harmless: a second tombstone for the same key is the same null record.
    """
    args.dry_run = not args.bootstrap
    service, closer = _service(args)
    try:
        subjects = [s for s, _ in service.warehouse.completed_requests(since_days=args.since_days)]
        if not subjects:
            print(f"no requests completed in the last {args.since_days} day(s)")
            return 0
        planned = [t for subject in subjects for t in tombstone_module.plan(subject)]
        for tombstone in planned:
            print(f"  {tombstone.describe()}")
        if not args.bootstrap:
            print(f"\n{len(planned)} tombstone(s) planned. No --bootstrap, so nothing was sent.")
            return 0
        sent = tombstone_module.emit(planned, args.bootstrap)
        print(f"\nproduced {sent} tombstone(s) to {args.bootstrap}")
        return 0
    finally:
        _close(closer)


def check_topics(args) -> int:  # noqa: ARG001
    problems = tombstone_module.check_erasure_config(tombstone_module.load_topics())
    for problem in problems:
        print(f"  {problem}")
    if problems:
        print(f"\n{len(problems)} topic(s) cannot satisfy an erasure request as configured")
        return 1
    print("every topic carrying personal data declares a workable erasure method")
    return 0


def show_targets(args) -> int:  # noqa: ARG001
    for target in load_targets():
        basis = f"  [{target.lawful_basis}]" if target.lawful_basis else ""
        print(f"  {target.action:<7} {target.table}{basis}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--target", choices=["duckdb", "bigquery"], default="duckdb")
    parser.add_argument("--database", default=DEFAULT_DB, help="DuckDB path")
    parser.add_argument("--project", default=None, help="GCP project for the bigquery target")
    sub = parser.add_subparsers(dest="command", required=True)

    sweep_parser = sub.add_parser("sweep", help="process pending erasure requests")
    sweep_parser.add_argument("--dry-run", action="store_true")
    sweep_parser.set_defaults(func=sweep)

    verify_parser = sub.add_parser("verify", help="rows still present for one subject")
    verify_parser.add_argument("--subject", required=True)
    verify_parser.set_defaults(func=verify)

    recheck = sub.add_parser("verify-all", help="re-check every completed request")
    recheck.set_defaults(func=verify_all)

    deadline_parser = sub.add_parser("deadlines", help="open requests and their age")
    deadline_parser.add_argument("--warn-days", type=int, default=21)
    deadline_parser.set_defaults(func=deadlines)

    tombstone_parser = sub.add_parser("tombstones", help="produce tombstones for recent erasures")
    tombstone_parser.add_argument("--bootstrap", default="", help="Kafka bootstrap servers")
    tombstone_parser.add_argument("--since-days", type=int, default=7)
    tombstone_parser.set_defaults(func=emit_tombstones)

    sub.add_parser("check-topics", help="CI gate on the registry").set_defaults(func=check_topics)
    sub.add_parser("targets", help="print the erasure inventory").set_defaults(func=show_targets)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
