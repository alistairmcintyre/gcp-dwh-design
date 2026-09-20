"""Daily GDPR erasure sweep.

Erasure is a queue, not an event. A request lands in ``raw.erasure_requests``, processing for that
client stops on the next dbt build because ``stg_clients`` filters the queue, and this DAG does the
slower half: destroy the key, delete the rows, tombstone the keyed topics, and prove the result.

Why daily rather than on demand. Nothing here is fast: Kafka compaction runs on its own schedule,
BigQuery DML on a large partitioned table is not instant, and the deadline is a month. A predictable
job that always runs, reports, and can be re-run is easier to defend to an auditor than a handler
fired by a webhook that may or may not have succeeded three weeks ago. The deadline check is what
turns the queue into an alert, and it runs before the sweep so a backlog is visible even if the
sweep then fails.

Order matters within the sweep itself (privacy/erasure.py destroys the key first). The order here is
sweep, then verify, then tombstone: the warehouse is the copy most likely to be queried by a human
tomorrow, and the tombstone is the part with a compaction delay attached to it anyway.
"""

from __future__ import annotations

import os
import sys

import pendulum
from airflow.sdk import DAG

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from alerts import slack_failure_callback  # noqa: E402
from dbt_k8s import pod_task  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")

with DAG(
    dag_id="gdpr_erasure",
    description="Shred keys, delete rows and tombstone topics for pending erasure requests.",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "on_failure_callback": slack_failure_callback,
    },
    tags=["privacy", "gdpr", "erasure"],
) as dag:
    # Fails the run when a request is close to the one-month deadline. First, so a backlog is
    # reported even on a day the sweep itself breaks.
    deadline_check = pod_task(
        task_id="check_deadlines",
        command=["python"],
        arguments=["-m", "privacy.cli", "deadlines", "--warn-days", "21"],
        name_prefix="erasure",
    )

    sweep = pod_task(
        task_id="sweep",
        command=["python"],
        arguments=["-m", "privacy.cli", "sweep", "--target", "bigquery"],
        name_prefix="erasure",
    )

    # Re-queries rather than trusting the sweep's own counts, so a table added to the inventory
    # after the request was raised still gets caught.
    verify = pod_task(
        task_id="verify",
        command=["python"],
        arguments=["-m", "privacy.cli", "verify-all", "--target", "bigquery"],
        name_prefix="erasure",
    )

    tombstones = pod_task(
        task_id="tombstone_topics",
        command=["python"],
        arguments=[
            "-m", "privacy.cli", "tombstones",
            "--target", "bigquery", "--bootstrap", KAFKA_BOOTSTRAP,
        ],
        name_prefix="erasure",
    )

    deadline_check >> sweep >> verify >> tombstones
