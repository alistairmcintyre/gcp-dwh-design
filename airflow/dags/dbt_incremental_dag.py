"""Daily incremental dbt run on Cloud Composer via KubernetesPodOperator.

Each dbt task is a pod running the dbt image against BigQuery. The run's data interval is passed to
dbt as `start_date`/`end_date` vars, so each daily run (and any re-run) idempotently rebuilds just
that day's partitions. No dbt is installed on Composer, it all lives in the image.

WHY SENSORS GATE THE BUILD, AND dbt TESTS STILL ASSERT IT
---------------------------------------------------------
Two different questions, with two different failure semantics:

* **"Should we run yet?"** belongs in Airflow, because a sensor can *wait*. A dbt test is a
  point-in-time assertion, it passes or it fails. If upstream is habitually twenty minutes late, a
  test turns that into a nightly page, whereas a sensor in `reschedule`/deferrable mode simply
  absorbs the lateness and starts when the data lands. dbt has no equivalent of waiting.
  The sensor also fails *before* a pod is started, so a missing partition costs nothing.

* **"Is what we're about to build sound?"** stays in dbt (`rows_for_window` on the sources), because
  that assertion has to travel with the project. The same models are built from Dagster, from CI, and
  from a developer's laptop during a backfill, none of which run this DAG. An Airflow-only gate
  protects exactly one of those paths.

They are not redundant: the sensor decides *whether* to start, the test decides whether the window is
safe to overwrite. The marts use `insert_overwrite`, so building an empty window would replace a good
partition with nothing, which is why the assertion is worth having twice.

WHY SOURCE FRESHNESS IS NO LONGER ON THE CRITICAL PATH
------------------------------------------------------
Once a partition sensor gates the DAG, `dbt source freshness` is redundant *as a gate*, the sensor
answers the same question more precisely and for the exact partition being built. It still runs, as an
independent task, because it produces `sources.json` covering **every** source in the project, not
just the two this DAG happens to depend on, and Elementary consumes those results for freshness
monitoring. It no longer blocks the build.
"""

from __future__ import annotations

import os
import sys

import pendulum
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTablePartitionExistenceSensor,
)
from airflow.sdk import DAG

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from alerts import slack_failure_callback  # noqa: E402
from dbt_k8s import dbt_task  # noqa: E402

GCP_PROJECT = os.getenv("GCP_PROJECT", "")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")

# dbt --vars payload (YAML/JSON). Airflow templates the {{ ... }} before the pod starts.
WINDOW_VARS = (
    '{"start_date": "{{ data_interval_start | ds }}", '
    '"end_date": "{{ data_interval_start | ds }}"}'
)

with DAG(
    dag_id="dbt_incremental",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": slack_failure_callback,  # Slack alert on task failure (incl. dbt test fails)
    },
    tags=["dbt", "kubernetes", "bigquery", "incremental"],
    doc_md=__doc__,
) as dag:
    # Partition-existence sensors: a METADATA lookup, so each poke is free. A SQLCheckOperator doing
    # `select count(*)` would cost a BigQuery query per poke, polling hourly for a day is 24 charged
    # queries per source. Prefer the partition sensor wherever the source is partitioned; fall back to
    # SQLCheckOperator only for sources that are not.
    #
    # `deferrable=True` hands the wait to the triggerer, so it holds no worker slot. Without it (or
    # without mode="reschedule") a poking sensor occupies a pool slot for the whole timeout and a
    # handful of them will starve the Composer worker pool, the classic way this pattern goes wrong.
    wait_for_deals = BigQueryTablePartitionExistenceSensor(
        task_id="wait_for_deals_partition",
        project_id=GCP_PROJECT,
        dataset_id=RAW_DATASET,
        table_id="trades",
        partition_id="{{ data_interval_start | ds_nodash }}",
        deferrable=True,
        timeout=60 * 60 * 4,  # give upstream four hours, then fail rather than wait forever
        poke_interval=300,
    )

    wait_for_transactions = BigQueryTablePartitionExistenceSensor(
        task_id="wait_for_account_transactions_partition",
        project_id=GCP_PROJECT,
        dataset_id=RAW_DATASET,
        table_id="account_transactions",
        partition_id="{{ data_interval_start | ds_nodash }}",
        deferrable=True,
        timeout=60 * 60 * 4,
        poke_interval=300,
    )

    # `dbt build` interleaves tests with models, so a failing source test (rows_for_window) blocks the
    # marts downstream of it rather than letting an empty window overwrite a good partition.
    build = dbt_task(
        task_id="dbt_build",
        dbt_args=["build", "--target", "prod", "--vars", WINDOW_VARS],
    )

    # Estate-wide freshness report, independent of the build. Covers every source in the project,
    # including ones this DAG does not gate on, and feeds Elementary's freshness monitoring.
    source_freshness = dbt_task(
        task_id="dbt_source_freshness_report",
        dbt_args=["source", "freshness", "--target", "prod"],
    )

    [wait_for_deals, wait_for_transactions] >> build
