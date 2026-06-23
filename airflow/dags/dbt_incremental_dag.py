"""Daily incremental dbt run on Cloud Composer via KubernetesPodOperator.

Each task is a pod running the dbt image against BigQuery. The run's data interval is passed to dbt as
`start_date`/`end_date` vars, so each daily run (and any re-run) idempotently rebuilds just that day's
partitions. No dbt is installed on Composer — it all lives in the image.
"""

from __future__ import annotations

import os
import sys

import pendulum
from airflow import DAG

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from dbt_k8s import dbt_task  # noqa: E402

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
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["dbt", "kubernetes", "bigquery", "incremental"],
    doc_md=__doc__,
) as dag:
    source_freshness = dbt_task(
        task_id="dbt_source_freshness",
        dbt_args=["source", "freshness", "--target", "prod"],
    )

    build = dbt_task(
        task_id="dbt_build",
        dbt_args=["build", "--target", "prod", "--vars", WINDOW_VARS],
    )

    source_freshness >> build
