"""Manual full-refresh / backfill dbt run on Cloud Composer via KubernetesPodOperator.

Trigger with config:
  * full_refresh=true                        -> rebuild marts from scratch (ignores the incremental window)
  * full_refresh=false + start_date/end_date -> backfill (insert_overwrite) that window

`force_full_refresh` is passed as a dbt var and consumed by the marts' `full_refresh` config.
"""

from __future__ import annotations

import os
import sys

import pendulum
from airflow import DAG
from airflow.models.param import Param

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from dbt_k8s import dbt_task  # noqa: E402

REFRESH_VARS = (
    '{"force_full_refresh": "{{ params.full_refresh }}", '
    '"start_date": "{{ params.start_date }}", '
    '"end_date": "{{ params.end_date }}"}'
)

with DAG(
    dag_id="dbt_full_refresh",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    params={
        "full_refresh": Param(True, type="boolean", description="Rebuild marts from scratch."),
        "start_date": Param("1900-01-01", type="string", description="Backfill window start (full_refresh=false)."),
        "end_date": Param("2999-12-31", type="string", description="Backfill window end (full_refresh=false)."),
    },
    tags=["dbt", "kubernetes", "bigquery", "full-refresh", "backfill"],
    doc_md=__doc__,
) as dag:
    build = dbt_task(
        task_id="dbt_build_full_refresh",
        dbt_args=["build", "--target", "prod", "--vars", REFRESH_VARS],
    )
