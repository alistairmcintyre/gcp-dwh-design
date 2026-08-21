"""Daily partitions: the Dagster equivalent of the Airflow data interval.

Materialising a partition passes that day as the ``start_date``/``end_date`` dbt vars, so the
incremental marts rebuild just that day (``insert_overwrite`` on BigQuery, ``delete+insert`` on DuckDB).
"""

from __future__ import annotations

import os

from dagster import DailyPartitionsDefinition

# Matches the Airflow DAGs' start_date and the dbt project's default window floor.
PARTITION_START_DATE = os.getenv("DAGSTER_PARTITION_START_DATE", "2026-01-01")

daily_partitions = DailyPartitionsDefinition(start_date=PARTITION_START_DATE)
