"""Schedules: the Dagster equivalent of the Airflow DAG ``schedule`` arg.

``build_schedule_from_partitioned_job`` derives the cron from the partition cadence (daily) and launches
the job for the most recent completed partition, like ``@daily`` + ``data_interval_start`` in Airflow.
"""

from __future__ import annotations

from dagster import ScheduleDefinition, build_schedule_from_partitioned_job

from .jobs import incremental_dbt_job, source_freshness_job

# Runs incremental_dbt_job daily for the latest partition (06:00).
daily_incremental_schedule = build_schedule_from_partitioned_job(
    incremental_dbt_job,
    hour_of_day=6,
    minute_of_hour=0,
)

# Check source freshness a bit earlier, before the build window.
source_freshness_schedule = ScheduleDefinition(
    name="source_freshness_schedule",
    job=source_freshness_job,
    cron_schedule="30 5 * * *",
)
