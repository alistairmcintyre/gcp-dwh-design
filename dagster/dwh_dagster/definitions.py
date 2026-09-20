"""The code location entrypoint: assets, jobs, schedules, sensors and the dbt resource.

This is the object Dagster loads (``[tool.dagster] module_name`` / ``workspace.yaml``).
"""

from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .dbt_assets import dwh_dbt_assets
from .gates import build_raw_freshness_checks, raw_partitions_ready_sensor
from .jobs import full_refresh_job, incremental_dbt_job, source_freshness_job
from .privacy_jobs import daily_erasure_schedule, gdpr_erasure_job
from .project import DBT_PROFILES_DIR, DBT_TARGET, dbt_project
from .raw_data import raw_data
from .schedules import daily_incremental_schedule, source_freshness_schedule
from .sensors import build_alert_sensors

# One dbt resource, shared by the assets and the op-based jobs (bound under the key `dbt`).
dbt_resource = DbtCliResource(project_dir=dbt_project, profiles_dir=str(DBT_PROFILES_DIR))

# raw_data is the dev-only ingestion stand-in; in prod the raw sources come from real ingestion.
assets = [dwh_dbt_assets]
if DBT_TARGET == "dev":
    assets.append(raw_data)

# Readiness gate: the Dagster analogue of the Airflow partition sensors. Registered STOPPED by
# default so it sits alongside the schedule rather than competing with it -- enable one or the
# other. The schedule runs on a clock; the sensor runs when the data is actually there.
sensors = [*build_alert_sensors(), raw_partitions_ready_sensor]

# Freshness checks on the raw assets, reported rather than gating -- see gates.py. Only registered
# where the raw asset keys are defined in this code location (dev); in prod they attach to the
# external source assets dbt declares.
asset_checks = list(build_raw_freshness_checks()) if DBT_TARGET == "dev" else []

defs = Definitions(
    assets=assets,
    asset_checks=asset_checks,
    jobs=[incremental_dbt_job, full_refresh_job, source_freshness_job, gdpr_erasure_job],
    schedules=[daily_incremental_schedule, source_freshness_schedule, daily_erasure_schedule],
    sensors=sensors,
    resources={"dbt": dbt_resource},
)
