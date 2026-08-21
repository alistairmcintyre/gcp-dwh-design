"""The code location entrypoint: assets, jobs, schedules, sensors and the dbt resource.

This is the object Dagster loads (``[tool.dagster] module_name`` / ``workspace.yaml``).
"""

from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .dbt_assets import dwh_dbt_assets
from .jobs import full_refresh_job, incremental_dbt_job, source_freshness_job
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

defs = Definitions(
    assets=assets,
    jobs=[incremental_dbt_job, full_refresh_job, source_freshness_job],
    schedules=[daily_incremental_schedule, source_freshness_schedule],
    sensors=build_alert_sensors(),
    resources={"dbt": dbt_resource},
)
