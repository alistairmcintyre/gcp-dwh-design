"""The dbt project as a partitioned Dagster asset graph.

``@dbt_assets`` reads the dbt manifest and produces one asset per dbt model (plus an asset check per
dbt test), preserving dbt's ``ref``/``source`` lineage.

The function body runs ``dbt build`` for the active partition and streams dbt's structured events back
to Dagster, which turns them into per-model materialisations and per-test asset-check results.
"""

import json

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .partitions import daily_partitions
from .project import dbt_project
from .translator import DBT_TRANSLATOR_SETTINGS, DwhDbtTranslator

dwh_dbt_translator = DwhDbtTranslator(settings=DBT_TRANSLATOR_SETTINGS)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=dwh_dbt_translator,
    partitions_def=daily_partitions,
)
def dwh_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # Single-day window, matching the Airflow incremental DAG's WINDOW_VARS. The marts filter
    # `where activity_date between start_date and end_date`, so this rebuilds just that day.
    day = context.partition_time_window.start.strftime("%Y-%m-%d")
    dbt_vars = {"start_date": day, "end_date": day}

    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)],
        context=context,
    ).stream()
