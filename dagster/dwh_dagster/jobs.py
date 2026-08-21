"""Jobs mirroring the Airflow DAGs, plus source freshness.

* ``incremental_dbt_job`` — daily run (analogue of ``dbt_incremental_dag.py``). Materialises the
  partitioned dbt assets; the schedule launches it for the latest partition each day.
* ``full_refresh_job`` — manual rebuild / backfill (analogue of ``dbt_full_refresh_dag.py``), driven by
  config (``full_refresh`` + ``start_date``/``end_date``) mapping to the marts' ``force_full_refresh`` and
  window vars. Op-based (a single ``dbt build``) so it can rebuild all history in one run.
* ``source_freshness_job`` — ``dbt source freshness``, the first step of the Airflow incremental DAG.
"""

import json

from dagster import Config, OpExecutionContext, define_asset_job, job, op
from dagster_dbt import DbtCliResource

from .dbt_assets import dwh_dbt_assets

# --- Daily incremental (asset-native; partitioning inferred from the dbt assets) -----------------------
incremental_dbt_job = define_asset_job(
    name="incremental_dbt_job",
    selection=[dwh_dbt_assets],
    description="dbt build for one daily partition — idempotent insert_overwrite/delete+insert window.",
)


# --- Full refresh / backfill ---------------------------------------------------------------------------
class FullRefreshConfig(Config):
    """Mirrors the Airflow full-refresh DAG params."""

    full_refresh: bool = True
    start_date: str = "1900-01-01"
    end_date: str = "2999-12-31"


@op(description="Rebuild marts from scratch (full_refresh) or backfill a window via `dbt build`.")
def dbt_full_refresh_op(context: OpExecutionContext, dbt: DbtCliResource, config: FullRefreshConfig):
    dbt_vars = {
        # the marts read var('force_full_refresh') (string 'true'/'false') to set full_refresh
        "force_full_refresh": str(config.full_refresh).lower(),
        "start_date": config.start_date,
        "end_date": config.end_date,
    }
    context.log.info(
        "dbt build (full_refresh=%s, window %s..%s)",
        config.full_refresh, config.start_date, config.end_date,
    )
    dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context, raise_on_error=True).wait()


@job(description="Manual full refresh / backfill — rebuild all history (or a window) in a single run.")
def full_refresh_job():
    dbt_full_refresh_op()


# --- Source freshness ----------------------------------------------------------------------------------
@op(description="Run `dbt source freshness` against the raw sources.")
def dbt_source_freshness_op(context: OpExecutionContext, dbt: DbtCliResource):
    # Surface freshness without aborting the run; gate downstream with a sensor if you want hard blocking.
    dbt.cli(["source", "freshness"], context=context, raise_on_error=False).wait()


@job(description="Check raw source freshness (the gate the Airflow incremental DAG runs before build).")
def source_freshness_job():
    dbt_source_freshness_op()
