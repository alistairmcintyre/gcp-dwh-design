"""Dev-only ingestion stand-in: generate the synthetic ``raw.*`` tables as Dagster assets.

In production the ``raw.*`` sources are populated by real ingestion (Datastream / Fivetran / Pub/Sub into
BigQuery), so these assets are only included when ``DBT_TARGET == dev`` (see ``definitions.py``); in prod
the dbt sources appear as external upstream assets instead.

Locally this runs ``scripts/generate_test_data.py`` into the same DuckDB file the dbt ``dev`` target reads
and emits the asset keys ``raw/appsflyer_events``, ``raw/users``, ``raw/bets``, ``raw/transactions`` —
the keys the dbt sources resolve to, which makes the generator the upstream of the dbt graph.
"""

import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, AssetSpec, Config, MaterializeResult, multi_asset

# repo_root/dagster/dwh_dagster/raw_data.py -> repo_root
REPO_ROOT = Path(__file__).joinpath("..", "..", "..").resolve()
GENERATOR = REPO_ROOT / "scripts" / "generate_test_data.py"

RAW_TABLES = ["appsflyer_events", "users", "bets", "transactions"]


class RawDataConfig(Config):
    """Knobs for the synthetic generator (editable from the Dagster Launchpad)."""

    users: int = 600
    days: int = 30
    seed: int = 42


@multi_asset(
    specs=[
        AssetSpec(
            key=AssetKey(["raw", table]),
            group_name="raw_ingestion",
            description=f"Synthetic raw.{table} — dev stand-in for real ingestion (DuckDB).",
        )
        for table in RAW_TABLES
    ],
    compute_kind="python",
)
def raw_data(context: AssetExecutionContext, config: RawDataConfig):
    """Run the deterministic data generator, then mark each raw table materialised."""
    cmd = [
        sys.executable,
        str(GENERATOR),
        "--target", "duckdb",
        "--users", str(config.users),
        "--days", str(config.days),
        "--seed", str(config.seed),
    ]
    context.log.info("Generating synthetic raw data: %s", " ".join(cmd))
    # cwd = repo root so the default DUCKDB_PATH (./data/dev.duckdb) and --duckdb-path resolve correctly;
    # an absolute DUCKDB_PATH env (set in the container) overrides it so dbt + this asset share one file.
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT))

    for table in RAW_TABLES:
        yield MaterializeResult(asset_key=AssetKey(["raw", table]))
