"""Locate the dbt project and (in local dev) keep its manifest up to date.

Dagster's dbt integration is driven by the dbt manifest (``target/manifest.json``). ``DbtProject`` wraps
the dbt project directory and:

* in local dev (``dagster dev``) regenerates the manifest via ``dbt parse`` on every code reload, so
  editing a model updates the asset graph — see ``prepare_if_dev()`` below;
* in the image the manifest is generated at build time (``dagster/Dockerfile`` runs ``dbt deps &&
  dbt parse``), so the container never shells out to dbt to load definitions.

Paths and target are env-configurable so the same code runs locally and in the container.
"""

from __future__ import annotations

import os
from pathlib import Path

from dagster_dbt import DbtProject

# repo_root/dagster/dwh_dagster/project.py -> repo_root/dbt
_DEFAULT_DBT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt").resolve()

DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", str(_DEFAULT_DBT_DIR)))
DBT_PROFILES_DIR = Path(os.getenv("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR)))
# dev = local DuckDB (same dual-target profiles.yml the Airflow/Composer flow uses); prod = BigQuery.
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

# dbt resolves profiles from --profiles-dir / $DBT_PROFILES_DIR; make the project's own profiles.yml the
# default so `dbt parse` (manifest build) works without extra flags.
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_PROFILES_DIR))

# Pin one absolute local DuckDB path shared by the dbt CLI and the dev raw-data asset. The dbt subprocess
# runs with cwd=<dbt project dir> while the generator runs from the repo root, so a *relative* DUCKDB_PATH
# would resolve to two different files. In the container DUCKDB_PATH is already absolute, so this no-ops.
_REPO_ROOT = Path(__file__).joinpath("..", "..", "..").resolve()
_duckdb_path = os.getenv("DUCKDB_PATH")
if not _duckdb_path:
    _duckdb_path = str(_REPO_ROOT / "data" / "dev.duckdb")
elif not os.path.isabs(_duckdb_path):
    _duckdb_path = str((_REPO_ROOT / _duckdb_path).resolve())
os.environ["DUCKDB_PATH"] = _duckdb_path

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    target=DBT_TARGET,
)

# No-op unless DAGSTER_IS_DEV_CLI is set (i.e. `dagster dev`); in the container the baked manifest is used.
dbt_project.prepare_if_dev()
