# gcp-dwh-design

A worked, runnable example of orchestrating dbt two ways, from **Airflow / Cloud Composer** and from
**Dagster**, over one dbt project that runs locally on DuckDB and in production on BigQuery.

It models AppsFlyer-style mobile-attribution events (installs, registrations by acquisition channel) and
sports-betting activity (bet volume, GGR, deposits, withdrawals) for a fictional operator. Production
transforms run on Cloud Composer via KubernetesPodOperator (dbt baked into a container image), or on
Dagster as software-defined assets with native dbt lineage.

Two local UIs, as separate Docker Compose profiles (run one at a time):

```bash
docker compose --profile dagster up --build     # Dagster UI  -> http://localhost:3000
docker compose --profile airflow up --build      # Airflow 3 UI -> http://localhost:8080 (admin/admin)
```

---

## What's inside

| Area | Contents |
|---|---|
| **dbt project** (`dbt/`) | staging → intermediate → incremental marts, dual-target (DuckDB `dev` / BigQuery `prod`), tests, docs with column descriptions, contracts, exposures, source freshness, Elementary |
| **Synthetic data** (`scripts/generate_test_data.py`) | deterministic AppsFlyer events + users + bets + transactions, written to a `raw` schema in DuckDB (or BigQuery) |
| **Orchestration — Airflow** (`airflow/`) | dbt baked into a container image; Composer runs it via KubernetesPodOperator (`dbt_task` wrapper + DAGs), with nothing dbt-related installed on Composer. Slack alert on failure. |
| **Orchestration — Dagster** (`dagster/`) | the same dbt project as software-defined assets: one asset per model with native lineage, dbt tests as asset checks, daily partitions/backfills, schedules, run-failure alerting. See [`dagster/README.md`](dagster/README.md). |
| **Local UIs** (`docker-compose.yml`) | two Docker Compose profiles, `dagster` and `airflow`, to view each UI locally against DuckDB. Run one at a time. |
| **Image deploy** (`.github/workflows/`) | build + push the dbt image to Artifact Registry: keyless WIF/OIDC (recommended) or legacy SA-key |
| **CI / quality** | GitHub Actions (SQLFluff + `dbt build` on DuckDB + DAG-integrity on Airflow 3), pre-commit, SQLFluff |

### One project, two targets

```
              ┌──────────── dev (local) ────────────┐      ┌────────── prod (Composer) ──────────┐
generate ───▶ │ DuckDB raw schema ─▶ dbt ─▶ DuckDB   │      │ BigQuery raw ─▶ dbt ─▶ BigQuery      │
test data     │ (source AND destination)             │      │ (source AND destination)            │
              └──────────────────────────────────────┘      └─────────────────────────────────────┘
```

The same models run on both. Portability is handled with target-aware config: BigQuery uses
`insert_overwrite` + date `partition_by` (replacing only the touched partitions); DuckDB uses
`delete+insert` + `unique_key`. The incremental window comes from the orchestrator's run interval, passed to
dbt as `start_date` / `end_date` vars, so any run (or re-run / backfill) of a date range is idempotent.

---

## Data model

```
sources (raw.*)                staging (views)              marts (incremental tables)
─────────────────────          ───────────────────          ──────────────────────────────
raw_appsflyer_events  ─▶  stg_appsflyer_events  ─┐
raw_users             ─▶  stg_users             ─┼─▶ fct_acquisition_events
raw_bets              ─▶  stg_bets              ─┤      (event_date × channel × platform:
raw_transactions      ─▶  stg_transactions      ─┘       installs, registrations, conv. rate)
                                                  └─▶ int_user_daily_activity ─▶ fct_user_activity
                                                          (activity_date × user × channel:
                                                           bet_count, bet_volume, GGR,
                                                           deposits, withdrawals, net_deposit)
```

Key definitions (also documented as dbt doc blocks on the columns):
- **Bet volume** = `sum(stake)` of settled bets.
- **GGR (Gross Gaming Revenue)** = `sum(stake) − sum(payout)`, i.e. what the operator keeps.
- **Acquisition channel** = human-friendly grouping of AppsFlyer `media_source` (e.g. `facebook_ads` →
  "Facebook Ads", `google_search_ads` → "Google Search Ads"), via the `dim_channel_grouping` seed.

---

## Quickstart (local, DuckDB)

Prereqs: [`uv`](https://docs.astral.sh/uv/) (Python is managed for you; this repo pins **3.12** because dbt
does not yet support 3.14).

```bash
uv sync                       # create venv + install the dev group
cp .env.example .env          # optional; defaults work out of the box
make deps                     # dbt deps (dbt_utils, dbt_expectations, elementary, codegen)
make data                     # generate synthetic raw data into data/dev.duckdb
make build                    # dbt build: seed + run + test everything on DuckDB
make docs                     # generate + serve dbt docs at http://localhost:8080
```

Or run the whole pipeline + lint in one go: `make verify`. (No `make`? Every target is a thin wrapper —
run the `uv run …` commands above directly.)

Inspect results:
```bash
uv run python -c "import duckdb; con=duckdb.connect('data/dev.duckdb'); \
  print(con.sql('select * from marts.fct_user_activity limit 5'))"
```

---

## Orchestration (KubernetesPodOperator)

dbt is baked into a container image in Artifact Registry; Composer launches it as pods. Nothing dbt-related
is installed on Composer (only the `cncf.kubernetes` provider it already ships), so there is no Airflow↔dbt
dependency-conflict surface.

- `airflow/docker/Dockerfile` — the dbt image (dbt + adapters + project + hub packages baked in).
- `airflow/include/dbt_k8s.py` — `dbt_task(...)`, a thin KubernetesPodOperator wrapper.
- `airflow/dags/` — an incremental DAG (daily; `source freshness` → `build`; idempotent window from the
  run's data interval) and a full-refresh / backfill DAG (manual, with params).

Build/push the image with GitHub Actions: keyless WIF/OIDC (`deploy-dbt-image-wif.yml`, recommended) or a
legacy SA key (`deploy-dbt-image-sa-key.yml`). See [`airflow/README.md`](airflow/README.md) for the image,
the wrapper, Composer deployment, and the WIF vs service-account-key comparison.

---

## Orchestration, two ways: Airflow vs Dagster

Both orchestrate the same dbt project. dbt is the portable core; the orchestrator is a swappable layer.
Neither installs dbt in-process (Airflow and dbt cannot co-resolve: protobuf 4 vs 5/6), so both run dbt in
its own container: `KubernetesPodOperator` (a pod on Composer's GKE) or, locally, `DockerOperator` / the
Dagster code-location image.

| | **Airflow** (`airflow/`) | **Dagster** (`dagster/`) |
|---|---|---|
| Unit of work | tasks; the whole `dbt build` is one task | one asset per dbt model (+ one check per dbt test) |
| dbt lineage | in dbt docs only, separately | native: the asset graph is the dbt DAG, in the UI |
| dbt tests | pass/fail inside the task log | asset checks: green/red per model, per run |
| Backfills | re-triggered runs over `data_interval` | partition grid: select a date range in the UI |
| Ingestion→dbt | separate DAGs/systems | dev `raw_data` asset shares the dbt source keys → one graph |
| Alerting | `on_failure_callback` → Slack | run-failure sensors (Slack/email) + red checks |
| Best for | managed GCP runtime, many-system DAGs | dbt-centric warehouse, lineage/observability |

Detail on the Dagster side and its design choices: [`dagster/README.md`](dagster/README.md).

## View the UIs locally (Docker Compose profiles)

Two profiles in `docker-compose.yml`, each self-contained (own Postgres + storage). Run one at a time.

```bash
cp .env.example .env                 # set HOST_PROJECT_DIR (abs repo path) + DOCKER_GID for the Airflow profile
make data                            # seed ./data/dev.duckdb (shared by both UIs)

docker compose --profile dagster up --build     # Dagster UI -> http://localhost:3000
#   Assets → "Materialize all": raw_data → dbt models → dbt tests as checks, full lineage graph.

docker compose --profile airflow up --build      # Airflow 3 UI -> http://localhost:8080 (admin/admin)
#   Trigger `dbt_local_demo`: DockerOperator runs the dbt image on DuckDB (mirrors KPO-on-Composer).

docker compose --profile dagster down -v         # stop + wipe that profile
```

- **Dagster** runs fully locally against DuckDB, with lineage, checks and partitions in the UI. See
  [`dagster/README.md`](dagster/README.md).
- **Airflow 3** (api-server + scheduler + dag-processor) shows the production KPO DAGs (parse-only locally,
  since they target Composer/GKE) plus a runnable `dbt_local_demo` DAG that executes dbt on DuckDB via
  `DockerOperator`. Local auth is `SimpleAuthManager` (`admin`/`admin`).

---

## Features

- **Incremental + idempotent** marts driven by the run's data interval (re-runs/backfills are safe).
- **Tests**: generic (`unique`, `not_null`, `accepted_values`, `relationships`), `dbt_utils` range &
  expression tests, and singular SQL tests in `dbt/tests/`.
- **Docs**: a description on every column, plus reusable `{% docs %}` blocks for shared metrics.
- **Contracts** (`contract: {enforced: true}`) with typed columns on the marts.
- **Exposures** documenting the downstream BI dashboards.
- **Source freshness** thresholds + `dbt source freshness`.
- **Elementary** package for run-results / data-observability.
- **SQLFluff** (dbt templater) + **pre-commit** + **GitHub Actions CI** (lint, `dbt build` on DuckDB,
  DAG-integrity).
- Env-aware schema naming (`generate_schema_name`), `persist_docs` to BigQuery, layered folder configs.

---

## Make targets

```
make install   # uv sync (dev group)
make deps      # dbt deps
make data      # generate synthetic raw data into DuckDB
make build     # dbt build (TARGET=dev by default; TARGET=prod for BigQuery)
make test      # dbt test
make freshness # dbt source freshness
make docs      # dbt docs generate + serve
make lint/fix  # SQLFluff
make dag-test  # AST-compile every DAG file (no Airflow needed)
make verify    # deps + data + build + freshness + lint
make clean     # remove the DuckDB file + dbt artefacts
```

## Layout

```
dbt/                     the dbt project (models, seeds, macros, tests)
scripts/                 synthetic data generator
airflow/docker/          dbt image (Dockerfile) + local Airflow 3 image (Dockerfile.local)
airflow/include/         KubernetesPodOperator wrapper (dbt_k8s.py) + Slack alert callback (alerts.py)
airflow/dags/            Composer DAGs (incremental + full-refresh) + local demo (DockerOperator)
dagster/                 Dagster code location (assets, checks, partitions, jobs, schedules, sensors)
docker-compose.yml       `dagster` and `airflow` profiles for viewing each UI locally
tests/                   DAG integrity / compile checks
.github/workflows/       CI + image deploy (WIF / SA-key)
```
