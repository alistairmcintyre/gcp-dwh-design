# gcp-dwh-design

A worked, runnable example of orchestrating **dbt** from **Airflow / Cloud Composer**, with **one dbt
project that runs locally on DuckDB and in production on BigQuery**.

It models **AppsFlyer-style mobile-attribution events** (installs, registrations by acquisition channel) and
**sports-betting activity** (bet volume, GGR, deposits, withdrawals) for a fictional operator, and runs the
production transforms on Cloud Composer via **KubernetesPodOperator** (dbt baked into a container image).

---

## What's inside

| Area | What you get |
|---|---|
| **dbt project** (`dbt/`) | staging → intermediate → incremental marts, dual-target (DuckDB `dev` / BigQuery `prod`), tests, docs with column descriptions, contracts, exposures, source freshness, Elementary |
| **Synthetic data** (`scripts/generate_test_data.py`) | deterministic AppsFlyer events + users + bets + transactions, written to a `raw` schema in DuckDB (or BigQuery) |
| **Orchestration** (`airflow/`) | dbt baked into a container image; Composer runs it via **KubernetesPodOperator** (`dbt_task` wrapper + DAGs) — nothing dbt-related installed on Composer |
| **Image deploy** (`.github/workflows/`) | build + push the dbt image to Artifact Registry — keyless **WIF/OIDC** (recommended) or legacy SA-key |
| **CI / quality** | GitHub Actions (SQLFluff + `dbt build` on DuckDB + DAG-integrity), pre-commit, SQLFluff |

### One project, two targets

```
              ┌──────────── dev (local) ────────────┐      ┌────────── prod (Composer) ──────────┐
generate ───▶ │ DuckDB raw schema ─▶ dbt ─▶ DuckDB   │      │ BigQuery raw ─▶ dbt ─▶ BigQuery      │
test data     │ (source AND destination)             │      │ (source AND destination)            │
              └──────────────────────────────────────┘      └─────────────────────────────────────┘
```

The same models run on both. Portability is handled with target-aware config: BigQuery uses
`insert_overwrite` + date `partition_by` (replaces only the touched partitions); DuckDB uses
`delete+insert` + `unique_key`. The incremental window is driven by the Airflow data interval, passed to dbt
as `start_date` / `end_date` vars, so any run (or re-run / backfill) of a date range is **idempotent**.

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

**Key definitions** (also documented as dbt doc blocks on the columns):
- **Bet volume** = `sum(stake)` of settled bets.
- **GGR (Gross Gaming Revenue)** = `sum(stake) − sum(payout)` — what the operator keeps.
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

dbt is baked into a container image in Artifact Registry; Composer launches it as pods — **nothing
dbt-related is installed on Composer** (only the `cncf.kubernetes` provider it already ships), so there's no
Airflow↔dbt dependency-conflict surface.

- `airflow/docker/Dockerfile` — the dbt image (dbt + adapters + project + hub packages baked in).
- `airflow/include/dbt_k8s.py` — `dbt_task(...)`, a thin KubernetesPodOperator wrapper.
- `airflow/dags/` — an **incremental** DAG (daily; `source freshness` → `build`; idempotent window from the
  run's data interval) and a **full-refresh / backfill** DAG (manual, with params).

Build/push the image with GitHub Actions — keyless **WIF/OIDC** (`deploy-dbt-image-wif.yml`, recommended) or
a legacy SA key (`deploy-dbt-image-sa-key.yml`). See [`airflow/README.md`](airflow/README.md) for the image,
the wrapper, Composer deployment, and **why WIF beats a service-account key**.

---

## Best-practice features included

- **Incremental + idempotent** marts driven by the run's data interval (re-runs/backfills are safe).
- **Tests**: generic (`unique`, `not_null`, `accepted_values`, `relationships`), `dbt_utils` range &
  expression tests, and singular SQL tests in `dbt/tests/`.
- **Docs**: a description on **every** column, plus reusable `{% docs %}` blocks for shared metrics.
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
airflow/docker/          dbt image (Dockerfile + requirements)
airflow/include/         KubernetesPodOperator wrapper (dbt_k8s.py)
airflow/dags/            Composer DAGs (incremental + full-refresh)
tests/                   DAG integrity / compile checks
.github/workflows/       CI + image deploy (WIF / SA-key)
```
