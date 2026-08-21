# Orchestrating dbt with Dagster (software-defined assets)

The Dagster counterpart to the Airflow/Composer orchestration in [`../airflow/`](../airflow). It runs the
same dbt project (`../dbt`, DuckDB locally / BigQuery in prod), but models it as a graph of
software-defined assets rather than a sequence of tasks: native dbt lineage, dbt tests as asset checks,
partitioned backfills, and a local UI to inspect it in.

```
scripts/generate_test_data.py         dbt (../dbt)                         BI
  ─────────────────────────           ──────────────                       ──────────
  raw_data asset (dev only)  ──▶  source: raw.* ──▶ staging ──▶ marts ──▶ exposures
  emits raw/<table> keys          (1 Dagster asset per dbt model, + 1 asset check per dbt test)
```

---

## Differences from the Airflow approach

In the Airflow DAGs the entire dbt run is one task (`KubernetesPodOperator` → `dbt build`). If model #30
fails, the task is red and you read pod logs. Dagster models the same run as an asset graph:

| Capability | Airflow (this repo) | Dagster (this folder) |
|---|---|---|
| **dbt lineage** | One `dbt build` task; lineage only in dbt docs, separately. | One asset per dbt model, `ref()`/`source()` edges rebuilt from the manifest. The asset graph is the dbt DAG, in the UI. |
| **dbt tests** | Pass/fail inside the task log. | Every dbt test is an asset check on the model it guards: green/red per model, per run. |
| **Scheduling** | Time/task-based (a task "succeeds"). | Asset-based: the UI tracks freshness/materialisation per asset, and you can target "rebuild this model and everything downstream". |
| **Partitions & backfills** | `data_interval` per run; backfills are re-triggered runs. | A partition grid per asset; select a date range and Dagster launches one run per partition, tracking which are filled/missing/failed. |
| **Ingestion → transform lineage** | Separate DAGs / systems. | The dev `raw_data` asset emits the same keys as the dbt sources, so ingestion and dbt are one connected graph. |
| **Local dev loop** | Needs Airflow running to see anything. | `dagster dev` renders the whole graph, and editing a model live-updates it. |
| **Alerting** | `on_failure_callback` (added here). | Run-failure sensors (Slack webhook / Slack bot / email) plus red asset checks; Dagster+ adds declarative alert policies. |

Airflow keeps its own strengths here: a large provider ecosystem, arbitrary cross-system DAGs, and the
managed Composer runtime. See [§ When to pick which](#when-to-pick-which).

---

## Design choices

1. **`@dbt_assets` off the manifest** (`dwh_dagster/dbt_assets.py`). One function turns the dbt project
   into the asset graph and streams dbt's structured events back as materialisations and check results.
   This is the current dagster-dbt pattern, not the deprecated per-model factory.
2. **dbt tests as asset checks** (`translator.py`, `enable_asset_checks=True`). A failing error-severity
   test fails the run (so alerts fire) and shows as a red check on the model. Warn-severity records a WARN
   check without failing, matching dbt's semantics.
3. **Daily partitions drive the incremental window** (`partitions.py`, `dbt_assets.py`). Materialising the
   `2026-06-15` partition runs `dbt build --vars '{"start_date":"2026-06-15","end_date":"2026-06-15"}'`, so
   the incremental marts rebuild that day only — `insert_overwrite` on BigQuery, `delete+insert` on DuckDB.
   Re-runs are idempotent and ranges can be backfilled from the UI.
4. **`DbtProject` with `prepare_if_dev()`** (`project.py`). Locally the manifest is regenerated on code
   reload; in the image it is baked at build time so the container never shells out to dbt to load
   definitions.
5. **Assets grouped by dbt layer** (`staging` / `intermediate` / `marts` / `raw_ingestion`).
6. **Dev-only ingestion asset** (`raw_data.py`). Loaded only when `DBT_TARGET=dev`; runs the repo's
   synthetic generator into DuckDB and emits the `raw/<table>` keys the dbt sources resolve to, so one
   materialisation covers raw data through to marts. In prod, ingestion is real (Datastream/Fivetran/
   Pub-Sub) and the sources appear as external upstream assets.
7. **Jobs and schedules mirror the Airflow DAGs** (`jobs.py`, `schedules.py`): a daily incremental
   schedule, a manual full-refresh/backfill job (same `force_full_refresh` + window config), and a
   `dbt source freshness` job/schedule.
8. **Alerting is env-gated** (`sensors.py`), so the demo runs with nothing configured.
9. **dbt stays in the code location's own environment**, never co-installed with Airflow (they cannot
   resolve together: protobuf 4 vs 5/6). Same discipline as the Composer image.

---

## Run the UI locally (Docker)

From the repo root (needs Docker; copy `.env.example` → `.env` first):

```bash
docker compose --profile dagster up --build       # Dagster UI -> http://localhost:3000
```

Then in the UI:

1. **Assets → View global asset lineage** — the full graph: `raw/*` → `staging` → `marts` → exposures.
2. Click **Materialize all**. The `raw_data` asset generates DuckDB data, then every dbt model builds and
   every dbt test runs as an asset check. The run page shows per-model timing and per-check pass/fail.
3. Open **`marts/fct_user_activity`** → its **Checks** tab: `not_null`, `relationships`, `dbt_utils`
   range/expression tests, and the singular `assert_user_activity_ggr_consistent`.
4. **Assets → fct_user_activity → Materialize → pick a partition** for a partitioned incremental run, or
   **Backfill** a date range.
5. **Automation** tab — `daily_incremental_schedule` / `source_freshness_schedule`; **Sensors** —
   `slack_webhook_on_run_failure`.

Stop and wipe: `docker compose --profile dagster down -v`.

> The Airflow UI is a separate profile — `docker compose --profile airflow up --build` (→
> http://localhost:8080, `admin`/`admin`). They are not run at the same time; see the root README.

### Run it without Docker (dev loop)

```bash
uv run --with-requirements dagster/requirements.txt \
  dagster dev -w dagster/workspace.yaml           # from repo root; UI on http://localhost:3000
```

(`prepare_if_dev()` builds the manifest on load. `DBT_TARGET=dev` by default → DuckDB.)

---

## Alerting

Wired in `dwh_dagster/sensors.py`. All sinks are optional and no-op if unconfigured:

- **`SLACK_WEBHOOK_URL`** — always-on run-failure sensor, stdlib only. Posts on any run failure, including
  a dbt error-severity test failing the build.
- **`DAGSTER_SLACK_BOT_TOKEN`** (+ `DAGSTER_SLACK_CHANNEL`) — adds the `dagster-slack` bot sensor.
- **SMTP** (`DAGSTER_SMTP_HOST`, `DAGSTER_ALERT_EMAIL_FROM/PASSWORD/TO`) — adds an email failure sensor.

How dbt test failures reach the alert: an error-severity test makes `dbt build` exit non-zero, the asset
step fails, the run fails, and the failure sensors fire. The failed test is also a red asset check on the
model. Warn-severity tests record a WARN check without failing the run; to alert on warns, use a Dagster+
alert policy or an asset-check sensor.

To see an alert fire, point `SLACK_WEBHOOK_URL` at a test webhook and break a test (e.g. loosen the data so
`assert_user_activity_ggr_consistent` fails), then materialize.

---

## Production (BigQuery) notes

- Set `DBT_TARGET=prod` + `GCP_PROJECT` / `BQ_DATASET` / `BQ_LOCATION` (the same env the dbt `prod` profile
  and the Composer image use). The dev `raw_data` asset drops out; the dbt sources become external assets
  fed by real ingestion.
- **Auth stays keyless**: run the code location on GKE with Workload Identity (the dbt `prod` profile is
  `method: oauth` / ADC), as with the pod in the Airflow flow. No JSON keys.
- **Deploy shapes**: self-host on GKE (Helm) or use Dagster+ (managed control plane, alert policies, asset
  catalog, insights). Build the image with `dagster/Dockerfile` (manifest baked at build time).
- BigQuery cost and performance are unchanged: same dbt models, partitioning, clustering, and
  `insert_overwrite`.

---

## When to pick which

- **Airflow/Composer** if you need a managed GCP runtime, a broad provider ecosystem, or DAGs that
  orchestrate many systems beyond dbt. (This repo's KPO pattern keeps dbt off the scheduler.)
- **Dagster** if the warehouse is dbt-centric and you want native asset lineage, dbt tests as first-class
  checks, partition/backfill UX, and data-aware observability without additional tooling.

Both are here to compare on the identical dbt project. dbt is the portable core; the orchestrator is a
swappable layer on top.

---

## Layout

```
dagster/
  dwh_dagster/
    project.py       DbtProject (manifest: prepare_if_dev locally / baked in image) + path/target env
    partitions.py    DailyPartitionsDefinition (the incremental window)
    translator.py    dbt tests -> asset checks; group models by layer
    dbt_assets.py    @dbt_assets — the dbt project as a partitioned asset graph
    raw_data.py      dev-only ingestion asset (runs the generator; emits raw/<table> keys)
    jobs.py          incremental / full-refresh / source-freshness jobs (mirror the Airflow DAGs)
    schedules.py     daily incremental + freshness schedules
    sensors.py       run-failure alerting (Slack webhook / Slack bot / email), env-gated
    definitions.py   the code location (assets + jobs + schedules + sensors + dbt resource)
  Dockerfile         webserver+daemon image; bakes the dbt manifest
  dagster.yaml       instance config (Postgres storage, queued run coordinator)
  workspace.yaml     how the webserver/daemon load the code location
  requirements.txt   dagster + dagster-dbt + dbt adapters (own env; never with Airflow)
  pyproject.toml     [tool.dagster] module + code location name
```
