# Dagster

The same dbt project as the Airflow side, modelled as software-defined assets: one asset per dbt
model, each dbt test as an asset check, daily partitions for the incremental window.

| Path | What it is |
|---|---|
| `dwh_dagster/dbt_assets.py` | the dbt project as a partitioned asset graph |
| `dwh_dagster/jobs.py`, `schedules.py` | incremental, full refresh and source freshness, mirroring the Airflow DAGs |
| `dwh_dagster/privacy_jobs.py` | the daily GDPR erasure sweep, as ops |
| `dwh_dagster/gates.py`, `sensors.py` | readiness gate and run-failure alerts (all optional) |
| `dwh_dagster/raw_data.py` | dev-only asset that generates the raw data, so one graph runs end to end |

**Run it:** `docker compose --profile dagster up --build`, then open http://localhost:3000 and
click *Materialize all*. Without Docker: `uv run --with-requirements dagster/requirements.txt
dagster dev -w dagster/workspace.yaml` from the repo root.

**Alerts** are off unless you set `SLACK_WEBHOOK_URL`, `DAGSTER_SLACK_BOT_TOKEN` or the
`DAGSTER_SMTP_*` variables.

Why it's built this way, and when to pick it over Airflow:
[decision guide, section 5](../docs/decision-guide.md#5-orchestration).
