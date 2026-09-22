# Airflow (Cloud Composer)

Runs the dbt project on Composer. dbt lives in a container image and Airflow only launches pods, so
nothing dbt-related is installed on Composer.

| Path | What it is |
|---|---|
| `docker/Dockerfile` | the dbt image: dbt, the adapters, this project and its packages |
| `include/dbt_k8s.py` | `pod_task` and `dbt_task`, thin KubernetesPodOperator wrappers |
| `dags/` | incremental (daily), full refresh and backfill (manual), GDPR erasure (daily), local demo |

**Run it locally:** `docker compose --profile airflow up --build`, then open http://localhost:8080
(admin/admin) and trigger `dbt_local_demo`. The Composer DAGs parse locally but need GKE to run.

**Deploy:** sync `dags/` and `include/` to the Composer bucket, and set `DBT_IMAGE` (pinned by
digest), `GCP_PROJECT`, `BQ_DATASET`, `BQ_LOCATION`, `DBT_K8S_NAMESPACE` and
`DBT_K8S_SERVICE_ACCOUNT` as environment variables. The image is built by
`.github/workflows/deploy-dbt-image-wif.yml`.

Why it's built this way, and how it compares with Dagster:
[decision guide, section 5](../docs/decision-guide.md#5-orchestration).
