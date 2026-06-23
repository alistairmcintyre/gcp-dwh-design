# Orchestrating dbt on Cloud Composer with KubernetesPodOperator

One approach: dbt lives entirely in a **container image** (Artifact Registry); Airflow just launches pods.
**Nothing dbt-related is installed on Composer** — only the `cncf.kubernetes` provider it already ships —
so there's zero Airflow↔dbt dependency-conflict surface.

```
 GitHub Actions ──build──▶ Artifact Registry          Cloud Composer (Airflow 2.10)
   (dbt image)             <region>-docker.pkg.dev      DAG ─▶ dbt_task() ─▶ KubernetesPodOperator
        │                  /<proj>/<repo>/dbt:<sha>                              │ launches pod
        └──────────────────────────────────────────────────────────────────────┘
                                                          pod runs `dbt build --target prod` ─▶ BigQuery
```

| Piece | Path |
|---|---|
| dbt image (dbt + adapters + project + hub packages baked in) | `airflow/docker/Dockerfile` (+ `requirements.txt`) |
| KubernetesPodOperator wrapper | `airflow/include/dbt_k8s.py` (`dbt_task(...)`) |
| DAGs | `airflow/dags/dbt_incremental_dag.py`, `dbt_full_refresh_dag.py` |
| Image build + push (GitHub Actions) | `.github/workflows/deploy-dbt-image-wif.yml` (keyless) / `…-sa-key.yml` (legacy) |

The incremental DAG runs `dbt source freshness` then `dbt build`, passing the run's data interval as
`start_date`/`end_date` vars (idempotent per partition). The full-refresh DAG is manual with
`full_refresh` + backfill window params, driving the marts' `force_full_refresh` var.

## Build & push the image

Two workflows; both build `airflow/docker/Dockerfile` from the repo root and push to Artifact Registry.

- **`deploy-dbt-image-wif.yml` (recommended, keyless)** — GitHub OIDC + Workload Identity Federation.
- **`deploy-dbt-image-sa-key.yml` (legacy)** — long-lived SA JSON key in a GitHub secret.

Set `REGION` / `PROJECT_ID` / `REPO` in the workflow `env:`. The WIF workflow needs two **non-secret**
repo variables, `WIF_PROVIDER` and `WIF_SERVICE_ACCOUNT` (one-time GCP setup is in the workflow header).

### Why WIF (keyless) beats a service-account JSON key

A SA key is a **permanent credential**. WIF gives each workflow run a **short-lived, scoped** identity instead:

- **No standing secret to leak.** A JSON key works forever until someone notices and rotates it; if it
  lands in a log, a fork, or a compromised runner, that's a long-lived breach. WIF mints a fresh GitHub
  OIDC token per run, exchanged (via GCP STS) for an access token that **expires in ~1 hour**. Nothing
  durable exists to steal.
- **Nothing sensitive stored in GitHub.** Keys live as a GitHub secret (secret sprawl, fork/admin
  exposure). WIF stores only the provider resource name + SA email — non-secret identifiers.
- **Least-privilege trust.** The provider's **attribute condition** restricts *which* repo/branch/
  environment may impersonate the SA (e.g. only `your-org/your-repo`). A stolen key has no such scoping.
- **No rotation, easy revocation.** Nothing to rotate; revoke by removing one IAM binding.
- **Auditable.** Each token exchange is logged and attributable to a specific GitHub identity.
- **Policy-friendly.** Many orgs enforce `iam.disableServiceAccountKeyCreation`, so keys aren't even an
  option. Google's own guidance is: prefer Workload Identity Federation; SA keys are a last resort.

> Note: this is **deploy-time** auth (GitHub → Artifact Registry). It's separate from **run-time** auth
> (the pod → BigQuery), covered below — both avoid keys.

## Deploy to Cloud Composer

1. **Provider** — already on Composer; nothing to add to PyPI packages.
2. **Deploy the DAGs + wrapper** to the environment's GCS bucket:
   ```
   gsutil -m rsync -r -d airflow/dags     gs://<composer-bucket>/dags
   gsutil -m rsync -r -d airflow/include  gs://<composer-bucket>/dags/include
   ```
   (`include/` under `dags/` keeps `dbt_k8s.py` importable; the DAGs add it to `sys.path`.)
3. **Environment variables** (Composer → Environment variables):
   ```
   DBT_IMAGE=<region>-docker.pkg.dev/<proj>/<repo>/dbt:<sha>
   GCP_PROJECT=<proj>
   BQ_DATASET=analytics
   BQ_LOCATION=EU
   DBT_K8S_NAMESPACE=composer-user-workloads      # Composer 3 user-workload namespace
   DBT_K8S_SERVICE_ACCOUNT=<ksa-bound-to-a-bq-gsa>
   ```
4. **Run-time auth (pod → BigQuery), also keyless:** bind the pod's Kubernetes SA to a Google SA with
   `roles/bigquery.jobUser` + `roles/bigquery.dataEditor` via **Workload Identity**; dbt's `prod` profile
   stays `method: oauth` (ADC) and picks it up. Set that KSA as `DBT_K8S_SERVICE_ACCOUNT`.
5. **Pin the image** by digest/sha in `DBT_IMAGE` for reproducible runs; bump it when the deploy workflow
   pushes a new build.

> Targeting a **separate** GKE cluster instead of Composer's? Swap `KubernetesPodOperator` for
> `GKEStartPodOperator` in `dbt_k8s.py` (same arguments + a cluster/location).

## Test the image locally (optional)

```bash
docker build -f airflow/docker/Dockerfile -t dbt:local .
docker run --rm dbt:local --version          # dbt + adapters present
docker run --rm dbt:local ls --target prod   # parses the baked project (no warehouse needed)
```
