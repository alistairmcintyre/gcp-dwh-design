"""Reusable KubernetesPodOperator wrapper for running dbt commands on Cloud Composer.

dbt lives entirely in the container image (built from ``airflow/docker/Dockerfile`` and pushed to
Artifact Registry), so **nothing dbt-related is installed on Composer** — only the
``apache-airflow-providers-cncf-kubernetes`` provider, which Composer already ships. Each call to
``dbt_task`` launches a pod that runs ``dbt <args>`` against BigQuery.

Configuration is via Composer **environment variables** (Composer → Environment variables):

    DBT_IMAGE                 full image ref, e.g. europe-west2-docker.pkg.dev/<proj>/<repo>/dbt:<tag>
    GCP_PROJECT, BQ_DATASET, BQ_LOCATION, BQ_THREADS   passed to the dbt `prod` profile
    DBT_K8S_NAMESPACE         pod namespace (Composer 3 default: composer-user-workloads)
    DBT_K8S_SERVICE_ACCOUNT   Kubernetes SA bound to a GCP SA via Workload Identity (BigQuery auth)
    DBT_K8S_CONN_ID           Airflow connection for the cluster (Composer default: kubernetes_default)
"""

from __future__ import annotations

import os

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Placeholder default so DAGs still parse if the env var isn't set yet (e.g. local DAG-integrity tests).
DBT_IMAGE = os.getenv("DBT_IMAGE", "DBT_IMAGE_NOT_SET")
NAMESPACE = os.getenv("DBT_K8S_NAMESPACE", "composer-user-workloads")
SERVICE_ACCOUNT = os.getenv("DBT_K8S_SERVICE_ACCOUNT")  # None -> pod uses the namespace default SA
KUBE_CONN_ID = os.getenv("DBT_K8S_CONN_ID", "kubernetes_default")

# Env passed into every dbt pod; consumed by profiles.yml `prod` target (method: oauth / ADC).
_DBT_POD_ENV = {
    "GCP_PROJECT": os.getenv("GCP_PROJECT", ""),
    "BQ_DATASET": os.getenv("BQ_DATASET", "analytics"),
    "BQ_LOCATION": os.getenv("BQ_LOCATION", "EU"),
    "BQ_THREADS": os.getenv("BQ_THREADS", "4"),
    "DBT_TARGET": os.getenv("DBT_TARGET", "prod"),
}


def dbt_task(
    *,
    task_id: str,
    dbt_args: list[str],
    cpu: str = "1",
    memory: str = "2Gi",
    **kwargs,
) -> KubernetesPodOperator:
    """Build a KubernetesPodOperator that runs ``dbt <dbt_args>`` in the dbt image.

    ``dbt_args`` is an Airflow-templated field, so you can pass ``--vars`` with ``{{ data_interval_* }}``.
    """
    resources = k8s.V1ResourceRequirements(
        requests={"cpu": "500m", "memory": "1Gi"},
        limits={"cpu": cpu, "memory": memory},
    )
    return KubernetesPodOperator(
        task_id=task_id,
        name="dbt-" + task_id.replace("_", "-"),
        namespace=NAMESPACE,
        kubernetes_conn_id=KUBE_CONN_ID,
        image=DBT_IMAGE,
        arguments=dbt_args,
        env_vars=[k8s.V1EnvVar(name=k, value=v) for k, v in _DBT_POD_ENV.items()],
        service_account_name=SERVICE_ACCOUNT,
        container_resources=resources,
        get_logs=True,
        log_events_on_failure=True,
        on_finish_action="delete_pod",  # clean up the pod once it finishes
        reattach_on_restart=False,
        startup_timeout_seconds=600,
        **kwargs,
    )
