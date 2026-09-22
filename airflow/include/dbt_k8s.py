"""Reusable KubernetesPodOperator wrapper for running dbt commands on Cloud Composer.

dbt lives entirely in the container image (built from ``airflow/docker/Dockerfile`` and pushed to
Artifact Registry), so **nothing dbt-related is installed on Composer**: only the
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


def pod_task(
    *,
    task_id: str,
    arguments: list[str] | None = None,
    command: list[str] | None = None,
    name_prefix: str = "job",
    extra_env: dict[str, str] | None = None,
    cpu: str = "1",
    memory: str = "2Gi",
    **kwargs,
) -> KubernetesPodOperator:
    """A pod running the project image, with the same auth and cleanup rules as the dbt tasks.

    The dbt image already carries the project, so anything else the platform needs to run on a
    schedule (the erasure sweep, a backfill utility) runs here rather than on the scheduler. Nothing
    extra gets installed on Composer, and the workload keeps using Workload Identity for GCP auth.
    """
    resources = k8s.V1ResourceRequirements(
        requests={"cpu": "500m", "memory": "1Gi"},
        limits={"cpu": cpu, "memory": memory},
    )
    env = dict(_DBT_POD_ENV)
    env.update(extra_env or {})
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"{name_prefix}-" + task_id.replace("_", "-"),
        namespace=NAMESPACE,
        kubernetes_conn_id=KUBE_CONN_ID,
        image=DBT_IMAGE,
        cmds=command,
        arguments=arguments,
        env_vars=[k8s.V1EnvVar(name=k, value=v) for k, v in env.items()],
        service_account_name=SERVICE_ACCOUNT,
        container_resources=resources,
        get_logs=True,
        log_events_on_failure=True,
        on_finish_action="delete_pod",  # clean up the pod once it finishes
        reattach_on_restart=False,
        startup_timeout_seconds=600,
        **kwargs,
    )


# Lineage from dbt runs. On by default. Set DBT_LINEAGE=off where the OpenLineage provider isn't
# installed or is disabled: the provider registers the parent-run macros below only when it's
# enabled, and without them the task fails to render.
DBT_LINEAGE = os.getenv("DBT_LINEAGE", "on") != "off"
# Knowledge Catalog region for the lineage events. Set it: the transport's own default is
# us-central1, so leaving it out quietly files a European project's lineage in the US.
LINEAGE_LOCATION = os.getenv("LINEAGE_LOCATION", "europe-west2")

_OL = "macros.OpenLineageProviderPlugin"
# Who launched this dbt run, so its events hang off the Airflow task in the lineage graph: the DAG
# is the root, the task is the parent, and each model's run sits under the dbt run.
_OPENLINEAGE_CONTEXT = (
    '{"parent": {'
    f'"run": {{"runId": "{{{{ {_OL}.lineage_run_id(task_instance) }}}}"}}, '
    f'"job": {{"namespace": "{{{{ {_OL}.lineage_job_namespace() }}}}", '
    f'"name": "{{{{ {_OL}.lineage_job_name(task_instance) }}}}"}}, '
    '"root": {'
    f'"run": {{"runId": "{{{{ {_OL}.lineage_root_run_id(task_instance) }}}}"}}, '
    f'"job": {{"namespace": "{{{{ {_OL}.lineage_root_job_namespace(task_instance) }}}}", '
    f'"name": "{{{{ {_OL}.lineage_root_job_name(task_instance) }}}}"}}'
    '}}}'
)


def _lineage_env() -> dict[str, str]:
    return {
        "OPENLINEAGE__TRANSPORT__TYPE": "gcplineage",
        "OPENLINEAGE__TRANSPORT__PROJECT_ID": os.getenv("GCP_PROJECT", ""),
        "OPENLINEAGE__TRANSPORT__LOCATION": LINEAGE_LOCATION,
        "OPENLINEAGE_NAMESPACE": os.getenv("OPENLINEAGE_NAMESPACE", "dbt"),
        "OPENLINEAGE_CONTEXT": _OPENLINEAGE_CONTEXT,
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

    With lineage on, the pod runs ``dbt-ol`` instead of ``dbt``. It runs dbt unchanged, then reads
    the manifest and run results and reports every model's inputs, outputs and column lineage.
    BigQuery would record table lineage for these jobs anyway; dbt-ol adds which dbt model and
    which Airflow task each one belongs to.
    """
    return pod_task(
        task_id=task_id,
        command=["dbt-ol"] if DBT_LINEAGE else None,
        arguments=dbt_args,
        name_prefix="dbt",
        extra_env=_lineage_env() if DBT_LINEAGE else None,
        cpu=cpu,
        memory=memory,
        **kwargs,
    )
