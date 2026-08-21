"""Local demo DAG: run the dbt project on DuckDB via DockerOperator (for the local Airflow UI profile).

Mirrors the production pattern locally. On Composer, ``KubernetesPodOperator`` launches the dbt image as a
pod on GKE; here ``DockerOperator`` launches the same image as a container on local Docker. Either way dbt
stays out of the Airflow image, since Airflow and dbt cannot co-install (protobuf 4 vs 5/6).

Prereqs (handled by ``docker-compose.yml`` plus a one-time seed — see the root README):
  * dbt image built:        ``docker build -f airflow/docker/Dockerfile -t gcp-dwh-dbt:local .``
  * DuckDB seeded on host:  ``make data``  (writes ``./data/dev.duckdb``)
  * ``HOST_PROJECT_DIR``=<abs repo path> and the docker socket mounted into the scheduler (compose does this)
"""

from __future__ import annotations

import os
import sys

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from alerts import slack_failure_callback  # noqa: E402

DBT_IMAGE = os.getenv("DBT_IMAGE", "gcp-dwh-dbt:local")
# Sibling containers launched via the docker socket see the HOST filesystem, so this must be a host path.
HOST_PROJECT_DIR = os.getenv("HOST_PROJECT_DIR", "")


def _dbt_task(task_id: str, dbt_args: list[str]) -> DockerOperator:
    """Run ``dbt <dbt_args>`` in the dbt image against the host's ./data DuckDB file."""
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=dbt_args,  # image ENTRYPOINT is ["dbt"], so these are the dbt subcommand + flags
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[Mount(source=f"{HOST_PROJECT_DIR}/data", target="/dbt/data", type="bind")],
        environment={"DBT_TARGET": "dev", "DUCKDB_PATH": "/dbt/data/dev.duckdb"},
    )


with DAG(
    dag_id="dbt_local_demo",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"on_failure_callback": slack_failure_callback},
    tags=["dbt", "docker", "duckdb", "local-demo"],
    doc_md=__doc__,
) as dag:
    source_freshness = _dbt_task("dbt_source_freshness", ["source", "freshness", "--target", "dev"])
    build = _dbt_task("dbt_build", ["build", "--target", "dev"])

    source_freshness >> build
