# dbt runner image: dbt + adapters + the dbt project + its hub packages, all baked in.
# A KubernetesPodOperator runs this image on Composer's GKE and executes `dbt <args>`.
# Build from the REPO ROOT so the dbt project can be copied in:
#   docker build -f airflow/docker/Dockerfile -t <region>-docker.pkg.dev/<proj>/<repo>/dbt:<tag> .
FROM python:3.12-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/dbt \
    DBT_TARGET=prod

WORKDIR /dbt

# git is occasionally needed by `dbt deps` for git-sourced packages.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY airflow/docker/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Bake the dbt project in, then resolve + vendor the hub packages (dbt_utils, elementary) at build time.
COPY dbt/ /dbt/
RUN dbt deps

# Run as non-root.
RUN useradd --create-home --uid 1000 dbt && chown -R dbt:dbt /dbt
USER dbt

ENTRYPOINT ["dbt"]
CMD ["--version"]
