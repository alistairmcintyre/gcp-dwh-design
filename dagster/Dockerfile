# Dagster image: webserver + daemon + the code location (dbt project, dagster code, data generator),
# with the dbt manifest baked at build time. Build from the REPO ROOT so dbt/ and scripts/ are in context:
#   docker build -f dagster/Dockerfile -t gcp-dwh-dagster:local .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    DBT_TARGET=dev \
    DBT_PROJECT_DIR=/opt/dagster/app/dbt \
    DBT_PROFILES_DIR=/opt/dagster/app/dbt \
    DUCKDB_PATH=/opt/dagster/data/dev.duckdb \
    PYTHONPATH=/opt/dagster/app/dagster

# git: dbt deps may fetch git-sourced packages.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

COPY dagster/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Code location: the dbt project, the Dagster package, and the data generator the dev raw asset shells to.
COPY dbt/      /opt/dagster/app/dbt/
COPY scripts/  /opt/dagster/app/scripts/
COPY dagster/  /opt/dagster/app/dagster/

# Vendor dbt hub packages and bake the manifest so @dbt_assets loads without running `dagster dev`.
RUN cd /opt/dagster/app/dbt && dbt deps && dbt parse --target dev

# Instance config + writable homes (DuckDB lives under /opt/dagster/data, a compose volume).
RUN mkdir -p "$DAGSTER_HOME" /opt/dagster/data
COPY dagster/dagster.yaml "$DAGSTER_HOME/dagster.yaml"

# Run from the code-location dir (workspace.yaml + pyproject [tool.dagster] live here).
WORKDIR /opt/dagster/app/dagster

# Default to the webserver; the daemon service overrides this command in docker-compose.
EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
