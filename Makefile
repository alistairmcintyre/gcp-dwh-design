.DEFAULT_GOAL := help
SHELL := /bin/bash

DBT := uv run dbt
DBT_DIRS := --project-dir dbt --profiles-dir dbt
TARGET ?= dev

# Load .env if present so DUCKDB_PATH / GCP vars are available.
ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: help install data deps build run test seed docs freshness lint fix dag-test verify clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Create the uv venv (Python 3.12) and install the dev dependency group
	uv sync

data: ## Generate synthetic raw data into the local DuckDB file
	uv run python scripts/generate_test_data.py --target duckdb

deps: ## Install dbt packages (dbt_utils, dbt_expectations, elementary, codegen)
	$(DBT) deps $(DBT_DIRS)

build: ## Run + test everything (dbt build) against $(TARGET)
	$(DBT) build $(DBT_DIRS) --target $(TARGET)

run: ## Run models only against $(TARGET)
	$(DBT) run $(DBT_DIRS) --target $(TARGET)

test: ## Run dbt tests against $(TARGET)
	$(DBT) test $(DBT_DIRS) --target $(TARGET)

seed: ## Load CSV seeds against $(TARGET)
	$(DBT) seed $(DBT_DIRS) --target $(TARGET)

freshness: ## Check source freshness against $(TARGET)
	$(DBT) source freshness $(DBT_DIRS) --target $(TARGET)

docs: ## Generate and serve dbt docs (Ctrl-C to stop)
	$(DBT) docs generate $(DBT_DIRS) --target $(TARGET)
	$(DBT) docs serve $(DBT_DIRS)

lint: ## Lint SQL models with SQLFluff
	uv run sqlfluff lint dbt/models

fix: ## Auto-fix SQL models with SQLFluff
	uv run sqlfluff fix dbt/models

dag-test: ## Parse/AST-check every DAG file (no Airflow install needed)
	uv run python tests/check_dags_compile.py

verify: deps data build freshness lint ## Full local verification pipeline (DuckDB)
	@echo "Local verification complete."

clean: ## Remove the local DuckDB file and dbt artefacts
	rm -f $(DUCKDB_PATH) data/*.duckdb data/*.duckdb.wal
	rm -rf dbt/target dbt/dbt_packages dbt/logs
