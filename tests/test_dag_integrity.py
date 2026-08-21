"""Import every DAG in a folder via Airflow's DagBag and assert there are no import/cycle errors.

Requires Apache Airflow and the providers the DAGs import, so it is skipped automatically in the local
`dev` venv (no Airflow). In CI, `DAG_INTEGRITY_DAG_DIR` points at the DAG folder to check.
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("airflow", reason="Airflow not installed (local dev venv)")

DAG_DIR = os.environ.get("DAG_INTEGRITY_DAG_DIR")


@pytest.mark.skipif(not DAG_DIR, reason="set DAG_INTEGRITY_DAG_DIR to a DAG folder")
def test_dagbag_imports_without_errors() -> None:
    from airflow.models.dagbag import DagBag

    dagbag = DagBag(dag_folder=DAG_DIR, include_examples=False)
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"
    assert dagbag.dags, f"no DAGs found in {DAG_DIR}"
