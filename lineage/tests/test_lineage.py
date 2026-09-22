"""Column lineage, and the checks built on it.

The fixture is a small dbt project in miniature: a source, a staging model, a tagged Gold model, and
one leak, an export that copies the email under a new name into a table nobody listed. The leak is
the case that matters, because matching column names would miss it and SQL-based lineage doesn't.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from lineage import checks
from lineage.graph import Column, build


def _node(schema, name, sql=None, columns=(), depends=(), pii=None, materialized="table"):
    return {
        "resource_type": "model",
        "config": {"materialized": materialized},
        "compiled_code": sql,
        "relation_name": f'"dev"."{schema}"."{name}"',
        "depends_on": {"nodes": list(depends)},
        "columns": {
            c: {"meta": {"pii_class": pii[c]} if pii and c in pii else {}} for c in columns
        },
    }


def _catalog(schema, name, columns):
    return {"metadata": {"database": "dev", "schema": schema, "name": name},
            "columns": {c: {"type": "VARCHAR"} for c in columns}}


@pytest.fixture
def target(tmp_path) -> pathlib.Path:
    source_id, stg_id = "source.dwh.raw.clients", "model.dwh.stg_clients"
    dim_id, leak_id = "model.dwh.dim_client", "model.dwh.partner_export"
    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "sources": {source_id: {
            "relation_name": '"dev"."raw"."clients"',
            "columns": {"client_id": {"meta": {}}, "email": {"meta": {"pii_class": "contact"}},
                        "country": {"meta": {}}},
        }},
        "nodes": {
            stg_id: _node("staging", "stg_clients",
                          'select client_id, email, country from "dev"."raw"."clients"',
                          depends=[source_id], materialized="view"),
            dim_id: _node("marts", "dim_client",
                          'select client_id, email, country from "dev"."staging"."stg_clients"',
                          columns=["client_id", "email", "country"], depends=[stg_id],
                          pii={"email": "contact"}),
            leak_id: _node("marts", "partner_export",
                           'select client_id, lower(email) as contact_email '
                           'from "dev"."staging"."stg_clients"',
                           depends=[stg_id]),
        },
    }
    catalog = {
        "sources": {source_id: _catalog("raw", "clients", ["client_id", "email", "country"])},
        "nodes": {
            stg_id: _catalog("staging", "stg_clients", ["client_id", "email", "country"]),
            dim_id: _catalog("marts", "dim_client", ["client_id", "email", "country"]),
            leak_id: _catalog("marts", "partner_export", ["client_id", "contact_email"]),
        },
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))
    return tmp_path


def test_trace_follows_a_column_back_to_its_source(target):
    graph = build(target)
    assert graph.trace(Column("marts.dim_client", "email")) == [[
        Column("marts.dim_client", "email"),
        Column("staging.stg_clients", "email"),
        Column("raw.clients", "email"),
    ]]


def test_lineage_follows_a_renamed_column(target):
    """lower(email) as contact_email. Name matching would call this a different column."""
    graph = build(target)
    downstream = graph.impact(Column("raw.clients", "email"))
    assert Column("marts.partner_export", "contact_email") in downstream


def test_a_column_with_no_personal_data_stays_clean(target):
    reach = build(target).pii_reach()
    assert Column("marts.dim_client", "country") not in reach


def test_the_leak_is_caught_on_both_counts(target):
    findings = checks.run(build(target), inventory={"raw.clients", "staging.stg_clients",
                                                    "marts.dim_client"})
    assert findings.missing_from_inventory == {"marts.partner_export"}
    assert findings.untagged_served_columns == [Column("marts.partner_export", "contact_email")]
    assert not findings.ok


def test_listing_and_tagging_the_table_clears_it(target):
    graph = build(target)
    graph.pii[Column("marts.partner_export", "contact_email")] = "contact"
    findings = checks.run(graph, inventory={"raw.clients", "staging.stg_clients",
                                            "marts.dim_client", "marts.partner_export"})
    assert findings.ok


def test_an_untraceable_column_next_to_personal_data_fails_closed(target):
    """If sqlglot can't follow it, the check assumes it might carry personal data."""
    catalog = json.loads((target / "catalog.json").read_text())
    catalog["nodes"]["model.dwh.dim_client"]["columns"]["mystery"] = {"type": "VARCHAR"}
    (target / "catalog.json").write_text(json.dumps(catalog))

    graph = build(target)
    findings = checks.run(graph, inventory={
        "raw.clients", "staging.stg_clients", "marts.dim_client", "marts.partner_export",
    })
    assert Column("marts.dim_client", "mystery") in findings.unresolved_near_pii


REAL_TARGET = pathlib.Path(__file__).resolve().parents[2] / "dbt" / "target"


@pytest.mark.skipif(not (REAL_TARGET / "catalog.json").exists(),
                    reason="needs dbt compile and dbt docs generate first (make lineage-check)")
def test_the_real_project_passes_every_check():
    graph = build(REAL_TARGET)
    findings = checks.run(graph)
    assert findings.ok, findings
    assert "marts.dim_client" in findings.reach
    assert not graph.unresolved
