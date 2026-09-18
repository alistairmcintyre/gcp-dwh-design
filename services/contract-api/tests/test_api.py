"""API-level tests, exercising the service the way CI will call it."""

from __future__ import annotations

import copy
import pathlib

import pytest
import yaml

fastapi_testclient = pytest.importorskip("fastapi.testclient")

REPO = pathlib.Path(__file__).resolve().parents[3]
CONTRACTS = REPO / "contracts"


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    import os

    os.environ["CONTRACTS_DIR"] = str(CONTRACTS)
    from app.main import app

    return fastapi_testclient.TestClient(app)


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_lists_the_real_contracts(client):
    body = client.get("/contracts").json()
    assert body["count"] >= 2
    ids = {c["id"] for c in body["contracts"]}
    assert {"marts.fct_client_activity", "marts.dim_client"} <= ids


def test_dim_client_is_flagged_as_pii(client):
    body = client.get("/contracts").json()
    dim = next(c for c in body["contracts"] if c["id"] == "marts.dim_client")
    assert dim["contains_pii"] is True


def test_unknown_contract_is_404(client):
    assert client.get("/contracts/marts.nope").status_code == 404


def test_consumers_endpoint_surfaces_high_criticality(client):
    body = client.get("/contracts/marts.fct_client_activity/consumers").json()
    assert len(body["high_criticality"]) >= 1


def test_validate_accepts_an_unchanged_contract(client):
    contract = yaml.safe_load((CONTRACTS / "fct_client_activity.yaml").read_text())
    body = client.post("/contracts/validate", json={"contract": contract}).json()
    assert body["compatible"] is True
    assert body["breaking_changes"] == []


def test_validate_rejects_a_silent_breaking_change(client):
    """The case the gate exists for: a column removed without a major version bump."""
    contract = yaml.safe_load((CONTRACTS / "fct_client_activity.yaml").read_text())
    proposed = copy.deepcopy(contract)
    proposed["schema"] = [f for f in proposed["schema"] if f["name"] != "trading_revenue"]

    body = client.post("/contracts/validate", json={"contract": proposed}).json()
    assert body["compatible"] is False
    assert any("trading_revenue" in change for change in body["breaking_changes"])
    assert body["version_problems"], "removing a field without a major bump must be rejected"


def test_validate_allows_a_declared_breaking_change(client):
    contract = yaml.safe_load((CONTRACTS / "fct_client_activity.yaml").read_text())
    proposed = copy.deepcopy(contract)
    proposed["schema"] = [f for f in proposed["schema"] if f["name"] != "trading_revenue"]
    proposed["version"] = "3.0.0"

    body = client.post("/contracts/validate", json={"contract": proposed}).json()
    assert body["compatible"] is False        # it IS breaking
    assert body["version_problems"] == []     # but it is declared, so it may ship


def test_validate_catches_pii_declassification(client):
    """Removing pii_class would publish personal data unmasked. Must never pass silently."""
    contract = yaml.safe_load((CONTRACTS / "dim_client.yaml").read_text())
    proposed = copy.deepcopy(contract)
    for field in proposed["schema"]:
        field.pop("pii_class", None)

    body = client.post("/contracts/validate", json={"contract": proposed}).json()
    assert any("pii_declassified" in change for change in body["breaking_changes"])


def test_a_brand_new_contract_is_always_valid(client):
    body = client.post("/contracts/validate", json={"contract": {"id": "marts.brand_new", "version": "1.0.0", "schema": []}}).json()
    assert body["compatible"] is True
