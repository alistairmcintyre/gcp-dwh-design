"""Data contract registry API.

A small Cloud Run service that serves the contracts in `contracts/` and, more usefully, validates a
proposed contract change against the current one. The validate endpoint is what CI calls on a pull
request that touches a contract, which is what turns a directory of YAML into an enforced standard.

Why a service rather than just a CI script (the compatibility logic is importable either way):
  * one answer for the whole estate -- squads in London, India and Poland call the same endpoint
    rather than each vendoring a copy of the rules that drift apart
  * the registry becomes queryable by things that are not CI: a lineage tool asking who consumes a
    table, a dashboard showing SLO coverage, an LLM agent grounding on what the data means
  * contract changes get an audit trail in one place

Run locally:  uvicorn app.main:app --reload
Deploy:       see terraform/modules/cloudrun
"""

from __future__ import annotations

import os
import pathlib
from typing import Any

import yaml
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.compatibility import check_compatibility, check_version_bump

CONTRACTS_DIR = pathlib.Path(os.environ.get("CONTRACTS_DIR", "/app/contracts"))

app = FastAPI(
    title="Data Contract Registry",
    description="Serves and validates the data contracts for the GCP lakehouse.",
    version="1.0.0",
)


def load_contracts() -> dict[str, dict[str, Any]]:
    """Read every contract from disk on each call.

    Deliberately not cached: the contracts are baked into the image, so a change ships as a new
    revision anyway, and re-reading a handful of small files costs nothing next to a cold start.
    Caching here would be an optimisation with a correctness risk and no measurable benefit.
    """
    contracts: dict[str, dict[str, Any]] = {}
    if not CONTRACTS_DIR.is_dir():
        return contracts
    for path in sorted(CONTRACTS_DIR.glob("*.yaml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(document, dict) and document.get("kind") == "DataContract":
            contracts[document["id"]] = document
    return contracts


class ValidateRequest(BaseModel):
    contract: dict[str, Any] = Field(..., description="The proposed contract, as parsed YAML/JSON.")
    against: str | None = Field(
        None,
        description="Contract id to compare against. Defaults to the proposed contract's own id, "
                    "which is what a CI check on a modified file wants.",
    )


class ValidateResponse(BaseModel):
    compatible: bool
    breaking_changes: list[str]
    version_problems: list[str]
    # Split deliberately: a breaking change is a decision for the owner to make, while a missing
    # version bump is always just a mistake. Collapsing them into one list loses that distinction.
    summary: str


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness probe. Cheap and dependency-free, so a slow disk cannot fail the container."""
    return {"status": "ok"}


@app.get("/contracts")
def list_contracts() -> dict[str, Any]:
    """Every contract, summarised. The estate-wide view: who owns what, and what they promised."""
    contracts = load_contracts()
    return {
        "count": len(contracts),
        "contracts": [
            {
                "id": contract["id"],
                "version": contract.get("version"),
                "owner": (contract.get("owner") or {}).get("team"),
                "contains_pii": (contract.get("classification") or {}).get("contains_pii", False),
                "freshness_target": ((contract.get("slo") or {}).get("freshness") or {}).get("target"),
                "consumers": len(contract.get("consumers") or []),
            }
            for contract in contracts.values()
        ],
    }


@app.get("/contracts/{contract_id}")
def get_contract(contract_id: str) -> dict[str, Any]:
    contract = load_contracts().get(contract_id)
    if contract is None:
        raise HTTPException(status_code=404, detail=f"no contract '{contract_id}'")
    return contract


@app.get("/contracts/{contract_id}/consumers")
def get_consumers(contract_id: str) -> dict[str, Any]:
    """Who breaks if this changes.

    The question nobody can answer without a registry, and the one that decides whether a schema
    change is a five-minute job or a three-team negotiation.
    """
    contract = load_contracts().get(contract_id)
    if contract is None:
        raise HTTPException(status_code=404, detail=f"no contract '{contract_id}'")
    consumers = contract.get("consumers") or []
    return {
        "contract_id": contract_id,
        "consumers": consumers,
        "high_criticality": [c for c in consumers if c.get("criticality") == "high"],
    }


@app.post("/contracts/validate", response_model=ValidateResponse)
def validate(request: ValidateRequest) -> ValidateResponse:
    """Check a proposed contract against the registered one. This is the CI gate."""
    proposed = request.contract
    contract_id = request.against or proposed.get("id")
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract has no 'id' and no 'against' was given")

    current = load_contracts().get(contract_id)
    if current is None:
        # A brand-new contract cannot break anyone. Registering one is always allowed.
        return ValidateResponse(
            compatible=True,
            breaking_changes=[],
            version_problems=[],
            summary=f"'{contract_id}' is new; nothing to be incompatible with.",
        )

    violations = check_compatibility(current, proposed)
    version_problems = check_version_bump(current, proposed, violations)

    if not violations and not version_problems:
        summary = f"'{contract_id}' is backward compatible."
    elif violations and not version_problems:
        summary = (
            f"'{contract_id}' has {len(violations)} breaking change(s), declared with a major "
            "version bump. Consumers must be notified before this ships."
        )
    else:
        summary = f"'{contract_id}' cannot ship: {'; '.join(str(p) for p in version_problems)}"

    return ValidateResponse(
        compatible=not violations,
        breaking_changes=[str(v) for v in violations],
        version_problems=[str(p) for p in version_problems],
        summary=summary,
    )
