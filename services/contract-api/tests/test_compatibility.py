"""Tests for the compatibility rules.

Each test is a change someone will genuinely try to make. The PII ones matter most: they are the
only checks here that prevent a security incident rather than a broken dashboard.
"""

from __future__ import annotations

import copy

import pytest

from app.compatibility import check_compatibility, check_version_bump

BASE = {
    "id": "marts.t",
    "version": "1.0.0",
    "grain": {"columns": ["id"]},
    "slo": {"freshness": {"max_lag_hours": 24}},
    "schema": [
        {"name": "id", "type": "STRING", "required": True},
        {"name": "amount", "type": "INT64", "required": False},
        {"name": "email", "type": "STRING", "required": False, "pii_class": "contact"},
    ],
}


def modified(**changes):
    contract = copy.deepcopy(BASE)
    contract.update(changes)
    return contract


def rules(violations):
    return {v.rule for v in violations}


def test_identical_contract_is_compatible():
    assert check_compatibility(BASE, copy.deepcopy(BASE)) == []


def test_adding_an_optional_field_is_compatible():
    proposed = copy.deepcopy(BASE)
    proposed["schema"].append({"name": "note", "type": "STRING", "required": False})
    assert check_compatibility(BASE, proposed) == []


def test_adding_a_required_field_breaks():
    proposed = copy.deepcopy(BASE)
    proposed["schema"].append({"name": "note", "type": "STRING", "required": True})
    assert "required_field_added" in rules(check_compatibility(BASE, proposed))


def test_removing_a_field_breaks():
    proposed = copy.deepcopy(BASE)
    proposed["schema"] = [f for f in proposed["schema"] if f["name"] != "amount"]
    assert "field_removed" in rules(check_compatibility(BASE, proposed))


def test_widening_a_type_is_compatible():
    proposed = copy.deepcopy(BASE)
    proposed["schema"][1]["type"] = "NUMERIC"  # INT64 -> NUMERIC
    assert check_compatibility(BASE, proposed) == []


def test_narrowing_a_type_breaks():
    proposed = copy.deepcopy(BASE)
    proposed["schema"][1]["type"] = "STRING"  # arithmetic breaks for every consumer
    assert "type_narrowed" in rules(check_compatibility(BASE, proposed))


def test_relaxing_nullability_breaks():
    proposed = copy.deepcopy(BASE)
    proposed["schema"][0]["required"] = False
    assert "nullability_relaxed" in rules(check_compatibility(BASE, proposed))


def test_tightening_nullability_is_compatible():
    proposed = copy.deepcopy(BASE)
    proposed["schema"][1]["required"] = True
    # Tightening an OPTIONAL field to required is safe for readers; adding a NEW required one is not.
    assert "nullability_relaxed" not in rules(check_compatibility(BASE, proposed))


def test_removing_a_pii_class_is_flagged():
    """The security check: dropping pii_class would publish the column unmasked, silently."""
    proposed = copy.deepcopy(BASE)
    del proposed["schema"][2]["pii_class"]
    assert "pii_declassified" in rules(check_compatibility(BASE, proposed))


def test_changing_a_pii_class_is_flagged():
    proposed = copy.deepcopy(BASE)
    proposed["schema"][2]["pii_class"] = "person_name"
    assert "pii_class_changed" in rules(check_compatibility(BASE, proposed))


def test_changing_the_grain_breaks():
    proposed = modified(grain={"columns": ["id", "day"]})
    assert "grain_changed" in rules(check_compatibility(BASE, proposed))


def test_loosening_the_freshness_slo_breaks():
    proposed = modified(slo={"freshness": {"max_lag_hours": 48}})
    assert "slo_relaxed" in rules(check_compatibility(BASE, proposed))


def test_tightening_the_freshness_slo_is_compatible():
    assert check_compatibility(BASE, modified(slo={"freshness": {"max_lag_hours": 6}})) == []


def test_breaking_change_without_major_bump_is_rejected():
    proposed = copy.deepcopy(BASE)
    proposed["schema"] = [f for f in proposed["schema"] if f["name"] != "amount"]
    proposed["version"] = "1.1.0"
    violations = check_compatibility(BASE, proposed)
    assert "version_bump_required" in rules(check_version_bump(BASE, proposed, violations))


def test_breaking_change_with_major_bump_is_allowed():
    """Teams must be able to break contracts -- just never silently."""
    proposed = copy.deepcopy(BASE)
    proposed["schema"] = [f for f in proposed["schema"] if f["name"] != "amount"]
    proposed["version"] = "2.0.0"
    violations = check_compatibility(BASE, proposed)
    assert check_version_bump(BASE, proposed, violations) == []


def test_malformed_version_is_reported_clearly():
    assert "version_malformed" in rules(check_version_bump(BASE, modified(version="two"), []))


@pytest.mark.parametrize("bad", ["1.0", "v1.0.0", ""])
def test_various_malformed_versions(bad):
    assert check_version_bump(BASE, modified(version=bad), []) != []
