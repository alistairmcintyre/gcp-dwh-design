"""Data contract compatibility checking.

This is the part of a contract registry that actually changes behaviour. Publishing contracts is
documentation; *refusing a merge that breaks a consumer* is enforcement. Everything here is pure
functions over parsed contracts so it can run in CI with no service, no network and no credentials
-- the check has to be cheap enough that nobody is tempted to skip it.

The rules mirror schema-registry semantics because the same reasoning applies: a warehouse replays
history constantly, so BACKWARD compatibility (new readers can read old data) is the default that
keeps replay working.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Widening is safe: every value of the source type fits in the target. Narrowing is not.
# Deliberately conservative -- STRING is not a universal target here, because silently stringifying
# a numeric column breaks arithmetic in every consumer while passing a naive "is it compatible" test.
SAFE_WIDENINGS: dict[str, set[str]] = {
    "INT64": {"NUMERIC", "BIGNUMERIC", "FLOAT64"},
    "NUMERIC": {"BIGNUMERIC"},
    "FLOAT64": set(),
    "DATE": {"DATETIME", "TIMESTAMP"},
    "DATETIME": {"TIMESTAMP"},
}


@dataclass(frozen=True)
class Violation:
    """One incompatibility. `field` is None for contract-level problems."""

    rule: str
    field: str | None
    detail: str

    def __str__(self) -> str:
        where = f"{self.field}: " if self.field else ""
        return f"[{self.rule}] {where}{self.detail}"


def _fields_by_name(contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {f["name"]: f for f in contract.get("schema", [])}


def _parse_version(version: str) -> tuple[int, int, int]:
    try:
        major, minor, patch = (int(part) for part in str(version).split("."))
    except ValueError as exc:
        raise ValueError(f"version must be MAJOR.MINOR.PATCH, got '{version}'") from exc
    return major, minor, patch


def check_compatibility(current: dict[str, Any], proposed: dict[str, Any]) -> list[Violation]:
    """Return every way `proposed` breaks consumers of `current`. Empty list means compatible.

    Returns ALL violations rather than the first, because a contract change is reviewed once: an
    author who has to re-run CI five times to discover five problems stops running CI.
    """
    violations: list[Violation] = []

    if current.get("id") != proposed.get("id"):
        violations.append(
            Violation("identity", None, f"contract id changed: {current.get('id')} -> {proposed.get('id')}")
        )

    current_fields = _fields_by_name(current)
    proposed_fields = _fields_by_name(proposed)

    # --- Removals: a consumer may be selecting the column ----------------------------------------
    for name in current_fields.keys() - proposed_fields.keys():
        violations.append(Violation("field_removed", name, "field removed; a consumer may select it"))

    for name, proposed_field in proposed_fields.items():
        current_field = current_fields.get(name)

        # --- Additions: only optional fields are safe --------------------------------------------
        if current_field is None:
            if proposed_field.get("required", False):
                violations.append(
                    Violation("required_field_added", name, "new field is required; existing writers do not produce it")
                )
            continue

        # --- Type changes: widening only ---------------------------------------------------------
        old_type, new_type = current_field.get("type"), proposed_field.get("type")
        if old_type != new_type and new_type not in SAFE_WIDENINGS.get(old_type, set()):
            violations.append(
                Violation("type_narrowed", name, f"type changed {old_type} -> {new_type}, which is not a safe widening")
            )

        # --- Nullability: relaxing breaks consumers that assume presence -------------------------
        was_required = current_field.get("required", False)
        now_required = proposed_field.get("required", False)
        if was_required and not now_required:
            violations.append(
                Violation("nullability_relaxed", name, "field was required and is now optional; consumers may assume it is present")
            )

        # --- Classification: removing a PII class silently un-masks a column ---------------------
        # Not a consumer-compatibility issue -- it is worse. Dropping `pii_class` removes the policy
        # tag on the next dbt build and publishes personal data in the clear, with nothing failing.
        old_class, new_class = current_field.get("pii_class"), proposed_field.get("pii_class")
        if old_class and not new_class:
            violations.append(
                Violation("pii_declassified", name, f"pii_class '{old_class}' removed; the column would be published unmasked")
            )
        elif old_class and new_class and old_class != new_class:
            violations.append(
                Violation("pii_class_changed", name, f"pii_class changed {old_class} -> {new_class}; masking rule changes with it")
            )

    # --- Grain: changing it changes what a row means ---------------------------------------------
    old_grain = list((current.get("grain") or {}).get("columns", []))
    new_grain = list((proposed.get("grain") or {}).get("columns", []))
    if old_grain != new_grain:
        violations.append(
            Violation("grain_changed", None, f"grain changed {old_grain} -> {new_grain}; every aggregate over this table changes meaning")
        )

    # --- SLO: loosening a promise consumers have built on is a breaking change ------------------
    old_lag = ((current.get("slo") or {}).get("freshness") or {}).get("max_lag_hours")
    new_lag = ((proposed.get("slo") or {}).get("freshness") or {}).get("max_lag_hours")
    if old_lag is not None and new_lag is not None and new_lag > old_lag:
        violations.append(
            Violation("slo_relaxed", None, f"freshness max_lag_hours loosened {old_lag} -> {new_lag}; consumers planned around the old number")
        )

    return violations


def required_version_bump(violations: list[Violation]) -> str:
    """What the version must do given these violations: 'major', 'minor' or 'none'."""
    return "major" if violations else "minor"


def check_version_bump(current: dict[str, Any], proposed: dict[str, Any], violations: list[Violation]) -> list[Violation]:
    """A breaking change is allowed -- but only with a MAJOR version bump.

    This is what makes the check usable rather than obstructive. Teams do need to break contracts
    sometimes; what they must not do is break one *silently*. Forcing the major bump makes the
    breakage visible in the version number, which is what consumers pin against.
    """
    problems: list[Violation] = []
    try:
        old_major, old_minor, _ = _parse_version(current.get("version", "0.0.0"))
        new_major, new_minor, _ = _parse_version(proposed.get("version", "0.0.0"))
    except ValueError as exc:
        return [Violation("version_malformed", None, str(exc))]

    if violations and new_major <= old_major:
        problems.append(
            Violation(
                "version_bump_required",
                None,
                f"{len(violations)} breaking change(s) require a MAJOR version bump "
                f"(currently {current.get('version')} -> {proposed.get('version')})",
            )
        )
    elif not violations and (new_major, new_minor) <= (old_major, old_minor):
        problems.append(
            Violation("version_bump_required", None, f"schema changed but version did not advance from {current.get('version')}")
        )
    return problems
