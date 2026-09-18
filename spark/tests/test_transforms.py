"""Tests for the transform registry that do not require a Spark session.

The operators themselves need Spark to execute, so what is verified here is the contract around
them: every type named in a shipped job spec exists in the registry, and misconfigured steps raise a
clear error rather than an AttributeError inside an executor.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

from framework.transforms import TRANSFORMS, TransformError, t_deduplicate, t_filter, t_select

JOBS_DIR = pathlib.Path(__file__).resolve().parents[1] / "jobs"


def test_every_transform_used_by_a_job_spec_exists():
    """Catches a typo like `dedupliate` at CI time instead of mid-batch."""
    for path in sorted(JOBS_DIR.rglob("*.yaml")):
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        for index, step in enumerate(spec.get("transforms") or []):
            assert step["type"] in TRANSFORMS, (
                f"{path.name} transform[{index}]: unknown type '{step['type']}'. "
                f"Supported: {sorted(TRANSFORMS)}"
            )


@pytest.mark.parametrize(
    "operator, args, expected",
    [
        (t_select, {}, "'columns' is required"),
        (t_filter, {}, "'expression' is required"),
        (t_deduplicate, {"keys": ["id"]}, "'keys' and 'order_by' are required"),
    ],
)
def test_missing_arguments_raise_a_named_error(operator, args, expected):
    with pytest.raises(TransformError, match=expected):
        operator(None, args, None)
