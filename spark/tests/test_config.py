"""Tests for the job-spec parser.

These matter more than they look: the framework's whole promise is that adding a pipeline is a
config change, which is only safe if a bad config fails in CI rather than in a paid Dataproc batch.
Every test here is a failure mode that would otherwise surface as a cryptic error inside a Spark
executor, minutes and money into a run.

No Spark session is needed, so they run in milliseconds on any machine.
"""

from __future__ import annotations

import pathlib

import pytest

from framework.config import ConfigError, JobSpec

JOBS_DIR = pathlib.Path(__file__).resolve().parents[1] / "jobs"

MINIMAL = """
name: t
sources:
  - name: s
    format: bigquery
    options: {table: p.d.t}
sink:
  format: bigquery
  options: {table: p.d.out}
"""


def test_minimal_spec_parses():
    job = JobSpec.from_yaml(MINIMAL)
    assert job.name == "t"
    assert job.sources[0].format == "bigquery"
    assert job.sink.mode == "append"  # the safe default: never silently replace data


def test_every_shipped_job_spec_is_valid():
    """Every YAML in jobs/ must parse. This is the gate that keeps the registry honest."""
    # rglob so the generated topic offload specs (jobs/generated/) are covered too:
    # a broken generator must fail the same test as a hand-written spec.
    specs = sorted(JOBS_DIR.rglob("*.yaml"))
    assert specs, "no job specs found -- the glob or the directory moved"
    for path in specs:
        job = JobSpec.from_yaml(path.read_text(encoding="utf-8"))
        assert job.name, f"{path.name}: job has no name"
        assert job.sources, f"{path.name}: job has no sources"


def test_unknown_source_format_is_rejected():
    bad = MINIMAL.replace("format: bigquery\n    options: {table: p.d.t}", "format: cassandra\n    options: {}")
    with pytest.raises(ConfigError, match="unsupported format 'cassandra'"):
        JobSpec.from_yaml(bad)


def test_unknown_sink_format_is_rejected():
    bad = MINIMAL.replace("  format: bigquery\n  options: {table: p.d.out}", "  format: kafka\n  options: {}")
    with pytest.raises(ConfigError, match="unsupported format 'kafka'"):
        JobSpec.from_yaml(bad)


def test_unknown_write_mode_is_rejected():
    bad = MINIMAL.replace("  options: {table: p.d.out}", "  mode: upsert\n  options: {table: p.d.out}")
    with pytest.raises(ConfigError, match="unsupported mode 'upsert'"):
        JobSpec.from_yaml(bad)


def test_duplicate_source_names_are_rejected():
    """Sources become temp views keyed by name; a duplicate would silently shadow the first."""
    bad = """
name: t
sources:
  - {name: s, format: bigquery, options: {table: p.d.a}}
  - {name: s, format: bigquery, options: {table: p.d.b}}
sink: {format: bigquery, options: {table: p.d.out}}
"""
    with pytest.raises(ConfigError, match="duplicate source names"):
        JobSpec.from_yaml(bad)


def test_no_sources_is_rejected():
    with pytest.raises(ConfigError, match="at least one source"):
        JobSpec.from_yaml("name: t\nsources: []\nsink: {format: gcs, options: {path: gs://b/p}}")


def test_missing_required_key_names_the_key():
    with pytest.raises(ConfigError, match="missing required key 'sink'"):
        JobSpec.from_yaml("name: t\nsources: [{name: s, format: gcs, options: {path: gs://b/p}}]")


def test_quality_on_failure_must_be_fail_or_warn():
    bad = MINIMAL + """
quality:
  - {name: q, expression: "x is not null", on_failure: explode}
"""
    with pytest.raises(ConfigError, match="on_failure must be"):
        JobSpec.from_yaml(bad)


def test_transform_args_exclude_the_type_key():
    """`args` must not carry `type`, or an operator could be handed its own dispatch key."""
    spec = MINIMAL + """
transforms:
  - {type: filter, expression: "a > 1"}
"""
    job = JobSpec.from_yaml(spec)
    assert job.transforms[0].type == "filter"
    assert job.transforms[0].args == {"expression": "a > 1"}
