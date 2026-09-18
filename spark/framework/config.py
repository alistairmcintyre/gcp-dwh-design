"""Job specification: the contract between a YAML file and the framework.

The whole point of a configurable framework is that adding a pipeline is a config change reviewed
by whoever owns the data, not a code change reviewed by whoever owns the framework. That only holds
if the config is validated hard and early -- a typo in a YAML key must fail before a Dataproc batch
is submitted, not thirty seconds into a paid Spark job. Everything here is dataclasses plus explicit
checks for that reason: parse errors surface locally, in CI, on a laptop, in milliseconds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

# Connector names are declared here rather than discovered dynamically so an unknown `format`
# fails validation with a list of what IS supported, instead of an AttributeError deep in a worker.
SUPPORTED_READERS = {"bigquery", "gcs", "jdbc"}
SUPPORTED_WRITERS = {"bigquery", "gcs", "firestore"}
SUPPORTED_MODES = {"append", "overwrite", "errorifexists", "ignore"}


class ConfigError(ValueError):
    """Raised for any malformed job spec. Always names the offending key."""


def _require(mapping: dict[str, Any], key: str, context: str) -> Any:
    if key not in mapping:
        raise ConfigError(f"{context}: missing required key '{key}'")
    return mapping[key]


@dataclass(frozen=True)
class SourceSpec:
    """Where the data comes from.

    `options` is passed straight to the Spark reader, so anything the underlying connector supports
    is reachable without a framework change -- the escape hatch that stops a config-driven design
    becoming a cage.
    """

    name: str
    format: str
    options: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def parse(raw: dict[str, Any]) -> SourceSpec:
        context = f"source '{raw.get('name', '<unnamed>')}'"
        fmt = _require(raw, "format", context)
        if fmt not in SUPPORTED_READERS:
            raise ConfigError(f"{context}: unsupported format '{fmt}'. Supported: {sorted(SUPPORTED_READERS)}")
        return SourceSpec(
            name=_require(raw, "name", context),
            format=fmt,
            options=raw.get("options", {}) or {},
        )


@dataclass(frozen=True)
class TransformSpec:
    """One declarative step. `type` selects the operator; the rest is that operator's arguments."""

    type: str
    args: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def parse(raw: dict[str, Any], index: int) -> TransformSpec:
        context = f"transform[{index}]"
        step_type = _require(raw, "type", context)
        return TransformSpec(type=step_type, args={k: v for k, v in raw.items() if k != "type"})


@dataclass(frozen=True)
class QualitySpec:
    """An in-pipeline assertion, evaluated before anything is written.

    Deliberately fail-fast and BEFORE the write: a Spark job that writes bad data and then reports a
    failed check has already caused the incident. These are the Spark-side equivalent of dbt tests
    blocking downstream models -- see docs/data-quality.md for how the three layers divide up.
    """

    name: str
    expression: str
    on_failure: str = "fail"  # fail | warn

    @staticmethod
    def parse(raw: dict[str, Any], index: int) -> QualitySpec:
        context = f"quality[{index}]"
        on_failure = raw.get("on_failure", "fail")
        if on_failure not in {"fail", "warn"}:
            raise ConfigError(f"{context}: on_failure must be 'fail' or 'warn', got '{on_failure}'")
        return QualitySpec(
            name=_require(raw, "name", context),
            expression=_require(raw, "expression", context),
            on_failure=on_failure,
        )


@dataclass(frozen=True)
class SinkSpec:
    """Where the data goes, and how it replaces what is already there."""

    format: str
    mode: str = "append"
    options: dict[str, Any] = field(default_factory=dict)
    partition_by: list[str] = field(default_factory=list)

    @staticmethod
    def parse(raw: dict[str, Any]) -> SinkSpec:
        context = "sink"
        fmt = _require(raw, "format", context)
        if fmt not in SUPPORTED_WRITERS:
            raise ConfigError(f"{context}: unsupported format '{fmt}'. Supported: {sorted(SUPPORTED_WRITERS)}")
        mode = raw.get("mode", "append")
        if mode not in SUPPORTED_MODES:
            raise ConfigError(f"{context}: unsupported mode '{mode}'. Supported: {sorted(SUPPORTED_MODES)}")
        return SinkSpec(
            format=fmt,
            mode=mode,
            options=raw.get("options", {}) or {},
            partition_by=raw.get("partition_by", []) or [],
        )


@dataclass(frozen=True)
class JobSpec:
    """A complete pipeline: sources -> transforms -> quality gate -> sink."""

    name: str
    sources: list[SourceSpec]
    transforms: list[TransformSpec]
    sink: SinkSpec
    quality: list[QualitySpec] = field(default_factory=list)
    spark_conf: dict[str, str] = field(default_factory=dict)
    description: str = ""

    @staticmethod
    def parse(raw: dict[str, Any]) -> JobSpec:
        if not isinstance(raw, dict):
            raise ConfigError("job spec must be a YAML mapping")

        sources_raw = _require(raw, "sources", "job")
        if not sources_raw:
            raise ConfigError("job: 'sources' must contain at least one source")
        sources = [SourceSpec.parse(s) for s in sources_raw]

        names = [s.name for s in sources]
        duplicates = {n for n in names if names.count(n) > 1}
        if duplicates:
            # Sources are registered as temp views by name; a duplicate would silently shadow.
            raise ConfigError(f"job: duplicate source names {sorted(duplicates)}")

        return JobSpec(
            name=_require(raw, "name", "job"),
            description=raw.get("description", ""),
            sources=sources,
            transforms=[TransformSpec.parse(t, i) for i, t in enumerate(raw.get("transforms", []) or [])],
            quality=[QualitySpec.parse(q, i) for i, q in enumerate(raw.get("quality", []) or [])],
            sink=SinkSpec.parse(_require(raw, "sink", "job")),
            spark_conf={str(k): str(v) for k, v in (raw.get("spark_conf", {}) or {}).items()},
        )

    @staticmethod
    def from_yaml(text: str) -> JobSpec:
        return JobSpec.parse(yaml.safe_load(text))
