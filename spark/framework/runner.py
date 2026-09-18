"""Wire a JobSpec into an actual Spark run: read -> transform -> assert -> write."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from pyspark.sql import Column, DataFrame, Observation, SparkSession
from pyspark.sql import functions as F

from framework.config import JobSpec
from framework.io.readers import READERS
from framework.io.writers import WRITERS
from framework.transforms import TRANSFORMS

logger = logging.getLogger("dataproc-framework")


class QualityGateFailed(RuntimeError):
    """Raised when a fail-level quality assertion does not hold. Aborts before the write."""


def _log_event(event: str, **fields: Any) -> None:
    """Emit one JSON line per event.

    Cloud Logging parses JSON on stdout into structured fields, which is what makes these usable as
    log-based metrics (rows written per job, step duration) and therefore as Cloud Monitoring alerts
    without shipping metrics separately. Unstructured prints cannot be alerted on.
    """
    logger.info(json.dumps({"event": event, **fields}))


def build_spark(job: JobSpec) -> SparkSession:
    builder = SparkSession.builder.appName(job.name)
    for key, value in job.spark_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def read_sources(spark: SparkSession, job: JobSpec) -> DataFrame:
    """Read every source and register it as a temp view named after the source.

    The FIRST source is the one the transform chain starts from; the rest exist so `join` and `sql`
    steps can reference them by name. That convention keeps the common single-source job trivial
    while still allowing multi-source ones.
    """
    primary: DataFrame | None = None
    for source in job.sources:
        started = time.monotonic()
        df = READERS[source.format](spark, source.options)
        df.createOrReplaceTempView(source.name)
        _log_event(
            "source_read",
            job=job.name,
            source=source.name,
            format=source.format,
            seconds=round(time.monotonic() - started, 3),
        )
        if primary is None:
            primary = df
    assert primary is not None  # JobSpec.parse guarantees at least one source
    return primary


def apply_transforms(spark: SparkSession, job: JobSpec, df: DataFrame) -> DataFrame:
    for index, step in enumerate(job.transforms):
        if step.type not in TRANSFORMS:
            raise ValueError(
                f"transform[{index}]: unknown type '{step.type}'. Supported: {sorted(TRANSFORMS)}"
            )
        df = TRANSFORMS[step.type](df, step.args, spark)
        _log_event("transform_applied", job=job.name, step=index, type=step.type)
    return df


def _violation_metric(check, alias: str) -> Column:
    """One aggregate that counts the rows violating `check`, for collection via `observe`.

    NULL handling is a deliberate change from the obvious `filter(not (expr))`. That formulation
    evaluates `NOT NULL` to NULL, which `filter` drops -- so a row whose predicate could not be
    evaluated silently PASSED the gate. For a quality gate that is the wrong default: an
    unevaluable assertion is a failed assertion, not a satisfied one. Here TRUE scores 0 and both
    FALSE and NULL score 1, so `amount > 0` now flags rows where `amount` is null rather than
    waving them through.
    """
    return F.coalesce(
        F.sum(F.when(F.expr(check.expression), F.lit(0)).otherwise(F.lit(1))),
        F.lit(0),
    ).alias(alias)


def run_quality_gate(job: JobSpec, df: DataFrame) -> None:
    """Evaluate assertions BEFORE the write, so a failure means nothing was published.

    Each expression is a row-level predicate that must hold for every row, and the check counts the
    rows that violate it. Counting violations rather than asserting a boolean lets the failure
    message say *how bad* it is -- the difference between an alert someone can triage and one they
    have to reproduce.

    All checks are collected in ONE pass using the Observation API. The obvious implementation --
    `df.filter(...).count()` per check -- needs the frame cached or it recomputes the whole DAG per
    assertion, and caching a large frame is itself the expensive part: it competes for executor
    memory with the job and spills to disk when it loses. `observe` attaches the aggregates to a
    single scan and needs no cache at all.

    The gate deliberately runs its own action rather than riding on the write. Attaching the
    observation to the write would make it one pass instead of two, but the metrics would then only
    be readable *after* the data had been published -- and a job that writes bad data and then
    reports a failed check has already caused the incident. Two passes is the price of the
    nothing-was-written guarantee, and it is worth paying.
    """
    if not job.quality:
        return

    # Aliases must be stable identifiers, so index rather than using the check's display name.
    aliases = {f"check_{index}": check for index, check in enumerate(job.quality)}
    observation = Observation("quality_gate")
    observed = df.observe(observation, *(_violation_metric(check, alias) for alias, check in aliases.items()))

    # A single action over the observed frame resolves every metric at once.
    row_count = observed.count()
    results = observation.get

    failures: list[str] = []
    for alias, check in aliases.items():
        violations = int(results.get(alias, 0) or 0)
        passed = violations == 0
        _log_event(
            "quality_check",
            job=job.name,
            check=check.name,
            passed=passed,
            violations=violations,
            rows=row_count,
            on_failure=check.on_failure,
        )
        if not passed:
            message = f"{check.name}: {violations} rows violate `{check.expression}`"
            if check.on_failure == "fail":
                failures.append(message)
            else:
                logger.warning("quality check warning -- %s", message)

    if failures:
        raise QualityGateFailed(
            f"{len(failures)} quality check(s) failed, nothing written: " + "; ".join(failures)
        )


def write_sink(job: JobSpec, df: DataFrame) -> None:
    started = time.monotonic()
    WRITERS[job.sink.format](df, job.sink.mode, job.sink.partition_by, job.sink.options)
    _log_event(
        "sink_written",
        job=job.name,
        format=job.sink.format,
        mode=job.sink.mode,
        seconds=round(time.monotonic() - started, 3),
    )


def run(job: JobSpec, spark: SparkSession | None = None) -> None:
    """Execute a job.

    `spark` is injectable so a caller that already owns a session -- a test harness, or a driver
    running several jobs in one batch -- can supply it. The session is only stopped when this
    function created it: stopping a session you were handed tears down the caller's context, which
    is a rude thing for a library to do and made the end-to-end tests fail in a way that took a
    minute to understand.
    """
    owns_session = spark is None
    spark = spark or build_spark(job)
    _log_event("job_started", job=job.name, sources=[s.name for s in job.sources])
    started = time.monotonic()
    try:
        df = read_sources(spark, job)
        df = apply_transforms(spark, job, df)
        run_quality_gate(job, df)
        write_sink(job, df)
        _log_event("job_succeeded", job=job.name, seconds=round(time.monotonic() - started, 3))
    except Exception as exc:
        _log_event(
            "job_failed",
            job=job.name,
            error=type(exc).__name__,
            message=str(exc)[:500],
            seconds=round(time.monotonic() - started, 3),
        )
        raise
    finally:
        if owns_session:
            spark.stop()
