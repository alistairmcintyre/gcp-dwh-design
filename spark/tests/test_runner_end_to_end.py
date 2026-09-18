"""End-to-end test: run a real job spec through a real Spark session.

The unit tests prove configs are validated. This proves the framework actually WORKS -- sources are
read, transforms compose in order, the quality gate blocks a bad write, and the sink receives what
it should. It uses local parquet rather than BigQuery so it runs anywhere, including CI, with no
cloud credentials and no cost, which is the only way a test like this gets run often enough to be
worth having.
"""

from __future__ import annotations

import pytest

from framework.config import JobSpec
from framework.runner import QualityGateFailed, apply_transforms, read_sources, run, run_quality_gate

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("framework-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")  # 200 default partitions on 6 rows is absurd
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def trades_parquet(spark, tmp_path):
    """Three clients, six trade events, one duplicate event to exercise deduplication."""
    rows = [
        # trade_id, client_id, date, notional, spread_revenue, version
        ("DIAAAA1", "cli-1", "2026-01-01", 1000.0, 0.6, 1),
        ("DIAAAA2", "cli-1", "2026-01-01", 2000.0, 1.2, 1),
        ("DIAAAA3", "cli-2", "2026-01-01", 500.0, 0.3, 1),
        ("DIAAAA4", "cli-2", "2026-01-02", 5000.0, 3.0, 1),
        ("DIAAAA5", "cli-3", "2026-01-02", 10000.0, 6.0, 1),
        # Same trade_id as DIAAAA5, later version: deduplicate must keep this one.
        ("DIAAAA5", "cli-3", "2026-01-02", 10000.0, 8.0, 2),
    ]
    df = spark.createDataFrame(
        rows,
        "trade_id string, client_id string, d string, notional_value double, "
        "spread_revenue double, version int",
    )
    path = str(tmp_path / "trades")
    df.write.mode("overwrite").parquet(path)
    return path


def _spec(source_path: str, sink_path: str, quality: str = "") -> JobSpec:
    return JobSpec.from_yaml(f"""
name: test_job
sources:
  - name: trades
    format: gcs
    options: {{format: parquet, path: "{source_path}"}}
transforms:
  - type: deduplicate
    keys: [trade_id]
    order_by: [version]
    descending: true
  - type: aggregate
    group_by: [client_id]
    aggregations:
      trade_count: count(1)
      total_notional: sum(notional_value)
      trading_revenue: sum(spread_revenue)
sink:
  format: gcs
  mode: overwrite
  options: {{format: parquet, path: "{sink_path}"}}
{quality}
""")


def test_transform_chain_deduplicates_and_aggregates(spark, trades_parquet, tmp_path):
    job = _spec(trades_parquet, str(tmp_path / "out"))
    df = apply_transforms(spark, job, read_sources(spark, job))
    result = {r["client_id"]: r for r in df.collect()}

    assert len(result) == 3
    assert result["cli-1"]["trade_count"] == 2
    assert result["cli-1"]["total_notional"] == pytest.approx(3000.0)
    assert result["cli-1"]["trading_revenue"] == pytest.approx(1.8)

    # The duplicate DIAAAA5 collapsed to one row, and to the HIGHER version (spread 8.0, not 6.0).
    assert result["cli-3"]["trade_count"] == 1
    assert result["cli-3"]["trading_revenue"] == pytest.approx(8.0)


def test_quality_gate_blocks_the_write(spark, trades_parquet, tmp_path):
    """A failing check must abort BEFORE the sink is touched -- writing then failing is an incident."""
    sink = tmp_path / "out_blocked"
    job = _spec(
        trades_parquet,
        str(sink),
        quality="""
quality:
  - {name: revenue_above_floor, expression: "trading_revenue > 5.0", on_failure: fail}
""",
    )
    with pytest.raises(QualityGateFailed) as excinfo:
        run(job, spark=spark)

    assert "revenue_above_floor" in str(excinfo.value)
    assert "rows violate" in str(excinfo.value)
    assert not sink.exists(), "the sink was written despite a failing fail-level quality check"


def test_warn_level_check_does_not_block(spark, trades_parquet, tmp_path):
    sink = tmp_path / "out_warned"
    job = _spec(
        trades_parquet,
        str(sink),
        quality="""
quality:
  - {name: revenue_above_floor, expression: "trading_revenue > 5.0", on_failure: warn}
""",
    )
    run(job, spark=spark)
    assert sink.exists(), "a warn-level check must not prevent the write"


def test_full_run_writes_the_expected_rows(spark, trades_parquet, tmp_path):
    sink = tmp_path / "out_ok"
    run(_spec(trades_parquet, str(sink)), spark=spark)

    written = spark.read.parquet(str(sink))
    assert written.count() == 3
    assert set(written.columns) == {"client_id", "trade_count", "total_notional", "trading_revenue"}


def test_null_predicate_counts_as_a_violation(spark, tmp_path):
    """An assertion that cannot be evaluated is a FAILED assertion, not a satisfied one.

    The obvious implementation, `df.filter(f"not ({expr})").count()`, evaluates `NOT NULL` to NULL
    and `filter` drops it -- so a row whose predicate is unevaluable silently passes the gate. That
    is the wrong default for a quality gate and it is exactly the kind of hole that lets a column of
    nulls reach a mart. The Observation-based gate scores NULL as a violation.
    """
    rows = [("a", 10.0), ("b", None), ("c", 5.0)]
    df = spark.createDataFrame(rows, "id string, amount double")
    source = str(tmp_path / "nulls")
    df.write.mode("overwrite").parquet(source)

    sink = tmp_path / "out_nulls"
    job = JobSpec.from_yaml(f"""
name: null_gate
sources:
  - {{name: s, format: gcs, options: {{format: parquet, path: "{source}"}}}}
sink: {{format: gcs, mode: overwrite, options: {{format: parquet, path: "{sink}"}}}}
quality:
  - {{name: amount_positive, expression: "amount > 0", on_failure: fail}}
""")

    with pytest.raises(QualityGateFailed) as excinfo:
        run(job, spark=spark)

    assert "amount_positive" in str(excinfo.value)
    assert "1 rows violate" in str(excinfo.value), "the NULL row should be the single violation"
    assert not sink.exists()


def test_all_checks_resolve_in_one_pass(spark, trades_parquet, tmp_path):
    """Several checks must not mean several scans -- that was the point of moving to observe()."""
    sink = tmp_path / "out_multi"
    job = _spec(
        trades_parquet,
        str(sink),
        quality="""
quality:
  - {name: has_user, expression: "client_id is not null", on_failure: fail}
  - {name: positive_stake, expression: "total_notional > 0", on_failure: fail}
  - {name: sane_trade_count, expression: "trade_count between 1 and 100", on_failure: fail}
""",
    )
    run(job, spark=spark)
    assert sink.exists(), "all three checks pass, so the write should proceed"
