"""A framework job reports column-level lineage when OpenLineage is switched on.

On Managed Service for Apache Spark (Dataproc) this is one property, spark.dataproc.lineage.enabled,
set in submit.sh, and the events go to Knowledge Catalog. Anywhere else it's the OpenLineage Spark
listener, configured through the job spec's spark_conf, which is what this exercises. The file
transport stands in for whichever backend receives the events.

Runs the real entrypoint in its own process, because the listener has to be there when the Spark
session starts, and the other tests in this directory share a session that doesn't have it.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

OPENLINEAGE_SPARK = "io.openlineage:openlineage-spark_2.12:1.53.0"
SPARK_DIR = Path(__file__).resolve().parents[1]


def test_job_reports_which_columns_feed_each_output_column(tmp_path):
    from pyspark.sql import SparkSession

    trades = tmp_path / "trades"
    spark = (SparkSession.builder.master("local[1]")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.createDataFrame(
        [("cli-1", 1.0, 0.5, 0.1), ("cli-2", 2.0, 0.0, 0.2)],
        ["client_id", "spread_revenue", "commission", "funding_charge"],
    ).write.mode("overwrite").parquet(str(trades))

    events = tmp_path / "lineage.jsonl"
    output = tmp_path / "client_revenue"
    spec = tmp_path / "job.yaml"
    spec.write_text(textwrap.dedent(f"""
        name: client_revenue
        sources:
          - {{name: trades, format: gcs, options: {{format: parquet, path: "{trades}"}}}}
        transforms:
          - type: aggregate
            group_by: [client_id]
            aggregations:
              trading_revenue: "sum(spread_revenue) + sum(commission) + sum(funding_charge)"
        sink:
          format: gcs
          mode: overwrite
          options: {{format: parquet, path: "{output}"}}
        spark_conf:
          spark.master: "local[1]"
          spark.ui.enabled: "false"
          spark.jars.packages: "{OPENLINEAGE_SPARK}"
          spark.extraListeners: io.openlineage.spark.agent.OpenLineageSparkListener
          spark.openlineage.namespace: dwh-local
          spark.openlineage.transport.type: file
          spark.openlineage.transport.location: "{events}"
    """))

    run = subprocess.run(
        [sys.executable, "-m", "framework.main", "--config", str(spec)],
        cwd=SPARK_DIR, capture_output=True, text=True, timeout=600,
    )
    if run.returncode != 0 and "Could not resolve" in run.stderr + run.stdout:
        pytest.skip("couldn't download the OpenLineage Spark listener")
    assert run.returncode == 0, run.stderr[-2000:]

    lineage = {}
    for line in events.read_text().splitlines():
        for dataset in json.loads(line).get("outputs", []):
            facet = dataset.get("facets", {}).get("columnLineage")
            if facet and dataset["name"].endswith("client_revenue"):
                for column, spec_ in facet["fields"].items():
                    lineage[column] = sorted(f["field"] for f in spec_["inputFields"])

    assert lineage["trading_revenue"] == ["commission", "funding_charge", "spread_revenue"]
    assert lineage["client_id"] == ["client_id"]
