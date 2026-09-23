"""Send a Spark job's lineage to the local Marquez, alongside the dbt lineage.

`make lineage-demo` runs dbt through dbt-ol first, which covers the warehouse side. This adds the
other half: a framework job reading files and writing files, reported by the OpenLineage Spark
listener. Together they show the thing a single-tool catalog can't, one graph covering two engines.

Nothing here is specific to Marquez. Point the transport at Knowledge Catalog or SageMaker Catalog
and the same events go there instead.
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
import textwrap

REPO = pathlib.Path(__file__).resolve().parents[1]
OPENLINEAGE_SPARK = "io.openlineage:openlineage-spark_2.12:1.53.0"


def export_trades(duckdb_path: pathlib.Path, out: pathlib.Path) -> int:
    import duckdb

    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        rows = con.execute("select count(*) from raw.trades").fetchone()[0]
        con.execute(
            "copy (select client_id, spread_revenue, commission, funding_charge from raw.trades) "
            f"to '{out}' (format parquet)"
        )
        return rows
    finally:
        con.close()


def run_spark_job(trades: pathlib.Path, output: pathlib.Path, marquez: str, namespace: str) -> None:
    spec = output.parent / "lineage_demo_job.yaml"
    spec.write_text(textwrap.dedent(f"""
        name: client_revenue_lineage_demo
        description: Reads the exported trades and writes revenue per client, reporting lineage.
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
          spark.master: "local[2]"
          spark.ui.enabled: "false"
          spark.jars.packages: "{OPENLINEAGE_SPARK}"
          spark.extraListeners: io.openlineage.spark.agent.OpenLineageSparkListener
          spark.openlineage.transport.type: http
          spark.openlineage.transport.url: "{marquez}"
          spark.openlineage.namespace: "{namespace}"
    """))
    subprocess.run(
        [sys.executable, "-m", "framework.main", "--config", str(spec)],
        cwd=REPO / "spark", check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--marquez", default="http://localhost:5000")
    parser.add_argument("--namespace", default="dwh-local")
    parser.add_argument("--duckdb", default=str(REPO / "data" / "dev.duckdb"))
    args = parser.parse_args()

    work = REPO / "data" / "lineage_demo"
    trades = work / "trades.parquet"
    rows = export_trades(pathlib.Path(args.duckdb), trades)
    print(f"exported {rows:,} trades to {trades.relative_to(REPO)}")

    run_spark_job(trades, work / "client_revenue", args.marquez, args.namespace)
    print(f"spark job done, lineage sent to {args.marquez} in namespace {args.namespace}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
