"""Source connectors. One function per format, selected by the job spec's `format`.

Each reader takes the Spark session and the source's `options` and returns a DataFrame. Keeping the
registry explicit (rather than getattr on a module) means an unknown format fails in config
validation with a useful message, and means every supported connector is greppable in one place.
"""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession


def read_bigquery(spark: SparkSession, options: dict[str, Any]) -> DataFrame:
    """Read a BigQuery table or query via the Spark BigQuery connector.

    `filter` is passed through deliberately: the connector pushes it down to BigQuery's storage read
    API, so partition pruning happens BEFORE bytes cross the wire. Reading a whole partitioned table
    into Spark and filtering in-engine is the most expensive mistake available in this connector,
    and it looks identical in the code.
    """
    reader = spark.read.format("bigquery")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load()


def read_gcs(spark: SparkSession, options: dict[str, Any]) -> DataFrame:
    """Read files from Cloud Storage (parquet, avro, orc, json, csv).

    Defaults to parquet: it is columnar, carries its schema, and splits cleanly. Schema inference on
    JSON/CSV is allowed but costs a full extra pass over the data, so a declared `schema` is
    preferred for anything that runs on a schedule.
    """
    file_format = options.get("format", "parquet")
    path = options["path"]
    reader = spark.read.format(file_format)
    for key, value in options.items():
        if key not in {"format", "path", "schema"}:
            reader = reader.option(key, value)
    if "schema" in options:
        reader = reader.schema(options["schema"])
    return reader.load(path)


def read_jdbc(spark: SparkSession, options: dict[str, Any]) -> DataFrame:
    """Read a relational source over JDBC.

    Note `numPartitions` / `partitionColumn` / `lowerBound` / `upperBound`: without them JDBC reads
    run on a SINGLE executor regardless of cluster size, which is the usual reason a "slow Spark
    job" is actually a slow single-threaded database read. Credentials come from Secret Manager via
    the launcher, never from this config.
    """
    reader = spark.read.format("jdbc")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load()


READERS: dict[str, Callable[[SparkSession, dict[str, Any]], DataFrame]] = {
    "bigquery": read_bigquery,
    "gcs": read_gcs,
    "jdbc": read_jdbc,
}
