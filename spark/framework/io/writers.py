"""Sink connectors. One function per format, selected by the job spec's `format`."""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import DataFrame


def write_bigquery(df: DataFrame, mode: str, partition_by: list[str], options: dict[str, Any]) -> None:
    """Write to BigQuery.

    `writeMethod` is the decision that matters:

      direct   -- the BigQuery Storage Write API. No staging bucket, lower latency, and the write is
                  atomic per stream. The default here.
      indirect -- Spark writes Avro/Parquet to GCS, then triggers a BigQuery load job. Needs a
                  `temporaryGcsBucket`, costs an extra hop, but is the better choice for very large
                  writes (load jobs are free, Storage Write API throughput is billed) and is the
                  only option for some column types.

    Partitioning is expressed to BigQuery (`partitionField`), not as a Spark `partitionBy`: the
    target is a BigQuery table, so the physical layout is BigQuery's to manage. Writing
    Spark-partitioned directories into a BigQuery sink is a common and expensive confusion.
    """
    writer = df.write.format("bigquery").mode(mode)
    writer = writer.option("writeMethod", options.get("writeMethod", "direct"))
    for key, value in options.items():
        if key != "writeMethod":
            writer = writer.option(key, value)
    if partition_by:
        if len(partition_by) > 1:
            raise ValueError(
                f"BigQuery supports a single partition column, got {partition_by}. "
                "Use clustering for the remaining columns."
            )
        writer = writer.option("partitionField", partition_by[0])
    writer.save()


def write_gcs(df: DataFrame, mode: str, partition_by: list[str], options: dict[str, Any]) -> None:
    """Write files to Cloud Storage.

    `partition_by` here IS Spark's directory partitioning, which is what downstream readers use for
    pruning. Choose low-cardinality columns: partitioning by something like client_id produces
    millions of tiny files and turns every subsequent read into a metadata storm.
    """
    file_format = options.get("format", "parquet")
    path = options["path"]
    writer = df.write.format(file_format).mode(mode)
    for key, value in options.items():
        if key not in {"format", "path"}:
            writer = writer.option(key, value)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_firestore(df: DataFrame, mode: str, partition_by: list[str], options: dict[str, Any]) -> None:
    """Write documents to Firestore, for low-latency serving of pipeline output.

    Firestore is an operational store with per-document write costs and quotas, not a warehouse
    sink. `foreachPartition` with a batched client is used rather than a row-at-a-time write, and
    the DataFrame is repartitioned first to cap write concurrency -- an unbounded Spark job will
    happily exceed Firestore's write quota and start failing the whole batch.
    """
    collection = options["collection"]
    key_field = options["key_field"]
    max_writers = int(options.get("max_writers", 8))
    batch_size = int(options.get("batch_size", 400))  # Firestore caps a batch at 500

    project = options.get("project")

    def _write_partition(rows):
        from google.cloud import firestore

        client = firestore.Client(project=project)
        batch = client.batch()
        pending = 0
        for row in rows:
            record = row.asDict(recursive=True)
            doc_id = str(record[key_field])
            batch.set(client.collection(collection).document(doc_id), record)
            pending += 1
            if pending >= batch_size:
                batch.commit()
                batch = client.batch()
                pending = 0
        if pending:
            batch.commit()

    df.repartition(max_writers).foreachPartition(_write_partition)


WRITERS: dict[str, Callable[[DataFrame, str, list[str], dict[str, Any]], None]] = {
    "bigquery": write_bigquery,
    "gcs": write_gcs,
    "firestore": write_firestore,
}
