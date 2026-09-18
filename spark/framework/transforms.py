"""Declarative transform steps.

A step is `{type: <name>, ...args}` in YAML. The registry below is the whole vocabulary available to
job authors, which is a deliberate constraint: a small set of composable operators keeps configs
reviewable, and the `sql` escape hatch covers everything else without letting arbitrary Python into
a config file. Config that can execute arbitrary code is just code with worse tooling.
"""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class TransformError(ValueError):
    """Raised when a step is misconfigured. Names the step type."""


def t_select(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Project columns. `columns` may be plain names or `expression as alias`."""
    columns = args.get("columns")
    if not columns:
        raise TransformError("select: 'columns' is required")
    return df.selectExpr(*columns)


def t_filter(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Keep rows matching a SQL boolean expression."""
    expression = args.get("expression")
    if not expression:
        raise TransformError("filter: 'expression' is required")
    return df.filter(expression)


def t_rename(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Rename columns from a {from: to} mapping."""
    mapping = args.get("columns") or {}
    for source, target in mapping.items():
        df = df.withColumnRenamed(source, target)
    return df


def t_cast(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Cast columns from a {column: type} mapping."""
    mapping = args.get("columns") or {}
    for column, target_type in mapping.items():
        df = df.withColumn(column, F.col(column).cast(target_type))
    return df


def t_with_columns(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Add derived columns from a {name: sql_expression} mapping."""
    mapping = args.get("columns") or {}
    for name, expression in mapping.items():
        df = df.withColumn(name, F.expr(expression))
    return df


def t_deduplicate(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Keep one row per key, choosing by an ordering column.

    This is the operator that earns a Spark framework its keep on Kafka-sourced data: topics deliver
    at-least-once and out of order, so "latest row per key by event time" is the single most common
    requirement in a Bronze-to-Silver step. Implemented as a window rather than `dropDuplicates`
    because `dropDuplicates` keeps an ARBITRARY row, which is almost never what anyone means.
    """
    keys = args.get("keys")
    order_by = args.get("order_by")
    if not keys or not order_by:
        raise TransformError("deduplicate: 'keys' and 'order_by' are required")
    descending = args.get("descending", True)

    from pyspark.sql.window import Window

    ordering = [F.col(c).desc() if descending else F.col(c).asc() for c in order_by]
    window = Window.partitionBy(*keys).orderBy(*ordering)
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def t_aggregate(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Group and aggregate. `aggregations` is a {alias: sql_expression} mapping."""
    group_by = args.get("group_by") or []
    aggregations = args.get("aggregations") or {}
    if not aggregations:
        raise TransformError("aggregate: 'aggregations' is required")
    exprs = [F.expr(expression).alias(alias) for alias, expression in aggregations.items()]
    return df.groupBy(*group_by).agg(*exprs) if group_by else df.agg(*exprs)


def t_join(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Join to another registered source by its temp-view name.

    `broadcast: true` forces a broadcast hash join. Worth setting explicitly for a small dimension:
    Spark's automatic broadcast depends on size estimates that are unreliable for BigQuery and JDBC
    sources, and a missed broadcast turns a cheap join into a full shuffle.
    """
    right_name = args.get("source")
    on = args.get("on")
    if not right_name or not on:
        raise TransformError("join: 'source' and 'on' are required")
    right = spark.table(right_name)
    if args.get("broadcast", False):
        right = F.broadcast(right)
    return df.join(right, on=on, how=args.get("how", "left"))


def t_sql(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Arbitrary Spark SQL over the registered sources, plus `this` for the current frame.

    The escape hatch. Anything the operators above cannot express goes here, in SQL, still reviewed
    as config rather than requiring a framework release.
    """
    query = args.get("query")
    if not query:
        raise TransformError("sql: 'query' is required")
    df.createOrReplaceTempView("this")
    return spark.sql(query)


def t_repartition(df: DataFrame, args: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Reshape partitioning before a wide step or a write.

    Explicit because the default (200 shuffle partitions) is wrong in both directions: far too many
    for a small job, far too few for a large one, and the resulting file sizes are what a downstream
    reader lives with. Adaptive Query Execution handles most cases; this is for when it does not.
    """
    if "columns" in args:
        return df.repartition(args.get("count", 200), *[F.col(c) for c in args["columns"]])
    if args.get("coalesce"):
        return df.coalesce(int(args["coalesce"]))
    return df.repartition(int(args.get("count", 200)))


TRANSFORMS: dict[str, Callable[[DataFrame, dict[str, Any], SparkSession], DataFrame]] = {
    "select": t_select,
    "filter": t_filter,
    "rename": t_rename,
    "cast": t_cast,
    "with_columns": t_with_columns,
    "deduplicate": t_deduplicate,
    "aggregate": t_aggregate,
    "join": t_join,
    "sql": t_sql,
    "repartition": t_repartition,
}
