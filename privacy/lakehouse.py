"""Erasure for Iceberg tables on object storage (S3, GCS).

Parquet files are immutable, so no row is ever deleted in place. Removing one means writing a new
file without it and then getting rid of the old file, and a table format keeps the old file around
on purpose, for time travel. So the steps below are the minimum that physically removes a person, in
the order that works, found by running them rather than from the docs:

1. DELETE                          queries stop returning the row. Under merge-on-read this only
                                   writes a delete file; the row is still in the data file. Under
                                   copy-on-write the old file is still referenced by the previous
                                   snapshot. Either way the bytes are still on storage.
2. rewrite_data_files              writes a new file without the row.
3. rewrite_position_delete_files   drops delete files that now point at rewritten data. Neither
                                   remove-dangling-deletes nor use-starting-sequence-number=false
                                   cleared them in testing; this procedure did.
4. expire_snapshots                deletes the files only old snapshots used, which is the step
                                   that removes the bytes. Tags and branches pin snapshots and are
                                   not expired by it, so an erasure-bearing table should not carry
                                   long-lived tags.

remove_orphan_files is the fifth step but not part of each request: it catches files written by
failed jobs that were never committed, and the Spark procedure refuses a window under 24 hours.
Run it on a schedule and count its delay against the deadline.

Timestamps passed to these procedures are read in the Spark session's time zone. Pass a UTC time to
a session on local time in summer and the cutoff lands an hour in the past, so nothing is expired
and the call still reports success. The cutoff here is converted into the session's own zone first.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import re
from zoneinfo import ZoneInfo

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclasses.dataclass
class LakehouseErasure:
    table: str
    rows_visible_before: int
    rewritten_data_files: int
    removed_delete_files: int
    expired_data_files: int


def _literal(value: str) -> str:
    """A SQL string literal. The procedures take their filter as text, so it has to be built."""
    return "'" + value.replace("'", "''") + "'"


def _check_identifier(name: str) -> str:
    if not IDENTIFIER.match(name):
        raise ValueError(f"not a plain column name: {name!r}")
    return name


def erase(spark, catalog: str, table: str, key_column: str, subject_id: str,
          expire_before: dt.datetime | None = None) -> LakehouseErasure:
    """Remove one subject from one Iceberg table, physically.

    `expire_before` defaults to now, which is right for a test and for a table nobody time-travels
    on. In production pick it deliberately: expiring up to the present breaks any reader still on an
    older snapshot, so a nightly job usually expires anything older than a few hours.
    """
    column = _check_identifier(key_column)
    predicate = f"{column} = {_literal(subject_id)}"
    qualified = f"{catalog}.{table}"

    visible = spark.sql(f"select count(*) from {qualified} where {predicate}").first()[0]
    spark.sql(f"delete from {qualified} where {predicate}")

    rewrite = spark.sql(
        f"call {catalog}.system.rewrite_data_files("
        f"table => '{table}', where => \"{predicate}\", "
        "options => map('rewrite-all', 'true'))"
    ).first()

    deletes = spark.sql(
        f"call {catalog}.system.rewrite_position_delete_files("
        f"table => '{table}', options => map('rewrite-all', 'true'))"
    ).first()

    cutoff = (expire_before or dt.datetime.now(dt.UTC)).astimezone(
        ZoneInfo(spark.conf.get("spark.sql.session.timeZone"))
    )
    expired = spark.sql(
        f"call {catalog}.system.expire_snapshots("
        f"table => '{table}', older_than => TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S.%f}', "
        "retain_last => 1)"
    ).first()

    return LakehouseErasure(
        table=qualified,
        rows_visible_before=int(visible),
        rewritten_data_files=int(rewrite["rewritten_data_files_count"]),
        removed_delete_files=int(deletes["rewritten_delete_files_count"]),
        expired_data_files=int(expired["deleted_data_files_count"]),
    )


def physically_present(spark, table_location: str, key_column: str, value: str) -> int:
    """Rows for `value` in the data files actually on storage, whatever the table metadata says.

    This is the check that matters, because every Iceberg metadata query answers from the current
    snapshot and so agrees the row is gone the moment DELETE commits. It lists the files through
    Hadoop's FileSystem, so the same code reads s3a://, gs:// or a local path. Delete files are
    Parquet too, with a different schema, and are skipped.

    It reads every file under the table, so scope it by partition on a real table.
    """
    column = _check_identifier(key_column)
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{table_location.rstrip('/')}/data")
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return 0

    hits = 0
    files = fs.listFiles(path, True)
    while files.hasNext():
        name = files.next().getPath().toString()
        if not name.endswith(".parquet"):
            continue
        frame = spark.read.parquet(name)
        if column in frame.columns:
            hits += frame.filter(frame[column] == value).count()
    return hits
