"""Erasure from Iceberg tables, checked against the files on disk, not the table's own answer.

Every query through Iceberg agrees a row is gone as soon as DELETE commits, which is exactly why
those queries can't be the test. These read the Parquet files directly.

Needs pyspark and downloads the Iceberg runtime on first run; skipped when either isn't available.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from privacy.lakehouse import erase, physically_present  # noqa: E402

ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"
ROWS = [
    ("cli-1", "a@example.com", "GB"),
    ("cli-42", "x@example.com", "GB"),
    ("cli-3", "c@example.com", "DE"),
]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("lake")
    try:
        session = (
            pyspark.sql.SparkSession.builder.master("local[2]")
            .appName("iceberg-erasure-tests")
            .config("spark.jars.packages", ICEBERG)
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", str(warehouse))
            # Not UTC, on purpose. A cutoff passed in UTC to a session on London time lands an hour
            # early in summer and expires nothing, so the suite runs where that would bite.
            .config("spark.sql.session.timeZone", "Europe/London")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 - no network for the jar, or no Java
        pytest.skip(f"Spark with Iceberg unavailable: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    session.warehouse = str(warehouse)
    yield session
    session.stop()


def make_table(spark, name: str, delete_mode: str, extra: str = "") -> str:
    spark.sql(f"drop table if exists lake.crm.{name}")
    spark.sql(f"""
        create table lake.crm.{name} (client_id string, email string, country string) using iceberg
        tblproperties ('format-version' = '2', 'write.delete.mode' = '{delete_mode}' {extra})
    """)
    frame = spark.createDataFrame(ROWS, ["client_id", "email", "country"])
    frame.writeTo(f"lake.crm.{name}").append()
    return f"{spark.warehouse}/crm/{name}"


def visible(spark, name: str, subject: str = "cli-42") -> int:
    sql = f"select count(*) from lake.crm.{name} where client_id = '{subject}'"
    return spark.sql(sql).first()[0]


@pytest.mark.parametrize("mode", ["merge-on-read", "copy-on-write"])
def test_delete_alone_leaves_the_row_on_disk(spark, mode):
    """The trap. The table says the person is gone, and the bytes are still in a data file."""
    name = f"trap_{mode.replace('-', '_')}"
    location = make_table(spark, name, mode)

    spark.sql(f"delete from lake.crm.{name} where client_id = 'cli-42'")

    assert visible(spark, name) == 0
    assert physically_present(spark, location, "client_id", "cli-42") >= 1


@pytest.mark.parametrize("mode", ["merge-on-read", "copy-on-write"])
def test_full_procedure_removes_the_row_from_every_file(spark, mode):
    name = f"erase_{mode.replace('-', '_')}"
    location = make_table(spark, name, mode)

    result = erase(spark, "lake", f"crm.{name}", "client_id", "cli-42")

    assert result.rows_visible_before == 1
    assert result.expired_data_files >= 1
    assert physically_present(spark, location, "client_id", "cli-42") == 0
    remaining = sorted(r[0] for r in spark.sql(f"select client_id from lake.crm.{name}").collect())
    assert remaining == ["cli-1", "cli-3"]


def test_no_delete_files_are_left_behind(spark):
    """Delete files are cleared too. Only rewrite_position_delete_files did it."""
    name = "tidy"
    make_table(spark, name, "merge-on-read")
    erase(spark, "lake", f"crm.{name}", "client_id", "cli-42")
    assert spark.sql(f"select count(*) from lake.crm.{name}.delete_files").first()[0] == 0


def test_personal_columns_can_be_kept_out_of_manifest_stats(spark):
    """Manifests store each file's min and max per column, which can be a person's email.

    Setting the metrics mode to none for personal columns means there is nothing there to erase.
    """
    name = "no_stats"
    make_table(spark, name, "merge-on-read", ", 'write.metadata.metrics.column.email' = 'none'")

    bounds = spark.sql(f"""
        select readable_metrics.email.lower_bound as email_min,
               readable_metrics.country.lower_bound as country_min
        from lake.crm.{name}.files
    """).first()
    assert bounds["email_min"] is None
    assert bounds["country_min"] is not None


def test_orphan_cleanup_refuses_a_window_under_a_day(spark):
    """A hard floor to budget into the deadline, pinned so an upgrade that changes it shows up."""
    name = "orphans"
    make_table(spark, name, "merge-on-read")
    # Procedure arguments must be literals, so "now" is written out, not current_timestamp().
    now = spark.sql("select date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')").first()[0]
    with pytest.raises(Exception, match="less than 24 hours"):
        spark.sql(f"""
            call lake.system.remove_orphan_files(
                table => 'crm.{name}', older_than => TIMESTAMP '{now}')
        """).collect()


def test_a_quote_in_the_subject_id_is_escaped(spark):
    name = "quoted"
    make_table(spark, name, "merge-on-read")
    spark.sql(f"insert into lake.crm.{name} values ('o''brien', 'o@example.com', 'IE')")

    erase(spark, "lake", f"crm.{name}", "client_id", "o'brien")

    assert visible(spark, name, "o''brien") == 0
    assert visible(spark, name, "cli-42") == 1


def test_only_plain_column_names_are_accepted():
    with pytest.raises(ValueError):
        erase(None, "lake", "crm.t", "client_id; drop table x", "cli-1")
