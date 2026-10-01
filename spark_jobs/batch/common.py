"""Shared helpers for the logistics DWH Spark jobs (spark_jobs/batch/{dim,fact,mart}).

Every job follows the same shape, one target table per file:
    1. ensure its target table exists (CREATE TABLE IF NOT EXISTS, ReplacingMergeTree)
    2. extract: read raw rows created in [start, end] from MinIO (the partition_date folders of the window)
    3. transform in Spark
    4. load: append to ClickHouse; ReplacingMergeTree merges rows on the business key

Every target table carries two timestamps:
    src_sys_create_date  the raw row's sys_create_date; the ReplacingMergeTree version, so the
                         newest *source* row wins no matter which load ran last (safe re-runs)
    etl_loaded_date      when the job wrote the row (audit only)

Raw data lives in MinIO, partitioned by partition_date = toDate(sys_create_date):
    DWH_RAW_HISTORY_ROOT  s3a://raw/history/<table>/    one-time bootstrap from the CSVs, all 14 tables
    DWH_RAW_STREAM_ROOT   s3a://raw/logistics/<table>/  written by the streaming app (6 tables)
ClickHouse is the warehouse only (dwh); the old raw tables in `default` are no longer read.
All transformation happens in Spark.

Shipped to the cluster with spark-submit --py-files .../common.py.

Config (env vars on the driver; JDBC options travel to executors with the plan):
    DWH_CH_URL       https://<host>:8443
    DWH_CH_USER / DWH_CH_PASSWORD
    DWH_TARGET_DB    star schema (default: dwh)
    DWH_RAW_HISTORY_ROOT / DWH_RAW_STREAM_ROOT   see above
The s3a:// credentials come from spark.hadoop.fs.s3a.* (set by the DAG).
"""

import argparse
import os
from datetime import date

from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

TARGET_DB = os.environ.get("DWH_TARGET_DB", "dwh")
RAW_HISTORY_ROOT = os.environ.get("DWH_RAW_HISTORY_ROOT", "s3a://raw/history")
RAW_STREAM_ROOT = os.environ.get("DWH_RAW_STREAM_ROOT", "s3a://raw/logistics")
UNKNOWN_KEY = -1

_JDBC_URL = "jdbc:clickhouse:" + os.environ["DWH_CH_URL"].rstrip("/")
_JDBC_OPTIONS = {
    "url": _JDBC_URL,
    "user": os.environ.get("DWH_CH_USER", "default"),
    "password": os.environ["DWH_CH_PASSWORD"],
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


# ---------------------------------------------------------------------- setup


def parse_window() -> tuple:
    """--job-date D (daily) or --start-date/--end-date (backfill). Returns (start, end) ISO strings."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()
    start = args.start_date or args.job_date
    end = args.end_date or args.job_date
    if not (start and end):
        parser.error("pass --job-date, or --start-date and --end-date")
    # validates the format and keeps the value safe to embed in SQL
    start, end = date.fromisoformat(start).isoformat(), date.fromisoformat(end).isoformat()
    if start > end:
        parser.error(f"start {start} is after end {end}")
    return start, end


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # source timestamps carry no zone; keep day boundaries in UTC
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )


# ----------------------------------------------------------------- ClickHouse


def _connection(spark: SparkSession):
    jvm = spark.sparkContext._jvm
    return jvm.java.sql.DriverManager.getConnection(
        _JDBC_OPTIONS["url"], _JDBC_OPTIONS["user"], _JDBC_OPTIONS["password"]
    )


def execute(spark: SparkSession, sql: str) -> None:
    """Run one DDL/housekeeping statement on ClickHouse from the driver (no data returned)."""
    conn = _connection(spark)
    try:
        conn.createStatement().execute(sql)
    finally:
        conn.close()


def query_scalar(spark: SparkSession, sql: str):
    """Run a single-value query on the driver (e.g. an existence check); returns it as a str.

    getString, not getObject: UInt64 (e.g. count()) comes back as a Java BigInteger,
    which py4j can't convert to a Python int.
    """
    conn = _connection(spark)
    try:
        rs = conn.createStatement().executeQuery(sql)
        return rs.getString(1) if rs.next() else None
    finally:
        conn.close()


def ensure_table(spark: SparkSession, ddl: str) -> None:
    execute(spark, f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    execute(spark, ddl.format(db=TARGET_DB))


def seed_unknown_member(spark: SparkSession, table: str, key_column: str, row: DataFrame) -> None:
    """Insert the dimension's Unknown member (key -1) once; no-op if it already exists.

    It's a constant, so it's seeded on first run rather than re-appended by every run.
    """
    exists = query_scalar(
        spark, f"SELECT count() FROM {TARGET_DB}.{table} WHERE {key_column} = {UNKNOWN_KEY}"
    )
    if int(exists) == 0:
        write_append(with_audit(row), table)
        print(f"[{table}] seeded Unknown member ({key_column} = {UNKNOWN_KEY})")


def _raw_sources(spark: SparkSession, table: str) -> list:
    """The table's raw parquet in MinIO: the history folder, plus the streaming folder if the
    table is streamed. The streaming folder is read through its _spark_metadata log, so files
    of micro-batches that never committed are ignored."""
    frames = []
    for root in (RAW_HISTORY_ROOT, RAW_STREAM_ROOT):
        try:
            frames.append(spark.read.parquet(f"{root}/{table}"))
        except AnalysisException:  # folder doesn't exist (e.g. a table that isn't streamed)
            pass
    if not frames:
        raise FileNotFoundError(
            f"no raw data for '{table}' under {RAW_HISTORY_ROOT} or {RAW_STREAM_ROOT}: "
            f"run the logistics_bootstrap DAG first"
        )
    return frames


def read_created_between(
    spark: SparkSession, table: str, columns: list, start: str, end: str
) -> DataFrame:
    """Raw rows recorded in [start, end]: only the partition_date folders of that window are read.

    Raw data is append-only, so this is the window's delta. History and streamed rows are unioned.
    """
    in_window = F.col("partition_date").between(F.lit(start).cast("date"), F.lit(end).cast("date"))
    frames = [df.filter(in_window).select(*columns) for df in _raw_sources(spark, table)]
    return reduce(DataFrame.unionByName, frames)


def read_raw_all(spark: SparkSession, table: str, columns: list) -> DataFrame:
    """All raw rows of a (small) table, every partition: for lookups such as routes."""
    return reduce(DataFrame.unionByName, [df.select(*columns) for df in _raw_sources(spark, table)])


def read_query(spark: SparkSession, sql: str) -> DataFrame:
    """Read the result of a plain SELECT (projection + WHERE only; keep transforms in Spark).

    Use for anything that isn't a raw-table delta, e.g. a mart reading
    `SELECT ... FROM {TARGET_DB}.fact_trip FINAL WHERE date_key BETWEEN ...`.
    """
    return spark.read.format("jdbc").options(**_JDBC_OPTIONS).option("query", sql).load()


def write_append(df: DataFrame, table: str, database: str = TARGET_DB) -> None:
    """Batched JDBC append into <database>.<table> (the dwh by default)."""
    (
        df.write.format("jdbc")
        .options(**_JDBC_OPTIONS)
        .option("dbtable", f"{database}.{table}")
        .option("batchsize", 50000)
        # ClickHouse has no transactions; stop Spark from calling setTransactionIsolation/commit
        .option("isolationLevel", "NONE")
        .mode("append")
        .save()
    )


# ------------------------------------------------------------------ transform


def latest_per_key(df: DataFrame, keys: list, version: str = "sys_create_date") -> DataFrame:
    """Keep one row per business key: the latest version in this batch."""
    w = Window.partitionBy(*keys).orderBy(F.col(version).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def surrogate_key(natural_id: Column) -> Column:
    """Stable Int64 key = xxhash64(id); same id -> same key on every run. Empty/NULL id -> -1."""
    return F.when(natural_id.isNull() | (F.trim(natural_id) == ""), F.lit(UNKNOWN_KEY)).otherwise(
        F.xxhash64(natural_id)
    )


def date_key(col) -> Column:
    """yyyyMMdd as Int, e.g. 2024-03-15 -> 20240315 (date or timestamp input)."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def money(col_name: str, scale: int = 2, precision: int = 18) -> Column:
    """Float64 amount -> Decimal. Round first so 0.29 (stored as 0.28999..) stays 0.29."""
    return F.round(F.col(col_name), scale).cast(f"decimal({precision},{scale})")


def empty_to_null(col: Column) -> Column:
    return F.when(F.trim(col) == "", None).otherwise(col)


def with_audit(df: DataFrame) -> DataFrame:
    """etl_loaded_date = when this job wrote the row (audit only; the merge version is src_sys_create_date)."""
    return df.withColumn("etl_loaded_date", F.current_timestamp())
