"""ClickHouse + transform helpers for the streaming jobs.

Deliberately a copy of the parts of spark_jobs/batch/common.py that streaming needs, so
the streaming code has no dependency on the batch code (owned separately). Keep the
two in sync by hand: the views dwh.v_<fact> union batch and streaming rows, so keys
(xxhash64), money rounding and the version column must behave identically.

Config (env vars): DWH_CH_URL (https://<host>:8443), DWH_CH_USER, DWH_CH_PASSWORD,
DWH_TARGET_DB (dwh tables, default: dwh), DWH_RAW_HISTORY_ROOT (bootstrap raw history in
MinIO, default: s3a://raw/history; read for lookups such as routes). ClickHouse is the
warehouse only: the old raw tables in `default` are not read.
"""

import os

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

RAW_HISTORY_ROOT = os.environ.get("DWH_RAW_HISTORY_ROOT", "s3a://raw/history")
TARGET_DB = os.environ.get("DWH_TARGET_DB", "dwh")
UNKNOWN_KEY = -1

_JDBC_OPTIONS = {
    "url": "jdbc:clickhouse:" + os.environ["DWH_CH_URL"].rstrip("/"),
    "user": os.environ.get("DWH_CH_USER", "default"),
    "password": os.environ["DWH_CH_PASSWORD"],
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


# ---------------------------------------------------------------------- setup


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # source timestamps carry no zone; keep day boundaries in UTC
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )


# ----------------------------------------------------------------- ClickHouse


def execute(spark: SparkSession, sql: str) -> None:
    """Run one DDL statement on ClickHouse from the driver (no data returned)."""
    conn = spark.sparkContext._jvm.java.sql.DriverManager.getConnection(
        _JDBC_OPTIONS["url"], _JDBC_OPTIONS["user"], _JDBC_OPTIONS["password"]
    )
    try:
        conn.createStatement().execute(sql)
    finally:
        conn.close()


def ensure_table(spark: SparkSession, ddl: str) -> None:
    execute(spark, f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    execute(spark, ddl.format(db=TARGET_DB))


def read_raw_all(spark: SparkSession, table: str, columns: list) -> DataFrame:
    """All raw history rows of a (small, not streamed) table from MinIO, e.g. routes for lookups.
    Same name/signature as the batch helper, so copied batch code runs unchanged."""
    return spark.read.parquet(f"{RAW_HISTORY_ROOT}/{table}").select(*columns)


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
    """Keep one row per business key: the latest version in this micro-batch."""
    w = Window.partitionBy(*keys).orderBy(F.col(version).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def surrogate_key(natural_id: Column) -> Column:
    """Stable Int64 key = xxhash64(id); same id -> same key as the batch jobs. Empty/NULL id -> -1."""
    return F.when(natural_id.isNull() | (F.trim(natural_id) == ""), F.lit(UNKNOWN_KEY)).otherwise(
        F.xxhash64(natural_id)
    )


def date_key(col) -> Column:
    """yyyyMMdd as Int, e.g. 2024-03-15 -> 20240315 (date or timestamp input)."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def money(col_name: str, scale: int = 2, precision: int = 18) -> Column:
    """Float64 amount -> Decimal. Round first so 0.29 (stored as 0.28999..) stays 0.29."""
    return F.round(F.col(col_name), scale).cast(f"decimal({precision},{scale})")


def with_audit(df: DataFrame) -> DataFrame:
    """etl_loaded_date = when this job wrote the row (audit only; the merge version is src_sys_create_date)."""
    return df.withColumn("etl_loaded_date", F.current_timestamp())
