"""dim_driver: default.drivers -> dwh.dim_driver  (SCD1, merge on driver_id)

Reads drivers recorded in [start, end] (raw table is append-only; a changed
driver arrives as a new row with the same driver_id), keeps the latest version
per driver_id, and appends. ReplacingMergeTree(src_sys_create_date) collapses rows
with the same driver_id to the latest source version, so re-runs never duplicate.

    spark-submit --py-files common.py dim/dim_driver.py --job-date 2024-03-15
    spark-submit --py-files common.py dim/dim_driver.py --start-date 2000-01-01 --end-date 2024-12-31
"""
from pyspark.sql import functions as F

from common import (
    UNKNOWN_KEY, empty_to_null, ensure_table, get_spark, latest_per_key, parse_window,
    read_created_between, seed_unknown_member, surrogate_key, with_audit, write_append,
)

TABLE = "dim_driver"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.dim_driver
(
    driver_key        Int64,
    driver_id         String,
    driver_name       String,
    date_of_birth     Nullable(Date32) COMMENT 'Date32: plain Date starts at 1970 and would clamp older birth dates',
    hire_date         Nullable(Date),
    termination_date  Nullable(Date),
    license_state     LowCardinality(String),
    experience_years  Nullable(Int32) COMMENT 'source: years_experience (inconsistent with hire_date)',
    home_terminal     LowCardinality(String),
    employment_status LowCardinality(String),
    cdl_class         LowCardinality(String),
    src_sys_create_date DateTime('UTC') COMMENT 'version: sys_create_date of the source row',
    etl_loaded_date     DateTime('UTC') COMMENT 'when the ETL job wrote this row'
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY driver_id
"""

SOURCE_COLUMNS = [
    "driver_id", "first_name", "last_name", "date_of_birth", "hire_date", "termination_date",
    "license_state", "years_experience", "home_terminal", "employment_status", "cdl_class",
    "sys_create_date",
]


def transform(raw):
    drivers = latest_per_key(raw, ["driver_id"])
    dim = drivers.select(
        surrogate_key(F.col("driver_id")).alias("driver_key"),
        "driver_id",
        F.concat_ws(" ", "first_name", "last_name").alias("driver_name"),
        F.to_date("date_of_birth").alias("date_of_birth"),
        "hire_date",
        # source stores "not terminated" as the epoch date
        F.when(F.col("termination_date") == F.lit("1970-01-01").cast("date"), None)
         .otherwise(F.col("termination_date")).alias("termination_date"),
        F.coalesce(empty_to_null(F.col("license_state")), F.lit("Unknown")).alias("license_state"),
        F.col("years_experience").cast("int").alias("experience_years"),
        F.coalesce(empty_to_null(F.col("home_terminal")), F.lit("Unknown")).alias("home_terminal"),
        F.coalesce(empty_to_null(F.col("employment_status")), F.lit("Unknown")).alias("employment_status"),
        F.coalesce(empty_to_null(F.col("cdl_class")), F.lit("Unknown")).alias("cdl_class"),
        F.col("sys_create_date").alias("src_sys_create_date"),
    )
    return dim


def unknown_member(spark):
    """Key -1 for facts whose driver_id is empty. Seeded once, see common.seed_unknown_member."""
    return spark.sql(f"""
        SELECT CAST({UNKNOWN_KEY} AS BIGINT) AS driver_key, 'UNKNOWN' AS driver_id,
               'Unknown' AS driver_name, CAST(NULL AS DATE) AS date_of_birth,
               CAST(NULL AS DATE) AS hire_date, CAST(NULL AS DATE) AS termination_date,
               'Unknown' AS license_state, CAST(NULL AS INT) AS experience_years,
               'Unknown' AS home_terminal, 'Unknown' AS employment_status, 'Unknown' AS cdl_class,
               CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS src_sys_create_date
    """)


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")

    ensure_table(spark, DDL)
    seed_unknown_member(spark, TABLE, "driver_key", unknown_member(spark))

    raw = read_created_between(spark, "drivers", SOURCE_COLUMNS, start, end).cache()
    raw_count = raw.count()
    if raw_count == 0:
        print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
    else:
        dim = with_audit(transform(raw)).cache()
        write_append(dim, TABLE)
        print(f"[{TABLE}] window {start}..{end}: read {raw_count} raw rows, wrote {dim.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
