"""fact_maintenance: default.maintenance_records -> dwh.fact_maintenance  (merge on maintenance_id)

One row per maintenance record (truck service visit).
transform() is shared with the streaming job (spark_jobs/streaming/fact/fact_maintenance.py).

    spark-submit --py-files common.py fact/fact_maintenance.py --job-date 2024-03-15
"""
from pyspark.sql import functions as F

from common import (
    date_key, ensure_table, get_spark, latest_per_key, money, parse_window, read_created_between,
    surrogate_key, with_audit, write_append,
)

TABLE = "fact_maintenance"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_maintenance
(
    maintenance_key     Int64,
    maintenance_id      String,
    date_key            Int32 COMMENT 'yyyyMMdd of maintenance_date',
    truck_key           Int64,
    maintenance_type    LowCardinality(String),
    odometer            Int64 COMMENT 'non-additive: do not SUM',
    labor_hours         Float64,
    labor_cost          Decimal(18, 2),
    parts_cost          Decimal(18, 2),
    total_cost          Decimal(18, 2),
    downtime_hours      Float64,
    facility_location   LowCardinality(String),
    service_description String,
    src_sys_create_date DateTime('UTC') COMMENT 'version: sys_create_date of the source row',
    etl_loaded_date     DateTime('UTC') COMMENT 'when the ETL job wrote this row'
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY maintenance_id
"""

SOURCE_COLUMNS = [
    "maintenance_id", "truck_id", "maintenance_date", "maintenance_type", "odometer_reading",
    "labor_hours", "labor_cost", "parts_cost", "total_cost", "facility_location", "downtime_hours",
    "service_description", "sys_create_date",
]


def transform(raw):
    """raw: rows shaped like default.maintenance_records (incl. sys_create_date)."""
    rows = latest_per_key(raw, ["maintenance_id"])
    return rows.select(
        surrogate_key(F.col("maintenance_id")).alias("maintenance_key"),
        "maintenance_id",
        date_key("maintenance_date").alias("date_key"),
        surrogate_key(F.col("truck_id")).alias("truck_key"),
        "maintenance_type",
        F.col("odometer_reading").alias("odometer"),
        "labor_hours",
        money("labor_cost").alias("labor_cost"),
        money("parts_cost").alias("parts_cost"),
        money("total_cost").alias("total_cost"),
        "downtime_hours",
        "facility_location",
        "service_description",
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)

    raw = read_created_between(spark, "maintenance_records", SOURCE_COLUMNS, start, end).cache()
    raw_count = raw.count()
    if raw_count == 0:
        print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
    else:
        out = with_audit(transform(raw)).cache()
        write_append(out, TABLE)
        print(f"[{TABLE}] window {start}..{end}: read {raw_count} raw rows, wrote {out.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
