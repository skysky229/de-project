"""fact_safety_incident: default.safety_incidents -> dwh.fact_safety_incident  (merge on incident_id)

One row per safety incident. trip_key = surrogate_key(trip_id) = fact_trip.trip_key.
Flags are 0/1 so SUM(injury_flag) = injury count. The source's single damage figure is
kept as its two parts (vehicle, cargo) plus the claim amount.
transform() is shared with the streaming job (spark_jobs/streaming/fact/fact_safety_incident.py).

    spark-submit --py-files common.py fact/fact_safety_incident.py --job-date 2024-03-15
"""
from pyspark.sql import functions as F

from common import (
    date_key, ensure_table, get_spark, latest_per_key, money, parse_window, read_created_between,
    surrogate_key, with_audit, write_append,
)

TABLE = "fact_safety_incident"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_safety_incident
(
    incident_key        Int64,
    incident_id         String,
    date_key            Int32 COMMENT 'yyyyMMdd of incident_datetime',
    driver_key          Int64,
    truck_key           Int64,
    trip_key            Int64 COMMENT '= fact_trip.trip_key',
    trip_id             String,
    incident_datetime   DateTime('UTC'),
    incident_type       LowCardinality(String),
    incident_city       LowCardinality(String),
    incident_state      LowCardinality(String),
    at_fault_flag       UInt8,
    injury_flag         UInt8,
    preventable_flag    UInt8,
    vehicle_damage_cost Decimal(18, 2),
    cargo_damage_cost   Decimal(18, 2),
    claim_cost          Decimal(18, 2),
    description         String,
    src_sys_create_date DateTime('UTC') COMMENT 'version: sys_create_date of the source row',
    etl_loaded_date     DateTime('UTC') COMMENT 'when the ETL job wrote this row'
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY incident_id
"""

SOURCE_COLUMNS = [
    "incident_id", "trip_id", "truck_id", "driver_id", "incident_date", "incident_type",
    "location_city", "location_state", "at_fault_flag", "injury_flag", "vehicle_damage_cost",
    "cargo_damage_cost", "claim_amount", "preventable_flag", "description", "sys_create_date",
]


def flag(col_name):
    return (F.col(col_name) == "True").cast("int")


def transform(raw):
    """raw: rows shaped like default.safety_incidents (incl. sys_create_date)."""
    rows = latest_per_key(raw, ["incident_id"])
    return rows.select(
        surrogate_key(F.col("incident_id")).alias("incident_key"),
        "incident_id",
        date_key("incident_date").alias("date_key"),
        surrogate_key(F.col("driver_id")).alias("driver_key"),
        surrogate_key(F.col("truck_id")).alias("truck_key"),
        surrogate_key(F.col("trip_id")).alias("trip_key"),
        "trip_id",
        F.col("incident_date").alias("incident_datetime"),
        "incident_type",
        F.col("location_city").alias("incident_city"),
        F.col("location_state").alias("incident_state"),
        flag("at_fault_flag").alias("at_fault_flag"),
        flag("injury_flag").alias("injury_flag"),
        flag("preventable_flag").alias("preventable_flag"),
        money("vehicle_damage_cost").alias("vehicle_damage_cost"),
        money("cargo_damage_cost").alias("cargo_damage_cost"),
        money("claim_amount").alias("claim_cost"),
        "description",
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)

    raw = read_created_between(spark, "safety_incidents", SOURCE_COLUMNS, start, end).cache()
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
