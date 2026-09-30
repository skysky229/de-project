"""fact_delivery_event: default.delivery_events -> dwh.fact_delivery_event  (merge on event_id)

One row per pickup/delivery event. Keys come only from the event's own row
(facility); trip context (driver, customer, route, ...) is reached by joining
fact_trip on trip_id at query time.

transform() is shared with the streaming job (spark_jobs/streaming/fact/
fact_delivery_event.py), so batch and streaming produce identical rows.

    spark-submit --py-files common.py fact/fact_delivery_event.py --job-date 2024-03-15
"""
from pyspark.sql import functions as F

from common import (
    date_key, ensure_table, get_spark, latest_per_key, parse_window, read_created_between,
    surrogate_key, with_audit, write_append,
)

TABLE = "fact_delivery_event"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_delivery_event
(
    event_key           Int64,
    event_id            String,
    trip_id             String COMMENT 'join to fact_trip for driver/truck/customer/route',
    load_id             String,
    facility_key        Int64,
    event_type          LowCardinality(String),
    scheduled_date_key  Int32 COMMENT 'yyyyMMdd',
    actual_date_key     Int32 COMMENT 'yyyyMMdd',
    scheduled_datetime  DateTime64(3, 'UTC'),
    actual_datetime     DateTime64(3, 'UTC'),
    detention_minutes   Int32,
    delay_minutes       Int32 COMMENT 'actual - scheduled; negative = early',
    is_on_time          UInt8,
    src_sys_create_date DateTime('UTC') COMMENT 'version: sys_create_date of the source row',
    etl_loaded_date     DateTime('UTC') COMMENT 'when the ETL job wrote this row'
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY event_id
"""

SOURCE_COLUMNS = [
    "event_id", "load_id", "trip_id", "event_type", "facility_id", "scheduled_datetime",
    "actual_datetime", "detention_minutes", "on_time_flag", "sys_create_date",
]


def transform(raw):
    """raw: rows shaped like default.delivery_events (incl. sys_create_date)."""
    events = latest_per_key(raw, ["event_id"])
    return events.select(
        surrogate_key(F.col("event_id")).alias("event_key"),
        "event_id",
        "trip_id",
        "load_id",
        surrogate_key(F.col("facility_id")).alias("facility_key"),
        "event_type",
        date_key("scheduled_datetime").alias("scheduled_date_key"),
        date_key("actual_datetime").alias("actual_date_key"),
        "scheduled_datetime",
        "actual_datetime",
        F.col("detention_minutes").cast("int").alias("detention_minutes"),
        ((F.unix_timestamp("actual_datetime") - F.unix_timestamp("scheduled_datetime")) / 60)
            .cast("int").alias("delay_minutes"),
        (F.col("on_time_flag") == "True").cast("int").alias("is_on_time"),
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)

    raw = read_created_between(spark, "delivery_events", SOURCE_COLUMNS, start, end).cache()
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
