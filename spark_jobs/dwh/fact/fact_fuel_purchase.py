"""fact_fuel_purchase: default.fuel_purchases -> dwh.fact_fuel_purchase  (merge on fuel_purchase_id)

One row per fuel purchase. trip_key = surrogate_key(trip_id), i.e. the same value as
fact_trip.trip_key, so the two facts join without a lookup.
transform() is shared with the streaming job (spark_jobs/streaming/fact/fact_fuel_purchase.py).

    spark-submit --py-files common.py fact/fact_fuel_purchase.py --job-date 2024-03-15
"""
from pyspark.sql import functions as F

from common import (
    ensure_table, get_spark, latest_per_key, money, parse_window, read_created_between,
    surrogate_key, date_key, with_audit, write_append,
)

TABLE = "fact_fuel_purchase"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_fuel_purchase
(
    fuel_purchase_key   Int64,
    fuel_purchase_id    String,
    date_key            Int32 COMMENT 'yyyyMMdd of purchase_datetime',
    driver_key          Int64,
    truck_key           Int64,
    trip_key            Int64 COMMENT '= fact_trip.trip_key',
    trip_id             String,
    purchase_datetime   DateTime('UTC'),
    purchase_city       LowCardinality(String),
    purchase_state      LowCardinality(String) COMMENT 'DQ: often inconsistent with city in the source',
    fuel_card_number    String,
    gallons             Float64,
    price_per_gallon    Decimal(10, 3) COMMENT 'do not AVG; use SUM(total_cost) / SUM(gallons)',
    total_cost          Decimal(18, 2),
    src_sys_create_date DateTime('UTC') COMMENT 'version: sys_create_date of the source row',
    etl_loaded_date     DateTime('UTC') COMMENT 'when the ETL job wrote this row'
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY fuel_purchase_id
"""

SOURCE_COLUMNS = [
    "fuel_purchase_id", "trip_id", "truck_id", "driver_id", "purchase_date", "location_city",
    "location_state", "gallons", "price_per_gallon", "total_cost", "fuel_card_number", "sys_create_date",
]


def transform(raw):
    """raw: rows shaped like default.fuel_purchases (incl. sys_create_date)."""
    rows = latest_per_key(raw, ["fuel_purchase_id"])
    return rows.select(
        surrogate_key(F.col("fuel_purchase_id")).alias("fuel_purchase_key"),
        "fuel_purchase_id",
        date_key("purchase_date").alias("date_key"),
        surrogate_key(F.col("driver_id")).alias("driver_key"),
        surrogate_key(F.col("truck_id")).alias("truck_key"),
        surrogate_key(F.col("trip_id")).alias("trip_key"),
        "trip_id",
        F.col("purchase_date").alias("purchase_datetime"),
        F.col("location_city").alias("purchase_city"),
        F.col("location_state").alias("purchase_state"),
        "fuel_card_number",
        "gallons",
        money("price_per_gallon", scale=3, precision=10).alias("price_per_gallon"),
        money("total_cost").alias("total_cost"),
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)

    raw = read_created_between(spark, "fuel_purchases", SOURCE_COLUMNS, start, end).cache()
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
