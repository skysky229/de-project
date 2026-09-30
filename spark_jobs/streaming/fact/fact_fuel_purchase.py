"""fact_fuel_purchase (streaming): Kafka raw.fuel_purchases -> dwh.fact_fuel_purchase_streaming  (merge on fuel_purchase_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/dwh/fact/fact_fuel_purchase.py) instead of imported, so streaming doesn't depend on
the batch code. Writes dwh.fact_fuel_purchase_streaming (not the batch table); the view
dwh.v_fact_fuel_purchase unions both, so their columns must stay identical.
"""
from pyspark.sql import functions as F

from streaming.helpers import date_key, latest_per_key, money, surrogate_key
from streaming.stream_common import start_fact

NAME = "fact_fuel_purchase_streaming"   # also the checkpoint folder name
TABLE = "fact_fuel_purchase_streaming"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_fuel_purchase_streaming
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
-- speed layer only: batch takes over each day, so rows expire after 72 h
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600   -- check for expired rows at least hourly
"""


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


def start(spark):
    return start_fact(spark, NAME, "fuel_purchases", TABLE, DDL, transform)
