"""fact_fuel_purchase (streaming): Kafka raw.fuel_purchases -> dwh.fact_fuel_purchase_streaming  (merge on fuel_purchase_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/batch/facts/fact_fuel_purchase.py) instead of imported, so streaming doesn't depend on
the batch code. Only the table name differs (fact_fuel_purchase_streaming, 72 h TTL). The view
dwh.v_fact_fuel_purchase unions both tables, so keep the columns identical to the batch job.
"""

from pyspark.sql import functions as F

from streaming.helpers import latest_per_key, surrogate_key
from streaming.stream_common import start_fact

NAME = "fact_fuel_purchase_streaming"  # also the checkpoint folder name
TABLE = "fact_fuel_purchase_streaming"

DDL = """
    CREATE TABLE IF NOT EXISTS {db}.fact_fuel_purchase_streaming 
    (
        fuel_purchase_key Int64,
        fuel_purchase_id String,
        date_key Int32,
        driver_key Int64,
        truck_key Int64,
        trip_key Int64,
        purchase_location String,
        gallons Nullable(Decimal(18,3)),
        price_per_gallon Nullable(Decimal(18,3)),
        total_cost Nullable(Decimal(18,2)),
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    )
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY fuel_purchase_id
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600
    """


def transform(raw):
    return latest_per_key(raw, ["fuel_purchase_id"]).select(
        surrogate_key(F.col("fuel_purchase_id")).alias("fuel_purchase_key"),
        "fuel_purchase_id",
        F.date_format("purchase_date", "yyyyMMdd").cast("int").alias("date_key"),
        surrogate_key(F.col("driver_id")).alias("driver_key"),
        surrogate_key(F.col("truck_id")).alias("truck_key"),
        surrogate_key(F.col("trip_id")).alias("trip_key"),
        F.concat_ws(", ", "location_city", "location_state").alias("purchase_location"),
        F.col("gallons").cast("decimal(18,3)").alias("gallons"),
        F.col("price_per_gallon").cast("decimal(18,3)").alias("price_per_gallon"),
        F.round("total_cost", 2).cast("decimal(18,2)").alias("total_cost"),
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def start(spark):
    return start_fact(spark, NAME, "fuel_purchases", TABLE, DDL, transform)
