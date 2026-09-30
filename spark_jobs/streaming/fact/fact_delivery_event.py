"""fact_delivery_event (streaming): Kafka raw.delivery_events -> dwh.fact_delivery_event_streaming  (merge on event_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/dwh/fact/fact_delivery_event.py) instead of imported, so streaming doesn't depend on
the batch code. Writes dwh.fact_delivery_event_streaming (not the batch table); the view
dwh.v_fact_delivery_event unions both, so their columns must stay identical.
"""
from pyspark.sql import functions as F

from streaming.helpers import date_key, latest_per_key, surrogate_key
from streaming.stream_common import start_fact

NAME = "fact_delivery_event_streaming"   # also the checkpoint folder name
TABLE = "fact_delivery_event_streaming"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_delivery_event_streaming
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
-- speed layer only: batch takes over each day, so rows expire after 72 h
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600   -- check for expired rows at least hourly
"""


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


def start(spark):
    return start_fact(spark, NAME, "delivery_events", TABLE, DDL, transform)
