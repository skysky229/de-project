"""fact_delivery_event (streaming): Kafka raw.delivery_events -> dwh.fact_delivery_event_streaming  (merge on event_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/batch/facts/fact_delivery_event.py) instead of imported, so streaming doesn't depend on
the batch code. Only the table name differs (fact_delivery_event_streaming, 72 h TTL). The view
dwh.v_fact_delivery_event unions both tables, so keep the columns identical to the batch job.
"""

from pyspark.sql import functions as F

from streaming.helpers import latest_per_key, surrogate_key
from streaming.stream_common import start_fact

NAME = "fact_delivery_event_streaming"  # also the checkpoint folder name
TABLE = "fact_delivery_event_streaming"

DDL = """
    CREATE TABLE IF NOT EXISTS {db}.fact_delivery_event_streaming
    (
        event_key Int64,
        event_id String,
        date_key Int32,
        scheduled_date_key Int32,
        actual_date_key Int32,
        facility_key Int64,
        trip_key Int64,
        load_id String,
        event_type LowCardinality(String),
        scheduled_datetime DateTime('UTC'),
        actual_datetime Nullable(DateTime('UTC')),
        detention_minutes Nullable(Int32),
        delay_minutes Nullable(Int32),
        is_on_time UInt8,
        event_count UInt8,
        on_time_count UInt8,
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    ) 
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY event_id
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600
    """


def transform(raw):
    rows = latest_per_key(raw, ["event_id"])
    actual = F.to_timestamp("actual_datetime")
    scheduled = F.to_timestamp("scheduled_datetime")
    on_time = F.coalesce(F.col("on_time_flag").cast("boolean"), F.lit(False))
    return rows.select(
        surrogate_key(F.col("event_id")).alias("event_key"),
        "event_id",
        F.date_format(actual, "yyyyMMdd").cast("int").alias("date_key"),
        F.date_format(scheduled, "yyyyMMdd").cast("int").alias("scheduled_date_key"),
        F.date_format(actual, "yyyyMMdd").cast("int").alias("actual_date_key"),
        surrogate_key(F.col("facility_id")).alias("facility_key"),
        surrogate_key(F.col("trip_id")).alias("trip_key"),
        "load_id",
        "event_type",
        scheduled.alias("scheduled_datetime"),
        actual.alias("actual_datetime"),
        F.col("detention_minutes").cast("int").alias("detention_minutes"),
        F.round((F.unix_timestamp(actual) - F.unix_timestamp(scheduled)) / 60)
        .cast("int")
        .alias("delay_minutes"),
        on_time.cast("byte").alias("is_on_time"),
        F.lit(1).cast("byte").alias("event_count"),
        F.when(on_time, 1).otherwise(0).cast("byte").alias("on_time_count"),
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def start(spark):
    return start_fact(spark, NAME, "delivery_events", TABLE, DDL, transform)
