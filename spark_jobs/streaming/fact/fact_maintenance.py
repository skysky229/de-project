"""fact_maintenance (streaming): Kafka raw.maintenance_records -> dwh.fact_maintenance_streaming  (merge on maintenance_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/batch/facts/fact_maintenance.py) instead of imported, so streaming doesn't depend on
the batch code. Only the table name differs (fact_maintenance_streaming, 72 h TTL). The view
dwh.v_fact_maintenance unions both tables, so keep the columns identical to the batch job.
"""

from pyspark.sql import functions as F

from streaming.helpers import latest_per_key, surrogate_key
from streaming.stream_common import start_fact

NAME = "fact_maintenance_streaming"  # also the checkpoint folder name
TABLE = "fact_maintenance_streaming"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_maintenance_streaming
(
    maintenance_key Int64,
    maintenance_id String,
    date_key Int32,
    truck_key Int64,
    maintenance_type LowCardinality(String),
    maintenance_date Date,
    odometer_reading Nullable(Int64),
    labor_hours Nullable(Decimal(18,2)),
    labor_cost Nullable(Decimal(18,2)),
    parts_cost Nullable(Decimal(18,2)),
    total_cost Nullable(Decimal(18,2)),
    downtime_hours Nullable(Decimal(18,2)),
    facility_location String,
    src_sys_create_date DateTime('UTC'),
    etl_loaded_date DateTime('UTC')
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY maintenance_id
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600
"""


def transform(raw):
    return latest_per_key(raw, ["maintenance_id"]).select(
        surrogate_key(F.col("maintenance_id")).alias("maintenance_key"),
        "maintenance_id",
        F.date_format("maintenance_date", "yyyyMMdd").cast("int").alias("date_key"),
        surrogate_key(F.col("truck_id")).alias("truck_key"),
        "maintenance_type",
        F.to_date("maintenance_date").alias("maintenance_date"),
        F.col("odometer_reading").cast("bigint").alias("odometer_reading"),
        F.col("labor_hours").cast("decimal(18,2)").alias("labor_hours"),
        F.round("labor_cost", 2).cast("decimal(18,2)").alias("labor_cost"),
        F.round("parts_cost", 2).cast("decimal(18,2)").alias("parts_cost"),
        F.round("total_cost", 2).cast("decimal(18,2)").alias("total_cost"),
        F.col("downtime_hours").cast("decimal(18,2)").alias("downtime_hours"),
        "facility_location",
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def start(spark):
    return start_fact(spark, NAME, "maintenance_records", TABLE, DDL, transform)
