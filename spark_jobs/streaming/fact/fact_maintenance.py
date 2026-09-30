"""fact_maintenance (streaming): Kafka raw.maintenance_records -> dwh.fact_maintenance_streaming  (merge on maintenance_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/dwh/fact/fact_maintenance.py) instead of imported, so streaming doesn't depend on
the batch code. Writes dwh.fact_maintenance_streaming (not the batch table); the view
dwh.v_fact_maintenance unions both, so their columns must stay identical.
"""
from pyspark.sql import functions as F

from streaming.helpers import date_key, latest_per_key, money, surrogate_key
from streaming.stream_common import start_fact

NAME = "fact_maintenance_streaming"   # also the checkpoint folder name
TABLE = "fact_maintenance_streaming"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_maintenance_streaming
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
-- speed layer only: batch takes over each day, so rows expire after 72 h
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600   -- check for expired rows at least hourly
"""


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


def start(spark):
    return start_fact(spark, NAME, "maintenance_records", TABLE, DDL, transform)
