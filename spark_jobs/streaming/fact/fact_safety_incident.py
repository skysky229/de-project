"""fact_safety_incident (streaming): Kafka raw.safety_incidents -> dwh.fact_safety_incident_streaming  (merge on incident_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/dwh/fact/fact_safety_incident.py) instead of imported, so streaming doesn't depend on
the batch code. Writes dwh.fact_safety_incident_streaming (not the batch table); the view
dwh.v_fact_safety_incident unions both, so their columns must stay identical.
"""
from pyspark.sql import functions as F

from streaming.helpers import date_key, latest_per_key, money, surrogate_key
from streaming.stream_common import start_fact

NAME = "fact_safety_incident_streaming"   # also the checkpoint folder name
TABLE = "fact_safety_incident_streaming"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_safety_incident_streaming
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
-- speed layer only: batch takes over each day, so rows expire after 72 h
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600   -- check for expired rows at least hourly
"""


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


def start(spark):
    return start_fact(spark, NAME, "safety_incidents", TABLE, DDL, transform)
