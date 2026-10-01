"""fact_safety_incident (streaming): Kafka raw.safety_incidents -> dwh.fact_safety_incident_streaming  (merge on incident_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/batch/facts/fact_safety_incident.py) instead of imported, so streaming doesn't depend on
the batch code. Only the table name differs (fact_safety_incident_streaming, 72 h TTL). The view
dwh.v_fact_safety_incident unions both tables, so keep the columns identical to the batch job.
"""

from pyspark.sql import functions as F

from streaming.helpers import latest_per_key, surrogate_key
from streaming.stream_common import start_fact

NAME = "fact_safety_incident_streaming"  # also the checkpoint folder name
TABLE = "fact_safety_incident_streaming"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_safety_incident_streaming
(
    incident_key Int64,
    incident_id String,
    date_key Int32,
    driver_key Int64,
    truck_key Int64,
    trip_key Int64,
    incident_type LowCardinality(String),
    location String,
    at_fault_flag UInt8,
    injury_flag UInt8,
    vehicle_damage_cost Nullable(Decimal(18,2)),
    cargo_damage_cost Nullable(Decimal(18,2)),
    claim_amount Nullable(Decimal(18,2)),
    preventable_flag UInt8,
    incident_count UInt8,
    injury_count UInt8,
    preventable_count UInt8,
    src_sys_create_date DateTime('UTC'),
    etl_loaded_date DateTime('UTC')
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY incident_id
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600
"""


def transform(raw):
    r = latest_per_key(raw, ["incident_id"])
    at_fault = F.coalesce(F.col("at_fault_flag").cast("boolean"), F.lit(False))
    injury = F.coalesce(F.col("injury_flag").cast("boolean"), F.lit(False))
    preventable = F.coalesce(F.col("preventable_flag").cast("boolean"), F.lit(False))
    return r.select(
        surrogate_key(F.col("incident_id")).alias("incident_key"),
        "incident_id",
        F.date_format("incident_date", "yyyyMMdd").cast("int").alias("date_key"),
        surrogate_key(F.col("driver_id")).alias("driver_key"),
        surrogate_key(F.col("truck_id")).alias("truck_key"),
        surrogate_key(F.col("trip_id")).alias("trip_key"),
        "incident_type",
        F.concat_ws(", ", "location_city", "location_state").alias("location"),
        at_fault.cast("byte").alias("at_fault_flag"),
        injury.cast("byte").alias("injury_flag"),
        F.round("vehicle_damage_cost", 2).cast("decimal(18,2)").alias("vehicle_damage_cost"),
        F.round("cargo_damage_cost", 2).cast("decimal(18,2)").alias("cargo_damage_cost"),
        F.round("claim_amount", 2).cast("decimal(18,2)").alias("claim_amount"),
        preventable.cast("byte").alias("preventable_flag"),
        F.lit(1).cast("byte").alias("incident_count"),
        F.when(injury, 1).otherwise(0).cast("byte").alias("injury_count"),
        F.when(preventable, 1).otherwise(0).cast("byte").alias("preventable_count"),
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def start(spark):
    return start_fact(spark, NAME, "safety_incidents", TABLE, DDL, transform)
