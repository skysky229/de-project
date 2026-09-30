"""fact_trip (streaming): Kafka raw.trips ⋈ raw.loads -> dwh.fact_trip_streaming  (merge on trip_id)

The only stateful streaming query. A trip and its load arrive as separate messages,
possibly in different micro-batches, so Spark buffers each side until its partner
arrives (stream-stream join on load_id). The watermark + time bound tell Spark when
a buffered row can never be matched any more and may be dropped: here, when its
partner is more than 10 minutes apart (the generator publishes both at once).

Self-contained on purpose: DDL, columns and build() are copied from the batch job
(spark_jobs/dwh/fact/fact_trip.py) instead of imported, so streaming doesn't depend on
the batch code. Writes dwh.fact_trip_streaming (not the batch table); the view
dwh.v_fact_trip unions both, so their columns must stay identical.
"""
from pyspark.sql import functions as F

from streaming.helpers import (
    SOURCE_DB, date_key, ensure_table, latest_per_key, money, read_query, surrogate_key,
)
from streaming.stream_common import read_raw_topic, start_query, write_fact

NAME = "fact_trip_streaming"   # also the checkpoint folder name
MAX_GAP = "10 minutes"
TABLE = "fact_trip_streaming"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_trip_streaming
(
    trip_key              Int64,
    trip_id               String,
    load_id               String,
    date_key              Int32 COMMENT 'yyyyMMdd of dispatch_date',
    driver_key            Int64,
    truck_key             Int64,
    trailer_key           Int64,
    customer_key          Int64,
    route_key             Int64,
    booking_type          LowCardinality(String),
    load_type             LowCardinality(String),
    load_status           LowCardinality(String),
    weight                Int64 COMMENT 'lbs',
    pieces                Int64,
    planned_miles         Nullable(Int64) COMMENT 'route typical distance',
    actual_miles          Int64,
    trip_duration_hours   Float64,
    revenue               Decimal(18, 2),
    fuel_surcharge        Decimal(18, 2),
    accessorial_charges   Decimal(18, 2),
    total_revenue         Decimal(18, 2) COMMENT 'revenue + fuel_surcharge + accessorial_charges',
    fuel_consumed_gallons Float64,
    idle_hours            Float64,
    src_sys_create_date   DateTime('UTC') COMMENT 'version: newest sys_create_date of the trip/load rows',
    etl_loaded_date       DateTime('UTC') COMMENT 'when the ETL job wrote this row'
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY trip_id
-- speed layer only: batch takes over each day, so rows expire after 72 h
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600   -- check for expired rows at least hourly
"""

TRIP_COLUMNS = [
    "trip_id", "load_id", "driver_id", "truck_id", "trailer_id", "dispatch_date",
    "actual_distance_miles", "actual_duration_hours", "fuel_gallons_used", "idle_time_hours",
    "sys_create_date",
]
LOAD_COLUMNS = [
    "load_id", "customer_id", "route_id", "load_type", "weight_lbs", "pieces", "revenue",
    "fuel_surcharge", "accessorial_charges", "load_status", "booking_type", "sys_create_date",
]


def prefix_loads(loads):
    """Load columns as l_<name>, so they don't clash with trip columns after the join."""
    return loads.select([F.col(c).alias(f"l_{c}") for c in LOAD_COLUMNS])


def read_routes(spark):
    routes = read_query(spark, f"SELECT route_id, typical_distance_miles, sys_create_date FROM {SOURCE_DB}.routes")
    return latest_per_key(routes, ["route_id"]).select(
        F.col("route_id").alias("r_route_id"), F.col("typical_distance_miles").alias("planned_miles"))


def build(joined, routes):
    """joined: trip columns + l_<load columns>. Returns fact_trip rows (without etl_loaded_date)."""
    rows = joined.withColumn("src_sys_create_date", F.greatest("sys_create_date", "l_sys_create_date"))
    rows = latest_per_key(rows, ["trip_id"], version="src_sys_create_date")
    rows = rows.join(F.broadcast(routes), rows.l_route_id == routes.r_route_id, "left")
    revenue, surcharge, accessorial = money("l_revenue"), money("l_fuel_surcharge"), money("l_accessorial_charges")
    return rows.select(
        surrogate_key(F.col("trip_id")).alias("trip_key"),
        "trip_id",
        "load_id",
        date_key("dispatch_date").alias("date_key"),
        surrogate_key(F.col("driver_id")).alias("driver_key"),
        surrogate_key(F.col("truck_id")).alias("truck_key"),
        surrogate_key(F.col("trailer_id")).alias("trailer_key"),
        surrogate_key(F.col("l_customer_id")).alias("customer_key"),
        surrogate_key(F.col("l_route_id")).alias("route_key"),
        F.col("l_booking_type").alias("booking_type"),
        F.col("l_load_type").alias("load_type"),
        F.col("l_load_status").alias("load_status"),
        F.col("l_weight_lbs").alias("weight"),
        F.col("l_pieces").alias("pieces"),
        "planned_miles",
        F.col("actual_distance_miles").alias("actual_miles"),
        F.col("actual_duration_hours").alias("trip_duration_hours"),
        revenue.alias("revenue"),
        surcharge.alias("fuel_surcharge"),
        accessorial.alias("accessorial_charges"),
        (revenue + surcharge + accessorial).cast("decimal(18,2)").alias("total_revenue"),
        F.col("fuel_gallons_used").alias("fuel_consumed_gallons"),
        F.col("idle_time_hours").alias("idle_hours"),
        "src_sys_create_date",
    )


def start(spark):
    ensure_table(spark, DDL)
    trips = read_raw_topic(spark, "trips").select(TRIP_COLUMNS).withWatermark("sys_create_date", MAX_GAP)
    loads = prefix_loads(read_raw_topic(spark, "loads")).withWatermark("l_sys_create_date", MAX_GAP)
    joined = trips.join(loads, F.expr(f"""
        load_id = l_load_id
        AND l_sys_create_date BETWEEN sys_create_date - INTERVAL {MAX_GAP}
                                  AND sys_create_date + INTERVAL {MAX_GAP}"""))

    def handle_batch(df, batch_id):
        # routes re-read each batch: 58 rows, and new routes are picked up without a restart
        return write_fact(build(df, read_routes(spark)), TABLE)

    return start_query(NAME, joined, handle_batch)
