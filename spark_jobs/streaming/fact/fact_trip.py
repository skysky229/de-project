"""fact_trip (streaming): Kafka raw.trips ⋈ raw.loads -> dwh.fact_trip_streaming  (merge on trip_id)

Self-contained on purpose: DDL and transform() are copied from the batch job
(spark_jobs/batch/facts/fact_trip.py) instead of imported, so streaming doesn't depend on
the batch code. Only the table name differs (fact_trip_streaming, 72 h TTL). The view
dwh.v_fact_trip unions both tables, so keep the columns identical to the batch job.

The only stateful streaming query: a trip and its load arrive as separate messages,
possibly in different micro-batches, so Spark buffers each side until its partner
arrives (stream-stream join on load_id). The watermark + time bound let Spark drop
buffered rows whose partner is more than 10 minutes apart (the generator publishes
both at once). Each joined micro-batch is split back into its trip and load columns
and goes through the batch transform(trips, loads, routes) unchanged.
"""

from pyspark.sql import functions as F

from streaming.helpers import ensure_table, latest_per_key, surrogate_key, read_raw_all
from streaming.stream_common import read_raw_topic, start_query, write_fact

NAME = "fact_trip_streaming"  # also the checkpoint folder name
TABLE = "fact_trip_streaming"
MAX_GAP = "10 minutes"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_trip_streaming
(
    trip_key Int64,
    trip_id String,
    date_key Int32,
    driver_key Int64,
    truck_key Int64,
    trailer_key Int64,
    customer_key Int64,
    route_key Int64,
    load_id String,
    booking_type LowCardinality(String),
    load_type LowCardinality(String),
    load_status LowCardinality(String),
    weight_lbs Nullable(Int64),
    pieces Nullable(Int32),
    planned_miles Nullable(Int32),
    actual_miles Nullable(Int32),
    trip_duration_hours Nullable(Decimal(18,2)),
    revenue Nullable(Decimal(18,2)),
    fuel_surcharge Nullable(Decimal(18,2)),
    accessorial_charges Nullable(Decimal(18,2)),
    total_revenue Nullable(Decimal(18,2)),
    fuel_consumed_gallons Nullable(Decimal(18,3)),
    idle_hours Nullable(Decimal(18,2)),
    src_sys_create_date DateTime('UTC'),
    etl_loaded_date DateTime('UTC')
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY trip_id
TTL src_sys_create_date + INTERVAL 72 HOUR
SETTINGS merge_with_ttl_timeout = 3600
"""

TRIP_COLUMNS = [
    "trip_id",
    "load_id",
    "driver_id",
    "truck_id",
    "trailer_id",
    "dispatch_date",
    "actual_distance_miles",
    "actual_duration_hours",
    "fuel_gallons_used",
    "idle_time_hours",
    "sys_create_date",
]

LOAD_COLUMNS = [
    "load_id",
    "customer_id",
    "route_id",
    "load_type",
    "weight_lbs",
    "pieces",
    "revenue",
    "fuel_surcharge",
    "accessorial_charges",
    "load_status",
    "booking_type",
    "sys_create_date",
]


def read_routes(spark):
    """route_id -> planned_miles; routes are a 58-row lookup, so every partition is read."""
    routes = read_raw_all(
        spark, "routes", ["route_id", "typical_distance_miles", "sys_create_date"]
    )
    return latest_per_key(routes, ["route_id"]).select(
        "route_id", F.col("typical_distance_miles").alias("planned_miles")
    )


def transform(trips, loads, routes):
    t = latest_per_key(trips, ["trip_id"]).alias("t")
    l = latest_per_key(loads, ["load_id"]).alias("l")
    r = F.broadcast(routes).alias("r")
    joined = t.join(l, "load_id", "inner").join(
        r, F.col("l.route_id") == F.col("r.route_id"), "left"
    )
    total = (
        F.coalesce(F.col("l.revenue"), F.lit(0))
        + F.coalesce(F.col("l.fuel_surcharge"), F.lit(0))
        + F.coalesce(F.col("l.accessorial_charges"), F.lit(0))
    )
    return joined.select(
        surrogate_key(F.col("t.trip_id")).alias("trip_key"),
        F.col("t.trip_id").alias("trip_id"),
        F.date_format("t.dispatch_date", "yyyyMMdd").cast("int").alias("date_key"),
        surrogate_key(F.col("t.driver_id")).alias("driver_key"),
        surrogate_key(F.col("t.truck_id")).alias("truck_key"),
        surrogate_key(F.col("t.trailer_id")).alias("trailer_key"),
        surrogate_key(F.col("l.customer_id")).alias("customer_key"),
        surrogate_key(F.col("l.route_id")).alias("route_key"),
        "load_id",
        F.col("l.booking_type").alias("booking_type"),
        F.col("l.load_type").alias("load_type"),
        F.col("l.load_status").alias("load_status"),
        F.col("l.weight_lbs").cast("bigint").alias("weight_lbs"),
        F.col("l.pieces").cast("int").alias("pieces"),
        F.col("r.planned_miles").cast("int").alias("planned_miles"),
        F.col("t.actual_distance_miles").cast("int").alias("actual_miles"),
        F.col("t.actual_duration_hours").cast("decimal(18,2)").alias("trip_duration_hours"),
        F.round("l.revenue", 2).cast("decimal(18,2)").alias("revenue"),
        F.round("l.fuel_surcharge", 2).cast("decimal(18,2)").alias("fuel_surcharge"),
        F.round("l.accessorial_charges", 2).cast("decimal(18,2)").alias("accessorial_charges"),
        F.round(total, 2).cast("decimal(18,2)").alias("total_revenue"),
        F.col("t.fuel_gallons_used").cast("decimal(18,3)").alias("fuel_consumed_gallons"),
        F.col("t.idle_time_hours").cast("decimal(18,2)").alias("idle_hours"),
        F.greatest(F.col("t.sys_create_date"), F.col("l.sys_create_date")).alias(
            "src_sys_create_date"
        ),
    )


def start(spark):
    ensure_table(spark, DDL)
    trips = (
        read_raw_topic(spark, "trips")
        .select(TRIP_COLUMNS)
        .withWatermark("sys_create_date", MAX_GAP)
    )
    loads = (
        read_raw_topic(spark, "loads")
        .select([F.col(c).alias(f"l_{c}") for c in LOAD_COLUMNS])
        .withWatermark("l_sys_create_date", MAX_GAP)
    )
    joined = trips.join(
        loads,
        F.expr(
            f"""
        load_id = l_load_id
        AND l_sys_create_date BETWEEN sys_create_date - INTERVAL {MAX_GAP}
                                  AND sys_create_date + INTERVAL {MAX_GAP}"""
        ),
    )

    def handle_batch(df, batch_id):
        trips_part = df.select(TRIP_COLUMNS)
        loads_part = df.select([F.col(f"l_{c}").alias(c) for c in LOAD_COLUMNS])
        # routes re-read each batch: 58 rows, and new routes are picked up without a restart
        return write_fact(transform(trips_part, loads_part, read_routes(spark)), TABLE)

    return start_query(NAME, joined, handle_batch)
