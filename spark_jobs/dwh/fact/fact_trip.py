"""fact_trip: default.trips ⋈ default.loads -> dwh.fact_trip  (merge on trip_id)

One row per trip (= one load; trips and loads are 1:1). Trip columns (driver, truck,
miles, fuel) come from trips, load columns (customer, route, revenue, weight) from loads.
planned_miles = the route's typical distance (lookup on the 58-row routes table).
`downtime_hours` from the original design isn't in the source trips/loads (it's a
maintenance measure), so it isn't here.

Shared with the streaming job (spark_jobs/streaming/fact/fact_trip.py):
    batch      join_trips_loads(trips, loads) -> build(joined, routes)
    streaming  stream-stream join (same columns) -> build(joined, routes)

    spark-submit --py-files common.py fact/fact_trip.py --job-date 2024-03-15
"""
from pyspark.sql import functions as F

from common import (
    SOURCE_DB, date_key, ensure_table, get_spark, latest_per_key, money, parse_window,
    read_created_between, read_query, surrogate_key, with_audit, write_append,
)

TABLE = "fact_trip"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.fact_trip
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


def join_trips_loads(trips, loads):
    """Batch join: newest version of each trip ⋈ newest version of each load."""
    trips = latest_per_key(trips, ["trip_id"])
    loads = prefix_loads(latest_per_key(loads, ["load_id"]))
    return trips.join(loads, trips.load_id == loads.l_load_id, "inner")


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


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)

    trips = read_created_between(spark, "trips", TRIP_COLUMNS, start, end).cache()
    loads = read_created_between(spark, "loads", LOAD_COLUMNS, start, end).cache()
    trip_count, load_count = trips.count(), loads.count()
    if trip_count == 0:
        print(f"[{TABLE}] window {start}..{end}: read 0 trips, nothing to write")
    else:
        out = with_audit(build(join_trips_loads(trips, loads), read_routes(spark))).cache()
        write_append(out, TABLE)
        print(f"[{TABLE}] window {start}..{end}: read {trip_count} trips + {load_count} loads, "
              f"wrote {out.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
