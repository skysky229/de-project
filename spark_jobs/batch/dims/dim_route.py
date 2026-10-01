"""Load the route SCD1 dimension from the MinIO raw ``routes`` partitions."""

from pyspark.sql import functions as F

from common import (
    UNKNOWN_KEY,
    ensure_table,
    get_spark,
    latest_per_key,
    parse_window,
    read_created_between,
    seed_unknown_member,
    surrogate_key,
    with_audit,
    write_append,
)

TABLE = "dim_route"

DDL = """
    CREATE TABLE IF NOT EXISTS {db}.dim_route
    (
        route_key Int64,
        route_id String,
        origin_city String,
        origin_state String,
        destination_city String,
        destination_state String,
        distance_miles Nullable(Int32),
        base_rate Nullable(Decimal(18,2)),
        fuel_surcharge_rate Nullable(Decimal(18,2)),
        typical_transit_days Nullable(Int32),
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    ) 
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY route_id"""

SOURCE_COLUMNS = [
    "route_id",
    "origin_city",
    "origin_state",
    "destination_city",
    "destination_state",
    "typical_distance_miles",
    "base_rate_per_mile",
    "fuel_surcharge_rate",
    "typical_transit_days",
    "sys_create_date",
]


def transform(raw):
    return latest_per_key(raw, ["route_id"]).select(
        surrogate_key(F.col("route_id")).alias("route_key"),
        "route_id",
        "origin_city",
        "origin_state",
        "destination_city",
        "destination_state",
        F.col("typical_distance_miles").cast("int").alias("distance_miles"),
        F.round("base_rate_per_mile", 2).cast("decimal(18,2)").alias("base_rate"),
        F.round("fuel_surcharge_rate", 2).cast("decimal(18,2)").alias("fuel_surcharge_rate"),
        F.col("typical_transit_days").cast("int"),
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def unknown_member(spark):
    return spark.sql(
        f"""SELECT CAST({UNKNOWN_KEY} AS BIGINT) route_key,
                      'UNKNOWN' route_id,
                      'Unknown' origin_city,
                      'Unknown' origin_state,
                      'Unknown' destination_city,
                      'Unknown' destination_state,
                      CAST(NULL AS INT) distance_miles,
                      CAST(NULL AS DECIMAL(18,2)) base_rate,
                      CAST(NULL AS DECIMAL(18,2)) fuel_surcharge_rate,
                      CAST(NULL AS INT) typical_transit_days,
                      CAST('1970-01-01' AS TIMESTAMP) src_sys_create_date
                     """
    )


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)
    seed_unknown_member(spark, TABLE, "route_key", unknown_member(spark))

    raw = read_created_between(spark, "routes", SOURCE_COLUMNS, start, end).cache()

    if raw.count():
        out = with_audit(transform(raw)).cache()
        write_append(out, TABLE)
        print(f"[{TABLE}] wrote {out.count()} rows")
    else:
        print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")

    spark.stop()


if __name__ == "__main__":
    main()
