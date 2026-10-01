"""One-time bootstrap: CSVs in MinIO -> partitioned raw history in MinIO.

    s3a://raw/csv/<table>.csv  ->  s3a://raw/history/<table>/partition_date=YYYY-MM-DD/*.parquet

Run by the `logistics_bootstrap` DAG (manual trigger) after s3_upload.py (same folder)
has copied dataset/*.csv to raw/csv/. Safe to re-run: each table folder is overwritten.

Every column is cast to the same type the streaming raw parquet uses
(spark_jobs/streaming/schemas.py), so the batch jobs can union history and
streamed partitions. sys_create_date ("when the row was recorded") is derived
from each table's business date (the second entry per table in TABLES: trips ->
dispatch_date, drivers -> hire_date, facilities/routes -> 2021-12-31, monthly metrics ->
first day of the next month, ...); partition_date = toDate(sys_create_date).
The batch jobs read only the partition_date folders of their window.

Fails if a non-empty CSV value can't be cast (instead of silently writing NULL).

    spark-submit build_raw_history.py [--tables trips,loads]
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CSV_ROOT = os.environ.get("DWH_RAW_CSV_ROOT", "s3a://raw/csv")
HISTORY_ROOT = os.environ.get("DWH_RAW_HISTORY_ROOT", "s3a://raw/history")

S, L, D, DATE, TS = "string", "bigint", "double", "date", "timestamp"

# table -> (columns with types, sys_create_date expression)
TABLES = {
    "customers": (
        {
            "customer_id": S,
            "customer_name": S,
            "customer_type": S,
            "credit_terms_days": L,
            "primary_freight_type": S,
            "account_status": S,
            "contract_start_date": DATE,
            "annual_revenue_potential": L,
        },
        "contract_start_date",
    ),
    "drivers": (
        {
            "driver_id": S,
            "first_name": S,
            "last_name": S,
            "hire_date": DATE,
            "termination_date": DATE,
            "license_number": S,
            "license_state": S,
            "date_of_birth": TS,
            "home_terminal": S,
            "employment_status": S,
            "cdl_class": S,
            "years_experience": L,
        },
        "hire_date",
    ),
    "trucks": (
        {
            "truck_id": S,
            "unit_number": L,
            "make": S,
            "model_year": L,
            "vin": S,
            "acquisition_date": DATE,
            "acquisition_mileage": L,
            "fuel_type": S,
            "tank_capacity_gallons": L,
            "status": S,
            "home_terminal": S,
        },
        "acquisition_date",
    ),
    "trailers": (
        {
            "trailer_id": S,
            "trailer_number": L,
            "trailer_type": S,
            "length_feet": L,
            "model_year": L,
            "vin": S,
            "acquisition_date": DATE,
            "status": S,
            "current_location": S,
        },
        "acquisition_date",
    ),
    "facilities": (
        {
            "facility_id": S,
            "facility_name": S,
            "facility_type": S,
            "city": S,
            "state": S,
            "latitude": D,
            "longitude": D,
            "dock_doors": L,
            "operating_hours": S,
        },
        "'2021-12-31'",
    ),
    "routes": (
        {
            "route_id": S,
            "origin_city": S,
            "origin_state": S,
            "destination_city": S,
            "destination_state": S,
            "typical_distance_miles": L,
            "base_rate_per_mile": D,
            "fuel_surcharge_rate": D,
            "typical_transit_days": L,
        },
        "'2021-12-31'",
    ),
    "loads": (
        {
            "load_id": S,
            "customer_id": S,
            "route_id": S,
            "load_date": DATE,
            "load_type": S,
            "weight_lbs": L,
            "pieces": L,
            "revenue": D,
            "fuel_surcharge": D,
            "accessorial_charges": L,
            "load_status": S,
            "booking_type": S,
        },
        "load_date",
    ),
    "trips": (
        {
            "trip_id": S,
            "load_id": S,
            "driver_id": S,
            "truck_id": S,
            "trailer_id": S,
            "dispatch_date": DATE,
            "actual_distance_miles": L,
            "actual_duration_hours": D,
            "fuel_gallons_used": D,
            "average_mpg": D,
            "idle_time_hours": D,
            "trip_status": S,
        },
        "dispatch_date",
    ),
    "delivery_events": (
        {
            "event_id": S,
            "load_id": S,
            "trip_id": S,
            "event_type": S,
            "facility_id": S,
            "scheduled_datetime": TS,
            "actual_datetime": TS,
            "detention_minutes": L,
            "on_time_flag": S,
            "location_city": S,
            "location_state": S,
        },
        "actual_datetime",
    ),
    "fuel_purchases": (
        {
            "fuel_purchase_id": S,
            "trip_id": S,
            "truck_id": S,
            "driver_id": S,
            "purchase_date": TS,
            "location_city": S,
            "location_state": S,
            "gallons": D,
            "price_per_gallon": D,
            "total_cost": D,
            "fuel_card_number": S,
        },
        "purchase_date",
    ),
    "maintenance_records": (
        {
            "maintenance_id": S,
            "truck_id": S,
            "maintenance_date": DATE,
            "maintenance_type": S,
            "odometer_reading": L,
            "labor_hours": D,
            "labor_cost": D,
            "parts_cost": D,
            "total_cost": D,
            "facility_location": S,
            "downtime_hours": D,
            "service_description": S,
        },
        "maintenance_date",
    ),
    "safety_incidents": (
        {
            "incident_id": S,
            "trip_id": S,
            "truck_id": S,
            "driver_id": S,
            "incident_date": TS,
            "incident_type": S,
            "location_city": S,
            "location_state": S,
            "at_fault_flag": S,
            "injury_flag": S,
            "vehicle_damage_cost": D,
            "cargo_damage_cost": D,
            "claim_amount": D,
            "preventable_flag": S,
            "description": S,
        },
        "incident_date",
    ),
    # monthly metrics are recorded once the month is over: first day of the next month
    "driver_monthly_metrics": (
        {
            "driver_id": S,
            "month": DATE,
            "trips_completed": L,
            "total_miles": L,
            "total_revenue": D,
            "average_mpg": D,
            "total_fuel_gallons": D,
            "on_time_delivery_rate": D,
            "average_idle_hours": D,
        },
        "add_months(month, 1)",
    ),
    "truck_utilization_metrics": (
        {
            "truck_id": S,
            "month": DATE,
            "trips_completed": L,
            "total_miles": L,
            "total_revenue": D,
            "average_mpg": D,
            "maintenance_events": L,
            "maintenance_cost": D,
            "downtime_hours": D,
            "utilization_rate": D,
        },
        "add_months(month, 1)",
    ),
}


def build(spark, table: str) -> int:
    columns, created_expr = TABLES[table]
    # every column as string first, so a failed cast can be detected instead of becoming NULL
    raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{CSV_ROOT}/{table}.csv")
    )
    missing = set(columns) - set(raw.columns)
    if missing:
        raise ValueError(f"[{table}] CSV is missing columns {sorted(missing)}")

    typed = raw.select(
        [F.col(c).cast(t).alias(c) for c, t in columns.items()]
        + [F.col(c).alias(f"_src_{c}") for c in columns]
    )
    bad = (
        typed.select(
            [
                F.sum(
                    (
                        F.col(f"_src_{c}").isNotNull()
                        & (F.trim(F.col(f"_src_{c}")) != "")
                        & F.col(c).isNull()
                    ).cast("int")
                ).alias(c)
                for c, t in columns.items()
                if t != S
            ]
        )
        .first()
        .asDict()
    )
    bad = {c: n for c, n in bad.items() if n}
    if bad:
        raise ValueError(f"[{table}] values that could not be cast: {bad}")

    out = (
        typed.select(*columns)
        .withColumn("sys_create_date", F.expr(f"to_timestamp({created_expr})"))
        .withColumn("partition_date", F.to_date("sys_create_date"))
    )
    (
        out.repartition("partition_date")  # one file per partition folder
        .write.mode("overwrite")
        .partitionBy("partition_date")
        .parquet(f"{HISTORY_ROOT}/{table}")
    )
    rows, days = out.count(), out.select("partition_date").distinct().count()
    print(
        f"[{table}] {rows} rows -> {HISTORY_ROOT}/{table} ({days} partition_date folders)",
        flush=True,
    )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", help="comma-separated subset (default: all 14)")
    args = parser.parse_args()
    tables = args.tables.split(",") if args.tables else list(TABLES)

    spark = (
        SparkSession.builder.appName("build_raw_history")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    for table in tables:
        build(spark, table)
    spark.stop()


if __name__ == "__main__":
    main()
