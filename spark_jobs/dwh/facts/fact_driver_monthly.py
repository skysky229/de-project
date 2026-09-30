"""Load source-provided driver monthly performance metrics."""
from pyspark.sql import functions as F
from common import ensure_table,get_spark,latest_per_key,parse_window,read_created_between,surrogate_key,with_audit,write_append
TABLE="fact_driver_monthly"
DDL="""
    CREATE TABLE IF NOT EXISTS {db}.fact_driver_monthly
    (
        driver_key Int64,
        month_date_key Int32,
        trips_completed Nullable(Int32),
        total_miles Nullable(Int64),
        total_revenue Nullable(Decimal(18,2)),
        average_mpg Nullable(Decimal(18,3)),
        total_fuel_gallons Nullable(Decimal(18,3)),
        on_time_delivery_rate Nullable(Decimal(9,6)),
        average_idle_hours Nullable(Decimal(18,2)),
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    ) 
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY (driver_key, month_date_key)
    """
SOURCE_COLUMNS=["driver_id","month","trips_completed","total_miles","total_revenue","average_mpg","total_fuel_gallons","on_time_delivery_rate","average_idle_hours","sys_create_date"]

def transform(raw):
    return latest_per_key(raw,["driver_id","month"]).select(surrogate_key(F.col("driver_id")).alias("driver_key"),
    F.date_format("month","yyyyMMdd").cast("int").alias("month_date_key"),
    F.col("trips_completed").cast("int"),
    F.col("total_miles").cast("bigint"),
    F.round("total_revenue",2).cast("decimal(18,2)").alias("total_revenue"),
    F.col("average_mpg").cast("decimal(18,3)").alias("average_mpg"),
    F.col("total_fuel_gallons").cast("decimal(18,3)").alias("total_fuel_gallons"),
    F.col("on_time_delivery_rate").cast("decimal(9,6)").alias("on_time_delivery_rate"),
    F.col("average_idle_hours").cast("decimal(18,2)").alias("average_idle_hours"),
    F.col("sys_create_date").alias("src_sys_create_date"))

def main():
    start,end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark,DDL)
    raw = read_created_between(spark,"driver_monthly_metrics",SOURCE_COLUMNS,start,end).cache()
    if raw.count():
        out = with_audit(transform(raw)).cache()
        write_append(out,TABLE)
        print(f"[{TABLE}] wrote {out.count()} rows")
    else:
       print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
    spark.stop()

if __name__ == "__main__": main()
