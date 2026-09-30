"""Load the trip fact by enriching trips with their load attributes in Spark."""
from pyspark.sql import functions as F
from common import ensure_table,get_spark,latest_per_key,parse_window,read_created_between,surrogate_key,with_audit,write_append
TABLE="fact_trip"
DDL="""CREATE TABLE IF NOT EXISTS {db}.fact_trip (
 trip_key Int64, trip_id String, date_key Int32, driver_key Int64, truck_key Int64, trailer_key Int64, customer_key Int64, route_key Int64,
 load_id String, booking_type LowCardinality(String), load_type LowCardinality(String), load_status LowCardinality(String), weight_lbs Nullable(Int64), pieces Nullable(Int32), planned_miles Nullable(Int32), actual_miles Nullable(Int32), trip_duration_hours Nullable(Decimal(18,2)), revenue Nullable(Decimal(18,2)), fuel_surcharge Nullable(Decimal(18,2)), accessorial_charges Nullable(Decimal(18,2)), total_revenue Nullable(Decimal(18,2)), fuel_consumed_gallons Nullable(Decimal(18,3)), idle_hours Nullable(Decimal(18,2)), src_sys_create_date DateTime('UTC'), etl_loaded_date DateTime('UTC')
) ENGINE = ReplacingMergeTree(src_sys_create_date) ORDER BY trip_id"""
TRIP_COLUMNS=["trip_id","load_id","driver_id","truck_id","trailer_id","dispatch_date","actual_distance_miles","actual_duration_hours","fuel_gallons_used","idle_time_hours","sys_create_date"]
LOAD_COLUMNS=["load_id","customer_id","route_id","load_type","weight_lbs","pieces","revenue","fuel_surcharge","accessorial_charges","load_status","booking_type","sys_create_date"]
def transform(trips,loads):
 t=latest_per_key(trips,["trip_id"]).alias("t"); l=latest_per_key(loads,["load_id"]).alias("l"); joined=t.join(l,"load_id","inner")
 total=F.coalesce(F.col("l.revenue"),F.lit(0))+F.coalesce(F.col("l.fuel_surcharge"),F.lit(0))+F.coalesce(F.col("l.accessorial_charges"),F.lit(0))
 return joined.select(surrogate_key(F.col("t.trip_id")).alias("trip_key"),F.col("t.trip_id").alias("trip_id"),F.date_format("t.dispatch_date","yyyyMMdd").cast("int").alias("date_key"),surrogate_key(F.col("t.driver_id")).alias("driver_key"),surrogate_key(F.col("t.truck_id")).alias("truck_key"),surrogate_key(F.col("t.trailer_id")).alias("trailer_key"),surrogate_key(F.col("l.customer_id")).alias("customer_key"),surrogate_key(F.col("l.route_id")).alias("route_key"),"load_id",F.col("l.booking_type").alias("booking_type"),F.col("l.load_type").alias("load_type"),F.col("l.load_status").alias("load_status"),F.col("l.weight_lbs").cast("bigint").alias("weight_lbs"),F.col("l.pieces").cast("int").alias("pieces"),F.lit(None).cast("int").alias("planned_miles"),F.col("t.actual_distance_miles").cast("int").alias("actual_miles"),F.col("t.actual_duration_hours").cast("decimal(18,2)").alias("trip_duration_hours"),F.round("l.revenue",2).cast("decimal(18,2)").alias("revenue"),F.round("l.fuel_surcharge",2).cast("decimal(18,2)").alias("fuel_surcharge"),F.round("l.accessorial_charges",2).cast("decimal(18,2)").alias("accessorial_charges"),F.round(total,2).cast("decimal(18,2)").alias("total_revenue"),F.col("t.fuel_gallons_used").cast("decimal(18,3)").alias("fuel_consumed_gallons"),F.col("t.idle_time_hours").cast("decimal(18,2)").alias("idle_hours"),F.greatest(F.col("t.sys_create_date"),F.col("l.sys_create_date")).alias("src_sys_create_date"))
def main():
 start,end=parse_window(); spark=get_spark(f"{TABLE}_{start}_{end}"); ensure_table(spark,DDL)
 trips=read_created_between(spark,"trips",TRIP_COLUMNS,start,end).cache(); loads=read_created_between(spark,"loads",LOAD_COLUMNS,start,end).cache()
 if trips.count() and loads.count(): out=with_audit(transform(trips,loads)).cache(); write_append(out,TABLE); print(f"[{TABLE}] wrote {out.count()} rows")
 else: print(f"[{TABLE}] window {start}..{end}: no joinable trip/load rows")
 spark.stop()
if __name__ == "__main__": main()
