"""Load truck maintenance transactions."""
from pyspark.sql import functions as F
from common import ensure_table,get_spark,latest_per_key,parse_window,read_created_between,surrogate_key,with_audit,write_append
TABLE="fact_maintenance"
DDL="""CREATE TABLE IF NOT EXISTS {db}.fact_maintenance (
 maintenance_key Int64, maintenance_id String, date_key Int32, truck_key Int64, maintenance_type LowCardinality(String), maintenance_date Date, odometer_reading Nullable(Int64), labor_hours Nullable(Decimal(18,2)), labor_cost Nullable(Decimal(18,2)), parts_cost Nullable(Decimal(18,2)), total_cost Nullable(Decimal(18,2)), downtime_hours Nullable(Decimal(18,2)), facility_location String, src_sys_create_date DateTime('UTC'), etl_loaded_date DateTime('UTC')
) ENGINE = ReplacingMergeTree(src_sys_create_date) ORDER BY maintenance_id"""
SOURCE_COLUMNS=["maintenance_id","truck_id","maintenance_date","maintenance_type","odometer_reading","labor_hours","labor_cost","parts_cost","total_cost","facility_location","downtime_hours","sys_create_date"]
def transform(raw):
 return latest_per_key(raw,["maintenance_id"]).select(surrogate_key(F.col("maintenance_id")).alias("maintenance_key"),"maintenance_id",F.date_format("maintenance_date","yyyyMMdd").cast("int").alias("date_key"),surrogate_key(F.col("truck_id")).alias("truck_key"),"maintenance_type",F.to_date("maintenance_date").alias("maintenance_date"),F.col("odometer_reading").cast("bigint").alias("odometer_reading"),F.col("labor_hours").cast("decimal(18,2)").alias("labor_hours"),F.round("labor_cost",2).cast("decimal(18,2)").alias("labor_cost"),F.round("parts_cost",2).cast("decimal(18,2)").alias("parts_cost"),F.round("total_cost",2).cast("decimal(18,2)").alias("total_cost"),F.col("downtime_hours").cast("decimal(18,2)").alias("downtime_hours"),"facility_location",F.col("sys_create_date").alias("src_sys_create_date"))
def main():
 start,end=parse_window(); spark=get_spark(f"{TABLE}_{start}_{end}"); ensure_table(spark,DDL); raw=read_created_between(spark,"maintenance_records",SOURCE_COLUMNS,start,end).cache()
 if raw.count(): out=with_audit(transform(raw)).cache(); write_append(out,TABLE); print(f"[{TABLE}] wrote {out.count()} rows")
 else: print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
 spark.stop()
if __name__ == "__main__": main()
