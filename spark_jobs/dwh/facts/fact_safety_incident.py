"""Load safety incidents at one row per incident_id."""
from pyspark.sql import functions as F
from common import ensure_table,get_spark,latest_per_key,parse_window,read_created_between,surrogate_key,with_audit,write_append
TABLE="fact_safety_incident"
DDL="""CREATE TABLE IF NOT EXISTS {db}.fact_safety_incident (
 incident_key Int64, incident_id String, date_key Int32, driver_key Int64, truck_key Int64, trip_key Int64, incident_type LowCardinality(String), location String, at_fault_flag UInt8, injury_flag UInt8, vehicle_damage_cost Nullable(Decimal(18,2)), cargo_damage_cost Nullable(Decimal(18,2)), claim_amount Nullable(Decimal(18,2)), preventable_flag UInt8, incident_count UInt8, injury_count UInt8, preventable_count UInt8, src_sys_create_date DateTime('UTC'), etl_loaded_date DateTime('UTC')
) ENGINE = ReplacingMergeTree(src_sys_create_date) ORDER BY incident_id"""
SOURCE_COLUMNS=["incident_id","trip_id","truck_id","driver_id","incident_date","incident_type","location_city","location_state","at_fault_flag","injury_flag","vehicle_damage_cost","cargo_damage_cost","claim_amount","preventable_flag","sys_create_date"]
def transform(raw):
 r=latest_per_key(raw,["incident_id"]); at_fault=F.coalesce(F.col("at_fault_flag").cast("boolean"),F.lit(False)); injury=F.coalesce(F.col("injury_flag").cast("boolean"),F.lit(False)); preventable=F.coalesce(F.col("preventable_flag").cast("boolean"),F.lit(False))
 return r.select(surrogate_key(F.col("incident_id")).alias("incident_key"),"incident_id",F.date_format("incident_date","yyyyMMdd").cast("int").alias("date_key"),surrogate_key(F.col("driver_id")).alias("driver_key"),surrogate_key(F.col("truck_id")).alias("truck_key"),surrogate_key(F.col("trip_id")).alias("trip_key"),"incident_type",F.concat_ws(", ","location_city","location_state").alias("location"),at_fault.cast("byte").alias("at_fault_flag"),injury.cast("byte").alias("injury_flag"),F.round("vehicle_damage_cost",2).cast("decimal(18,2)").alias("vehicle_damage_cost"),F.round("cargo_damage_cost",2).cast("decimal(18,2)").alias("cargo_damage_cost"),F.round("claim_amount",2).cast("decimal(18,2)").alias("claim_amount"),preventable.cast("byte").alias("preventable_flag"),F.lit(1).cast("byte").alias("incident_count"),F.when(injury,1).otherwise(0).cast("byte").alias("injury_count"),F.when(preventable,1).otherwise(0).cast("byte").alias("preventable_count"),F.col("sys_create_date").alias("src_sys_create_date"))
def main():
 start,end=parse_window(); spark=get_spark(f"{TABLE}_{start}_{end}"); ensure_table(spark,DDL); raw=read_created_between(spark,"safety_incidents",SOURCE_COLUMNS,start,end).cache()
 if raw.count(): out=with_audit(transform(raw)).cache(); write_append(out,TABLE); print(f"[{TABLE}] wrote {out.count()} rows")
 else: print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
 spark.stop()
if __name__ == "__main__": main()
