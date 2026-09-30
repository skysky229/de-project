"""Load fuel-purchase transactions at one row per fuel_purchase_id."""
from pyspark.sql import functions as F
from common import ensure_table,get_spark,latest_per_key,parse_window,read_created_between,surrogate_key,with_audit,write_append
TABLE="fact_fuel_purchase"
DDL="""
    CREATE TABLE IF NOT EXISTS {db}.fact_fuel_purchase 
    (
        fuel_purchase_key Int64,
        fuel_purchase_id String,
        date_key Int32,
        driver_key Int64,
        truck_key Int64,
        trip_key Int64,
        purchase_location String,
        gallons Nullable(Decimal(18,3)),
        price_per_gallon Nullable(Decimal(18,3)),
        total_cost Nullable(Decimal(18,2)),
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    )
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY fuel_purchase_id
    """
SOURCE_COLUMNS=["fuel_purchase_id","trip_id","truck_id","driver_id","purchase_date","location_city","location_state","gallons","price_per_gallon","total_cost","sys_create_date"]

def transform(raw):
 return latest_per_key(raw,["fuel_purchase_id"]).select(surrogate_key(F.col("fuel_purchase_id")).alias("fuel_purchase_key"),"fuel_purchase_id",F.date_format("purchase_date","yyyyMMdd").cast("int").alias("date_key"),surrogate_key(F.col("driver_id")).alias("driver_key"),surrogate_key(F.col("truck_id")).alias("truck_key"),surrogate_key(F.col("trip_id")).alias("trip_key"),F.concat_ws(", ","location_city","location_state").alias("purchase_location"),F.col("gallons").cast("decimal(18,3)").alias("gallons"),F.col("price_per_gallon").cast("decimal(18,3)").alias("price_per_gallon"),F.round("total_cost",2).cast("decimal(18,2)").alias("total_cost"),F.col("sys_create_date").alias("src_sys_create_date"))
def main():
 start,end=parse_window(); spark=get_spark(f"{TABLE}_{start}_{end}"); ensure_table(spark,DDL); raw=read_created_between(spark,"fuel_purchases",SOURCE_COLUMNS,start,end).cache()
 if raw.count(): out=with_audit(transform(raw)).cache(); write_append(out,TABLE); print(f"[{TABLE}] wrote {out.count()} rows")
 else: print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
 spark.stop()
if __name__ == "__main__": main()
