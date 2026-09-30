"""Load the truck SCD1 dimension from ``default.trucks``."""
from pyspark.sql import functions as F
from common import (UNKNOWN_KEY, ensure_table, get_spark, latest_per_key, parse_window, read_created_between, seed_unknown_member, surrogate_key, with_audit, write_append)
TABLE = "dim_truck"
DDL = """
    CREATE TABLE IF NOT EXISTS {db}.dim_truck
    (
        truck_key Int64,
        truck_id String,
        unit_number String,
        make String,
        model_year Nullable(Int32),
        vin String,
        acquisition_date Nullable(Date),
        fuel_type LowCardinality(String),
        tank_capacity_gallons Nullable(Decimal(18,2)),
        status LowCardinality(String),
        home_terminal String,
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    ) 
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY truck_id
    """
SOURCE_COLUMNS=["truck_id","unit_number","make","model_year","vin","acquisition_date","fuel_type","tank_capacity_gallons","status","home_terminal","sys_create_date"]

def transform(raw):
 return latest_per_key(raw,["truck_id"]).select(surrogate_key(F.col("truck_id")).alias("truck_key"),
        "truck_id",
        F.col("unit_number").cast("string").alias("unit_number"),
        "make",F.col("model_year").cast("int").alias("model_year"),
        "vin",F.to_date("acquisition_date").alias("acquisition_date"),
        "fuel_type",
        F.col("tank_capacity_gallons").cast("decimal(18,2)").alias("tank_capacity_gallons"),
        "status",
        "home_terminal",
        F.col("sys_create_date").alias("src_sys_create_date"))


def unknown_member(spark):
 return spark.sql(f"""SELECT CAST({UNKNOWN_KEY} AS BIGINT) truck_key,
                   'UNKNOWN' truck_id,
                   'Unknown' unit_number,
                   'Unknown' make,
                   CAST(NULL AS INT) model_year,
                   'Unknown' vin,
                   CAST(NULL AS DATE) acquisition_date,
                   'Unknown' fuel_type,
                   CAST(NULL AS DECIMAL(18,2)) tank_capacity_gallons, 'Unknown' status, 'Unknown' home_terminal, CAST('1970-01-01' AS TIMESTAMP) src_sys_create_date""")
def main():
    start,end=parse_window(); spark=get_spark(f"{TABLE}_{start}_{end}"); ensure_table(spark,DDL); seed_unknown_member(spark,TABLE,"truck_key",unknown_member(spark)); raw=read_created_between(spark,"trucks",SOURCE_COLUMNS,start,end).cache()
    if raw.count():
        out=with_audit(transform(raw)).cache()
        write_append(out,TABLE)
        print(f"[{TABLE}] wrote {out.count()} rows")
    else:
       print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
    spark.stop()
    
if __name__ == "__main__": main()
