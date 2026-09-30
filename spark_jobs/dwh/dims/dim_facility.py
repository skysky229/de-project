"""Load the facility SCD1 dimension from ``default.facilities``."""
from pyspark.sql import functions as F
from common import (UNKNOWN_KEY, ensure_table, get_spark, latest_per_key, parse_window, read_created_between, seed_unknown_member, surrogate_key, with_audit, write_append, empty_to_null)
TABLE="dim_facility"
DDL="""
CREATE TABLE IF NOT EXISTS {db}.dim_facility
(
    facility_key Int64,
    facility_id String,
    facility_name String,
    facility_type LowCardinality(String), 
    city String,
    state String,
    latitude Nullable(Decimal(10,6)),
    longitude Nullable(Decimal(10,6)),
    dock_doors Nullable(Int32), 
    operating_hours String,
    src_sys_create_date DateTime('UTC'),
    etl_loaded_date DateTime('UTC')
) 
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY facility_id"""

SOURCE_COLUMNS=["facility_id","facility_name","facility_type","city","state","latitude","longitude","dock_doors","operating_hours","sys_create_date"]

def transform(raw):
  drivers = latest_per_key(raw, ["facility_id"])
  dim = drivers.select(
    surrogate_key(F.col("facility_id")).alias("facility_key"),
    "facility_id",
    "facility_name",
    F.coalesce(empty_to_null(F.col("facility_type")), F.lit("Unknown")).alias("facility_type"),
    "city",
    "state",
    F.col("latitude").cast("decimal(10,6)").alias("latitude"),
    F.col("longitude").cast("decimal(10,6)").alias("longitude"),
    F.col("dock_doors").cast("int").alias("dock_doors"),"operating_hours",F.col("sys_create_date").alias("src_sys_create_date"))
  return dim


def unknown_member(spark):
  return spark.sql(f"""
        SELECT CAST({UNKNOWN_KEY} AS BIGINT) facility_key,
        'UNKNOWN' facility_id,
        'Unknown' facility_name,
        'Unknown' facility_type,
        'Unknown' city,
        'Unknown' state,
        CAST(NULL AS DECIMAL(10,6)) latitude,
        CAST(NULL AS DECIMAL(10,6)) longitude,
        CAST(NULL AS INT) dock_doors,
        'Unknown' operating_hours,
        CAST('1970-01-01' AS TIMESTAMP) src_sys_create_date""")

def main():
  start,end=parse_window(); spark=get_spark(f"{TABLE}_{start}_{end}"); ensure_table(spark,DDL); seed_unknown_member(spark,TABLE,"facility_key",unknown_member(spark)); raw=read_created_between(spark,"facilities",SOURCE_COLUMNS,start,end).cache()
  if raw.count():
    out=with_audit(transform(raw)).cache()
    write_append(out,TABLE)
    print(f"[{TABLE}] wrote {out.count()} rows")
  else:
    print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
  spark.stop()

if __name__ == "__main__": main()
