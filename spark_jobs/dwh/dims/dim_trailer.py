"""Load the trailer SCD1 dimension from ``default.trailers``."""
from pyspark.sql import functions as F
from common import (UNKNOWN_KEY, ensure_table, get_spark, latest_per_key, parse_window, read_created_between, seed_unknown_member, surrogate_key, with_audit, write_append)
TABLE = "dim_trailer"
DDL = """
    CREATE TABLE IF NOT EXISTS {db}.dim_trailer
    (
        trailer_key Int64,
        trailer_id String,
        trailer_number String,
        trailer_type LowCardinality(String),
        length_feet Nullable(Int32),
        model_year Nullable(Int32),
        vin String,
        acquisition_date Nullable(Date),
        trailer_status LowCardinality(String),
        current_location String,
        src_sys_create_date DateTime('UTC'),
        setl_loaded_date DateTime('UTC')
    )
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY trailer_id
    """
SOURCE_COLUMNS=["trailer_id","trailer_number","trailer_type","length_feet","model_year","vin","acquisition_date","status","current_location","sys_create_date"]

def transform(raw):
  return latest_per_key(raw,["trailer_id"]).select(surrogate_key(F.col("trailer_id")).alias ("trailer_key"),
    "trailer_id",
    F.col("trailer_number").cast("string").alias("trailer_number"),
    "trailer_type",
    F.col("length_feet").cast("int").alias("length_feet"),
    F.col("model_year").cast("int").alias("model_year"),
    "vin",
    F.to_date("acquisition_date").alias("acquisition_date"),
    F.col("status").alias("trailer_status"),
    "current_location",
    F.col("sys_create_date").alias("src_sys_create_date"))

def unknown_member(spark):
   return spark.sql(f"""SELECT CAST({UNKNOWN_KEY} AS BIGINT) trailer_key,
                   'UNKNOWN' trailer_id,
                   'Unknown' trailer_number,
                   'Unknown' trailer_type,
                   CAST(NULL AS INT) length_feet,
                   CAST(NULL AS INT) model_year,
                   'Unknown' vin,
                   CAST(NULL AS DATE) acquisition_date,
                   'Unknown' trailer_status,
                   'Unknown' current_location,
                   CAST('1970-01-01' AS TIMESTAMP) src_sys_create_date
                  """
                  )
def main():
    start,end=parse_window(); spark=get_spark(f"{TABLE}_{start}_{end}"); ensure_table(spark,DDL); seed_unknown_member(spark,TABLE,"trailer_key",unknown_member(spark)); raw=read_created_between(spark,"trailers",SOURCE_COLUMNS,start,end).cache()
    if raw.count():
        out=with_audit(transform(raw)).cache()
        write_append(out,TABLE)
        print(f"[{TABLE}] wrote {out.count()} rows")
    else:
       print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
    spark.stop()

if __name__ == "__main__": main()
