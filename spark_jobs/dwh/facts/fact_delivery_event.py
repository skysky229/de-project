"""Load pickup and delivery events at one row per event_id."""
from pyspark.sql import functions as F
from common import ensure_table,get_spark,latest_per_key,parse_window,read_created_between,surrogate_key,with_audit,write_append
TABLE="fact_delivery_event"
DDL = """
    CREATE TABLE IF NOT EXISTS {db}.fact_delivery_event
    (
        event_key Int64,
        event_id String,
        date_key Int32,
        scheduled_date_key Int32,
        actual_date_key Int32,
        facility_key Int64,
        trip_key Int64,
        load_id String,
        event_type LowCardinality(String),
        scheduled_datetime DateTime('UTC'),
        actual_datetime Nullable(DateTime('UTC')),
        detention_minutes Nullable(Int32),
        delay_minutes Nullable(Int32),
        is_on_time UInt8,
        event_count UInt8,
        on_time_count UInt8,
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    ) 
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY event_id
    """

SOURCE_COLUMNS=["event_id","load_id","trip_id","event_type","facility_id","scheduled_datetime","actual_datetime","detention_minutes","on_time_flag","sys_create_date"]

def transform(raw):
  rows=latest_per_key(raw,["event_id"])
  actual=F.to_timestamp("actual_datetime"); scheduled=F.to_timestamp("scheduled_datetime")
  on_time=F.coalesce(F.col("on_time_flag").cast("boolean"),F.lit(False))
  return rows.select(surrogate_key(F.col("event_id")).alias("event_key"),
        "event_id",
        F.date_format(actual,"yyyyMMdd").cast("int").alias("date_key"),
        F.date_format(scheduled,"yyyyMMdd").cast("int").alias("scheduled_date_key"),
        F.date_format(actual,"yyyyMMdd").cast("int").alias("actual_date_key"),
        surrogate_key(F.col("facility_id")).alias("facility_key"),
        surrogate_key(F.col("trip_id")).alias("trip_key"),
        "load_id","event_type",scheduled.alias("scheduled_datetime"),
        actual.alias("actual_datetime"),
        F.col("detention_minutes").cast("int").alias("detention_minutes"),
        F.round((F.unix_timestamp(actual)-F.unix_timestamp(scheduled))/60).cast("int").alias("delay_minutes"),on_time.cast("byte").alias("is_on_time"),F.lit(1).cast("byte").alias("event_count"),F.when(on_time,1).otherwise(0).cast("byte").alias("on_time_count"),F.col("sys_create_date").alias("src_sys_create_date"))

def main():
    start,end=parse_window()
    spark=get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark,DDL)
    raw=read_created_between(spark,"delivery_events",SOURCE_COLUMNS,start,end).cache()
    if raw.count():
        out=with_audit(transform(raw)).cache()
        write_append(out,TABLE)
        print(f"[{TABLE}] wrote {out.count()} rows")
    else:
        print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
    spark.stop()
    
if __name__ == "__main__": main()
