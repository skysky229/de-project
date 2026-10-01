"""Count the streamed raw parquet rows in MinIO, per table (used by scripts/streaming_check.sh).

Reads s3a://raw/logistics/<table> through Spark, so only committed files count
(_spark_metadata). Prints one machine-readable line per table:
    LAKE <table> <rows> <distinct business keys>

    spark-submit count_raw_lake.py [--since "2026-09-30 08:00:00"]   # UTC, filters on sys_create_date
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

RAW_ROOT = os.environ.get("STREAM_RAW_ROOT", "s3a://raw/logistics")
TABLES = {  # table -> business key
    "trips": "trip_id",
    "loads": "load_id",
    "delivery_events": "event_id",
    "fuel_purchases": "fuel_purchase_id",
    "maintenance_records": "maintenance_id",
    "safety_incidents": "incident_id",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--since", help="UTC 'YYYY-MM-DD HH:MM:SS'; count rows with sys_create_date >= since"
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("count_raw_lake")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    for table, key in TABLES.items():
        try:
            df = spark.read.parquet(f"{RAW_ROOT}/{table}")
        except AnalysisException:  # nothing written yet
            print(f"LAKE {table} 0 0", flush=True)
            continue
        if args.since:
            df = df.filter(F.col("sys_create_date") >= F.to_timestamp(F.lit(args.since)))
        row = df.agg(F.count("*").alias("rows"), F.countDistinct(key).alias("ids")).first()
        print(f"LAKE {table} {row.rows} {row.ids}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
