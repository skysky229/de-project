"""Load logistics CSV objects from MinIO into existing raw ClickHouse tables."""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_TABLES = (
    "drivers", "trucks", "trailers", "customers", "facilities", "routes", "loads", "trips",
    "fuel_purchases", "maintenance_records", "delivery_events", "safety_incidents",
    "driver_monthly_metrics", "truck_utilization_metrics",
)
JDBC_OPTIONS = {
    "url": "jdbc:clickhouse:" + os.environ["DWH_CH_URL"].rstrip("/"),
    "user": os.environ.get("DWH_CH_USER", "default"),
    "password": os.environ["DWH_CH_PASSWORD"],
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def main() -> None:
    spark = SparkSession.builder.appName("minio_to_raw_clickhouse").getOrCreate()
    bucket = os.environ.get("MINIO_BUCKET_RAW", "raw")
    database = os.environ.get("DWH_SOURCE_DB", "default")
    for table in RAW_TABLES:
        path = f"s3a://{bucket}/logistics/{table}.csv"
        raw = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        # This is the raw-layer arrival/version timestamp. Fact date keys still
        # come from business-date columns such as dispatch_date and load_date.
        out = raw.withColumn("sys_create_date", F.current_timestamp())
        (out.write.format("jdbc").options(**JDBC_OPTIONS)
            .option("dbtable", f"{database}.{table}").option("isolationLevel", "NONE")
            .mode("append").save())
        print(f"[{table}] loaded {out.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
