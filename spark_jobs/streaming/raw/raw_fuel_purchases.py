"""raw_fuel_purchases: Kafka raw.fuel_purchases -> MinIO s3a://raw/logistics/fuel_purchases  (parquet, partitioned by partition_date, as-is)"""

from streaming.stream_common import start_raw_ingest

NAME = "raw_fuel_purchases"


def start(spark):
    return start_raw_ingest(spark, NAME, "fuel_purchases")
