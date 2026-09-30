"""raw_trips: Kafka raw.trips -> MinIO s3a://raw/logistics/trips  (parquet, partitioned by partition_date, as-is)"""
from streaming.stream_common import start_raw_ingest

NAME = "raw_trips"


def start(spark):
    return start_raw_ingest(spark, NAME, "trips")
