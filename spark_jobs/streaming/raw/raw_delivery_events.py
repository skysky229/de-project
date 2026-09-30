"""raw_delivery_events: Kafka raw.delivery_events -> MinIO s3a://raw/logistics/delivery_events  (parquet, partitioned by partition_date, as-is)"""
from streaming.stream_common import start_raw_ingest

NAME = "raw_delivery_events"


def start(spark):
    return start_raw_ingest(spark, NAME, "delivery_events")
