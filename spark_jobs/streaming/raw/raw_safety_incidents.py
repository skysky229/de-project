"""raw_safety_incidents: Kafka raw.safety_incidents -> MinIO s3a://raw/logistics/safety_incidents  (parquet, partitioned by partition_date, as-is)"""
from streaming.stream_common import start_raw_ingest

NAME = "raw_safety_incidents"


def start(spark):
    return start_raw_ingest(spark, NAME, "safety_incidents")
