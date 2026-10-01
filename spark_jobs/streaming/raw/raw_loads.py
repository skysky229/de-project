"""raw_loads: Kafka raw.loads -> MinIO s3a://raw/logistics/loads  (parquet, partitioned by partition_date, as-is)"""

from streaming.stream_common import start_raw_ingest

NAME = "raw_loads"


def start(spark):
    return start_raw_ingest(spark, NAME, "loads")
