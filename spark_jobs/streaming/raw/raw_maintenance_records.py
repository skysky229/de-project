"""raw_maintenance_records: Kafka raw.maintenance_records -> MinIO s3a://raw/logistics/maintenance_records  (parquet, partitioned by partition_date, as-is)"""

from streaming.stream_common import start_raw_ingest

NAME = "raw_maintenance_records"


def start(spark):
    return start_raw_ingest(spark, NAME, "maintenance_records")
