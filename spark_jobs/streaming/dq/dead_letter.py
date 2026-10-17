"""dq_dead_letter: Kafka raw.* (all 6 topics) -> dwh.dq_dead_letter  (the messages the other queries drop)

Every raw/fact query reads its topic through stream_common.read_raw_topic(), which drops the
messages validation_error() rejects (not JSON, a required column missing or not castable).
This query reads the same topics, applies the same validation and keeps exactly those
messages, as-is (raw_value) with their Kafka coordinates and the reason, so nothing is lost
silently and a fixed message can be re-published from here.

Replays are harmless: ReplacingMergeTree on (topic, partition, offset). Rows expire after 90 days.
dq_streaming.py (DAG logistics_streaming_dq) alerts on the dead-letter rate.
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.helpers import ensure_table, write_append
from streaming.schemas import SCHEMAS
from streaming.stream_common import parse_json, read_kafka, start_query, validation_error

NAME = "dq_dead_letter"  # also the checkpoint folder name
TABLE = "dq_dead_letter"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.dq_dead_letter
(
    topic           LowCardinality(String),
    kafka_partition Int32,
    kafka_offset    Int64,
    kafka_timestamp DateTime('UTC') COMMENT 'when Kafka received it (= sys_create_date of a valid row)',
    kafka_key       Nullable(String),
    raw_value       String COMMENT 'the message exactly as published',
    error           String COMMENT 'why it was rejected, e.g. "missing trip_id; invalid dispatch_date"',
    detected_at     DateTime('UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (topic, kafka_partition, kafka_offset)
TTL kafka_timestamp + INTERVAL 90 DAY
"""


def dead_letters(batch: DataFrame) -> DataFrame:
    """The rejected messages of one micro-batch (all topics), with the reason."""
    frames = []
    for table in SCHEMAS:
        frames.append(
            batch.where(F.col("topic") == f"raw.{table}")
            .select(
                "topic",
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
                F.col("key").cast("string").alias("kafka_key"),
                F.col("value").cast("string").alias("raw_value"),
                parse_json(table),
            )
            .select("*", validation_error(table).alias("error"))
            .where(F.col("error") != "")
            .drop("m")
        )
    return reduce(DataFrame.unionByName, frames).withColumn("detected_at", F.current_timestamp())


def start(spark):
    ensure_table(spark, DDL)

    def handle_batch(df, batch_id):
        out = dead_letters(df).persist()
        try:
            n = out.count()
            if n:
                write_append(out, TABLE)
            return n
        finally:
            out.unpersist()

    return start_query(NAME, read_kafka(spark, [f"raw.{t}" for t in SCHEMAS]), handle_batch)
