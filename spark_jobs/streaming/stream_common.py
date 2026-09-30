"""Shared helpers for the streaming jobs (spark_jobs/streaming/{raw,fact}).

Each job file = one target table = one streaming query, exposing
    start(spark) -> StreamingQuery
run_all.py starts them all inside one long-lived Spark application.

Two standard query shapes cover almost every job:
    start_raw_ingest(spark, name, table)        topic raw.<table> -> MinIO parquet, as-is
    start_fact(spark, name, table, target, ddl, transform)
                                                topic raw.<table> -> dwh.<target>, via the
                                                job's own transform()
Both are stateless. Raw ingest uses Spark's file sink (exactly-once: committed files are
listed in <path>/_spark_metadata, so a replayed batch never duplicates files); facts use
foreachBatch and ClickHouse absorbs replays. Only fact_trip (stream-stream join) keeps state.
Kafka offsets (and fact_trip's join state) are checkpointed in MinIO, so a restart
continues where it stopped.

Env: KAFKA_BOOTSTRAP (default kafka:9092), STREAM_CHECKPOINT_ROOT
(default s3a://processed/checkpoints/streaming), STREAM_RAW_ROOT (default
s3a://raw/logistics), STREAM_TRIGGER (default "30 seconds").
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.helpers import ensure_table, with_audit, write_append
from streaming.schemas import CASTS, SCHEMAS

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINT_ROOT = os.environ.get("STREAM_CHECKPOINT_ROOT", "s3a://processed/checkpoints/streaming")
RAW_ROOT = os.environ.get("STREAM_RAW_ROOT", "s3a://raw/logistics")
TRIGGER = os.environ.get("STREAM_TRIGGER", "30 seconds")


def read_raw_topic(spark: SparkSession, table: str) -> DataFrame:
    """Topic raw.<table> -> rows shaped like the raw ClickHouse table (incl. sys_create_date).

    sys_create_date = the Kafka record timestamp: when the platform received the event.
    It's stable across replays (unlike now()), so a replayed micro-batch produces identical rows.
    """
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", f"raw.{table}")
        .option("startingOffsets", "earliest")      # only used on the very first start
        .option("failOnDataLoss", "false")          # topic retention may delete unread data
        .load()
        .select(F.from_json(F.col("value").cast("string"), SCHEMAS[table]).alias("m"),
                F.col("timestamp").alias("sys_create_date"))
        .select("m.*", "sys_create_date")
    )
    for col, dtype in CASTS.get(table, {}).items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def start_query(name: str, stream: DataFrame, handle_batch) -> "StreamingQuery":
    """Run handle_batch(batch_df, batch_id) -> rows_written on every non-empty micro-batch."""

    def _batch(df: DataFrame, batch_id: int):
        if df.isEmpty():
            return
        df = df.persist()
        try:
            written = handle_batch(df, batch_id)
            print(f"[{name}] batch {batch_id}: wrote {written} rows", flush=True)
        finally:
            df.unpersist()

    return (
        stream.writeStream.queryName(name)
        .foreachBatch(_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{name}")
        .trigger(processingTime=TRIGGER)
        .start()
    )


def write_fact(df: DataFrame, table: str) -> int:
    out = with_audit(df).persist()
    try:
        write_append(out, table)
        return out.count()
    finally:
        out.unpersist()


def start_raw_ingest(spark: SparkSession, name: str, table: str):
    """Topic raw.<table> -> MinIO: <RAW_ROOT>/<table>/partition_date=YYYY-MM-DD/*.parquet, rows unchanged.

    partition_date = toDate(sys_create_date), so a daily reader picks exactly one folder per day.
    Read it back with Spark (spark.read.parquet(<RAW_ROOT>/<table>)): Spark honours
    _spark_metadata and ignores files of batches that never committed.
    """
    stream = read_raw_topic(spark, table).withColumn("partition_date", F.to_date("sys_create_date"))
    return (
        stream.coalesce(1)                      # 1 file per micro-batch and day, not 1 per Kafka partition
        .writeStream.queryName(name)
        .format("parquet")
        .option("path", f"{RAW_ROOT}/{table}")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{name}")
        .partitionBy("partition_date")
        .trigger(processingTime=TRIGGER)
        .start()
    )


def start_fact(spark: SparkSession, name: str, table: str, target: str, ddl: str, transform):
    """Topic raw.<table> -> dwh.<target>: create the table if missing, then transform() each micro-batch."""
    ensure_table(spark, ddl)
    return start_query(name, read_raw_topic(spark, table),
                       lambda df, batch_id: write_fact(transform(df), target))
