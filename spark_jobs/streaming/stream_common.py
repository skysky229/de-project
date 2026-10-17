"""Shared helpers for the streaming jobs (spark_jobs/streaming/{raw,fact}).

Each job file = one target table = one streaming query, exposing
    start(spark) -> StreamingQuery
run_all.py starts them all inside one long-lived Spark application.

Two standard query shapes cover almost every job:
    start_raw_ingest(spark, name, table)        topic raw.<table> -> MinIO parquet, as-is
    start_fact(spark, name, table, target, ddl, transform)
                                                topic raw.<table> -> dwh.<target>, via the
                                                job's own transform()
Both read through read_raw_topic(), which drops invalid messages (see validation_error());
dq/dead_letter.py stores those in dwh.dq_dead_letter. Both are stateless. Raw ingest uses Spark's file sink (exactly-once: committed files are
listed in <path>/_spark_metadata, so a replayed batch never duplicates files); facts use
foreachBatch and ClickHouse absorbs replays. Only fact_trip (stream-stream join) keeps state.
Kafka offsets (and fact_trip's join state) are checkpointed in MinIO, so a restart
continues where it stopped.

Env: KAFKA_BOOTSTRAP (default kafka:9092), STREAM_CHECKPOINT_ROOT
(default s3a://processed/checkpoints/streaming), STREAM_RAW_ROOT (default
s3a://raw/logistics), STREAM_TRIGGER (default "30 seconds").
"""

import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.helpers import ensure_table, with_audit, write_append
from streaming.schemas import CASTS, REQUIRED, SCHEMAS

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINT_ROOT = os.environ.get("STREAM_CHECKPOINT_ROOT", "s3a://processed/checkpoints/streaming")
RAW_ROOT = os.environ.get("STREAM_RAW_ROOT", "s3a://raw/logistics")
TRIGGER = os.environ.get("STREAM_TRIGGER", "30 seconds")


def read_kafka(spark: SparkSession, topics: list) -> DataFrame:
    """The Kafka source: one row per message (key, value, topic, partition, offset, timestamp)."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", "earliest")  # only used on the very first start
        .option("failOnDataLoss", "false")  # topic retention may delete unread data
        .load()
    )


def parse_json(table: str):
    """The message value parsed with the table's schema, as struct column `m`."""
    return F.from_json(F.col("value").cast("string"), SCHEMAS[table]).alias("m")


def validation_error(table: str):
    """Why a parsed message (struct column `m`) is unusable, '' if it's fine.

    Unusable = not JSON, or a REQUIRED column is missing/empty, or doesn't cast to its type
    (e.g. a date that isn't one). Such a row would otherwise land in the lake with a NULL key,
    or break the fact insert (date_key is not Nullable).
    """
    casts = CASTS.get(table, {})
    # from_json gives a struct of all NULLs (not a NULL) for text that isn't a JSON object
    fields = [F.col(f"m.{name}").isNull() for name in SCHEMAS[table].fieldNames()]
    malformed = F.col("m").isNull() | reduce(lambda a, b: a & b, fields)
    problems = []
    for col in REQUIRED[table]:
        value = F.col(f"m.{col}")
        empty = value.isNull() | (F.trim(value.cast("string")) == "")
        problems.append(F.when(empty, F.lit(f"missing {col}")))
        if col in casts:
            problems.append(
                F.when(~empty & value.cast(casts[col]).isNull(), F.lit(f"invalid {col}"))
            )
    return F.when(malformed, F.lit("malformed JSON")).otherwise(F.concat_ws("; ", *problems))


def read_raw_topic(spark: SparkSession, table: str) -> DataFrame:
    """Topic raw.<table> -> rows shaped like the raw table (same columns as raw/history/<table>, incl. sys_create_date).

    sys_create_date = the Kafka record timestamp: when the platform received the event.
    It's stable across replays (unlike now()), so a replayed micro-batch produces identical rows.
    Invalid messages (validation_error) are dropped here; dq/dead_letter.py keeps them.
    """
    df = (
        read_kafka(spark, [f"raw.{table}"])
        .select(parse_json(table), F.col("timestamp").alias("sys_create_date"))
        .where(validation_error(table) == "")
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
        stream.coalesce(1)  # 1 file per micro-batch and day, not 1 per Kafka partition
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
    return start_query(
        name, read_raw_topic(spark, table), lambda df, batch_id: write_fact(transform(df), target)
    )
