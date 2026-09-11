"""Smoke-test job: write a small dataset to MinIO, read it back, and count words.

Run via spark-submit (see dags/example_minio_spark_pipeline.py for how Airflow
triggers this), or manually:

    docker compose exec spark-master \\
      spark-submit \\
      --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \\
      --conf spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER \\
      --conf spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD \\
      --conf spark.hadoop.fs.s3a.path.style.access=true \\
      /opt/spark_jobs/word_count.py
"""
import os

from pyspark.sql import SparkSession

RAW_BUCKET = os.environ.get("MINIO_BUCKET_RAW", "raw")
PROCESSED_BUCKET = os.environ.get("MINIO_BUCKET_PROCESSED", "processed")

SAMPLE_LINES = [
    "data platforms move data from source to sink",
    "airflow orchestrates spark spark processes data",
    "minio stores data as s3 compatible objects",
]


def main() -> None:
    spark = SparkSession.builder.appName("word_count").getOrCreate()

    raw_path = f"s3a://{RAW_BUCKET}/sample/lines.txt"
    spark.createDataFrame([(line,) for line in SAMPLE_LINES], ["value"]).write.mode(
        "overwrite"
    ).text(raw_path)

    lines = spark.read.text(raw_path)
    counts = (
        lines.rdd.flatMap(lambda row: row.value.split(" "))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    processed_path = f"s3a://{PROCESSED_BUCKET}/word_count"
    spark.createDataFrame(counts, ["word", "count"]).write.mode("overwrite").parquet(
        processed_path
    )

    print(f"Wrote word counts to {processed_path}")
    spark.stop()


if __name__ == "__main__":
    main()
