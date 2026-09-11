"""Reads the word-count parquet output (written to MinIO by word_count.py) and
loads it into a ClickHouse table via JDBC.

Run via spark-submit (see dags/example_minio_spark_pipeline.py for how Airflow
triggers this), or manually:

    docker compose exec spark-master \\
      spark-submit \\
      --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \\
      --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \\
      --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \\
      --conf spark.hadoop.fs.s3a.path.style.access=true \\
      /opt/spark_jobs/minio_to_clickhouse.py
"""
import os

from pyspark.sql import SparkSession

PROCESSED_BUCKET = os.environ.get("MINIO_BUCKET_PROCESSED", "processed")
CLICKHOUSE_JDBC_URL = os.environ["CLICKHOUSE_JDBC_URL"]
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "word_counts")


def main() -> None:
    spark = SparkSession.builder.appName("minio_to_clickhouse").getOrCreate()

    df = spark.read.parquet(f"s3a://{PROCESSED_BUCKET}/word_count")

    (
        # append: each run adds rows rather than replacing the table, matching
        # how a real batch-load pipeline behaves. Re-running this smoke test
        # repeatedly will duplicate rows - truncate the table between runs if
        # that matters for what you're testing.
        df.write.format("jdbc")
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("dbtable", CLICKHOUSE_TABLE)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )

    print(f"Loaded {df.count()} rows into ClickHouse table {CLICKHOUSE_TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
