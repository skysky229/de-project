"""Smoke-test DAG: raw text -> Spark -> MinIO -> Spark -> ClickHouse.

Confirms the Airflow -> Spark -> MinIO -> ClickHouse wiring works end to end.
Requires a Spark connection named `spark_default` pointing at
spark://spark-master:7077 (Admin -> Connections in the Airflow UI, or
`airflow connections add`).
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    # spark.hadoop.* configs ship to executors too (unlike plain env vars),
    # which is required since executors run in a separate container
    # (spark-worker) that doesn't inherit the driver's env.
    "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
    "spark.hadoop.fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
}

with DAG(
    dag_id="example_minio_spark_pipeline",
    description="raw text -> Spark -> MinIO -> Spark -> ClickHouse",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    submit_word_count = SparkSubmitOperator(
        task_id="submit_word_count",
        application="/opt/airflow/spark_jobs/word_count.py",
        conn_id="spark_default",
        conf=S3A_CONF,
        env_vars={
            "MINIO_BUCKET_RAW": "raw",
            "MINIO_BUCKET_PROCESSED": "processed",
        },
    )

    submit_minio_to_clickhouse = SparkSubmitOperator(
        task_id="submit_minio_to_clickhouse",
        application="/opt/airflow/spark_jobs/minio_to_clickhouse.py",
        conn_id="spark_default",
        conf=S3A_CONF,
        env_vars={
            "MINIO_BUCKET_PROCESSED": "processed",
            "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse:8123/{}".format(
                os.environ.get("CLICKHOUSE_DB", "default")
            ),
            "CLICKHOUSE_USER": os.environ["CLICKHOUSE_USER"],
            "CLICKHOUSE_PASSWORD": os.environ["CLICKHOUSE_PASSWORD"],
            "CLICKHOUSE_TABLE": "word_counts",
        },
    )

    submit_word_count >> submit_minio_to_clickhouse
