"""Logistics bootstrap (one-time, manual trigger): CSVs -> partitioned raw history in MinIO.

    upload_csv_to_minio   dataset/*.csv -> s3a://raw/csv/<table>.csv            (spark_jobs/batch/bootstrap/s3_upload.py)
    build_raw_history     raw/csv/ -> s3a://raw/history/<table>/partition_date=YYYY-MM-DD/
                          (spark_jobs/batch/bootstrap/build_raw_history.py)

Run it once on a fresh MinIO (or after changing the CSVs), then backfill logistics_dwh_daily.
Both tasks overwrite what they write, so re-running is safe. MinIO keeps the data in its
docker volume, so it survives restarts; no need to run this at every start-up.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BUCKET_RAW = os.environ.get("MINIO_BUCKET_RAW", "raw")

with DAG(
    dag_id="logistics_bootstrap",
    description="One-time: dataset CSVs -> MinIO raw/csv -> partitioned raw/history",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "logistics", "bootstrap"],
) as dag:
    upload_csv_to_minio = BashOperator(
        task_id="upload_csv_to_minio",
        bash_command="python /opt/airflow/spark_jobs/batch/bootstrap/s3_upload.py",
        env={
            "DATA_DIR": "/opt/airflow/dataset",
            "MINIO_ENDPOINT": os.environ["AWS_ENDPOINT_URL"],
            "MINIO_ROOT_USER": os.environ["AWS_ACCESS_KEY_ID"],
            "MINIO_ROOT_PASSWORD": os.environ["AWS_SECRET_ACCESS_KEY"],
            "MINIO_BUCKET_RAW": BUCKET_RAW,
            "MINIO_CSV_PREFIX": "csv",
        },
        append_env=True,
    )

    build_raw_history = SparkSubmitOperator(
        task_id="build_raw_history",
        application="/opt/airflow/spark_jobs/batch/bootstrap/build_raw_history.py",
        conn_id="spark_default",
        name="build_raw_history",
        conf={
            "spark.hadoop.fs.s3a.endpoint": os.environ["AWS_ENDPOINT_URL"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
            "spark.hadoop.fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
        },
        env_vars={
            "DWH_RAW_CSV_ROOT": f"s3a://{BUCKET_RAW}/csv",
            "DWH_RAW_HISTORY_ROOT": os.environ.get("DWH_RAW_HISTORY_ROOT", f"s3a://{BUCKET_RAW}/history"),
        },
    )

    upload_csv_to_minio >> build_raw_history
