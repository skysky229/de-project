"""Logistics DWH daily ETL: ClickHouse raw (`default`) -> Spark -> ClickHouse `dwh`.

One task per target table; each task spark-submits its own job file. Tasks
are grouped by layer into TaskGroups that run in order:
    dims  (spark_jobs/dwh/dims/<table>.py)    first: facts depend on their keys
    facts (spark_jobs/dwh/facts/<table>.py)   after the whole dimension group
    mart (spark_jobs/dwh/mart/<table>.py)  after the whole fact group
Task ids are <group>.<table>, e.g. dim.dim_driver.

Each run processes raw rows created on the run's logical date ({{ ds }}).
Backfill: trigger manually with conf {"start_date": "2000-01-01", "end_date": "2024-12-31"}.
Every job appends into a ReplacingMergeTree keyed on the business key, so
re-running any task or date is safe.

Requires the `spark_default` connection (spark://spark-master:7077) and
DWH_CH_* in the Airflow environment (see .env.example).
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

JOBS_DIR = "/opt/airflow/spark_jobs/dwh"

# one entry per target table = one job file = one task
DIM_TABLES = [
    "dim_date", "dim_driver", "dim_truck", "dim_trailer", "dim_customer",
    "dim_facility", "dim_route",
]
FACT_TABLES = [
    "fact_trip", "fact_delivery_event", "fact_fuel_purchase", "fact_maintenance",
    "fact_safety_incident", "fact_driver_monthly", "fact_truck_monthly",
]
MART_TABLES: list = []

DWH_ENV = {
    name: os.environ[name]
    for name in ("DWH_CH_URL", "DWH_CH_USER", "DWH_CH_PASSWORD", "DWH_SOURCE_DB", "DWH_TARGET_DB")
}

with DAG(
    dag_id="logistics_dwh_daily",
    description="ClickHouse raw -> Spark -> ClickHouse dwh (one task per table)",
    start_date=datetime(2022, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    params={
        # empty = use the run's logical date; set both for a backfill
        "start_date": Param("", type="string", description="YYYY-MM-DD, backfill only"),
        "end_date": Param("", type="string", description="YYYY-MM-DD, backfill only"),
    },
    tags=["dwh", "logistics", "spark"],
) as dag:

    upload_csv_to_minio = BashOperator(
        task_id="upload_csv_to_minio",
        bash_command="python /opt/airflow/upload_jobs/s3_upload.py",
        env={
            "DATA_DIR": "/opt/airflow/dataset",
            "MINIO_ENDPOINT": os.environ["AWS_ENDPOINT_URL"],
            "MINIO_ROOT_USER": os.environ["AWS_ACCESS_KEY_ID"],
            "MINIO_ROOT_PASSWORD": os.environ["AWS_SECRET_ACCESS_KEY"],
            "MINIO_BUCKET_RAW": os.environ.get("MINIO_BUCKET_RAW", "raw"),
        },
        append_env=True,
    )

    load_minio_to_raw_clickhouse = SparkSubmitOperator(
        task_id="load_minio_to_raw_clickhouse",
        application="/opt/airflow/spark_jobs/minio_to_raw_clickhouse.py",
        conn_id="spark_default",
        name="load_minio_to_raw_clickhouse_{{ ds }}",
        conf={
            "spark.hadoop.fs.s3a.endpoint": os.environ["AWS_ENDPOINT_URL"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
            "spark.hadoop.fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
        },
        env_vars={**DWH_ENV, "MINIO_BUCKET_RAW": os.environ.get("MINIO_BUCKET_RAW", "raw")},
    )

    def spark_task(layer: str, table: str) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=table,
            application=f"{JOBS_DIR}/{layer}/{table}.py",
            py_files=f"{JOBS_DIR}/common.py",
            conn_id="spark_default",
            name=f"{table}_{{{{ ds }}}}",
            application_args=[
                "--start-date", "{{ params.start_date or ds }}",
                "--end-date", "{{ params.end_date or ds }}",
            ],
            env_vars=DWH_ENV,
        )

    # one TaskGroup per layer; the groups run in sequence dim >> fact >> mart
    groups = []
    for layer, tables in (("dims", DIM_TABLES), ("facts", FACT_TABLES), ("mart", MART_TABLES)):
        if not tables:
            continue
        with TaskGroup(group_id=layer) as group:
            for table in tables:
                spark_task(layer, table)
        groups.append(group)

    upload_csv_to_minio >> load_minio_to_raw_clickhouse >> groups[0]
    chain(*groups)
