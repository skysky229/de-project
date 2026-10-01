"""Logistics DWH daily ETL: MinIO raw parquet -> Spark -> ClickHouse `dwh`.

One task per target table; each task spark-submits its own job file. Tasks
are grouped by layer into TaskGroups that run in order:
    dims  (spark_jobs/batch/dims/<table>.py)    first: facts depend on their keys
    facts (spark_jobs/batch/facts/<table>.py)   after the whole dimension group
    mart (spark_jobs/batch/mart/<table>.py)  after the whole fact group
Task ids are <group>.<table>, e.g. dims.dim_driver.

Each run reads only the raw partitions of its logical date ({{ ds }}):
s3a://raw/history/<table>/partition_date=ds/ (bootstrap, see logistics_bootstrap) and
s3a://raw/logistics/<table>/partition_date=ds/ (streamed data).
Backfill: trigger manually with conf {"start_date": "2000-01-01", "end_date": "2025-01-31"}.
Every job appends into a ReplacingMergeTree keyed on the business key, so
re-running any task or date is safe.

Requires the `spark_default` connection (spark://spark-master:7077), the MinIO
credentials (AWS_*) and DWH_CH_* in the Airflow environment (see .env.example),
and the raw history in MinIO (run the logistics_bootstrap DAG once).
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

JOBS_DIR = "/opt/airflow/spark_jobs/batch"

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
    name: os.environ[name] for name in ("DWH_CH_URL", "DWH_CH_USER", "DWH_CH_PASSWORD", "DWH_TARGET_DB")
}
DWH_ENV.update({
    "DWH_RAW_HISTORY_ROOT": os.environ.get("DWH_RAW_HISTORY_ROOT", "s3a://raw/history"),
    "DWH_RAW_STREAM_ROOT": os.environ.get("DWH_RAW_STREAM_ROOT", "s3a://raw/logistics"),
})
# spark.hadoop.* confs reach the executors too, which read MinIO directly
S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint": os.environ["AWS_ENDPOINT_URL"],
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
    "spark.hadoop.fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
}

with DAG(
    dag_id="logistics_dwh_daily",
    description="MinIO raw partitions -> Spark -> ClickHouse dwh (one task per table)",
    start_date=datetime(2022, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    # every task is a Spark driver inside the scheduler container: 2 at a time keeps it responsive
    max_active_tasks=2,
    # ClickHouse Cloud idles when unused and refuses connections for a short while when it
    # wakes up (NoHttpResponse / SSL errors): retry instead of failing the whole run
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        # empty = use the run's logical date; set both for a backfill
        "start_date": Param("", type="string", description="YYYY-MM-DD, backfill only"),
        "end_date": Param("", type="string", description="YYYY-MM-DD, backfill only"),
    },
    tags=["dwh", "logistics", "spark"],
) as dag:

    def spark_task(layer: str, table: str) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=table,
            application=f"{JOBS_DIR}/{layer}/{table}.py",
            py_files=f"{JOBS_DIR}/common.py",
            conn_id="spark_default",
            # 1 core per job: a TaskGroup's jobs share the worker instead of the first one taking
            # every core while the rest wait (the streaming app keeps its own 2 cores)
            conf={**S3A_CONF, "spark.cores.max": "1", "spark.executor.memory": "1g"},
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

    chain(*groups)
