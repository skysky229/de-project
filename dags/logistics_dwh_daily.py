"""Logistics DWH daily ETL: ClickHouse raw (`default`) -> Spark -> ClickHouse `dwh`.

One task per target table; each task spark-submits its own job file. Tasks
are grouped by layer into TaskGroups that run in order:
    dim  (spark_jobs/dwh/dim/<table>.py)   first: facts look up dimension keys
    fact (spark_jobs/dwh/fact/<table>.py)  after the whole dim group
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
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

JOBS_DIR = "/opt/airflow/spark_jobs/dwh"

# one entry per target table = one job file = one task
DIM_TABLES = ["dim_driver"]
FACT_TABLES = [
    "fact_trip", "fact_delivery_event", "fact_fuel_purchase", "fact_maintenance", "fact_safety_incident",
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
    for layer, tables in (("dim", DIM_TABLES), ("fact", FACT_TABLES), ("mart", MART_TABLES)):
        if not tables:
            continue
        with TaskGroup(group_id=layer) as group:
            for table in tables:
                spark_task(layer, table)
        groups.append(group)

    chain(*groups)
