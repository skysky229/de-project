"""Streaming data quality: every 15 min, checks the speed layer (dwh.<fact>_streaming and
dwh.dq_dead_letter) with spark_jobs/dq/dq_streaming.py; results in dwh.dq_results.

    freshness / latency  is streaming alive and keeping up?
    dead letters         share of Kafka messages rejected by the streaming app
    row rules            the batch facts' rules (fact_rules.py) on the last hour of streamed rows

A plain Python task (the checks are SQL over ClickHouse HTTP, no Spark needed). Fails when an
error-severity check fails, e.g. no trip streamed for 5 min. Created paused: unpause it while
the streaming stack (kafka, spark-streaming, streamgen) runs, or every run fails on freshness.
See docs/data_quality.md.
"""
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DQ_DIR = "/opt/airflow/spark_jobs/dq"


def run_streaming_checks():
    sys.path.insert(0, DQ_DIR)
    import dq_streaming  # noqa: E402  (lives next to the batch DQ code, not in dags/)

    dq_streaming.run()


with DAG(
    dag_id="logistics_streaming_dq",
    description="Data-quality checks of the streaming layer (freshness, dead letters, row rules)",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    # each run checks "now", so retrying a failed run later would check a different window
    default_args={"retries": 0},
    tags=["dwh", "logistics", "streaming", "dq"],
) as dag:
    PythonOperator(task_id="dq_streaming", python_callable=run_streaming_checks)
