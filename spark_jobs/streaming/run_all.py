"""Starts every streaming query in ONE long-lived Spark application.

One application (instead of one per table) so the queries share executors on the
small cluster. Each job module stays one-table-per-file and exposes start(spark).
If any query dies, the application exits non-zero and the `spark-streaming`
container restarts it; checkpoints make every query resume where it stopped.

Add a job: write streaming/<raw|fact>/<name>.py with start(spark) and list it in JOBS.

    spark-submit --master spark://spark-master:7077 /opt/spark_jobs/streaming/run_all.py
"""

import importlib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SPARK_JOBS = os.path.dirname(HERE)
# makes the `streaming.*` package importable; streaming never imports the batch code (spark_jobs/batch)
sys.path.insert(0, SPARK_JOBS)

from streaming.helpers import get_spark  # noqa: E402

JOBS = [
    # raw: topic -> MinIO raw/logistics/<table>/partition_date=.../, as-is
    "streaming.raw.raw_trips",
    "streaming.raw.raw_loads",
    "streaming.raw.raw_delivery_events",
    "streaming.raw.raw_fuel_purchases",
    "streaming.raw.raw_maintenance_records",
    "streaming.raw.raw_safety_incidents",
    # fact: topic(s) -> dwh.fact_*_streaming
    "streaming.fact.fact_trip",  # stream-stream join trips ⋈ loads (stateful)
    "streaming.fact.fact_delivery_event",
    "streaming.fact.fact_fuel_purchase",
    "streaming.fact.fact_maintenance",
    "streaming.fact.fact_safety_incident",
]


def main():
    spark = get_spark("logistics_streaming")
    for module_name in JOBS:
        query = importlib.import_module(module_name).start(spark)
        print(f"started query {query.name} ({module_name})", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
