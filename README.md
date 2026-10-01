# Logistics Data Platform

A local, docker-composed data platform: a logistics data warehouse loaded by a daily
**batch** pipeline and a real-time **streaming** pipeline.

```
dataset/*.csv ─▶ MinIO raw/history/ ──┐                                   (one-time bootstrap)
streamgen ─▶ Kafka ─▶ spark-streaming ─┼─▶ MinIO raw/logistics/ ──┐        (live events)
                                       │                          ▼
                                       │        Airflow daily DAG (Spark) ─▶ ClickHouse dwh.<table>
                                       └─▶ ClickHouse dwh.<fact>_streaming          │
                                                         └──────────▶ views dwh.v_<fact> ◀┘
```

- **Raw data** lives in **MinIO**, partitioned by `partition_date` (the day the row was recorded).
- **The warehouse** is on **ClickHouse Cloud**, database `dwh`. ClickHouse only stores; all
  transformation happens in Spark.
- **Batch** (`logistics_dwh_daily`): a run for day D reads only the `partition_date=D` folders.
- **Streaming**: every 30 s it lands Kafka events in MinIO and writes the facts to `dwh.<fact>_streaming`.
- **Views** `dwh.v_<fact>` serve batch rows up to the last day batch has loaded, plus newer
  streaming rows. Query these.

## Repository

```
dags/              Airflow DAGs: logistics_bootstrap (one-time), logistics_dwh_daily (daily batch)
spark_jobs/batch/      batch ETL: bootstrap/, dims/, facts/, common.py (one file = one table = one task)
spark_jobs/streaming/  Structured Streaming: raw/ (Kafka -> MinIO), fact/ (Kafka -> dwh.*_streaming)
scripts/           fact_views.sql (the views), generate_fact_views.py, streaming_check.sh, streaming_reset.sh
docker/            one folder per service (Dockerfile + compose file); streamgen/ = event generator
docs/              how_to_add_a_table.md, streaming_design.md, streaming_test_runbook.md, architecture.drawio
dataset/           the original CSVs (input of the bootstrap)
webapp/            not started
```

## Getting started

**Prerequisites:** Docker (Rancher Desktop) with at least **12 GB RAM / 6 CPUs** for the VM, and
a ClickHouse Cloud service.

1. **Configure:** `cp .env.example .env`, then fill in `DWH_CH_URL` / `DWH_CH_USER` /
   `DWH_CH_PASSWORD`. If `quay.io` images can't be pulled, set `MINIO_IMAGE=minio/minio` and
   `MINIO_MC_IMAGE=minio/mc`.
2. **Start the core services:**
   ```bash
   docker compose build
   docker compose up -d minio minio-init spark-master spark-worker airflow-webserver airflow-scheduler
   ```
3. **First time only:** register Spark in Airflow:
   ```bash
   docker compose exec airflow-webserver airflow connections add spark_default \
     --conn-type spark --conn-host spark://spark-master --conn-port 7077
   ```
4. **Bootstrap the raw history (once):** in Airflow, trigger the `logistics_bootstrap` DAG.
   It copies `dataset/*.csv` into MinIO and builds `raw/history/`. MinIO keeps the data in its
   Docker volume, so it survives restarts.
5. **Backfill the warehouse:** trigger `logistics_dwh_daily` with conf
   `{"start_date": "2000-01-01", "end_date": "2025-01-31"}` (about 30 min). Unpause the DAG
   for daily runs.
6. **Start streaming:** `docker compose up -d kafka spark-streaming streamgen`.
7. **Create the views (once):** run every statement of `scripts/fact_views.sql` in ClickHouse.

## Where to look

| What | Where |
|---|---|
| Airflow (admin login in `.env`) | http://localhost:8080 |
| Spark master (workers, running apps, Structured Streaming tab) | http://localhost:8081 |
| MinIO console (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`) | http://localhost:9001 |
| Superset | http://localhost:8088 |
| Kafka, from the host | `localhost:9094` |
| ClickHouse Cloud, from DBeaver or another SQL client | `jdbc:clickhouse://<host>:8443/default?ssl=true&compress=0` |

The `compress=0` in that URL is required for older JDBC drivers (0.5/0.6).

## More documentation

- **Adding or changing a batch table:** [docs/how_to_add_a_table.md](docs/how_to_add_a_table.md)
- **Streaming design:** [docs/streaming_design.md](docs/streaming_design.md)
- **Testing streaming end to end, plus the check and reset scripts:**
  [docs/streaming_test_runbook.md](docs/streaming_test_runbook.md)
- **After changing a fact's columns:** change the batch job and its copy in
  `spark_jobs/streaming/fact/`, run `python3 scripts/generate_fact_views.py`, then re-run
  `scripts/fact_views.sql`.