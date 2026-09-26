# Data Platform

A small, local, docker-composed data platform for learning/experimentation.
This README is the map of the repo — read it before adding anything, and keep
it in sync with what's actually running.

## Compose files — three independent stacks, or all together

Each tool owns its Dockerfile *and* its compose file, in its own directory
under `docker/`. The root `docker-compose.yml` is a thin convenience file that
`include:`s all five:

| File                                   | Contains                              | Self-contained? |
|-------------------------------------------|----------------------------------------|------------------|
| `docker/airflow/docker-compose.yml`     | `postgres`, `airflow-init`, `airflow-webserver`, `airflow-scheduler` | Yes — Postgres is bundled since it's only ever Airflow's metadata DB |
| `docker/spark/docker-compose.yml`       | `spark-master`, `spark-worker`        | Yes — has no hard dependency on MinIO/ClickHouse to start |
| `docker/minio/docker-compose.yml`       | `minio`, `minio-init`                 | Yes |
| `docker/clickhouse/docker-compose.yml`  | `clickhouse`                          | Yes |
| `docker/superset/docker-compose.yml`    | `superset-init`, `superset`            | Yes — own SQLite metadata DB, so it doesn't need Postgres. The pre-wired ClickHouse *connection* just won't be queryable until `clickhouse` is also up. |
| `docker-compose.yml` (repo root)        | `include:`s all five above              | Convenience entrypoint for the full stack |

Everything together, from the repo root:
```bash
docker compose up -d
```

Any one standalone — run from the repo root, with `--env-file .env`:
```bash
docker compose --env-file .env -f docker/airflow/docker-compose.yml up -d
docker compose --env-file .env -f docker/spark/docker-compose.yml up -d
```

## Folder structure

```
.
├── docker-compose.yml                 # include:s the five files below — full-stack convenience
├── .env.example                        # copy to .env before running; real secrets go in .env (gitignored)
├── docker/
│   ├── airflow/                         # custom Airflow image + its compose file
│   │   ├── Dockerfile                     # base + extra pip packages + JDK + s3a/JDBC jars
│   │   ├── requirements.txt               # apache-airflow-providers-apache-spark, -amazon, boto3, pyspark
│   │   └── docker-compose.yml             # postgres + airflow-init/webserver/scheduler (standalone-runnable)
│   ├── spark/                            # custom Spark image + its compose file
│   │   ├── Dockerfile                     # bitnamilegacy/spark + hadoop-aws + clickhouse-jdbc jars
│   │   └── docker-compose.yml             # spark-master + spark-worker (standalone-runnable)
│   ├── minio/                            # no custom image (official minio/minio), just its compose file
│   │   └── docker-compose.yml             # minio + minio-init (standalone-runnable)
│   ├── clickhouse/                       # no custom image (official clickhouse-server), just its compose file
│   │   ├── docker-compose.yml             # clickhouse (standalone-runnable)
│   │   └── init/001-create-tables.sql     # runs on first startup: creates default.word_counts
│   └── superset/                         # custom Superset image + its compose file
│       ├── Dockerfile                     # base + clickhouse-connect (SQLAlchemy dialect for ClickHouse)
│       ├── requirements.txt               # clickhouse-connect
│       └── docker-compose.yml             # superset-init + superset (standalone-runnable; own SQLite metadata DB)
├── dags/                                # Airflow DAG definitions, mounted into the airflow containers
│   ├── example_minio_spark_pipeline.py   # smoke test: Airflow -> Spark -> MinIO -> Spark -> ClickHouse
│   └── logistics_dwh_daily.py            # logistics DWH: one Spark task per dwh table
├── dwh/
│   └── migrations/
│       └── 001_add_sys_create_date.sql   # adds sys_create_date to the 14 raw tables in ClickHouse `default`
├── docs/
│   ├── dw_design_review.md               # DWH table design (your design + review notes)
│   ├── spark_etl_plan.md                 # how the Spark ETL reads, transforms, writes, optimizes
│   └── how_to_add_a_table.md             # step-by-step guide + template for a new dim/fact/mart job
├── dataset/                             # original CSVs + schema notes (source of the raw ClickHouse tables)
├── spark_jobs/                          # PySpark application code, mounted into airflow + spark containers
│   ├── word_count.py                    # task 1: raw text -> MinIO -> word-count parquet -> MinIO
│   └── minio_to_clickhouse.py            # task 2: reads that parquet from MinIO, loads into ClickHouse via JDBC
├── upload_jobs/                          # PySpark application code, mounted into airflow + spark containers
│   ├── s3_upload.py                    # task 1: Raw csv files upload -> MinIO
│
│   ├── minio_to_clickhouse.py            # task 2: reads that parquet from MinIO, loads into ClickHouse via JDBC
│   └── dwh/                              # logistics DWH jobs: one file = one target table = one task
│       ├── common.py                      # shared helpers, shipped with --py-files: JDBC read/write,
│       │                                  #   window args, dedup, surrogate keys, Unknown member
│       ├── dim/                           # TaskGroup `dim` (runs first)
│       │   └── dim_driver.py              #   default.drivers -> dwh.dim_driver
│       ├── fact/                          # TaskGroup `fact` (after dim) - none yet
│       └── mart/                          # TaskGroup `mart` (after fact) - none yet
└── webapp/                              # NOT STARTED — see webapp/README.md
```

**Where things run:**
- `dags/*.py` is mounted into the Airflow containers at `/opt/airflow/dags`.
- `spark_jobs/*.py` is mounted into both Airflow and Spark containers at
  `/opt/airflow/spark_jobs` and `/opt/spark_jobs` respectively, so Airflow can
  reference the same script path it hands to `SparkSubmitOperator`.
- MinIO data lives in the `minio_data` docker volume, organized into two
  buckets created on startup by `minio-init`: `raw` and `processed`.
- ClickHouse data lives in the `clickhouse_data` docker volume. The
  `default.word_counts` table is created on first startup from
  `docker/clickhouse/init/001-create-tables.sql` (any `.sql`/`.sh` file
  dropped into that folder runs the same way).
- Superset's own metadata (users, dashboards, saved queries, **and** the
  registered database connections, ClickHouse included) lives in the
  `superset_home` docker volume as a SQLite file.

## Services

| Service            | Image                    | Purpose                                      | Exposed port         |
|---------------------|--------------------------|-----------------------------------------------|-----------------------|
| `postgres`          | postgres:15              | Airflow metadata DB                           | -                     |
| `airflow-init`       | build: docker/airflow    | one-shot: migrate DB, create admin user       | -                     |
| `airflow-webserver`  | build: docker/airflow    | Airflow UI                                    | 8080                  |
| `airflow-scheduler`  | build: docker/airflow    | Airflow scheduler                             | -                     |
| `spark-master`       | build: docker/spark      | Spark cluster master                          | 7077 (RPC), 8081 (UI) |
| `spark-worker`       | build: docker/spark      | Spark worker (2 cores / 2G, adjust as needed) | -                     |
| `minio`              | minio/minio              | S3-compatible object storage                  | 9000 (API), 9001 (console) |
| `minio-init`         | minio/mc                 | one-shot: create `raw` / `processed` buckets  | -                     |
| `clickhouse`         | clickhouse/clickhouse-server:24.8 | queryable table store, loaded from MinIO via Spark | 8123 (HTTP/JDBC), 9002 (native, remapped) |
| `superset-init`      | build: docker/superset   | one-shot: db upgrade, create admin, register ClickHouse connection | - |
| `superset`           | build: docker/superset   | Superset BI UI                                | 8088                  |

Note: Spark master's UI is remapped to host port **8081** (Airflow webserver
uses 8080). ClickHouse's native TCP port is remapped to **9002** on the host
(MinIO's S3 API uses 9000) — inside the Docker network each service is still
reached on its own real port (`clickhouse:9000`, `minio:9000`).

## Getting started

```bash
cp .env.example .env     
docker compose up -d --build
```

- Airflow UI: http://localhost:8080 (login from `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` in `.env`)
- Spark master UI: http://localhost:8081
- MinIO console: http://localhost:9001 (login from `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`)
- ClickHouse: `docker compose exec clickhouse clickhouse-client` for a SQL
  shell, or HTTP at http://localhost:8123 (login from `CLICKHOUSE_USER` /
  `CLICKHOUSE_PASSWORD` in `.env`)
- Superset UI: http://localhost:8088 (login from `SUPERSET_ADMIN_USER` /
  `SUPERSET_ADMIN_PASSWORD` in `.env`) — a "ClickHouse" connection is already
  registered under Data → Databases, ready to build charts/SQL Lab queries
  against `default.word_counts` once the pipeline below has run at least once.

First time only: register the Spark connection so `SparkSubmitOperator` can
find the cluster (Admin → Connections → `+` in the UI, or via CLI):
```bash
docker compose exec airflow-webserver bash -c \
  "airflow connections add spark_default --conn-type spark --conn-host spark://spark-master --conn-port 7077"
```

Then trigger the `example_minio_spark_pipeline` DAG (UI, or
`airflow dags trigger example_minio_spark_pipeline`). It runs two chained
tasks:
1. `spark_jobs/word_count.py` — writes sample text to `s3a://raw/sample/`,
   reads it back, and writes word counts as parquet to
   `s3a://processed/word_count/`.
2. `spark_jobs/minio_to_clickhouse.py` — reads that parquet back from MinIO
   and loads it into the `default.word_counts` ClickHouse table via JDBC.

Check the result with:
```bash
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM default.word_counts"
```

# Data processing jobs
Run the job to process uploading .csv files to MinIO
```bash
python3 upload_jobs/s3_upload.py
```
* Please set up the file directory in the .env file
## Logistics DWH pipeline

`dags/logistics_dwh_daily.py` loads the star schema from the raw tables on the
ClickHouse Cloud service set in `.env` (`DWH_CH_*`):
raw `DWH_SOURCE_DB` (`default`) -> Spark -> `DWH_TARGET_DB` (`dwh`).
Design: `docs/dw_design_review.md`. Plan: `docs/spark_etl_plan.md`.
**Adding a table: `docs/how_to_add_a_table.md`.**

- One task per target table, each running its own file under
  `spark_jobs/dwh/{dim,fact,mart}/`, grouped into TaskGroups `dim -> fact -> mart`.
- Spark does every transformation; ClickHouse only filters on read and stores.
- Each run reads raw rows created on the run date
  (`toDate(sys_create_date) = ds`; raw tables are treated as append-only).
- Targets are `ReplacingMergeTree(src_sys_create_date)` tables ordered by the
  business key (`driver_id`, `trip_id`, ...). Jobs only append; the newest
  *source* version wins, so re-runs and backfills are safe. `etl_loaded_date`
  records when the row was written. Query with `FINAL` for exact results.
- Backfill: trigger the DAG with conf
  `{"start_date": "2000-01-01", "end_date": "2024-12-31"}`.

Needs the Spark cluster up (`docker compose up -d spark-master spark-worker`).

Connecting a SQL client (DBeaver etc.) to the ClickHouse Cloud service: use
`jdbc:clickhouse://<host>:8443/default?ssl=true&compress=0` (HTTPS port 8443;
`compress=0` is required for older JDBC drivers such as 0.5/0.6).
