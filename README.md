# Mini Data Platform

A small, local, docker-composed data platform for learning/experimentation.
This README is the map of the repo — read it before adding anything, and keep
it in sync with what's actually running.

## Status

- **Working, tested end-to-end:** Airflow (orchestration) + Spark (processing) +
  MinIO (S3-compatible storage) + ClickHouse (queryable table store) +
  Superset (BI, pre-wired to ClickHouse), wired together with a smoke-test
  pipeline (`dags/example_minio_spark_pipeline.py`, two chained tasks)
  verified to run successfully via the Airflow scheduler, not just manually —
  including the data actually landing in ClickHouse and Superset's ClickHouse
  connection confirmed live (via its own REST API, not just a config check)
  while building this.
- **Not started:** `webapp/` — deliberately deferred, see `webapp/README.md`.

## Architecture

```
                 ┌─────────────┐
                 │   Airflow   │  orchestrates DAGs (dags/)
                 │ (webserver, │
                 │  scheduler) │
                 └──────┬──────┘
                         │ SparkSubmitOperator (two tasks, chained)
                         ▼
                 ┌─────────────┐        ┌──────────────┐        ┌─────────────┐        ┌─────────────┐
                 │    Spark    │◄──────►│    MinIO     │──────► │  ClickHouse │◄────── │  Superset   │
                 │(master+wkr) │  s3a   │ (raw/,       │  JDBC  │ (word_counts│  SQL   │ (BI, pre-   │
                 └─────────────┘        │  processed/) │        │  table)     │        │  wired conn)│
                                         └──────────────┘        └─────────────┘        └─────────────┘
                 ┌─────────────┐
                 │  Postgres   │  Airflow's metadata DB only —
                 │             │  not a warehouse for pipeline output
                 └─────────────┘
```

Task 1 (`submit_word_count`) writes sample text to MinIO, reads it back, and
writes word-count parquet back to MinIO. Task 2
(`submit_minio_to_clickhouse`) reads that parquet from MinIO and loads it into
ClickHouse via JDBC — the read-from-MinIO-write-to-ClickHouse pipeline.
Superset's own metadata DB is SQLite (`superset_home` volume) — it's a BI
tool sitting on top of ClickHouse, not another warehouse.

## Compose files — three independent stacks, or all together

Each tool owns its Dockerfile *and* its compose file, in its own directory
under `docker/`. The root `docker-compose.yml` is a thin convenience file that
`include:`s all three:

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

Any one standalone — **must** be run from the repo root, with `--env-file .env`
(this is the one gotcha of the split: `-f docker/airflow/docker-compose.yml`
alone makes Compose look for `.env` inside `docker/airflow/`, where it
doesn't exist, silently blanking out every `${VAR}`):
```bash
docker compose --env-file .env -f docker/airflow/docker-compose.yml up -d
docker compose --env-file .env -f docker/spark/docker-compose.yml up -d
```
Do **not** add `--project-directory .` to "fix" this — it also shifts where
`build:`/`volumes:` relative paths resolve from, breaking them instead
(verified: this was tried and confirmed broken while building this scaffold).
`--env-file .env` alone is sufficient and doesn't have that problem, since
build/volume paths are always resolved relative to each compose file's own
directory regardless of `--env-file`.

A standalone run also gets its own Compose **project name** (defaulting to
the containing folder's name, e.g. `airflow`, vs. `project` for the combined
stack via the root file) — meaning separate volumes, so a standalone
Postgres starts with a fresh DB rather than sharing the combined stack's. Add
`-p project` to the command above if you want it to share state with the
combined stack instead.

All five files declare `networks: default: name: de-platform`, so whichever
one starts first creates that network and the others join it — meaning
cross-file service-name resolution (Airflow → `spark-master`, Spark → `minio`,
Superset → `clickhouse`) works whether you start them together or separately,
as long as they're all run from this same directory. The actual pipeline
(Airflow submitting to Spark, Spark reading/writing MinIO and ClickHouse,
Superset querying ClickHouse) obviously needs the relevant services running
at once — the split is about independent start/iterate, not independent
operation.

## Folder structure

```
.
├── docker-compose.yml                 # include:s the five files below — full-stack convenience
├── .env.example                        # copy to .env before running; real secrets go in .env (gitignored)
├── docker/
│   ├── airflow/                         # custom Airflow image + its compose file
│   │   ├── Dockerfile                     # base + extra pip packages + JDK + s3a/JDBC jars
│   │   ├── requirements.txt               # apache-airflow-providers-apache-spark, -amazon, boto3, pyspark
│   │   └── docker-compose.yml             # postgres + airflow-init/webserver/scheduler (standalone-runnable, see gotcha above)
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
│   └── example_minio_spark_pipeline.py   # smoke test: Airflow -> Spark -> MinIO -> Spark -> ClickHouse
├── spark_jobs/                          # PySpark application code, mounted into airflow + spark containers
│   ├── word_count.py                    # task 1: raw text -> MinIO -> word-count parquet -> MinIO
│   └── minio_to_clickhouse.py            # task 2: reads that parquet from MinIO, loads into ClickHouse via JDBC
└── webapp/                              # NOT STARTED — see webapp/README.md
```

Note: paths inside each `docker/<tool>/docker-compose.yml` (build context,
bind mounts) are written relative to that tool's own directory, e.g.
`build: .` and `../../dags`, `../../spark_jobs` — not the repo root, since
that's what each compose file resolves relative paths against.

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
  dropped into that folder runs the same way — this is the official
  `clickhouse-server` image's init-script convention, same idea as
  `minio-init`'s `mc mb`, just built into the image instead of a separate
  one-shot container).
- Superset's own metadata (users, dashboards, saved queries, **and** the
  registered database connections, ClickHouse included) lives in the
  `superset_home` docker volume as a SQLite file — no separate Postgres for
  Superset, to keep the footprint down.

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

Note: Spark master's UI is remapped to host port **8081** because 8080 is
taken by the Airflow webserver. ClickHouse's native TCP port is remapped to
**9002** on the host because its default (9000) is taken by MinIO's S3 API —
inside the Docker network the containers still talk over each service's real
port (`clickhouse:9000`, `minio:9000` — no conflict there since they're
different container hostnames).

## Version pinning — read this before touching either Dockerfile

This tripped us up while building the platform, so it's written down instead
of left to be rediscovered:

1. **`bitnami/spark` tags are gone from Docker Hub** (retired in 2025). We use
   `bitnamilegacy/spark:<version>` instead. If that also stops resolving,
   switch to `apache/spark:3.5.x` and adjust `docker/spark/Dockerfile`'s
   master/worker entrypoint handling (it currently relies on Bitnami's
   `SPARK_MODE` env var).
2. **The Spark image version and the Airflow image's `pyspark` version must
   match exactly.** `spark-submit` runs a JVM driver process — in this setup
   that driver runs *inside the Airflow container* (client deploy mode, the
   default), separate from the Spark cluster's executors. If the driver's
   pyspark/Spark jars don't exactly match the executors' Spark version, jobs
   fail with `InvalidClassException: ... local class incompatible:
   serialVersionUID mismatch`. Airflow 2.9.3's own constraints file
   (`constraints-2.9.3`) already pins `pyspark==3.5.1`, so
   `docker/spark/Dockerfile` is pinned to `bitnamilegacy/spark:3.5.1` to
   match — don't bump one without the other.
3. **Extra pip packages must be installed against Airflow's constraints
   file**, not with hand-picked versions. `apache-airflow-providers-amazon`
   at a recent version pulls in Airflow 3.x as a transitive dependency, which
   silently replaces this image's Airflow 2.9.3 and breaks the CLI entirely
   (`airflow: command not found` — the console script disappears because the
   package identity changes). `docker/airflow/Dockerfile` installs with
   `--constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt`
   specifically to prevent this.
4. **The Airflow image needs a JDK** (`default-jre-headless`) even though
   Airflow itself is pure Python — `spark-submit`'s driver process is a JVM.
   The base `apache/airflow` image has none by default.
5. **S3A jars (`hadoop-aws`, `aws-java-sdk-bundle`) are needed on *both*
   sides** — the Spark image (for executors) and the Airflow image (for the
   driver, which does the initial `Path.getFileSystem()` resolution before
   any executor work happens). Both are pinned to `hadoop-aws-3.3.4`,
   matching the `hadoop-client-api` version pyspark 3.5.1 bundles.
6. **S3A credentials must be passed via `spark.hadoop.fs.s3a.access.key` /
   `secret.key` Spark confs**, not plain env vars on the Airflow container.
   `spark.hadoop.*` Spark confs propagate to executors automatically;
   ordinary env vars set on the Airflow container do not reach the
   spark-worker container where executors actually run.
7. **The ClickHouse JDBC jar (`clickhouse-jdbc-<version>-all.jar`) is needed
   on both sides too**, same reasoning as the S3A jars — `DataFrameWriter.jdbc`
   /`.format("jdbc")` opens a connection from the driver (to resolve/validate
   schema) as well as from each executor (to actually write rows). Use the
   `all` classifier (not `shaded-all`, which doesn't exist for this
   artifact) — it bundles the driver's dependencies into one jar.
8. **Superset needs `SUPERSET_SECRET_KEY` set to something other than its own
   well-known placeholder**, or it refuses to start at all (not just a
   warning). `.env.example` ships a random one that's fine for local dev —
   rotate it if this ever stops being just-your-laptop.
9. **Superset's ClickHouse connection uses the `clickhousedb://` URI scheme**
   from the `clickhouse-connect` package (`docker/superset/requirements.txt`),
   not the older `clickhouse://` scheme from `clickhouse-sqlalchemy` — they're
   different packages/dialects and not interchangeable. The connection is
   pre-registered by `superset-init` via `superset set-database-uri`, so it
   shows up in Data → Databases without any manual UI setup.

## Resource requirements

Running everything together (Postgres + Airflow webserver + scheduler + Spark
master + worker + MinIO + ClickHouse + Superset-init + Superset — 9
containers) needs meaningfully more than Docker Desktop / Rancher Desktop's
defaults. We hit a hard VM crash (OOM, containers killed with exit code 137,
Docker socket disappearing) on a 4GB/2-CPU VM, *before* ClickHouse or Superset
were even added. **8GB RAM / 4 CPUs is sufficient** — measured at roughly 3GB
total across all nine containers at idle (Superset itself ~250MB, ClickHouse
~700MB, Airflow's two processes together ~1.2GB), so there's headroom left,
but if you hit OOM again after adding more, that's the budget to work with.
If you're on Rancher Desktop:
```bash
rdctl set --virtual-machine.memory-in-gb 8 --virtual-machine.number-cpus 4
```
(This restarts the Docker backend.) If you can't spare that much, run
individual `docker/<tool>/docker-compose.yml` files at a time rather than the
full stack — that's exactly what the split above is for.

## Getting started

```bash
cp .env.example .env      # edit credentials if you want non-default ones
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
This whole chain — Airflow → Spark → MinIO → Spark → ClickHouse — has been
run successfully end-to-end (not just each piece manually) as part of
building this scaffold. Re-running the DAG appends more rows to
`word_counts` each time (see the comment in `minio_to_clickhouse.py`) —
`TRUNCATE TABLE default.word_counts` first if you want a clean count.

## Extending this platform

- **New pipeline:** add a DAG under `dags/`, and if it needs Spark, a job under
  `spark_jobs/` that the DAG submits via `SparkSubmitOperator` (see the example).
  If you bump the Spark image version, re-read the version-pinning section above.
- **New raw/processed data:** use the existing `raw` / `processed` MinIO buckets
  unless there's a reason to add another one (edit `minio-init`'s `mc mb` command
  and `.env.example` if so).
- **A queryable warehouse:** `postgres` only holds Airflow's own metadata —
  don't write pipeline output there. ClickHouse (`docker/clickhouse/`) is the
  queryable table store; add new tables via a new `.sql` file under
  `docker/clickhouse/init/` (numbered after `001-create-tables.sql` — they
  run in order, but only on that volume's *first* startup, so an existing
  `clickhouse_data` volume won't pick up a new init file: apply it by hand
  with `clickhouse-client`, or drop the volume to start fresh).
- **New Spark → ClickHouse loads:** follow `minio_to_clickhouse.py`'s pattern
  (`.format("jdbc")`, driver `com.clickhouse.jdbc.ClickHouseDriver`, URL
  `jdbc:clickhouse://clickhouse:8123/<db>`) — the JDBC jar is already on both
  the Spark and Airflow images (see version-pinning point 7).
- **New BI connections in Superset:** add another `superset set-database-uri`
  call to `superset-init`'s command block in
  `docker/superset/docker-compose.yml`, following the ClickHouse one, if you
  add another queryable store later. A driver package likely needs adding to
  `docker/superset/requirements.txt` too, same as `clickhouse-connect`.
- **webapp:** see `webapp/README.md` — stack was intentionally left undecided.
  It'll most likely query ClickHouse directly for anything dashboard-shaped,
  or embed/link out to Superset for BI rather than reimplementing charting.
