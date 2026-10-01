# Streaming design (Generator → Kafka → Spark Structured Streaming → MinIO + ClickHouse)

```
 event generator ──▶ Kafka topics ──▶ Spark Streaming ──┬──▶ MinIO s3a://raw/logistics/   (raw, parquet)
 (random live data)  (1 per raw table)  (micro-batches)  └──▶ ClickHouse dwh.<fact>_streaming
                                                               ▲
                        daily batch DAG ──▶ dwh.<fact> ────────┴── view dwh.v_<fact> combines both
```
Implemented and tested. How to run and check it: [streaming_test_runbook.md](streaming_test_runbook.md).

## 1. Event generator
- A small Python script (`streamgen`) publishes new random events to Kafka at a configurable rate (default: 1 trip per second).
- **Random, never identical to existing rows:**
  - New ids with an `S` marker + the generator's start time (e.g. `TRIPS1790504323-000005`).
  - Random values in realistic ranges (weight, miles, revenue, gallons, ...).
  - Timestamps = now.
- Foreign keys point at **existing** drivers, trucks, trailers, customers, routes and facilities (picked at random from the bootstrap CSVs in MinIO `raw/csv/`), so the facts join to the dimensions.
- Per trip it publishes: 1 load + 1 trip, then a pickup and a delivery event and 1–2 fuel purchases a few seconds later, and occasionally an incident. Maintenance records are published independently (a few per minute).

## 2. Kafka
- Single-node Kafka (KRaft mode, no ZooKeeper).
- **One topic per streamed raw table (6):** `raw.trips`, `raw.loads`, `raw.delivery_events`, `raw.fuel_purchases`, `raw.maintenance_records`, `raw.safety_incidents`.
- JSON messages, keyed by the business key (`trip_id`, ...).
- Dimensions are **not** streamed; the daily batch DAG keeps loading them.

## 3. Spark Structured Streaming
- `readStream.format("kafka")` per topic, parse JSON with a fixed schema, micro-batch every 30 s.
- Checkpoints in MinIO (`s3a://processed/checkpoints/streaming/<query>/`), so a restart continues where it stopped.
- **Stateless:** parse → dedup inside the micro-batch → write. No watermark, except in `fact_trip` (see below).

## 4. Write (one table per file, one query per file)
| Folder | Jobs | Reads | Writes |
|---|---|---|---|
| `streaming/raw/` | `raw_trips.py`, `raw_loads.py`, … (6) | its topic | MinIO `s3a://raw/logistics/<table>/partition_date=YYYY-MM-DD/` (parquet, Spark file sink, exactly-once), `sys_create_date` = Kafka record time |
| `streaming/fact/` | `fact_trip.py`, `fact_delivery_event.py`, `fact_fuel_purchase.py`, `fact_maintenance.py`, `fact_safety_incident.py` | its topic(s) | `dwh.fact_*_streaming` (72 h TTL) |

- Raw jobs: Spark's parquet file sink, 1 file per micro-batch and day, committed files listed in `_spark_metadata/`.
- Fact jobs: `foreachBatch` → `latest_per_key()` → `write_append()` into `dwh.<fact>_streaming`.
- Streaming is **self-contained**: helpers (`streaming/helpers.py`), each fact's DDL and `transform()` are copied from the batch jobs in `spark_jobs/batch/facts/`, not imported. Only the table name differs (`<fact>_streaming`, 72 h TTL). The view `dwh.v_<fact>` unions both tables, so their columns (and key/money logic) must stay identical: keep the copies in sync by hand, then run `scripts/generate_fact_views.py`.
- Each fact stores only the keys its own raw row has; delivery events, fuel purchases and incidents carry `trip_key` (= `fact_trip.trip_key`, a hash of `trip_id`) to reach the trip's customer/route.
- `fact_trip` looks up `planned_miles` in routes, read from MinIO `raw/history/routes` (routes aren't streamed).
- **`fact_trip`** needs `trips` ⋈ `loads`: a stream-stream join on `load_id` with a 10 min watermark. It's the only stateful query.
- Replays and duplicates are harmless: `dwh` tables merge on the business key (ReplacingMergeTree).

## 4b. Serving: batch + streaming views (Lambda)
- Raw data: streaming lands it in MinIO (`s3a://raw/logistics/<table>/partition_date=.../`). The daily batch DAG reads those folders together with the bootstrap history (`raw/history/`), so streamed events reach the batch tables on the next daily run. ClickHouse `default.*` is retired.
- Transformed data: batch writes `dwh.<fact>`, streaming writes `dwh.<fact>_streaming`.
- `dwh.v_<fact>` = batch rows with `toDate(src_sys_create_date) <= C` + streaming rows with `> C`, where
  **C = the last day the batch table has loaded** (`max(toDate(src_sys_create_date))`). Not `today() - 1`,
  so there's no gap between midnight and the moment the nightly batch run finishes.
- No overlap: the batch run for day D reads exactly the raw partitions `partition_date = D`.
- `_streaming` rows expire after **72 h** (ClickHouse TTL), long enough to cover a late batch run.
  (Better long-term: the batch job deletes the streaming rows it has taken over.)
- Known, accepted gap: if streaming catches up *after* the batch run for that day, those late rows are in neither layer.

## 5. Run
- One long-lived Spark application (`streaming/run_all.py`) starts all 11 queries (6 raw + 5 fact). Compose service `spark-streaming` runs it, not Airflow; it uses 2 of the worker's 4 cores.
- Check it in: the Spark UI (Structured Streaming tab), the MinIO console (bucket `raw`), and row counts in ClickHouse (`_streaming` tables and `v_*` views). Commands: [streaming_test_runbook.md](streaming_test_runbook.md).

## 6. Components
| Change | Why |
|---|---|
| `docker/kafka/` | Kafka broker |
| `docker/streamgen/` | Event generator |
| Spark image + Kafka connector jars (`spark-sql-kafka-0-10_2.12:3.5.1`) | Spark reads Kafka |
| `spark-streaming` service (in `docker/spark/`) | Runs the streaming application |
| `spark-worker`: 2 → 4 cores, 2 → 4 GB | Streaming + batch at the same time |
| `scripts/fact_views.sql` | The 5 `v_<fact>` views |
