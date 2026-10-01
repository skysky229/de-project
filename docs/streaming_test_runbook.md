# Streaming test runbook

Step-by-step test of the full streaming flow:

```
streamgen ──▶ Kafka (6 topics) ──▶ spark-streaming ──┬──▶ MinIO  s3a://raw/logistics/<table>/partition_date=…/
                                                     └──▶ ClickHouse dwh.<fact>_streaming ──▶ view dwh.v_<fact>
```

Run every command from the repo root. Times are UTC.
Two helper scripts do the heavy lifting:
- `scripts/streaming_check.sh [SINCE]`: compares Kafka = MinIO = `_streaming` = view, per stream.
- `scripts/streaming_reset.sh`: wipes the streaming test data for a clean start.

---

## 0. Prerequisites (once)

1. **Docker is running** (Rancher Desktop): `docker info` must answer.
2. **`.env` exists** (`cp .env.example .env`) and has the ClickHouse Cloud settings `DWH_CH_URL`, `DWH_CH_USER`, `DWH_CH_PASSWORD`.
3. **Images are built:**
   ```bash
   docker compose build spark-master streamgen airflow-webserver
   ```
4. **The bootstrap has run once** (`logistics_bootstrap` DAG, README "Getting started" step 4):
   the generator reads its driver/truck/route ids from MinIO `raw/csv/`, and streaming `fact_trip`
   reads routes from `raw/history/routes`. Check in the MinIO console that both folders exist.

## 1. Start the infrastructure

```bash
docker compose up -d minio minio-init kafka spark-master spark-worker
docker compose ps kafka minio spark-master spark-worker     # kafka + minio show "(healthy)"
```
✅ Check http://localhost:8081 (Spark master). **Workers** lists 1 ALIVE worker with **4 cores**.

## 2. (Optional) Reset to a clean state

For a test where every count starts from zero:
```bash
scripts/streaming_reset.sh          # asks for confirmation
```
This deletes the Kafka `raw.*` topics, the streamed raw parquet (`raw/logistics/`) and the checkpoints in MinIO, and the rows of the 5 `dwh.*_streaming` tables. It keeps the bootstrap data (`raw/csv/`, `raw/history/`), the batch tables and the views.

Skipping the reset is fine too. Step 6 can count only the rows created during your test.

## 3. Start the streaming application

```bash
docker compose up -d spark-streaming
docker compose logs -f spark-streaming | grep --line-buffered -E "started query|batch|Exception"
```
✅ Within ~1 min you see **11** lines `started query …`: 6 `raw_*` and 5 `fact_*_streaming`. Stop following the logs with Ctrl+C; the app keeps running.

The 5 `dwh.*_streaming` tables are created on this first start if they don't exist yet.

## 4. Create the views (first time, or after column changes)

The views need both the batch table and the streaming table to exist: the streaming ones after step 3,
the batch ones after the first daily/backfill run of `logistics_dwh_daily`.
Run every statement in `scripts/fact_views.sql` in ClickHouse, e.g. in DBeaver (URL in README).
If a fact's columns changed, regenerate the file first: `python3 scripts/generate_fact_views.py`.
`CREATE OR REPLACE` makes re-running safe.
```sql
SHOW TABLES FROM dwh LIKE 'v_%';      -- ✅ 5 views
```

## 5. Start generating data

Note the start time first (for step 6), then start the generator:
```bash
date -u +"%Y-%m-%d %H:%M:%S"            # e.g. 2026-09-30 09:00:00, keep it as SINCE
docker compose up -d streamgen
docker compose logs -f streamgen         # every 30 s: "sent so far: {...}"
```
Rates: 1 trip per second by default (plus 2 delivery events, 1–2 fuel purchases, rare incidents, ~3 maintenance records per minute). Change them with `STREAMGEN_TRIPS_PER_SECOND` / `STREAMGEN_MAINTENANCE_PER_MINUTE` in `.env`, then `docker compose up -d streamgen`.

### Watch it flow (any time while it runs)

| Where | How | ✅ What you should see |
|---|---|---|
| Streaming app log | `docker compose logs -f spark-streaming \| grep batch` | a `wrote N rows` line per query every ~30 s |
| Kafka | see "Kafka commands" below | offsets growing, JSON messages with `…S…-…` ids |
| Spark UI | http://localhost:8081 → application `logistics_streaming` → **Structured Streaming** tab | 11 active queries, input rate ~1–2 rows/s each |
| MinIO | http://localhost:9001 (`minioadmin` / `minioadmin`) → bucket `raw` → `logistics/<table>/partition_date=<today>/` | new `.parquet` files every ~30 s |
| ClickHouse | queries below | counts growing |

```sql
SELECT count() FROM dwh.fact_trip_streaming FINAL;
SELECT source, count() FROM dwh.v_fact_trip GROUP BY source;          -- source = 'streaming'
SELECT max(etl_loaded_date), now() FROM dwh.fact_trip_streaming;      -- latest write, ~30 s ago
```

## 6. Check that every layer matches

Pause the input, wait for the last micro-batches, then compare:
```bash
docker compose stop streamgen
sleep 60
scripts/streaming_check.sh "2026-09-30 09:00:00"     # SINCE from step 5; no argument after a reset
```
✅ Every line says **OK** and the script prints `All layers match.`:
```
stream                  kafka    minio minio_ids  _streaming       view  status
trips                     161      161      161         161        161  OK
loads                     161      161      161         161        161  OK
delivery_events           275      275      275         275        275  OK
...
```
- `kafka` = messages published, `minio` = raw rows landed, `minio_ids` = distinct ids (equal to `minio`, so no duplicates), `_streaming` = fact rows, `view` = rows the view serves from streaming.
- `loads` and `trips` both feed `fact_trip`, so both compare against `fact_trip_streaming`.
- **DIFF** usually means the generator was still running, or you waited too little. Wait 30 s more and re-run.

## 7. Restart test (recovery from checkpoints)

```bash
date -u +"%Y-%m-%d %H:%M:%S"                  # new SINCE
docker compose start streamgen
sleep 60
docker compose restart spark-streaming        # kill + restart the app while data keeps coming
sleep 60
docker compose stop streamgen
sleep 90
scripts/streaming_check.sh "<new SINCE>"
```
✅ All **OK**: nothing lost and nothing duplicated across the restart. In the log after the restart, micro-batch numbers continue (e.g. `batch 7`, not `batch 0`).

## 8. Manual end-to-end message (optional)

Publish one hand-written event and follow it through:
```bash
docker compose exec kafka bash -c "echo 'MAINTTEST-1|{\"maintenance_id\":\"MAINTTEST-1\",\"truck_id\":\"TRK00001\",\"maintenance_date\":\"2026-09-30\",\"maintenance_type\":\"Tire\",\"odometer_reading\":100000,\"labor_hours\":1.0,\"labor_cost\":100.0,\"parts_cost\":50.0,\"total_cost\":150.0,\"facility_location\":\"Denver\",\"downtime_hours\":2.0,\"service_description\":\"manual test\"}' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic raw.maintenance_records --property parse.key=true --property key.separator='|'"
```
After ~30 s:
```sql
SELECT * FROM dwh.fact_maintenance_streaming FINAL WHERE maintenance_id = 'MAINTTEST-1';   -- ✅ 1 row
SELECT * FROM dwh.v_fact_maintenance WHERE maintenance_id = 'MAINTTEST-1';                -- ✅ source = 'streaming'
```
and in MinIO: `raw/logistics/maintenance_records/partition_date=<today>/` has a new file.

## 9. Hand-over test: batch takes over streamed data (optional)

Shows that the daily batch reads the streamed raw partitions and the view switches those rows
from `streaming` to `batch`. Needs Airflow up and `logistics_dwh_daily` unpaused.
**Only do this with the generator paused**: loading an unfinished day into batch moves the view's
cut-off to today, so rows streamed later today would stay hidden until tomorrow's run.
```sql
SELECT source, count() FROM dwh.v_fact_trip GROUP BY source;        -- before: batch + streaming
```
```bash
docker compose exec airflow-scheduler airflow dags trigger logistics_dwh_daily \
  -c '{"start_date": "<today>", "end_date": "<today>"}'           # wait until the run succeeds
```
```sql
SELECT source, count() FROM dwh.v_fact_trip GROUP BY source;        -- after: same total, all 'batch'
SELECT count() - uniqExact(trip_id) FROM dwh.v_fact_trip;           -- ✅ 0 duplicates
```

## 10. Stop

```bash
docker compose stop streamgen spark-streaming                  # stop the flow, keep all data
docker compose stop kafka minio spark-master spark-worker      # stop the infrastructure too
```
Starting again later continues from the checkpoints, so no data is lost or re-processed.

---

## Kafka commands

```bash
# 6 raw.* topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# messages per partition (topic:partition:end offset)
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic raw.trips
# sample messages (key = business key, value = JSON row)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic raw.trips --from-beginning --max-messages 3 --property print.key=true
```
Kafka consumer-group tools don't show Spark's progress: Structured Streaming keeps its offsets in its checkpoints in MinIO.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `docker: failed to connect to the docker API` | Rancher Desktop isn't running: start it, wait ~1 min |
| Apps stuck in WAITING, 0 cores (Spark UI) | The worker dropped out (often after the Mac slept): `docker compose restart spark-worker` |
| `streaming_check.sh` shows DIFF right after stopping the generator | Last micro-batch not written yet: wait 30–60 s, re-run |
| DIFF in the `view` column only | The batch table has loaded those days, so the view serves them from batch (as designed) |
| DIFF for a window older than 72 h | Kafka messages and `_streaming` rows expire after 72 h; MinIO keeps everything |
| First ClickHouse query fails (`NoHttpResponse`, SSL errors) | ClickHouse Cloud was asleep: wait a few seconds and retry |
| `streamgen` exits at start | Kafka not healthy yet, or the bootstrap CSVs are missing in MinIO `raw/csv/` (it reads dimension ids at start): `docker compose logs streamgen` |
| Changed a raw job's path/partitioning/format | Its checkpoint no longer fits: run `scripts/streaming_reset.sh`, or delete that table's MinIO folder + `processed/checkpoints/streaming/raw_<table>/` |
