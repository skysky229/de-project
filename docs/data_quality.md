# Data quality

Data-quality (DQ) checks run in three places, and all of them record their results in one
ClickHouse table, `dwh.dq_results`.

```
streamgen ─▶ Kafka ─▶ spark-streaming ──┬─▶ MinIO raw/logistics/ + dwh.<fact>_streaming   (valid messages)
                                        └─▶ dwh.dq_dead_letter                            (rejected messages)  ①

Airflow logistics_dwh_daily:  dims ─▶ facts ─▶ dq.dq_batch   (Spark, checks the day just loaded)          ②
Airflow logistics_streaming_dq (every 15 min): dq_streaming  (SQL only, checks the speed layer)          ③
                                             └──────────────┴─▶ dwh.dq_results ─▶ dashboards / alerts
```

| | What | Code | When |
|---|---|---|---|
| ① | Dead-letter queue: messages the streaming app can't use | `spark_jobs/streaming/dq/dead_letter.py`, `stream_common.validation_error()` | every micro-batch (30 s) |
| ② | Batch checks of the window the daily run loaded | `spark_jobs/dq/dq_batch.py` (task `dq.dq_batch`) | after the facts, every daily run and backfill |
| ③ | Streaming checks: freshness, latency, dead-letter rate, row rules | `spark_jobs/dq/dq_streaming.py` (DAG `logistics_streaming_dq`) | every 15 min |

The rules themselves (ranges, accepted values, business rules, foreign keys) live in one place,
`spark_jobs/dq/fact_rules.py`, and ② and ③ apply the same rules to the batch and the streaming
tables. `spark_jobs/dq/dq_framework.py` runs the checks and writes the results.

## Severity and status

| Severity | Meaning | On failure |
|---|---|---|
| `error` | The pipeline produced something wrong or unusable: missing rows or ids, broken keys, negative amounts, a total the ETL computes that doesn't add up, streaming stopped | the Airflow task fails |
| `warn` | The source looks suspicious: implausible values, unknown categories, empty foreign ids, source totals that don't add up | recorded only |

A check's SQL returns `failed, total`; it **fails** when `failed / total > max_fail_rate`
(0 unless the check says otherwise). Status is `pass`, `fail`, `skipped` (not applicable, e.g.
consistency on a backfill) or `error` (the check itself crashed; this also fails the task).

## The checks

### Batch (`dq.dq_batch`, rows with `toDate(src_sys_create_date)` in the run's window)

| Type | Tables | Rule | Severity |
|---|---|---|---|
| `not_empty` | all dims and event facts | business id is never NULL / `''` | error |
| `key_mapping` | all dims and event facts | surrogate key is 1:1 with the id (no xxhash64 collision) | error |
| `rule` unknown_member_present | dims | the Unknown member (`key = -1`) exists | error |
| `relationship` | facts | every `*_key` exists in its dim, every date key in `dim_date` | error (warn for `scheduled_date_key`, which can be tomorrow) |
| `unknown_rate` | facts | at most 5% of rows point at the Unknown member | warn |
| `relationship` trip_key | delivery events, fuel, incidents | the trip exists in `fact_trip` (≤ 1% may not) | warn |
| `non_negative` | facts | amounts, miles, hours, gallons ≥ 0 | error |
| `range` | facts | plausible values (e.g. 1–5000 miles, 1–10 $/gal, mpg 3–12) | warn |
| `accepted_values` | facts | categories are known (`load_type`, `event_type`, `incident_type`, ...) | warn |
| `rule` (ETL-computed) | trip, delivery, incident, driver monthly | `total_revenue = revenue + fuel_surcharge + accessorial_charges`, counts match flags, on-time rate in [0, 1] | error |
| `rule` (source) | trip, fuel, maintenance | mpg plausible, actual vs planned miles, `total_cost = gallons × price`, `total_cost = labor + parts` | warn |
| `reconciliation` | dims and event facts | every raw id recorded in the window (MinIO) is in the table: catches rows lost by joins or failed writes | error |
| `volume` | trip, delivery, fuel (single-day runs) | the day has rows, within ±50% of the previous 7 days' average | warn |
| `consistency` | the 5 streamed facts (single-day runs for yesterday or today, if streaming ran that day) | batch and streaming agree on the day: same ids, same keys and measures (≤ 1% may differ) | warn |

`fact_trip` reconciliation tolerates 0.1%: a trip whose load is recorded on the next day (across
midnight) is dropped by the per-day `trips ⋈ loads` join. This check is how you'll see it happen.

### Streaming (`logistics_streaming_dq`, rows of `[now − 65 min, now − 5 min]`)

| Type | Rule | Severity |
|---|---|---|
| `freshness` | newest row of each `_streaming` table is recent: trip/delivery/fuel 5 min, maintenance 15 min, incidents 60 min | error for `fact_trip`, warn for the rest |
| `latency` | 95% of rows reach `dwh` within 180 s of arriving in Kafka | warn |
| `dead_letter` | rejected messages / all messages: any → warn, > 1% → error | warn / error |
| row rules | the batch rules above on `dwh.<fact>_streaming` (except `dim_date`, which only gets today's date from the next daily run) | as above |

### Dead-letter queue

`stream_common.read_raw_topic()` drops a message when it is not JSON, a required column
(`schemas.REQUIRED`: the business key and the fact's date) is missing or empty, or that date
doesn't parse. Such a message would otherwise land in the lake with a NULL key or break the
fact insert. The `dq_dead_letter` query reads the same topics, applies the same validation
and stores exactly those messages in `dwh.dq_dead_letter`: raw value, Kafka topic / partition /
offset / timestamp, and the reason. Rows are kept 90 days.

```sql
SELECT topic, error, count() FROM dwh.dq_dead_letter FINAL
WHERE kafka_timestamp > now() - INTERVAL 1 DAY GROUP BY topic, error ORDER BY count() DESC;
```

To test it, set `STREAMGEN_BAD_MESSAGE_RATE=0.01` in `.env` and restart `streamgen`: about 1% of
messages are then truncated JSON, lack their key, or have `not-a-date` as their date.
`scripts/streaming_check.sh` subtracts dead letters from the Kafka counts, so the layers still match.

## Reading the results

```sql
-- latest result of every check
SELECT table_name, check_name, layer, status, severity, failed_rows, total_rows,
       round(fail_rate * 100, 2) AS fail_pct, observed_value, details
FROM dwh.dq_results
ORDER BY checked_at DESC
LIMIT 1 BY layer, table_name, check_name;

-- everything that failed in the last 7 days
SELECT checked_at, layer, table_name, check_name, severity, failed_rows, total_rows, details
FROM dwh.dq_results
WHERE status IN ('fail', 'error') AND checked_at > now() - INTERVAL 7 DAY
ORDER BY checked_at DESC;
```

In Superset, add `dwh.dq_results` as a dataset: a pass-rate-over-time chart per `layer` and a
table of the latest failures make a basic DQ dashboard. Results are kept 180 days.

The Airflow task log also prints every check (`[dq] PASS/FAIL/SKIPPED/ERROR ...`) and a summary.

## Running the checks by hand

```bash
# batch, for one day (needs the Spark cluster)
docker compose exec airflow-scheduler bash -c "spark-submit --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.s3a.endpoint=\$AWS_ENDPOINT_URL --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.access.key=\$AWS_ACCESS_KEY_ID --conf spark.hadoop.fs.s3a.secret.key=\$AWS_SECRET_ACCESS_KEY \
  --py-files /opt/airflow/spark_jobs/batch/common.py,/opt/airflow/spark_jobs/dq/dq_framework.py,/opt/airflow/spark_jobs/dq/fact_rules.py \
  /opt/airflow/spark_jobs/dq/dq_batch.py --job-date 2026-10-16"

# streaming (plain Python)
docker compose exec airflow-scheduler python /opt/airflow/spark_jobs/dq/dq_streaming.py
```

`logistics_streaming_dq` is created **paused**: unpause it while the streaming stack runs,
otherwise every run fails on freshness.

## Adding or changing a rule

- A fact's rules: edit its entry in `FACTS` in `spark_jobs/dq/fact_rules.py` (`non_negative`,
  `ranges`, `accepted`, `rules`, `fks`, `compare`). Both runners pick it up.
- A new fact: add an entry to `FACTS`; a new dim: add it to `DIMS`.
- A new kind of check: write a builder in `dq_framework.py` that returns a `Check` whose SQL
  yields `failed, total[, observed, details]`, or a `compute` callable returning the same tuple.
