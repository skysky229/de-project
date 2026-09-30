# How to add a DWH table (dim / fact / mart)

Every table in `dwh` is built by **one Spark job file**, run by **one Airflow task**.
To add a table you write one file, register it in the DAG, and test it.

Working reference: `spark_jobs/dwh/dims/dim_driver.py`. Copy it.

---

## 1. The rules every job follows

| Rule | What it means in code |
|---|---|
| One table, one file | `spark_jobs/dwh/<layer>/<table>.py`, file name = table name (e.g. `dims/dim_truck.py`) |
| Spark transforms, ClickHouse stores | SQL sent to ClickHouse is only `SELECT <columns> FROM ... WHERE ...` plus DDL. Joins, dedup, derived columns and aggregation happen in Spark. |
| Read only the run's window | Raw tables are append-only; read them with `read_created_between()` (`WHERE toDate(sys_create_date) BETWEEN start AND end`). |
| Append only, merge on business key | Target is a `ReplacingMergeTree(src_sys_create_date)` with `ORDER BY <business key>`. The job only appends; ClickHouse keeps the newest source version per key. Re-runs are safe. |
| No `PARTITION BY` | ReplacingMergeTree only merges within a partition. |
| Two timestamps on every row | `src_sys_create_date` = raw row's `sys_create_date` (the merge version). `etl_loaded_date` = when the job wrote the row (`with_audit()`), audit only. |
| Stable surrogate keys | `surrogate_key(col)` = `xxhash64(id)`, same on every run; empty/NULL id → `-1`. Always generate keys **in Spark**: ClickHouse's `xxHash64` gives different numbers. |
| Readers use `FINAL` | Until background merges run, a key can have several physical rows. `SELECT ... FROM dwh.x FINAL` returns exactly one. |

## 2. Helpers in `spark_jobs/dwh/common.py`

| Helper | Use it to |
|---|---|
| `parse_window()` | Parse `--job-date` or `--start-date/--end-date` → `(start, end)` |
| `get_spark(app_name)` | Create the SparkSession (UTC session time zone) |
| `ensure_table(spark, DDL)` | `CREATE DATABASE/TABLE IF NOT EXISTS`; write `{db}` in the DDL for the target database |
| `seed_unknown_member(spark, table, key_col, row_df)` | Insert the `-1` Unknown row once (dims only) |
| `read_created_between(spark, raw_table, columns, start, end)` | Read the raw delta for the window |
| `read_query(spark, sql)` | Read anything else with a plain SELECT, e.g. a `dwh` table with `FINAL` (marts) |
| `latest_per_key(df, keys)` | Keep the newest version per business key within the batch (by `sys_create_date`) |
| `surrogate_key(col)` | Stable Int64 key, `-1` for empty ids |
| `date_key(col)` | yyyyMMdd Int key from a date/timestamp column |
| `money(col_name, scale=2)` | Float64 amount → Decimal, rounded first (0.29 stays 0.29) |
| `empty_to_null(col)` | Turn `''` into NULL |
| `with_audit(df)` | Add `etl_loaded_date` |
| `write_append(df, table)` | Batched JDBC append into `dwh.<table>` |
| `execute(spark, sql)` / `query_scalar(spark, sql)` | Run a DDL statement / read one value on the driver |

## 3. Step by step

### Step 1: Create the file from the template

```python
"""<table>: default.<raw_table> -> dwh.<table>  (merge on <business key>)"""
from pyspark.sql import functions as F

from common import (
    ensure_table, get_spark, latest_per_key, parse_window, read_created_between,
    surrogate_key, with_audit, write_append,
)

TABLE = "<table>"

DDL = """
CREATE TABLE IF NOT EXISTS {db}.<table>
(
    <x>_key             Int64,
    <x>_id              String,
    -- ... attributes / measures ...
    src_sys_create_date DateTime('UTC') COMMENT 'version: sys_create_date of the source row',
    etl_loaded_date     DateTime('UTC') COMMENT 'when the ETL job wrote this row'
)
ENGINE = ReplacingMergeTree(src_sys_create_date)
ORDER BY <x>_id
"""

SOURCE_COLUMNS = ["<x>_id", "...", "sys_create_date"]      # always include sys_create_date


def transform(raw):
    rows = latest_per_key(raw, ["<x>_id"])
    return rows.select(
        surrogate_key(F.col("<x>_id")).alias("<x>_key"),
        "<x>_id",
        # ... renames / derived columns ...
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)

    raw = read_created_between(spark, "<raw_table>", SOURCE_COLUMNS, start, end).cache()
    raw_count = raw.count()
    if raw_count == 0:
        print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")
    else:
        out = with_audit(transform(raw)).cache()
        write_append(out, TABLE)
        print(f"[{TABLE}] window {start}..{end}: read {raw_count} raw rows, wrote {out.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
```

Then add what your layer needs (sections 4–6).

### Step 2: Register it in the DAG

In `dags/logistics_dwh_daily.py`, add the table name to the list for its layer:

```python
DIM_TABLES = ["dim_driver", "dim_truck"]     # file: spark_jobs/dwh/dims/dim_truck.py
FACT_TABLES = ["fact_trip"]
MART_TABLES = []
```

The DAG creates one task per name inside the layer's TaskGroup (`dim.dim_truck`).
Groups run in order `dim → fact → mart`. No other DAG change is needed.

### Step 3: Test (see section 7), then check the DAG parses

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow tasks list logistics_dwh_daily --tree
```

---

## 4. Dimension specifics (`dim/`)

- **Business key:** the entity id (`truck_id`, `customer_id`, ...). `ORDER BY` it.
- **SCD1:** the newest source version wins; no history is kept.
- **Unknown member:** add an `unknown_member(spark)` function returning one row with key `-1`,
  id `'UNKNOWN'`, `'Unknown'` text attributes and `src_sys_create_date = 1970-01-01`, and call
  `seed_unknown_member(spark, TABLE, "<x>_key", unknown_member(spark))` right after `ensure_table`.
  It is inserted once, never per run.
- Tables with no business date (`facilities`, `routes`) work the same way. Their raw
  `sys_create_date` is `2021-12-31`, so a backfill window must include that day.

## 5. Fact specifics (`fact/`)

- **Business key:** the event/transaction id (`trip_id`, `event_id`, `fuel_purchase_id`, ...).
- **Dimension keys** come from `surrogate_key(<fk id>)` on the fact's own foreign-key column.
  No join with the dim table is needed, because keys are deterministic hashes, and a fact can load
  before its dimension row exists. An empty FK becomes `-1` (Unknown).
- **Date keys:** `date_key = yyyyMMdd` as Int, e.g. `F.date_format(col, "yyyyMMdd").cast("int")`.
- **Two raw sources, one target** (e.g. `fact_trip` = `trips` ⋈ `loads`): read each with
  `read_created_between()`, dedup each with `latest_per_key()`, join in Spark, and set
  `src_sys_create_date = greatest(<both sys_create_date>)`.
- **Money:** `F.round(col, 2).cast("decimal(18,2)")`. Don't store Float64 money.
- **Ratios:** store numerator and denominator, not the ratio (e.g. miles and gallons, not mpg).
  Add the ratio as a ClickHouse `ALIAS` column in the DDL if you want it pre-defined.

## 6. Mart specifics (`mart/`)

Marts are aggregates rebuilt from the `dwh` facts, not read from raw.

- **Read** the facts for the affected period with `read_query()` and **`FINAL`**:
  ```python
  read_query(spark, f"SELECT ... FROM {TARGET_DB}.fact_trip FINAL "
                    f"WHERE date_key BETWEEN {month_start} AND {month_end}")
  ```
  Re-aggregate whole periods (e.g. the whole month containing the window), never just the window's rows.
- **Business key:** the grain, e.g. `ORDER BY (driver_key, month_date_key)`.
- **Version:** `src_sys_create_date = max(src_sys_create_date)` of the input rows. Raw data is
  append-only, so a recomputed period can only get the same or a newer version, and the newest
  aggregate wins.
- **Store additive columns only** (counts, sums). Put ratios in `ALIAS` columns so they roll up correctly.

## 7. Testing a new job

**Run it directly** (fast, no Airflow):
```bash
docker compose exec airflow-scheduler bash -c "spark-submit --master spark://spark-master:7077 \
  --py-files /opt/airflow/spark_jobs/dwh/common.py \
  /opt/airflow/spark_jobs/dwh/<layer>/<table>.py --start-date 2000-01-01 --end-date 2024-12-31"
```

**Use throwaway databases** for anything that writes test data. Never append test rows to `default`.
The job reads its databases from env vars, so override them:
```bash
docker compose exec -e DWH_SOURCE_DB=etl_test_src -e DWH_TARGET_DB=etl_test_dwh airflow-scheduler bash -c "spark-submit ..."
```
Create `etl_test_src.<raw_table>` with `CREATE TABLE ... AS default.<raw_table>` + `INSERT ... SELECT`,
append new versions there, and drop both databases afterwards.

**Checklist**
- [ ] Backfill: `SELECT count() FROM dwh.<table> FINAL` = distinct business keys in the source (+1 for a dim's Unknown row).
- [ ] Every column matches the source (write a `LEFT JOIN ... WHERE a != b` count; expect 0).
- [ ] Empty day: logs `nothing to write`.
- [ ] Re-run the same window: `FINAL` count unchanged.
- [ ] New source version (test DB): `FINAL` shows the new values; re-running an older window doesn't bring the old ones back.
- [ ] Through Airflow: `airflow dags trigger logistics_dwh_daily -e <date>` (the DAG must be unpaused to run), then open the task log in the UI.

## 8. Pitfalls we've hit

| Pitfall | Fix |
|---|---|
| ClickHouse `Date` starts at 1970; older dates are silently clamped to 1970-01-01 | Use `Date32` for dates that can be before 1970 (birth dates) |
| `CREATE TABLE IF NOT EXISTS` doesn't apply DDL changes | Drop the table (or `ALTER`) and re-run a backfill |
| `SELECT *` / `m.*` skips `ALIAS` columns | Name them explicitly |
| `count()` via JDBC is a Java `BigInteger` | `query_scalar()` returns a string; wrap it in `int()` |
| Querying without `FINAL` shows duplicates right after a load | Always use `FINAL` (or a view with it) for exact results |
| Airflow skips runs dated before the DAG's `start_date` (2022-01-01) | Use the backfill params (`start_date` / `end_date` in the run conf) |
| `airflow dags test` doesn't write task logs to disk | Use `airflow dags trigger` or the UI when you need the log |
