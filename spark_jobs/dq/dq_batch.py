"""dq_batch: data-quality checks of the daily load, the last task of logistics_dwh_daily.

Checks what the run's window [start, end] loaded into dwh (see fact_rules.py for the rules):
    dims            whole table: id never empty, surrogate key 1:1 with the id, Unknown member present
    facts           rows with toDate(src_sys_create_date) in the window: required ids, keys exist
                    in their dims (and dim_date), Unknown-member rate, non-negative amounts,
                    plausible ranges, accepted categories, business rules
    reconciliation  every raw id of the window (MinIO) made it into its dim/fact: catches rows
                    dropped by joins or failed writes
    volume          single-day runs: the day has rows and isn't > 50% off the previous 7 days' average
                    (trips, delivery events, fuel purchases)
    consistency     single-day runs within the streaming TTL, when streaming ran that day: the batch
                    rows of the day equal the rows streaming wrote for it (dwh.<fact> vs <fact>_streaming)

Results go to dwh.dq_results. The task fails if an error-severity check fails (or a check
crashes); warnings are only recorded.

    spark-submit --py-files common.py,dq_framework.py,fact_rules.py dq/dq_batch.py --job-date 2026-10-16
"""

from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import (
    TARGET_DB,
    execute,
    get_spark,
    parse_window,
    query_row,
    read_created_between,
    read_query,
)
from dq_framework import Check, DQRunner, key_mapping
from fact_rules import DIMS, FACTS, dim_checks, fact_row_checks

VOLUME_TOLERANCE = 0.5  # a day may differ this much from the previous 7 days' average
STREAMING_LOOKBACK_DAYS = 1  # streaming rows live 72 h: only compare days that are surely still there
# facts with enough rows per day for a volume check (maintenance and incidents are a few a day
# in the history, so +-50% is noise)
VOLUME_FACTS = ["fact_trip", "fact_delivery_event", "fact_fuel_purchase"]


def reconciliation(spark, raw_table: str, raw_id: str, target: str, target_id: str,
                   start: str, end: str, max_fail_rate: float = 0.0) -> Check:
    """Raw ids recorded in the window that are missing from the target table (any version)."""

    def compute():
        ids = read_created_between(spark, raw_table, [raw_id], start, end)
        ids = ids.where(F.col(raw_id).isNotNull() & (F.trim(raw_id) != "")).distinct()
        # a target row's version is >= the raw row's sys_create_date, so it's >= start
        loaded = read_query(
            spark,
            f"SELECT DISTINCT {target_id} AS loaded_id FROM {TARGET_DB}.{target} "
            f"WHERE src_sys_create_date >= toDateTime('{start} 00:00:00', 'UTC')",
        )
        missing = ids.join(loaded, ids[raw_id] == loaded["loaded_id"], "left_anti").cache()
        n_missing, total = missing.count(), ids.count()
        sample = [r[0] for r in missing.limit(5).collect()]
        return n_missing, total, None, ("e.g. " + ", ".join(sample)) if sample else ""

    return Check(
        target, f"reconciles_with_raw_{raw_table}", "reconciliation", compute=compute,
        max_fail_rate=max_fail_rate,
        description=f"every {raw_table}.{raw_id} recorded in the window is in {target}",
    )


def volume(table: str, day: str) -> Check:
    """The day has rows, and not more than VOLUME_TOLERANCE away from the trailing 7-day average."""
    t = f"{TARGET_DB}.{table} FINAL"
    sql = f"""
        WITH (SELECT count() FROM {t} WHERE toDate(src_sys_create_date) = toDate('{day}')) AS today,
             (SELECT count() / 7 FROM {t}
               WHERE toDate(src_sys_create_date) BETWEEN toDate('{day}') - 7 AND toDate('{day}') - 1) AS avg7
        SELECT today = 0 OR (avg7 > 0 AND abs(today - avg7) / avg7 > {VOLUME_TOLERANCE}), 1, today,
               concat('previous 7 days average: ', toString(round(avg7)))
    """
    return Check(
        table, "daily_volume", "volume", sql, severity="warn",
        description=f"the day has rows, within {VOLUME_TOLERANCE:.0%} of the previous 7 days' average",
    )


def consistency_checks(table: str, day: str, skip_reason: str) -> list:
    """Batch rows of `day` vs the rows streaming wrote for it: same ids, same values."""
    spec = FACTS[table]
    id_col, streaming = spec["id"], f"{table}_streaming"
    same = " AND ".join(f"ifNull(toString(s.{c}), '') = ifNull(toString(b.{c}), '')" for c in spec["compare"])
    in_day = f"toDate(src_sys_create_date) = toDate('{day}')"
    matches = f"""
        SELECT countIf(b.{id_col} = '' OR NOT ({same})), count(), countIf(b.{id_col} = ''),
               concat('missing in batch: ', toString(countIf(b.{id_col} = '')),
                      ', values differ: ', toString(countIf(b.{id_col} != '' AND NOT ({same}))))
        FROM (SELECT * FROM {TARGET_DB}.{streaming} FINAL WHERE {in_day}) AS s
        LEFT JOIN (SELECT * FROM {TARGET_DB}.{table} FINAL WHERE {in_day}) AS b ON s.{id_col} = b.{id_col}
        SETTINGS join_use_nulls = 0
    """
    present = f"""
        SELECT countIf({id_col} NOT IN (SELECT {id_col} FROM {TARGET_DB}.{streaming})), count()
        FROM {TARGET_DB}.{table} FINAL WHERE {in_day}
    """
    return [
        Check(table, "matches_streaming", "consistency", matches, severity="warn", max_fail_rate=0.01,
              skip_reason=skip_reason,
              description=f"every {streaming} row of the day is in {table} with the same "
                          f"{', '.join(spec['compare'])}"),
        Check(table, "present_in_streaming", "consistency", present, severity="warn", max_fail_rate=0.01,
              skip_reason=skip_reason,
              description=f"every {table} row of the day was also streamed to {streaming}"),
    ]


def table_exists(spark, table: str) -> bool:
    return query_row(spark, f"EXISTS TABLE {TARGET_DB}.{table}")[0] == "1"


def streamed_rows(spark, fact: str, day: str) -> int:
    sql = (f"SELECT count() FROM {TARGET_DB}.{fact}_streaming "
           f"WHERE toDate(src_sys_create_date) = toDate('{day}')")
    return int(query_row(spark, sql)[0])


def build_checks(spark, start: str, end: str) -> list:
    window = f"toDate(src_sys_create_date) BETWEEN toDate('{start}') AND toDate('{end}')"
    single_day = start == end
    today = datetime.now(timezone.utc).date()
    checks = []

    for dim, (key, natural_id, raw_table) in DIMS.items():
        checks += dim_checks(TARGET_DB, dim)
        checks.append(reconciliation(spark, raw_table, natural_id, dim, natural_id, start, end))

    for fact, spec in FACTS.items():
        checks += fact_row_checks(TARGET_DB, fact, fact, window, trip_table="fact_trip")
        if spec["key"]:
            checks.append(key_mapping(fact, TARGET_DB, spec["key"], spec["id"]))
        if spec["raw"]:
            # fact_trip: a trip whose load lands on the next day (across midnight) is dropped by
            # the per-day inner join; tolerated up to 0.1% until the job handles it
            tolerance = 0.001 if fact == "fact_trip" else 0.0
            checks.append(reconciliation(spark, spec["raw"], spec["id"], fact, spec["id"], start, end, tolerance))
        if not spec["streamed"]:
            continue
        if single_day and fact in VOLUME_FACTS:
            checks.append(volume(fact, start))
        if not single_day:
            skip = "backfill window: consistency is checked on single-day runs"
        elif date.fromisoformat(start) < today - timedelta(days=STREAMING_LOOKBACK_DAYS):
            skip = f"{start} is older than the streaming tables keep (72 h TTL)"
        elif not table_exists(spark, f"{fact}_streaming"):
            skip = f"{fact}_streaming doesn't exist (streaming never ran)"
        elif streamed_rows(spark, fact, start) == 0:
            # nothing to compare (streaming was off); a dead stream is caught by logistics_streaming_dq
            skip = f"{fact}_streaming has no rows for {start}"
        else:
            skip = ""
        checks += consistency_checks(fact, start, skip)
    return checks


def main():
    start, end = parse_window()
    spark = get_spark(f"dq_batch_{start}_{end}")
    runner = DQRunner(
        query_row=lambda sql: query_row(spark, sql),
        execute=lambda sql: execute(spark, sql),
        db=TARGET_DB,
        layer="batch",
        window_start=f"{start} 00:00:00",
        window_end=f"{end} 23:59:59",
    )
    results = runner.run(build_checks(spark, start, end))
    spark.stop()
    DQRunner.raise_on_errors(results)


if __name__ == "__main__":
    main()
