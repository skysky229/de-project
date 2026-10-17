"""dq_streaming: data-quality checks of the streaming (speed) layer, run every 15 min by the
DAG logistics_streaming_dq. Plain Python over the ClickHouse HTTP interface: every check is
a small SQL query, so no Spark application is started for it.

    freshness    the newest row of each dwh.<fact>_streaming is recent (is streaming alive?)
    latency      p95 of etl_loaded_date - src_sys_create_date (event received -> row in dwh)
    dead_letter  share of Kafka messages rejected into dwh.dq_dead_letter
    row rules    the same rules as the batch facts (fact_rules.py) on the streamed rows of the
                 last hour, so a streaming transform that drifts from batch shows up here first

Window: rows with src_sys_create_date in [now - 65 min, now - 5 min]; the 5 min margin lets
child events (delivery, fuel) and their trip land in their micro-batches first.

    python3 spark_jobs/dq/dq_streaming.py        # needs DWH_CH_URL / DWH_CH_USER / DWH_CH_PASSWORD
"""

import os
from datetime import datetime, timedelta, timezone

import requests

from dq_framework import Check, DQRunner
from fact_rules import FACTS, fact_row_checks

TARGET_DB = os.environ.get("DWH_TARGET_DB", "dwh")
WINDOW = timedelta(hours=1)
SETTLE = timedelta(minutes=5)

# fact -> max seconds since its newest streamed row, severity. Rates come from the generator:
# 1 trip/s (+ events, fuel), 3 maintenance/min, incidents on 0.5% of trips.
FRESHNESS = {
    "fact_trip": (300, "error"),
    "fact_delivery_event": (300, "warn"),
    "fact_fuel_purchase": (300, "warn"),
    "fact_maintenance": (900, "warn"),
    "fact_safety_incident": (3600, "warn"),
}
MAX_P95_LATENCY_SECONDS = 180  # trigger is 30 s; fact_trip waits for its load (watermark)
DEAD_LETTER_WARN_RATE = 0.0  # any rejected message is worth a look
DEAD_LETTER_ERROR_RATE = 0.01  # more than 1% means a producer or schema is broken


class ClickHouseHttp:
    """Minimal ClickHouse HTTP client: the same query_row/execute pair the Spark runner gets."""

    def __init__(self):
        self.url = os.environ["DWH_CH_URL"].rstrip("/")
        self.auth = (os.environ.get("DWH_CH_USER", "default"), os.environ["DWH_CH_PASSWORD"])

    def _post(self, sql: str) -> str:
        # ClickHouse Cloud may be idle and refuse the first connection while waking up
        for attempt in range(3):
            try:
                resp = requests.post(self.url, data=sql.encode(), auth=self.auth, timeout=120)
                break
            except requests.ConnectionError:
                if attempt == 2:
                    raise
        if resp.status_code != 200:
            raise RuntimeError(f"ClickHouse {resp.status_code}: {resp.text.strip()[:500]}")
        return resp.text

    def query_row(self, sql: str):
        text = self._post(sql.rstrip().rstrip(";") + "\nFORMAT TabSeparated")
        line = text.split("\n", 1)[0]
        return line.split("\t") if text else None

    def execute(self, sql: str) -> None:
        self._post(sql)


def freshness(fact: str, max_lag: int, severity: str) -> Check:
    table = f"{fact}_streaming"
    sql = f"""
        SELECT lag_s > {max_lag}, 1, lag_s, if(n = 0, 'no rows', concat('newest row: ', toString(newest)))
        FROM (SELECT count() AS n, max(src_sys_create_date) AS newest,
                     if(n = 0, 999999999, dateDiff('second', newest, now())) AS lag_s
              FROM {TARGET_DB}.{table})
    """
    return Check(table, "freshness", "freshness", sql, severity=severity,
                 description=f"newest row is at most {max_lag} s old (streaming is alive)")


def latency(fact: str, src: str) -> Check:
    table = f"{fact}_streaming"
    sql = f"""
        SELECT countIf(delay > {MAX_P95_LATENCY_SECONDS}), count(), quantile(0.95)(delay)
        FROM (SELECT dateDiff('second', src_sys_create_date, etl_loaded_date) AS delay FROM {src})
    """
    # p95 <= limit  <=>  at most 5% of rows above it
    return Check(table, "latency_p95", "latency", sql, severity="warn", max_fail_rate=0.05,
                 description=f"95% of rows reach dwh within {MAX_P95_LATENCY_SECONDS} s of arriving in Kafka")


def dead_letter_checks(start: str, end: str) -> list:
    """Rejected messages vs all messages of the window (valid ones = rows in the raw lake =
    rows of the _streaming facts; loads are counted through fact_trip, one load per trip)."""
    in_window = f"BETWEEN toDateTime('{start}', 'UTC') AND toDateTime('{end}', 'UTC')"
    valid = " + ".join(
        f"(SELECT count() FROM {TARGET_DB}.{f}_streaming WHERE src_sys_create_date {in_window})"
        + (" * 2" if f == "fact_trip" else "")
        for f in FACTS if FACTS[f]["streamed"]
    )
    sql = f"""
        WITH (SELECT count() FROM {TARGET_DB}.dq_dead_letter FINAL WHERE kafka_timestamp {in_window}) AS bad
        SELECT bad, bad + {valid}, bad,
               (SELECT arrayStringConcat(groupArray(concat(topic, ': ', error)), ' | ')
                FROM (SELECT topic, error FROM {TARGET_DB}.dq_dead_letter FINAL
                      WHERE kafka_timestamp {in_window} LIMIT 3))
    """
    return [
        Check("dq_dead_letter", "dead_letter_any", "dead_letter", sql, severity="warn",
              max_fail_rate=DEAD_LETTER_WARN_RATE, description="no Kafka message was rejected"),
        Check("dq_dead_letter", "dead_letter_rate", "dead_letter", sql, severity="error",
              max_fail_rate=DEAD_LETTER_ERROR_RATE,
              description=f"at most {DEAD_LETTER_ERROR_RATE:.0%} of Kafka messages are rejected"),
    ]


def build_checks(start: str, end: str) -> list:
    where = (f"src_sys_create_date BETWEEN toDateTime('{start}', 'UTC') "
             f"AND toDateTime('{end}', 'UTC')")
    checks = []
    for fact, (max_lag, severity) in FRESHNESS.items():
        table = f"{fact}_streaming"
        checks.append(freshness(fact, max_lag, severity))
        checks.append(latency(fact, f"{TARGET_DB}.{table} WHERE {where}"))
        checks += fact_row_checks(TARGET_DB, fact, table, where, trip_table="fact_trip_streaming",
                                  check_dates=False)
    if ClickHouseHttp().query_row(f"EXISTS TABLE {TARGET_DB}.dq_dead_letter")[0] == "1":
        checks += dead_letter_checks(start, end)
    else:  # created by the streaming app's dq_dead_letter query on its first start
        checks += [Check("dq_dead_letter", c.name, c.check_type, severity=c.severity,
                         skip_reason="dwh.dq_dead_letter doesn't exist yet (streaming app not started)")
                   for c in dead_letter_checks(start, end)]
    return checks


def run() -> None:
    """Entry point (Airflow task / CLI): run every check, store results, fail on blocking ones."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    end = now - SETTLE
    start = end - WINDOW
    fmt = "%Y-%m-%d %H:%M:%S"
    ch = ClickHouseHttp()
    runner = DQRunner(ch.query_row, ch.execute, TARGET_DB, "streaming",
                      start.strftime(fmt), end.strftime(fmt))
    results = runner.run(build_checks(start.strftime(fmt), end.strftime(fmt)))
    DQRunner.raise_on_errors(results)


if __name__ == "__main__":
    run()
