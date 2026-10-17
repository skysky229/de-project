"""Data-quality framework, shared by the two DQ runners:
    dq_batch.py      Spark job, last task group of the daily DAG (checks the window just loaded)
    dq_streaming.py  plain Python, DAG logistics_streaming_dq every 15 min (checks dwh.*_streaming)

Pure Python, no pyspark import: each runner passes its own ClickHouse access
(JDBC through Spark, or HTTP) as two callables:
    query_row(sql) -> list of str (the first row) or None
    execute(sql)   -> None

A check is one SQL query that returns one row: `failed, total[, observed]`
    failed    rows (or 0/1 for a freshness-style check) that break the rule
    total     rows checked
    observed  optional metric shown on the dashboard (e.g. lag in seconds)
or a `compute` callable returning the same tuple (e.g. a Spark reconciliation).

Status of a check:
    pass     failed / total <= max_fail_rate
    fail     above it; with severity "error" the task fails, "warn" only records it
    skipped  not applicable to this run (skip_reason says why)
    error    the check itself crashed (bad SQL, missing table): fails the task

Every result is appended to dwh.dq_results (one row per check per run), the table
dashboards and alerts read.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

RESULTS_TABLE = "dq_results"

RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS {db}.dq_results
(
    run_id        String,
    checked_at    DateTime('UTC'),
    layer         LowCardinality(String) COMMENT 'batch | streaming',
    window_start  DateTime('UTC') COMMENT 'rows checked: src_sys_create_date in [window_start, window_end]',
    window_end    DateTime('UTC'),
    table_name    LowCardinality(String),
    check_name    String,
    check_type    LowCardinality(String),
    severity      LowCardinality(String) COMMENT 'error | warn',
    status        LowCardinality(String) COMMENT 'pass | fail | skipped | error',
    failed_rows   UInt64,
    total_rows    UInt64,
    fail_rate     Float64,
    max_fail_rate Float64,
    observed_value Nullable(Float64),
    details       String,
    description   String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(checked_at)
ORDER BY (table_name, check_name, checked_at)
TTL checked_at + INTERVAL 180 DAY
"""


class DataQualityError(Exception):
    """Raised at the end of a run when an error-severity check failed or a check crashed."""


@dataclass
class Check:
    table: str
    name: str
    check_type: str  # not_empty, key_mapping, relationship, unknown_rate, non_negative, range,
    #                  accepted_values, rule, reconciliation, volume, freshness, latency,
    #                  dead_letter, consistency
    sql: Optional[str] = None
    compute: Optional[Callable[[], tuple]] = None
    severity: str = "error"
    max_fail_rate: float = 0.0
    description: str = ""
    skip_reason: str = ""


@dataclass
class Result:
    check: Check
    status: str
    failed: int = 0
    total: int = 0
    observed: Optional[float] = None
    details: str = ""

    @property
    def fail_rate(self) -> float:
        if self.total:
            return self.failed / self.total
        return 1.0 if self.failed else 0.0

    @property
    def blocking(self) -> bool:
        return self.status == "error" or (self.status == "fail" and self.check.severity == "error")


# ------------------------------------------------------------------ check builders
# `src` is the FROM/WHERE part, e.g. "dwh.fact_trip FINAL WHERE toDate(src_sys_create_date) BETWEEN ...".
# Each builder returns a Check whose SQL yields `failed, total`.


def not_empty(table: str, src: str, column: str, severity: str = "error") -> Check:
    """Business id / required column is neither NULL nor ''."""
    return Check(
        table, f"{column}_not_empty", "not_empty",
        f"SELECT countIf({column} IS NULL OR toString({column}) = ''), count() FROM {src}",
        severity=severity, description=f"{column} is never NULL or empty",
    )


def key_mapping(table: str, db: str, key: str, natural_id: str) -> Check:
    """The surrogate key is 1:1 with the natural id: no two ids hash to the same xxhash64 key."""
    return Check(
        table, f"{key}_unique_per_{natural_id}", "key_mapping",
        f"SELECT uniqExact({natural_id}) - uniqExact({key}), uniqExact({natural_id}) FROM {db}.{table}",
        description=f"{key} is unique per {natural_id} (no surrogate-key collision)",
    )


def relationship(table: str, src: str, fk: str, db: str, dim: str, dim_key: str,
                 severity: str = "error", max_fail_rate: float = 0.0) -> Check:
    """Every foreign key exists in its dimension (the Unknown member -1 counts as existing)."""
    return Check(
        table, f"{fk}_in_{dim}", "relationship",
        f"SELECT countIf({fk} NOT IN (SELECT {dim_key} FROM {db}.{dim})), count() FROM {src}",
        severity=severity, max_fail_rate=max_fail_rate,
        description=f"{fk} references an existing {dim}.{dim_key}",
    )


def unknown_rate(table: str, src: str, fk: str, max_fail_rate: float) -> Check:
    """Share of rows pointing at the Unknown member (-1): the source id was empty."""
    return Check(
        table, f"{fk}_unknown_rate", "unknown_rate",
        f"SELECT countIf({fk} = -1), count() FROM {src}",
        severity="warn", max_fail_rate=max_fail_rate,
        description=f"at most {max_fail_rate:.0%} of rows have {fk} = -1 (Unknown)",
    )


def non_negative(table: str, src: str, column: str) -> Check:
    return Check(
        table, f"{column}_non_negative", "non_negative",
        f"SELECT countIf({column} < 0), count() FROM {src}",
        description=f"{column} >= 0",
    )


def in_range(table: str, src: str, column: str, low, high, severity: str = "warn") -> Check:
    """Plausible range (NULLs are not counted: not_empty covers required columns)."""
    return Check(
        table, f"{column}_in_range", "range",
        f"SELECT countIf({column} IS NOT NULL AND ({column} < {low} OR {column} > {high})), count() FROM {src}",
        severity=severity, description=f"{column} between {low} and {high}",
    )


def accepted_values(table: str, src: str, column: str, values: list, severity: str = "warn") -> Check:
    listed = ", ".join(sql_str(v) for v in values)
    return Check(
        table, f"{column}_accepted_values", "accepted_values",
        f"SELECT countIf({column} NOT IN ({listed})), count() FROM {src}",
        severity=severity, description=f"{column} in ({listed})",
    )


def rule(table: str, src: str, name: str, valid_if: str, severity: str = "error",
         description: str = "") -> Check:
    """Row-level business rule; `valid_if` is a ClickHouse boolean expression a good row satisfies."""
    return Check(
        table, name, "rule",
        f"SELECT countIf(NOT ({valid_if})), count() FROM {src}",
        severity=severity, description=description or valid_if,
    )


# --------------------------------------------------------------------------- runner


def sql_str(value) -> str:
    """ClickHouse string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _num(value) -> Optional[float]:
    if value is None or value in ("", "\\N", "NULL", "nan"):
        return None
    return float(value)


class DQRunner:
    def __init__(self, query_row: Callable, execute: Callable, db: str, layer: str,
                 window_start: str, window_end: str):
        """window_start / window_end: 'YYYY-MM-DD HH:MM:SS' (UTC), the rows this run checks."""
        self.query_row = query_row
        self.execute = execute
        self.db = db
        self.layer = layer
        self.window_start = window_start
        self.window_end = window_end
        self.run_id = f"{layer}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"

    def run_check(self, check: Check) -> Result:
        if check.skip_reason:
            return Result(check, "skipped", details=check.skip_reason)
        try:
            if check.compute is not None:
                row = check.compute()
            else:
                row = self.query_row(check.sql)
            if not row:
                return Result(check, "error", details="query returned no row")
            failed, total = int(_num(row[0]) or 0), int(_num(row[1]) or 0)
            observed = _num(row[2]) if len(row) > 2 else None
            details = str(row[3]) if len(row) > 3 and row[3] is not None else ""
            result = Result(check, "pass", failed, total, observed, details)
            if failed and result.fail_rate > check.max_fail_rate:
                result.status = "fail"
            return result
        except Exception as exc:  # a broken check must not hide the others
            return Result(check, "error", details=f"{type(exc).__name__}: {exc}"[:2000])

    def run(self, checks: List[Check]) -> List[Result]:
        self.execute(RESULTS_DDL.format(db=self.db))
        results = [self.run_check(c) for c in checks]
        self.save(results)
        self.report(results)
        return results

    def save(self, results: List[Result]) -> None:
        now = f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}"
        rows = []
        for r in results:
            c = r.check
            observed = "NULL" if r.observed is None else repr(float(r.observed))
            rows.append(
                "(" + ", ".join([
                    sql_str(self.run_id), sql_str(now), sql_str(self.layer),
                    sql_str(self.window_start), sql_str(self.window_end),
                    sql_str(c.table), sql_str(c.name), sql_str(c.check_type),
                    sql_str(c.severity), sql_str(r.status), str(r.failed), str(r.total),
                    repr(r.fail_rate), repr(float(c.max_fail_rate)), observed,
                    sql_str(r.details), sql_str(c.description),
                ]) + ")"
            )
        for i in range(0, len(rows), 200):
            self.execute(f"INSERT INTO {self.db}.{RESULTS_TABLE} VALUES " + ", ".join(rows[i:i + 200]))

    def report(self, results: List[Result]) -> None:
        print(f"[dq] run {self.run_id}  layer={self.layer}  window {self.window_start} .. {self.window_end}")
        for r in results:
            c = r.check
            observed = "" if r.observed is None else f" observed={r.observed:g}"
            print(
                f"[dq] {r.status.upper():7} {c.severity:5} {c.table}.{c.name}: "
                f"{r.failed}/{r.total} ({r.fail_rate:.2%}){observed} {r.details}".rstrip(),
                flush=True,
            )
        counts = {s: sum(1 for r in results if r.status == s) for s in ("pass", "fail", "skipped", "error")}
        print(f"[dq] summary {counts}; results in {self.db}.{RESULTS_TABLE} run_id='{self.run_id}'")

    @staticmethod
    def raise_on_errors(results: List[Result]) -> None:
        blocking = [r for r in results if r.blocking]
        if blocking:
            names = ", ".join(f"{r.check.table}.{r.check.name} ({r.status})" for r in blocking)
            raise DataQualityError(f"{len(blocking)} blocking data-quality check(s): {names}")
