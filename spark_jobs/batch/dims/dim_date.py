"""Create calendar rows for the requested processing window."""

from pyspark.sql import functions as F

from common import ensure_table, get_spark, parse_window, with_audit, write_append

TABLE = "dim_date"

DDL = """
    CREATE TABLE IF NOT EXISTS {db}.dim_date 
    (
        date_key Int32,
        full_date Date,
        day UInt8,
        week UInt8,
        month UInt8,
        quarter UInt8,
        year UInt16,
        is_weekend UInt8,
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    ) 
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY date_key
    """


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)
    dates = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS full_date"
    )
    out = dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.dayofmonth("full_date").cast("byte").alias("day"),
        F.weekofyear("full_date").cast("byte").alias("week"),
        F.month("full_date").cast("byte").alias("month"),
        F.quarter("full_date").cast("byte").alias("quarter"),
        F.year("full_date").cast("short").alias("year"),
        F.when(F.dayofweek("full_date").isin(1, 7), 1)
        .otherwise(0)
        .cast("byte")
        .alias("is_weekend"),
        F.current_timestamp().alias("src_sys_create_date"),
    )
    write_append(with_audit(out), TABLE)
    print(f"[{TABLE}] wrote calendar dates {start}..{end}")

    spark.stop()


if __name__ == "__main__":
    main()
