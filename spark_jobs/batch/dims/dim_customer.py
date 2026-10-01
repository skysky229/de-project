from pyspark.sql import functions as F

from common import (
    UNKNOWN_KEY,
    ensure_table,
    get_spark,
    latest_per_key,
    parse_window,
    read_created_between,
    seed_unknown_member,
    surrogate_key,
    with_audit,
    write_append,
)

TABLE = "dim_customer"

DDL = """
    CREATE TABLE IF NOT EXISTS {db}.dim_customer
    (
        customer_key Int64,
        customer_id String,
        customer_name String,
        customer_type LowCardinality(String),
        credit_terms_days Nullable(Int32),
        primary_freight_type LowCardinality(String),
        account_status LowCardinality(String),
        contract_start_date Nullable(Date),
        annual_revenue_potential Nullable(Decimal(18,2)),
        src_sys_create_date DateTime('UTC'),
        etl_loaded_date DateTime('UTC')
    ) 
    ENGINE = ReplacingMergeTree(src_sys_create_date)
    ORDER BY customer_id
    """

SOURCE_COLUMNS = [
    "customer_id",
    "customer_name",
    "customer_type",
    "credit_terms_days",
    "primary_freight_type",
    "account_status",
    "contract_start_date",
    "annual_revenue_potential",
    "sys_create_date",
]


def transform(raw):
    return latest_per_key(raw, ["customer_id"]).select(
        surrogate_key(F.col("customer_id")).alias("customer_key"),
        "customer_id",
        "customer_name",
        "customer_type",
        F.col("credit_terms_days").cast("int").alias("credit_terms_days"),
        "primary_freight_type",
        "account_status",
        F.to_date("contract_start_date").alias("contract_start_date"),
        F.col("annual_revenue_potential").cast("decimal(18,2)").alias("annual_revenue_potential"),
        F.col("sys_create_date").alias("src_sys_create_date"),
    )


def unknown_member(spark):
    return spark.sql(
        f"""
    SELECT CAST({UNKNOWN_KEY} AS BIGINT) customer_key,
    'UNKNOWN' customer_id,
    'Unknown' customer_name,
    'Unknown' customer_type,
    CAST(NULL AS INT) credit_terms_days,
    'Unknown' primary_freight_type,
    'Unknown' account_status,
    CAST(NULL AS DATE) contract_start_date, CAST(NULL AS DECIMAL(18,2)) annual_revenue_potential,
    CAST('1970-01-01' AS TIMESTAMP) src_sys_create_date
    """
    )


def main():
    start, end = parse_window()
    spark = get_spark(f"{TABLE}_{start}_{end}")
    ensure_table(spark, DDL)
    seed_unknown_member(spark, TABLE, "customer_key", unknown_member(spark))

    raw = read_created_between(spark, "customers", SOURCE_COLUMNS, start, end).cache()

    if raw.count():
        out = with_audit(transform(raw)).cache()
        write_append(out, TABLE)
        print(f"[{TABLE}] wrote {out.count()} rows")
    else:
        print(f"[{TABLE}] window {start}..{end}: read 0 raw rows, nothing to write")

    spark.stop()


if __name__ == "__main__":
    main()
