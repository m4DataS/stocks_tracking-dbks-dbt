import argparse
from pyspark.sql import SparkSession


def setup_silver_cdf(
    spark: SparkSession,
    catalog: str,
    schema: str,
    tickers: list[str],
) -> None:
    """
    One-time setup:
    - Create pipeline_control table
    - Seed initial tickers
    - Create silver_<ticker> and silver_<ticker>_cdf tables for each ticker
    """

    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")

    pipeline_control_tbl = f"{catalog}.{schema}.pipeline_control"

    # 1. Control table (one-time, idempotent)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {pipeline_control_tbl} (
          ticker              STRING,
          last_ingestion_time TIMESTAMP,
          last_cdf_version    BIGINT,
          last_run_time       TIMESTAMP,
          backfill_active     BOOLEAN
        )
        USING DELTA
        TBLPROPERTIES (
          delta.constraint.ticker_pk = 'ticker IS NOT NULL'
        )
    """)

    # Seed initial tickers with MERGE (no defaults needed)
    if tickers:
        values_clause = " UNION ALL ".join(
            [f"SELECT '{t}' AS ticker" for t in tickers]
        )
        spark.sql(f"""
            MERGE INTO {pipeline_control_tbl} AS tgt
            USING ({values_clause}) AS src
            ON tgt.ticker = src.ticker
            WHEN NOT MATCHED THEN
              INSERT (ticker, last_cdf_version, backfill_active)
              VALUES (src.ticker, 0, false)
        """)

    # 2–3. Per-ticker silver + CDF tables
    for t in tickers:
        silver = f"{catalog}.{schema}.silver_{t}"
        silver_cdf = f"{catalog}.{schema}.silver_{t}_cdf"

        # Silver table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {silver} (
              ticker           STRING,
              timestamp        TIMESTAMP,
              interval         STRING,
              open             DECIMAL(38,10),
              high             DECIMAL(38,10),
              low              DECIMAL(38,10),
              close            DECIMAL(38,10),
              adj_close        DECIMAL(38,10),
              volume           BIGINT,
              first_seen_time  TIMESTAMP,
              last_update_time TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (interval)
        """)

        # Enable CDF
        spark.sql(f"""
            ALTER TABLE {silver}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)

        # History table (CDF)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {silver_cdf}
            USING DELTA AS
            SELECT *, current_timestamp() AS _event_time
            FROM (
              SELECT * FROM table_changes('{silver}', 0)
            ) WHERE 1 = 0
        """)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup silver and CDF tables for tickers")
    parser.add_argument("--catalog", type=str, required=True, help="Unity Catalog name, e.g. workspace")
    parser.add_argument("--schema", type=str, required=True, help="Schema name, e.g. finance_tracking_stocks")
    parser.add_argument("--tickers", type=str, required=True, help="Comma-separated list of tickers, e.g. AAPL,GOOGL")

    args = parser.parse_args()

    tickers_list = [t.strip() for t in args.tickers.split(",") if t.strip()]

    spark = SparkSession.builder.getOrCreate()

    setup_silver_cdf(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        tickers=tickers_list,
    )
