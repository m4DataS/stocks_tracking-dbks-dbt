import argparse
from datetime import datetime

from pyspark.sql import SparkSession, functions as F


def process_silver_for_ticker(
    spark: SparkSession,
    catalog: str,
    schema: str,
    ticker: str,
    backfill: bool,
    backfill_start: str,
    backfill_end: str,
) -> None:
    """
    Main processing logic for one ticker:
    - Read control_state
    - Build bronze_inc
    - MERGE into silver_<ticker>
    - Read CDF and append into silver_<ticker>_cdf
    - Update pipeline_control
    """

    # Fully qualified objects
    pipeline_control_tbl = f"{catalog}.{schema}.pipeline_control"
    bronze_tbl = f"{catalog}.{schema}.bronze_yahoo_stocks"
    silver_tbl = f"{catalog}.{schema}.silver_{ticker}"
    silver_cdf_tbl = f"{catalog}.{schema}.silver_{ticker}_cdf"

    # 0. Set catalog / schema
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")

    # 1. control_state temp view
    backfill_str = "true" if backfill else "false"
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW control_state AS
        SELECT *,
               CASE
                   WHEN '{backfill_str}' = 'true' THEN NULL
                   ELSE last_ingestion_time
               END AS effective_ingestion_filter
        FROM {pipeline_control_tbl}
        WHERE ticker = '{ticker}'
    """)

    # 2. bronze_inc temp view + MERGE into silver
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW bronze_inc AS
        SELECT
          b.ticker,
          b.timestamp,
          b.interval,
          b.open,
          b.high,
          b.low,
          b.close,
          b.adj_close,
          b.volume,
          (CASE WHEN b.open IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN b.high IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN b.low  IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN b.close IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN b.adj_close IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN b.volume IS NOT NULL THEN 1 ELSE 0 END
          ) AS quality,
          current_timestamp() AS load_time
        FROM {bronze_tbl} b
        CROSS JOIN control_state c
        WHERE b.ticker = '{ticker}'
          AND (
            ('{backfill_str}' = 'false' AND b.ingestion_time > coalesce(c.effective_ingestion_filter, '1900-01-01'))
            OR
            ('{backfill_str}' = 'true' AND b.timestamp BETWEEN '{backfill_start}' AND '{backfill_end}')
          )
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY b.ticker, b.timestamp, b.interval
          ORDER BY
            (CASE WHEN b.open IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN b.high IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN b.low  IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN b.close IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN b.adj_close IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN b.volume IS NOT NULL THEN 1 ELSE 0 END
            ) DESC,
            b.ingestion_time DESC
        ) = 1
    """)

    spark.sql(f"""
        MERGE INTO {silver_tbl} AS tgt
        USING bronze_inc AS src
        ON  tgt.ticker   = src.ticker
        AND tgt.interval = src.interval
        AND tgt.timestamp = src.timestamp

        WHEN MATCHED AND src.quality >= (
          (CASE WHEN tgt.open IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN tgt.high IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN tgt.low IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN tgt.close IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN tgt.adj_close IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN tgt.volume IS NOT NULL THEN 1 ELSE 0 END)
        ) THEN
          UPDATE SET
            open             = COALESCE(src.open, tgt.open),
            high             = COALESCE(src.high, tgt.high),
            low              = COALESCE(src.low, tgt.low),
            close            = COALESCE(src.close, tgt.close),
            adj_close        = COALESCE(src.adj_close, tgt.adj_close),
            volume           = COALESCE(src.volume, tgt.volume),
            last_update_time = src.load_time

        WHEN NOT MATCHED THEN
          INSERT (ticker, timestamp, interval, open, high, low, close, adj_close, volume, first_seen_time, last_update_time)
          VALUES (src.ticker, src.timestamp, src.interval, src.open, src.high, src.low, src.close, src.adj_close, src.volume, src.load_time, src.load_time)
    """)

    # 3. CDF consumption
    # 3.1 last_cdf_version from pipeline_control
    ctrl = (
        spark.table(pipeline_control_tbl)
        .filter(F.col("ticker") == ticker)
        .select(F.coalesce("last_cdf_version", F.lit(0)).alias("last_cdf_version"))
        .collect()
    )
    last_cdf_version = ctrl[0]["last_cdf_version"] if ctrl else 0

    # 3.2 current version of silver table
    current_ver = (
        spark.sql(f"DESCRIBE HISTORY {silver_tbl}")
        .agg(F.max("version").alias("ver"))
        .collect()[0]["ver"]
    )

    # 3.3 read CDF and append to history table
    cdf_changes_df = spark.sql(f"""
        SELECT *, current_timestamp() AS _event_time
        FROM table_changes('{silver_tbl}', {last_cdf_version + 1}, {current_ver})
    """)

    cdf_changes_df.write.mode("append").insertInto(silver_cdf_tbl)

    # 4. Update pipeline_control
    # 4.1 compute new_last_ingestion_time
    bronze_max_ing = (
        spark.table(bronze_tbl)
        .filter(F.col("ticker") == ticker)
        .agg(F.max("ingestion_time").alias("max_ing"))
        .collect()[0]["max_ing"]
    )

    ctrl_state = spark.table("control_state").select("effective_ingestion_filter").collect()
    effective_filter = ctrl_state[0]["effective_ingestion_filter"] if ctrl_state else None

    default_min = datetime(1900, 1, 1)
    candidates = [bronze_max_ing or default_min, effective_filter or default_min]
    new_last_ingestion_time = max(candidates)

    new_last_cdf_version = current_ver
    new_last_run_time = datetime.utcnow()
    backfill_active = backfill

    temp_df = spark.createDataFrame(
        [(ticker, new_last_ingestion_time, new_last_cdf_version, new_last_run_time, backfill_active)],
        ["ticker", "new_last_ingestion_time", "new_last_cdf_version", "new_last_run_time", "backfill_active"],
    )
    temp_df.createOrReplaceTempView("control_update_src")

    spark.sql(f"""
        MERGE INTO {pipeline_control_tbl} AS tgt
        USING control_update_src AS src
        ON tgt.ticker = src.ticker

        WHEN MATCHED THEN
          UPDATE SET
            tgt.last_ingestion_time = src.new_last_ingestion_time,
            tgt.last_cdf_version    = src.new_last_cdf_version,
            tgt.last_run_time       = src.new_last_run_time,
            tgt.backfill_active     = src.backfill_active

        WHEN NOT MATCHED THEN
          INSERT (ticker, last_ingestion_time, last_cdf_version, last_run_time, backfill_active)
          VALUES (src.ticker, src.new_last_ingestion_time, src.new_last_cdf_version, src.new_last_run_time, src.backfill_active)
    """)


def parse_bool(value: str) -> bool:
    v = value.lower()
    if v in ("True","true", "1", "yes", "y"):
        return True
    if v in ("False", "false", "0", "no", "n"):
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Layer Merge + CDF for one ticker")

    parser.add_argument("--catalog", type=str, required=True, help="Unity Catalog name, e.g. workspace")
    parser.add_argument("--schema", type=str, required=True, help="Schema name, e.g. finance_tracking_stocks")
    parser.add_argument("--ticker", type=str, required=True, help="Ticker symbol, e.g. GOOGL")
    parser.add_argument("--backfill", type=parse_bool, default=False, help="Whether to run in backfill mode (true/false)")
    parser.add_argument("--backfill_start", type=str, default="1900-01-01", help="Backfill start timestamp (YYYY-MM-DD or full timestamp)")
    parser.add_argument("--backfill_end", type=str, default="2100-01-01", help="Backfill end timestamp (YYYY-MM-DD or full timestamp)")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process_silver_for_ticker(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        ticker=args.ticker,
        backfill=args.backfill,
        backfill_start=args.backfill_start,
        backfill_end=args.backfill_end,
    )
