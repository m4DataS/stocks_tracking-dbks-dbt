import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StructType, StructField, StringType, DoubleType, LongType


# Force all columns to safe supertypes at read time — avoids BIGINT vs DOUBLE conflict on volume
FORCE_SCHEMA = StructType([
    StructField("date",      LongType(), True),
    StructField("datetime",  LongType(), True),
    StructField("open",      DoubleType(), True),
    StructField("high",      DoubleType(), True),
    StructField("low",       DoubleType(), True),
    StructField("close",     DoubleType(), True),
    StructField("adj_close", DoubleType(), True),
    StructField("volume",    DoubleType(), True),  # read as Double, cast to BIGINT after
    StructField("ticker",    StringType(), True),
])

def normalize_time_col(col_name):
    """Handles INT64 that can be epoch micros, millis, or seconds."""
    return (
        F.when(
            F.col(col_name) > 1e10,
            F.to_timestamp(F.col(col_name) / 1e6)    # microseconds
        ).when(
            F.col(col_name) > 1e7,
            F.to_timestamp(F.col(col_name) / 1000)   # milliseconds
        ).otherwise(
            F.to_timestamp(F.col(col_name))           # seconds
        )
    )

def ingest_to_bronze(input_path: str, output_table: str):
    spark = SparkSession.builder.getOrCreate()

    DECIMAL_TYPE = DecimalType(38, 10)
    PRICE_COLS = ["open", "high", "low", "close", "adj_close"]

    print(f"[{output_table}] Reading Parquet files from: {input_path}")

    # ── Read with forced schema to avoid type conflicts across files ────────────
    df = spark.read.option("inferSchema", "false").schema(FORCE_SCHEMA).parquet(input_path)

    # ── Extract interval from filename ────────────────────────────────────────
    # Pattern: ticker_timestamp_interval_threadid.parquet
    df = (df
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("interval", F.regexp_extract(F.col("file_name"), r"_[0-9]+_([a-z0-9]+)_[0-9]+\.parquet$", 1))
        .drop("file_name")
    )

    # ── Normalize timestamp (daily = 'date', intraday = 'datetime') ───────────
    if "datetime" in df.columns and "date" in df.columns:
        df = df.withColumn("timestamp", F.coalesce(normalize_time_col("datetime"), normalize_time_col("date")))
    elif "datetime" in df.columns:
        df = df.withColumn("timestamp", normalize_time_col("datetime"))
    elif "date" in df.columns:
        df = df.withColumn("timestamp", normalize_time_col("date"))

    for time_col in ["datetime", "date"]:
        if time_col in df.columns:
            df = df.drop(time_col)

    # ── Cast price columns ────────────────────────────────────────────────────
    for col in PRICE_COLS:
        df = (df
            .withColumn(f"{col}_str", F.col(col).cast("string"))
            .withColumn(col,          F.col(col).cast(DECIMAL_TYPE))
        )

    # ── Cast remaining columns ────────────────────────────────────────────────
    df = (df
        .withColumn("volume", F.col("volume").cast("bigint"))
        .withColumn("ingestion_time", F.current_timestamp())
    )

    # ── Column ordering ───────────────────────────────────────────────────────
    final_cols = [
        "ticker",
        "timestamp",
        "interval",
        "open",       "open_str",
        "high",       "high_str",
        "low",        "low_str",
        "close",      "close_str",
        "adj_close",  "adj_close_str",
        "volume",
        "ingestion_time"
    ]
    df = df.select([c for c in final_cols if c in df.columns])

    row_count = df.count()
    print(f"[{output_table}] Loaded {row_count} rows.")

    if row_count == 0:
        print("No data to write. Skipping.")
        return

    # ── Write ─────────────────────────────────────────────────────────────────
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "false")
       .saveAsTable(output_table)
    )

    print(f"[{output_table}] Bronze ingestion complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion")
    parser.add_argument("--input_path", type=str, required=True)
    parser.add_argument("--output_table", type=str, required=True)
    args = parser.parse_args()

    ingest_to_bronze(args.input_path, args.output_table)