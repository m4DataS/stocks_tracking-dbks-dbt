import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql import DataFrame
from functools import reduce


def cast_df(df: DataFrame, decimal_type: DecimalType) -> DataFrame:
    PRICE_COLS = ["open", "high", "low", "close", "adj_close"]

    # ── Extract interval from filename ────────────────────────────────────────
    df = (df
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("interval", F.regexp_extract(F.col("file_name"), r"_[0-9]+_([a-z0-9]+)_[0-9]+\.parquet$", 1))
        .drop("file_name")
    )

    # ── Normalize timestamp ───────────────────────────────────────────────────
    time_col = "datetime" if "datetime" in df.columns else "date" if "date" in df.columns else None
    if time_col:
        col_type = dict(df.dtypes)[time_col]

        if col_type in ("timestamp", "timestamp_ntz", "date"):
            # Already a proper date/timestamp — just cast
            df = df.withColumn("timestamp", F.col(time_col).cast("timestamp"))
        else:
            # Numeric epoch — convert based on magnitude
            raw = F.col(time_col).cast("double")
            df = df.withColumn(
                "timestamp",
                F.when(raw > 1e10, F.to_timestamp(raw / 1e6))   # microseconds
                 .when(raw > 1e7,  F.to_timestamp(raw / 1000))  # milliseconds
                 .otherwise(       F.to_timestamp(raw))         # seconds
            )

        for c in ["datetime", "date"]:
            if c in df.columns:
                df = df.drop(c)

    # ── Cast price columns ────────────────────────────────────────────────────
    for col in PRICE_COLS:
        if col in df.columns:
            df = (df.withColumn(f"{col}_str", F.col(col).cast("string"))
                .withColumn(col,          F.col(col).cast(decimal_type))
                )

    # ── Cast remaining columns ────────────────────────────────────────────────
    df = (df.withColumn("volume",       F.col("volume").cast("bigint"))
        .withColumn("ingestion_time", F.current_timestamp())
        )

    return df


def ingest_to_bronze(input_path: str, output_table: str, checkpoint_path: str):
    spark = SparkSession.builder.getOrCreate()
    DECIMAL_TYPE = DecimalType(38, 10)

    print(f"[{output_table}] Reading from: {input_path}")
    print(f"[{output_table}] Checkpoint  : {checkpoint_path}")

    final_cols = [
        "ticker", 
        "timestamp",
        "interval",
        "open",      "open_str",
        "high",      "high_str",
        "low",       "low_str",
        "close",     "close_str",
        "adj_close", "adj_close_str",
        "volume",    
        "ingestion_time"
    ]

    # ── Read with Auto Loader (tracks processed files automatically) ──────────
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(input_path)
    )

    df = cast_df(df, DECIMAL_TYPE)
    df = df.select([F.col(c) if c in df.columns else F.lit(None).alias(c) for c in final_cols])

    # ── Write stream to Delta (checkpoint prevents reprocessing) ─────────────
    (df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)   # batch-like: process all new files then stop
        .toTable(output_table)
    )

    print(f"[{output_table}] Bronze ingestion complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion")
    parser.add_argument("--input_path",       type=str, required=True)
    parser.add_argument("--output_table",     type=str, required=True)
    parser.add_argument("--checkpoint_path",  type=str, required=True,
                        help="DBFS or Volume path to store Auto Loader checkpoint, e.g. dbfs:/checkpoints/bronze_stock")
    args = parser.parse_args()

    ingest_to_bronze(args.input_path, args.output_table, args.checkpoint_path)