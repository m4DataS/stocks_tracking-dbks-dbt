import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr

spark = SparkSession.builder.getOrCreate()

# --- ADLS Gen2 auth via account key in secret scope ---
ACCOUNT_NAME = "sa4finnhub2gen"
SCOPE_NAME   = "finance_tracking"          # your scope
SECRET_KEY   = "sa4finnhub2gen_key"        # where you stored the account key

account_key = dbutils.secrets.get(SCOPE_NAME, SECRET_KEY)

spark.conf.set(
    f"fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net",
    account_key
)

def ingest_to_bronze(input_path: str, output_table: str, checkpoint_path: str):
    # 1) Read Event Hubs Capture Avro files with Auto Loader
    raw_df = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "avro")          # Capture format
             .option("cloudFiles.inferColumnTypes", "true")
             .load(input_path)
    )

    # TODO: adjust column name after inspecting one file.
    # For many Event Hubs captures it's "Body" (capital B) or "body".
    # Start with "Body" and change if needed.
    json_df = raw_df.selectExpr("CAST(Body AS STRING) AS json_str")

    # 2) Parse Finnhub JSON payload
    schema = """
      type STRING,
      data ARRAY<STRUCT<
        p: DOUBLE,
        s: STRING,
        t: LONG,
        v: DOUBLE
      >>,
      ingestion_ts LONG
    """

    parsed_df = json_df.select(from_json(col("json_str"), schema).alias("j")).select("j.*")

    trades_df = (
        parsed_df
          .where(col("type") == "trade")
          .select(expr("explode(data) AS t"), col("ingestion_ts"))
          .select(
              col("t.s").alias("symbol"),
              col("t.p").alias("price"),
              col("t.v").alias("volume"),
              col("t.t").alias("event_ts"),
              col("ingestion_ts")
          )
    )

    # 3) Write as Bronze Delta (streaming, but finite with availableNow/once)
    (
        trades_df.writeStream
                 .format("delta")
                 .option("checkpointLocation", checkpoint_path)
                 .outputMode("append")
                 .trigger(availableNow=True)   # or .trigger(once=True)
                 .toTable(output_table)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finnhub Bronze Layer Ingestion from Event Hubs Capture (Avro)")
    parser.add_argument(
        "--input_path",
        type=str,
        required=True,
        help="Root capture path, e.g. abfss://<container>@<acct>.dfs.core.windows.net/<capture-prefix>"
    )
    parser.add_argument(
        "--output_table",
        type=str,
        required=True,
        help="Target Delta table name, e.g. catalog.schema.finnhub_trades_bronze"
    )
    parser.add_argument(
        "--checkpoint_path",
        type=str,
        required=True,
        help="DBFS or Volume path for checkpoint, e.g. dbfs:/checkpoints/finnhub_trades_bronze"
    )

    args = parser.parse_args()
    ingest_to_bronze(args.input_path, args.output_table, args.checkpoint_path)
