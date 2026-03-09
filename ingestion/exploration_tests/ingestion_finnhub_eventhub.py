import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr

spark = SparkSession.builder.getOrCreate()


def ingest_from_event_hubs(output_table: str, checkpoint_path: str):
    # 1) Get Event Hubs connection string from Databricks secrets
    # Adjust scope/key to your setup.
    eh_conn_str = dbutils.secrets.get("finance_tracking", "eventhub_conn_str")

    eh_conf = {
        "eventhubs.connectionString": eh_conn_str
    }

    # 2) Read stream from Event Hubs
    raw_df = (
        spark.readStream
             .format("eventhubs")
             .options(**eh_conf)
             .load()
    )

    # 3) Parse JSON from body
    json_df = raw_df.selectExpr("CAST(body AS STRING) as json_str")

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
        .select(expr("explode(data) as t"), col("ingestion_ts"))
        .select(
            col("t.s").alias("symbol"),
            col("t.p").alias("price"),
            col("t.v").alias("volume"),
            col("t.t").alias("event_ts"),
            col("ingestion_ts")
        )
    )

    # 4) Write to Delta (Bronze)
    (
        trades_df.writeStream
                 .format("delta")
                 .option("checkpointLocation", checkpoint_path)
                 .outputMode("append")
                 .toTable(output_table)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finnhub Bronze Layer Ingestion from Event Hubs")
    parser.add_argument("--output_table", type=str, required=True)
    parser.add_argument(
        "--checkpoint_path",
        type=str,
        required=True,
        help="DBFS or Volume path to store checkpoint, e.g. dbfs:/checkpoints/bronze_finnhub_trades"
    )
    args = parser.parse_args()

    ingest_from_event_hubs(args.output_table, args.checkpoint_path)
