import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr

spark = SparkSession.builder.getOrCreate()


def ingest_from_event_hubs_kafka(namespace: str,
                                 topic: str,
                                 output_table: str,
                                 checkpoint_path: str):
    # Get Event Hubs *namespace* connection string from secrets
    # Same one you already stored
    SCOPE_NAME = "finance_tracking"          # change if different
    SECRET_KEY = "eventhub_conn_str"         # change if different

    eh_connection_string = dbutils.secrets.get(SCOPE_NAME, SECRET_KEY)

    # Kafka bootstrap
    bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"

    # SASL JAAS config
    eh_sasl = (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" '
        f'password="{eh_connection_string}";'
    )

    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": eh_sasl,
        "startingOffsets": "latest"
    }


    # Read stream from Kafka (Event Hubs)
    raw_df = (
        spark.readStream
             .format("kafka")
             .options(**kafka_options)
             .load()
    )

    json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str")

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

    (
        trades_df.writeStream
                 .format("delta")
                 .option("checkpointLocation", checkpoint_path)
                 .outputMode("append")
                 .trigger(availableNow=True)
                 .toTable(output_table)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finnhub Bronze Layer Ingestion via Event Hubs Kafka")
    parser.add_argument("--namespace",
                        type=str,
                        required=True,
                        help="Event Hubs namespace name (without domain), e.g. eh-ns-finnhub")
    parser.add_argument("--topic",
                        type=str,
                        required=True,
                        help="Event Hub name (Kafka topic), e.g. eh-finnhub-trades")
    parser.add_argument("--output_table",
                        type=str,
                        required=True,
                        help="Target Delta table name, e.g. catalog.schema.table")
    parser.add_argument("--checkpoint_path",
                        type=str,
                        required=True,
                        help="DBFS or Volume path for checkpoint, e.g. dbfs:/checkpoints/bronze_finnhub_trades")
    args = parser.parse_args()

    ingest_from_event_hubs_kafka(
        namespace=args.namespace,
        topic=args.topic,
        output_table=args.output_table,
        checkpoint_path=args.checkpoint_path,
    )
