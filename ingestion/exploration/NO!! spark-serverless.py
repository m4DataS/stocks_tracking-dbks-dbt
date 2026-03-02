from datetime import datetime, timezone
import pandas as pd
from pyspark.sql import Row
import yfinance as yf

########## Function
# fetch function for one ticker
def fetch_ticker(ticker):
    """
    Fetch Yahoo data for a single ticker.
    Returns a list of Row objects (Spark-friendly) or empty list if failed
    """
    try:
        df = yf.download(
            tickers=ticker,
            start="2000-01-01",
            group_by="ticker",
            auto_adjust=False,
            threads=False,  # threads=False since Spark is handling parallelism
            progress=False
        )
        if df.empty:
            return []
        df = df.reset_index()
        df["ticker"] = ticker
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        # Convert to list of Row objects for Spark
        return [Row(**row) for row in df.to_dict(orient="records")]
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return []

########### Run  
##### Parallelize tickers with Spark on serverless cluster (no access to JVM)
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
# Create an RDD from the ticker list
rdd = spark.sparkContext.parallelize(tickers, numSlices=len(tickers))
# Map the fetch function over the RDD
fetched_rdd = rdd.flatMap(fetch_ticker)  # flatMap: returns multiple rows per ticker


##### Convert to spark DataFrame
df_spark = spark.createDataFrame(fetched_rdd)
df_spark.show(5)

##### Save as Parquet

BASE_PATH = "/FileStore/finance_news/stock-tracking/yahoo/stock_prices_spark"
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
output_path = f"{BASE_PATH}/{INGESTION_DATE}/"

df_spark.write.mode("overwrite").parquet(output_path)