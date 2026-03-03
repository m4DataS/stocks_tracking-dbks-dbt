import argparse
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import pandas as pd
import finnhub
from math import ceil
import json
import unicodedata

# ==========================
# Utility functions
# ==========================
class RateLimiter:
    """
    Thread-safe rate limiter for N requests per minute.
    """
    def __init__(self, max_requests_per_minute):
        self.lock = threading.Lock()
        self.max_requests = max_requests_per_minute
        self.requests_made = 0
        self.start_time = time.time()

    def wait(self):
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.start_time

            # Reset every minute
            if elapsed >= 60:
                self.requests_made = 0
                self.start_time = current_time
                elapsed = 0

            if self.requests_made >= self.max_requests:
                # Wait until the minute resets
                sleep_time = 60 - elapsed
                time.sleep(sleep_time)
                self.requests_made = 0
                self.start_time = time.time()

            self.requests_made += 1

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def log_metrics(stage=""):
    import psutil
    p = psutil.Process()
    cpu = p.cpu_percent(interval=1)
    mem = p.memory_info().rss / 1e6
    threads = threading.active_count()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    all_metrics.append((ts, stage, cpu, mem, threads))
    return ts, stage, cpu, mem, threads

def monitor_metrics(interval=5):
    while not stop_monitoring.is_set():
        log_metrics("Monitor")
        time.sleep(interval)

# Normalize ticker names
def normalize_ticker(s):
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if c.isascii())
    return s.replace(" ", "").replace("'", "").replace("-", "").upper()

# ==========================
# Data ingestion functions
# ==========================
def ingest_chunk_stock(chunk, start, end, output_dir):
    ts, stage, cpu, mem, threads = log_metrics(f"Start chunk {chunk}")
    start_time = time.time()
    records = []

    for symbol in chunk:
        try:
            quote = client.quote(symbol)
            rate_limiter.wait()
            news = client.company_news(symbol, _from=start, to=end)
            rate_limiter.wait()
            financials = client.company_basic_financials(symbol, 'all')
            rate_limiter.wait()

            records.append({
                "symbol": symbol,
                "quote": json.dumps(quote),
                "news": json.dumps(news),
                "financials": json.dumps(financials)
            })
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            records.append({"symbol": symbol, "error": str(e)})

    elapsed_sec = time.time() - start_time
    ts, stage, cpu, mem, threads = log_metrics(f"End chunk {chunk}")

    os.makedirs(output_dir, exist_ok=True)
    file_name = os.path.join(output_dir, f"{chunk[0]}_{int(start_time)}.parquet")
    pd.DataFrame(records).to_parquet(file_name, index=False)
    print(f"Saved {len(records)} rows for chunk {chunk} to {file_name}")

    chunk_metrics.append((datetime.now(timezone.utc), str(chunk), elapsed_sec, len(records)))
    return {"chunk": chunk, "rows": len(records), "elapsed_sec": elapsed_sec}


def ingest_chunk_crypto(chunk, start, end, output_dir):
    ts, stage, cpu, mem, threads = log_metrics(f"Start chunk {chunk}")
    start_time = time.time()
    records = []

    for symbol in chunk:
        try:
            profile = client.crypto_profile(symbol)
            rate_limiter.wait()
            exchanges = client.crypto_exchanges()
            rate_limiter.wait()
            candles = client.crypto_candles(
                f"BINANCE:{symbol}USDT",
                'D',
                int(datetime.fromisoformat(start).timestamp()),
                int(datetime.fromisoformat(end).timestamp())
            )
            rate_limiter.wait()

            records.append({
                "symbol": symbol,
                "profile": json.dumps(profile),
                "exchanges": json.dumps(exchanges),
                "candles": json.dumps(candles)
            })
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            records.append({"symbol": symbol, "error": str(e)})

    elapsed_sec = time.time() - start_time
    ts, stage, cpu, mem, threads = log_metrics(f"End chunk {chunk}")

    os.makedirs(output_dir, exist_ok=True)
    file_name = os.path.join(output_dir, f"{chunk[0]}_{int(start_time)}.parquet")
    pd.DataFrame(records).to_parquet(file_name, index=False)
    print(f"Saved {len(records)} rows for chunk {chunk} to {file_name}")

    chunk_metrics.append((datetime.now(timezone.utc), str(chunk), elapsed_sec, len(records)))
    return {"chunk": chunk, "rows": len(records), "elapsed_sec": elapsed_sec}

# ==========================
# Global variables
# ==========================
european_stocks = [
    "ASML", "LVMH", "SAP", "AstraZeneca", "HSBC", "Nestle", "Roche", "Novo Nordisk", "Shell", "Novartis", "L’Oreal", "Inditex", "TotalEnergies", "Unilever", "Siemens", "Schneider Electric", "Deutsche Telekom", "Airbus", "Sanofi", "Allianz", "UBS", "Rio Tinto", "EssilorLuxottica", "BP", "Mercedes-Benz", "Zurich Insurance", "BNP Paribas", "AXA", "Iberdrola", "Ferrari", "Enel", "ABB", "Diageo", "Equinor", "Munich Re", "RELX", "Volkswagen", "Compass Group", "Vinci", "National Grid", "Bayer", "Intesa Sanpaolo", "Volvo Group", "Deutsche Post", "Prudential", "Danone", "Hermes", "Barclays", "Kering"
    ]
binance_tokens = [
    "BTC", "ETH", "BNB", "USDT", "USDC", "XRP", "SOL", "ADA", "DOGE", "SHIB", "TRX", "AVAX", "LINK", "DOT", "LTC", "TON", "XLM", "UNI", "VET", "FIL"
    ]
BASE_PATH = "/Volumes/workspace/finance_news/stock-tracking/finhub/stock_prices"

# ==========================
# Main ingestion
# ==========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finnhub Stock/Crypto Data Ingestion")
    parser.add_argument("--tickers", type=str, default=None)
    parser.add_argument("--start", type=str, default="2022-01-15")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--output", type=str, default=BASE_PATH)
    parser.add_argument("--max_workers", type=int, default=5)
    parser.add_argument("--chunk_size", type=int, default=10)
    parser.add_argument("--metric_interval", type=int, default=5)
    parser.add_argument("--asset_type", type=str, default="stock", help="stock or crypto")
    parser.add_argument("--requests_per_minute", type=int, default=60, help="API rate limit per minute")
    args = parser.parse_args([])

    REQUESTS_PER_MINUTE = args.requests_per_minute
    rate_limiter = RateLimiter(REQUESTS_PER_MINUTE)

    # Databricks secret for API key
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        API_KEY = dbutils.secrets.get(scope="finance_tracking", key="finnhub_api_key")
    except Exception:
        API_KEY = os.getenv("FINNHUB_API_KEY", "YOUR_API_KEY")

    client = finnhub.Client(api_key=API_KEY)

    # Determine tickers
    if args.tickers is None:
        tickers = european_stocks if args.asset_type == "stock" else binance_tokens
    else:
        tickers = [normalize_ticker(t) for t in args.tickers.split(",")]

    print(f"Fetching data for {len(tickers)} {args.asset_type} tickers")

    ticker_chunks = list(chunk_list(tickers, args.chunk_size))
    results = []
    all_metrics = []
    chunk_metrics = []

    if args.end is None:
        args.end = datetime.now().strftime("%Y-%m-%d")

    # Start metrics monitor
    stop_monitoring = threading.Event()
    monitor_thread = threading.Thread(target=monitor_metrics, args=(args.metric_interval,))
    monitor_thread.start()

    # Multithread ingestion
    ingest_fn = ingest_chunk_stock if args.asset_type == "stock" else ingest_chunk_crypto
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [executor.submit(ingest_fn, chunk, args.start, args.end, args.output) for chunk in ticker_chunks]
        for future in as_completed(futures):
            results.append(future.result())

    stop_monitoring.set()
    monitor_thread.join()

    # Save metrics
    metrics_df = pd.DataFrame(all_metrics, columns=["timestamp", "stage", "cpu_percent", "memory_mb", "threads"])
    metrics_df.to_parquet(os.path.join(args.output, f"metrics_{int(time.time())}.parquet"), index=False)

    chunk_df = pd.DataFrame(chunk_metrics, columns=["timestamp", "chunk", "elapsed_sec", "rows_downloaded"])
    chunk_df.to_parquet(os.path.join(args.output, f"chunks_{int(time.time())}.parquet"), index=False)

    total_rows = sum(r["rows"] for r in results)
    print(f"Ingestion complete: {total_rows} rows saved from {len(results)} chunks.")