# python multi threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import os
import pandas as pd
import yfinance as yf
import psutil
import threading
import time


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def fetch_yahoo(tickers, start="2000-01-01", end=None):
    df = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False
    )

    records = []

    for ticker in tickers:
        if ticker not in df.columns.levels[0]:
            continue

        tmp = df[ticker].reset_index()
        tmp["ticker"] = ticker
        records.append(tmp)

    if not records:
        return pd.DataFrame()

    out = pd.concat(records)
    out.columns = [c.lower().replace(" ", "_") for c in out.columns]

    missing = set(tickers) - set(df.columns.levels[0])
    if missing:
        print(f"Missing tickers: {missing}")

    return out


def ingest_chunk(chunk, start, end, output_dir):
    ts, stage, cpu, mem, threads = log_metrics(f"Start chunk {chunk}")
    all_metrics.append((ts, stage, cpu, mem, threads))

    start_time = time.time()
    pdf = fetch_yahoo(chunk, start=start, end=end)
    elapsed_sec = time.time() - start_time
    rows_downloaded = len(pdf)

    if pdf.empty:
        print(f"Chunk {chunk} returned no data")
        return {"chunk": chunk, "rows": 0, "elapsed_sec": elapsed_sec}

    chunk_metrics.append(
        (datetime.now(timezone.utc), str(chunk), elapsed_sec, rows_downloaded)
    )

    ts, stage, cpu, mem, threads = log_metrics(f"End chunk {chunk}")
    all_metrics.append((ts, stage, cpu, mem, threads))

    os.makedirs(output_dir, exist_ok=True)
    file_name = os.path.join(output_dir, f"{chunk[0]}_{int(start_time)}.parquet")
    pdf.to_parquet(file_name, index=False)

    print(f"Saved {len(pdf)} rows for chunk {chunk} to {file_name}")

    return {"chunk": chunk, "rows": len(pdf), "elapsed_sec": elapsed_sec}


def log_metrics(stage=""):
    p = psutil.Process()
    cpu = p.cpu_percent(interval=1)
    mem = p.memory_info().rss / 1e6
    threads = threading.active_count()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{ts}] Stage: {stage} | CPU: {cpu:.1f}% | Memory: {mem:.1f} MB | Threads: {threads}")
    return ts, stage, cpu, mem, threads


def monitor_metrics(interval=5):
    while not stop_monitoring.is_set():
        ts, stage, cpu, mem, threads = log_metrics("Monitor")
        all_metrics.append((ts, stage, cpu, mem, threads))
        time.sleep(interval)


BASE_PATH = "/Volumes/workspace/finance_news/stock-tracking/yahoo/stock_prices"
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

tickers_base = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

tickers_top30 = [
    "PCAR","MDLZ","INSM","AAPL","ALNY","ZS","META","CSGP","KDP","CCEP",
    "HON","INTU","MAR","MCHP","BKR","VRSK","MELI","WMT","TSLA","SBUX",
    "AMGN","DDOG","FTNT","CEG","STX","AMAT","MRVL","APP","WDC","ARM"
]

more_tickers = [
    "GOOGL","MSFT","NVDA","ADBE","ORCL","NFLX","PYPL","CSCO","CRM","INTC",
    "QCOM","TXN","AMD","BKNG","EXC","ADP","MU","LRCX","ROST","BIIB"
]

extra_tickers = [
    "F","GM","GE","BA","NKE","SBUX","SHOP","SQ","UBER","LYFT",
    "TWTR","SNAP","ZM","DOCU","ETSY","SPOT","PLTR","COIN","NVAX","CRWD"
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Yahoo Stock Data Ingestion")
    parser.add_argument("--tickers", type=str, default=None)
    parser.add_argument("--start", type=str, default="2000-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--output", type=str, default=BASE_PATH)
    parser.add_argument("--max_workers", type=int, default=5)
    parser.add_argument("--chunk_size", type=int, default=10)
    parser.add_argument("--metric_interval", type=int, default=5)

    args = parser.parse_args()

    if args.tickers is None:
        tickers = tickers_base
    elif args.tickers.lower() == "top30":
        tickers = tickers_top30
    elif args.tickers.lower() == "all":
        tickers = tickers_top30 + more_tickers + extra_tickers
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]

    print(f"Fetching data for {len(tickers)} tickers")

    ticker_chunks = list(chunk_list(tickers, args.chunk_size))

    results = []
    all_metrics = []
    chunk_metrics = []

    stop_monitoring = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_metrics,
        args=(args.metric_interval,)
    )
    monitor_thread.start()

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [
            executor.submit(ingest_chunk, chunk, args.start, args.end, args.output)
            for chunk in ticker_chunks
        ]

        for future in as_completed(futures):
            results.append(future.result())

    stop_monitoring.set()
    monitor_thread.join()

    metrics_df = pd.DataFrame(
        all_metrics,
        columns=["timestamp", "stage", "cpu_percent", "memory_mb", "threads"]
    )

    chunk_df = pd.DataFrame(
        chunk_metrics,
        columns=["timestamp", "chunk", "elapsed_sec", "rows_downloaded"]
    )

    total_rows = sum(r["rows"] for r in results)
    print(f"Ingestion complete: {total_rows} rows saved from {len(results)} chunks.")