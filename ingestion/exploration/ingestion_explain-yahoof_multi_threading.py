import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import os
import pandas as pd
import yfinance as yf
#### Custom metrics tracking
import psutil
import threading
import time

########## Functions
##### Chunk tickers : chunk the list of tickers into smaller lists
def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

##### Yahoo fetch function (pure Python)
def fetch_yahoo(tickers, start="2000-01-01", end=None):
    """
    Fetch historical prices for a list of tickers from start to end date
    """
    df = yf.download(
        tickers=tickers,
        start=start,
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False
    )

    records = []

    for ticker in tickers:
        if ticker not in df.columns.levels[0]:
            continue
        # skips the ticker if no data found for him

        tmp = df[ticker].reset_index()
        tmp["ticker"] = ticker
        records.append(tmp)

    if not records:
        return pd.DataFrame()

    out = pd.concat(records)
    out.columns = [c.lower().replace(" ", "_") for c in out.columns]

    # logging missing tickers
    missing = set(tickers) - set(df.columns.levels[0])
    if missing:
        print(f"Missing tickers: {missing}")

    return out

##### Ingest one chunk (unit of parallelism)
def ingest_chunk(chunk, start, end, output_dir):
    """Ingest one ticker chunk and save as parquet"""
    # metrics tracking bloc
    ts, stage, cpu, mem, threads = log_metrics(f"Start chunk {chunk}")
    all_metrics.append((ts, stage, cpu, mem, threads))

    # Measure wall-clock time for the fetch
    start_time = time.time()
    pdf = fetch_yahoo(chunk, start=start, end=end)  # main code : before adding metrcis tracking
    end_time = time.time()
    elapsed_sec = end_time - start_time
    rows_downloaded = len(pdf)

    if pdf.empty:
        print(f"Chunk {chunk} returned no data")
        return {"chunk": chunk, "rows": 0, "elapsed_sec": elapsed_sec}

    # Log chunk activity
    chunk_metrics.append((datetime.now(timezone.utc), str(chunk), elapsed_sec, rows_downloaded))
    
    # Log end metrics
    ts, stage, cpu, mem, threads = log_metrics(f"End chunk {chunk}")
    all_metrics.append((ts, stage, cpu, mem, threads))
    
    ## Write to Parquet
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    file_name = os.path.join(output_dir, f"{chunk[0]}_{int(start_time)}.parquet")
    pdf.to_parquet(file_name, index=False)  # index=False -> Don’t include Pandas row index in Parquet file.
    print(f"Saved {len(pdf)} rows for chunk {chunk} to {file_name}")

    return {"chunk": chunk, "rows": len(pdf), "elapsed_sec": elapsed_sec}


##### function to log metrics
def log_metrics(stage=""):
    """
    Logs CPU %, memory usage, and active threads at a specific stage
    """
    p = psutil.Process()
    cpu = p.cpu_percent(interval=1)  # CPU usage of driver process
    mem = p.memory_info().rss / 1e6  # Memory in MB
    threads = threading.active_count()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[{ts}] Stage: {stage} | CPU: {cpu:.1f}% | Memory: {mem:.1f} MB | Threads: {threads}")
    return ts, stage, cpu, mem, threads

# function to save metrics every N seconds
def monitor_metrics(interval=5):
    """
    Continuously logs CPU, memory, and thread count every `interval` seconds
    """
    while not stop_monitoring.is_set():  # We'll use a flag to stop the thread later
        ts, stage, cpu, mem, threads = log_metrics("Monitor")
        all_metrics.append((ts, stage, cpu, mem, threads))
        time.sleep(interval)

########## Params definition
BASE_PATH = "/Volumes/workspace/finance_tracking_stocks/stock_data/yahoo/ingestion/"
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

CHUNK_SIZE = 50        # number of tickers per API call
MAX_WORKERS = 5        # number of parallel threads


# ---------------------------
# Define ticker lists
# ---------------------------
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


##########  Run ingestion with multithreading
# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Yahoo Stock Data Ingestion")
    parser.add_argument("--tickers", type=str, default=None,
                        help="Comma-separated tickers (e.g. AAPL,MSFT) OR 'top30', 'all' for pre-defined lists")
    parser.add_argument("--start", type=str, default="2000-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--output", type=str, default="/Volumes/workspace/finance_tracking_stocks/stock_data/yahoo/ingestion/", help="Output directory for parquet files")
    parser.add_argument("--max_workers", type=int, default=5, help="Number of threads")
    parser.add_argument("--chunk_size", type=int, default=10, help="Number of tickers per chunk")
    parser.add_argument("--metric_interval", type=int, default=5, help="Start monitor thread a separate thread that samples CPU/memory/threads every X seconds")

    args = parser.parse_args()

    # ---------------------------
    # Determine ticker list
    # ---------------------------
    if args.tickers is None:
        tickers = tickers_base
    elif args.tickers.lower() == "top30":
        tickers = tickers_top30
    elif args.tickers.lower() == "all":
        tickers = tickers_top30 + more_tickers + extra_tickers
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]

    print(f"Fetching data for {len(tickers)} tickers: {tickers[:10]}{'...' if len(tickers) > 10 else ''}")

    # ---------------------------
    # Split tickers into chunks
    # ---------------------------
    ticker_chunks = list(chunk_list(tickers, args.chunk_size))

    results = []
    all_metrics = []  # existing list for CPU/memory/thread metrics
    chunk_metrics = []  # new list for chunk-level activity metrics


    # ---------------------------
    # Multithread ingestion
    # ---------------------------
    # Flag to control when monitoring stops
    stop_monitoring = threading.Event()
    # Start monitor thread a separate thread that samples CPU/memory/threads every X(=args.metric_interval) seconds. Runs in parallel with ingestion threads
    monitor_thread = threading.Thread(target=monitor_metrics, args=(args.metric_interval,))
    monitor_thread.start()

    # Creation of a pool of worker threads
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:  # 5 threads
        # If submit more tasks than threads => they get queued
        # Ex: 5 workers, and I’ll give them tasks one by one.
        futures = [executor.submit(ingest_chunk, chunk, args.start, args.end, args.output) for chunk in ticker_chunks]
        # run ingest_chunk(chunk) in a separate thread : does wait for function to finish
        # returns a future = A placeholder for a result representing
        # # A task that is running or scheduled
        # # And will eventually finish or fail
        # Ex: ask someone to do a task; this is the receipt that lets check later
        # ==> Submit all chunks as tasks - Get a list of Future objects - Tasks start running immediately (up to MAX_WORKERS in parallel)
        for future in as_completed(futures):
            # Iterate over futures in the order they finish NOT in the order they were submitted :
            # for future in futures: == wait for tasks in submission order (A slow task could block everything behind it)
            results.append(future.result())
            # store query to yahoo results

    stop_monitoring.set()  # Signals monitor thread to stop
    monitor_thread.join()   # Wait for it to finish

    ########## Logging
    ##### metrics tracking
    # CPU/memory/dashboard
    metrics_df = pd.DataFrame(all_metrics, columns=["timestamp", "stage", "cpu_percent", "memory_mb", "threads"])
    display(metrics_df)
    # Chunk activity: rows and elapsed time
    chunk_df = pd.DataFrame(chunk_metrics, columns=["timestamp", "chunk", "elapsed_sec", "rows_downloaded"])
    display(chunk_df)

    ### Final thread number = 
    # active threads ≈ 1 (main) + MAX_WORKERS (executor) + 1 (monitor) = MAX_WORKERS + 2
    # 2 = 
    # +1 → the monitoring thread that runs monitor_metrics() continuously
    # +1 → the main thread that runs the ThreadPoolExecutor

    # ---------------------------
    # Summary
    # ---------------------------
    total_rows = sum(r["rows"] for r in results)
    print(f"Ingestion complete: {total_rows} rows saved from {len(results)} chunks.")
    # success = [r for r in results if r[0] == "OK"]
    # empty = [r for r in results if r[0] == "EMPTY"]
    # print(f"Successful files: {len(success)}")
    # print(f"Empty batches: {len(empty)}")