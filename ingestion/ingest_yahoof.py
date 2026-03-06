# python multi threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import pytz
import os
from typing import Any, Dict, List, Optional, Tuple, Union, Generator
import pandas as pd
import yfinance as yf
import psutil
import threading
import time
import requests

# Thread-safe metrics collection
metrics_lock = threading.Lock()
all_metrics: List[Tuple[str, str, float, float, int]] = []
chunk_metrics: List[Tuple[datetime, str, float, int]] = []

class RateLimiter:
    def __init__(self, max_requests_per_minute):
        self.lock = threading.Lock()
        self.max_requests = max_requests_per_minute
        self.requests = 0
        self.start_time = time.time()

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.start_time

            if elapsed >= 60:
                self.requests = 0
                self.start_time = now

            if self.requests >= self.max_requests:
                sleep_time = 60 - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self.requests = 0
                self.start_time = time.time()

            self.requests += 1

# Global instance initialized in main
rate_limiter: Optional[RateLimiter] = None

def chunk_list(lst: List[Any], size: int) -> Generator[List[Any], None, None]:
    for i in range(0, len(lst), size):
        # Using a typed slice
        yield lst[i:i + size]


def fetch_yahoo(tickers: List[str], start: str, end: Optional[str], interval: str, max_retries: int = 3) -> Tuple[pd.DataFrame, List[str]]:
    """
    Fetch data from Yahoo Finance with retries and exponential backoff
    to handle rate limiting (429 errors).
    """
    retries = 0
    while retries < max_retries:
        try:
            if rate_limiter:
                rate_limiter.wait()
                
            df = yf.download(
                tickers=tickers,
                start=start,
                end=end,
                interval=interval,
                group_by="ticker",
                auto_adjust=False,
                threads=False,  # use our custom thread management
                progress=False
            )
            
            if df.empty:
                return pd.DataFrame(), tickers

            records = []
            for ticker in tickers:
                if ticker not in df.columns.levels[0]:
                    continue

                tmp = df[ticker].reset_index()
                tmp["ticker"] = ticker
                records.append(tmp)

            if not records:
                return pd.DataFrame(), tickers

            out = pd.concat(records)
            out.columns = [c.lower().replace(" ", "_") for c in out.columns]

            missing = set(tickers) - set(df.columns.levels[0])
            if missing:
                print(f"Missing tickers after successful download: {missing}")
                for ticker in missing:
                    try:
                        if rate_limiter:
                            rate_limiter.wait()

                        retry_df = yf.download(
                            tickers=ticker,
                            start=start,
                            end=end,
                            interval=interval,
                            auto_adjust=False,
                            threads=False,
                            progress=False
                        )

                        if retry_df.empty:
                            print(f"Retry failed for {ticker}")
                            continue

                        retry_df = retry_df.reset_index()
                        retry_df["ticker"] = ticker
                        retry_df.columns = [c.lower().replace(" ", "_") for c in retry_df.columns]

                        out = pd.concat([out, retry_df], ignore_index=True)
                        print(f"Recovered missing ticker {ticker}")

                    except Exception as e:
                        print(f"Retry error for {ticker}: {e}")

            # Final check: which tickers are still missing after all attempts?
            if not out.empty:
                valid_tickers = set(out["ticker"].unique())
                still_missing = list(set(tickers) - valid_tickers)
            else:
                still_missing = tickers
            
            # Filter out tickers that have rows but all OHLC values are NaN
            if not out.empty:
                # Identification of truly empty tickers that yfinance returned as NaN blocks
                actual_valid = []
                for t in out["ticker"].unique():
                    t_df = out[out["ticker"] == t]
                    # If all core price columns are NaN, it's not a valid ticker download
                    if t_df[["open", "high", "low", "close"]].isnull().all().all():
                        if t not in still_missing:
                            still_missing.append(t)
                    else:
                        actual_valid.append(t)
                
                # Update 'out' to only contain actual valid data
                out = out[out["ticker"].isin(actual_valid)]
                still_missing = sorted(list(set(still_missing)))

            return out, still_missing

        except Exception as e:
            retries += 1
            # Improvement 3 & 7: Specific 429 handling and capped backoff
            if "429" in str(e):
                wait_time = 60
                print(f"Rate limit hit (429) for {tickers}. Sleeping 60s...")
            else:
                wait_time = min(60, 2 ** retries)
                print(f"Error fetching data for {tickers}: {e}. Retry {retries}/{max_retries} in {wait_time}s...")
            
            time.sleep(wait_time)
            
    return pd.DataFrame(), tickers


def ingest_chunk(chunk: List[str], start: str, end: Optional[str], interval: str, output_dir: str) -> Dict[str, Any]:
    try:
        ts, stage, cpu, mem, threads = log_metrics(f"Start chunk {chunk}")
        with metrics_lock:
            all_metrics.append((ts, stage, cpu, mem, threads))

        start_time = time.time()
        pdf, missing_tickers = fetch_yahoo(chunk, start=start, end=end, interval=interval)
        elapsed_sec = time.time() - start_time
        rows_downloaded = len(pdf)

        if pdf.empty:
            print(f"Chunk {chunk} returned no data")
            return {"chunk": chunk, "rows": 0, "elapsed_sec": elapsed_sec, "missing": missing_tickers}

        with metrics_lock:
            chunk_metrics.append(
                (datetime.now(timezone.utc), str(chunk), elapsed_sec, rows_downloaded)
            )

        ts, stage, cpu, mem, threads = log_metrics(f"End chunk {chunk}")
        with metrics_lock:
            all_metrics.append((ts, stage, cpu, mem, threads))

        os.makedirs(output_dir, exist_ok=True)
        # Improvement 5: Avoid duplicate filenames with thread ID and include interval for metadata
        file_name = os.path.join(output_dir, f"{chunk[0]}_{int(start_time)}_{interval}_{threading.get_ident()}.parquet")
        # Enforce microsecond precision (us) as Spark doesn't support nanoseconds by default
        pdf.to_parquet(file_name, index=False, coerce_timestamps='us')

        print(f"Saved {len(pdf)} rows (observations) for chunk {chunk} to {file_name}")
        return {"chunk": chunk, "rows": len(pdf), "elapsed_sec": elapsed_sec, "missing": missing_tickers}
        
    except Exception as e:
        print(f"CRITICAL ERROR in chunk {chunk}: {e}")
        return {"chunk": chunk, "rows": 0, "elapsed_sec": 0, "error": str(e), "missing": chunk}


def log_metrics(stage: str = "") -> Tuple[str, str, float, float, int]:
    p = psutil.Process()
    # interval=1 is blocking, we might want to reduce this if it slows down ingestion
    # or use interval=None for instantaneous value
    cpu = p.cpu_percent(interval=0.1) 
    mem = p.memory_info().rss / 1e6
    threads = threading.active_count()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{ts}] Stage: {stage} | CPU: {cpu:.1f}% | Memory: {mem:.1f} MB | Threads: {threads}")
    return ts, stage, cpu, mem, threads


def monitor_metrics(interval: int = 5) -> None:
    while not stop_monitoring.is_set():
        ts, stage, cpu, mem, threads = log_metrics("Monitor")
        with metrics_lock:
            all_metrics.append((ts, stage, cpu, mem, threads))
        time.sleep(interval)


BASE_PATH = "/Volumes/workspace/finance_tracking_stocks/stock_data/yahoo/ingestion/"

tickers_base = ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN", "META"]

tickers_top30 = [
    "PCAR","MDLZ","INSM","ALNY","ZS","CSGP","KDP","CCEP",
    "HON","INTU","MAR","MCHP","BKR","VRSK","MELI","WMT","TSLA","SBUX",
    "AMGN","DDOG","FTNT","CEG","STX","AMAT","MRVL","APP","WDC","ARM"
]

more_tickers = [
    "ADBE","ORCL","NFLX","PYPL","CSCO","CRM","INTC",
    "QCOM","TXN","AMD","BKNG","EXC","ADP","MU","LRCX","ROST","BIIB"
]

extra_tickers = [
    "F","GM","GE","BA","NKE","SBUX","SHOP","SQ","UBER","LYFT","GOOG",
    "TWTR","SNAP","ZM","DOCU","ETSY","SPOT","PLTR","COIN","NVAX","CRWD"
]


if __name__ == "__main__":
    # Local parameters : will be overridden by Databricks job parameters
    parser = argparse.ArgumentParser(description="Yahoo Stock Data Ingestion")
    parser.add_argument("--tickers", type=str, default="base")
    parser.add_argument("--start", type=str, default="")
    parser.add_argument("--end", type=str, default="")
    parser.add_argument("--interval", type=str, default="")
    parser.add_argument("--output", type=str, default=BASE_PATH)
    parser.add_argument("--max_workers", type=int, default=3)
    parser.add_argument("--chunk_size", type=int, default=10)
    parser.add_argument("--metric_interval", type=int, default=5)

    args = parser.parse_args()

    # Improvement 1: Initialize global rate limiter
    REQUESTS_PER_MINUTE = 60
    rate_limiter = RateLimiter(REQUESTS_PER_MINUTE)

    # Dynamic date calculation for daily automation
    # If no dates are provided, or if Databricks dynamic tokens are passed as literals
    # (which happens during local runs or if the variables aren't resolved)
    interval = getattr(args, "interval", None)
    # --------------------------------------------------
    # DEFAULT interval fallback
    # --------------------------------------------------
    if not interval or "{{" in str(interval):
        args.interval = "1d"
        print("Interval empty or token → default to 1d")
    else:
        print(f"Interval: {args.interval}")
    print(f"Requested interval: {interval}")
    ny = pytz.timezone("America/New_York")
    now = datetime.now(ny)

    start_val = getattr(args, "start", None)
    end_val = getattr(args, "end", None)

    # --------------------------------------------------
    # CASE 1: BOTH start AND end NOT provided
    # -> compute full window based on interval
    # --------------------------------------------------
    if (not start_val or "{{" in str(start_val)) and (not end_val or "{{" in str(end_val)):
        if interval == "1m":
            args.end = now
            args.start = now - timedelta(minutes=1)
            print(f"Streaming window (both missing): {args.start} → {args.end}")

        elif interval == "15m":
            args.end = now
            args.start = now - timedelta(hours=1)
            print(f"Hourly window (both missing): {args.start} → {args.end}")

        else:
            args.start = (now - timedelta(days=1)).strftime("%Y-%m-%d")
            args.end = now.strftime("%Y-%m-%d")
            print(f"Daily window (both missing): {args.start} → {args.end}")

    # --------------------------------------------------
    # CASE 2: ONLY start MISSING -> use end (or now) - window
    # --------------------------------------------------
    elif not start_val or "{{" in str(start_val):
        if not end_val or "{{" in str(end_val):
            ref = now
        else:
            try:
                ref = datetime.fromisoformat(str(end_val))
            except Exception:
                ref = now

        if interval == "1m":
            args.start = ref - timedelta(minutes=1)
        elif interval == "15m":
            args.start = ref - timedelta(hours=1)
        else:
            args.start = (ref - timedelta(days=1)).strftime("%Y-%m-%d")

        args.end = ref
        print(f"Start auto (end provided): {args.start} → {args.end}")

    # --------------------------------------------------
    # CASE 3: ONLY end MISSING -> use start + window
    # --------------------------------------------------
    elif not end_val or "{{" in str(end_val):
        try:
            ref = datetime.fromisoformat(str(start_val))
        except Exception:
            ref = now

        if interval == "1m":
            args.end = ref + timedelta(minutes=1)
        elif interval == "15m":
            args.end = ref + timedelta(hours=1)
        else:
            args.end = now

        print(f"End auto (start provided): {args.start} → {args.end}")

    # --------------------------------------------------
    # CASE 4: BOTH provided -> use as-is
    # --------------------------------------------------
    else:
        print(f"Using provided window: {args.start} → {args.end}")

    if args.tickers.lower() == "base":
        tickers = tickers_base
    elif args.tickers.lower() == "top30":
        tickers = tickers_top30
    elif args.tickers.lower() == "all":
        tickers = tickers_base + tickers_top30 + more_tickers + extra_tickers
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]

    print(f"Fetching data for {len(tickers)} tickers with max_workers={args.max_workers} and chunk_size={args.chunk_size}")

    ticker_chunks = list(chunk_list(tickers, args.chunk_size))

    stop_monitoring = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_metrics,
        args=(args.metric_interval,)
    )
    monitor_thread.start()

    results = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [
            executor.submit(ingest_chunk, chunk, args.start, args.end,args.interval, args.output)
            for chunk in ticker_chunks
        ]

        for future in as_completed(futures):
            results.append(future.result())

    stop_monitoring.set()
    monitor_thread.join()

    # Final summary
    total_rows = sum(r.get("rows", 0) for r in results)
    total_elapsed = sum(r.get("elapsed_sec", 0) for r in results)
    errors = [r for r in results if "error" in r]
    
    all_missing = []
    for r in results:
        all_missing.extend(r.get("missing", []))
    
    # Unique and sorted list of missing tickers
    all_missing = sorted(list(set(all_missing)))
    
    print("-" * 50)
    print(f"Ingestion complete: {total_rows} rows saved from {len(results)} chunks.")
    if all_missing:
        print(f"Tickers that remained missing after all attempts: {all_missing}")
    if errors:
        print(f"Encountered {len(errors)} chunk errors.")
    print("-" * 50)