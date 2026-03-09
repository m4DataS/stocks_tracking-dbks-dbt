import argparse
import os
import json
import requests
from requests.exceptions import ReadTimeout
from datetime import datetime, timezone

DEFAULT_BASE_URL = "https://ca-finnhub-ws-endpoint.salmonrock-60dca45f.westus2.azurecontainerapps.io"
DEFAULT_CHECKPOINT_PATH = "/Volumes/workspace/finance_tracking_stocks/stock_data/finnhub/checkpoint/since_ts.txt"

def human_ts(ts_seconds):
    if ts_seconds is None:
        return "None"
    return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")


def load_since_ts(checkpoint_path: str) -> int:
    if not checkpoint_path:
        return 0
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path, "r") as f:
            s = f.read().strip()
            if s:
                return int(s)
    return 0

def save_since_ts(checkpoint_path: str, since_ts: int):
    if not checkpoint_path:
        return
    os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
    with open(checkpoint_path, "w") as f:
        f.write(str(since_ts))

import requests
from requests.exceptions import ReadTimeout

def fetch_trades(base_url: str, since_ts: int, limit: int):
    url = f"{base_url.rstrip('/')}/trades"
    params = {"since_ts": since_ts, "limit": limit}

    # Separate connect and read timeouts, and a couple of retries
    attempts = 3
    for i in range(attempts):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=(5, 30),  # 5s connect, 30s read
            )
            resp.raise_for_status()
            return resp.json()
        except ReadTimeout:
            print(f"Read timeout on attempt {i+1}/{attempts} for {url}")
            if i == attempts - 1:
                raise

def process_trades(trades_batch, output_path: str = None):
    trades = trades_batch.get("trades", [])
    if not trades:
        print("No new trades.")
        return None

    max_ingestion_ts = None
    rows = []

    for msg in trades:
        msg_type = msg.get("type")
        ingestion_ts = msg.get("ingestion_ts")
        data = msg.get("data", [])

        for t in data:
            price = t.get("p")
            volume = t.get("v")
            symbol = t.get("s")
            trade_ts = t.get("t")

            rows.append({
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "trade_ts": trade_ts,
                "ingestion_ts": ingestion_ts,
                "msg_type": msg_type,
            })

        if ingestion_ts is not None:
            if max_ingestion_ts is None or ingestion_ts > max_ingestion_ts:
                max_ingestion_ts = ingestion_ts

    if output_path:
        os.makedirs(output_path, exist_ok=True)
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_file = os.path.join(output_path, f"trades_{now}.jsonl")
        with open(out_file, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        print(f"Wrote {len(rows)} records to {out_file}")
    else:
        for r in rows:
            print(r)

    return max_ingestion_ts

def main(args):
    since_ts = load_since_ts(args.checkpoint_path)
    print(f"Current since_ts={since_ts} ({human_ts(since_ts)})")

    try:
        batch = fetch_trades(args.base_url, since_ts, args.limit)
        new_since_ts = process_trades(
            batch,
            output_path=args.output_path,
        )

        if new_since_ts is not None:
            save_since_ts(args.checkpoint_path, new_since_ts)
            print(f"Updated since_ts to {new_since_ts} ({human_ts(new_since_ts)})")
        else:
            print("No new since. Nothing to update, checkpoint state stays the same.")
    except Exception as e:
        print(f"Error during polling/processing: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Poll Finnhub trades from Container App and ingest incrementally"
    )
    parser.add_argument(
        "--base_url",
        type=str,
        default=DEFAULT_BASE_URL,
        help="Base URL of the container app, e.g. https://ca-finnhub-ws-endpoint...azurecontainerapps.io"
    )
    parser.add_argument(
        "--checkpoint_path",
        type=str,
        default=DEFAULT_CHECKPOINT_PATH,
        help="Path in a volume/DBFS to store since_ts cursor, e.g. /Volumes/raw/finnhub/checkpoint/since_ts.txt"
    )
    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="Output folder (volume/DBFS) for ingested files, e.g. /Volumes/raw/finnhub/trades"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max number of buffered messages to fetch per run"
    )

    args = parser.parse_args()
    main(args)
