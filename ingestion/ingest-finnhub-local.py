#!/usr/bin/env python3
import os
import json
import time
from datetime import datetime
from pathlib import Path

import websocket  # pip install websocket-client

# --------------------------------------------------
# Config
# --------------------------------------------------

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
if not FINNHUB_API_KEY:
    raise RuntimeError("FINNHUB_API_KEY env var is not set.")

# Up to 50 symbols are allowed; mix of US stocks and crypto.[web:71]
STOCK_SYMBOLS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "NVDA",
    "TSLA",
    "JPM",
    "BRK.B",
    "SPY",
]

CRYPTO_SYMBOLS = [
    "BINANCE:BTCUSDT",  # Bitcoin[web:83]
    "BINANCE:ETHUSDT",  # Ethereum
    "BINANCE:SOLUSDT",  # Solana
    "BINANCE:XRPUSDT",  # XRP
]

SYMBOLS = STOCK_SYMBOLS + CRYPTO_SYMBOLS

BASE_URL = "wss://ws.finnhub.io"
OUTPUT_DIR = Path("data/finnhub_ws_trades")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ts_str = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
OUT_PATH = OUTPUT_DIR / f"trades_{ts_str}.jsonl"

MAX_MESSAGES = 1000  # or None for infinite
message_counter = 0


# --------------------------------------------------
# Callbacks
# --------------------------------------------------

def on_message(ws, message: str):
    global message_counter
    message_counter += 1

    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        print("Failed to decode message:", message)
        return

    if data.get("type") != "trade":
        return

    with OUT_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data) + "\n")

    if message_counter % 50 == 0:
        print(f"Received {message_counter} messages so far...")

    if MAX_MESSAGES is not None and message_counter >= MAX_MESSAGES:
        print(f"Reached MAX_MESSAGES={MAX_MESSAGES}, closing WebSocket.")
        ws.close()


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print("### WebSocket closed ###", close_status_code, close_msg)


def on_open(ws):
    print("WebSocket opened, subscribing to symbols:")
    for sym in SYMBOLS:
        print("  ", sym)
        sub_msg = json.dumps({"type": "subscribe", "symbol": sym})
        ws.send(sub_msg)
        time.sleep(0.05)


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    token_query = f"?token={FINNHUB_API_KEY}"
    ws_url = BASE_URL + token_query

    websocket.enableTrace(False)
    ws_app = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws_app.on_open = on_open

    print(f"Connecting to {ws_url}")
    ws_app.run_forever()  # stops after MAX_MESSAGES


if __name__ == "__main__":
    main()
