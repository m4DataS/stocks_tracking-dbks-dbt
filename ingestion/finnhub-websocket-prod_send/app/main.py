import os
import json
import time
import threading
from typing import List

import websocket
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

# ---------- Config ----------
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
if not FINNHUB_API_KEY:
    raise RuntimeError("FINNHUB_API_KEY not set")

CRYPTO_SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT", "BINANCE:XRPUSDT"]
SYMBOLS = CRYPTO_SYMBOLS

BASE_URL = "wss://ws.finnhub.io"

# ---------- In‑memory buffer ----------
# Each item: {"type": "trade", "data": [...], "ingestion_ts": <epoch_sec>}
TRADE_BUFFER: List[dict] = []
BUFFER_LOCK = threading.Lock()
MAX_BUFFER_SIZE = 10_000


def add_to_buffer(payload: dict):
    payload["ingestion_ts"] = int(time.time())
    with BUFFER_LOCK:
        TRADE_BUFFER.append(payload)
        if len(TRADE_BUFFER) > MAX_BUFFER_SIZE:
            # keep only last MAX_BUFFER_SIZE messages
            del TRADE_BUFFER[: len(TRADE_BUFFER) - MAX_BUFFER_SIZE]


def get_trades_since(since_ts: int) -> List[dict]:
    with BUFFER_LOCK:
        return [m for m in TRADE_BUFFER if m.get("ingestion_ts", 0) > since_ts]


# ---------- WebSocket handlers ----------
def on_message(ws, message: str):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        print("Decode error:", message)
        return

    if data.get("type") != "trade":
        return

    add_to_buffer(data)


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, code, msg):
    print("WebSocket closed:", code, msg)


def on_open(ws):
    print("Subscribing to:", SYMBOLS)
    for sym in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
        time.sleep(0.05)


def run_finnhub_ws():
    ws_url = f"{BASE_URL}?token={FINNHUB_API_KEY}"
    websocket.enableTrace(False)
    while True:
        try:
            app_ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            app_ws.on_open = on_open
            app_ws.run_forever()
        except Exception as e:
            print("WS loop error, retrying in 5s:", e)
            time.sleep(5)


# ---------- FastAPI app ----------
app = FastAPI()


@app.get("/health")
def health():
    with BUFFER_LOCK:
        size = len(TRADE_BUFFER)
    return {"status": "ok", "buffer_size": size}


@app.get("/trades")
def http_get_trades(since_ts: int = 0, limit: int = 1000):
    """
    Return trades with ingestion_ts > since_ts.
    Databricks will call /trades?since_ts=<last_seen_ts>&limit=...
    """
    trades = get_trades_since(since_ts)
    if limit > 0:
        trades = trades[:limit]
    return JSONResponse(content={"trades": trades})


# ---------- Entrypoint ----------
def main():
    # Start the Finnhub WS client in a background thread
    t = threading.Thread(target=run_finnhub_ws, daemon=True)
    t.start()

    # Start FastAPI HTTP server
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
