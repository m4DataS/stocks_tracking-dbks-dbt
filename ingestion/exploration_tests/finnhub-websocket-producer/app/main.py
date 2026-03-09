import os
import json
import time
from datetime import datetime

import websocket
from azure.eventhub import EventHubProducerClient, EventData

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
if not FINNHUB_API_KEY:
    raise RuntimeError("FINNHUB_API_KEY not set")

EVENTHUB_CONN_STR = os.getenv("EVENTHUB_CONN_STR")  # full connection string
if not EVENTHUB_CONN_STR:
    raise RuntimeError("EVENTHUB_CONN_STR not set")

producer = EventHubProducerClient.from_connection_string(conn_str=EVENTHUB_CONN_STR)

# STOCK_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
CRYPTO_SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT", "BINANCE:XRPUSDT"]
SYMBOLS = CRYPTO_SYMBOLS

BASE_URL = "wss://ws.finnhub.io"
message_counter = 0


def send_to_event_hubs(payload: dict):
    event = EventData(json.dumps(payload))
    with producer:
        producer.send_batch([event])


def on_message(ws, message: str):
    global message_counter
    message_counter += 1

    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        print("Decode error:", message)
        return

    if data.get("type") != "trade":
        return

    data["ingestion_ts"] = int(time.time())
    send_to_event_hubs(data)

    if message_counter % 100 == 0:
        print(f"Sent {message_counter} messages to Event Hubs")


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, code, msg):
    print("WebSocket closed:", code, msg)


def on_open(ws):
    print("Subscribing to:", SYMBOLS)
    for sym in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
        time.sleep(0.05)


def main():
    ws_url = f"{BASE_URL}?token={FINNHUB_API_KEY}"
    websocket.enableTrace(False)
    app = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    app.on_open = on_open
    app.run_forever()


if __name__ == "__main__":
    main()
