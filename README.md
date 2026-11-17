# Bybit Order Book Lab

Real-time perpetual futures order-book dashboard built with Python and Streamlit. Read-only public market data - no API key required.

## Features

- Live order book via Bybit WebSocket (v5, 200 levels per side): snapshot + delta updates, automatic reconnect, stale-data detection
- Liquidity-zone detection: log-scaled price bucketing, zone size vs median filter, persistence tracking (how long a zone has held)
- Metrics: mid price, bid/ask notional within a configurable distance, displayed-depth imbalance
- Depth-coverage warnings so truncated depth is never mistaken for the full book
- Cached USDT-perpetual instrument list and hourly kline context via Bybit REST API

## Run

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Tech

Python, Streamlit, pandas, websocket-client, threading
