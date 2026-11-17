import json
import math
import threading
import time
from collections import deque

import pandas as pd
import requests
import streamlit as st
import websocket

REST = "https://api.bybit.com"
WS = "wss://stream.bybit.com/v5/public/linear"

class Feed:
    def __init__(self, symbol):
        self.symbol = symbol
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.bids, self.asks = {}, {}
        self.history = deque(maxlen=900)
        self.last = 0.0
        self.status = "Connecting"
        self.error = ""
        self.ws = None
        self.ready = False
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def close(self):
        self.stop.set()
        if self.ws:
            self.ws.close()

    def run(self):
        while not self.stop.is_set():
            with self.lock:
                self.ready = False
                self.bids.clear()
                self.asks.clear()
                self.history.clear()
                self.last = 0
                self.status = "Connecting"
            def opened(ws):
                ws.send(json.dumps({"op": "subscribe", "args": [f"orderbook.200.{self.symbol}"]}))
            def message(ws, raw):
                msg = json.loads(raw)
                if msg.get("op") == "subscribe" and not msg.get("success", False):
                    with self.lock:
                        self.error = str(msg.get("ret_msg", "Subscription failed"))
                    return
                if not msg.get("topic", "").startswith("orderbook."):
                    return
                data = msg.get("data", {})
                now = time.time()
                with self.lock:
                    if msg.get("type") == "snapshot" or data.get("u") == 1:
                        self.bids.clear()
                        self.asks.clear()
                        self.history.clear()
                        self.ready = True
                    if not self.ready:
                        return
                    for key, book in [("b", self.bids), ("a", self.asks)]:
                        for price, qty in data.get(key, []):
                            p, q = float(price), float(qty)
                            if q == 0:
                                book.pop(p, None)
                            else:
                                book[p] = q
                    self.last = now
                    self.status = "Live"
                    self.error = ""
                    if self.bids and self.asks and (not self.history or now-self.history[-1][0] >= 1):
                        self.history.append((now, self.bids.copy(), self.asks.copy()))
            def errored(ws, error):
                with self.lock:
                    self.error = str(error)
                    self.status = "Connection error"
            self.ws = websocket.WebSocketApp(WS, on_open=opened, on_message=message, on_error=errored)
            try:
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                with self.lock:
                    self.error = str(exc)
            with self.lock:
                self.status = "Disconnected; reconnecting"
            if self.stop.wait(3):
                break

    def snapshot(self):
        with self.lock:
            return (self.bids.copy(), self.asks.copy(), list(self.history),
                    self.last, self.status, self.error)

@st.cache_data(ttl=300)
def instruments():
    result, cursor = [], ""
    while True:
        params = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        r = requests.get(REST+"/v5/market/instruments-info", params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            raise RuntimeError(data.get("retMsg", "API error"))
        result.extend(x["symbol"] for x in data["result"]["list"]
                      if x.get("contractType") == "LinearPerpetual"
                      and x.get("quoteCoin") == "USDT" and x.get("status") == "Trading")
        cursor = data["result"].get("nextPageCursor", "")
        if not cursor:
            return sorted(result)

@st.cache_data(ttl=60)
def candles(symbol):
    r = requests.get(REST+"/v5/market/kline", params={"category": "linear", "symbol": symbol,
                     "interval": "60", "limit": 1000}, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(data.get("retMsg", "API error"))
    df = pd.DataFrame(data["result"]["list"], columns=["time", "open", "high", "low", "close", "volume", "turnover"])
    df = df.astype(float).sort_values("time")
    server_ms = data.get("time", time.time()*1000)
    df = df[df.time+3600000 <= server_ms].copy()
    df["EMA50"] = df.close.ewm(span=50, adjust=False).mean()
    df["EMA200"] = df.close.ewm(span=200, adjust=False).mean()
    df["UTC"] = pd.to_datetime(df.time, unit="ms", utc=True)
    return df.set_index("UTC")

def bucket(book, step):
    result = {}
    base = math.log1p(step)
    for p, q in book.items():
        idx = math.floor(math.log(p)/base)
        result[idx] = result.get(idx, 0)+p*q
    return result

def zones(book, history, side, mid, radius, step, window, min_seconds, multiplier):
    current = bucket(book, step)
    values = list(current.values())
    baseline = pd.Series(values).median() if values else 0
    records = []
    recent = [x for x in history if x[0] >= time.time()-window]
    prior = [(t, bucket(b if side == "Buy" else a, step)) for t,b,a in recent]
    for idx, amount in current.items():
        low = math.exp(idx*math.log1p(step))
        high = math.exp((idx+1)*math.log1p(step))
        center = (low+high)/2
        if abs(center/mid-1) > radius or amount < baseline*multiplier:
            continue
        since = None
        for t, bins in reversed(prior):
            if bins.get(idx, 0) < amount*0.5:
                break
            since = t
        age = max(0, prior[-1][0]-since) if since is not None and prior else 0
        records.append({"side":side, "low":low, "high":high,
                        "distance_%":(center/mid-1)*100, "notional_USDT":round(amount,2),
                        "vs_median":round(amount/baseline,2) if baseline else 0,
                        "observed_seconds":round(age,1),
                        "state":"Persistent candidate" if age >= min_seconds else "Collecting"})
    return sorted(records, key=lambda x:x["notional_USDT"], reverse=True)[:5]

st.set_page_config(page_title="Bybit Orderbook Lab", layout="wide")
st.title("Bybit perpetual orderbook lab")
st.caption("Read-only public market data. Candidate liquidity zones are NOT guaranteed support/resistance.")
try:
    symbols = instruments()
except Exception as exc:
    st.error(f"Cannot load contracts: {exc}")
    st.info("Check network/API availability. HTTP 403 may indicate regional access restrictions; use an available authorized data source.")
    st.stop()
symbol = st.sidebar.selectbox("USDT perpetual", symbols, index=symbols.index("ETHUSDT") if "ETHUSDT" in symbols else 0)
radius = st.sidebar.slider("Analysis distance (%)", 0.1, 3.0, 0.5, 0.1)/100
step = st.sidebar.slider("Price bucket width (%)", 0.01, 0.20, 0.05, 0.01)/100
multiplier = st.sidebar.slider("Minimum size / median bucket", 1.0, 10.0, 3.0, 0.5)
min_seconds = st.sidebar.slider("Persistence required (seconds)", 5, 180, 30, 5)
window = 300
if "feed" not in st.session_state or st.session_state.feed.symbol != symbol:
    if "feed" in st.session_state:
        st.session_state.feed.close()
    st.session_state.feed = Feed(symbol)
st.sidebar.warning("Only 200 levels per side are sampled. A selected distance may extend beyond available depth.")

@st.fragment(run_every="2s")
def dashboard():
    bids, asks, history, last, status, error = st.session_state.feed.snapshot()
    stale = time.time()-last if last else float("inf")
    st.write(f"Connection: {status} | Local data age: {stale:.1f}s")
    if error:
        st.warning(error)
    if not bids or not asks or stale > 10:
        st.warning("Waiting for fresh orderbook. Analysis hidden while disconnected or stale.")
        return
    bid, ask = max(bids), min(asks)
    if bid >= ask:
        st.warning("Invalid/crossed book; wait for a fresh snapshot.")
        return
    mid = (bid+ask)/2
    buy = sum(p*q for p,q in bids.items() if p >= mid*(1-radius))
    sell = sum(p*q for p,q in asks.items() if p <= mid*(1+radius))
    imbalance = (buy-sell)/(buy+sell) if buy+sell else 0
    cols = st.columns(4)
    cols[0].metric("Mid price", f"{mid:,.8g}")
    cols[1].metric("Bid notional (USDT)", f"{buy:,.0f}")
    cols[2].metric("Ask notional (USDT)", f"{sell:,.0f}")
    cols[3].metric("Displayed-depth imbalance", f"{imbalance:+.1%}")
    coverage_b = (mid-min(bids))/mid*100
    coverage_a = (max(asks)-mid)/mid*100
    st.caption(f"Available depth coverage: bids {coverage_b:.3f}%, asks {coverage_a:.3f}%. Positive imbalance means more displayed bid notional, not a buy signal.")
    if min(coverage_b, coverage_a) < radius*100:
        st.warning("Incomplete coverage of selected distance. Imbalance is truncated; do not interpret it as a full symmetric-range comparison.")
    records = zones(bids, history, "Buy", mid, radius, step, window, min_seconds, multiplier)
    records += zones(asks, history, "Sell", mid, radius, step, window, min_seconds, multiplier)
    st.subheader("Candidate liquidity zones")
    if records:
        st.dataframe(pd.DataFrame(records), hide_index=True, use_container_width=True)
    else:
        st.info("No buckets pass the size filter yet.")
    st.caption("Persistence measures sampled bucket notional remaining >=50% of its current size, within the last 5 minutes. It does not track individual orders or prove absorption. Reconnect/snapshot resets history.")
    with st.expander("Top 20 orderbook levels"):
        b = sorted(bids.items(), reverse=True)[:20]
        a = sorted(asks.items())[:20]
        st.dataframe(pd.DataFrame({"bid_price":[p for p,q in b], "bid_qty":[q for p,q in b],
                                  "ask_price":[p for p,q in a], "ask_qty":[q for p,q in a]}), hide_index=True)
    try:
        df = candles(symbol)
        st.subheader("Closed 1-hour candles: close + EMA50/200")
        if len(df) >= 200:
            row = df.iloc[-1]
            earlier = df.iloc[-6]
            bull = row.EMA50 > row.EMA200 and row.close > row.EMA50 and row.EMA50 > earlier.EMA50 and row.EMA200 > earlier.EMA200
            bear = row.EMA50 < row.EMA200 and row.close < row.EMA50 and row.EMA50 < earlier.EMA50 and row.EMA200 < earlier.EMA200
            st.write("Simple EMA context: "+("BULLISH" if bull else "BEARISH" if bear else "NEUTRAL"))
        else:
            st.warning("Insufficient candle history for EMA200 context.")
        st.line_chart(df[["close","EMA50","EMA200"]].tail(300))
        st.caption("This prototype uses a simple EMA context, not the earlier Pine hysteresis state machine. No historical orderbook backtest is included.")
    except Exception as exc:
        st.warning(f"Kline fetch failed: {exc}")

dashboard()
