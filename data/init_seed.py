# data/init_seed.py

import requests
import time
import os
import json
import threading
import atexit
from core.logger import global_logger as logger

# === 1H CANDLE CONFIG ===
BINANCE_1H_URL = "https://fapi.binance.com/fapi/v1/klines"
CANDLE_LIMIT_1H = 120  # Use 60 for buffer

# === Fetch historical 1H candles ===
def fetch_klines_1h(symbol, limit=CANDLE_LIMIT_1H):
    params = {
        "symbol": symbol,
        "interval": "1h",
        "limit": limit
    }
    try:
        resp = requests.get(BINANCE_1H_URL, params=params, timeout=10)
        if resp.status_code != 200:
            logger.log_error(f"{symbol} ❌ Failed to fetch 1H klines: HTTP {resp.status_code}")
            return []
        data = resp.json()
        candles = [{
            "symbol": symbol,
            "interval": "1h",
            "open_time": k[0],
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "is_closed": True,
            "timestamp": k[6]
        } for k in data]
        return candles
    except Exception as e:
        logger.log_error(f"{symbol} ❌ Exception while fetching 1H klines: {e}")
        return []
def warm_start_cache(pairs, rolling_engine):
    for pair in pairs:
        logger.log_info(f"{pair} 🧊 Warming up rolling cache with 1 Hour historical candles...")
        candles = fetch_klines_1h(pair)
        if not candles:
            continue
        for candle in candles:
            rolling_engine.update(pair, candle)
        logger.log_once(f"{pair} ✅ 1H candles loaded: {len(candles)}")
        time.sleep(0.2)
