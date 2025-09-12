# data/data_loader.py
"""
Lightweight candle loader used by scalper_runner.py

- Pulls futures klines from Binance (via utils.exchange.client)
- Returns a pd.DataFrame with the columns your UT/STC logic expects:
  ['open', 'high', 'low', 'close', 'volume'] (+ datetime index)
"""

import os
from typing import Optional

import pandas as pd

try:
    # your .env based client
    from utils.exchange import client
except Exception as e:  # fallback if you kept a different path
    raise ImportError(
        "Could not import Binance client from utils.exchange. "
        "Make sure utils/exchange.py exists and exposes `client`."
    ) from e


def _to_dataframe(klines: list) -> pd.DataFrame:
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ]
    df = pd.DataFrame(klines, columns=cols)

    # Cast to numeric
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Timestamps
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df.set_index("open_time", inplace=True)
    df.sort_index(inplace=True)

    # Return only what strategies need
    return df[["open", "high", "low", "close", "volume"]]


def load_latest_candles(symbol: str,
                        timeframe: str,
                        min_candles: int = 300,
                        limit: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch the most recent candles for `symbol` & `timeframe`.
    Ensures at least `min_candles` rows are returned (or as many as Binance can give).
    """
    limit = limit or max(min_candles, 300)

    # python-binance uses UPPERCASE interval constants, but the raw string (e.g. '5m') also works.
    # Using futures klines since you're trading futures.
    klines = client.futures_klines(symbol=symbol, interval=timeframe, limit=limit)

    if not klines or len(klines) == 0:
        return pd.DataFrame()

    df = _to_dataframe(klines)

    if len(df) < min_candles:
        # Return what we have; caller can decide to skip if too few
        return df

    return df.tail(min_candles)
