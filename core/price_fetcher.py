# core/price_fetcher.py
"""
Price fetcher + small dispatcher.

Minimal safe changes:
 - Expose `process_price_update(symbol, price)` which invokes partial-TP checks.
 - Call `process_price_update` after candles are fetched/updated and when direct ticker is used.
 - Keep existing behavior; do not remove existing functions.
"""

import time
import traceback
from typing import Optional, Any

import pandas as pd
from datetime import datetime, timezone

# reuse your existing logger and exchange client
from core.logger import global_logger as logger
from core.position_manager import position_manager
from core.config import get_config

# utils.exchange.client is used elsewhere in your repo for Binance calls
from utils.exchange import client  # client should be the binance client already configured
# If your repo provides a function to fetch recent klines, prefer that (we'll call the one in this file)
# Note: This file provides get_recent_klines for convenience and to centralize the partial-TP hook.

POLL_INTERVAL = 1.0  # default polling delay (seconds)


def _to_float_safe(v: Optional[Any]) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def process_price_update(symbol: str, price: float) -> None:
    """
    Minimal integration point: call this with the latest price for a symbol
    whenever your bot receives an update (kline close or ticker update).
    This will invoke position_manager.check_partial_tp for both directions.

    Safe to call repeatedly; position_manager.check_partial_tp() will quickly return
    if there is no position or partial TP isn't configured.
    """
    try:
        price_f = _to_float_safe(price)
        if price_f is None:
            return

        # Trigger partial TP checks for both long & short (only one will be active)
        try:
            position_manager.check_partial_tp(symbol, "long", price_f)
            position_manager.check_partial_tp(symbol, "short", price_f)
        except Exception as e:
            logger.log_error(f"{symbol} ❌ process_price_update: failed to run partial checks: {e}")
            logger.log_debug(traceback.format_exc())
    except Exception as e:
        logger.log_error(f"process_price_update failed for {symbol}: {e}")
        logger.log_debug(traceback.format_exc())


# -------------------------
# Candle / ticker fetchers
# -------------------------

def get_recent_klines(symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    """
    Fetch recent klines and return a cleaned DataFrame.
    This mirrors the pattern used across your repo and ensures we call process_price_update
    right after the latest candle is known.
    """
    try:
        # Use the binance client to fetch klines similar to your earlier implementation
        klines = client.get_historical_klines(symbol=symbol, interval=timeframe, limit=limit + 417)
        if not klines or len(klines) < 1:
            logger.log_warning(f"{symbol} ⚠ No klines returned for {timeframe}")
            return pd.DataFrame()

        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignored"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        df = df[["open_time", "open", "high", "low", "close", "volume"]]
        df = df.astype({
            "open": float, "high": float, "low": float, "close": float, "volume": float
        })

        # Log latest candle and call the partial-TP hook using the latest close price
        latest_time = df["open_time"].iloc[-1]
        current_time = datetime.now(timezone.utc)
        time_diff = (current_time - latest_time).total_seconds()
        logger.log_info(f"{symbol} ✅ Latest candle: {latest_time} UTC, current: {current_time} UTC, diff: {time_diff}s")
        logger.log_info(f"{symbol} ✅ {timeframe} candles loaded: {len(df)}")

        # --- Minimal hook: call partial TP checks ---
        try:
            latest_close = float(df["close"].iloc[-1])
            process_price_update(symbol, latest_close)
        except Exception as e:
            logger.log_debug(f"{symbol} ⚠ Couldn't run process_price_update on latest candle: {e}")

        return df
    except Exception as e:
        logger.log_error(f"{symbol} ❌ Failed to fetch klines: {e}")
        logger.log_debug(traceback.format_exc())
        return pd.DataFrame()


def get_ticker_price(symbol: str) -> float:
    """
    Fetch the current ticker price from Binance and run the partial-TP hook.
    This function is intentionally minimal & defensive.
    """
    try:
        tick = client.get_symbol_ticker(symbol=symbol)
        price = float(tick.get("price", 0.0))
        logger.log_info(f"{symbol} ✅ Current price: {price}")
        # Trigger partial TP check
        try:
            process_price_update(symbol, price)
        except Exception as e:
            logger.log_debug(f"{symbol} ⚠ process_price_update failed for ticker: {e}")
        return price
    except Exception as e:
        logger.log_error(f"{symbol} ❌ Failed to fetch price: {e}")
        return 0.0


# -------------------------
# Simple monitor helpers
# -------------------------

def monitor_symbol(symbol: str, direction_hint: Optional[str] = None, use_ticker: bool = False) -> None:
    """
    Lightweight monitor that either polls the ticker price or periodically refreshes recent klines.
    It calls process_price_update whenever a new price/candle is seen.

    Use: spawn monitor_symbol(...) in a background Thread for basic price-driven hooks.
    """
    last_price = None
    try:
        while True:
            try:
                if use_ticker:
                    price = get_ticker_price(symbol)
                else:
                    # fetch a small number of candles but ensure we get the latest close
                    df = get_recent_klines(symbol, "5m", 2)
                    if df.empty:
                        price = None
                    else:
                        price = float(df["close"].iloc[-1])

                if price is None:
                    time.sleep(POLL_INTERVAL)
                    continue

                if last_price is None or price != last_price:
                    logger.log_debug(f"{symbol} latest price update: {price} (prev {last_price})")
                    # If a direction hint provided, call only that direction (small optimization)
                    try:
                        if direction_hint and direction_hint.lower() in ("long", "short"):
                            position_manager.check_partial_tp(symbol, direction_hint.lower(), price)
                        else:
                            # call the general process helper (which checks both directions)
                            process_price_update(symbol, price)
                    except Exception as e:
                        logger.log_error(f"{symbol} ❌ Error running partial TP check: {e}")
                        logger.log_debug(traceback.format_exc())

                    last_price = price

                time.sleep(POLL_INTERVAL)
            except KeyboardInterrupt:
                logger.log_info(f"{symbol} monitor interrupted by user.")
                break
            except Exception as e:
                logger.log_error(f"{symbol} price monitoring error: {e}")
                logger.log_debug(traceback.format_exc())
                time.sleep(max(1.0, POLL_INTERVAL))
    except Exception:
        logger.log_debug(traceback.format_exc())
        raise


# -------------------------
# CLI / quick test helper
# -------------------------
if __name__ == "__main__":
    # Quick manual tester: replace symbol as needed
    test_symbol = "QNTUSDT"
    logger.log_info(f"Starting quick price monitor for {test_symbol} (Ctrl+C to stop)")
    monitor_symbol(test_symbol)
