# binance_utils.py
"""
Compatibility-focused Binance helpers wrapper.

Drop-in replacement that:
 - Exposes BinanceClient with get_latest_price, get_price, get_ticker_price aliases.
 - Provides module-level _default_client instance and top-level helper functions.
 - If an existing client object exists in project (e.g. utils.exchange.client), it will try to wrap/use it.
 - Defensive: errors are logged, functions return None on failure (so calling code can continue).
"""

import os
import time
from typing import Optional, Dict, Any, List

# Try to import python-binance Client but don't fail if it's not installed
try:
    from binance.client import Client as BinancePyClient
    from binance.enums import KLINE_INTERVAL_5MINUTE
except Exception:
    BinancePyClient = None
    KLINE_INTERVAL_5MINUTE = "5m"

# prefer your project's logger if available
try:
    from core.logger import global_logger as logger
except Exception:
    import logging as _logging
    logger = _logging.getLogger("binance_utils")
    if not logger.handlers:
        # ensure at least basic config so logs appear in stdout
        _logging.basicConfig(level=_logging.INFO)

# Try to reuse an existing top-level client if your project created one earlier
_existing_external_client = None
try:
    # many of your modules reference utils.exchange.client
    import utils.exchange as _ue
    _existing_external_client = getattr(_ue, "client", None)
    if _existing_external_client:
        logger.info("binance_utils: found existing client in utils.exchange.client — will prefer using it where possible.")
except Exception:
    _existing_external_client = None

# API credentials from env (optional)
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

class BinanceClient:
    """
    Wrapper around whatever API client is available.

    - If you pass test_client, that will be used as underlying client.
    - If no test_client and python-binance is installed and env creds exist, will instantiate BinancePyClient.
    - If an external client object was detected in utils.exchange, it will be used.
    """
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, test_client: Optional[Any] = None):
        self.api_key = api_key or API_KEY
        self.api_secret = api_secret or API_SECRET
        self.client = test_client

        # prefer externally created client if available
        global _existing_external_client
        if self.client is None and _existing_external_client is not None:
            try:
                self.client = _existing_external_client
                logger.info("BinanceClient: using project-existing external client object")
            except Exception:
                self.client = None

        # otherwise try to create python-binance Client if possible
        if self.client is None and BinancePyClient is not None and self.api_key and self.api_secret:
            try:
                self.client = BinancePyClient(api_key=self.api_key, api_secret=self.api_secret)
                logger.info("BinanceClient: instantiated python-binance Client using env credentials")
            except Exception as e:
                logger.warning(f"BinanceClient: failed to instantiate python-binance Client: {e}")
                self.client = None

        if self.client is None:
            logger.info("BinanceClient: no underlying API client available (read-only / dry-run mode)")

    # ---------- Market data ----------
    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Return latest ticker price (float) or None."""
        try:
            if not self.client:
                logger.debug(f"get_latest_price: no client for {symbol}")
                return None
            # many clients expose get_symbol_ticker; others use get_avg_price / ticker_price
            if hasattr(self.client, "get_symbol_ticker"):
                t = self.client.get_symbol_ticker(symbol=symbol)
                if isinstance(t, dict) and "price" in t:
                    return float(t["price"])
            if hasattr(self.client, "ticker_price"):
                t = self.client.ticker_price(symbol=symbol)
                if isinstance(t, dict) and "price" in t:
                    return float(t["price"])
                # some wrappers return string
                if isinstance(t, str):
                    return float(t)
            if hasattr(self.client, "get_avg_price"):
                t = self.client.get_avg_price(symbol=symbol)
                if isinstance(t, dict) and "price" in t:
                    return float(t["price"])
            # last fallback: try a '.get_price' on underlying client (some wrappers)
            if hasattr(self.client, "get_price"):
                try:
                    p = self.client.get_price(symbol)
                    return float(p) if p is not None else None
                except Exception:
                    pass
            return None
        except Exception as e:
            logger.error(f"{symbol} get_latest_price error: {e}")
            return None

    # aliases many codebases use
    def get_price(self, symbol: str) -> Optional[float]:
        return self.get_latest_price(symbol)

    def get_ticker_price(self, symbol: str) -> Optional[float]:
        return self.get_latest_price(symbol)

    def get_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if not self.client:
                return None
            if hasattr(self.client, "get_symbol_ticker"):
                return self.client.get_symbol_ticker(symbol=symbol)
            if hasattr(self.client, "ticker_price"):
                return self.client.ticker_price(symbol=symbol)
            return None
        except Exception as e:
            logger.error(f"{symbol} get_ticker error: {e}")
            return None

    # ---------- Symbol / precision helpers ----------
    def get_symbol_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if not self.client:
                return None
            if hasattr(self.client, "get_symbol_info"):
                return self.client.get_symbol_info(symbol)
            if hasattr(self.client, "get_exchange_info"):
                info = self.client.get_exchange_info()
                if isinstance(info, dict):
                    for s in info.get("symbols", []):
                        if s.get("symbol") == symbol:
                            return s
            return None
        except Exception as e:
            logger.error(f"{symbol} get_symbol_info error: {e}")
            return None

    def get_step_size(self, symbol: str) -> Optional[float]:
        info = self.get_symbol_info(symbol)
        if not info:
            return None
        for f in info.get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                v = f.get("stepSize") or f.get("minQty")
                try:
                    return float(v)
                except Exception:
                    return None
        return None

    def get_tick_size(self, symbol: str) -> Optional[float]:
        info = self.get_symbol_info(symbol)
        if not info:
            return None
        for f in info.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                v = f.get("tickSize")
                try:
                    return float(v)
                except Exception:
                    return None
        return None

    def get_min_notional(self, symbol: str) -> Optional[float]:
        info = self.get_symbol_info(symbol)
        if not info:
            return None
        for f in info.get("filters", []):
            if f.get("filterType") in ("MIN_NOTIONAL", "NOTIONAL"):
                v = f.get("notional") or f.get("minNotional") or f.get("notional")
                try:
                    return float(v)
                except Exception:
                    return None
        return None

    def safe_klines_fetch(self, symbol: str, interval: str = KLINE_INTERVAL_5MINUTE, limit: int = 100) -> Optional[List]:
        try:
            if not self.client:
                return None
            if hasattr(self.client, "get_klines"):
                return self.client.get_klines(symbol=symbol, interval=interval, limit=limit)
            if hasattr(self.client, "klines"):
                return self.client.klines(symbol=symbol, interval=interval, limit=limit)
            return None
        except Exception as e:
            logger.error(f"{symbol} safe_klines_fetch error: {e}")
            return None

    # ---------- Futures helpers ----------
    def get_futures_balance(self) -> Optional[Dict[str, float]]:
        try:
            if not self.client:
                return None
            # try common python-binance methods
            if hasattr(self.client, "futures_account_balance"):
                bal = self.client.futures_account_balance()
                result = {}
                for b in bal or []:
                    asset = b.get("asset")
                    if not asset:
                        continue
                    try:
                        result[asset] = float(b.get("balance") or b.get("walletBalance") or 0.0)
                    except Exception:
                        result[asset] = 0.0
                return result
            # alternate wrapper method names
            if hasattr(self.client, "futures_account"):
                resp = self.client.futures_account()
                if isinstance(resp, dict) and "assets" in resp:
                    result = {}
                    for b in resp.get("assets", []) or []:
                        asset = b.get("asset")
                        if not asset: continue
                        try:
                            result[asset] = float(b.get("balance") or 0.0)
                        except Exception:
                            result[asset] = 0.0
                    return result
            # last fallback: if a method called 'get_futures_balance' exists on underlying client, use it
            if hasattr(self.client, "get_futures_balance"):
                try:
                    r = self.client.get_futures_balance()
                    if isinstance(r, dict):
                        return {k: float(v) for k, v in r.items()}
                except Exception:
                    pass
            return None
        except Exception as e:
            logger.error(f"get_futures_balance error: {e}")
            return None

    def get_futures_position_information(self, symbol: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        try:
            if not self.client:
                return None
            if symbol and hasattr(self.client, "futures_position_information"):
                return self.client.futures_position_information(symbol=symbol)
            if hasattr(self.client, "futures_position_information"):
                return self.client.futures_position_information()
            # try alternate naming
            if hasattr(self.client, "get_positions"):
                return self.client.get_positions(symbol=symbol) if symbol else self.client.get_positions()
            return None
        except Exception as e:
            logger.error(f"{symbol or 'all'} get_futures_position_information error: {e}")
            return None

    def get_futures_account(self) -> Optional[Dict[str, Any]]:
        try:
            if not self.client:
                return None
            if hasattr(self.client, "futures_account"):
                return self.client.futures_account()
            if hasattr(self.client, "futures_account_balance"):
                # not the same shape, but return container
                return {"balance_list": self.client.futures_account_balance()}
            return None
        except Exception as e:
            logger.error(f"get_futures_account error: {e}")
            return None

    def ping(self) -> bool:
        try:
            if not self.client:
                return False
            if hasattr(self.client, "ping"):
                self.client.ping()
                return True
            return True
        except Exception:
            return False


# module-level client (singleton) for compatibility
_default_client = BinanceClient()

# Convenience module functions (keeps old code working)
def get_latest_price(symbol: str) -> Optional[float]:
    return _default_client.get_latest_price(symbol)

def get_price(symbol: str) -> Optional[float]:
    return _default_client.get_price(symbol)

def get_ticker_price(symbol: str) -> Optional[float]:
    return _default_client.get_ticker_price(symbol)

def get_symbol_info(symbol: str) -> Optional[Dict[str, Any]]:
    return _default_client.get_symbol_info(symbol)

def safe_klines_fetch(symbol: str, interval: str = KLINE_INTERVAL_5MINUTE, limit: int = 100) -> Optional[List]:
    return _default_client.safe_klines_fetch(symbol, interval=interval, limit=limit)

def get_step_size(symbol: str) -> Optional[float]:
    return _default_client.get_step_size(symbol)

def get_tick_size(symbol: str) -> Optional[float]:
    return _default_client.get_tick_size(symbol)

def get_min_notional(symbol: str) -> Optional[float]:
    return _default_client.get_min_notional(symbol)

def get_futures_balance() -> Optional[Dict[str, float]]:
    return _default_client.get_futures_balance()

def get_futures_total_balance(convert_asset: str = "USDT") -> Optional[float]:
    try:
        bal = get_futures_balance()
        if not bal:
            return None
        if convert_asset in bal:
            return float(bal[convert_asset])
        # sum commonly stable coins
        total = 0.0
        for a in ("USDT","USDC","BUSD"):
            total += float(bal.get(a, 0.0))
        if total > 0:
            return total
        return sum(float(v) for v in bal.values())
    except Exception:
        return None

def get_futures_position_information(symbol: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    return _default_client.get_futures_position_information(symbol)
