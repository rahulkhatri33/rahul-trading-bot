# core/symbol_precision.py

import json
import os
from decimal import Decimal, ROUND_DOWN, getcontext
from core.logger import global_logger as logger
from utils.notifier import Notifier

PRECISION_FILE = 'config/symbol_precision.json'
notifier = Notifier()

class SymbolPrecision:
    def __init__(self):
        try:
            with open(PRECISION_FILE, 'r') as f:
                self.data = json.load(f)
            logger.log_once("✅ Symbol precision file loaded.")
        except Exception as e:
            logger.log_error(f"❌ Failed to load precision file: {e}")
            notifier.send_critical(f"❌ CRITICAL: Could not load symbol precision file.\nError: {e}")
            self.data = {}

    def get_step_size(self, symbol):
        return self._get_value(symbol, "step_size")

    def get_tick_size(self, symbol):
        return self._get_value(symbol, "tick_size")

    def get_min_notional(self, symbol):
        return self._get_value(symbol, "min_notional")

    def get_trimmed_quantity(self, symbol, qty):
        step = self.get_step_size(symbol)
        if step is None:
            logger.log_error(f"{symbol} ⚠️ Step size missing — qty not trimmed.")
            return qty

        getcontext().prec = 18
        qty_dec = Decimal(str(qty))
        step_dec = Decimal(str(step))
        trimmed = (qty_dec // step_dec) * step_dec
        return float(trimmed.quantize(step_dec, rounding=ROUND_DOWN))

    def get_trimmed_price(self, symbol, price):
        tick = self.get_tick_size(symbol)
        if tick is None:
            logger.log_error(f"{symbol} ⚠️ Tick size missing — price not trimmed.")
            return price

        getcontext().prec = 18
        price_dec = Decimal(str(price))
        tick_dec = Decimal(str(tick))
        trimmed = (price_dec // tick_dec) * tick_dec
        return float(trimmed.quantize(tick_dec, rounding=ROUND_DOWN))

    def _decimal_places(self, num):
        s = f"{num:.20f}".rstrip('0')
        if '.' in s:
            return len(s.split('.')[-1])
        return 0

    def _get_value(self, symbol, key):
        if symbol not in self.data:
            logger.log_error(f"{symbol} ❌ Not found in symbol_precision.json")
            return None
        return self.data[symbol].get(key)

# === Singleton Instance ===
symbol_precision = SymbolPrecision()

# === Module-Level Helpers ===
def get_step_size(symbol):
    return symbol_precision.get_step_size(symbol)

def get_tick_size(symbol):
    return symbol_precision.get_tick_size(symbol)

def get_min_notional(symbol):
    return symbol_precision.get_min_notional(symbol)

def get_trimmed_quantity(symbol, qty):
    return symbol_precision.get_trimmed_quantity(symbol, qty)

def get_trimmed_price(symbol, price):
    return symbol_precision.get_trimmed_price(symbol, price)

def get_precise_price(symbol, price):
    """Alias for get_trimmed_price (for compatibility with sl_tp_engine)."""
    return symbol_precision.get_trimmed_price(symbol, price)
