# core/symbol_precision.py
"""
Symbol precision helpers.

Provides:
 - loading symbol precision metadata from config/symbol_precision.json (if present)
 - get_tick_size(symbol)
 - get_min_notional(symbol)
 - get_trimmed_quantity(symbol, qty, price=None)
 - get_trimmed_price(symbol, price)

Behavior notes:
 - Quantities are rounded DOWN to the allowed step (binance requires this).
 - If rounding down would produce 0 (because the requested qty is smaller than a step),
   this module will return the minimal allowed quantity (derived from min_notional/price)
   rounded **up** to the step where necessary — this avoids trimmed-to-zero problems.
 - All rounding uses Decimal for numeric safety.
"""

import json
import os
import math
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, getcontext
from typing import Optional

# keep logs consistent with your logger
try:
    from core.logger import global_logger as logger
except Exception:
    class _FakeLogger:
        def log_debug(self, *a, **k): print("DEBUG:", *a)
        def log_info(self, *a, **k): print("INFO:", *a)
        def log_warning(self, *a, **k): print("WARN:", *a)
        def log_error(self, *a, **k): print("ERR:", *a)
    logger = _FakeLogger()

# default precision file path (optional)
PRECISION_FILE = os.path.join("config", "symbol_precision.json")

# default safe fallbacks
DEFAULT_TICK_SIZE = 1e-8
DEFAULT_STEP_SIZE = 1e-8
DEFAULT_MIN_NOTIONAL = 1e-6

# Decimal context
getcontext().prec = 28


class SymbolPrecision:
    def __init__(self, precision_file: Optional[str] = None):
        self.precision_file = precision_file or PRECISION_FILE
        self.data = {}
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.precision_file):
                with open(self.precision_file, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
                    logger.log_info(f"Loaded symbol precision from {self.precision_file}")
            else:
                logger.log_warning(f"Symbol precision file not found: {self.precision_file}. Using defaults.")
                self.data = {}
        except Exception as e:
            logger.log_error(f"Failed to load symbol precision file {self.precision_file}: {e}")
            self.data = {}

    def get_symbol_entry(self, symbol: str) -> dict:
        # direct lookup; support both symbol and uppercase symbol
        if symbol in self.data:
            return self.data.get(symbol, {})
        uc = symbol.upper()
        return self.data.get(uc, {})

    def get_step_size(self, symbol: str) -> float:
        """Return stepSize (quantity precision) for symbol or fallback."""
        s = self.get_symbol_entry(symbol)
        try:
            # Accept many possible key names (both camelCase and snake_case)
            for k in ("stepSize", "lotSize", "quantity_step", "qty_step", "step_size", "step"):
                if k in s:
                    return float(s[k])
            # sometimes precision is encoded as integer precision
            for k in ("quantityPrecision", "qtyPrecision", "quantity_precision"):
                if k in s:
                    prec = int(s[k])
                    return float(10 ** -prec)
        except Exception:
            pass
        return float(DEFAULT_STEP_SIZE)

    def get_tick_size(self, symbol: str) -> float:
        """Return tick size for price increments."""
        s = self.get_symbol_entry(symbol)
        try:
            for k in ("tickSize", "priceTick", "pricePrecision", "tick_size", "price_tick"):
                if k in s:
                    return float(s[k])
            # pricePrecision as int
            for k in ("pricePrecision", "price_precision"):
                if k in s:
                    prec = int(s[k])
                    return float(10 ** -prec)
        except Exception:
            pass
        return float(DEFAULT_TICK_SIZE)

    def get_min_notional(self, symbol: str) -> float:
        """Return minNotional for the symbol or default fallback."""
        s = self.get_symbol_entry(symbol)
        try:
            for k in ("minNotional", "min_notional", "minNot", "min_not"):
                if k in s:
                    return float(s[k])
        except Exception:
            pass
        return float(DEFAULT_MIN_NOTIONAL)

    def round_price(self, symbol: str, price: float) -> float:
        """Round price to the tick size (ROUND_DOWN)."""
        try:
            tick = Decimal(str(self.get_tick_size(symbol)))
            p = Decimal(str(price))
            if tick == 0:
                return float(p)
            # rounding down to multiple of tick
            quant = (p // tick) * tick
            # quantize to tick decimal places
            places = max(0, -tick.as_tuple().exponent)
            quant = quant.quantize(Decimal(10) ** -places)
            return float(quant)
        except Exception:
            try:
                return float(round(price, 8))
            except Exception:
                return float(price)

    def round_quantity_down(self, symbol: str, qty: float) -> float:
        """Round quantity down to the allowed step (stepSize) using FLOOR semantics."""
        try:
            step = Decimal(str(self.get_step_size(symbol)))
            q = Decimal(str(qty))
            if step == 0:
                return float(q)
            # number of increments (floor division)
            increments = (q // step)
            rounded = increments * step
            # ensure not negative
            if rounded < 0:
                rounded = Decimal("0")
            # quantize to step decimal places
            places = max(0, -step.as_tuple().exponent)
            rounded = rounded.quantize(Decimal(10) ** -places)
            return float(rounded)
        except Exception:
            try:
                # fallback to simple floor at 8 decimals
                return float(math.floor(qty * 1e8) / 1e8)
            except Exception:
                return float(max(0.0, qty))

    def get_min_qty_by_min_notional(self, symbol: str, price: Optional[float]) -> float:
        """
        Compute the minimal quantity that satisfies the min_notional constraint.
        If price is None or invalid, fallback to 1 * step size (safe).
        """
        try:
            min_notional = Decimal(str(self.get_min_notional(symbol)))
            if price is None or price <= 0:
                # fallback to a single step
                step = Decimal(str(self.get_step_size(symbol)))
                return float(step)
            price_d = Decimal(str(price))
            # minimal qty required (rounded UP to be safe)
            # use ROUND_HALF_UP to avoid tiny truncation issues
            min_qty = (min_notional / price_d).quantize(Decimal("1e-18"), rounding=ROUND_HALF_UP)
            # ensure at least one step
            step = Decimal(str(self.get_step_size(symbol)))
            if min_qty < step:
                min_qty = step
            return float(min_qty)
        except Exception:
            try:
                return float(self.get_step_size(symbol))
            except Exception:
                return float(DEFAULT_STEP_SIZE)

    def get_trimmed_quantity(self, symbol: str, qty: float, price: Optional[float] = None) -> float:
        """
        Trim qty to the allowed step size. If trimming yields zero (too small),
        compute and return the minimum allowed quantity using min_notional (if price provided)
        or at least one step size.

        Args:
            symbol: trading symbol string
            qty: requested quantity (float)
            price: optional current price (used to compute min qty from min_notional)

        Returns:
            float: quantity that is legal on exchange (rounded down to step size or adjusted to minimum)
        """
        try:
            if qty is None:
                return 0.0
            qty_f = float(qty)
            if qty_f <= 0:
                return 0.0

            # Step-round down
            trimmed = self.round_quantity_down(symbol, qty_f)

            if trimmed >= 1e-12:
                return float(trimmed)

            # trimmed is zero => requested qty is below one step
            # compute minimal allowed qty using min_notional and price (if provided)
            min_qty = self.get_min_qty_by_min_notional(symbol, price)
            # round min_qty up to next multiple of step when needed
            step = float(self.get_step_size(symbol))
            if step <= 0:
                return float(min_qty)

            if min_qty < step:
                candidate = step
            else:
                multiples = math.ceil(min_qty / step)
                candidate = multiples * step

            # final safety: ensure candidate is quantized to the step
            final = self.round_quantity_down(symbol, candidate)
            if final <= 0:
                final = step

            logger.log_warning(f"{symbol} trimmed quantity was zero for requested {qty}; returning minimum allowed {final} (price={price})")
            return float(final)
        except Exception as e:
            logger.log_error(f"{symbol} get_trimmed_quantity error: {e}")
            # fallback: return qty floored to 8 decimal places not to crash caller
            try:
                return float(math.floor(float(qty) * 1e8) / 1e8)
            except Exception:
                return float(max(0.0, qty))

    # convenience wrapper (optional)
    def get_trimmed_price(self, symbol: str, price: float) -> float:
        try:
            return self.round_price(symbol, price)
        except Exception:
            try:
                return float(round(price, 8))
            except Exception:
                return float(price)


# Module-level convenience instance and helpers for backward compatibility
symbol_precision = SymbolPrecision()

def get_step_size(symbol: str) -> float:
    return symbol_precision.get_step_size(symbol)

def get_tick_size(symbol: str) -> float:
    return symbol_precision.get_tick_size(symbol)

def get_min_notional(symbol: str) -> float:
    return symbol_precision.get_min_notional(symbol)

def get_trimmed_quantity(symbol: str, qty: float, price: Optional[float] = None) -> float:
    return symbol_precision.get_trimmed_quantity(symbol, qty, price)

def get_trimmed_price(symbol: str, price: float) -> float:
    return symbol_precision.get_trimmed_price(symbol, price)

def get_precise_price(symbol: str, price: float) -> float:
    """Alias for get_trimmed_price (compatibility)."""
    return symbol_precision.get_trimmed_price(symbol, price)
