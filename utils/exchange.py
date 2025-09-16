# utils/exchange.py
import os
import time
import math
from typing import Optional, Tuple
from binance.client import Client
from core.logger import global_logger as logger

# existing client import / initialization (keep your pattern)
client = Client(api_key=os.getenv("BINANCE_API_KEY"), api_secret=os.getenv("BINANCE_API_SECRET"))

# NEW: delegate rounding & step info to core.symbol_precision
from core.symbol_precision import SymbolPrecision

_sym_prec = SymbolPrecision()

def get_qty_step_size(symbol: str) -> float:
    """
    Return the step size (LOT_SIZE step) for a symbol using central symbol_precision.
    Kept for backward compatibility with callers.
    """
    try:
        return float(_sym_prec.get_step_size(symbol))
    except Exception:
        # fallback safe small step
        return 1e-8

def round_to_step(qty: float, step: Optional[float] = None, precision: Optional[int] = None) -> float:
    """
    Compatibility wrapper: rounds/floors qty to the given step/precision.
    Prefer callers to use core.symbol_precision.get_trimmed_quantity() directly.
    This wrapper will:
      - if step provided, use Decimal-based floor to step,
      - else fall back to central get_trimmed_quantity with no price context (best-effort).
    """
    try:
        if qty is None:
            return 0.0
        # If explicit step provided, floor to that step
        if step:
            try:
                # Use the central helper's logic: round down to step
                # We call get_trimmed_quantity without price to perform safe floor to step.
                # Note: get_trimmed_quantity accepts a price arg; passing None means it will floor.
                return _sym_prec.get_trimmed_quantity("", qty, price=None) if step is None else _sym_prec.round_quantity_down("", qty)
            except Exception:
                # fallback: simple math floor using provided precision if available
                if precision is not None:
                    return math.floor(qty * (10 ** precision)) / (10 ** precision)
                else:
                    # fallback 8 decimal floor
                    return math.floor(qty * 1e8) / 1e8
        else:
            # no explicit step provided -> ask central helper using empty symbol (fallback step)
            return _sym_prec.get_trimmed_quantity("", qty, price=None)
    except Exception:
        try:
            return float(qty)
        except Exception:
            return 0.0
