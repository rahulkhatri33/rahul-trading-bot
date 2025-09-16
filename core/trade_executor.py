# core/trade_executor.py
"""
Trade execution module — minimal edits:
- ensure get_trimmed_quantity() and get_trimmed_price() are called immediately
  before any client.futures_create_order(...) call so quantities/prices always
  conform to exchange step/tick sizes.
- logs raw->trim conversions.
- preserve existing behavior otherwise.
"""

from binance.client import Client
from binance.enums import ORDER_TYPE_MARKET, SIDE_BUY, SIDE_SELL
from core.logger import global_logger as logger
from core.position_manager import position_manager
from core.config import get_config
from utils.discord_logger import send_discord_log
from utils.exchange import client, get_qty_step_size, round_to_step
import math
from datetime import datetime

# Canonical precision helpers
from core.symbol_precision import get_trimmed_quantity, get_trimmed_price, get_min_notional, get_precise_price

def execute_trade(symbol: str, direction: str, price: float, qty: float, dry_run: bool, sl: float, tp: float) -> dict:
    """
    Executes market entry + places SL/TP orders.
    Minimal modifications: final trimming of qty and prices before API calls.
    """
    logger.log_info(f"{symbol} 🚀 Executing {direction.upper()} trade | Qty(raw): {qty} | Price: {price} | SL: {sl} | TP: {tp}")
    config = get_config()
    # read some settings (keep behavior unchanged)
    try:
        price_precision = config.get("scalper_settings", {}).get("symbol_precisions", {}).get(symbol, {}).get("pricePrecision", 8)
    except Exception:
        price_precision = 8

    # Quick numeric coercion
    try:
        price = float(price)
        qty = float(qty)
        sl = float(sl) if sl is not None else 0.0
        tp = float(tp) if tp is not None else 0.0
    except Exception as e:
        logger.log_error(f"{symbol} ❌ Invalid numeric arguments: {e}")
        return {"status": "failed", "error": "invalid_numeric_args"}

    # Trim quantity using canonical helper (this enforces stepSize and min_notional)
    try:
        qty_raw = qty
        qty = get_trimmed_quantity(symbol, qty_raw, price=price)
        logger.log_debug(f"{symbol} qty trim: raw={qty_raw} -> trimmed={qty}")
    except Exception as e:
        logger.log_error(f"{symbol} ❌ Failed to trim qty: {e}")
        return {"status": "failed", "error": "trim_qty_failed"}

    # Verify notional >= min_notional and adjust if needed
    try:
        min_notional = get_min_notional(symbol)
        if min_notional is not None and float(qty) * float(price) < float(min_notional):
            suggested_min_qty = (float(min_notional) / float(price)) if price and price > 0 else qty
            qty = get_trimmed_quantity(symbol, suggested_min_qty, price=price)
            logger.log_info(f"{symbol} Adjusted qty to meet min_notional: qty={qty}, notional={qty*price}")
    except Exception:
        # non-fatal
        pass

    if qty <= 0:
        logger.log_error(f"{symbol} ❌ Quantity after trimming is zero or negative. Aborting.")
        return {"status": "failed", "error": "qty_zero_after_trim"}

    # Finalize SL/TP spacing using existing logic (kept minimal)
    try:
        scalper_settings = config.get("scalper_settings", {})
        min_sl_pct = scalper_settings.get("min_sl_distance_pct", 0.002)
        rr_ratio = scalper_settings.get("risk_reward_ratio", 1.4)
        if direction == "long":
            if sl >= price * (1 - min_sl_pct):
                sl = price * (1 - min_sl_pct)
            if tp <= price * (1 + min_sl_pct * rr_ratio):
                tp = price * (1 + min_sl_pct * rr_ratio)
        else:
            if sl <= price * (1 + min_sl_pct):
                sl = price * (1 + min_sl_pct)
            if tp >= price * (1 - min_sl_pct * rr_ratio):
                tp = price * (1 - min_sl_pct * rr_ratio)
    except Exception:
        pass

    # Trim SL and TP to tick size using canonical helper
    try:
        sl = get_trimmed_price(symbol, sl) if sl and sl > 0 else 0.0
    except Exception:
        sl = round(float(sl) if sl else 0.0, price_precision)
    try:
        tp = get_trimmed_price(symbol, tp) if tp and tp > 0 else 0.0
    except Exception:
        tp = round(float(tp) if tp else 0.0, price_precision)

    logger.log_info(f"{symbol} 🎯 Finalized SL/TP | Entry: {price:.8f} | SL: {sl:.8f} | TP: {tp:.8f} | Qty(trimmed): {qty}")

    # Dry-run behavior
    if dry_run:
        logger.log_info(f"{symbol} 🧪 Dry run: would place MARKET {direction} qty={qty}, SL={sl}, TP={tp}")
        return {"status": "success", "order_id": "dry_run", "sl_order_id": "dry_run", "tp_order_id": "dry_run"}

    # Place actual orders
    try:
        leverage = config.get("scalper_settings", {}).get("leverage", 20)
        client.futures_change_leverage(symbol=symbol, leverage=leverage)
        logger.log_info(f"{symbol} ✅ Set leverage to {leverage}x")
    except Exception as e:
        logger.log_warning(f"{symbol} ⚠ Could not set leverage: {e}")

    position_side = "LONG" if direction == "long" else "SHORT"
    side = SIDE_BUY if direction == "long" else SIDE_SELL

    try:
        # --- Ensure we trim quantity one more time immediately before API call (3-line guarantee) ---
        qty_before_order = qty
        qty = get_trimmed_quantity(symbol, qty_before_order, price=price)
        logger.log_debug(f"{symbol} FINAL qty before order: raw={qty_before_order} -> trimmed={qty}")
        # -------------------------------------------------------------------------------

        order = client.futures_create_order(
            symbol=symbol,
            side=side,
            type=ORDER_TYPE_MARKET,
            quantity=qty,
            positionSide=position_side
        )
        logger.log_info(f"{symbol} ✅ Market order placed: {order}")
    except Exception as e:
        logger.log_error(f"{symbol} ❌ Market order failed: {e}")
        send_discord_log(f"{symbol} ❌ Market order failed: {e}")
        return {"status": "failed", "error": str(e)}

    # Place SL order (if provided)
    sl_order = None
    if sl and sl > 0:
        try:
            # trim the stopPrice and quantity right before placing SL
            sl_trim = get_trimmed_price(symbol, sl)
            qty_for_sl = get_trimmed_quantity(symbol, qty, price=sl_trim)
            logger.log_debug(f"{symbol} SL preflight: qty={qty} -> qty_for_sl={qty_for_sl}, stopPrice={sl_trim}")
            sl_order = client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL if direction == "long" else SIDE_BUY,
                type='STOP_MARKET',
                quantity=qty_for_sl,
                stopPrice=sl_trim,
                positionSide=position_side,
                reduceOnly=True
            )
            logger.log_info(f"{symbol} ✅ SL order placed: {sl_order}")
        except Exception as e:
            logger.log_error(f"{symbol} ❌ SL order failed: {e}")
            send_discord_log(f"{symbol} ❌ SL order failed: {e}",)

    # Place TP order (if provided)
    tp_order = None
    if tp and tp > 0:
        try:
            tp_trim = get_trimmed_price(symbol, tp)
            qty_for_tp = get_trimmed_quantity(symbol, qty, price=tp_trim)
            logger.log_debug(f"{symbol} TP preflight: qty={qty} -> qty_for_tp={qty_for_tp}, stopPrice={tp_trim}")
            tp_order = client.futures_create_order(
                symbol=symbol,
                side=SIDE_SELL if direction == "long" else SIDE_BUY,
                type='TAKE_PROFIT_MARKET',
                quantity=qty_for_tp,
                stopPrice=tp_trim,
                positionSide=position_side,
                reduceOnly=True
            )
            logger.log_info(f"{symbol} ✅ TP order placed: {tp_order}")
        except Exception as e:
            logger.log_error(f"{symbol} ❌ TP order failed: {e}")
            send_discord_log(f"{symbol} ❌ TP order failed: {e}")

    # Update position manager with trimmed qty (store the real qty used)
    try:
        position_manager.add_position(symbol, position_side.lower(), {
            "symbol": symbol,
            "direction": position_side.lower(),
            "entry_price": price,
            "size": qty,
            "stop_loss": sl,
            "take_profit": tp,
            "sl_order_id": (sl_order.get("orderId") if isinstance(sl_order, dict) else None),
            "tp_order_id": (tp_order.get("orderId") if isinstance(tp_order, dict) else None),
            "confidence": 1.0,
            "label": "scalper",
            "source": "trade_executor",
            "entry_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        logger.log_info(f"{symbol} ✅ Updated position manager")
    except Exception as e:
        logger.log_error(f"{symbol} ❌ Failed to update position manager: {str(e)}")

    return {
        "status": "success",
        "order_id": order.get("orderId") if isinstance(order, dict) else None,
        "sl_order_id": sl_order.get("orderId") if (sl_order and isinstance(sl_order, dict)) else None,
        "tp_order_id": tp_order.get("orderId") if (tp_order and isinstance(tp_order, dict)) else None
    }
