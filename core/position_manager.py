# core/position_manager.py
"""
Position manager module — minimal safe patch to fix partial TP / SL exit issues.

Key changes:
 - Always trim executed quantities with get_trimmed_quantity when updating local sizes.
 - Poll order status after creating partial/SL orders to confirm executedQty before updating local state.
 - If remainder after partial is below one step (or becomes zero after trimming), treat as fully closed and remove local position.
 - Add better Discord alerts in important branches (failed fills, full-close, simulated dry-run).
 - No deletion of existing public API functions; function names/signatures preserved.
"""

import json
import os
import time
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

from binance.exceptions import BinanceAPIException

# keep existing imports used in your repo
from utils.exchange import client
from utils.discord_logger import send_discord_log
from core.logger import global_logger as logger
from core.config import get_config

# symbol precision helpers (canonical)
from core.symbol_precision import get_trimmed_quantity, get_trimmed_price

POSITIONS_FILE_DEFAULT = "open_positions.json"
BINANCE_MISSING_GRACE_SECONDS = 30  # seconds

# Polling configuration for order confirmation (tweak to taste)
_ORDER_POLL_INTERVAL = 0.5   # seconds between polls
_ORDER_POLL_TIMEOUT = 8.0    # seconds total wait for fills before giving up
_MIN_EXECUTED_TO_ACCEPT = 1e-8  # numerical tolerance to treat executedQty > 0


def _to_float_safe(v):
    """Try to coerce a value to float. Accept numeric string, numpy/pandas scalar, single-element list/tuple.
    Return None when not parseable.
    """
    try:
        if isinstance(v, (list, tuple)) and len(v) > 0:
            v = v[0]
        # bools convert to 1/0 but that's acceptable as a numeric fallback in some contexts.
        return float(v)
    except Exception:
        return None


def _sum_fills_qty(fills):
    """Return the sum of qty in a fills array (string or numeric qtys)."""
    try:
        if not fills:
            return 0.0
        s = 0.0
        for f in fills:
            if isinstance(f, dict):
                q = f.get("qty") or f.get("quantity") or f.get("executedQty")
            else:
                q = None
            try:
                s += float(q or 0.0)
            except Exception:
                pass
        return s
    except Exception:
        return 0.0


class PositionManager:
    def __init__(self, positions_file: str = POSITIONS_FILE_DEFAULT):
        self.positions_file = positions_file
        self.positions: Dict[str, Any] = self.load_positions()

    def load_positions(self) -> Dict[str, Any]:
        """Load and coerce numeric fields where possible."""
        try:
            if os.path.exists(self.positions_file):
                with open(self.positions_file, "r") as f:
                    data = json.load(f)

                # Coerce numeric types for stability
                for key, pos in list(data.items()):
                    if isinstance(pos, dict):
                        for num_key in [
                            "entry_price",
                            "stop_loss",
                            "take_profit",
                            "peak_price",
                            "size",
                            "qty",
                            "confidence",
                            "trailing_sl",
                            "partial_tp_price",
                            "partial_tp_size",
                        ]:
                            if num_key in pos:
                                try:
                                    coerced = _to_float_safe(pos[num_key])
                                    if coerced is not None:
                                        pos[num_key] = coerced
                                except Exception:
                                    pass
                return data
            return {}
        except Exception as e:
            logger.log_error(f"Error loading positions: {e}")
            logger.log_debug(traceback.format_exc())
            return {}

    def save_positions(self) -> None:
        try:
            with open(self.positions_file, "w") as f:
                json.dump(self.positions, f, indent=4)
        except Exception as e:
            logger.log_error(f"Error saving positions: {e}")
            logger.log_debug(traceback.format_exc())

    def is_position_sane(self, pos: Dict[str, Any]) -> bool:
        """
        Sanity check: ensure entry_price, stop_loss, take_profit are numeric and ordered properly.
        Uses configured minimum SL distance (min_sl_distance_pct) rather than strict inequality so
        very small differences due to rounding don't cause positions to be rejected.

        For long: stop_loss < entry_price < take_profit
        For short: stop_loss > entry_price > take_profit
        Also require entry_price > 0 and size > 0.

        This version accepts stop_loss == entry_price when an explicit breakeven flag or
        tp1/awaiting_trail flags indicate the SL was intentionally moved to breakeven.
        """
        try:
            if not isinstance(pos, dict):
                return False

            config = get_config()
            scalper_settings = config.get("scalper_settings", {}) if isinstance(config, dict) else {}
            min_sl_pct = scalper_settings.get("min_sl_distance_pct", 0.0005)

            direction = pos.get("direction")
            entry = _to_float_safe(pos.get("entry_price"))
            sl = _to_float_safe(pos.get("stop_loss"))
            tp = _to_float_safe(pos.get("take_profit"))
            size = _to_float_safe(pos.get("size") or pos.get("qty"))

            if entry is None or size is None:
                return False
            if entry <= 0 or (size is None or size <= 0):
                return False

            # compute minimum absolute SL distance
            try:
                min_sl_abs = abs(entry) * float(min_sl_pct)
            except Exception:
                min_sl_abs = abs(entry) * 0.0005

            # determine if breakeven is explicitly allowed for this position
            try:
                tp1_triggered = bool(pos.get('tp1_triggered', False))
                awaiting_trail = bool(pos.get('awaiting_trail_activation', False))
                breakeven_flag = bool(pos.get('breakeven', False))
            except Exception:
                tp1_triggered = False
                awaiting_trail = False
                breakeven_flag = False

            allow_breakeven = tp1_triggered or awaiting_trail or breakeven_flag

            # tolerance for floating comparisons
            EPS = max(abs(entry) * 1e-8, 1e-12)

            if direction == "long":
                # normal valid case: sl < entry < tp (if tp provided)
                if sl is not None and tp is not None:
                    if (sl + EPS < entry) and (entry + EPS < tp):
                        # ensure minimum SL distance
                        if (entry - sl) < min_sl_abs:
                            return False
                        return True

                # allow breakeven when flagged (sl approximately equals entry)
                if allow_breakeven and sl is not None:
                    if abs(sl - entry) <= max(EPS, abs(entry) * 1e-8):
                        if tp is None or (entry + EPS < tp):
                            return True

                # otherwise invalid
                logger.log_debug(f"is_position_sane failed for long: entry={entry}, sl={sl}, tp={tp}, allow_breakeven={allow_breakeven}")
                return False
            else:
                # short side checks
                if sl is not None and tp is not None:
                    if (sl - EPS > entry) and (entry - EPS > tp):
                        if (sl - entry) < min_sl_abs:
                            return False
                        return True

                if allow_breakeven and sl is not None:
                    if abs(sl - entry) <= max(EPS, abs(entry) * 1e-8):
                        if tp is None or (entry - EPS > tp):
                            return True

                logger.log_debug(f"is_position_sane failed for short: entry={entry}, sl={sl}, tp={tp}, allow_breakeven={allow_breakeven}")
                return False

        except Exception as e:
            logger.log_error(f"is_position_sane error: {e}")
            logger.log_debug(traceback.format_exc())
            return False

    def add_position(self, symbol: str, direction: str, position_data: Dict[str, Any]) -> None:
        key = f"{symbol}_{direction}"
        try:
            # harmonize naming: prefer 'size' but allow 'qty' input
            if "size" not in position_data and "qty" in position_data:
                try:
                    position_data["size"] = float(position_data["qty"])
                except Exception:
                    position_data["size"] = _to_float_safe(position_data.get("qty"))

            entry = position_data.get("entry_price")
            size = position_data.get("size", 0.0)

            # Validate basic numeric viability
            if entry is None or entry <= 0 or (size is None or size <= 0):
                # don't persist invalid live position — create an incomplete marker
                marker_key = f"{key}_synced_incomplete"
                self.positions[marker_key] = {
                    "symbol": symbol,
                    "direction": direction,
                    "raw": position_data,
                    "created_at": datetime.utcnow().isoformat(),
                    "note": "entry_price or size invalid; manual reconciliation required",
                }
                self.save_positions()
                logger.log_warning(f"{marker_key} created (invalid entry/size). raw entry={position_data.get('entry_price')!r}, size={position_data.get('size')!r}")
                return

            # Enforce minimum SL distance (auto-correct if too close)
            try:
                cfg = get_config()
                scalper_settings = cfg.get("scalper_settings", {}) if isinstance(cfg, dict) else {}
                min_sl_pct = float(scalper_settings.get("min_sl_distance_pct", 0.0005))
                fallback_sl_pct = float(scalper_settings.get("fallback_sl_pct", 0.03))
            except Exception:
                min_sl_pct = 0.0005
                fallback_sl_pct = 0.03

            sl = position_data.get("stop_loss")
            if sl is not None:
                try:
                    min_sl_abs = abs(entry) * float(min_sl_pct)
                    fallback_abs = abs(entry) * float(fallback_sl_pct)
                    # ensure fallback_abs is at least min_sl_abs
                    desired_abs = max(min_sl_abs, fallback_abs)
                    if direction == "long":
                        # require entry - sl >= desired_abs
                        if (entry - sl) < desired_abs:
                            new_sl = entry - desired_abs
                            position_data["stop_loss"] = float(max(new_sl, 0.00000001))
                            logger.log_warning(f"{key} stop_loss too close to entry. Auto-adjusted stop_loss -> {position_data['stop_loss']} (was {sl}).")
                    else:  # short
                        if (sl - entry) < desired_abs:
                            new_sl = entry + desired_abs
                            position_data["stop_loss"] = float(new_sl)
                            logger.log_warning(f"{key} stop_loss too close to entry. Auto-adjusted stop_loss -> {position_data['stop_loss']} (was {sl}).")
                except Exception:
                    pass

            # Persist valid position
            self.positions[key] = position_data
            self.save_positions()
            logger.log_info(f"Added position: {key}")
        except Exception as e:
            logger.log_error(f"add_position failed for {key}: {e}")
            logger.log_debug(traceback.format_exc())

    def update_position(self, symbol: str, direction: str, updates: Dict[str, Any]) -> None:
        key = f"{symbol}_{direction}"
        try:
            coerced_updates = {}
            for k, v in (updates or {}).items():
                coerced_updates[k] = v
            if key in self.positions:
                # update existing
                # harmonize qty->size if provided
                if "qty" in coerced_updates and "size" not in coerced_updates:
                    coerced_updates["size"] = coerced_updates.get("qty")
                self.positions[key].update(coerced_updates)
                self.save_positions()
                logger.log_info(f"Updated position {key}: {coerced_updates}")
                return
            else:
                # creating new via update - validate entry & size
                entry = coerced_updates.get("entry_price")
                size = coerced_updates.get("size", coerced_updates.get("qty", 0.0))
                if entry is None or entry <= 0 or (size is None or size <= 0):
                    marker_key = f"{key}_synced_incomplete"
                    self.positions[marker_key] = {
                        "symbol": symbol,
                        "direction": direction,
                        "raw": coerced_updates,
                        "created_at": datetime.utcnow().isoformat(),
                        "note": "update attempted to create position but entry/size invalid",
                    }
                    self.save_positions()
                    logger.log_warning(f"{marker_key} created (invalid entry/size via update). {coerced_updates!r}")
                    return
                # safe to create
                self.positions[key] = coerced_updates
                self.save_positions()
                logger.log_info(f"Created position {key} via update_position")
        except Exception as e:
            logger.log_error(f"Failed to update/create position {key}: {e}")
            logger.log_debug(traceback.format_exc())

    def get_position(self, symbol: str, direction: str) -> Optional[Dict[str, Any]]:
        key = f"{symbol}_{direction}"
        return self.positions.get(key)

    def get_all_positions(self) -> Dict[str, Any]:
        return self.positions

    def set_peak_price(self, symbol: str, direction: str, price: float) -> None:
        key = f"{symbol}_{direction}"
        pos = self.positions.get(key)
        if pos:
            val = _to_float_safe(price)
            if val is not None:
                pos["peak_price"] = val
                self.save_positions()

    def remove_position(self, key: str) -> None:
        if key in self.positions:
            self.positions.pop(key)
            self.save_positions()
            logger.log_info(f"Removed position: {key}")

    def close_position(self, symbol: str, direction: str) -> bool:
        """
        Close local position record and attempt to cancel associated orders.
        Emits a debug caller stack so you can identify who requested the close.
        """
        key = f"{symbol}_{direction}"
        try:
            caller_stack = "".join(traceback.format_list(traceback.extract_stack()[-6:-1]))
            logger.log_warning(f"[DEBUG_CLOSE] close_position called for {key} — caller stack:\n{caller_stack}")
        except Exception:
            logger.log_warning(f"[DEBUG_CLOSE] close_position called for {key} — (failed to get stack)")

        position = self.positions.get(key)
        if not position:
            logger.log_warning(f"No position found for {key}")
            return False

        config = get_config()
        live_mode = config.get("live_mode", False)
        if live_mode:
            try:
                for order_id_key in ["sl_order_id", "tp_order_id"]:
                    order_id = position.get(order_id_key)
                    if order_id:
                        try:
                            client.futures_cancel_order(symbol=symbol, orderId=order_id)
                            logger.log_info(f"Cancelled {order_id_key} {order_id} for {key}")
                        except BinanceAPIException as e:
                            if getattr(e, "code", None) == -2011:
                                logger.log_info(f"{order_id_key} {order_id} for {key} already cancelled/filled.")
                            else:
                                logger.log_error(f"Failed to cancel {order_id_key} for {key}: {e}")
                        except Exception as e:
                            logger.log_error(f"Unexpected error cancelling {order_id_key} for {key}: {e}")
            except Exception as e:
                logger.log_error(f"Unexpected error while cancelling orders for {key}: {e}")
                logger.log_debug(traceback.format_exc())

        # finally remove local position
        try:
            self.positions.pop(key, None)
            self.save_positions()
            logger.log_info(f"Closed position: {key}")
            return True
        except Exception as e:
            logger.log_error(f"Failed to remove local position {key}: {e}")
            logger.log_debug(traceback.format_exc())
            return False

    def check_partial_tp(self, symbol: str, direction: str, price: float) -> None:
        """
        Check and execute partial TP; updates local size using executed qty from exchange.
        Only execute if partial_tp_price is numeric and lies between entry and TP.
        This function uses targeted checks (not full is_position_sane) so partials
        can still run when full-sanity would reject (e.g., entry_price_estimated).

        Important behavior (minimal changes):
        - place market reduceOnly order for partial TP and poll order status until executedQty observed or timeout.
        - update local position using executed_qty trimmed with get_trimmed_quantity.
        - if remainder after trimming is effectively zero, treat as fully closed (call close_position).
        - send Discord alerts on success/failure/timeouts.
        """
        # Lazy import to avoid circular import with analytics/trade_recorder
        try:
            from core.analytics.trade_recorder import append_lifecycle, snapshot_equity  # may not exist; fine if import fails
        except Exception:
            append_lifecycle = None
            snapshot_equity = None

        try:
            key = f"{symbol}_{direction}"
            position = self.positions.get(key)
            if not position:
                return

            # targeted numeric coercion checks
            ptp_price = _to_float_safe(position.get("partial_tp_price"))
            ptp_size = _to_float_safe(position.get("partial_tp_size"))
            entry = _to_float_safe(position.get("entry_price"))
            tp = _to_float_safe(position.get("take_profit"))
            size = _to_float_safe(position.get("size"))

            if ptp_price is None or ptp_size is None or entry is None or tp is None or size is None:
                logger.log_debug(f"{key} skipped partial TP: insufficient numeric data (ptp={ptp_price}, ptp_size={ptp_size}, entry={entry}, tp={tp}, size={size})")
                return

            # ensure partial price lies between entry and final TP (basic sanity)
            if direction == "long":
                if not (entry < ptp_price < tp):
                    logger.log_warning(f"{key} invalid partial_tp_price {ptp_price} not between entry {entry} and tp {tp}; skipping partial TP.")
                    return
            else:
                if not (tp < ptp_price < entry):
                    logger.log_warning(f"{key} invalid partial_tp_price {ptp_price} not between tp {tp} and entry {entry}; skipping partial TP.")
                    return

            reached = (direction == "long" and price >= ptp_price) or (direction == "short" and price <= ptp_price)
            if not position.get("partial_tp_done", False) and reached:
                logger.log_info(f"{symbol} 🎯 Partial TP triggered at {ptp_price} for target size {ptp_size}")

                config = get_config()
                live_mode = config.get("live_mode", False)
                if not live_mode:
                    # dry-run behavior: emulate the partial close and mark breakeven
                    executed_sim = get_trimmed_quantity(symbol, float(ptp_size), price=ptp_price)
                    new_size_sim = max(0.0, float(size) - float(executed_sim))
                    new_size_sim_trimmed = get_trimmed_quantity(symbol, new_size_sim, price=price)
                    if new_size_sim_trimmed <= 0:
                        position["last_partial_order_id"] = "DRY_RUN"
                        position["last_partial_order_status"] = "FILLED"
                        position["last_partial_executed_qty"] = executed_sim
                        position["partial_tp_done"] = True
                        self.save_positions()
                        self.close_position(symbol, direction)
                        send_discord_log(f"{symbol} (DRY) Partial TP simulated and fully closed: executed={executed_sim}", level="INFO")
                        return
                    position["last_partial_order_id"] = "DRY_RUN"
                    position["last_partial_order_status"] = "FILLED"
                    position["last_partial_executed_qty"] = executed_sim
                    position["size"] = new_size_sim_trimmed
                    position["partial_tp_done"] = True
                    position["tp1_triggered"] = True
                    position["stop_loss"] = float(entry)
                    position["breakeven"] = True
                    position["breakeven_set_at"] = int(time.time())
                    self.save_positions()
                    send_discord_log(f"{symbol} (DRY) Partial TP simulated: executed={executed_sim}, new_size={new_size_sim_trimmed}", level="INFO")
                    return

                # LIVE mode: attempt partial close
                try:
                    close_side = "SELL" if direction == "long" else "BUY"
                    # determine hedge/one-way
                    try:
                        pos_mode = client.futures_get_position_mode()
                        is_hedge = bool(pos_mode.get("dualSidePosition", False))
                    except Exception:
                        is_hedge = False

                    qty_to_close = float(ptp_size)
                    qty_trimmed = get_trimmed_quantity(symbol, qty_to_close, price=ptp_price)
                    if qty_trimmed <= 0:
                        logger.log_warning(f"{symbol} ⚠️ partial_tp qty trimmed to <=0 (requested {qty_to_close}) - aborting partial close attempt.")
                        send_discord_log(f"{symbol} ⚠️ Partial TP aborted: qty trimmed to 0 (requested {qty_to_close}).", level="WARNING")
                        return

                    # Use MARKET reduceOnly for immediate partial (works for one-way); when hedge, set positionSide.
                    order_payload = {
                        "symbol": symbol,
                        "side": close_side,
                        "type": "MARKET",
                        "quantity": qty_trimmed,
                        "reduceOnly": True
                    }
                    if is_hedge:
                        order_payload.pop("reduceOnly", None)
                        order_payload["positionSide"] = "LONG" if direction == "long" else "SHORT"

                    logger.log_info(f"{symbol} [partial_tp] placing MARKET reduceOnly payload: {order_payload}")
                    resp = client.futures_create_order(**order_payload)
                    logger.log_info(f"{symbol} [partial_tp] create response: {resp}")

                    # Poll for executedQty up to timeout
                    order_id = resp.get("orderId") or resp.get("clientOrderId")
                    executed = 0.0
                    last_status = resp.get("status", "UNKNOWN")

                    # immediate fills sometimes included in response
                    if isinstance(resp, dict) and resp.get("executedQty"):
                        executed = float(resp.get("executedQty") or 0.0)
                    elif isinstance(resp, dict) and resp.get("fills"):
                        executed = _sum_fills_qty(resp.get("fills"))

                    if order_id:
                        start_ts = time.time()
                        while time.time() - start_ts < _ORDER_POLL_TIMEOUT:
                            try:
                                o = client.futures_get_order(symbol=symbol, orderId=order_id)
                                last_status = o.get("status", last_status)
                                executed = _to_float_safe(o.get("executedQty") or 0.0) or executed
                                # fallback to fills if executedQty missing
                                if (not executed or executed <= 0) and o.get("fills"):
                                    executed = _sum_fills_qty(o.get("fills"))
                                logger.log_debug(f"{symbol} poll order {order_id} status={last_status} executedQty={executed}")
                                if executed > _MIN_EXECUTED_TO_ACCEPT or str(last_status).upper() == "FILLED":
                                    break
                            except Exception as e:
                                logger.log_debug(f"{symbol} poll error: {e}")
                            time.sleep(_ORDER_POLL_INTERVAL)

                    executed = float(executed or 0.0)
                    executed_trimmed = get_trimmed_quantity(symbol, executed, price=ptp_price)

                    logger.log_info(f"{symbol} [partial_tp] final executed_qty={executed}, executed_trimmed={executed_trimmed}, intended={qty_trimmed}")

                    if executed_trimmed <= 0:
                        status_upper = str(last_status).upper() if last_status else "UNKNOWN"
                        msg = f"{symbol} ⚠ Partial TP order not filled (status={status_upper}, executed={executed})."
                        logger.log_warning(msg)
                        send_discord_log(msg, level="WARNING")
                        # try cancel attempt (best-effort)
                        if order_id:
                            try:
                                client.futures_cancel_order(symbol=symbol, orderId=order_id)
                            except Exception:
                                pass
                        return

                    # Compute new_size = current_size - executed_trimmed, trimmed to step
                    current_size = float(position.get("size", 0.0))
                    new_size_raw = max(0.0, current_size - executed_trimmed)
                    new_size_trimmed = get_trimmed_quantity(symbol, new_size_raw, price=price)

                    logger.log_debug(f"{symbol} [partial_tp] current_size={current_size}, new_size_raw={new_size_raw}, new_size_trimmed={new_size_trimmed}")

                    # minimal step detection
                    minimal_step = get_trimmed_quantity(symbol, 1e-12, price=price)

                    # If after trimming new_size becomes zero or less than one step, treat as fully closed
                    if new_size_trimmed <= 0 or new_size_trimmed < minimal_step:
                        logger.log_info(f"{symbol} ✅ Partial TP resulted in full close (executed {executed_trimmed} ≈ {current_size}).")
                        send_discord_log(f"{symbol} ✅ Position fully closed by partial TP. executed={executed_trimmed}, previous_size={current_size}", level="INFO")
                        position["last_partial_order_id"] = order_id
                        position["last_partial_order_status"] = str(last_status)
                        position["last_partial_executed_qty"] = executed_trimmed
                        position["last_partial_executed_price"] = None
                        position["partial_tp_done"] = True
                        self.save_positions()
                        self.close_position(symbol, direction)
                        return

                    # Otherwise update local position size to new_size_trimmed and mark partial done
                    position["last_partial_order_id"] = order_id
                    position["last_partial_order_status"] = str(last_status)
                    position["last_partial_executed_qty"] = executed_trimmed
                    position["last_partial_executed_price"] = None
                    position["size"] = new_size_trimmed
                    position["partial_tp_done"] = True
                    position["tp1_triggered"] = True
                    try:
                        position["stop_loss"] = float(entry)
                        position["breakeven"] = True
                        position["breakeven_set_at"] = int(time.time())
                    except Exception:
                        pass

                    self.save_positions()
                    send_discord_log(f"{symbol} ✅ Partial TP executed: executed={executed_trimmed}, new_size={new_size_trimmed}", level="INFO")
                    logger.log_info(f"{symbol} Partial-TP processed: executed={executed_trimmed}, new_size={new_size_trimmed}")

                except BinanceAPIException as e:
                    logger.log_error(f"{symbol} ❌ Partial TP BinanceAPIException: {e}")
                    send_discord_log(f"{symbol} ❌ Partial TP BinanceAPIException: {e}", level="ERROR")
                except Exception as e:
                    logger.log_error(f"{symbol} ❌ Partial TP unexpected error: {e}")
                    logger.log_debug(traceback.format_exc())
                    send_discord_log(f"{symbol} ❌ Partial TP unexpected error: {e}", level="ERROR")
        except Exception as e:
            logger.log_error(f"{symbol} ❌ Partial TP check error: {e}")
            logger.log_debug(traceback.format_exc())

    def check_stop_loss(self, symbol: str, direction: str, price: float) -> None:
        """
        Triggered when live price crosses the stop_loss level.
        Attempts to close remaining size reliably, polls for fill and updates local state.
        """
        try:
            key = f"{symbol}_{direction}"
            pos = self.positions.get(key)
            if not pos:
                return

            sl = _to_float_safe(pos.get("stop_loss"))
            size = _to_float_safe(pos.get("size") or pos.get("qty"))
            if sl is None or size is None:
                return

            triggered = (direction == "long" and price <= sl) or (direction == "short" and price >= sl)
            if not triggered:
                return

            logger.log_info(f"{symbol} ⛔ Stop-loss triggered at {sl} — attempting to close remaining size {size}")
            config = get_config()
            live_mode = config.get("live_mode", False)
            if not live_mode:
                # simulate close in dry-run
                pos["last_stop_order_id"] = "DRY_RUN"
                pos["last_stop_order_status"] = "FILLED"
                pos["last_stop_executed_qty"] = get_trimmed_quantity(symbol, size, price=price)
                self.save_positions()
                self.close_position(symbol, direction)
                send_discord_log(f"{symbol} (DRY) SL simulated closed size {pos['last_stop_executed_qty']}", level="INFO")
                return

            try:
                close_side = "SELL" if direction == "long" else "BUY"
                qty_trimmed = get_trimmed_quantity(symbol, float(size), price=price)
                if qty_trimmed <= 0:
                    send_discord_log(f"{symbol} ⚠ SL abort: remaining qty trimmed to zero", level="WARNING")
                    return

                # prefer MARKET reduceOnly for immediate exit
                order_payload = {
                    "symbol": symbol,
                    "side": close_side,
                    "type": "MARKET",
                    "quantity": qty_trimmed,
                    "reduceOnly": True,
                }
                # hedge mode handling
                try:
                    pos_mode = client.futures_get_position_mode()
                    is_hedge = bool(pos_mode.get("dualSidePosition", False))
                except Exception:
                    is_hedge = False
                if is_hedge:
                    order_payload.pop("reduceOnly", None)
                    order_payload["positionSide"] = "LONG" if direction == "long" else "SHORT"

                resp = client.futures_create_order(**order_payload)
                order_id = resp.get("orderId") or resp.get("clientOrderId")
                executed = 0.0
                # quick check resp
                if isinstance(resp, dict) and resp.get("executedQty"):
                    executed = float(resp.get("executedQty") or 0.0)
                elif isinstance(resp, dict) and resp.get("fills"):
                    executed = _sum_fills_qty(resp.get("fills"))

                if order_id:
                    start = time.time()
                    while time.time() - start < _ORDER_POLL_TIMEOUT:
                        try:
                            o = client.futures_get_order(symbol=symbol, orderId=order_id)
                            executed_candidate = _to_float_safe(o.get("executedQty") or 0.0) or 0.0
                            if (not executed_candidate or executed_candidate <= 0) and o.get("fills"):
                                executed_candidate = _sum_fills_qty(o.get("fills"))
                            if executed_candidate and executed_candidate > executed:
                                executed = executed_candidate
                            if executed > _MIN_EXECUTED_TO_ACCEPT or str(o.get("status","")).upper() == "FILLED":
                                break
                        except Exception as e:
                            logger.log_debug(f"{symbol} SL poll error: {e}")
                        time.sleep(_ORDER_POLL_INTERVAL)

                executed_trimmed = get_trimmed_quantity(symbol, float(executed), price=price) if executed else 0.0
                logger.log_info(f"{symbol} SL executed {executed} -> trimmed {executed_trimmed}")

                if executed_trimmed <= 0:
                    send_discord_log(f"{symbol} ⚠ SL order not filled (executed={executed}). Manual reconciliation required.", level="ERROR")
                    try:
                        pos["last_stop_order_id"] = order_id
                        pos["last_stop_order_status"] = resp.get("status") if isinstance(resp, dict) else None
                        pos["last_stop_resp"] = resp
                        self.save_positions()
                    except Exception:
                        pass
                    return

                send_discord_log(f"{symbol} ⛔ SL executed, closed {executed_trimmed}", level="ERROR")
                pos["last_stop_order_id"] = order_id
                pos["last_stop_order_status"] = resp.get("status") if isinstance(resp, dict) else "FILLED"
                pos["last_stop_executed_qty"] = executed_trimmed
                self.save_positions()
                self.close_position(symbol, direction)
                return

            except Exception as e:
                logger.log_error(f"{symbol} ❌ SL execution failed: {e}")
                logger.log_debug(traceback.format_exc())
                try:
                    send_discord_log(f"{symbol} ❌ SL execution failed: {e}", level="ERROR")
                except Exception:
                    pass

        except Exception as e:
            logger.log_error(f"{symbol} ❌ check_stop_loss error: {e}")
            logger.log_debug(traceback.format_exc())

    def sync_with_binance(self, symbol: str = None) -> None:
        """
        Sync local positions with exchange positions.
        When adding positions found on Binance, skip if Binance 'entryPrice' is 0 or missing.
        When Binance reports missing positions, mark `binance_missing_since` and only
        remove local state after a grace period to avoid race conditions.
        """
        try:
            config = get_config()
            symbols = [symbol] if symbol else config.get("base_pairs", [])
            scalper_settings = config.get("scalper_settings", {})
            min_sl_pct = scalper_settings.get("min_sl_distance_pct", 0.02)
            rr_ratio = scalper_settings.get("risk_reward_ratio", 2)

            binance_positions = []
            try:
                if client:
                    binance_positions = client.futures_position_information()
            except Exception:
                binance_positions = []

            for sym in symbols:
                relevant_positions = [p for p in binance_positions if p.get("symbol") == sym]
                synced_positions: Dict[str, Any] = {}

                for p in relevant_positions:
                    try:
                        amt = float(p.get("positionAmt", 0.0))
                    except Exception:
                        amt = 0.0
                    if abs(amt) <= 0:
                        continue
                    side = "long" if amt > 0 else "short"
                    key = f"{sym}_{side}"
                    if key not in self.positions:
                        # Guard: ensure entryPrice is valid before creating a local record
                        entry_price_raw = p.get("entryPrice", None)
                        try:
                            entry_price = float(entry_price_raw) if entry_price_raw is not None else 0.0
                        except Exception:
                            entry_price = 0.0

                        if not entry_price or entry_price <= 0:
                            logger.log_warning(
                                f"Binance position {key} has invalid entryPrice={entry_price_raw}. Skipping local add; marking for manual reconciliation."
                            )
                            self.positions[f"{key}_synced_incomplete"] = {
                                "symbol": sym,
                                "direction": side,
                                "size": abs(amt),
                                "entryPrice_raw": entry_price_raw,
                                "source": "binance_sync_incomplete",
                                "entry_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }
                            self.save_positions()
                            continue

                        sl = entry_price * (1 - min_sl_pct) if side == "long" else entry_price * (1 + min_sl_pct)
                        tp = entry_price * (1 + min_sl_pct * rr_ratio) if side == "long" else entry_price * (1 - min_sl_pct * rr_ratio)
                        logger.log_warning(f"Found Binance position {key} not in local state. Syncing with SL: {sl}, TP: {tp}")
                        self.add_position(
                            sym,
                            side,
                            {
                                "symbol": sym,
                                "direction": side,
                                "entry_price": entry_price,
                                "size": abs(amt),
                                "stop_loss": float(sl),
                                "take_profit": float(tp),
                                "confidence": 1.0,
                                "label": "synced",
                                "source": "binance_sync",
                                "entry_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            },
                        )
                    synced_positions[f"{sym}_{side}"] = self.positions.get(f"{sym}_{side}")

                # When remote says there is no position but we have local state, mark missing and remove after grace
                for direction in ["long", "short"]:
                    key = f"{sym}_{direction}"
                    if key in self.positions:
                        position_exists = any(
                            (p.get("symbol") == sym and
                             ((direction == "long" and float(p.get("positionAmt", 0.0)) > 0) or
                              (direction == "short" and float(p.get("positionAmt", 0.0)) < 0))
                             and abs(float(p.get("positionAmt", 0.0))) > 0)
                            for p in relevant_positions
                        )

                        if not position_exists:
                            local_pos = self.positions.get(key)
                            now_ts = int(time.time())
                            missing_since = local_pos.get("binance_missing_since") if isinstance(local_pos, dict) else None
                            if not missing_since:
                                if isinstance(local_pos, dict):
                                    local_pos["binance_missing_since"] = now_ts
                                    self.save_positions()
                                logger.log_warning(f"No Binance position for {key}. Marked missing_since={now_ts}; will wait {BINANCE_MISSING_GRACE_SECONDS}s before removing.")
                            else:
                                if now_ts - missing_since > BINANCE_MISSING_GRACE_SECONDS:
                                    logger.log_warning(f"No Binance position for {key} for >{BINANCE_MISSING_GRACE_SECONDS}s. Removing local state.")
                                    self.close_position(sym, direction)
                                else:
                                    logger.log_debug(f"No Binance position for {key} but within grace ({now_ts - missing_since}s).")

                logger.log_debug(f"Synced positions for {sym}: {synced_positions}")
        except Exception as e:
            logger.log_error(f"Unexpected error syncing positions for {symbol or 'all symbols'}: {e}")
            logger.log_debug(traceback.format_exc())

    def _save_positions(self) -> None:
        self.save_positions()


# module-level instance for convenient imports
position_manager = PositionManager()
