# data/atr_cache.py

import os
import json
from typing import Dict, Optional
from datetime import datetime
from core.logger import global_logger as logger
from utils.notifier import Notifier, notifier
from collections import deque

notifier = Notifier()

class ATRCache:
    def __init__(self, path: str = "logs/atr_cache.json"):
        self.path = path
        self.cache: Dict[str, Dict[str, float or str]] = {}
        self._load_cache()

    def update_atr(self, symbol: str, atr: float):
        try:
            self.prune_old_entries()
            self.cache[symbol] = {
                "atr": float(atr),
                "last_update": datetime.now().astimezone().isoformat()
            }
            self._save_cache()
        except Exception as e:
            logger.log_error(f"❌ ATRCache: Failed to update ATR for {symbol}: {e}")
            notifier.send_critical(f"❌ ATRCache: Failed to update ATR for {symbol}: {e}")

    def prune_old_entries(self, max_age_days: int = 7):
        now = datetime.now().astimezone()
        stale_keys = []
        for symbol, entry in self.cache.items():
            try:
                last_update = datetime.fromisoformat(entry.get("last_update", "1970-01-01T00:00:00"))
                if (now - last_update).days > max_age_days:
                    stale_keys.append(symbol)
            except Exception:
                stale_keys.append(symbol)  # purge if corrupt

        for key in stale_keys:
            del self.cache[key]
            logger.log_debug(f"{key} 🧹 Removed from ATR cache (stale > {max_age_days}d).")

    def _is_fresh(self, entry: dict, max_age_days: int = 14) -> bool:
        try:
            last_update = datetime.fromisoformat(entry.get("last_update", "1970-01-01T00:00:00"))
            return (datetime.now().astimezone() - last_update).days <= max_age_days
        except Exception:
            return False


    def get_atr(self, symbol: str) -> Optional[float]:
        """Returns the ATR value for the given symbol, or None if not present."""
        entry = self.cache.get(symbol)
        return entry["atr"] if entry else None

    def get_last_update_time(self, symbol: str) -> Optional[datetime]:
        """Returns the last update time as a datetime object for a symbol."""
        entry = self.cache.get(symbol)
        if entry and "last_update" in entry:
            try:
                return datetime.fromisoformat(entry["last_update"])
            except Exception as e:
                logger.log_error(f"⚠️ ATRCache: Invalid timestamp format for {symbol}: {e}")
                return None
        return None

    def _load_cache(self):
        """Loads ATR cache from disk, with error handling."""
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    self.cache = json.load(f)
                logger.log_once(f"📂 ATR cache loaded successfully. Entries: {len(self.cache)}", level="INFO")
            except Exception as e:
                logger.log_error(f"❌ ATRCache: Failed to load cache file: {e}")
                notifier.send_critical(f"❌ ATRCache: Failed to load cache file. ATR data may be incomplete.\nError: {e}")
                self.cache = {}
        else:
            logger.log_warning("📁 ATR cache file not found — starting fresh.")

    def _save_cache(self):
        try:
            filtered_cache = {
                sym: entry for sym, entry in self.cache.items()
                if self._is_fresh(entry)
            }
            with open(self.path, "w") as f:
                json.dump(filtered_cache, f, indent=2)
        except Exception as e:
            logger.log_error(f"❌ ATRCache: Failed to save cache file: {e}")
            notifier.send_critical(f"❌ ATRCache: Failed to save cache file. Manual intervention may be required.\nError: {e}")


#5M ATR
class ScalperATRCache:
    def __init__(self, path: str = "logs/scalper_atr_cache.json"):
        self.path = path
        self.cache: Dict[str, Dict[str, float or str]] = {}
        self._load_cache()

    def update_atr(self, symbol: str, atr: float):
        try:
            self.prune_old_entries()
            self.cache[symbol] = {
                "atr": float(atr),
                "last_update": datetime.now().astimezone().isoformat()
            }
            self._save_cache()
        except Exception as e:
            logger.log_error(f"❌ ScalperATRCache: Failed to update ATR for {symbol}: {e}")
            notifier.send_critical(f"❌ ScalperATRCache: Failed to update ATR for {symbol}: {e}")

    def get_atr(self, symbol: str) -> Optional[float]:
        """Returns the ATR value for the given symbol (scalper), or None if not present."""
        entry = self.cache.get(symbol)
        return entry["atr"] if entry else None

    def prune_old_entries(self, max_age_days: int = 7):
        now = datetime.now().astimezone()
        stale_keys = []
        for symbol, entry in self.cache.items():
            try:
                last_update = datetime.fromisoformat(entry.get("last_update", "1970-01-01T00:00:00"))
                if (now - last_update).days > max_age_days:
                    stale_keys.append(symbol)
            except Exception:
                stale_keys.append(symbol)  # purge if corrupt

        for key in stale_keys:
            del self.cache[key]
            logger.log_debug(f"{key} 🧹 Removed from ATR cache (stale > {max_age_days}d).")

    def _is_fresh(self, entry: dict, max_age_days: int = 14) -> bool:
        try:
            last_update = datetime.fromisoformat(entry.get("last_update", "1970-01-01T00:00:00"))
            return (datetime.now().astimezone() - last_update).days <= max_age_days
        except Exception:
            return False

    def get_last_update_time(self, symbol: str) -> Optional[datetime]:
        """Returns the last update time as a datetime object for a symbol (scalper)."""
        entry = self.cache.get(symbol)
        if entry and "last_update" in entry:
            try:
                return datetime.fromisoformat(entry["last_update"])
            except Exception as e:
                logger.log_error(f"⚠️ ScalperATRCache: Invalid timestamp format for {symbol}: {e}")
                return None
        return None

    def _load_cache(self):
        """Loads Scalper ATR cache from disk, with error handling."""
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    self.cache = json.load(f)
                logger.log_once(f"📂 Scalper ATR cache loaded successfully. Entries: {len(self.cache)}")
            except Exception as e:
                logger.log_error(f"❌ ScalperATRCache: Failed to load cache file: {e}")
                notifier.send_critical(f"❌ ScalperATRCache: Failed to load cache file. ATR data may be incomplete.\nError: {e}")
                self.cache = {}
        else:
            logger.log_warning("📁 Scalper ATR cache file not found — starting fresh.")

    def _save_cache(self):
        try:
            filtered_cache = {
                sym: entry for sym, entry in self.cache.items()
                if self._is_fresh(entry)
            }
            with open(self.path, "w") as f:
                json.dump(filtered_cache, f, indent=2)
        except Exception as e:
            logger.log_error(f"❌ ScalperATRCache: Failed to save cache file: {e}")
            notifier.send_critical(f"❌ ScalperATRCache: Failed to save cache file. Manual intervention may be required.\nError: {e}")


# === Singleton Instance ===
atr_cache = ATRCache()
scalper_atr_cache = ScalperATRCache()
