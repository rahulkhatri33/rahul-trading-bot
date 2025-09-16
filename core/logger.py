# core/logger.py
"""
Central logging for the bot — compatibility restore + live-feed method.

Exports:
 - YogiLogger      : class (alias of BotLogger) for backward compatibility
 - global_logger   : wrapper object with .log_debug/.log_info/.log_warning/.log_error/.log_once/.log_live_feed and attribute forwarding to underlying logging.Logger
 - global_raw_logger : the underlying logging.Logger (if raw Logger methods are needed)
 - logger          : same as global_logger (convenience)

Features:
 - Rotating file handlers: logs/all.log, logs/info.log, logs/warning.log, logs/error.log
 - Module-specific file: logs/trade_executor.log
 - log_live_feed(msg): writes INFO-level message and also appends to logs/live_feed.log
 - Discord alert handler posts ERROR/CRITICAL messages to Discord (lazy import)
 - log_once(msg, level='info', key=None, ttl=300) to suppress duplicate messages for a TTL
"""

import logging
import logging.handlers
import os
import time
import traceback
from typing import Optional

# Defensive import of get_config
try:
    from core.config import get_config
except Exception:
    def get_config():
        return {}

# --- Helpers / Filters ------------------------------------------------------
class LevelRangeFilter(logging.Filter):
    def __init__(self, min_level: Optional[int] = None, max_level: Optional[int] = None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        level = record.levelno
        if self.min_level is not None and level < self.min_level:
            return False
        if self.max_level is not None and level > self.max_level:
            return False
        return True

class ModuleFilter(logging.Filter):
    def __init__(self, module_name: str):
        super().__init__()
        self.module_name = module_name

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if record.module == self.module_name or (isinstance(record.name, str) and record.name.startswith(self.module_name)):
                return True
            if getattr(record, "pathname", "") and self.module_name in record.pathname:
                return True
            return False
        except Exception:
            return False

def ensure_logs_dir(path: str = "logs"):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

# --- Discord alert handler (defensive lazy import) -------------------------
class DiscordAlertHandler(logging.Handler):
    def __init__(self, enabled: bool = True):
        super().__init__(level=logging.ERROR)
        self.enabled = enabled
        self._fn = None

    def _load_fn(self):
        if self._fn is not None:
            return self._fn
        try:
            from utils.discord_logger import send_discord_log
            self._fn = send_discord_log
            return self._fn
        except Exception:
            self._fn = None
            return None

    def emit(self, record: logging.LogRecord):
        if not self.enabled:
            return
        try:
            fn = self._load_fn()
            if not fn:
                return
            msg = self.format(record)
            try:
                fn(msg, level=record.levelname)
            except TypeError:
                try:
                    fn(msg)
                except Exception:
                    pass
            except Exception:
                pass
        except Exception:
            try:
                traceback.print_exc()
            except Exception:
                pass

# --- Logger implementation --------------------------------------------------
class BotLogger:
    """
    Primary logger implementation with log_once and log_live_feed support.
    """
    def __init__(self, name: str = "trading_bot"):
        self._name = name
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)
        self._configured = False

        # For log_once dedupe: map key -> expiry_timestamp
        self._recent_once = {}
        self._default_once_ttl = 300  # seconds

        self.configure()

    def configure(self):
        if self._configured:
            return

        cfg = {}
        try:
            cfg = get_config() or {}
        except Exception:
            cfg = {}

        logging_cfg = cfg.get("logging", {}) if isinstance(cfg, dict) else {}
        max_file_mb = logging_cfg.get("max_file_size_mb", 5)
        backup_count = logging_cfg.get("backup_count", 10)
        logs_path = logging_cfg.get("logs_path", "logs")

        ensure_logs_dir(logs_path)
        max_bytes = int(max_file_mb * 1024 * 1024)

        formatter = logging.Formatter(fmt="%(asctime)s [%(levelname)s] - %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        # console handler (INFO+)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

        # all.log: DEBUG and up
        all_file = os.path.join(logs_path, "all.log")
        fh_all = logging.handlers.RotatingFileHandler(all_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh_all.setLevel(logging.DEBUG)
        fh_all.setFormatter(formatter)
        self._logger.addHandler(fh_all)

        # info.log: INFO only
        info_file = os.path.join(logs_path, "info.log")
        fh_info = logging.handlers.RotatingFileHandler(info_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh_info.setLevel(logging.INFO)
        fh_info.addFilter(LevelRangeFilter(min_level=logging.INFO, max_level=logging.INFO))
        fh_info.setFormatter(formatter)
        self._logger.addHandler(fh_info)

        # warning.log: WARNING only
        warn_file = os.path.join(logs_path, "warning.log")
        fh_warn = logging.handlers.RotatingFileHandler(warn_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh_warn.setLevel(logging.WARNING)
        fh_warn.addFilter(LevelRangeFilter(min_level=logging.WARNING, max_level=logging.WARNING))
        fh_warn.setFormatter(formatter)
        self._logger.addHandler(fh_warn)

        # error.log: ERROR and CRITICAL only
        err_file = os.path.join(logs_path, "error.log")
        fh_err = logging.handlers.RotatingFileHandler(err_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh_err.setLevel(logging.ERROR)
        fh_err.addFilter(LevelRangeFilter(min_level=logging.ERROR, max_level=None))
        fh_err.setFormatter(formatter)
        self._logger.addHandler(fh_err)

        # module-specific log for trade_executor
        try:
            trade_file = os.path.join(logs_path, "trade_executor.log")
            fh_trade = logging.handlers.RotatingFileHandler(trade_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
            fh_trade.setLevel(logging.DEBUG)
            fh_trade.addFilter(ModuleFilter("trade_executor"))
            fh_trade.setFormatter(formatter)
            self._logger.addHandler(fh_trade)
        except Exception:
            pass

        # Discord alert handler for ERROR / CRITICAL (configurable)
        alerts_cfg = logging_cfg.get("alerts", {}) if isinstance(logging_cfg, dict) else {}
        alerts_enabled = alerts_cfg.get("enabled", False)
        if not alerts_enabled:
            top_alerts = cfg.get("alerts", {}) if isinstance(cfg, dict) else {}
            alerts_enabled = bool(top_alerts.get("discord_webhook"))

        try:
            discord_handler = DiscordAlertHandler(enabled=alerts_enabled)
            discord_handler.setLevel(logging.ERROR)
            discord_handler.setFormatter(formatter)
            self._logger.addHandler(discord_handler)
        except Exception:
            pass

        self._logger.propagate = False
        self._configured = True

    @property
    def logger(self):
        """Underlying logging.Logger"""
        return self._logger

    # Standard convenience logging methods (used across repo)
    def log_debug(self, msg: str, *args, **kwargs):
        try:
            self._logger.debug(msg, *args, **kwargs)
        except Exception:
            pass

    def log_info(self, msg: str, *args, **kwargs):
        try:
            self._logger.info(msg, *args, **kwargs)
        except Exception:
            pass

    def log_warning(self, msg: str, *args, **kwargs):
        try:
            self._logger.warning(msg, *args, **kwargs)
        except Exception:
            pass

    def log_error(self, msg: str, *args, **kwargs):
        try:
            self._logger.error(msg, *args, **kwargs)
        except Exception:
            pass

    # New: de-duplicating log_once
    def log_once(self, msg: str, level: str = "info", key: Optional[str] = None, ttl: Optional[int] = None):
        """
        Log a message only once for a given `key` (or the message text if key is None)
        within a TTL window. TTL defaults to self._default_once_ttl seconds.

        level: 'debug'|'info'|'warning'|'error'|'critical'
        key: optional stable key for de-duplication
        ttl: seconds to suppress subsequent identical key logs
        """
        try:
            if ttl is None:
                ttl = self._default_once_ttl
            dedupe_key = key if key is not None else f"log_once:{msg}"
            now = time.time()
            expiry = self._recent_once.get(dedupe_key)
            if expiry and expiry > now:
                # suppressed
                return
            # record new expiry
            self._recent_once[dedupe_key] = now + float(ttl)

            # map level to method
            level = (level or "info").lower()
            if level == "debug":
                self.log_debug(msg)
            elif level == "info":
                self.log_info(msg)
            elif level == "warning":
                self.log_warning(msg)
            elif level == "error":
                self.log_error(msg)
            elif level == "critical":
                try:
                    self._logger.critical(msg)
                except Exception:
                    self.log_error(msg)
            else:
                self.log_info(msg)
        except Exception:
            try:
                traceback.print_exc()
            except Exception:
                pass

    # New: log_live_feed for runner and other live-stream messages
    def log_live_feed(self, msg: str, *args, **kwargs):
        """
        Specialized logging entrypoint for live feed / heartbeat / stream messages.
        Writes an INFO-level log (so it appears in all/info logs) AND appends
        a timestamped line to logs/live_feed.log for quick inspection.
        Defensive: never raises.
        """
        try:
            # Log via standard logger (captures into all.log and info.log)
            try:
                self._logger.info(msg, *args, **kwargs)
            except Exception:
                # fallback to plain message if formatting fails
                try:
                    self._logger.info(str(msg))
                except Exception:
                    pass

            # Also append to a dedicated live_feed log file for easier tailing.
            try:
                logs_path = get_config().get("logging", {}).get("logs_path", "logs")
            except Exception:
                logs_path = "logs"
            try:
                ensure_logs_dir(logs_path)
                live_file = os.path.join(logs_path, "live_feed.log")
                # timestamp similar to other logs
                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                line = f"{ts} [LIVE] - {self._name} - {msg}\n"
                # append safely
                with open(live_file, "a", encoding="utf-8") as fh:
                    fh.write(line)
            except Exception:
                # swallow any file I/O errors
                pass
        except Exception:
            try:
                traceback.print_exc()
            except Exception:
                pass

# Wrapper that exposes log_* methods AND forwards unknown attributes to underlying logging.Logger.
class LoggerWrapper:
    def __init__(self, bot_logger: BotLogger):
        self._bot = bot_logger

    # explicit wrappers
    def log_debug(self, *a, **k): return self._bot.log_debug(*a, **k)
    def log_info(self, *a, **k): return self._bot.log_info(*a, **k)
    def log_warning(self, *a, **k): return self._bot.log_warning(*a, **k)
    def log_error(self, *a, **k): return self._bot.log_error(*a, **k)
    def log_once(self, *a, **k): return self._bot.log_once(*a, **k)
    def log_live_feed(self, *a, **k): return self._bot.log_live_feed(*a, **k)

    # forward other attributes (like .info, .debug) to the underlying logging.Logger
    def __getattr__(self, name):
        try:
            return getattr(self._bot.logger, name)
        except Exception:
            raise AttributeError(name)

# Create a single BotLogger instance and wrappers
_bot_logger_instance = BotLogger()
_wrapped_logger = LoggerWrapper(_bot_logger_instance)

# Exported objects for compatibility:
YogiLogger = BotLogger
global_logger = _wrapped_logger
global_raw_logger = _bot_logger_instance.logger
logger = _wrapped_logger

# End of file
