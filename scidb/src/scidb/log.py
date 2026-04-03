"""Minimal structured logger for scidb with levels and file output.

Usage:
    from scidb.log import Log

    Log.set_level('DEBUG')          # show all messages
    Log.set_level('INFO')           # show INFO, WARN, ERROR (default)
    Log.set_level('WARN')           # show WARN, ERROR only
    Log.set_level('ERROR')          # show ERROR only

    Log.debug('Processing %d items', n)
    Log.info('Loaded %s', type_name)
    Log.warn('No data for %s', key)
    Log.error('Failed: %s', msg)

Output format: [HH:MM:SS.FFF] [LEVEL] message

Writes to scidb.log next to the database file (set automatically by
configure_database()). Each log call opens, appends, and closes the file
so every line is flushed to disk immediately.
"""

import threading
from datetime import datetime


class Log:
    """Singleton-style logger with file output and log levels."""

    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3

    _level: int = INFO
    _path: "str | None" = None
    _lock = threading.Lock()

    @classmethod
    def set_level(cls, level: "int | str") -> None:
        """Set the global log level.

        Args:
            level: A string ('DEBUG', 'INFO', 'WARN', 'ERROR') or numeric (0-3).
        """
        if isinstance(level, str):
            level = cls._parse_level(level.upper())
        cls._level = level

    @classmethod
    def get_level(cls) -> int:
        """Get the current log level (default: INFO)."""
        return cls._level

    @classmethod
    def set_path(cls, log_path: "str | None") -> None:
        """Set the log file path for file output.

        Called automatically by configure_database().
        """
        cls._path = log_path

    @classmethod
    def get_path(cls) -> "str | None":
        """Get the current log file path (None if not set)."""
        return cls._path

    @classmethod
    def debug(cls, msg: str) -> None:
        """Log a message at DEBUG level."""
        if cls._level <= cls.DEBUG:
            cls._emit("DEBUG", msg)

    @classmethod
    def info(cls, msg: str) -> None:
        """Log a message at INFO level."""
        if cls._level <= cls.INFO:
            cls._emit("INFO", msg)

    @classmethod
    def warn(cls, msg: str) -> None:
        """Log a message at WARN level."""
        if cls._level <= cls.WARN:
            cls._emit("WARN", msg)

    @classmethod
    def error(cls, msg: str) -> None:
        """Log a message at ERROR level."""
        if cls._level <= cls.ERROR:
            cls._emit("ERROR", msg)

    @classmethod
    def _emit(cls, level_str: str, msg: str) -> None:
        """Format and write a log message with timestamp to the log file."""
        if cls._path is None:
            return
        now = datetime.now()
        ts = now.strftime("%H:%M:%S.") + f"{now.microsecond // 1000:03d}"
        line = f"[{ts}] [{level_str}] {msg}\n"
        with cls._lock:
            try:
                with open(cls._path, "a") as f:
                    f.write(line)
            except OSError:
                pass

    @classmethod
    def _parse_level(cls, name: str) -> int:
        """Convert a level name string to numeric value."""
        levels = {
            "DEBUG": cls.DEBUG,
            "INFO": cls.INFO,
            "WARN": cls.WARN,
            "ERROR": cls.ERROR,
        }
        if name not in levels:
            import warnings
            warnings.warn(
                f"Unknown log level '{name}', defaulting to INFO.",
                UserWarning,
            )
            return cls.INFO
        return levels[name]
