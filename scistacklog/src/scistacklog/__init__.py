"""scistacklog — the shared logging facade for every scistack layer.

Dual-sink design:

- **console** (stderr): concise pipeline narrative for the person running the
  pipeline. Time-only timestamps, level shown only at WARN and above.
- **file** (``scidb.log`` next to the database, set via ``Log.set_path``):
  the run document. Date + millisecond timestamps, level, and originating
  layer on every line; one record per line.

Both sinks default to INFO. DEBUG detail (per-iteration lines, timing phase
tables, framework internals) is opt-in per sink::

    Log.set_level("DEBUG", sink="file")     # full detail in the file only
    Log.set_level("DEBUG")                  # both sinks

or via the ``SCIDB_LOG_LEVEL`` environment variable (applied to both sinks).

Usage::

    from scistacklog import Log

    Log.info("for_each(compute_psd) — 120 iterations", layer="scifor")
    Log.debug("combo detail: %s", combo, layer="scifor")
    Log.error("save failed", layer="scidb", exc_info=True)

Every emit goes through a stdlib logger named after the layer (``scidb``,
``scifor``, …), so existing module loggers such as ``scidb.discover`` or
``scilineage.hashing`` are covered automatically as children. Level
filtering happens on the two handlers, never on the loggers: records always
propagate to the root logger, so pytest's ``caplog`` captures them
regardless of sink levels.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
import warnings
from contextlib import contextmanager

__all__ = ["Log", "LAYERS"]

#: Top-level logger name for each scistack layer. A record's ``[layer]`` tag
#: in the log line is the first dotted component of its logger name.
LAYERS = (
    "scidb",
    "scifor",
    "sciduck",
    "scihist",
    "scilineage",
    "scistack",
    "scistack_gui",
    "scistackplot",
    "scistackplotdb",
    "matlab",
)

# scidb's historical 0-3 level scale — the MATLAB-facing numeric contract.
_SCIDB_TO_PY = {
    0: logging.DEBUG,
    1: logging.INFO,
    2: logging.WARNING,
    3: logging.ERROR,
}
_NAME_TO_SCIDB = {"DEBUG": 0, "INFO": 1, "WARN": 2, "WARNING": 2, "ERROR": 3}
_SHORT_NAMES = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARN",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "ERROR",
}

_FILE_FORMAT = "%(asctime)s.%(msecs)03d %(levelshort)-5s [%(layer)s] %(message)s"
_FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"
_CONSOLE_DATEFMT = "%H:%M:%S"


class _RecordTagger(logging.Filter):
    """Adds the fields the formatters need: layer and short level name."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.layer = record.name.split(".", 1)[0]
        record.levelshort = _SHORT_NAMES.get(record.levelno, record.levelname)
        return True


class _ConsoleFormatter(logging.Formatter):
    """Time-only timestamps; level prefix only at WARN and above."""

    def __init__(self) -> None:
        super().__init__(
            "%(asctime)s [%(layer)s] %(message)s", datefmt=_CONSOLE_DATEFMT
        )
        self._alert = logging.Formatter(
            "%(asctime)s [%(layer)s] %(levelshort)s: %(message)s",
            datefmt=_CONSOLE_DATEFMT,
        )

    def format(self, record: logging.LogRecord) -> str:
        if record.levelno >= logging.WARNING:
            return self._alert.format(record)
        return super().format(record)


class _StderrHandler(logging.StreamHandler):
    """Console handler that resolves ``sys.stderr`` at emit time.

    Looking the stream up dynamically keeps the handler correct when the
    hosting process swaps stderr (pytest's capsys, MATLAB, GUI harnesses).
    """

    def __init__(self) -> None:
        super().__init__(sys.stderr)

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value):
        pass  # always dynamic; StreamHandler.__init__/setStream assignments are no-ops


class _Timer:
    """Collects named phase durations for :meth:`Log.timer`.

    ``live`` makes a phase announce itself on entry and on exit instead of
    waiting for the summary. That matters for operations measured in minutes:
    the summary is a post-mortem, and an operation that has not finished yet
    produces no summary at all — a 25-minute figure save (scidb.log 2026-09-11,
    12:28 -> 12:54) was 25 minutes of complete silence followed by one line.
    A phase that says "started" is the only way a log can distinguish "working"
    from "hung".
    """

    def __init__(
        self, *, name: str = "", layer: str = "scidb", live: bool = False
    ) -> None:
        self.phases: list[tuple[str, float]] = []
        self._name = name
        self._layer = layer
        self._live = live

    @contextmanager
    def phase(self, name: str, *, extra: str | None = None):
        if self._live:
            detail = f" — {extra}" if extra else ""
            Log.info(
                f"[timing] {self._name}: {name} started{detail}", layer=self._layer
            )
        t0 = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - t0
            self.phases.append((name, elapsed))
            if self._live:
                Log.info(
                    f"[timing] {self._name}: {name} done in {elapsed:.3f}s",
                    layer=self._layer,
                )

    def note(self, msg: str, *args) -> None:
        """Progress *within* a phase — only when the timer is live.

        For the loop a long phase is actually made of (figure 2 of 30, file 7 of
        12). Silent on a quiet timer, so the same instrumented code serves the
        interactive path without flooding it.
        """
        if self._live:
            Log.info(f"[timing] {self._name}: {msg}", *args, layer=self._layer)


class Log:
    """Framework-wide logging facade (classmethod API, MATLAB-bridged).

    Backed by stdlib logging: one stderr console handler and one file
    handler, both attached to every layer logger in :data:`LAYERS`.
    """

    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3

    _lock = threading.RLock()
    _attached = False
    _tagger = _RecordTagger()
    _console_handler: logging.Handler | None = None
    _file_handler: logging.FileHandler | None = None
    _path: str | None = None
    _console_py_level: int = logging.INFO
    _file_py_level: int = logging.INFO
    _unknown_layers: set[str] = set()

    # -- setup ------------------------------------------------------------

    @classmethod
    def attach(cls) -> None:
        """Create the sinks and attach them to every layer logger.

        Idempotent; called lazily by every other method, so nothing happens
        at import time. Reads ``SCIDB_LOG_LEVEL`` (both sinks) on first call.
        """
        with cls._lock:
            if cls._attached:
                return
            env_level = os.environ.get("SCIDB_LOG_LEVEL")
            if env_level:
                py_level = cls._to_py_level(env_level)
                cls._console_py_level = py_level
                cls._file_py_level = py_level
            handler = _StderrHandler()
            handler.setLevel(cls._console_py_level)
            handler.setFormatter(_ConsoleFormatter())
            handler.addFilter(cls._tagger)
            cls._console_handler = handler
            for name in LAYERS:
                layer_logger = logging.getLogger(name)
                # Filtering happens on the handlers; the logger passes
                # everything so caplog/root handlers see all records.
                layer_logger.setLevel(logging.DEBUG)
                layer_logger.propagate = True
                if handler not in layer_logger.handlers:
                    layer_logger.addHandler(handler)
            cls._attached = True

    @classmethod
    def bridge_python_logging(cls) -> None:
        """Deprecated alias for :meth:`attach`.

        The old implementation installed a bridge handler on a hardcoded
        subset of logger names; the layer-logger hierarchy replaces it.
        """
        cls.attach()

    @classmethod
    def set_path(cls, log_path: str | None) -> None:
        """Point the file sink at ``log_path`` (``None`` detaches it).

        Called automatically by ``scidb.configure_database()``.
        """
        cls.attach()
        with cls._lock:
            if cls._file_handler is not None:
                for name in LAYERS:
                    logging.getLogger(name).removeHandler(cls._file_handler)
                cls._file_handler.close()
                cls._file_handler = None
            cls._path = None if log_path is None else str(log_path)
            if cls._path is None:
                return
            handler = logging.FileHandler(
                cls._path, mode="a", encoding="utf-8", delay=True
            )
            handler.setLevel(cls._file_py_level)
            handler.setFormatter(logging.Formatter(_FILE_FORMAT, datefmt=_FILE_DATEFMT))
            handler.addFilter(cls._tagger)
            cls._file_handler = handler
            for name in LAYERS:
                logging.getLogger(name).addHandler(handler)

    @classmethod
    def get_path(cls) -> str | None:
        """Current log file path (``None`` if the file sink is detached)."""
        return cls._path

    # -- levels -----------------------------------------------------------

    @classmethod
    def set_level(cls, level: int | str, sink: str = "both") -> None:
        """Set the level of one or both sinks.

        Args:
            level: ``'DEBUG'``/``'INFO'``/``'WARN'``/``'ERROR'`` or numeric 0-3.
            sink: ``'console'``, ``'file'``, or ``'both'``.
        """
        if sink not in ("console", "file", "both"):
            raise ValueError(
                f"Unknown sink '{sink}' (expected 'console', 'file', or 'both')"
            )
        cls.attach()
        py_level = cls._to_py_level(level)
        with cls._lock:
            if sink in ("console", "both"):
                cls._console_py_level = py_level
                if cls._console_handler is not None:
                    cls._console_handler.setLevel(py_level)
            if sink in ("file", "both"):
                cls._file_py_level = py_level
                if cls._file_handler is not None:
                    cls._file_handler.setLevel(py_level)

    @classmethod
    def get_level(cls, sink: str | None = None) -> int:
        """Level of a sink on the 0-3 scale; min of both sinks when ``None``.

        The ``None`` form is what MATLAB caches to gate calls client-side:
        a message suppressed by both sinks never needs to cross the bridge.
        """
        if sink == "console":
            return cls._from_py_level(cls._console_py_level)
        if sink == "file":
            return cls._from_py_level(cls._file_py_level)
        return cls._from_py_level(min(cls._console_py_level, cls._file_py_level))

    # -- emission ---------------------------------------------------------

    @classmethod
    def debug(
        cls, msg: str, *args, layer: str = "scidb", exc_info: bool = False
    ) -> None:
        """Log a message at DEBUG level."""
        cls._log(logging.DEBUG, msg, args, layer, exc_info)

    @classmethod
    def info(
        cls, msg: str, *args, layer: str = "scidb", exc_info: bool = False
    ) -> None:
        """Log a message at INFO level."""
        cls._log(logging.INFO, msg, args, layer, exc_info)

    @classmethod
    def warn(
        cls, msg: str, *args, layer: str = "scidb", exc_info: bool = False
    ) -> None:
        """Log a message at WARN level."""
        cls._log(logging.WARNING, msg, args, layer, exc_info)

    @classmethod
    def error(
        cls, msg: str, *args, layer: str = "scidb", exc_info: bool = False
    ) -> None:
        """Log a message at ERROR level."""
        cls._log(logging.ERROR, msg, args, layer, exc_info)

    # -- instrumentation ----------------------------------------------------

    @classmethod
    @contextmanager
    def step(cls, name: str, *, layer: str = "scidb"):
        """Trace one named operation: entry/exit at DEBUG, failure at ERROR.

        Names are snake_case operations (``resolve_empty_lists``,
        ``delegate_to_scifor``) — never numbered steps; ordering is conveyed
        by the log's own sequence. At the default (INFO) level these are
        silent; at DEBUG the file reads as a nested execution trace with
        per-step durations. An exception is logged with its traceback and
        duration, then re-raised — so a failed run's log always ends with
        the error in context.

        Usage::

            with Log.step("save_batch(PSD)", layer="scidb"):
                ...
        """
        cls.debug(f"→ {name}", layer=layer)
        t0 = time.perf_counter()
        try:
            yield
        except Exception as e:
            cls.error(
                f"✗ {name} failed after {time.perf_counter() - t0:.3f}s: "
                f"{type(e).__name__}: {e}",
                layer=layer,
                exc_info=True,
            )
            raise
        cls.debug(f"← {name} done in {time.perf_counter() - t0:.3f}s", layer=layer)

    @classmethod
    @contextmanager
    def timer(
        cls,
        name: str,
        *,
        layer: str = "scidb",
        extra: str | None = None,
        live: bool = False,
    ):
        """Phase timing for a hot operation, emitted under the ``[timing]`` tag.

        Yields a :class:`_Timer`; wrap sub-phases with ``t.phase("name")``
        (snake_case, never numbered). On exit emits one INFO summary line —
        ``[timing] name: TOTAL=…s (phase=…s, …)`` — plus one DEBUG table
        line per phase. The ``[timing]`` prefix is a stable grep target
        (MATLAB timing-test archives rely on it); only the summary carries
        ``TOTAL=``, so a parser looking for totals is unaffected by ``live``.

        ``live=True`` additionally narrates each phase as it starts and ends,
        at INFO, and enables :meth:`_Timer.note`. Use it for the paths that can
        run for minutes (a full-resolution figure save), not for the ones that
        run in milliseconds — see :class:`_Timer`.

        Usage::

            with Log.timer("save_batch(PSD)", extra="114 items") as t:
                with t.phase("canonical_hash"): ...
                with t.phase("commit"): ...
        """
        t = _Timer(name=name, layer=layer, live=live)
        t0 = time.perf_counter()
        try:
            yield t
        finally:
            total = time.perf_counter() - t0
            parts = ", ".join(f"{p}={s:.3f}s" for p, s in t.phases)
            detail = f" ({parts})" if parts else ""
            prefix = f"{extra}, " if extra else ""
            cls.info(
                f"[timing] {name}: {prefix}TOTAL={total:.3f}s{detail}", layer=layer
            )
            for p, s in t.phases:
                cls.debug(f"  {name} {p:<30s} {s:.3f}s", layer=layer)

    # -- internals ---------------------------------------------------------

    @classmethod
    def _log(
        cls, py_level: int, msg: str, args: tuple, layer: str, exc_info: bool
    ) -> None:
        cls.attach()
        name = str(layer).split(".", 1)[0]
        if name not in LAYERS:
            with cls._lock:
                if name not in cls._unknown_layers:
                    cls._unknown_layers.add(name)
                    warnings.warn(
                        f"Unknown scistack layer '{layer}', logging as 'scidb'.",
                        UserWarning,
                        stacklevel=2,
                    )
            name = "scidb"
        logging.getLogger(name).log(py_level, msg, *args, exc_info=exc_info)

    @classmethod
    def _to_py_level(cls, level: int | str) -> int:
        if isinstance(level, str):
            name = level.upper()
            if name not in _NAME_TO_SCIDB:
                warnings.warn(
                    f"Unknown log level '{name}', defaulting to INFO.",
                    UserWarning,
                    stacklevel=2,
                )
                return logging.INFO
            return _SCIDB_TO_PY[_NAME_TO_SCIDB[name]]
        try:
            numeric = int(level)
        except (TypeError, ValueError):
            warnings.warn(
                f"Unknown log level {level!r}, defaulting to INFO.",
                UserWarning,
                stacklevel=2,
            )
            return logging.INFO
        if numeric not in _SCIDB_TO_PY:
            warnings.warn(
                f"Unknown log level {level!r}, defaulting to INFO.",
                UserWarning,
                stacklevel=2,
            )
            return logging.INFO
        return _SCIDB_TO_PY[numeric]

    @staticmethod
    def _from_py_level(py_level: int) -> int:
        if py_level <= logging.DEBUG:
            return Log.DEBUG
        if py_level <= logging.INFO:
            return Log.INFO
        if py_level <= logging.WARNING:
            return Log.WARN
        return Log.ERROR

    @classmethod
    def _reset_for_tests(cls) -> None:
        """Detach all handlers and reset state. Test isolation only."""
        with cls._lock:
            for handler in (cls._console_handler, cls._file_handler):
                if handler is not None:
                    for name in LAYERS:
                        logging.getLogger(name).removeHandler(handler)
                    handler.close()
            cls._console_handler = None
            cls._file_handler = None
            cls._path = None
            cls._attached = False
            cls._console_py_level = logging.INFO
            cls._file_py_level = logging.INFO
