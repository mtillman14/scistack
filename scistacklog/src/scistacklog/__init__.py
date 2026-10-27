"""scistacklog — the shared logging facade for every scistack layer.

Two audiences, three destinations:

- **journal** (``scistack.log`` beside the database, ``Log.set_path(...,
  journal_path=...)``): written for the scientist. One block per run (what
  ran, the counts, each failure labelled with who can fix it) and nothing
  about the framework's internals. Written only through :meth:`Log.journal`
  and :meth:`Log.run`; rendered by :mod:`scistacklog.journal`.
- **debug file** (``scistack-debug.log``, ``Log.set_path``): written for
  whoever debugs SciStack. Everything, at DEBUG by default, size-capped.
  Date + millisecond timestamps, level, originating layer, and ``run=<id>``
  on every line written inside a run (the id the journal block names).
- **console** (stderr): the narrative for the person watching. INFO by
  default; time-only timestamps, level shown only at WARN and above.

The debug file is a superset of the journal: every journal block is also
written to it (and to the console) as one INFO record. Levels per sink::

    Log.set_level("INFO", sink="file")      # a quieter debug file
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

import json
import logging
import os
import sys
import threading
import time
import uuid
import warnings
from contextlib import contextmanager
from contextvars import ContextVar

from . import journal as _journal

__all__ = ["Log", "LAYERS", "debug_path_for"]

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
    "scistackstats",
    "scistackstatsdb",
    "matlab",
)

#: Set inside :meth:`Log.trial`: the label its demoted records carry.
_TRIAL: ContextVar[str | None] = ContextVar("scistacklog_trial", default=None)

#: Set inside :meth:`Log.run`: the outermost run in progress on this context.
_RUN: ContextVar["JournalRun | None"] = ContextVar("scistacklog_run", default=None)

# The debug file's size cap: past DEBUG_MAX_BYTES it rolls to .1 … .N. A
# rollover that fails (another process holds the file — routine on Windows,
# where the GUI server and MATLAB share it) is given up for the session; the
# file keeps growing rather than spraying handler errors.
DEBUG_MAX_BYTES = 10 * 1024 * 1024
DEBUG_BACKUPS = 5
# How many records between size checks (a stat per record would cost more
# than the write).
_ROLLOVER_CHECK_EVERY = 500


def debug_path_for(journal_path) -> str:
    """``scistack-debug.log`` for ``scistack.log``: the one derivation of the
    debug file's name from the journal's (``<stem>-debug<suffix>``)."""
    root, ext = os.path.splitext(str(journal_path))
    return f"{root}-debug{ext or '.log'}"

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

_FILE_FORMAT = (
    "%(asctime)s.%(msecs)03d %(levelshort)-5s [%(layer)s]%(runtag)s %(message)s"
)
_FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"
_CONSOLE_DATEFMT = "%H:%M:%S"


class _RecordTagger(logging.Filter):
    """Adds the fields the formatters need: layer, short level name, and the
    ``run=<id>`` token of the run in progress (empty outside a run)."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.layer = record.name.split(".", 1)[0]
        record.levelshort = _SHORT_NAMES.get(record.levelno, record.levelname)
        if not hasattr(record, "runtag"):
            run = _RUN.get()
            record.runtag = f" run={run.run_id}" if run is not None else ""
        return True


class _CappedFileHandler(logging.FileHandler):
    """The debug file: a FileHandler that rolls itself over past a size cap.

    Not ``RotatingFileHandler``: that one retries (and reports) a failed
    rename on every record, and on Windows the rename fails whenever a second
    process (MATLAB, the sidecar) has the file open. Here a failed rollover is
    logged once and abandoned for the session.
    """

    def __init__(self, path: str) -> None:
        super().__init__(path, mode="a", encoding="utf-8", delay=True)
        self._since_check = 0
        self._rollover_disabled = False
        self.rollover_note: str | None = None
        self._rollover_if_large()

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self._since_check += 1
        if self._since_check >= _ROLLOVER_CHECK_EVERY:
            self._since_check = 0
            self._rollover_if_large()

    def _rollover_if_large(self) -> None:
        if self._rollover_disabled:
            return
        try:
            if os.path.getsize(self.baseFilename) < DEBUG_MAX_BYTES:
                return
        except OSError:
            return
        self.acquire()
        try:
            if self.stream is not None:
                self.stream.close()
                self.stream = None  # delay=True reopens on the next emit
            for i in range(DEBUG_BACKUPS - 1, 0, -1):
                src = f"{self.baseFilename}.{i}"
                if os.path.exists(src):
                    os.replace(src, f"{self.baseFilename}.{i + 1}")
            os.replace(self.baseFilename, f"{self.baseFilename}.1")
            self.rollover_note = (
                f"[log] rolled {self.baseFilename} over at "
                f"{DEBUG_MAX_BYTES // (1024 * 1024)} MB (keeping {DEBUG_BACKUPS})"
            )
        except OSError as e:
            self._rollover_disabled = True
            self.rollover_note = (
                f"[log] could not roll {self.baseFilename} over ({e}); "
                f"it keeps growing for this session"
            )
        finally:
            self.release()


class JournalRun:
    """One run's journal entry, collected while the run executes.

    Created by :meth:`Log.run` for the OUTERMOST run on a context; nested
    runs (scidb.for_each → scifor.for_each, EachOf alternatives) share it, so
    one user-visible call is one journal block. Emitted on exit only if
    something reached it — a summary or an error — so a dry run or a call
    that only registered a pipeline step writes nothing.
    """

    def __init__(self, name: str, *, language: str = "python") -> None:
        self.run_id = uuid.uuid4().hex[:8]
        self.name = name
        self.language = language
        self.started = time.time()
        self._t0 = time.perf_counter()
        self.summaries: list[dict] = []
        self.error: dict | None = None
        self.error_handled = False
        self.counters: dict[str, int] = {}

    def add_summary(self, summary: dict) -> None:
        """A loop's end-of-run summary (scifor's ``summary`` event shape)."""
        self.summaries.append(dict(summary))

    def fail(self, failure: "dict | None") -> None:
        """Record the error that ended the run (a ``ComboFailure.to_dict()``).

        The first call wins: the innermost handler knows the most. ``None``
        marks the exception as already accounted for (an ``on_error="stop"``
        failure is in the summary), so :meth:`Log.run` adds nothing.
        """
        if self.error_handled:
            return
        self.error_handled = True
        self.error = failure

    def count(self, key: str, n: int) -> None:
        """Add to a counter shown in the block (``saved``, ``up_to_date``)."""
        self.counters[key] = self.counters.get(key, 0) + int(n)

    def to_event(self) -> dict:
        totals = {"completed": 0, "failed": 0, "no_data": 0, "total": 0}
        groups: dict[str, dict] = {}
        cancelled = stopped = False
        for s in self.summaries:
            for k in totals:
                totals[k] += int(s.get(k) or 0)
            cancelled = cancelled or bool(s.get("cancelled"))
            stopped = stopped or bool(s.get("stopped"))
            for g in s.get("failures") or []:
                reason = g.get("reason")
                if reason in groups:
                    merged = groups[reason]
                    merged["count"] = int(merged.get("count") or 0) + int(g.get("count") or 0)
                    merged["combos"] = list(merged.get("combos") or []) + list(g.get("combos") or [])
                else:
                    groups[reason] = dict(g)
        return {
            "kind": "run",
            "name": self.name,
            "run_id": self.run_id,
            "language": self.language,
            "started": self.started,
            "elapsed": time.perf_counter() - self._t0,
            **totals,
            "up_to_date": self.counters.get("up_to_date", 0),
            "saved": self.counters.get("saved"),
            "cancelled": cancelled,
            "stopped": stopped,
            "failures": list(groups.values()),
            "error": self.error,
        }


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
    produces no summary at all — a 25-minute figure save (scistack.log 2026-09-11,
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

    Backed by stdlib logging: one stderr console handler and one debug-file
    handler, both attached to every layer logger in :data:`LAYERS`, plus the
    journal file, which only :meth:`journal` writes.
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
    _journal_path: str | None = None
    _journal_lock = threading.Lock()
    _notes_seen: set[str] = set()
    _console_py_level: int = logging.INFO
    # The debug file records everything unless told otherwise: a bug should
    # never need a second run with -v to be diagnosable.
    _file_py_level: int = logging.DEBUG
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
    def set_path(cls, log_path: str | None, journal_path: str | None = None) -> None:
        """Point the debug file at ``log_path`` and the journal at
        ``journal_path`` (``None`` detaches either).

        ``scidb.log.attach_log_file`` (called by ``configure_database``) sets
        both: ``scistack-debug.log`` and ``scistack.log`` beside the database.
        """
        cls.attach()
        with cls._lock:
            if cls._file_handler is not None:
                for name in LAYERS:
                    logging.getLogger(name).removeHandler(cls._file_handler)
                cls._file_handler.close()
                cls._file_handler = None
            cls._path = None if log_path is None else str(log_path)
            cls._journal_path = None if journal_path is None else str(journal_path)
            if cls._path is None:
                return
            handler = _CappedFileHandler(cls._path)
            handler.setLevel(cls._file_py_level)
            handler.setFormatter(logging.Formatter(_FILE_FORMAT, datefmt=_FILE_DATEFMT))
            handler.addFilter(cls._tagger)
            cls._file_handler = handler
            for name in LAYERS:
                logging.getLogger(name).addHandler(handler)
        if handler.rollover_note:
            cls.info(handler.rollover_note, layer="scistack")
            handler.rollover_note = None

    @classmethod
    def get_path(cls) -> str | None:
        """Current debug-file path (``None`` if the file sink is detached)."""
        return cls._path

    @classmethod
    def get_journal_path(cls) -> str | None:
        """Current journal path (``None`` if no journal is written)."""
        return cls._journal_path

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
    def trial(cls, label: str):
        """Demote INFO and WARN records to DEBUG while a trial runs.

        For work whose result is measured and then discarded — a layout tried
        at one candidate text size, say — whose own INFO/WARN lines would
        otherwise read as the real result's. Each demoted record keeps its
        original level in the text: ``[label] WARN: …``. ERROR is never
        demoted. Context-local (``contextvars``), so a trial on one thread
        does not quiet another; nested trials keep the innermost label.

        Usage::

            with Log.trial("auto-size 12pt"):
                fig = render(candidate)
        """
        token = _TRIAL.set(label)
        try:
            yield
        finally:
            _TRIAL.reset(token)

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

    @classmethod
    def timings(
        cls,
        name: str,
        phases,
        *,
        layer: str = "scidb",
        extra: str | None = None,
        total: float | None = None,
        top: int | None = None,
        total_key: str = "total",
    ) -> None:
        """Emit an ALREADY-COLLECTED phase mapping in :meth:`timer`'s format.

        Same output contract as ``timer`` — one INFO
        ``[timing] name: TOTAL=…s (phase=…s, …)`` line plus one DEBUG line per
        phase — for callers that accumulate their own ``timings`` dict rather
        than wrapping sub-phases in context managers. Hot paths that already
        hand-roll ``time.perf_counter()`` deltas (``save_batch``,
        ``record_run``) use this instead of being restructured, so there is
        still exactly ONE definition of the ``[timing]`` line shape.

        Args:
            phases: Mapping of phase name -> seconds, or an iterable of
                ``(name, seconds)`` pairs. Order is preserved for the DEBUG
                table; the INFO line is ordered by cost when ``top`` is set.
            total: The run total. Defaults to ``phases[total_key]`` if present,
                else the sum of all phases.
            top: Include only the ``top`` most expensive phases in the INFO
                line (every phase still gets its DEBUG line). Use it when a
                path has more phases than fit one readable line; the omitted
                ones are summarised as ``+N more``.
            total_key: Phase name to treat as the total rather than a phase.
        """
        items = list(phases.items() if hasattr(phases, "items") else phases)
        # The total is not itself a phase — pull it out so it is neither summed
        # into the parts nor printed as one.
        parts = [(p, s) for p, s in items if p != total_key]
        if total is None:
            declared = dict(items).get(total_key)
            total = declared if declared is not None else sum(s for _p, s in parts)

        shown = parts
        omitted = 0
        if top is not None and len(parts) > top:
            shown = sorted(parts, key=lambda kv: kv[1], reverse=True)[:top]
            omitted = len(parts) - len(shown)

        detail_parts = ", ".join(f"{p}={s:.3f}s" for p, s in shown)
        if omitted:
            detail_parts = f"{detail_parts}, +{omitted} more"
        detail = f" ({detail_parts})" if detail_parts else ""
        prefix = f"{extra}, " if extra else ""
        cls.info(f"[timing] {name}: {prefix}TOTAL={total:.3f}s{detail}", layer=layer)
        for p, s in parts:
            cls.debug(f"  {name} {p:<30s} {s:.3f}s", layer=layer)

    # -- journal ------------------------------------------------------------

    @classmethod
    @contextmanager
    def run(cls, name: str, *, language: str = "python"):
        """One user-visible run: a journal block, and a ``run=<id>`` token on
        every debug-file line written inside it.

        Only the OUTERMOST ``run`` on a context creates a :class:`JournalRun`;
        nested ones yield the same object, so ``scidb.for_each`` delegating to
        ``scifor.for_each`` (or an EachOf fanning out) is one block. The block
        is written on exit only if a summary or an error reached it.

        An exception that escapes without the caller having classified it
        (``JournalRun.fail``) is recorded by type and message only; the layers
        that know who can fix it (``scifor.failures.journal_run``) classify
        before it gets here.

        Usage::

            with Log.run("compute_psd") as run:
                ...
                run.add_summary(summary_event)
        """
        outer = _RUN.get()
        if outer is not None:
            yield outer
            return
        run = JournalRun(name, language=language)
        token = _RUN.set(run)
        cls.debug(f"[run] {name}: started (run={run.run_id})", layer="scistack")
        try:
            yield run
        except BaseException as e:
            if not run.error_handled:
                run.fail({"exc_type": type(e).__name__, "message": str(e), "blame": None})
            raise
        finally:
            try:
                if run.summaries or run.error is not None:
                    cls.journal(run.to_event())
                cls.debug(f"[run] {name}: ended (run={run.run_id})", layer="scistack")
            finally:
                _RUN.reset(token)

    @classmethod
    def current_run(cls) -> "JournalRun | None":
        """The run in progress on this context, or ``None``."""
        return _RUN.get()

    @classmethod
    def journal(cls, event: dict) -> str:
        """Write one event to the journal; return the rendered text.

        The ONLY writer of the journal file (an AST test pins that nothing
        else opens it). The same text goes to the debug file and the console
        as one INFO record tagged ``[journal]``, so the debug file stays a
        superset of the journal. A journal that cannot be written never fails
        the caller: the error goes to the debug file.
        """
        event = dict(event)
        if not event.get("run_id"):
            current = _RUN.get()
            event["run_id"] = current.run_id if current is not None else uuid.uuid4().hex[:8]
        event.setdefault("started", time.time())
        text = _journal.render(event)
        path = cls._journal_path
        if path is not None:
            try:
                with cls._journal_lock, open(path, "a", encoding="utf-8") as fh:
                    fh.write(text + "\n\n")
            except OSError as e:
                cls.error(f"[journal] could not write {path}: {e}", layer="scistack")
        cls.info("[journal]\n" + text, layer="scistack")
        return text

    @classmethod
    def journal_json(cls, event_json: str) -> str:
        """:meth:`journal` for a JSON-encoded event (the MATLAB bridge)."""
        try:
            event = json.loads(str(event_json))
        except ValueError as e:
            cls.error(f"[journal] unreadable event from MATLAB: {e}", layer="scistack")
            return ""
        if not isinstance(event, dict):
            cls.error(f"[journal] event is not an object: {str(event_json)[:200]}", layer="scistack")
            return ""
        return cls.journal(event)

    @classmethod
    def journal_note(cls, text: str, *, key: str | None = None) -> None:
        """A note about the data for the journal, written once per ``key``
        per process (``text`` when no key), so a warning a refresh repeats
        does not repeat in the journal."""
        dedupe = key or text
        with cls._lock:
            if dedupe in cls._notes_seen:
                return
            cls._notes_seen.add(dedupe)
        cls.journal({"kind": "note", "text": text})

    # -- internals ---------------------------------------------------------

    @classmethod
    def _log(
        cls, py_level: int, msg: str, args: tuple, layer: str, exc_info: bool
    ) -> None:
        cls.attach()
        trial = _TRIAL.get()
        if trial is not None and logging.DEBUG < py_level < logging.ERROR:
            # A trial's INFO and WARN describe a layout that is thrown away;
            # at their own level they would read as the real figure's.
            short = _SHORT_NAMES.get(py_level, logging.getLevelName(py_level))
            msg = f"[{trial}] {short}: {msg}"
            py_level = logging.DEBUG
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
            cls._journal_path = None
            cls._notes_seen = set()
            cls._attached = False
            cls._console_py_level = logging.INFO
            cls._file_py_level = logging.DEBUG
