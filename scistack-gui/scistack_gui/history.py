"""Undo/redo for the GUI: change records captured at the handler choke point.

Conceptual reference: ``docs/claude/undo-redo.md``. Plan:
``.claude/plan-undo-redo.md``.

Undo records **what state changed**, never how to reverse an operation, so
no feature writes inverse code. :func:`recording` wraps one undoable handler
call (``api/handlers.py:Handler.invoke``) and keeps:

* **rows** — every tracked DuckDB table (:func:`tracked_tables`, each owner
  module's ``UNDOABLE_TABLES``) is read before and after, and diffed on the
  table's primary key as DuckDB itself reports it;
* **files** — every project file the call wrote. Writers call
  :func:`note_write` *before* writing; the after-bytes are read when the
  recording closes.

:func:`undo` writes a record's before side back, :func:`redo` its after side.
Both are idempotent (a record is ``applied`` or ``undone``; asking for the
state it is already in is a ``noop``) and both refuse — writing nothing —
when any touched row or file no longer holds the value the record left
there (``conflict``).

This module is storage-agnostic about meaning: it never interprets a row.
"""

from __future__ import annotations

import contextvars
import logging
import os
import tempfile
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: How many change records are kept; the oldest are forgotten first.
MAX_RECORDS = 500

#: Serialises undoable handlers and undo/redo, so no recording's before/after
#: window ever contains another undoable call's writes. Reentrant because an
#: undoable service may call another undoable service in-process.
_lock = threading.RLock()

_records: "OrderedDict[str, ChangeRecord]" = OrderedDict()

#: Primary-key columns per table, read once from DuckDB (schemas never change
#: in place: no migrations).
_pk_cache: dict[str, tuple[str, ...]] = {}

_active: contextvars.ContextVar["_Recording | None"] = contextvars.ContextVar(
    "scistack_history_recording", default=None
)


# ---------------------------------------------------------------------------
# What is tracked
# ---------------------------------------------------------------------------


def tracked_tables() -> tuple[str, ...]:
    """Every undoable GUI table, from the module that creates it.

    Each owner lists its own tables beside its ``CREATE TABLE``; this module
    only reads the lists (one owner per table). The guard test
    ``test_every_gui_table_is_tracked`` fails when a new table is missed.
    """
    from scistack_gui import intent_store, layout, node_wiring, pipeline_store

    return (
        *pipeline_store.UNDOABLE_TABLES,
        *layout.UNDOABLE_TABLES,
        *intent_store.UNDOABLE_TABLES,
        *node_wiring.UNDOABLE_TABLES,
    )


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

Row = tuple
#: ``{table: {key: (before_row | None, after_row | None)}}``
RowChanges = dict[str, dict[tuple, tuple["Row | None", "Row | None"]]]


@dataclass
class FileChange:
    before: "bytes | None"
    after: "bytes | None"
    #: Whether restoring this file changes what the registries discover
    #: (source, config) — False for the layout file, which is cosmetic.
    reload: bool = True


@dataclass
class ChangeRecord:
    change_id: str
    label: str
    method: str
    columns: dict[str, tuple[str, ...]] = field(default_factory=dict)
    keys: dict[str, tuple[str, ...]] = field(default_factory=dict)
    rows: RowChanges = field(default_factory=dict)
    files: dict[str, FileChange] = field(default_factory=dict)
    state: str = "applied"  # "applied" | "undone"
    created_at: float = field(default_factory=time.time)
    #: The client's undo stack ("page", "plot"), so a restarted surface seeds
    #: its own stack only (:func:`recent_changes`). Empty: not known (a record
    #: from before 2026-10-22, a script, a test).
    surface: str = ""

    @property
    def empty(self) -> bool:
        return not any(self.rows.values()) and not self.files

    def summary(self) -> str:
        n_rows = sum(len(v) for v in self.rows.values())
        tables = ",".join(sorted(t for t, v in self.rows.items() if v)) or "-"
        files = ",".join(Path(p).name for p in sorted(self.files)) or "-"
        return f"rows={n_rows} tables={tables} files={files}"


@dataclass
class _Recording:
    change_id: str
    #: path -> (before bytes | None, reload)
    files: dict[str, tuple["bytes | None", bool]] = field(default_factory=dict)


def _read_bytes(path: str) -> "bytes | None":
    try:
        return Path(path).read_bytes()
    except FileNotFoundError:
        return None


def note_write(path: "str | Path", *, reload: bool = True) -> None:
    """Call immediately BEFORE writing *path* (create or overwrite).

    Inside an undoable handler this records the file's current bytes the
    first time it is touched; outside one it does nothing. Every project
    file write in ``scistack_gui`` must call this (guard test
    ``test_every_file_write_notes_history``). ``reload=False`` marks a file
    whose restoration needs no registry reload (the layout positions).
    """
    rec = _active.get()
    if rec is None:
        return
    key = str(Path(path).resolve())
    if key not in rec.files:
        rec.files[key] = (_read_bytes(key), reload)


def get_record(change_id: str) -> "ChangeRecord | None":
    with _lock:
        _ensure_loaded()
        return _records.get(change_id)


def clear() -> None:
    """Forget every record held in memory (tests; a database switch). The
    persisted log is untouched: the next read reloads from it."""
    global _loaded_for
    with _lock:
        _records.clear()
        _pk_cache.clear()
        _loaded_for = None


# ---------------------------------------------------------------------------
# The persisted change log (plan-canvas-as-document Q5)
# ---------------------------------------------------------------------------
#
# Every non-empty record is also written to ``_canvas_change`` in the
# project's .duckdb, so (a) there is an append-only audit trail of GUI edits
# that outlives the process, and (b) undo/redo of a change made before a
# restart works: the first read after a restart reloads the newest
# MAX_RECORDS. Replaying an old record is safe for the same reason replaying a
# recent one is: `_conflicts` refuses when any touched row or file no longer
# holds what the record left there (a script run in between, say).
#
# The table is NOT itself undoable (it is the undo log) and is never pruned:
# memory holds MAX_RECORDS, the table holds everything. No user identity
# (feedback_no_user_identity).

AUDIT_TABLE = "_canvas_change"

#: GUI tables deliberately NOT in ``tracked_tables`` (guard:
#: ``test_every_gui_table_is_tracked``), and why.
UNTRACKED_TABLES: dict[str, str] = {
    AUDIT_TABLE: "the undo log itself",
    "_adopted": "canvas adoption markers: written by graph builds, never by an undoable edit",
}

#: How the table travels on export (see ``pipeline_store.PORTABILITY``).
PORTABILITY: dict[str, str] = {AUDIT_TABLE: "history"}

#: The database path the in-memory records were loaded for (None: not yet).
_loaded_for: "str | None" = None


def ensure_tables(db) -> None:
    """Create ``_canvas_change`` if absent (from ``pipeline_store._ensure_tables``)."""
    db._duck._execute("""
        CREATE TABLE IF NOT EXISTS _canvas_change (
            change_id   VARCHAR PRIMARY KEY,
            seq         BIGINT NOT NULL,
            created_at  DOUBLE NOT NULL,
            updated_at  DOUBLE NOT NULL,
            label       VARCHAR NOT NULL,
            method      VARCHAR NOT NULL,
            state       VARCHAR NOT NULL,
            summary     VARCHAR NOT NULL,
            record_json VARCHAR NOT NULL
        )
    """)


def _enc(value):
    """A row value as JSON, keeping the types DuckDB hands back."""
    import base64
    import datetime as _dt
    import decimal

    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return {"__t": type(value).__name__, "v": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"__t": "decimal", "v": str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {"__t": "bytes", "v": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return {"__t": "list", "v": [_enc(v) for v in value]}
    if isinstance(value, dict):
        return {"__t": "dict", "v": {k: _enc(v) for k, v in value.items()}}
    return value


def _dec(value):
    import base64
    import datetime as _dt
    import decimal

    if not isinstance(value, dict) or "__t" not in value:
        return value
    t, v = value["__t"], value["v"]
    if t == "datetime":
        return _dt.datetime.fromisoformat(v)
    if t == "date":
        return _dt.date.fromisoformat(v)
    if t == "time":
        return _dt.time.fromisoformat(v)
    if t == "decimal":
        return decimal.Decimal(v)
    if t == "bytes":
        return base64.b64decode(v)
    if t == "list":
        return [_dec(x) for x in v]
    if t == "dict":
        return {k: _dec(x) for k, x in v.items()}
    raise ValueError(f"unknown encoded type {t!r}")


def _row(values) -> "Row | None":
    return None if values is None else tuple(_dec(v) for v in values)


def record_to_json(rec: ChangeRecord) -> str:
    import base64
    import json

    def b64(b):
        return None if b is None else base64.b64encode(b).decode("ascii")

    return json.dumps(
        {
            "columns": {t: list(c) for t, c in rec.columns.items()},
            "keys": {t: list(c) for t, c in rec.keys.items()},
            "rows": {
                table: [
                    [[_enc(k) for k in key],
                     None if b is None else [_enc(v) for v in b],
                     None if a is None else [_enc(v) for v in a]]
                    for key, (b, a) in diff.items()
                ]
                for table, diff in rec.rows.items()
            },
            "files": {
                path: {"before": b64(f.before), "after": b64(f.after), "reload": f.reload}
                for path, f in rec.files.items()
            },
            "surface": rec.surface,
        }
    )


def record_from_json(
    change_id: str, label: str, method: str, state: str, created_at: float, text: str
) -> ChangeRecord:
    import base64
    import json

    data = json.loads(text)

    def unb64(s):
        return None if s is None else base64.b64decode(s)

    return ChangeRecord(
        change_id=change_id,
        label=label,
        method=method,
        columns={t: tuple(c) for t, c in data["columns"].items()},
        keys={t: tuple(c) for t, c in data["keys"].items()},
        rows={
            table: {tuple(_dec(k) for k in key): (_row(b), _row(a)) for key, b, a in entries}
            for table, entries in data["rows"].items()
        },
        files={
            path: FileChange(unb64(f["before"]), unb64(f["after"]), bool(f["reload"]))
            for path, f in data["files"].items()
        },
        state=state,
        created_at=created_at,
        surface=str(data.get("surface") or ""),
    )


def _db_key() -> "str | None":
    from scistack_gui.db import get_db_path, is_loaded

    if not is_loaded():
        return None
    return str(get_db_path())


def _ensure_loaded() -> None:
    """Load the newest persisted records once per open database. Never raises:
    an unreadable log costs undo of old changes, not the edit at hand."""
    global _loaded_for
    key = _db_key()
    if key is None or _loaded_for == key:
        return
    _loaded_for = key
    from scistack_gui.db import db_connection

    t0 = time.monotonic()
    try:
        with db_connection("history load"):
            duck = _duck()
            if not duck._table_exists(AUDIT_TABLE):
                return
            rows = duck._fetchall(
                f"SELECT change_id, label, method, state, created_at, record_json "
                f"FROM {AUDIT_TABLE} ORDER BY seq DESC LIMIT ?",
                [MAX_RECORDS],
            )
    except Exception:
        logger.exception("[history] loading the persisted change log failed")
        return
    loaded = 0
    for change_id, label, method, state, created_at, text in reversed(rows):
        if change_id in _records or state == EVENT_STATE:
            # An event (adoption) is audit only: nothing to undo.
            continue
        try:
            _records[change_id] = record_from_json(
                change_id, label, method, state, created_at, text
            )
            loaded += 1
        except Exception:
            logger.exception("[history] persisted change %s is unreadable; skipped", change_id)
    # Records made in this process before the load stay newest.
    for change_id in [c for c in _records if c not in {r[0] for r in rows}]:
        _records.move_to_end(change_id)
    logger.info(
        "[history] loaded %d persisted change(s) for %s (%.1fms)",
        loaded,
        key,
        (time.monotonic() - t0) * 1000,
    )


def _persist(rec: ChangeRecord) -> None:
    """Write *rec* (insert or update) to the log. Never raises."""
    if rec.empty or _db_key() is None:
        return
    from scistack_gui.db import DatabaseLockedError, db_connection, get_db

    t0 = time.monotonic()
    try:
        with db_connection("history persist"):
            duck = _duck()
            if not duck._table_exists(AUDIT_TABLE):
                ensure_tables(get_db())
            now = time.time()
            duck._execute(
                f"INSERT INTO {AUDIT_TABLE} (change_id, seq, created_at, updated_at, "
                "label, method, state, summary, record_json) "
                f"SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ?, ?, ?, ?, ?, ? FROM {AUDIT_TABLE} "
                "ON CONFLICT (change_id) DO UPDATE SET updated_at = excluded.updated_at, "
                "label = excluded.label, state = excluded.state, "
                "summary = excluded.summary, record_json = excluded.record_json",
                [
                    rec.change_id,
                    rec.created_at,
                    now,
                    rec.label,
                    rec.method,
                    rec.state,
                    rec.summary(),
                    record_to_json(rec),
                ],
            )
    except DatabaseLockedError as exc:
        logger.warning("[history] change %s not persisted — database locked (%s)", rec.change_id, exc)
        return
    except Exception:
        logger.exception("[history] persisting change %s failed", rec.change_id)
        return
    logger.debug(
        "[history] persisted change %s state=%s (%.1fms)",
        rec.change_id,
        rec.state,
        (time.monotonic() - t0) * 1000,
    )


def list_changes(limit: int = 50) -> list[dict]:
    """The persisted audit trail, newest first:
    ``[{change_id, created_at, updated_at, label, method, state, summary}]``."""
    from scistack_gui.db import db_connection

    if _db_key() is None:
        return []
    with db_connection("history list"):
        duck = _duck()
        if not duck._table_exists(AUDIT_TABLE):
            return []
        rows = duck._fetchall(
            f"SELECT change_id, created_at, updated_at, label, method, state, summary "
            f"FROM {AUDIT_TABLE} ORDER BY seq DESC LIMIT ?",
            [int(limit)],
        )
    keys = ("change_id", "created_at", "updated_at", "label", "method", "state", "summary")
    return [dict(zip(keys, r)) for r in rows]


def recent_changes(surface: str, limit: int = 200) -> dict:
    """What a restarted *surface* should put back on its undo stack:
    ``{"past": [{change_id, label}], "future": [...]}``, each oldest first
    (the frontend's stack order: the last element is the next undo / redo).

    Plan-canvas-as-document Q5: undo survives a restart. The records are the
    persisted ``_canvas_change`` rows (never events); a record is replayed
    exactly as one made before the restart would be, and ``undo`` refuses on
    a conflict, so seeding is safe even if something changed in between.

    * **past**: records in state ``applied``, in the order they were made.
    * **future**: records in state ``undone`` that are still redoable: undone
      AFTER the newest applied record was made. An undone record followed by
      a new edit was dropped from the redo side when that edit was made, and
      stays dropped.

    Only records whose surface is *surface* are returned. A record from
    before surfaces were recorded (2026-10-22) has none and is left out: it
    may belong to another stack.
    """
    import json

    from scistack_gui.db import db_connection

    out: dict = {"past": [], "future": []}
    if _db_key() is None:
        return out
    with db_connection("history recent"):
        duck = _duck()
        if not duck._table_exists(AUDIT_TABLE):
            return out
        rows = duck._fetchall(
            f"SELECT change_id, label, state, created_at, updated_at, record_json "
            f"FROM {AUDIT_TABLE} WHERE state != ? ORDER BY seq DESC LIMIT ?",
            [EVENT_STATE, int(limit) * 4],
        )
    mine = []
    for change_id, label, state, created_at, updated_at, text in rows:
        try:
            rec_surface = (json.loads(text) or {}).get("surface") or ""
        except ValueError:
            continue
        if rec_surface == surface:
            mine.append((change_id, label, state, created_at, updated_at))
    mine.reverse()  # oldest first
    applied = [m for m in mine if m[2] == "applied"]
    newest_applied = max((m[3] for m in applied), default=0.0)
    out["past"] = [{"change_id": m[0], "label": m[1]} for m in applied][-int(limit):]
    undone = sorted(
        (m for m in mine if m[2] == "undone" and m[4] > newest_applied), key=lambda m: m[4]
    )
    out["future"] = [{"change_id": m[0], "label": m[1]} for m in undone][-int(limit):]
    logger.info(
        "[history] recent_changes surface=%s: %d to undo, %d to redo (of %d persisted for it)",
        surface,
        len(out["past"]),
        len(out["future"]),
        len(mine),
    )
    return out


#: ``_canvas_change.state`` of a row that records something the GUI did on
#: its own (canvas adoption) rather than an undoable edit. Never loaded as an
#: undo record (``_ensure_loaded``).
EVENT_STATE = "event"


def is_recording() -> bool:
    """Whether an undoable handler's recording window is open in this context.

    Canvas adoption checks this: a write made inside the window would become
    part of the user's undo step, and undoing that step would then remove the
    adopted rows while the adoption marker says they were adopted, so they
    would never come back (plan-canvas-as-document §8b: adoption is not an
    undo step)."""
    return _active.get() is not None


def log_event(db, label: str, method: str, summary: str) -> "str | None":
    """Append an audit-only row to ``_canvas_change`` (state
    :data:`EVENT_STATE`) and return its id. Never raises: losing an audit line
    must not fail the graph build that wrote it."""
    import uuid

    change_id = f"{method}-{uuid.uuid4().hex[:12]}"
    empty = ChangeRecord(change_id, label, method)
    try:
        duck = db._duck
        if not duck._table_exists(AUDIT_TABLE):
            ensure_tables(db)
        now = time.time()
        duck._execute(
            f"INSERT INTO {AUDIT_TABLE} (change_id, seq, created_at, updated_at, "
            "label, method, state, summary, record_json) "
            f"SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ?, ?, ?, ?, ?, ? FROM {AUDIT_TABLE}",
            [change_id, now, now, label, method, EVENT_STATE, summary, record_to_json(empty)],
        )
    except Exception:
        logger.exception("[history] audit event %s (%s) not written", method, label)
        return None
    logger.info("[history] audit event id=%s method=%s %s", change_id, method, summary)
    return change_id


# ---------------------------------------------------------------------------
# Snapshots and diffs
# ---------------------------------------------------------------------------


def _duck():
    """The open SciDuck, or None when this process has no database."""
    from scistack_gui.db import get_db, is_loaded

    if not is_loaded():
        return None
    return get_db()._duck


def _pk_columns(duck, table: str, columns: tuple[str, ...]) -> tuple[str, ...]:
    """The table's primary key as DuckDB reports it; all columns when it has
    none (the row itself is then its identity)."""
    if table not in _pk_cache:
        rows = duck._fetchall(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
            [table],
        )
        _pk_cache[table] = tuple(rows[0][0]) if rows else ()
    return _pk_cache[table] or columns


@dataclass
class _TableSnap:
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    rows: dict[tuple, Row]


def _snapshot(duck, tables: "tuple[str, ...] | None" = None) -> dict[str, _TableSnap]:
    existing = {
        r[0]
        for r in duck._fetchall(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
        )
    }
    out: dict[str, _TableSnap] = {}
    for table in tables or tracked_tables():
        if table not in existing:
            continue
        cols, rows = duck._fetch_table(f'SELECT * FROM "{table}"')
        cols = tuple(cols)
        key_cols = _pk_columns(duck, table, cols)
        idx = [cols.index(c) for c in key_cols]
        out[table] = _TableSnap(
            cols, key_cols, {tuple(r[i] for i in idx): tuple(r) for r in rows}
        )
    return out


def _snapshot_guarded(label: str) -> "dict[str, _TableSnap] | None":
    """Snapshot under the connection policy; None when there is no database
    or it cannot be had (MATLAB holds it) — the record then covers files
    only, and says so in the log."""
    from scistack_gui.db import DatabaseLockedError, db_connection, is_loaded

    if not is_loaded():
        return None
    try:
        with db_connection(f"history {label}"):
            return _snapshot(_duck())
    except DatabaseLockedError as exc:
        logger.warning(
            "[history] %s snapshot skipped — database locked (%s); this "
            "change's table rows will not be undoable",
            label,
            exc,
        )
        return None
    except Exception:
        # Undo is never worth failing the edit itself over.
        logger.exception("[history] %s snapshot failed; rows not recorded", label)
        return None


def _diff(
    before: dict[str, _TableSnap], after: dict[str, _TableSnap]
) -> tuple[RowChanges, dict[str, tuple[str, ...]], dict[str, tuple[str, ...]]]:
    changes: RowChanges = {}
    columns: dict[str, tuple[str, ...]] = {}
    keys: dict[str, tuple[str, ...]] = {}
    for table in set(before) | set(after):
        b = before.get(table)
        a = after.get(table)
        snap = a or b
        assert snap is not None
        b_rows = b.rows if b else {}
        a_rows = a.rows if a else {}
        diff = {
            k: (b_rows.get(k), a_rows.get(k))
            for k in set(b_rows) | set(a_rows)
            if b_rows.get(k) != a_rows.get(k)
        }
        if diff:
            changes[table] = diff
            columns[table] = snap.columns
            keys[table] = snap.key_columns
    return changes, columns, keys


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------


@contextmanager
def exclusive():
    """Hold the history lock without recording — for an undoable handler
    called with no change id, so it still cannot land inside another
    call's recording window."""
    with _lock:
        yield


@contextmanager
def recording(change_id: str, *, label: str, method: str, surface: "str | None" = None):
    """Record one undoable call under *change_id*.

    Nothing is stored if the block raises. A second recording under the same
    id MERGES into the first (earliest before, latest after), which is how a
    multi-request gesture becomes one undo step and why a retried request is
    harmless.
    """
    with _lock:
        t0 = time.monotonic()
        before = _snapshot_guarded("before")
        t_before = time.monotonic() - t0
        rec = _Recording(change_id)
        token = _active.set(rec)
        try:
            yield
        finally:
            _active.reset(token)
        t1 = time.monotonic()
        after = _snapshot_guarded("after") if before is not None else None
        t_after = time.monotonic() - t1
        if before is not None and after is not None:
            rows, columns, keys = _diff(before, after)
        else:
            rows, columns, keys = {}, {}, {}
        files = {
            path: FileChange(b, _read_bytes(path), reload)
            for path, (b, reload) in rec.files.items()
        }
        files = {p: f for p, f in files.items() if f.before != f.after}
        new = ChangeRecord(change_id, label, method, columns, keys, rows, files, surface=surface or "")
        stored = _store(new)
        _persist(stored)
        logger.info(
            "[history] history_record id=%s method=%s label=%r %s merged=%s "
            "snapshot_ms=%.1f+%.1f",
            change_id,
            method,
            label,
            stored.summary(),
            stored is not new,
            t_before * 1000,
            t_after * 1000,
        )


def _store(new: ChangeRecord) -> ChangeRecord:
    _ensure_loaded()
    old = _records.get(new.change_id)
    if old is None or old.state != "applied":
        if old is not None:
            logger.warning(
                "[history] change %s re-recorded after it was undone — the "
                "old record is replaced",
                new.change_id,
            )
        _records[new.change_id] = new
        _records.move_to_end(new.change_id)
        while len(_records) > MAX_RECORDS:
            gone, _ = _records.popitem(last=False)
            logger.debug("[history] forgot oldest record %s", gone)
        return new
    # Merge: keep the earliest before, take the latest after.
    old.surface = old.surface or new.surface
    for table, diff in new.rows.items():
        merged = old.rows.setdefault(table, {})
        old.columns.setdefault(table, new.columns[table])
        old.keys.setdefault(table, new.keys[table])
        for key, (b, a) in diff.items():
            first_before = merged[key][0] if key in merged else b
            merged[key] = (first_before, a)
    for table in list(old.rows):
        old.rows[table] = {k: v for k, v in old.rows[table].items() if v[0] != v[1]}
        if not old.rows[table]:
            del old.rows[table]
    for path, f in new.files.items():
        prev = old.files.get(path)
        old.files[path] = FileChange(prev.before if prev else f.before, f.after, f.reload)
    old.files = {p: f for p, f in old.files.items() if f.before != f.after}
    _records.move_to_end(old.change_id)
    return old


# ---------------------------------------------------------------------------
# Undo / redo
# ---------------------------------------------------------------------------


def undo(change_id: str) -> dict:
    """Restore the before side of *change_id*. Idempotent."""
    return _apply(change_id, "undo")


def redo(change_id: str) -> dict:
    """Restore the after side of *change_id*. Idempotent."""
    return _apply(change_id, "redo")


def _side(pair: tuple, direction: str, *, target: bool):
    """The value a row/file must hold now (``target=False``) or will hold
    after the operation (``target=True``)."""
    before, after = pair
    if direction == "undo":
        return before if target else after
    return after if target else before


def _apply(change_id: str, direction: str) -> dict:
    goal = "undone" if direction == "undo" else "applied"
    with _lock:
        _ensure_loaded()
        rec = _records.get(change_id)
        if rec is None:
            logger.info("[history] %s id=%s -> unknown", direction, change_id)
            return {"status": "unknown", "change_id": change_id}
        base = {"change_id": change_id, "label": rec.label, "method": rec.method}
        if rec.state == goal:
            logger.info("[history] %s id=%s -> noop (already %s)", direction, change_id, goal)
            return {**base, "status": "noop"}
        if rec.empty:
            rec.state = goal
            _persist(rec)
            logger.info("[history] %s id=%s -> empty", direction, change_id)
            return {**base, "status": "empty"}

        t0 = time.monotonic()
        conflicts = _conflicts(rec, direction)
        if conflicts:
            logger.warning(
                "[history] %s id=%s method=%s -> conflict, nothing written: %s",
                direction,
                change_id,
                rec.method,
                "; ".join(conflicts),
            )
            return {**base, "status": "conflict", "conflicts": conflicts}

        if rec.rows:
            _write_rows(rec, direction)
        try:
            _write_files(rec, direction)
        except Exception:
            logger.exception(
                "[history] %s id=%s: a file write failed — putting the rows back",
                direction,
                change_id,
            )
            if rec.rows:
                _write_rows(rec, "redo" if direction == "undo" else "undo")
            raise
        rec.state = goal
        _persist(rec)
        reload = any(f.reload for f in rec.files.values())
        logger.info(
            "[history] %s id=%s method=%s -> ok %s reload=%s (%.1fms)",
            direction,
            change_id,
            rec.method,
            rec.summary(),
            reload,
            (time.monotonic() - t0) * 1000,
        )
        return {
            **base,
            "status": "ok",
            "reload": reload,
            "files": sorted(rec.files),
        }


def _describe_key(rec: ChangeRecord, table: str, key: tuple) -> str:
    cols = rec.keys.get(table, ())
    if len(cols) == len(key) and len(cols) <= 5:
        parts = ", ".join(f"{c}={v!r}" for c, v in zip(cols, key))
    else:
        parts = repr(key)[:120]
    return f"{table}({parts})"


def _conflicts(rec: ChangeRecord, direction: str) -> list[str]:
    out: list[str] = []
    if rec.rows:
        from scistack_gui.db import db_connection, is_loaded

        if not is_loaded():
            return ["the database is not open"]
        # The connection is fetched INSIDE the hold: under the per-request
        # policy the manager's connection is closed between holds.
        with db_connection("history check"):
            now = _snapshot(_duck(), tuple(rec.rows))
        for table, diff in rec.rows.items():
            current = now[table].rows if table in now else {}
            for key, pair in diff.items():
                if current.get(key) != _side(pair, direction, target=False):
                    out.append(f"{_describe_key(rec, table, key)} changed since")
    for path, f in rec.files.items():
        if _read_bytes(path) != _side((f.before, f.after), direction, target=False):
            out.append(f"{path} was edited since")
    return out


def _write_rows(rec: ChangeRecord, direction: str) -> None:
    from scistack_gui.db import db_connection

    with db_connection("history write"):
        duck = _duck()
        duck._begin()
        try:
            for table, diff in rec.rows.items():
                cols = rec.columns[table]
                key_cols = rec.keys[table]
                # A table with no primary key is keyed on the whole row, so a
                # key's target is either absent (delete it) or a row that the
                # precondition proved absent now (plain insert). Never a
                # delete followed by a re-insert of the same key in one
                # transaction, which DuckDB's constraint checking can refuse.
                # (A PK of every column is the same case: no row is ever
                # modified in place, only added or removed.)
                has_pk = key_cols != cols
                col_sql = ", ".join(f'"{c}"' for c in cols)
                marks = ", ".join("?" for _ in cols)
                where = " AND ".join(f'"{c}" IS NOT DISTINCT FROM ?' for c in key_cols)
                for key, pair in diff.items():
                    target = _side(pair, direction, target=True)
                    if target is None:
                        duck._execute(f'DELETE FROM "{table}" WHERE {where}', list(key))
                        continue
                    verb = "INSERT OR REPLACE" if has_pk else "INSERT"
                    duck._execute(
                        f'{verb} INTO "{table}" ({col_sql}) VALUES ({marks})',
                        list(target),
                    )
            duck._commit()
        except Exception:
            try:
                duck._rollback()
            except Exception:
                logger.exception("[history] rollback failed")
            raise


def _write_files(rec: ChangeRecord, direction: str) -> None:
    for path, f in rec.files.items():
        target = _side((f.before, f.after), direction, target=True)
        p = Path(path)
        if target is None:
            p.unlink(missing_ok=True)
            continue
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=f".{p.name}.")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(target)
            os.replace(tmp, p)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise


def status() -> dict[str, Any]:
    """Counts for diagnostics."""
    with _lock:
        return {
            "records": len(_records),
            "max_records": MAX_RECORDS,
            "undone": sum(1 for r in _records.values() if r.state == "undone"),
        }
