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
    from scistack_gui import intent_store, node_wiring, pipeline_store

    return (
        *pipeline_store.UNDOABLE_TABLES,
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
        return _records.get(change_id)


def clear() -> None:
    """Forget every record (tests; a database switch)."""
    with _lock:
        _records.clear()
        _pk_cache.clear()


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
def recording(change_id: str, *, label: str, method: str):
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
        new = ChangeRecord(change_id, label, method, columns, keys, rows, files)
        stored = _store(new)
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
