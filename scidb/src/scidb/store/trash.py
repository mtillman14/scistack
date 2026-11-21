"""The undo trash of a variant delete (GitHub #26).

``docs/claude/deleting-entries-with-data.md``; plan
``.claude/plan-delete-entries-with-data.md`` stage 1.

``variant_delete.delete_variant`` used to DROP the rows it deleted. It now
saves every row it is about to delete as a Parquet file first, inside the same
transaction, so the delete can be undone (``variant_delete.restore_deleted``)
and redone (``variant_delete.delete_again``). This module owns only the
storage side:

* **where** the files go: ``<db>.trash/<tombstone_id>/`` beside the database
  (:func:`trash_root`);
* **writing** them (:class:`TrashWriter`) and **reading them back**
  (:func:`insert_files`);
* the **state** row per delete in ``_trash`` (``trashed`` / ``restored`` /
  ``purged``): :func:`write_state`, :func:`set_state`, :func:`entry`,
  :func:`entries`;
* **purging** (:func:`purge_trash`) and sweeping orphan folders
  (:func:`sweep_orphans`).

Which rows a delete means, the precondition of a restore and the order of the
writes stay with ``variant_delete``: one owner for executing a delete.

Why files, not mirror tables:

* the live tables are untouched by the trash, so no reader has a "deleted"
  flag to remember (``_record.excluded`` already means "hidden");
* Parquet keeps every DuckDB column type, and ``INSERT ... BY NAME`` restores
  into a table that gained a column since;
* the table registry forbids undeclared tables, and a mirror per
  ``<Type>_data`` table would be a second owner of every column list;
* purging frees the disk space by removing a folder.

A database with no file (``:memory:``) has no trash: its deletes stay hard, and
that is logged.
"""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from scidb.model.log import Log
from scidb.store.tables import TRASH

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

TRASH_TABLE = TRASH.name

STATE_TRASHED = "trashed"
STATE_RESTORED = "restored"
STATE_PURGED = "purged"

#: Ids per COPY statement. The ids are inlined as literals (DuckDB does not
#: bind parameters inside COPY), so a chunk bounds the statement's size.
_CHUNK = 5000


class TrashConflict(ValueError):
    """A restore or redo that cannot be applied; nothing was written.
    ``conflicts`` names each reason."""

    def __init__(self, message: str, conflicts: list[str]):
        super().__init__(message + (": " + "; ".join(conflicts) if conflicts else ""))
        self.conflicts = list(conflicts)


@dataclass
class TrashEntry:
    tombstone_id: str
    state: str
    trash_dir: str
    #: ``[{"table", "column", "file", "rows"}]`` in the order they were written.
    files: list[dict]
    record_ids: list[str]
    invocation_ids: list[str]
    run_ids: list[str]
    #: ``[{"variable", "selection"}]``: pins the delete released.
    released_pins: list[dict]
    fingerprint: str
    bytes: int
    updated_at: str


# ---------------------------------------------------------------------------
# Where
# ---------------------------------------------------------------------------


def trash_root(db: "DatabaseManager") -> Path | None:
    """``<db file>.trash``, or None for a database with no file."""
    path = getattr(db, "dataset_db_path", None)
    if path is None or str(path) in ("", ":memory:"):
        return None
    path = Path(path)
    return path.with_name(path.name + ".trash")


def _literal_list(ids) -> str:
    return ", ".join("'" + str(i).replace("'", "''") + "'" for i in ids)


def _path_literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


@dataclass
class TrashWriter:
    """Saves rows before they are deleted. One writer per delete; ``directory``
    None means no trash (a hard delete)."""

    duck: object
    directory: Path | None
    files: list[dict] = field(default_factory=list)
    _step: int = 0

    def save(self, table: str, column: str, ids) -> int:
        """Write the rows of ``table`` whose ``column`` is in ``ids`` to Parquet.
        Returns how many rows were saved. Call it right before deleting them."""
        ids = list(ids)
        self._step += 1
        if self.directory is None or not ids:
            return 0
        self.directory.mkdir(parents=True, exist_ok=True)
        saved = 0
        for chunk_no, start in enumerate(range(0, len(ids), _CHUNK)):
            chunk = ids[start : start + _CHUNK]
            where = f'"{column}" IN ({_literal_list(chunk)})'
            (n,) = self.duck.fetchone(f'SELECT COUNT(*) FROM "{table}" WHERE {where}')
            if not n:
                continue
            path = self.directory / f"{self._step:02d}_{chunk_no:03d}__{table}.parquet"
            self.duck.execute(
                f'COPY (SELECT * FROM "{table}" WHERE {where}) '
                f"TO {_path_literal(path)} (FORMAT PARQUET)"
            )
            self.files.append({"table": table, "column": column, "file": path.name, "rows": int(n)})
            saved += int(n)
        return saved

    @property
    def bytes(self) -> int:
        if self.directory is None:
            return 0
        return sum((self.directory / f["file"]).stat().st_size for f in self.files)

    def discard(self) -> None:
        """Remove what was written (the delete rolled back)."""
        if self.directory is not None and self.directory.exists():
            shutil.rmtree(self.directory, ignore_errors=True)
            Log.info(f"trash: discarded {self.directory} (the delete rolled back)")


# ---------------------------------------------------------------------------
# State rows
# ---------------------------------------------------------------------------


def write_state(
    duck,
    tombstone_id: str,
    writer: TrashWriter,
    *,
    record_ids: list[str],
    invocation_ids: list[str],
    run_ids: list[str],
    released_pins: list[dict],
    fingerprint: str,
) -> None:
    """Record a trashed delete (replacing the row of a redone one). The caller
    holds the transaction."""
    duck.execute(f"DELETE FROM {TRASH_TABLE} WHERE tombstone_id = ?", [tombstone_id])
    duck.execute(
        f"INSERT INTO {TRASH_TABLE} (tombstone_id, state, trash_dir, files_json, "
        "ids_json, pins_json, fingerprint, bytes, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            tombstone_id,
            STATE_TRASHED,
            str(writer.directory),
            json.dumps(writer.files),
            json.dumps(
                {"record_ids": record_ids, "invocation_ids": invocation_ids, "run_ids": run_ids}
            ),
            json.dumps(released_pins, sort_keys=True, default=str),
            fingerprint,
            writer.bytes,
            datetime.now().isoformat(),
        ],
    )


def set_state(duck, tombstone_id: str, state: str) -> None:
    duck.execute(
        f"UPDATE {TRASH_TABLE} SET state = ?, updated_at = ? WHERE tombstone_id = ?",
        [state, datetime.now().isoformat(), tombstone_id],
    )


def _entry_from_row(row) -> TrashEntry:
    tid, state, tdir, files, ids, pins, fingerprint, size, at = row
    ids = json.loads(ids)
    return TrashEntry(
        tombstone_id=tid,
        state=state,
        trash_dir=tdir,
        files=json.loads(files),
        record_ids=ids["record_ids"],
        invocation_ids=ids["invocation_ids"],
        run_ids=ids["run_ids"],
        released_pins=json.loads(pins),
        fingerprint=fingerprint,
        bytes=int(size),
        updated_at=at,
    )


_COLUMNS = (
    "tombstone_id, state, trash_dir, files_json, ids_json, pins_json, "
    "fingerprint, bytes, updated_at"
)


def entry(db: "DatabaseManager", tombstone_id: str) -> TrashEntry | None:
    duck = db._duck
    if not duck.table_exists(TRASH_TABLE):
        return None
    row = duck.fetchone(
        f"SELECT {_COLUMNS} FROM {TRASH_TABLE} WHERE tombstone_id = ?", [tombstone_id]
    )
    return _entry_from_row(row) if row else None


def entries(db: "DatabaseManager", state: str | None = None) -> list[TrashEntry]:
    """Every trash row, oldest first (only those in ``state`` when given)."""
    duck = db._duck
    if not duck.table_exists(TRASH_TABLE):
        return []
    rows = duck.fetchall(f"SELECT {_COLUMNS} FROM {TRASH_TABLE} ORDER BY updated_at")
    out = [_entry_from_row(r) for r in rows]
    return [e for e in out if state is None or e.state == state]


def states(db: "DatabaseManager") -> dict[str, str]:
    """``{tombstone_id: state}`` for every delete that had a trash."""
    duck = db._duck
    if not duck.table_exists(TRASH_TABLE):
        return {}
    return dict(duck.fetchall(f"SELECT tombstone_id, state FROM {TRASH_TABLE}"))


# ---------------------------------------------------------------------------
# Reading back
# ---------------------------------------------------------------------------


def file_paths(e: TrashEntry) -> list[Path]:
    return [Path(e.trash_dir) / f["file"] for f in e.files]


def missing_files(e: TrashEntry) -> list[str]:
    return [str(p) for p in file_paths(e) if not p.exists()]


def distinct_in_files(duck, e: TrashEntry, table: str, column: str) -> set:
    """Distinct ``column`` values over the trashed rows of ``table``."""
    paths = [Path(e.trash_dir) / f["file"] for f in e.files if f["table"] == table]
    if not paths:
        return set()
    files = "[" + ", ".join(_path_literal(p) for p in paths) + "]"
    return {v for (v,) in duck.fetchall(f'SELECT DISTINCT "{column}" FROM read_parquet({files})')}


def insert_files(duck, e: TrashEntry) -> dict[str, int]:
    """Insert every trashed row back into its live table, by column name.
    Returns ``{table: rows}``. The caller holds the transaction and has checked
    the precondition."""
    restored: dict[str, int] = {}
    for f in reversed(e.files):
        path = Path(e.trash_dir) / f["file"]
        duck.execute(
            f'INSERT INTO "{f["table"]}" BY NAME SELECT * FROM read_parquet({_path_literal(path)})'
        )
        restored[f["table"]] = restored.get(f["table"], 0) + int(f["rows"])
    return restored


def remove_dir(e: TrashEntry) -> None:
    path = Path(e.trash_dir)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# Purging
# ---------------------------------------------------------------------------


def purge_trash(
    db: "DatabaseManager",
    tombstone_ids: list[str] | None = None,
    *,
    older_than_days: float | None = None,
) -> list[str]:
    """Delete the files of trashed deletes for good (they can no longer be
    undone) and mark them ``purged``. With no arguments, every trashed delete.
    Returns the purged tombstone ids. Orphan folders are swept too."""
    t0 = time.perf_counter()
    duck = db._duck
    cutoff = (
        datetime.now() - timedelta(days=older_than_days) if older_than_days is not None else None
    )
    wanted = set(tombstone_ids) if tombstone_ids is not None else None
    purged: list[str] = []
    freed = 0
    for e in entries(db, STATE_TRASHED):
        if wanted is not None and e.tombstone_id not in wanted:
            continue
        if cutoff is not None and datetime.fromisoformat(e.updated_at) > cutoff:
            continue
        remove_dir(e)
        set_state(duck, e.tombstone_id, STATE_PURGED)
        purged.append(e.tombstone_id)
        freed += e.bytes
    swept = sweep_orphans(db)
    Log.info(
        f"trash: purged {len(purged)} delete(s) {purged}, freed {freed} bytes, "
        f"swept {len(swept)} orphan folder(s) ({(time.perf_counter() - t0) * 1000:.1f} ms)"
    )
    return purged


def sweep_orphans(db: "DatabaseManager") -> list[str]:
    """Remove trash folders no ``trashed`` row points at: left by a crash
    between writing the files and committing, or by a clear."""
    root = trash_root(db)
    if root is None or not root.exists():
        return []
    live = {Path(e.trash_dir).name for e in entries(db, STATE_TRASHED)}
    swept = []
    for child in root.iterdir():
        if child.is_dir() and child.name not in live:
            shutil.rmtree(child, ignore_errors=True)
            swept.append(child.name)
    if swept:
        Log.info(f"trash: swept orphan folder(s) {swept} under {root}")
    return swept
