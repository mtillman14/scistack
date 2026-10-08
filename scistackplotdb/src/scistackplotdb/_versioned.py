"""
Named, versioned, hideable rows: the store behind saved plots and presets.

Both are "a JSON envelope the user names, re-saves and removes", with the same
rules (docs/claude/saved-plots.md, docs/claude/plot-presets.md):

* **The id is the identity; the name is a label.** Rename is an UPDATE of
  ``name``. A new item reusing a removed item's name gets a new id.
* **Re-saving a visible name appends a version.** The newest version is what
  lists and opens. The version number is computed inside the INSERT, so two
  racing saves cannot claim the same one.
* **Nothing is ever deleted** (feedback_never_delete_mark_hidden): "remove"
  sets ``hidden`` on every version.
* **Reads never create the table.** Writes call :meth:`ensure_table`.
* All reads go through ``_fetchall``/``_fetchone`` (the fetch-locking guard).

This module owns those rules once, so the two stores cannot drift apart
(CLAUDE.md NOTE 4). A store with a *scope column* (saved plots: ``variable``)
keeps names unique per scope; a store without one (presets) keeps them unique
across the project. Callers turn the raw rows this returns into their own
info objects.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from scistacklog import Log

LAYER = "scistackplotdb"

#: Longest name accepted. A name is a list label, not a description.
MAX_NAME_LENGTH = 120

#: ``(item_id, scope, name, version, saved_at, hidden)``; scope is None for an
#: unscoped store.
Row = tuple


@dataclass(frozen=True)
class VersionedStore:
    """One append-only table of named, versioned envelopes."""

    table: str
    #: Column holding the item's identity (``plot_id``, ``preset_id``).
    id_column: str
    #: Column names are unique within, or None for project-wide names.
    scope_column: str | None
    #: What an item is called in messages ("saved plot", "preset").
    noun: str
    #: Log tag, without brackets.
    tag: str
    #: The error every refusal raises.
    error: type[Exception]

    # ---- table ----------------------------------------------------------

    def ensure_table(self, db) -> None:
        scope = f"{self.scope_column} VARCHAR NOT NULL," if self.scope_column else ""
        db._duck._execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                {self.id_column} VARCHAR NOT NULL,
                {scope}
                name          VARCHAR NOT NULL,
                version       INTEGER NOT NULL,
                saved_at      VARCHAR NOT NULL,
                hidden        BOOLEAN NOT NULL DEFAULT FALSE,
                envelope_json VARCHAR NOT NULL,
                PRIMARY KEY ({self.id_column}, version)
            )
        """)

    def table_exists(self, db) -> bool:
        row = db._duck._fetchone(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [self.table],
        )
        return bool(row and row[0])

    # ---- names ----------------------------------------------------------

    def check_name(self, name: Any) -> str:
        """*name* trimmed, or the store's error saying why it is not one."""
        if not isinstance(name, str):
            raise self.error(f"A {self.noun} needs a name.")
        name = " ".join(name.split())  # trim, and no tabs/newlines in a list label
        if not name:
            raise self.error(f"A {self.noun} needs a name.")
        if len(name) > MAX_NAME_LENGTH:
            raise self.error(
                f"A {self.noun}'s name is at most {MAX_NAME_LENGTH} characters "
                f"(this one is {len(name)})."
            )
        return name

    def clash_message(self, scope: str | None, name: str) -> str:
        if self.scope_column:
            return f"{scope} already has a {self.noun} named {name!r}."
        return f"There is already a {self.noun} named {name!r}."

    def describe(self, scope: str | None, name: str) -> str:
        """``StepLength / 'Fig 3'`` or ``'Session box'``, for logs."""
        return f"{scope} / {name!r}" if self.scope_column else repr(name)

    # ---- writes ---------------------------------------------------------

    def save(
        self,
        db,
        scope: str | None,
        name: str,
        envelope: dict,
        *,
        overwrite: bool,
        current_id: str | None,
        on_clash: Callable[[Row], Exception],
    ) -> tuple[Row, bool]:
        """Append *envelope* as a new version, returning ``(row, is_new)``.

        A visible item called *name* (in *scope*) gets a new version.
        Otherwise a new item is started. With ``overwrite=False``, appending
        to an item other than *current_id* raises ``on_clash(that row)`` so
        the caller can ask first.
        """
        try:
            text = json.dumps(envelope, sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise self.error(f"The settings are not storable as JSON: {exc}") from exc

        self.ensure_table(db)
        current = self.visible_by_name(db, scope, name)
        if current is not None and not overwrite and current[0] != current_id:
            Log.info(
                "[%s] save of %s refused: another %s (%s) has that name and "
                "overwrite was not confirmed",
                self.tag,
                self.describe(scope, name),
                self.noun,
                current[0][:8],
                layer=LAYER,
            )
            raise on_clash(current)
        item_id = current[0] if current else uuid.uuid4().hex
        saved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        scope_col = f"{self.scope_column}, " if self.scope_column else ""
        scope_val = [scope] if self.scope_column else []
        # One statement: the next version number is decided inside the insert,
        # so two saves racing on one item cannot both claim the same version.
        db._duck._execute(
            f"""
            INSERT INTO {self.table}
                ({self.id_column}, {scope_col}name, version, saved_at, hidden, envelope_json)
            SELECT ?, {"?, " if self.scope_column else ""}?,
                   COALESCE(MAX(version), 0) + 1, ?, FALSE, ?
            FROM {self.table} WHERE {self.id_column} = ?
            """,
            [item_id, *scope_val, name, saved_at, text, item_id],
        )
        row = self.latest(db, item_id)
        Log.info(
            "[%s] saved %s as version %d (%s, %d bytes, envelope format %r)",
            self.tag,
            self.describe(scope, name),
            row[3],
            f"new {self.noun}" if current is None else f"{self.noun} {item_id[:8]}",
            len(text),
            envelope.get("format"),
            layer=LAYER,
        )
        return row, current is None

    def rename(self, db, item_id: str, new_name: str) -> Row:
        new_name = self.check_name(new_name)
        row = self.require(db, item_id)
        clash = self.visible_by_name(db, row[1], new_name)
        if clash is not None and clash[0] != item_id:
            raise self.error(self.clash_message(row[1], new_name))
        db._duck._execute(
            f"UPDATE {self.table} SET name = ? WHERE {self.id_column} = ?",
            [new_name, item_id],
        )
        Log.info(
            "[%s] renamed %s -> %r (%s)",
            self.tag,
            self.describe(row[1], row[2]),
            new_name,
            item_id[:8],
            layer=LAYER,
        )
        return self.latest(db, item_id)

    def set_hidden(self, db, item_id: str, hidden: bool) -> Row:
        row = self.require(db, item_id)
        if not hidden:
            clash = self.visible_by_name(db, row[1], row[2])
            if clash is not None and clash[0] != item_id:
                raise self.error(
                    f"{self.clash_message(row[1], row[2])[:-1]}; rename one of them first."
                )
        db._duck._execute(
            f"UPDATE {self.table} SET hidden = ? WHERE {self.id_column} = ?",
            [bool(hidden), item_id],
        )
        Log.info(
            "[%s] %s %s (%s)",
            self.tag,
            "hid" if hidden else "unhid",
            self.describe(row[1], row[2]),
            item_id[:8],
            layer=LAYER,
        )
        return self.latest(db, item_id)

    # ---- reads ----------------------------------------------------------

    def _columns(self) -> str:
        scope = self.scope_column or "NULL"
        return f"{self.id_column}, {scope}, name, version, saved_at, hidden"

    def list_rows(
        self,
        db,
        scope: str | None = None,
        *,
        include_hidden: bool = False,
        with_envelope: bool = False,
    ) -> list[Row]:
        """The newest version of each item (in *scope*), newest first.

        With *with_envelope*, each row carries its envelope JSON as a 7th
        element, for a list that shows something stored inside it.
        """
        envelope = ", envelope_json" if with_envelope else ""
        if not self.table_exists(db):
            return []
        where = f"{self.scope_column} = ? AND " if self.scope_column else ""
        rows = db._duck._fetchall(
            f"""
            SELECT {self._columns()}{envelope}
            FROM {self.table} AS t
            WHERE {where}version = (
                SELECT MAX(version) FROM {self.table}
                WHERE {self.id_column} = t.{self.id_column}
            )
              {"" if include_hidden else "AND NOT hidden"}
            ORDER BY saved_at DESC, name
            """,
            [scope] if self.scope_column else [],
        )
        Log.debug(
            "[%s] %s: %d %s(s) listed%s",
            self.tag,
            scope if self.scope_column else "project",
            len(rows),
            self.noun,
            " (hidden included)" if include_hidden else "",
            layer=LAYER,
        )
        return list(rows)

    def history(self, db, item_id: str, *, with_envelope: bool = False) -> list[Row]:
        if not self.table_exists(db):
            return []
        envelope = ", envelope_json" if with_envelope else ""
        return list(db._duck._fetchall(
            f"""
            SELECT {self._columns()}{envelope}
            FROM {self.table} WHERE {self.id_column} = ? ORDER BY version DESC
            """,
            [item_id],
        ))

    def load(self, db, item_id: str, version: int | None = None) -> tuple[Row, str]:
        """``(row, envelope_json)`` of one version (the newest by default)."""
        if not self.table_exists(db):
            raise self.error(f"No {self.noun} {item_id!r}: nothing has been saved yet.")
        if version is None:
            row = db._duck._fetchone(
                f"""
                SELECT {self._columns()}, envelope_json
                FROM {self.table} WHERE {self.id_column} = ?
                ORDER BY version DESC LIMIT 1
                """,
                [item_id],
            )
        else:
            row = db._duck._fetchone(
                f"""
                SELECT {self._columns()}, envelope_json
                FROM {self.table} WHERE {self.id_column} = ? AND version = ?
                """,
                [item_id, int(version)],
            )
        if row is None:
            which = f"version {version} of " if version is not None else ""
            raise self.error(f"No {self.noun} {which}{item_id!r}.")
        return tuple(row[:6]), row[6]

    def visible_by_name(self, db, scope: str | None, name: str) -> Row | None:
        if not self.table_exists(db):
            return None
        where = f"{self.scope_column} = ? AND " if self.scope_column else ""
        row = db._duck._fetchone(
            f"""
            SELECT {self._columns()}
            FROM {self.table}
            WHERE {where}name = ? AND NOT hidden
            ORDER BY version DESC LIMIT 1
            """,
            [*([scope] if self.scope_column else []), name],
        )
        return tuple(row) if row else None

    def latest(self, db, item_id: str) -> Row:
        row = db._duck._fetchone(
            f"""
            SELECT {self._columns()}
            FROM {self.table} WHERE {self.id_column} = ?
            ORDER BY version DESC LIMIT 1
            """,
            [item_id],
        )
        if row is None:
            raise self.error(f"No {self.noun} {item_id!r}.")
        return tuple(row)

    def require(self, db, item_id: str) -> Row:
        if not self.table_exists(db):
            raise self.error(f"No {self.noun} {item_id!r}: nothing has been saved yet.")
        return self.latest(db, item_id)

    def parse_envelope(self, text: str, label: str) -> dict:
        """The stored envelope as a dict. A row that is not one opens on
        defaults rather than failing the open."""
        try:
            envelope = json.loads(text)
        except (TypeError, ValueError) as exc:
            Log.warn("[%s] %s: stored envelope is not JSON (%s)", self.tag, label, exc,
                     layer=LAYER)
            return {}
        if not isinstance(envelope, dict):
            Log.warn(
                "[%s] %s: stored envelope is %s, not a table",
                self.tag,
                label,
                type(envelope).__name__,
                layer=LAYER,
            )
            return {}
        return envelope

    # ---- copying a whole store (project bundles) ------------------------

    def _all_columns(self) -> list[str]:
        scope = [self.scope_column] if self.scope_column else []
        return [self.id_column, *scope, "name", "version", "saved_at", "hidden", "envelope_json"]

    def dump_rows(self, db) -> list[dict]:
        """Every row, every version, hidden ones too, as plain dicts: what a
        project bundle carries (``scistackplotdb.bundle_section``). Nothing
        is filtered -- a copy keeps the history the original kept."""
        if not self.table_exists(db):
            return []
        cols = self._all_columns()
        rows = db._duck._fetchall(
            f"SELECT {', '.join(cols)} FROM {self.table} ORDER BY {self.id_column}, version"
        )
        Log.info("[%s] dump_rows: %d row(s)", self.tag, len(rows), layer=LAYER)
        return [dict(zip(cols, row)) for row in rows]

    def load_rows(self, db, rows: list[dict]) -> int:
        """Insert *rows* (from :meth:`dump_rows`) verbatim, ids and versions
        kept; a row already present is left as it is. Returns rows inserted."""
        if not rows:
            return 0
        self.ensure_table(db)
        cols = self._all_columns()
        before = db._duck._fetchone(f"SELECT count(*) FROM {self.table}")[0]
        for row in rows:
            db._duck._execute(
                f"INSERT INTO {self.table} ({', '.join(cols)}) "
                f"VALUES ({', '.join('?' for _ in cols)}) ON CONFLICT DO NOTHING",
                [row.get(c) for c in cols],
            )
        after = db._duck._fetchone(f"SELECT count(*) FROM {self.table}")[0]
        Log.info("[%s] load_rows: %d of %d row(s) inserted", self.tag, after - before,
                 len(rows), layer=LAYER)
        return after - before
