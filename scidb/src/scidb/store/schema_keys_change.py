"""Change the schema keys of an EMPTY dataset (GitHub issue #10;
``.claude/plan-change-schema-keys.md``, docs/claude/changing-schema-keys.md).

Schema keys are the ``_schema`` columns, fixed when a database is created. They
can still change while the dataset holds no data (``scidb.store.emptiness``,
the one owner of "empty"), which is the case when the first load collides
because the keys were chosen too coarse.

Owners here: :func:`validate_schema_keys` (a valid key list),
:func:`propose_schema_keys` (the default proposal) and
:func:`change_schema_keys` (the change). Which tables are rebuilt comes from
``scidb.store.tables.schema_keyed_tables``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Sequence

from scidb.model.log import Log
from scidb.store import tables as table_registry
from scidb.store.emptiness import is_empty

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

RESERVED_KEYS = ("schema_id", "schema_level")


class SchemaKeysChangeRefused(ValueError):
    """A change that must not run, with the reason (shown as one line)."""


@dataclass
class SchemaKeysChange:
    old_keys: list[str]
    new_keys: list[str]
    renames: dict[str, str]
    #: ``{table: rows carried into the new shape}`` (intent tables).
    rows_carried: dict[str, int] = field(default_factory=dict)
    #: Old keys that no longer exist (not renamed), with their declarations dropped.
    deleted_keys: list[str] = field(default_factory=list)
    #: Things the user may still have to update by hand.
    leftovers: list[str] = field(default_factory=list)


def validate_schema_keys(keys: Sequence[str]) -> list[str]:
    """The keys as a list, or ``ValueError`` naming the bad one: non-empty,
    unique, text, not ``schema_id``/``schema_level``, no ``__`` prefix."""
    keys = list(keys)
    if not keys:
        raise ValueError("A dataset needs at least one schema key")
    seen: set[str] = set()
    for key in keys:
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"Schema key {key!r} must be a non-empty name")
        if key in RESERVED_KEYS:
            raise ValueError(f"Schema key {key!r} is reserved")
        if key.startswith("__"):
            raise ValueError(f"Schema key {key!r} may not start with '__'")
        if '"' in key:
            raise ValueError(f"Schema key {key!r} may not contain a double quote")
        if key in seen:
            raise ValueError(f"Schema key {key!r} is listed twice")
        seen.add(key)
    return keys


def propose_schema_keys(
    path_input_keys: Sequence[Sequence[str]], data_columns: Sequence[str]
) -> list[dict]:
    """The default new keys: each PathInput's placeholder keys in template
    order (path nesting = coarse to fine), then each data column not already
    listed, in column order. Pure.

    Returns ``[{"key", "source"}]``, source ``"path_input"`` or ``"data_column"``.
    """
    proposed: list[dict] = []
    seen: set[str] = set()
    for keys in path_input_keys:
        for key in keys:
            if key not in seen:
                seen.add(key)
                proposed.append({"key": key, "source": "path_input"})
    for column in data_columns:
        if column not in seen:
            seen.add(column)
            proposed.append({"key": column, "source": "data_column"})
    return proposed


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _plan_columns(old_keys, new_keys, renames) -> "tuple[list[tuple[str | None, str]], list[str]]":
    """``([(source old key or None, new key)], deleted old keys)``."""
    source_of = {new: old for old, new in renames.items()}
    mapping = []
    for new in new_keys:
        if new in source_of:
            mapping.append((source_of[new], new))
        elif new in old_keys and new not in renames:
            mapping.append((new, new))
        else:
            mapping.append((None, new))
    kept_old = {old for old, _ in mapping if old is not None}
    return mapping, [k for k in old_keys if k not in kept_old]


def change_schema_keys(
    db: "DatabaseManager",
    new_keys: Sequence[str],
    *,
    renames: "dict[str, str] | None" = None,
    reason: str = "",
) -> SchemaKeysChange:
    """Give the (empty) dataset *new_keys*, in one transaction.

    *renames* is ``{old key: new key}`` for existing keys the user renamed.
    Refuses (:class:`SchemaKeysChangeRefused`) when the dataset holds data,
    when the keys are invalid, or when an exclusion / expectation mark has a
    value under a key that would be deleted (it would be dropped silently).

    History tables (``_schema``, ``_run_outcome``) are emptied and rebuilt;
    intent tables (exclusions, expectation marks) are copied into the new
    shape (a new key reads NULL = every value); key types and expectations
    follow a rename by appended rows. Every variable view is re-created.

    **The database is closed afterwards**: the caller's in-memory keys are
    stale. Reopen through the normal open path with ``new_keys``.
    """
    started = time.perf_counter()
    old_keys = list(db.dataset_schema_keys)
    renames = {o: n for o, n in (renames or {}).items() if o != n}
    try:
        new_keys = validate_schema_keys(new_keys)
        for old, new in renames.items():
            if old not in old_keys:
                raise ValueError(f"Cannot rename {old!r}: it is not a current schema key")
            if new not in new_keys:
                raise ValueError(f"Renamed key {new!r} is not in the new keys")
        if len(set(renames.values())) != len(renames):
            raise ValueError("Two keys are renamed to the same name")
        for new in renames.values():
            if new in old_keys and new not in renames:
                raise ValueError(f"Cannot rename a key to {new!r}: it is an existing key")
    except ValueError as exc:
        Log.warn(f"[schema_keys_change] refused: {exc}")
        raise SchemaKeysChangeRefused(str(exc)) from exc

    mapping, deleted = _plan_columns(old_keys, new_keys, renames)
    duck = db._duck
    result = SchemaKeysChange(old_keys, list(new_keys), renames, deleted_keys=deleted)
    Log.info(
        f"[schema_keys_change] {old_keys} -> {new_keys}"
        + (f", renames {renames}" if renames else "")
        + (f", reason: {reason}" if reason else "")
    )

    with duck.transaction("change_schema_keys"):
        # Re-checked inside the transaction: the popup's answer can be stale.
        if not is_empty(db):
            msg = (
                "This dataset already holds data, so its schema keys can't be "
                "changed. Create a new project database with the keys you want."
            )
            Log.warn(f"[schema_keys_change] refused: {msg}")
            raise SchemaKeysChangeRefused(msg)

        intent = [d for d in table_registry.schema_keyed_tables() if d.bundle == table_registry.BUNDLE_INTENT]
        history = [d for d in table_registry.schema_keyed_tables() if d.bundle != table_registry.BUNDLE_INTENT]

        # Refuse before changing anything: intent rows under a deleted key.
        if deleted:
            for decl in intent:
                if not duck.table_exists(decl.name):
                    continue
                for key in deleted:
                    rows = duck.fetchall(
                        f"SELECT COUNT(*) FROM {_qi(decl.name)} WHERE {_qi(key)} IS NOT NULL"
                    )[0][0]
                    if rows:
                        msg = (
                            f"{decl.name} has {rows} row(s) with a value for "
                            f"{key!r}, which would be deleted. Keep the key (or "
                            f"rename it), or remove those rows first."
                        )
                        Log.warn(f"[schema_keys_change] refused: {msg}")
                        raise SchemaKeysChangeRefused(msg)

        # Views reference _schema's columns: drop, rebuild after.
        from scidb.store.db_save import variable_view_sql

        views = []
        if duck.table_exists("_registered_types"):
            views = [
                (t, tbl)
                for t, tbl in duck.fetchall("SELECT type_name, table_name FROM _registered_types")
            ]
        for view, _ in views:
            duck.execute(f"DROP VIEW IF EXISTS {_qi(view)}")

        # Intent tables: read, drop, recreate, refill.
        for decl in intent:
            if not duck.table_exists(decl.name):
                continue
            t0 = time.perf_counter()
            cols, rows = duck.fetch_table(f"SELECT * FROM {_qi(decl.name)}")
            duck.execute(f"DROP TABLE {_qi(decl.name)}")
            for statement in decl.sql(new_keys):
                duck.execute(statement)
            new_cols = [new for _, new in mapping] + [c for c in cols if c not in old_keys]
            idx = {c: i for i, c in enumerate(cols)}
            out_rows = []
            for row in rows:
                out = []
                for old, _new in mapping:
                    out.append(row[idx[old]] if old is not None else None)
                out += [row[idx[c]] for c in cols if c not in old_keys]
                out_rows.append(out)
            if out_rows:
                duck.bulk_insert(decl.name, new_cols, out_rows)
            result.rows_carried[decl.name] = len(out_rows)
            Log.info(
                f"[schema_keys_change] {decl.name}: {len(out_rows)} row(s) carried "
                f"({(time.perf_counter() - t0) * 1000:.0f} ms)"
            )

        # History tables: empty, then rebuilt for the new keys.
        for decl in history:
            if not duck.table_exists(decl.name):
                continue
            t0 = time.perf_counter()
            if decl.name == "_schema" and duck.table_exists("_record"):
                n = duck.fetchall(
                    "SELECT COUNT(*) FROM _record WHERE schema_id IS NOT NULL"
                )[0][0]
                duck.execute("UPDATE _record SET schema_id = NULL WHERE schema_id IS NOT NULL")
                Log.info(f"[schema_keys_change] _record: {n} history row(s) lost their location id")
            duck.execute(f"DROP TABLE {_qi(decl.name)}")
            for statement in decl.sql(new_keys):
                duck.execute(statement)
            Log.info(
                f"[schema_keys_change] {decl.name}: emptied and rebuilt "
                f"({(time.perf_counter() - t0) * 1000:.0f} ms)"
            )

        for view, table in views:
            if duck.table_exists(table):
                duck.execute(variable_view_sql(view, table, new_keys))

        _carry_declarations(duck, renames, deleted, reason or "change_schema_keys")

    leftovers = _config_leftovers(old_keys, deleted, renames)
    result.leftovers = leftovers
    for line in leftovers:
        Log.warn(f"[schema_keys_change] {line}")
    db.close()
    Log.info(
        f"[schema_keys_change] done in {(time.perf_counter() - started) * 1000:.0f} ms; "
        f"database closed, reopen with {new_keys}"
    )
    return result


def _carry_declarations(duck, renames: dict, deleted: list, source: str) -> None:
    """Key types and expectations are keyed by name and append-only: a renamed
    key's newest row is appended under its new name, and the old name (renamed
    or deleted) gets a NULL row that ends its declaration."""
    from datetime import datetime, timezone

    # UTC, like schema_types and expectation: rows are ordered by this string.
    now = datetime.now(timezone.utc).isoformat()
    if duck.table_exists("_schema_key_type"):
        for old in list(renames) + list(deleted):
            row = duck.fetchone(
                "SELECT key_type FROM _schema_key_type WHERE key = ? "
                "ORDER BY declared_at DESC LIMIT 1", [old]
            )
            if row is None or row[0] is None:
                continue
            if old in renames:
                duck.execute(
                    "INSERT INTO _schema_key_type (key, key_type, declared_at, source) VALUES (?, ?, ?, ?)",
                    [renames[old], row[0], now, source],
                )
            duck.execute(
                "INSERT INTO _schema_key_type (key, key_type, declared_at, source) VALUES (?, NULL, ?, ?)",
                [old, now, source],
            )
    if duck.table_exists("_schema_key_expectation"):
        for old in list(renames) + list(deleted):
            row = duck.fetchone(
                "SELECT sharing, declared_values FROM _schema_key_expectation "
                "WHERE key = ? ORDER BY declared_at DESC LIMIT 1", [old]
            )
            if row is None or (row[0] is None and row[1] is None):
                continue
            if old in renames:
                duck.execute(
                    "INSERT INTO _schema_key_expectation (key, sharing, declared_values, declared_at, source) "
                    "VALUES (?, ?, ?, ?, ?)",
                    [renames[old], row[0], row[1], now, source],
                )
            duck.execute(
                "INSERT INTO _schema_key_expectation (key, sharing, declared_values, declared_at, source) "
                "VALUES (?, NULL, NULL, ?, ?)",
                [old, now, source],
            )


def _config_leftovers(old_keys, deleted, renames) -> list[str]:
    """Project-config ``[schema_keys]`` declarations naming a key that is gone.
    Reported, never edited: the config is the user's file."""
    try:
        from scifor.project import schema_order

        declared = schema_order.project_level_order()
    except Exception as exc:  # reading the config must not undo a committed change
        Log.warn(f"[schema_keys_change] could not read [schema_keys]: {exc}")
        return []
    lines = []
    for key in declared:
        if key in renames:
            lines.append(
                f"[schema_keys] in the project config still declares {key!r}; "
                f"rename it to {renames[key]!r}."
            )
        elif key in deleted:
            lines.append(
                f"[schema_keys] in the project config still declares {key!r}, "
                f"which is no longer a schema key; remove it."
            )
    return lines
