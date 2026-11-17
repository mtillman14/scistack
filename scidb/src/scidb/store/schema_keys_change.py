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
from scifor.schema import SchemaTree, as_tree
from scidb.store import tables as table_registry
from scidb.store import schema_tree_store
from scidb.store.emptiness import is_empty

if TYPE_CHECKING:
    from pathlib import Path

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
    #: Keys appended by this change.
    added_keys: list[str] = field(default_factory=list)
    #: History rows removed with a deleted key, ``{table: rows}``.
    leftovers_removed: dict[str, int] = field(default_factory=dict)
    #: Things the user may still have to update by hand (references to a
    #: deleted key, kept as written; ``scidb.project.key_rename``).
    leftovers: list[str] = field(default_factory=list)
    #: What renaming the references did, ``{place: changed, ..., "flagged": []}``
    #: (``scidb.project.key_rename.rename_key_references``).
    references: dict = field(default_factory=dict)


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
    new_keys: "Sequence[str] | Sequence[Sequence[str]] | SchemaTree",
    *,
    renames: "dict[str, str] | None" = None,
    reason: str = "",
    root: "Path | str | None" = None,
    providers: Sequence = (),
) -> SchemaKeysChange:
    """Give the (empty) dataset *new_keys*, in one transaction.

    *new_keys* is a plain list (one branch), one list per branch, or a
    ``SchemaTree``; the stored parents (``_schema_key_parent``) are rewritten
    with the keys.

    *renames* is ``{old key: new key}`` for existing keys the user renamed.
    Refuses (:class:`SchemaKeysChangeRefused`) when the dataset holds data,
    when the keys are invalid, or when an exclusion / expectation mark has a
    value under a key that would be deleted (it would be dropped silently).

    History tables (``_schema``, ``_run_outcome``) are emptied and rebuilt;
    intent tables (exclusions, expectation marks) are copied into the new
    shape (a new key reads NULL = every value); key types and expectations
    follow a rename by appended rows. Every variable view is re-created.

    Then every reference OUTSIDE the key tables follows a rename
    (``scidb.project.key_rename``): the project config and entities file at
    *root* (default ``scifor.project.root.project_root()``), and whatever each
    of *providers* (bundle sections: the GUI canvas, saved plots, saved
    stats) owns. References to a deleted key are kept and reported in
    ``leftovers``.

    **The database is closed afterwards**: the caller's in-memory keys are
    stale. Reopen through the normal open path with ``new_keys``.
    """
    started = time.perf_counter()
    old_keys = list(db.dataset_schema_keys)
    renames = {o: n for o, n in (renames or {}).items() if o != n}
    try:
        new_tree = as_tree(new_keys)
        new_keys = validate_schema_keys(new_tree.keys)
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

    if not is_empty(db):
        # Data exists: only UNUSED keys may change, and new keys may be added.
        # (An empty dataset takes the rebuild below, where every key is unused.)
        with duck.transaction("change_schema_keys"):
            _change_in_place(db, new_tree, renames, deleted, result, reason)
        return _finish(db, result, started, root, providers)

    with duck.transaction("change_schema_keys"):
        # Re-checked inside the transaction: the popup's answer can be stale.
        if not is_empty(db):
            msg = (
                "This dataset gained data while the change was being prepared, so "
                "nothing was changed. Try again: keys that no record names can still change."
            )
            Log.warn(f"[schema_keys_change] refused: {msg}")
            raise SchemaKeysChangeRefused(msg)

        intent = [d for d in table_registry.schema_keyed_tables() if d.bundle == table_registry.BUNDLE_INTENT]
        history = [d for d in table_registry.schema_keyed_tables() if d.bundle != table_registry.BUNDLE_INTENT]

        # Refuse before changing anything: intent rows under a deleted key.
        _refuse_intent_rows_under(duck, deleted)

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

        schema_tree_store.write_tree(duck, new_tree)

        _carry_declarations(duck, renames, deleted, reason or "change_schema_keys")

    return _finish(db, result, started, root, providers)


def _finish(db, result: SchemaKeysChange, started: float, root, providers) -> SchemaKeysChange:
    """After the transaction: rename the references outside the key tables
    (``scidb.project.key_rename``), report what still names a deleted key,
    and CLOSE the database (the caller's in-memory keys are stale).

    The references are renamed AFTER the key tables committed: a failure
    there leaves the keys changed and is reported, never undoes the change."""
    from scidb.project.key_rename import rename_key_references

    if root is None:
        from scifor.project.root import project_root

        root = project_root()
    try:
        result.references = rename_key_references(
            db, root, result.renames, result.deleted_keys, providers=providers
        )
    except Exception as exc:  # the keys are committed; say what was not renamed
        Log.warn(f"[schema_keys_change] renaming references failed: {exc!r}")
        result.references = {"error": str(exc), "dropped": [], "flagged": []}
        result.leftovers.append(
            f"The keys changed, but references to them could not all be renamed: {exc}"
        )
    for where, why in result.references.get("flagged", []):
        result.leftovers.append(f"{where}: {why}")
    for line in result.leftovers:
        Log.warn(f"[schema_keys_change] {line}")
    db.close()
    Log.info(
        f"[schema_keys_change] done in {(time.perf_counter() - started) * 1000:.0f} ms; "
        f"database closed, reopen with {result.new_keys}"
    )
    return result


def _refuse_intent_rows_under(duck, deleted: list) -> None:
    """Intent rows (exclusions, expectation marks) with a value under a key
    that would be deleted would be dropped silently, so refuse."""
    intent = [
        d for d in table_registry.schema_keyed_tables() if d.bundle == table_registry.BUNDLE_INTENT
    ]
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




# ---------------------------------------------------------------------------
# A dataset that holds data (plan-branched-schema-keys.md, stage extend_tree)
# ---------------------------------------------------------------------------
#
# Unused keys are free, used keys are fixed. A NULL key column already means
# "this record does not name this key", so a NEW key changes the meaning of
# nothing that exists, and a key no record or run names can be renamed,
# re-parented or deleted without changing any record. The one check is
# ``scidb.store.emptiness.key_usage``; an empty dataset is the case where every
# key is unused, and takes the rebuild in :func:`change_schema_keys`.


def _refuse(msg: str) -> "SchemaKeysChangeRefused":
    Log.warn(f"[schema_keys_change] refused: {msg}")
    return SchemaKeysChangeRefused(msg)


def _plan_in_place(old_tree: SchemaTree, new_tree: SchemaTree, renames: dict, deleted: list, usage: dict):
    """Validate a change to a dataset that holds data. Returns the added keys.
    Raises SchemaKeysChangeRefused naming the key and the reason."""
    old_keys = list(old_tree.keys)
    new_keys = list(new_tree.keys)
    added = [k for k in new_keys if k not in old_keys and k not in renames.values()]
    kept_old = [k for k in old_keys if k not in deleted]
    expected = [renames.get(k, k) for k in kept_old] + added
    if new_keys != expected:
        raise _refuse(
            "Existing keys keep their order, and a new key goes after them "
            f"(expected {expected}, got {new_keys}). Keys can't be inserted "
            "between existing ones or moved."
        )

    for key in deleted:
        below = [d for d in _descendants(old_tree, key) if d not in deleted]
        if below:
            raise _refuse(
                f"Deleting {key!r} would leave {below} without a parent. Delete the keys "
                "below it too, or give them a new parent."
            )

    reparented = []
    for key in kept_old:
        old_parent = old_tree.parent_of[key]
        mapped = renames.get(old_parent, old_parent) if old_parent else None
        if new_tree.parent_of[renames.get(key, key)] != mapped:
            reparented.append(key)

    for key in sorted({*deleted, *renames, *reparented}, key=old_keys.index):
        u = usage[key]
        if u.used:
            what = "deleted" if key in deleted else "renamed" if key in renames else "moved under a new parent"
            raise _refuse(
                f"{key!r} is in use ({_used_by(u)}), so it can't be {what}. "
                "Only keys that no record or run names can change; new keys can always be added."
            )
    return added


def _used_by(u) -> str:
    parts = []
    if u.records:
        parts.append(f"{u.records} record(s)")
    if u.run_levels:
        parts.append(f"{u.run_levels} run level(s)")
    return ", ".join(parts)


def _descendants(tree: SchemaTree, key: str) -> list[str]:
    out: list[str] = []
    for child in tree.children(key):
        out.append(child)
        out.extend(_descendants(tree, child))
    return out


def _change_in_place(db, new_tree: SchemaTree, renames: dict, deleted: list, result, reason: str) -> None:
    """Rename, re-parent and delete UNUSED keys and append new ones, in the
    caller's transaction. Nothing a record says is rewritten."""
    from scidb.store.db_save import variable_view_sql
    from scidb.store.emptiness import key_usage

    duck = db._duck
    old_tree = db.schema_tree
    old_keys = list(old_tree.keys)
    new_keys = list(new_tree.keys)
    usage = key_usage(db)
    added = _plan_in_place(old_tree, new_tree, renames, deleted, usage)
    _refuse_intent_rows_under(duck, deleted)

    keyed = [d.name for d in table_registry.schema_keyed_tables() if duck.table_exists(d.name)]
    result.leftovers_removed = {}

    # Leftover history naming a key that is going away goes with it, counted.
    if deleted:
        any_named = " OR ".join(f"{_qi(k)} IS NOT NULL" for k in deleted)
        if duck.table_exists("_run_outcome"):
            n = duck.fetchone(f"SELECT COUNT(*) FROM _run_outcome WHERE {any_named}")[0]
            duck.execute(f"DELETE FROM _run_outcome WHERE {any_named}")
            result.leftovers_removed["_run_outcome"] = int(n)
        if duck.table_exists("_schema"):
            referenced = (
                "schema_id IN (SELECT schema_id FROM _record WHERE schema_id IS NOT NULL)"
                if duck.table_exists("_record")
                else "FALSE"
            )
            blocked = duck.fetchone(f"SELECT COUNT(*) FROM _schema WHERE ({any_named}) AND {referenced}")[0]
            if blocked:
                raise _refuse(
                    f"{blocked} saved location(s) still name a key that would be deleted "
                    f"({deleted}). Nothing was changed."
                )
            n = duck.fetchone(f"SELECT COUNT(*) FROM _schema WHERE {any_named}")[0]
            duck.execute(f"DELETE FROM _schema WHERE {any_named}")
            result.leftovers_removed["_schema"] = int(n)
        Log.info(f"[schema_keys_change] removed leftover history naming {deleted}: {result.leftovers_removed}")

    views = []
    if duck.table_exists("_registered_types"):
        views = list(duck.fetchall("SELECT type_name, table_name FROM _registered_types"))
    for view, _ in views:
        duck.execute(f"DROP VIEW IF EXISTS {_qi(view)}")

    for table in keyed:
        t0 = time.perf_counter()
        for key in deleted:
            duck.execute(f"ALTER TABLE {_qi(table)} DROP COLUMN {_qi(key)}")
        for old, new in renames.items():
            duck.execute(f"ALTER TABLE {_qi(table)} RENAME COLUMN {_qi(old)} TO {_qi(new)}")
        for key in added:
            duck.execute(f"ALTER TABLE {_qi(table)} ADD COLUMN {_qi(key)} VARCHAR")
        Log.info(
            f"[schema_keys_change] {table}: dropped {deleted}, renamed {renames}, added {added} "
            f"({(time.perf_counter() - t0) * 1000:.0f} ms)"
        )

    for view, table in views:
        if duck.table_exists(table):
            duck.execute(variable_view_sql(view, table, new_keys))

    _carry_declarations(duck, renames, deleted, reason or "change_schema_keys")
    schema_tree_store.write_tree(duck, new_tree)
    result.added_keys = added
    Log.info(
        f"[schema_keys_change] in place: {old_keys} -> {new_keys} "
        f"(added {added}, deleted {deleted}, renamed {renames})"
    )


def extend_schema_keys(db: "DatabaseManager", paths, *, reason: str = "") -> SchemaKeysChange:
    """Add new keys (a branch, or a deeper level) to a dataset, whether or not
    it holds data. Everything that exists keeps its meaning: the old keys, their
    order and their parents must be unchanged and the new keys go after them,
    each under an existing key or another new one. Anything else is refused
    with the reason; use :func:`change_schema_keys` for renames, re-parenting
    and deletes of UNUSED keys.

    **The database is closed afterwards** (see :func:`change_schema_keys`).
    """
    new_tree = as_tree(paths)
    old_tree = db.schema_tree
    n = len(old_tree.keys)
    if new_tree.keys[:n] != old_tree.keys:
        raise _refuse(
            f"Extending keeps the existing keys {list(old_tree.keys)} in order and adds new "
            f"ones after them; got {list(new_tree.keys)}."
        )
    changed = [
        k for k in old_tree.keys if new_tree.parent_of[k] != old_tree.parent_of[k]
    ]
    if changed:
        raise _refuse(f"Extending can't change the parent of existing key(s) {changed}.")
    if len(new_tree.keys) == n:
        raise _refuse("No new keys to add.")
    return change_schema_keys(db, new_tree, reason=reason or "extend_schema_keys")
