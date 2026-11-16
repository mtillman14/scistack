"""THE owner of the stored schema key tree (``_schema_key_parent``).

``_schema`` (its columns) owns key ORDER; this table owns each key's PARENT.
The tree itself, and every rule about it, is ``scifor.schema.SchemaTree``;
this module only stores it and checks a declared tree against the stored one.
See docs/claude/branched-schema-keys.md.

* A new database gets the declared tree written when it is created.
* A database that has no rows yet (created before trees, or with the table
  just added) is given the declared tree on its first writable open: a plain
  list is a linear tree, which is what it always meant.
* Opening with a different tree raises ``SchemaKeysMismatchError``, as a
  different key list does.
* Written ONLY by creation, ``change_schema_keys`` and ``extend_schema_keys``.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
from scifor.schema import SchemaTree

from scidb.model.exceptions import SchemaKeysMismatchError
from scidb.model.log import Log

if TYPE_CHECKING:
    from scidb.store.sciduck import SciDuck

TABLE = "_schema_key_parent"


def read_parents(duck: "SciDuck") -> "dict[str, str | None] | None":
    """``{key: parent}`` as stored, or None when nothing is stored yet."""
    if not duck.table_exists(TABLE):
        return None
    rows = duck.fetchall(f'SELECT "key", parent FROM {TABLE}')
    return {k: p for k, p in rows} if rows else None


def write_tree(duck: "SciDuck", tree: SchemaTree) -> None:
    """Replace the stored parents with *tree*'s. The caller owns the transaction."""
    duck.execute(f"DELETE FROM {TABLE}")
    for key, parent in zip(tree.keys, tree.parents, strict=True):
        duck.execute(f'INSERT INTO {TABLE} ("key", parent) VALUES (?, ?)', [key, parent])
    Log.info(f"[schema_tree] stored: {tree.describe()}")


def reconcile(duck: "SciDuck", tree: SchemaTree, *, db_path=None) -> None:
    """Check *tree* against the stored one, or store it when nothing is.

    Read-only connections never write: a database with no stored rows is
    simply not checked.
    """
    stored = read_parents(duck)
    if stored is None:
        if not duck.read_only and duck.table_exists(TABLE):
            write_tree(duck, tree)
        return
    if stored != tree.parent_of:
        Log.warn(
            f"[schema_tree] mismatch on {db_path}: passed {tree.parent_of}, database {stored}"
        )
        raise SchemaKeysMismatchError(
            list(tree.keys),
            list(tree.keys),
            db_path,
            passed_parents=tree.parent_of,
            existing_parents=stored,
        )


def tree_from_db(db_path: "str | Path") -> SchemaTree:
    """The tree stored in an existing database (read-only), keys in the
    order of ``_schema``'s columns. A database with no stored parents is a
    linear tree. Replaces "read the key list" wherever a tool opens a
    database without knowing its keys in advance."""
    from scidb.store.sciduck import schema_keys_from_db

    keys = schema_keys_from_db(db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        has = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [TABLE]
        ).fetchone()[0]
        rows = con.execute(f'SELECT "key", parent FROM {TABLE}').fetchall() if has else []
    finally:
        con.close()
    if not rows:
        return SchemaTree.linear(keys)
    return SchemaTree.from_parents(keys, {k: p for k, p in rows})
