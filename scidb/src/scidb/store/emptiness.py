"""THE owner of "does this dataset hold data" (``.claude/plan-change-schema-keys.md``).

A dataset is **empty** when no variable holds a saved record. Failure
outcomes (``_run_outcome``), the run log, invocations and the internal record
types (constants, PathInput, glue, selection: ``INTERNAL_RECORD_TYPES``) are
run history, not data, and declarations (exclusions, expectation marks, key
types) are not data either.

Consumers: ``clear_data.clear_plan`` (per-variable counts), the
``DuplicateLocationError`` remedy and ``schema_keys_change.change_schema_keys``
(via :func:`is_empty`), and ``scidb.is_empty``. A dry run of ``clear_plan``
cannot answer this: its table counts include ``_run_outcome`` rows, which the
failed first load itself writes.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from scidb.model.log import Log

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager


def _internal_types_sql() -> str:
    from scidb.provenance.graph import INTERNAL_RECORD_TYPES

    return ", ".join("'" + t.replace("'", "''") + "'" for t in INTERNAL_RECORD_TYPES)


def variable_record_counts(db: "DatabaseManager") -> dict[str, int]:
    """``{variable: saved records}``, internal record types excluded."""
    duck = db._duck
    if not duck.table_exists("_record"):
        return {}
    rows = duck.fetchall(
        f"SELECT type, COUNT(*) FROM _record WHERE type NOT IN "
        f"({_internal_types_sql()}) GROUP BY type ORDER BY type"
    )
    return {rtype: int(n) for rtype, n in rows}


def is_empty(db: "DatabaseManager") -> bool:
    """True when no variable holds a saved record. One ``EXISTS``, so it never
    counts a large table."""
    started = time.perf_counter()
    duck = db._duck
    first = None
    if duck.table_exists("_record"):
        row = duck.fetchone(
            f"SELECT type FROM _record WHERE type NOT IN "
            f"({_internal_types_sql()}) LIMIT 1"
        )
        first = row[0] if row else None
    ms = (time.perf_counter() - started) * 1000
    if first is None:
        Log.info("[emptiness] is_empty: True (%.1f ms)", ms)
        return True
    Log.info("[emptiness] is_empty: False (first variable found: %s) (%.1f ms)", first, ms)
    return False
