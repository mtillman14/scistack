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


# ---------------------------------------------------------------------------
# Which schema KEYS are in use (plan-branched-schema-keys.md, extend_tree)
# ---------------------------------------------------------------------------
#
# "Unused keys are free; used keys are fixed." A NULL in a key column already
# means "this record does not name this key", so a key no record and no run
# names can be renamed, re-parented or deleted without changing what any record
# means, and a NEW key changes nothing that exists. ``is_empty`` is the special
# case "every key is unused".


from dataclasses import dataclass


@dataclass(frozen=True)
class KeyUsage:
    """What names one schema key.

    ``records`` and ``run_levels`` make a key USED (it keeps its name, parent
    and position). ``exclusions`` and ``marks`` are intent rows naming it: they
    follow a rename and block a delete until removed, but do not make it used.
    """

    key: str
    records: int = 0
    run_levels: int = 0
    exclusions: int = 0
    marks: int = 0

    @property
    def used(self) -> bool:
        return bool(self.records or self.run_levels)

    def describe(self) -> str:
        if not self.used:
            return f"{self.key}: unused"
        parts = []
        if self.records:
            parts.append(f"{self.records} record(s)")
        if self.run_levels:
            parts.append(f"{self.run_levels} run level(s)")
        return f"{self.key}: in use ({', '.join(parts)})"


def key_usage(db: "DatabaseManager") -> "dict[str, KeyUsage]":
    """``{key: KeyUsage}`` for every schema key, in dataset order."""
    started = time.perf_counter()
    duck = db._duck
    keys = list(db.dataset_schema_keys)
    records = dict.fromkeys(keys, 0)
    if keys and duck.table_exists("_record") and duck.table_exists("_schema"):
        sums = ", ".join(
            f'COALESCE(SUM(CASE WHEN s."{k}" IS NOT NULL THEN 1 ELSE 0 END), 0)' for k in keys
        )
        row = duck.fetchone(
            f"SELECT {sums} FROM _record r JOIN _schema s ON r.schema_id = s.schema_id "
            f"WHERE r.type NOT IN ({_internal_types_sql()})"
        )
        records = {k: int(n) for k, n in zip(keys, row)}
    out: dict[str, KeyUsage] = {}
    for key in keys:
        run_levels = 0
        if duck.table_exists("_invocation"):
            run_levels = int(
                duck.fetchone(
                    "SELECT COUNT(*) FROM _invocation WHERE list_contains(iteration_level, ?)", [key]
                )[0]
            )
        counts = {}
        for name, table in (("exclusions", "__scidb_schema_overrides"), ("marks", "_expectation_mark")):
            counts[name] = (
                int(duck.fetchone(f'SELECT COUNT(*) FROM {table} WHERE "{key}" IS NOT NULL')[0])
                if duck.table_exists(table)
                else 0
            )
        out[key] = KeyUsage(key, records[key], run_levels, counts["exclusions"], counts["marks"])
    Log.info(
        "[emptiness] key_usage: %s (%.1f ms)",
        "; ".join(u.describe() for u in out.values()),
        (time.perf_counter() - started) * 1000,
    )
    return out


def usage_by_branch(db: "DatabaseManager") -> "list[tuple[list[str], bool, int]]":
    """``[(branch keys, any key used, records)]``, one row per root-to-leaf
    branch, for display: "brand, category, item: in use (1,240 records)"."""
    usage = key_usage(db)
    rows = []
    shown: set[str] = set()
    for path in db.schema_tree.paths():
        # A shared trunk (brand) belongs to the first branch that lists it: the
        # second branch is judged by its OWN keys, so "location, person: unused".
        branch = [k for k in path if k not in shown]
        shown.update(path)
        if not branch:
            continue
        used = any(usage[k].used for k in branch)
        rows.append((branch, used, max((usage[k].records for k in branch), default=0)))
    return rows
