"""Schema-based data exclusions for scidb.

Provides a persistent registry of schema-key combinations to skip in
every analysis.  Unlike value-based filters (the ``where=`` system), these
exclusions are stored in the database and consulted automatically by
``for_each`` before iterating.

Usage::

    scidb.exclude_schema(subject=1, trial=2,
                         reason="equipment malfunction")
    scidb.include_schema(subject=1, trial=2,
                         reason="re-reviewed, recording was valid")
    exclusions_df = scidb.list_exclusions()

Conflict resolution: when multiple rows match a given combo, the most
specific row wins (most non-NULL schema columns); ties are broken by the
most recent ``changed_at``.  NULL in a schema column acts as a wildcard
that matches any value of that key.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database import DatabaseManager

_TABLE = "__scidb_schema_overrides"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def exclude_schema(
    reason: str, db: DatabaseManager | None = None, **schema_keys
) -> None:
    """Mark a schema-key combination as permanently excluded from analyses.

    The record is stored in the database and consulted by ``for_each`` before
    iterating.  Omitted schema keys act as wildcards (NULL in the table),
    so ``exclude_schema(subject=3, reason=...)`` excludes *every* trial of
    subject 3.

    Args:
        reason: Human-readable explanation of why this data is excluded.
        db: DatabaseManager (defaults to ``get_database()``).
        **schema_keys: Schema key values identifying the combination to exclude.

    Raises:
        ValueError: If no schema keys are given, a key is unknown, or the
            exact keyset is already excluded.
    """
    db = _get_db(db)
    if not schema_keys:
        raise ValueError("At least one schema key must be specified.")
    _validate_keys(db, schema_keys)

    status = _current_status(db, schema_keys)
    if status is False:
        raise ValueError(
            f"Schema combination {schema_keys!r} is already excluded. "
            "Call include_schema() first if you want to re-exclude it with a "
            "different reason."
        )

    _insert_row(db, schema_keys, status=False, reason=reason)


def include_schema(
    reason: str, db: DatabaseManager | None = None, **schema_keys
) -> None:
    """Re-include a previously excluded schema-key combination.

    The record is stored in the database (the original exclusion row is NOT
    deleted — the full history is preserved).

    Args:
        reason: Human-readable explanation of why this data is re-included.
        db: DatabaseManager (defaults to ``get_database()``).
        **schema_keys: Schema key values identifying the combination to include.

    Raises:
        ValueError: If the exact keyset has no exclusion record or is already
            included.
    """
    db = _get_db(db)
    if not schema_keys:
        raise ValueError("At least one schema key must be specified.")
    _validate_keys(db, schema_keys)

    status = _current_status(db, schema_keys)
    if status is None:
        raise ValueError(
            f"Schema combination {schema_keys!r} has no exclusion record. "
            "Call exclude_schema() first."
        )
    if status is True:
        raise ValueError(f"Schema combination {schema_keys!r} is already included.")

    _insert_row(db, schema_keys, status=True, reason=reason)


def list_exclusions(db: DatabaseManager | None = None):
    """Return a DataFrame of currently-excluded schema combinations.

    Shows the latest row per exact keyset where the effective status is
    excluded (``status = FALSE``).

    Args:
        db: DatabaseManager (defaults to ``get_database()``).

    Returns:
        pandas.DataFrame with schema-key columns plus ``reason`` and
        ``changed_at`` columns.
    """
    db = _get_db(db)
    schema_keys = db.dataset_schema_keys
    key_cols = ", ".join(f'"{k}"' for k in schema_keys)

    rows = db._duck._fetchdf(
        f"""
        WITH latest AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {key_cols}
                       ORDER BY changed_at DESC
                   ) AS rn
            FROM {_TABLE}
        )
        SELECT {key_cols}, reason, changed_at
        FROM latest
        WHERE rn = 1 AND status = FALSE
        ORDER BY changed_at DESC
        """,
    )
    return rows


# ---------------------------------------------------------------------------
# Internal helpers (used by for_each and the public API)
# ---------------------------------------------------------------------------


def get_schema_overrides_hash(db: DatabaseManager) -> str:
    """Return a 16-hex-char SHA-256 hash of the full ``__scidb_schema_overrides`` table.

    Hashes ALL rows (both directions of state change) so that any edit —
    new exclusion, re-inclusion — changes the hash and invalidates cached
    ``for_each`` results.

    Returns the hash of an empty payload if the table is empty.

    Backends without a DuckDB layer (no ``_duck`` — e.g. remote/net backends
    or test doubles) cannot store overrides, so they are treated as having
    none: the empty-payload hash is returned rather than crashing.  This keeps
    ``for_each`` backend-agnostic (see ``_for_each_prepare`` Step 9.5).
    """
    duck = _overrides_backend(db)
    if duck is None:
        rows = []
    else:
        schema_keys = db.dataset_schema_keys
        key_cols = ", ".join(f'"{k}"' for k in schema_keys)
        rows = duck._fetchall(
            f"SELECT {key_cols}, status, reason, changed_at "
            f"FROM {_TABLE} "
            f"ORDER BY changed_at, {key_cols}, CAST(status AS INTEGER)"
        )
    payload = json.dumps(
        [[str(v) if v is not None else None for v in row] for row in rows],
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def filter_excluded_combos(
    combos: list[dict],
    schema_keys_order: list[str],
    db: DatabaseManager,
) -> list[dict]:
    """Remove excluded combos from the list.

    Resolution algorithm (per design doc):
    1. Collect all rows in ``__scidb_schema_overrides`` whose non-NULL columns
       equal the combo's values at those keys (NULL = wildcard).
    2. Among those, pick the row with the most non-NULL columns (most specific);
       break ties by the most recent ``changed_at``.
    3. The combo is excluded iff that row's ``status = FALSE``.  A combo with
       no matching row is included (default state).

    Combos that contain schema-key columns not present in the combo dict
    (e.g., wider exclusions when only a subset of keys is iterated) are
    matched via wildcard: a NULL for that key in the override row will
    still match.

    Args:
        combos: List of combo dicts (e.g., ``[{"subject": "1", "trial": "2"}]``).
            Values should already be stringified (as produced by Step 9 in
            ``_for_each_prepare``).
        schema_keys_order: Ordered list of all schema keys from the DB.
        db: DatabaseManager.

    Backends without a DuckDB layer (no ``_duck``) cannot store overrides, so
    the combos are returned unchanged.

    Returns:
        Filtered list with excluded combos removed.
    """
    # Load all override rows once
    duck = _overrides_backend(db)
    if duck is None:
        return combos
    key_cols_sql = ", ".join(f'"{k}"' for k in schema_keys_order)
    rows = duck._fetchall(
        f"SELECT {key_cols_sql}, status, changed_at FROM {_TABLE} "
        f"ORDER BY changed_at DESC"
    )

    if not rows:
        return combos

    n_keys = len(schema_keys_order)

    # Parse rows: list of (specificity, changed_at, {key: val}, is_included)
    override_rows: list[tuple] = []
    for row in rows:
        key_values = {}
        for i, k in enumerate(schema_keys_order):
            v = row[i]
            if v is not None:
                key_values[k] = str(v)
        is_included = bool(row[n_keys])
        changed_at = row[n_keys + 1]
        specificity = len(key_values)
        override_rows.append((specificity, changed_at, key_values, is_included))

    result = []
    for combo in combos:
        combo_str = {k: str(v) for k, v in combo.items() if k in schema_keys_order}

        best_specificity = -1
        best_changed_at = None
        best_included = True  # default: included

        for specificity, changed_at, key_values, is_included in override_rows:
            if not all(combo_str.get(k) == v for k, v in key_values.items()):
                continue
            # This row matches the combo; check if it beats the current best
            if specificity > best_specificity or (
                specificity == best_specificity and changed_at > best_changed_at
            ):
                best_specificity = specificity
                best_changed_at = changed_at
                best_included = is_included

        if best_specificity == -1 or best_included:
            result.append(combo)
        # else: excluded — drop

    removed = len(combos) - len(result)
    if removed > 0:
        from .log import Log

        Log.info(
            f"[scidb] schema exclusions: removed {removed} excluded combo(s) "
            f"(from {len(combos)} to {len(result)})"
        )

    return result


def exclude_schema_dict(
    schema_keys: dict, reason: str, db: DatabaseManager | None = None
) -> None:
    """Dict-accepting entry point for MATLAB callers (wraps ``exclude_schema``)."""
    exclude_schema(reason=reason, db=db, **schema_keys)


def include_schema_dict(
    schema_keys: dict, reason: str, db: DatabaseManager | None = None
) -> None:
    """Dict-accepting entry point for MATLAB callers (wraps ``include_schema``)."""
    include_schema(reason=reason, db=db, **schema_keys)


def ensure_overrides_table(db: DatabaseManager) -> None:
    """Create ``__scidb_schema_overrides`` if it doesn't exist (idempotent)."""
    schema_cols = ",\n            ".join(
        f'"{k}" VARCHAR' for k in db.dataset_schema_keys
    )
    db._duck._execute(f"""
        CREATE TABLE IF NOT EXISTS {_TABLE} (
            {schema_cols},
            status     BOOLEAN   NOT NULL,
            reason     TEXT      NOT NULL,
            changed_at TIMESTAMP NOT NULL
        )
    """)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _get_db(db: DatabaseManager | None) -> DatabaseManager:
    if db is not None:
        return db
    from .database import get_database

    return get_database()


def _overrides_backend(db: DatabaseManager):
    """Return the DuckDB backend if this db can store schema overrides, else None.

    Schema exclusions live in the DuckDB table ``__scidb_schema_overrides``,
    which only the DuckDB-backed ``Database`` (with a ``_duck`` attribute)
    provides.  Remote/net backends and test doubles have no such table, so
    callers degrade to the "no overrides" default instead of raising
    ``AttributeError``.  A real ``Database`` always creates the table at init,
    so a missing backend never masks a genuine setup bug.
    """
    duck = getattr(db, "_duck", None)
    if duck is None:
        from .log import Log

        Log.debug(
            f"[scidb] schema overrides unavailable: {type(db).__name__} has no "
            f"_duck backend; treating as no exclusions"
        )
    return duck


def _validate_keys(db: DatabaseManager, keys: dict) -> None:
    unknown = set(keys) - set(db.dataset_schema_keys)
    if unknown:
        raise ValueError(
            f"Unknown schema key(s): {sorted(unknown)}. "
            f"Valid schema keys: {db.dataset_schema_keys}"
        )


def _current_status(db: DatabaseManager, keyset: dict) -> bool | None:
    """Return the effective status of the exact keyset, or None if no row exists.

    Matches rows where the specified keys have the given values AND all
    unspecified schema keys are NULL.

    Returns:
        True  = currently included (most recent row has status=TRUE)
        False = currently excluded (most recent row has status=FALSE)
        None  = no row with this exact keyset on file (implicitly included)
    """
    schema_keys = db.dataset_schema_keys
    where_parts = []
    params = []
    for k in schema_keys:
        if k in keyset:
            where_parts.append(f'"{k}" = ?')
            params.append(str(keyset[k]))
        else:
            where_parts.append(f'"{k}" IS NULL')

    where_clause = " AND ".join(where_parts)
    rows = db._duck._fetchall(
        f"SELECT status FROM {_TABLE} "
        f"WHERE {where_clause} "
        f"ORDER BY changed_at DESC LIMIT 1",
        params or None,
    )
    if not rows:
        return None
    return bool(rows[0][0])


def _insert_row(
    db: DatabaseManager,
    schema_keys: dict,
    status: bool,
    reason: str,
) -> None:
    all_keys = db.dataset_schema_keys
    col_names = list(all_keys) + ["status", "reason", "changed_at"]
    values: list = []
    for k in all_keys:
        v = schema_keys.get(k)
        values.append(str(v) if v is not None else None)
    values += [status, reason, datetime.now(timezone.utc)]

    placeholders = ", ".join(["?"] * len(col_names))
    col_str = ", ".join(f'"{c}"' for c in col_names)
    db._duck._execute(
        f"INSERT INTO {_TABLE} ({col_str}) VALUES ({placeholders})",
        values,
    )
