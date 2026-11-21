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

**Scoped exclusions** (``variable=``, GitHub #28; stored in the
``scoped_to`` column, since a schema key may itself be named ``variable``).
A row with a scope
says "this variable is bad or absent here"; a row without one is a bad
trial, out of everything. Asked about variable V, the rows that compete are
the unscoped ones and those scoped to V, under the same most-specific rule.
A step skips a location excluded for any variable it reads or writes, and
its outputs read excluded "inherited from <input>" there
(:func:`governing_exclusions`). "Mark missing as expected" is a scoped
exclusion with :data:`MARKED_MISSING_REASON`, "mark present as expected" a
scoped include with :data:`MARKED_PRESENT_REASON` (:func:`mark_missing_expected`,
:func:`mark_present_expected`).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from scidb.store.tables import SCHEMA_OVERRIDES

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

_TABLE = SCHEMA_OVERRIDES.name

#: The reason the "Mark missing as expected" preset writes (a scoped exclusion).
MARKED_MISSING_REASON = "marked missing as expected"
#: The reason the "Mark present as expected" preset writes (a scoped include).
MARKED_PRESENT_REASON = "marked present as expected"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def exclude_schema(
    reason: str, db: DatabaseManager | None = None, variable=None, **schema_keys
) -> None:
    """Mark a schema-key combination as permanently excluded from analyses.

    The record is stored in the database and consulted by ``for_each`` before
    iterating.  Omitted schema keys act as wildcards (NULL in the table),
    so ``exclude_schema(subject=3, reason=...)`` excludes *every* trial of
    subject 3.

    Args:
        reason: Human-readable explanation of why this data is excluded.
        db: DatabaseManager (defaults to ``get_database()``).
        variable: Optional variable (class or name) the exclusion is scoped
            to: only that variable is bad or absent here. Every step that
            reads it skips the location. ``None`` = out of everything.
        **schema_keys: Schema key values identifying the combination to exclude.

    Raises:
        ValueError: If no schema keys are given, a key is unknown, or the
            exact keyset is already excluded (for the same scope).
    """
    db = _get_db(db)
    if not schema_keys:
        raise ValueError("At least one schema key must be specified.")
    _validate_keys(db, schema_keys)
    _check_scope_keyword(db, variable)
    scope = variable_scope(variable)

    status = _current_status(db, schema_keys, scope)
    if status is False:
        raise ValueError(
            f"Schema combination {schema_keys!r}"
            + (f" for {scope}" if scope else "")
            + " is already excluded. "
            "Call include_schema() first if you want to re-exclude it with a "
            "different reason."
        )

    _insert_row(db, schema_keys, status=False, reason=reason, variable=scope)
    from scidb.model.log import Log

    Log.info(
        f"[scidb] exclude_schema: {schema_keys} scope={scope or '(everything)'} "
        f"reason={reason!r}"
    )


def include_schema(
    reason: str, db: DatabaseManager | None = None, variable=None, **schema_keys
) -> None:
    """Re-include a schema-key combination that is currently excluded.

    The exclusion may be on this exact keyset or on a coarser one: after
    ``exclude_schema(subject=3)``, ``include_schema(subject=3, trial=2)``
    re-includes only trial 2 (a more specific row wins), and the rest of
    subject 3 stays excluded. The record is stored in the database (the
    original exclusion row is NOT deleted — the full history is preserved).

    Args:
        reason: Human-readable explanation of why this data is re-included.
        db: DatabaseManager (defaults to ``get_database()``).
        variable: Optional variable (class or name) the include is scoped to:
            it re-includes the location for that variable only, against an
            exclusion of everything or of that variable.
        **schema_keys: Schema key values identifying the combination to include.

    Raises:
        ValueError: If the keyset is already included in effect: no
            exclusion covers it, or the row that governs it is an include
            (:func:`governing_overrides`). The exact keyset's own row, when
            there is one, always governs, so this is the old exact-keyset
            guard whenever such a row exists.
    """
    db = _get_db(db)
    if not schema_keys:
        raise ValueError("At least one schema key must be specified.")
    _validate_keys(db, schema_keys)

    _check_scope_keyword(db, variable)
    keyset = {k: str(v) for k, v in schema_keys.items()}
    scope = variable_scope(variable)
    (governing,) = governing_overrides(
        [keyset], db.dataset_schema_keys, db, variable=scope
    )
    from scidb.model.log import Log

    Log.debug(
        f"[scidb] include_schema: keyset={keyset} scope={scope or '(everything)'} governed by "
        + (
            f"{governing.keys} (included={governing.included}, "
            f"reason={governing.reason!r})"
            if governing is not None
            else "nothing (default: included)"
        )
    )
    if governing is None:
        raise ValueError(
            f"Schema combination {schema_keys!r} has no exclusion record and "
            "no exclusion covers it, so it is already included. "
            "Call exclude_schema() first."
        )
    if governing.included:
        raise ValueError(f"Schema combination {schema_keys!r} is already included.")

    _insert_row(db, schema_keys, status=True, reason=reason, variable=scope)


def mark_missing_expected(
    variable, db: DatabaseManager | None = None, **schema_keys
) -> None:
    """"Mark missing as expected": *variable* is absent here on purpose.

    A preset of :func:`exclude_schema` scoped to *variable* with
    :data:`MARKED_MISSING_REASON`. It removes the location from every step
    that reads *variable* from now on; the user can include it again later.
    """
    exclude_schema(MARKED_MISSING_REASON, db=db, variable=variable, **schema_keys)


def mark_present_expected(
    variable, db: DatabaseManager | None = None, **schema_keys
) -> bool:
    """"Mark present as expected": *variable* belongs here.

    A preset of :func:`include_schema` scoped to *variable* with
    :data:`MARKED_PRESENT_REASON`. Returns False (and writes nothing) when
    nothing excludes the location for *variable*: it is already expected.
    """
    db = _get_db(db)
    _validate_keys(db, schema_keys)
    scope = variable_scope(variable)
    (governing,) = governing_overrides(
        [{k: str(v) for k, v in schema_keys.items()}],
        db.dataset_schema_keys, db, variable=scope,
    )
    if governing is None or governing.included:
        from scidb.model.log import Log

        Log.info(
            f"[scidb] mark_present_expected: {scope} at {schema_keys} is not "
            f"excluded; nothing to write"
        )
        return False
    include_schema(MARKED_PRESENT_REASON, db=db, variable=scope, **schema_keys)
    return True


def variable_scope(variable) -> str | None:
    """The stored spelling of an exclusion's scope: a variable's type name,
    or None (everything). The one reader of ``variable=``."""
    # Never `variable == ""`: a variable class overloads == to build a
    # (truthy) VariableFilter, which would turn every class scope into None.
    if variable is None or (isinstance(variable, str) and variable == ""):
        return None
    name = getattr(variable, "__name__", variable)
    if not isinstance(name, str):
        raise TypeError(
            f"variable= must be a variable class or its name, got {type(variable).__name__}"
        )
    return name


def list_exclusions(db: DatabaseManager | None = None):
    """Return a DataFrame of currently-excluded schema combinations.

    Shows the latest row per exact keyset (and scope) where the effective
    status is excluded (``status = FALSE``). ``scoped_to`` is the scope
    (None = everything).

    Args:
        db: DatabaseManager (defaults to ``get_database()``).

    Returns:
        pandas.DataFrame with schema-key columns plus ``scoped_to`` (the
        variable the exclusion is scoped to, or None), ``reason`` and
        ``changed_at`` columns.
    """
    db = _get_db(db)
    schema_keys = db.dataset_schema_keys
    key_cols = ", ".join(f'"{k}"' for k in schema_keys)

    rows = db._duck.fetchdf(
        f"""
        WITH latest AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {key_cols}, scoped_to
                       ORDER BY changed_at DESC
                   ) AS rn
            FROM {_TABLE}
        )
        SELECT {key_cols}, scoped_to, reason, changed_at
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
        rows = duck.fetchall(
            f"SELECT {key_cols}, scoped_to, status, reason, changed_at "
            f"FROM {_TABLE} "
            f"ORDER BY changed_at, {key_cols}, scoped_to, CAST(status AS INTEGER)"
        )
    payload = json.dumps(
        [[str(v) if v is not None else None for v in row] for row in rows],
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class Override:
    """One ``__scidb_schema_overrides`` row: the keyset it names (NULL columns
    left out), whether it includes or excludes, why, and when."""

    keys: dict[str, str]
    included: bool
    reason: str
    changed_at: datetime
    #: The variable it is scoped to; None = everything.
    variable: str | None = None

    def to_dict(self) -> dict:
        return {
            "keys": dict(self.keys),
            "included": self.included,
            "reason": self.reason,
            "changed_at": self.changed_at.isoformat() if self.changed_at else None,
            "variable": self.variable,
        }


def governing_overrides(
    combos: list[dict],
    schema_keys_order: list[str],
    db: DatabaseManager,
    variable: str | None = None,
) -> list[Override | None]:
    """The override row that decides each combo, or None. One per combo, in order.

    The one owner of "which exclusion applies here" — :func:`exclusion_status`
    (and through it ``for_each`` and ``scidb.provenance.inventory``), the
    ``include_schema`` guard and the GUI inventory popup all ask this.

    Resolution algorithm (per design doc):
    1. Collect all rows in ``__scidb_schema_overrides`` whose non-NULL columns
       equal the combo's values at those keys (NULL = wildcard), and whose
       scope is everything or *variable*.
    2. Among those, pick the row with the most non-NULL columns (most specific);
       break ties by the most recent ``changed_at``.
    3. No matching row = None (the default state, included).

    *variable* None asks about everything: only unscoped rows compete, which
    is exactly the pre-#28 answer.

    A combo may name only some of the schema keys (a coarser iteration). An
    override row that names a key the combo lacks does NOT match it: excluding
    ``trial=3`` never excludes the whole ``subject/session`` above it.

    Backends without a DuckDB layer (no ``_duck``), and databases that predate
    the overrides table, have no overrides: every combo reads None.
    """
    rows = _override_rows(db, schema_keys_order)
    if not rows:
        return [None] * len(combos)
    scope = variable_scope(variable)
    competing = [o for o in rows if o.variable is None or o.variable == scope]
    return [
        most_specific_match(
            competing, {k: v for k, v in combo.items() if k in schema_keys_order}
        )
        for combo in combos
    ]


def _override_rows(db: DatabaseManager, schema_keys_order: list[str]) -> list[Override]:
    """Every override row, newest first (``[]`` without a table)."""
    duck = _overrides_backend(db)
    if duck is None:
        return []
    table_exists = getattr(duck, "table_exists", None)
    if table_exists is not None and not table_exists(_TABLE):
        from scidb.model.log import Log

        Log.debug(f"[scidb] schema overrides: no {_TABLE} table; no exclusions")
        return []
    key_cols_sql = ", ".join(f'"{k}"' for k in schema_keys_order)
    rows = duck.fetchall(
        f"SELECT {key_cols_sql}, status, reason, changed_at, scoped_to FROM {_TABLE} "
        f"ORDER BY changed_at DESC"
    )
    n_keys = len(schema_keys_order)
    out = []
    for row in rows:
        out.append(Override(
            keys={k: str(row[i]) for i, k in enumerate(schema_keys_order) if row[i] is not None},
            included=bool(row[n_keys]),
            reason=row[n_keys + 1],
            changed_at=row[n_keys + 2],
            variable=row[n_keys + 3],
        ))
    return out


@dataclass(frozen=True)
class Exclusion:
    """Why one location is left out for a step: the row that excludes it,
    and the variable whose scope it came from (``inherited_from`` names an
    input of the step when that is where the exclusion lives)."""

    override: Override
    #: The variable the excluding scope belongs to; None = everything.
    scope: str | None

    @property
    def reason(self) -> str:
        return self.override.reason

    def inherited_from(self, variable: str | None) -> str | None:
        """The input this exclusion is inherited from, when asked about
        *variable* (an output): None when it is global or *variable*'s own."""
        if self.scope is None or self.scope == variable:
            return None
        return self.scope

    def describe(self, variable: str | None = None) -> str:
        source = self.inherited_from(variable)
        text = self.override.reason
        return f"{text} (inherited from {source})" if source else text

    def to_dict(self, variable: str | None = None) -> dict:
        return {
            **self.override.to_dict(),
            "scope": self.scope,
            "inherited_from": self.inherited_from(variable),
            "description": self.describe(variable),
        }


def governing_exclusions(
    combos: list[dict],
    schema_keys_order: list[str],
    db: DatabaseManager,
    variables=None,
) -> list[Exclusion | None]:
    """The exclusion that leaves each combo out for a step, or None.

    *variables*: every variable the step reads or writes (names or classes).
    A combo is excluded when the row governing it is an exclusion for
    everything, or for any one of *variables* (each scope asked by
    :func:`governing_overrides`, the matching rules' owner). Scopes are
    asked in order, everything first, so a global exclusion is reported
    before an inherited one. ``None``/empty = everything only.
    """
    rows = _override_rows(db, schema_keys_order)
    if not rows:
        return [None] * len(combos)
    scopes: list[str | None] = [None]
    for v in variables or ():
        s = variable_scope(v)
        if s is not None and s not in scopes:
            scopes.append(s)
    by_scope = {
        s: [o for o in rows if o.variable is None or o.variable == s] for s in scopes
    }
    out: list[Exclusion | None] = []
    for combo in combos:
        c = {k: v for k, v in combo.items() if k in schema_keys_order}
        found = None
        for s in scopes:
            o = most_specific_match(by_scope[s], c)
            if o is not None and not o.included:
                found = Exclusion(o, o.variable)
                break
        out.append(found)
    return out


def most_specific_match(rows, combo: dict):
    """The row that governs *combo*, or None: THE wildcard matching rule.

    Each row has ``keys`` (``{key: value}``, keys it leaves out are
    wildcards) and ``changed_at``. A row matches when every one of its keys
    equals the combo's value there; a row naming a key the combo lacks does
    not match. Among matches the row naming the most keys wins, and a tie
    goes to the newest ``changed_at``.

    Shared by :func:`governing_overrides` (exclusions) and
    ``scidb.provenance.expectation`` (Expected / Not expected marks), so the
    two kinds of keyed statement can never resolve differently.
    """
    combo_str = {k: str(v) for k, v in combo.items() if v is not None}
    best = None
    best_specificity = -1
    for row in rows:
        if not all(combo_str.get(k) == v for k, v in row.keys.items()):
            continue
        specificity = len(row.keys)
        if specificity > best_specificity or (
            specificity == best_specificity and row.changed_at > best.changed_at
        ):
            best_specificity = specificity
            best = row
    return best


def override_history(db: DatabaseManager | None = None) -> list[Override]:
    """Every exclusion and re-inclusion ever written, oldest first. The
    table is append-only, so this is when and why each combination changed."""
    db = _get_db(db)
    duck = _overrides_backend(db)
    if duck is None or not duck.table_exists(_TABLE):
        return []
    schema_keys = list(db.dataset_schema_keys)
    key_cols = ", ".join(f'"{k}"' for k in schema_keys)
    rows = duck.fetchall(
        f"SELECT {key_cols}, status, reason, changed_at, scoped_to FROM {_TABLE} "
        f"ORDER BY changed_at, rowid"
    )
    n = len(schema_keys)
    return [
        Override(
            keys={k: str(v) for k, v in zip(schema_keys, row[:n]) if v is not None},
            included=bool(row[n]),
            reason=row[n + 1],
            changed_at=row[n + 2],
            variable=row[n + 3],
        )
        for row in rows
    ]


def exclusion_status(
    combos: list[dict],
    schema_keys_order: list[str],
    db: DatabaseManager,
    variables=None,
) -> list[bool]:
    """Is each combo excluded? One bool per combo, in order.

    A consumer of :func:`governing_exclusions` (and through it
    :func:`governing_overrides`, which owns the matching rules). *variables*
    as there: the step's variables, whose scoped exclusions count too.
    ``filter_excluded_combos`` (for_each) and ``scidb.provenance.inventory`` ask this.
    """
    return [e is not None for e in governing_exclusions(combos, schema_keys_order, db, variables)]


def filter_excluded_combos(
    combos: list[dict],
    schema_keys_order: list[str],
    db: DatabaseManager,
    variables=None,
) -> list[dict]:
    """Remove excluded combos from the list (``for_each``'s consumer of
    :func:`exclusion_status`, which owns the matching rules).

    Args:
        combos: List of combo dicts (e.g., ``[{"subject": "1", "trial": "2"}]``).
            Values should already be stringified (as produced by Step 9 in
            ``_for_each_prepare``).
        schema_keys_order: Ordered list of all schema keys from the DB.
        db: DatabaseManager.
        variables: The variables the step reads and writes: a location
            excluded for any one of them is removed too (scoped exclusions).

    Returns:
        Filtered list with excluded combos removed.
    """
    found = governing_exclusions(combos, schema_keys_order, db, variables)
    result = [c for c, ex in zip(combos, found) if ex is None]

    removed = len(combos) - len(result)
    if removed > 0:
        from collections import Counter

        from scidb.model.log import Log

        by_scope = Counter(e.scope or "(everything)" for e in found if e is not None)
        Log.info(
            f"[scidb] schema exclusions: removed {removed} excluded combo(s) "
            f"(from {len(combos)} to {len(result)}); by scope {dict(by_scope)}"
        )

    return result



def exclude_schema_dict(
    schema_keys: dict, reason: str, db: DatabaseManager | None = None, variable=None
) -> None:
    """Dict-accepting entry point for MATLAB callers (wraps ``exclude_schema``).
    Also the way to scope an exclusion in a dataset that has a schema key
    literally named ``variable``."""
    _insert_checked(schema_keys, reason, db, variable, exclude=True)


def include_schema_dict(
    schema_keys: dict, reason: str, db: DatabaseManager | None = None, variable=None
) -> None:
    """Dict-accepting entry point for MATLAB callers (wraps ``include_schema``)."""
    _insert_checked(schema_keys, reason, db, variable, exclude=False)


def _insert_checked(schema_keys: dict, reason: str, db, variable, exclude: bool) -> None:
    fn = exclude_schema if exclude else include_schema
    if "variable" in schema_keys:
        # A schema key named "variable" cannot travel as a keyword next to
        # the scope keyword; pass it through the validated internals.
        db = _get_db(db)
        _validate_keys(db, schema_keys)
        _insert_row(db, schema_keys, status=not exclude, reason=reason,
                    variable=variable_scope(variable))
        return
    fn(reason=reason, db=db, variable=variable, **schema_keys)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _get_db(db: DatabaseManager | None) -> DatabaseManager:
    if db is not None:
        return db
    from scidb.store.database import get_database

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
        from scidb.model.log import Log

        Log.debug(
            f"[scidb] schema overrides unavailable: {type(db).__name__} has no "
            f"_duck backend; treating as no exclusions"
        )
    return duck


def _check_scope_keyword(db: DatabaseManager, variable) -> None:
    """``variable=`` is the scope; a dataset with a schema KEY of that name
    must say which it means through the ``*_dict`` entry points."""
    if variable is not None and "variable" in db.dataset_schema_keys:
        raise ValueError(
            "This dataset has a schema key named 'variable', so variable= is "
            "ambiguous. Use exclude_schema_dict(schema_keys, reason, "
            "variable=<scope>) / include_schema_dict(...) instead."
        )


def _validate_keys(db: DatabaseManager, keys: dict) -> None:
    unknown = set(keys) - set(db.dataset_schema_keys)
    if unknown:
        raise ValueError(
            f"Unknown schema key(s): {sorted(unknown)}. "
            f"Valid schema keys: {db.dataset_schema_keys}"
        )


def _current_status(
    db: DatabaseManager, keyset: dict, variable: str | None = None
) -> bool | None:
    """Return the effective status of the exact keyset, or None if no row exists.

    Matches rows where the specified keys have the given values AND all
    unspecified schema keys are NULL, in the same scope (*variable*).

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

    if variable is None:
        where_parts.append("scoped_to IS NULL")
    else:
        where_parts.append("scoped_to = ?")
        params.append(variable)
    where_clause = " AND ".join(where_parts)
    rows = db._duck.fetchall(
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
    variable: str | None = None,
) -> None:
    all_keys = db.dataset_schema_keys
    col_names = list(all_keys) + ["status", "reason", "changed_at", "scoped_to"]
    values: list = []
    for k in all_keys:
        v = schema_keys.get(k)
        values.append(str(v) if v is not None else None)
    values += [status, reason, datetime.now(timezone.utc), variable]

    placeholders = ", ".join(["?"] * len(col_names))
    col_str = ", ".join(f'"{c}"' for c in col_names)
    db._duck.execute(
        f"INSERT INTO {_TABLE} ({col_str}) VALUES ({placeholders})",
        values,
    )
