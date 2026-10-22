"""Declared schema-key types, stored in the database.

A schema key may be declared ``"numeric"`` or ``"string"``; undeclared keys
keep their spelling and have their load type inferred
(``docs/claude/schema-key-types.md``). The declaration decides how values are
STORED (``"001"`` becomes ``1`` under ``numeric``), so it is a property of the
dataset and lives with it, in the database, like the schema keys themselves
(user decision 2026-10-21). A ``scistack.toml`` table was rejected: it does
not travel with the ``.duckdb`` and can disagree with the rows already stored.

**One owner.** This module is the only reader and writer of the declaration.
``DatabaseManager.dataset_schema_key_types`` is a read-only view over what
:func:`read_types` returned, and there are three ways to write:

* :func:`declare` — ``configure_database(schema_key_types=...)``. Stores a
  type for a key that has none, does nothing when the same type is stored,
  and raises :class:`SchemaKeyTypeConflictError` when a different one is: a
  script never silently overrides a choice made in the GUI, nor the reverse.
* :func:`set_type` — the GUI checkbox and the ``scidb`` CLI. Changes or
  clears a declaration.
* Both refuse a change that would re-spell stored values unless the caller
  confirmed it (:class:`SchemaKeyTypeChangeError` carries the
  :func:`impact`). Nothing is migrated: rows already stored keep their
  spelling, so ``"001"`` (old) and ``1`` (new) become different locations
  until the loaders are re-run.

Storage: ``_schema_key_type``, append-only. A change adds a row and the
newest row per key is the current declaration (``key_type`` NULL = cleared),
so "when did trial become numeric" stays answerable from the database itself.
No user identity is recorded (feedback_no_user_identity).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .exceptions import (
    SchemaKeyTypeChangeError,
    SchemaKeyTypeConflictError,
    SchemaKeyTypeError,
)
from .log import Log
from .schema_values import VALID_SCHEMA_KEY_TYPES, canonical_numeric_value, schema_str

if TYPE_CHECKING:
    from .database import DatabaseManager

TABLE = "_schema_key_type"

#: Values shown per list in error messages and log lines.
_PREVIEW = 5


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def ensure_table(duck) -> None:
    """Create ``_schema_key_type`` if missing (idempotent; skipped read-only)."""
    duck._execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            key         VARCHAR NOT NULL,
            key_type    VARCHAR,
            declared_at VARCHAR NOT NULL,
            source      VARCHAR NOT NULL
        )
    """)


def read_types(duck) -> dict[str, str]:
    """The current declaration ``{key: "numeric" | "string"}``.

    The newest row per key wins; a NULL ``key_type`` (cleared) leaves the key
    out. ``{}`` when the table does not exist yet — a database created before
    2026-10-21 opened read-only, where :func:`ensure_table` cannot run.
    """
    if not duck._table_exists(TABLE):
        return {}
    rows = duck._fetchall(f"""
        SELECT key, key_type FROM (
            SELECT key, key_type,
                   ROW_NUMBER() OVER (
                       PARTITION BY key ORDER BY declared_at DESC, rowid DESC
                   ) AS rn
            FROM {TABLE}
        ) WHERE rn = 1 AND key_type IS NOT NULL
    """)
    return {key: key_type for key, key_type in rows}


def history(duck, key: "str | None" = None) -> list[dict]:
    """Every declaration ever written, oldest first (optionally one key)."""
    if not duck._table_exists(TABLE):
        return []
    where, params = ("WHERE key = ?", [key]) if key is not None else ("", None)
    rows = duck._fetchall(
        f"SELECT key, key_type, declared_at, source FROM {TABLE} {where} "
        f"ORDER BY declared_at, rowid",
        params,
    )
    return [
        {"key": k, "key_type": t, "declared_at": at, "source": src}
        for k, t, at, src in rows
    ]


def _write(duck, key: str, key_type: "str | None", source: str) -> None:
    duck._execute(
        f"INSERT INTO {TABLE} (key, key_type, declared_at, source) VALUES (?, ?, ?, ?)",
        [key, key_type, datetime.now(timezone.utc).isoformat(), source],
    )


# ---------------------------------------------------------------------------
# Validation and impact
# ---------------------------------------------------------------------------


def validate(types: dict, schema_keys: list[str]) -> dict[str, str]:
    """Reject keys that are not schema keys and types that are not
    ``"numeric"`` / ``"string"``. Returns a plain-dict copy."""
    types = dict(types or {})
    unknown_keys = set(types) - set(schema_keys)
    if unknown_keys:
        raise ValueError(
            f"schema_key_types declares keys that are not schema keys: "
            f"{sorted(unknown_keys)}. Schema keys: {list(schema_keys)}"
        )
    bad_types = {k: t for k, t in types.items() if t not in VALID_SCHEMA_KEY_TYPES}
    if bad_types:
        raise ValueError(
            f"schema_key_types values must be one of "
            f"{VALID_SCHEMA_KEY_TYPES}, got: {bad_types}"
        )
    return types


def impact(db: "DatabaseManager", key: str, new_type: "str | None") -> dict:
    """What declaring *key* as *new_type* would do to the values already stored.

    Only a change TO ``"numeric"`` re-spells anything: every stored value is
    put through the one conversion rule (``canonical_numeric_value``, then
    ``schema_str``) and compared with its stored spelling. ``"string"`` and
    ``None`` keep every spelling (loads may change type; storage does not).

    Returns ``{key, from, to, n_values, respelled: [[old, new], ...],
    merged: {new: [old, ...]}, invalid: [old, ...]}``. ``merged`` lists the
    canonical spellings that two or more stored spellings would share (``"1"``
    and ``"001"`` both becoming ``"1"``): from then on those are one location
    for new saves while the old rows stay apart.
    """
    validate({key: new_type} if new_type is not None else {}, db.dataset_schema_keys)
    if key not in db.dataset_schema_keys:
        raise ValueError(
            f"'{key}' is not a schema key. Schema keys: {db.dataset_schema_keys}"
        )
    values = [v for v in db._duck.distinct_schema_values(key) if v is not None]
    respelled: list[list[str]] = []
    invalid: list[str] = []
    by_canonical: dict[str, list[str]] = {}
    if new_type == "numeric":
        for stored in values:
            try:
                canonical = schema_str(canonical_numeric_value(key, stored))
            except SchemaKeyTypeError:
                invalid.append(stored)
                continue
            by_canonical.setdefault(canonical, []).append(stored)
            if canonical != stored:
                respelled.append([stored, canonical])
    merged = {c: olds for c, olds in by_canonical.items() if len(olds) > 1}
    return {
        "key": key,
        "from": db.dataset_schema_key_types.get(key),
        "to": new_type,
        "n_values": len(values),
        "respelled": respelled,
        "merged": merged,
        "invalid": invalid,
    }


def _describe_impact(imp: dict) -> str:
    parts = []
    if imp["respelled"]:
        shown = ", ".join(f"{o!r} -> {n!r}" for o, n in imp["respelled"][:_PREVIEW])
        more = len(imp["respelled"]) - _PREVIEW
        parts.append(
            f"{len(imp['respelled'])} of {imp['n_values']} stored value(s) would be "
            f"spelled differently by new saves ({shown}{', ...' if more > 0 else ''})"
        )
    if imp["merged"]:
        shown = "; ".join(
            f"{olds} -> {c!r}" for c, olds in list(imp["merged"].items())[:_PREVIEW]
        )
        parts.append(f"{len(imp['merged'])} location(s) would merge ({shown})")
    return "; ".join(parts)


def _check_change(imp: dict, confirm_respelling: bool, how_to_confirm: str) -> None:
    key = imp["key"]
    if imp["invalid"]:
        raise SchemaKeyTypeError(
            f"Schema key '{key}' cannot be declared numeric: "
            f"{len(imp['invalid'])} stored value(s) are not numbers "
            f"(e.g. {imp['invalid'][:_PREVIEW]}). Fix or delete those records first."
        )
    if (imp["respelled"] or imp["merged"]) and not confirm_respelling:
        raise SchemaKeyTypeChangeError(
            f"Declaring schema key '{key}' {imp['to']!r} changes how it is stored: "
            f"{_describe_impact(imp)}. Rows already saved keep their spelling, so "
            f"re-run the functions that save '{key}' afterwards. {how_to_confirm}",
            imp,
        )


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def declare(
    db: "DatabaseManager",
    types: dict,
    *,
    source: str = "configure_database",
    dry_run: bool = False,
) -> None:
    """Apply ``schema_key_types=...`` from a call against the stored
    declaration: store what is missing, ignore what matches, raise on a
    difference. *types* must already be :func:`validate`-d. *dry_run* runs
    every check and logs what would be stored, but writes nothing."""
    stored = db.dataset_schema_key_types
    conflicts = {
        k: (stored[k], t) for k, t in types.items() if k in stored and stored[k] != t
    }
    if conflicts:
        detail = ", ".join(
            f"'{k}' is stored as {s!r} but declared {t!r}" for k, (s, t) in conflicts.items()
        )
        raise SchemaKeyTypeConflictError(
            f"schema_key_types disagrees with the database {db.dataset_db_path.name}: "
            f"{detail}. The database owns this setting. Remove the argument, or "
            f"change the stored type with the GUI's Numeric checkbox or "
            f"db.set_schema_key_type(key, type)."
        )
    missing = {k: t for k, t in types.items() if k not in stored}
    for key in sorted(set(types) - set(missing)):
        Log.debug(f"schema key types: '{key}' already stored as {types[key]!r}; nothing to do")
    if not missing:
        return
    if db.read_only and not dry_run:
        raise SchemaKeyTypeConflictError(
            f"schema_key_types={missing} is not stored in {db.dataset_db_path.name}, "
            f"and the database is open read-only, so it cannot be recorded."
        )
    for key, key_type in sorted(missing.items()):
        _check_change(
            impact(db, key, key_type),
            confirm_respelling=False,
            how_to_confirm=(
                f"To accept that, call db.set_schema_key_type({key!r}, {key_type!r}, "
                f"confirm_respelling=True) once."
            ),
        )
    if dry_run:
        Log.info(
            f"schema key types: dry run, would store {describe(missing)} (source={source})"
        )
        return
    for key, key_type in sorted(missing.items()):
        _write(db._duck, key, key_type, source)
        Log.info(f"schema key types: stored '{key}' = {key_type!r} (source={source})")
    db._refresh_schema_key_types()


def set_type(
    db: "DatabaseManager",
    key: str,
    key_type: "str | None",
    *,
    confirm_respelling: bool = False,
    source: str = "api",
) -> dict:
    """Declare *key* as ``"numeric"`` / ``"string"``, or clear it with ``None``.

    Returns the :func:`impact` of the change. Raises ``SchemaKeyTypeError``
    when stored values cannot be numbers, and :class:`SchemaKeyTypeChangeError`
    when the change re-spells stored values and *confirm_respelling* is False.
    Setting the type already stored is a no-op (nothing written).
    """
    if db.read_only:
        raise ValueError(
            f"Cannot change schema key types: {db.dataset_db_path.name} is open read-only."
        )
    imp = impact(db, key, key_type)
    if imp["from"] == key_type:
        Log.debug(f"schema key types: '{key}' is already {key_type!r}; nothing written")
        return imp
    _check_change(
        imp,
        confirm_respelling,
        how_to_confirm="Pass confirm_respelling=True to accept.",
    )
    _write(db._duck, key, key_type, source)
    db._refresh_schema_key_types()
    db._forget_schema_key_kinds()
    Log.info(
        f"schema key types: '{key}' {imp['from']!r} -> {key_type!r} (source={source}); "
        f"{imp['n_values']} stored value(s)"
        + (f"; {_describe_impact(imp)}" if imp["respelled"] or imp["merged"] else "")
    )
    return imp


def describe(types: dict[str, Any]) -> str:
    """``trial=numeric, subject=string`` or ``none declared`` — for log lines."""
    if not types:
        return "none declared"
    return ", ".join(f"{k}={t}" for k, t in sorted(types.items()))
