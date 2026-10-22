"""Schema-key types (numeric / string) for the GUI — the "Numeric" checkbox.

The database owns the declaration (``scidb.schema_types``, user decision
2026-10-21); this module only adapts it for the sidebar. Nothing here decides
a type or converts a value:

* :func:`get_schema_key_types` — every schema key with its stored type, how it
  currently LOADS (numbers or strings, ``DatabaseManager.schema_key_is_numeric``)
  and how many values it has;
* :func:`schema_key_type_impact` — ``scidb.schema_types.impact``, unchanged;
* :func:`set_schema_key_type` — the checkbox write. A change that re-spells
  stored values comes back ``needs_confirm`` with the impact instead of
  raising, so the sidebar can ask; the confirmed call passes
  ``confirm_respelling=True``;
* :func:`auto_declare_from_key_regex` — user decision Q2: saving a digit-only
  ``key_regex`` through the GUI for an UNDECLARED schema key declares it
  numeric (``scifor.key_regex_is_digit_only``). Only when that re-spells
  nothing; otherwise it returns a hint and the user ticks the box.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: ``source`` recorded in ``_schema_key_type`` for each GUI route.
SOURCE_CHECKBOX = "gui"
SOURCE_KEY_REGEX = "gui:key_regex"


def get_schema_key_types(db) -> dict:
    """``{schema_keys, key_types, keys: [{key, key_type, loads_as, n_values}]}``."""
    key_types = dict(db.dataset_schema_key_types)
    keys = []
    for key in db.dataset_schema_keys:
        n_values = len(db._duck.distinct_schema_values(key))
        keys.append(
            {
                "key": key,
                "key_type": key_types.get(key),
                "loads_as": "numbers" if db.schema_key_is_numeric(key) else "strings",
                "n_values": n_values,
            }
        )
    logger.info(
        "[schema_key_types] get_schema_key_types: %s",
        ", ".join(f"{k['key']}={k['key_type'] or 'undeclared'}" for k in keys) or "no keys",
    )
    return {"schema_keys": list(db.dataset_schema_keys), "key_types": key_types, "keys": keys}


def schema_key_type_impact(db, key: str, key_type: "str | None") -> dict:
    """What declaring *key* as *key_type* would do (nothing is written)."""
    impact = db.schema_key_type_impact(key, key_type)
    logger.info(
        "[schema_key_types] impact %s -> %r: %d respelled, %d merged, %d invalid of %d",
        key,
        key_type,
        len(impact["respelled"]),
        len(impact["merged"]),
        len(impact["invalid"]),
        impact["n_values"],
    )
    return impact


def set_schema_key_type(
    db, key: str, key_type: "str | None", confirm_respelling: bool = False
) -> dict:
    """Write the checkbox. Returns ``{ok, key_types, impact}``; a change that
    needs confirmation returns ``{ok: False, needs_confirm: True, impact,
    error}``; values that cannot be numbers return ``{ok: False, error,
    impact}``."""
    from scidb import SchemaKeyTypeChangeError, SchemaKeyTypeError

    try:
        impact = db.set_schema_key_type(
            key, key_type, confirm_respelling=confirm_respelling, source=SOURCE_CHECKBOX
        )
    except SchemaKeyTypeChangeError as exc:
        logger.info("[schema_key_types] set %s -> %r needs confirmation: %s", key, key_type, exc)
        return {"ok": False, "needs_confirm": True, "error": str(exc), "impact": exc.impact}
    except SchemaKeyTypeError as exc:
        logger.info("[schema_key_types] set %s -> %r refused: %s", key, key_type, exc)
        return {
            "ok": False,
            "error": str(exc),
            "impact": db.schema_key_type_impact(key, key_type),
        }
    logger.info("[schema_key_types] set %s -> %r (confirm=%s)", key, key_type, confirm_respelling)
    return {"ok": True, "key_types": dict(db.dataset_schema_key_types), "impact": impact}


def digit_only_keys(
    key_regex: "dict | None", alternate_templates: "list[dict] | None" = None
) -> list[str]:
    """Keys whose ``key_regex`` can only match digits in EVERY arm that
    declares one (an arm that says ``[A-Za-z]+`` for the key vetoes it)."""
    from scifor import key_regex_is_digit_only

    verdicts: dict[str, bool] = {}
    arms = [key_regex or {}] + [alt.get("key_regex") or {} for alt in (alternate_templates or [])]
    for arm in arms:
        for key, pattern in arm.items():
            verdicts[key] = verdicts.get(key, True) and key_regex_is_digit_only(pattern)
    return sorted(k for k, digit_only in verdicts.items() if digit_only)


def auto_declare_from_key_regex(
    db, key_regex: "dict | None", alternate_templates: "list[dict] | None" = None
) -> dict:
    """Declare digit-only-regex keys numeric when undeclared (Q2).

    Returns ``{declared: [key], hints: [{key, reason}]}``. A key is only
    declared when that re-spells no stored value; otherwise a hint says why
    and the user decides with the checkbox (which shows the impact).
    """
    declared: list[str] = []
    hints: list[dict] = []
    for key in digit_only_keys(key_regex, alternate_templates):
        if key not in db.dataset_schema_keys:
            continue
        stored = db.dataset_schema_key_types.get(key)
        if stored is not None:
            logger.debug("[schema_key_types] key_regex digit-only for %s, already %r", key, stored)
            continue
        impact = db.schema_key_type_impact(key, "numeric")
        if impact["invalid"]:
            hints.append({"key": key, "reason": (
                f"{key}'s pattern is digit-only, but {len(impact['invalid'])} stored value(s) "
                f"are not numbers (e.g. {impact['invalid'][:3]}), so it was not declared numeric."
            )})
            continue
        if impact["respelled"] or impact["merged"]:
            hints.append({"key": key, "reason": (
                f"{key}'s pattern is digit-only. Tick Numeric to declare it: "
                f"{len(impact['respelled'])} stored value(s) would be spelled differently by new saves."
            )})
            continue
        db.set_schema_key_type(key, "numeric", source=SOURCE_KEY_REGEX)
        declared.append(key)
    if declared or hints:
        logger.info(
            "[schema_key_types] auto_declare_from_key_regex: declared=%s hints=%s",
            declared,
            [h["key"] for h in hints],
        )
    return {"declared": declared, "hints": hints}
