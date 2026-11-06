"""The picker's selection, and the per-key bound a MATLAB command starts from.

A schema selection is a PAIR — ragged ``include`` prefixes plus a standing
``exclude_levels`` rule (docs/claude/location-filter-semantics.md). Python and
MATLAB runs both carry it whole as ``locations=``, and scifor/scidb apply it
exactly.

A MATLAB command ALSO passes :func:`to_schema_filter`'s projection: one value
list per schema key, which is a Cartesian product by construction, so "all of
subject 01, plus trial 3 of subject 02" projects to "subjects 01-02 × trials
1-3". That is wider than the selection on purpose. It only bounds what MATLAB
resolves, and ``locations=`` then narrows to exactly what was selected, so a
wide projection is not a wider run and is not worth a warning.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def as_selection(raw) -> dict:
    """Normalize whatever arrived over the wire into the pair.

    Tolerates ``None``, ``{}``, and a half-filled dict, because a spec written
    before this feature has neither key and must mean "everything".
    """
    value = raw or {}
    include = value.get("include") or []
    exclude_levels = {
        str(key): [str(v) for v in values]
        for key, values in (value.get("exclude_levels") or {}).items()
        if values
    }
    return {"include": list(include), "exclude_levels": exclude_levels}


def is_empty(selection) -> bool:
    """Inert: constrains nothing, so the run is unfiltered."""
    normalized = as_selection(selection)
    return not normalized["include"] and not normalized["exclude_levels"]


def to_schema_filter(selection, db) -> tuple[dict[str, list] | None, list[str]]:
    """``({key: [values]}, warnings)`` — the Cartesian projection.

    Per key:

    * ``exclude_levels`` subtracts from every value the database has for that
      key. Exact: a per-key rule IS a per-key list.
    * ``include`` contributes only when EVERY prefix names the key; the values
      are then the union across prefixes. A key some prefix leaves unnamed is
      unconstrained, because that prefix selects all of it.

    The second return value is a list of plain sentences, meant to be logged and
    shown, for what is wrong with the selection itself. A projection wider than
    the selection is not on it: ``locations=`` travels beside this bound and
    decides what actually runs.
    """
    normalized = as_selection(selection)
    include = normalized["include"]
    excluded = normalized["exclude_levels"]
    if not include and not excluded:
        return None, []

    warnings: list[str] = []
    per_key: dict[str, list] = {}

    for key, levels in excluded.items():
        try:
            known = [str(v) for v in db.distinct_schema_values(key)]
        except Exception:  # pragma: no cover - a key the database never saw
            logger.warning(
                "[schema_selection] no stored values for key %r; its omitted "
                "levels cannot be projected",
                key,
            )
            warnings.append(
                f"'{key}' has no values in the database, so the levels omitted "
                f"for it could not be applied to this run."
            )
            continue
        kept = [value for value in known if value not in set(levels)]
        per_key[key] = kept
        if not kept:
            warnings.append(
                f"Every known level of '{key}' is omitted, so this run has "
                f"nothing to iterate over."
            )

    if include:
        named_everywhere = set.intersection(
            *({str(step[0]) for step in prefix} for prefix in include)
        )
        for key in sorted(named_everywhere):
            values: list[str] = []
            for prefix in include:
                for step_key, step_value in prefix:
                    if str(step_key) == key and str(step_value) not in values:
                        values.append(str(step_value))
            existing = per_key.get(key)
            per_key[key] = (
                [v for v in values if v in set(existing)] if existing else values
            )

    return (per_key or None), warnings


def report(selection, db, *, context: str) -> dict | None:
    """:func:`to_schema_filter`, with the warnings logged against *context*."""
    schema_filter, warnings = to_schema_filter(selection, db)
    for warning in warnings:
        logger.warning("[schema_selection] %s: %s", context, warning)
    pair = as_selection(selection)
    if schema_filter or not is_empty(pair):
        # The prefix depths are what to read when a run keeps rows you did not
        # expect: a prefix that stops above a key selects every value of it.
        depths: dict[str, int] = {}
        for prefix in pair["include"]:
            deepest = str(prefix[-1][0]) if prefix else "(everything)"
            depths[deepest] = depths.get(deepest, 0) + 1
        logger.info(
            "[schema_selection] %s: bound to %s; locations= applies the exact "
            "selection (%d prefix(es) by deepest key %s, level rules %s)",
            context,
            {key: f"{len(values)} value(s)" for key, values in (schema_filter or {}).items()},
            len(pair["include"]),
            depths,
            pair["exclude_levels"],
        )
    return schema_filter
