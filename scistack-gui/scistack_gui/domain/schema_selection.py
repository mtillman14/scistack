"""The picker's selection, and the lossy projection MATLAB still needs.

A schema selection is a PAIR — ragged ``include`` prefixes plus a standing
``exclude_levels`` rule (docs/claude/location-filter-semantics.md). Python runs
carry it whole: ``scidb.for_each(locations=...)`` takes exactly this shape and
``scifor`` applies it to the combo list.

MATLAB runs cannot. A MATLAB command is GENERATED source, and what it can spell
is one value list per schema key — ``subject = ["01","02"]`` — which is a
Cartesian product by construction. Projecting a ragged selection onto that
shape is an OVER-APPROXIMATION: "all of subject 01, plus trial 3 of subject 02"
becomes "subjects 01-02 × trials 1-3", which runs combinations the user
excluded.

So the projection exists, and it is never silent. :func:`to_schema_filter`
returns what was lost alongside the filter, and every caller reports it.
Closing the gap properly means teaching ``+scifor/for_each.m`` the same
``locations=`` argument the Python side has — tracked as the MATLAB parity item
in .claude/plan-schema-key-picker-and-level-order.md.
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

    The second return value names what the projection cannot carry. It is a
    list of plain sentences, meant to be logged and shown, not a bool — "this
    will run more than you asked" needs to say which key and why.
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

        if len(include) > 1:
            warnings.append(
                f"{len(include)} separate locations were selected. A MATLAB "
                f"command can only express one value list per schema key, so "
                f"this run covers every COMBINATION of the values they name — "
                f"more than was selected."
            )
        unnamed = {
            str(step[0]) for prefix in include for step in prefix
        } - named_everywhere
        if unnamed:
            warnings.append(
                f"Some selected locations name {sorted(unnamed)} and others do "
                f"not, so those keys are left unconstrained here."
            )

    return (per_key or None), warnings


def report(selection, db, *, context: str) -> dict | None:
    """:func:`to_schema_filter`, with the warnings logged against *context*."""
    schema_filter, warnings = to_schema_filter(selection, db)
    for warning in warnings:
        logger.warning("[schema_selection] %s: %s", context, warning)
    if schema_filter:
        logger.info(
            "[schema_selection] %s: projected to %s",
            context,
            {key: f"{len(values)} value(s)" for key, values in schema_filter.items()},
        )
    return schema_filter
