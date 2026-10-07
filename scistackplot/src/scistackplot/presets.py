"""
Plot presets: a figure's settings with the data taken out, applied to any
variable.

A **saved plot** is settings + data for ONE variable. A **preset** is the same
settings minus everything that belongs to the variable being plotted, so
"draw StepWidth the way I drew StepLength" is one click.

What counts as "the data" is decided **once**, in :data:`FIELD_CLASSES`. The
same table is read when a preset is made (:func:`make_template` strips) and
when it is applied (:func:`apply_preset` takes those settings from the target
panel), so the two can never disagree about which settings a preset owns
(CLAUDE.md NOTE 4). Every ``PlotSpec`` field must be classified there:
``scistackplot/tests/test_presets.py`` fails on a field nobody classified.

Applying is the saved-plot salvage path, not new logic:

1. :func:`~scistackplot.restore.restore_spec` reads the template, so a preset
   saved by an older build still applies (salvage, never migrate);
2. columns of the plotted variable used as groupings (``Gait.Side``) are
   rebased onto the new variable, and dropped with a note if it has no such
   column;
3. :func:`~scistackplot.restore.reconcile` checks the result against the new
   variable's data;
4. a plot kind the new variable cannot draw falls back to the default kind,
   with a note.

See ``docs/claude/plot-presets.md`` and ``.claude/plan-plot-presets.md``.
"""

from __future__ import annotations

import copy
from dataclasses import fields, replace
from enum import Enum
from typing import Any, Callable

from scistacklog import Log

from .panels import PanelOverride
from .restore import NoteKind, Restored, RestoreNote, reconcile, restore_spec
from .spec import PlotSpec, StyleOptions, YAxis
from .table import LongTable

LAYER = "scistackplot"


class FieldClass(str, Enum):
    """Who a setting belongs to when a preset moves between variables."""

    #: The variable itself: never stored, always the target's.
    DATA = "data"
    #: Text and limits written for one variable's units and meaning (title,
    #: y label, y limits). Never stored, always the target's (user, 2026-10-06).
    VARIABLE_TEXT = "variable_text"
    #: Stored, then pointed at the target variable (own-column groupings).
    REBASED = "rebased"
    #: Stored and applied as-is, then checked against the target's data.
    TEMPLATE = "template"

    def __str__(self) -> str:
        return self.value


_D, _V, _R, _T = (
    FieldClass.DATA,
    FieldClass.VARIABLE_TEXT,
    FieldClass.REBASED,
    FieldClass.TEMPLATE,
)

#: THE classification. ``a.b`` is field ``b`` of the block ``a``; ``a[].b`` is
#: field ``b`` of every entry of the list ``a``. A block listed by its own name
#: is classified as a whole.
FIELD_CLASSES: dict[str, FieldClass] = {
    "measures": _D,
    "variant_sets": _D,
    "x_measure": _T,
    "roles": _T,
    "groups": _T,
    "color": _T,
    "kind": _T,
    "aggregate": _T,
    "cell_statistic": _T,
    "index_column": _T,
    "show_sample": _T,
    "join_sample": _T,
    "sample_color": _T,
    "sample_in_legend": _T,
    "facet": _T,
    "y_axis.scope": _T,
    "y_axis.minimum": _V,
    "y_axis.maximum": _V,
    "style.palette": _T,
    "style.width": _T,
    "style.height": _T,
    "style.text": _T,
    "style.log_x": _T,
    "style.log_y": _T,
    "style.title": _V,
    "style.x_label": _T,
    "style.y_label": _V,
    "style.marker_size": _T,
    "style.sample_weight": _T,
    "style.line_weight": _T,
    "style.alpha": _T,
    "style.mark_color": _T,
    "style.hide_legend_ticks": _T,
    "style.tick_rotation": _T,
    "style.tick_every": _T,
    "style.y_titles": _T,
    # The plotted measure's OWN alias entry is variable text; every other
    # entry (schema keys, levels, other variables) is template. Handled by
    # `_strip` / `_take_from_target` under the pseudo-path below.
    "aliases": _T,
    "aliases[<measure>]": _V,
    "colors": _T,
    "panel_overrides[].match": _T,
    "panel_overrides[].y_minimum": _V,
    "panel_overrides[].y_maximum": _V,
    "panel_overrides[].y_label": _V,
    "panel_overrides[].y_label_hidden": _T,
    "difference_bars": _T,
    "comparison": _T,
    "filters": _T,
    "location_filter": _T,
    "factor_variables": _R,
    "level_groups": _T,
}

#: Blocks classified field by field rather than as a whole.
_SPLIT_BLOCKS: dict[str, type] = {
    "y_axis": YAxis,
    "style": StyleOptions,
    "panel_overrides[]": PanelOverride,
}

#: Stands in for the plotted variable in a stored own-column grouping, so a
#: template never names the variable it was made on.
MEASURE_PLACEHOLDER = "<measure>"


def unclassified_fields() -> tuple[list[str], list[str]]:
    """``(missing, stale)``: spec fields :data:`FIELD_CLASSES` does not
    classify, and entries naming a field the spec no longer has.

    The guard behind "every field has an owner"; a new ``PlotSpec`` field
    fails ``test_every_spec_field_is_classified`` until it is placed.
    """
    expected: set[str] = set()
    for f in fields(PlotSpec):
        if not f.init:
            continue
        block = f.name if f.name in _SPLIT_BLOCKS else (
            f"{f.name}[]" if f"{f.name}[]" in _SPLIT_BLOCKS else None
        )
        if block is None:
            expected.add(f.name)
            continue
        expected.update(
            f"{block}.{sub.name}" for sub in fields(_SPLIT_BLOCKS[block]) if sub.init
        )
    known = {path for path in FIELD_CLASSES if "<" not in path}
    return sorted(expected - known), sorted(known - expected)


def _paths(*classes: FieldClass) -> list[str]:
    return [path for path, cls in FIELD_CLASSES.items() if cls in classes]


# ---------------------------------------------------------------------------
# Making a template
# ---------------------------------------------------------------------------


def make_template(spec: PlotSpec) -> dict:
    """*spec* as a preset template: its settings with the variable taken out.

    The result is a plain dict in ``PlotSpec.to_dict`` form, minus every
    DATA and VARIABLE_TEXT setting, with own-column groupings pointed at
    :data:`MEASURE_PLACEHOLDER`.
    """
    raw = _strip(spec.to_dict(), spec.y_measure)
    Log.info(
        "[preset] template made from %s: %d setting(s) kept; %s left with the "
        "variable",
        spec.y_measure,
        len(raw),
        ", ".join(_paths(_D, _V)),
        layer=LAYER,
    )
    return raw


def _strip(raw: dict, measure: str | None) -> dict:
    """Remove DATA and VARIABLE_TEXT settings from a spec dict, in place.

    *measure* is the variable the dict was drawn for, or None for a stored
    template (which is stripped again on apply, so a setting reclassified
    since the preset was saved is still left with the variable).
    """
    for path in _paths(_D, _V):
        if "<" in path:
            continue
        _pop_path(raw, path)

    overrides = raw.get("panel_overrides")
    if isinstance(overrides, list):
        raw["panel_overrides"] = [
            entry
            for entry in overrides
            if not (isinstance(entry, dict) and set(entry) <= {"match"})
        ]

    aliases = raw.get("aliases")
    if measure is not None and isinstance(aliases, dict):
        aliases.pop(measure, None)

    groupings = raw.get("factor_variables")
    if measure is not None and isinstance(groupings, list):
        for entry in groupings:
            if (
                isinstance(entry, dict)
                and entry.get("variable") == measure
                and entry.get("column")
            ):
                entry["variable"] = MEASURE_PLACEHOLDER
                # The measure's own variant decides which record a carried
                # label comes from; a pin here would be the old variable's.
                entry["variant"] = {}
    return raw


def _pop_path(raw: dict, path: str) -> None:
    if "[]." in path:
        block, key = path.split("[].", 1)
        entries = raw.get(block)
        if isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, dict):
                    entry.pop(key, None)
        return
    if "." in path:
        block, key = path.split(".", 1)
        sub = raw.get(block)
        if isinstance(sub, dict):
            sub.pop(key, None)
        return
    raw.pop(path, None)


# ---------------------------------------------------------------------------
# Applying a template
# ---------------------------------------------------------------------------


def apply_preset(
    template: Any,
    target: PlotSpec,
    *,
    table: LongTable | None = None,
    table_for: Callable[[PlotSpec], LongTable] | None = None,
    name: str | None = None,
) -> Restored:
    """*target*'s variable drawn with *template*'s settings.

    *target* is the panel's current spec for the variable the preset is being
    applied to. Its DATA and VARIABLE_TEXT settings are kept; every other
    setting comes from the template (the preset replaces them all, user
    decision 2026-10-06).

    Pass *table* (the target's ``LongTable``) or *table_for* (a loader taking
    the applied spec, as for saved plots) to check the result against the
    target's data. Without either, the spec is only restored. *name* is for
    the log.

    Never raises for a drifted or mismatched template: what did not carry
    over comes back as notes.
    """
    label = name or "<preset>"
    notes: list[RestoreNote] = []
    if not isinstance(template, dict):
        notes.append(
            RestoreNote(
                "",
                NoteKind.INVALID_VALUE,
                f"the preset's settings are not a table "
                f"({type(template).__name__}); nothing was applied",
            )
        )
        template = {}

    raw = _strip(copy.deepcopy(template), None)
    _take_from_target(raw, target)

    restored = restore_spec(raw, fallback_measure=target.y_measure)
    spec, notes = restored.spec, notes + list(restored.notes)

    if table is None and table_for is not None:
        spec, table, load_notes = _load_table(spec, table_for, label)
        notes += load_notes

    kind_before = spec.kind
    if table is not None:
        reconciled = reconcile(spec, table)
        spec, notes = reconciled.spec, notes + list(reconciled.notes)
        spec, kind_notes = _fit_kind(spec, table)
        notes += kind_notes

    _log_apply(label, spec, kind_before, notes, checked=table is not None)
    return Restored(spec=spec, notes=notes)


def _take_from_target(raw: dict, target: PlotSpec) -> None:
    """Fill *raw* (a stripped template) with *target*'s DATA and
    VARIABLE_TEXT settings, and point own-column groupings at its measure."""
    source = target.to_dict()
    measure = target.y_measure

    for path in _paths(_D, _V):
        if "<" in path or "[]." in path:
            continue
        if "." in path:
            block, key = path.split(".", 1)
            if key in (source.get(block) or {}):
                raw.setdefault(block, {})
                if isinstance(raw[block], dict):
                    raw[block][key] = source[block][key]
        elif path in source:
            raw[path] = source[path]

    # Per-panel limits and titles: the target's own, matched by panel.
    panel_keys = [path.split("[].", 1)[1] for path in _paths(_D, _V) if "[]." in path]
    for entry in source.get("panel_overrides") or []:
        carried = {key: entry[key] for key in panel_keys if key in entry}
        if not carried:
            continue
        overrides = raw.setdefault("panel_overrides", [])
        twin = next(
            (o for o in overrides if isinstance(o, dict) and o.get("match") == entry.get("match")),
            None,
        )
        if twin is None:
            overrides.append({"match": entry.get("match"), **carried})
        else:
            twin.update(carried)

    own_alias = (source.get("aliases") or {}).get(measure)
    if own_alias is not None:
        raw.setdefault("aliases", {})
        if isinstance(raw["aliases"], dict):
            raw["aliases"][measure] = own_alias

    for entry in raw.get("factor_variables") or []:
        if isinstance(entry, dict) and entry.get("variable") == MEASURE_PLACEHOLDER:
            entry["variable"] = measure
            Log.info(
                "[preset] own-column grouping %r rebased onto %s",
                entry.get("column"),
                measure,
                layer=LAYER,
            )


def _load_table(
    spec: PlotSpec, table_for: Callable[[PlotSpec], LongTable], label: str
) -> tuple[PlotSpec, LongTable | None, list[RestoreNote]]:
    """The target's table for *spec*. A load that fails because of a rebased
    own-column grouping (the new variable has no such column) is retried
    without those groupings, each dropped with a note."""
    try:
        return spec, table_for(spec), []
    except Exception as first:
        own = [
            (i, group)
            for i, group in enumerate(spec.factor_variables)
            if group.is_own_column(spec.y_measure)
        ]
        Log.warn(
            "[preset] %s: loading %s's data failed (%s)%s",
            label,
            spec.y_measure,
            first,
            "; retrying without its own-column groupings" if own else "",
            layer=LAYER,
        )
        if own:
            kept = [g for g in spec.factor_variables if not g.is_own_column(spec.y_measure)]
            trimmed = replace(spec, factor_variables=kept)
            try:
                table = table_for(trimmed)
            except Exception as second:
                first = second
            else:
                notes = [
                    RestoreNote(
                        f"factor_variables[{i}]",
                        NoteKind.NOT_IN_DATA,
                        f"grouping by {group.column!r} dropped: it was a column "
                        f"of the preset's variable, and {spec.y_measure} cannot "
                        f"be grouped by it ({first})",
                        group.column,
                    )
                    for i, group in own
                ]
                return trimmed, table, notes
        return spec, None, [
            RestoreNote(
                "",
                NoteKind.NOT_IN_DATA,
                f"{spec.y_measure}'s data could not be loaded, so the preset was "
                f"not checked against it: {first}",
            )
        ]


def _fit_kind(spec: PlotSpec, table: LongTable) -> tuple[PlotSpec, list[RestoreNote]]:
    """Keep the preset's plot kind if the target can draw it, else fall back
    to the default kind. ``capabilities`` is the one judge of which kinds a
    spec can draw, so the panel never shows a kind it then greys out."""
    from .capability import capabilities
    from .spec import PlotKind

    try:
        caps = capabilities(spec, table)
    except Exception as exc:
        Log.warn(
            "[preset] %s: could not judge plot kind %s (%s); kept",
            spec.y_measure,
            spec.kind,
            exc,
            layer=LAYER,
        )
        return spec, []
    available = caps.get("available") or []
    if str(spec.kind) in available:
        return spec, []
    reason = next(
        (e.get("reason") for e in caps.get("kinds", []) if e.get("kind") == str(spec.kind)),
        None,
    )
    fallback = caps.get("default") or (available[0] if available else "")
    if not fallback:
        return spec, [
            RestoreNote(
                "kind",
                NoteKind.KIND_UNAVAILABLE,
                f"{spec.y_measure} cannot be drawn as {spec.kind}"
                f"{f' ({reason})' if reason else ''}, and no other kind is available",
                str(spec.kind),
            )
        ]
    note = RestoreNote(
        "kind",
        NoteKind.KIND_UNAVAILABLE,
        f"{spec.y_measure} ({caps.get('raw_shape')}) cannot be drawn as "
        f"{spec.kind}{f' ({reason})' if reason else ''}; drawn as {fallback}",
        str(spec.kind),
    )
    Log.warn("[apply_preset] kind (%s): %s", note.kind, note.message, layer=LAYER)
    return replace(spec, kind=PlotKind(fallback)), [note]


def shape_warning(made_on_shape: str | None, shape: str | None) -> str | None:
    """Why a preset may not fit a variable of *shape*, or None.

    The rail's warning on a preset made on a differently shaped variable. It
    still applies; :func:`apply_preset` reports what did not carry over.
    """
    if not made_on_shape or not shape or made_on_shape == shape:
        return None
    return (
        f"made on a {made_on_shape} variable; this one is {shape}, so some "
        f"settings may not carry over"
    )


def _log_apply(
    label: str,
    spec: PlotSpec,
    kind_before,
    notes: list[RestoreNote],
    *,
    checked: bool,
) -> None:
    # Each note was logged where it arose (restore, reconcile, load, kind).
    Log.info(
        "[preset] applied %r to %s: %d note(s), kind %s%s",
        label,
        spec.y_measure,
        len(notes),
        kind_before if kind_before == spec.kind else f"{kind_before}->{spec.kind}",
        "" if checked else " (not checked against data)",
        layer=LAYER,
    )
