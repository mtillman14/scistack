"""Which plot settings are the DATA DESIGN, the one owner (statistics S11).

A figure and a StatStudio table may hold different designs (user decision
2026-10-17). Both studios show a banner when they diverge, and each has a
one-way "Sync to…" button. What syncs is the design and only the design: the
variables, the roles, the grouping order, the collapse, filters and
exclusions, joined factors, combines and variant selection. Display settings
(kind, colours, sizes, text, layout) never sync. Neither do stats-only
settings, which do not live on a ``PlotSpec`` at all.

:data:`DESIGN_FIELDS` classifies every ``PlotSpec`` field once. It is read by:

- :func:`design_of` / :func:`design_fingerprint`: what a stats spec stores,
  and what the banners compare;
- :func:`design_diff`: the banner's list of differences, with labels;
- :func:`copy_design`: the Sync buttons, in both directions.

``tests/test_design.py`` fails on an unclassified field, the same guard
``presets.FIELD_CLASSES`` has. The two tables answer different questions
(what travels with a preset vs. what is the design), so they are separate,
but each has one owner.

The plot layer never imports statistics. A stats spec *holds* a ``PlotSpec``
as its design carrier, and the GUI service compares the two.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, fields, replace
from enum import Enum
from typing import Any

from scistacklog import Log

from .spec import Aggregation, PlotSpec, Role

LAYER = "scistackplot"


class SyncClass(str, Enum):
    """Whether a setting is part of the data design."""

    #: Changes which rows exist or what a row means. Syncs.
    DESIGN = "design"
    #: How the rows are drawn. Never syncs.
    DISPLAY = "display"

    def __str__(self) -> str:
        return self.value


_D, _P = SyncClass.DESIGN, SyncClass.DISPLAY

#: Blocks classified per sub-field: ``aggregate.pooled`` changes the rows
#: (weight by N), while ``aggregate.statistic`` / ``.error`` only draw them.
_SPLIT_BLOCKS: dict[str, type] = {"aggregate": Aggregation}

#: Every ``PlotSpec`` field, classified. Paths into split blocks are dotted.
DESIGN_FIELDS: dict[str, SyncClass] = {
    "measures": _D,
    "variant_sets": _D,
    "x_measure": _D,
    "roles": _D,
    "groups": _D,
    # Which grouping layer is painted. The layer itself is in `groups`.
    "color": _P,
    # Kind decides the drawing. The stats layer reads its rows at a fixed
    # analysis kind, so a bar-vs-box choice never moves a number.
    "kind": _P,
    "custom": _P,
    "aggregate.statistic": _P,
    "aggregate.error": _P,
    "aggregate.pooled": _D,
    # A 1-D measure's cell statistic makes the scalar each row holds.
    "cell_statistic": _D,
    "index_column": _D,
    "show_sample": _P,
    "join_sample": _P,
    "sample_color": _P,
    "sample_in_legend": _P,
    "show_missing_levels": _P,
    # How a histogram bins and a regression fits: drawing, like the kind.
    "histogram": _P,
    "regression": _P,
    "facet": _P,
    "y_axis": _P,
    "style": _P,
    "aliases": _P,
    "colors": _P,
    "panel_overrides": _P,
    "difference_bars": _P,
    # Compare-to-reference changes the values themselves.
    "comparison": _D,
    # A datetime's wall clock depends on the zone it is read in, so a
    # converted time is a different value (scistackplot.temporal).
    "time_zone": _D,
    "filters": _D,
    "location_filter": _D,
    "factor_variables": _D,
    "level_groups": _D,
}

#: What the banner calls each design field.
LABELS: dict[str, str] = {
    "measures": "Variable",
    "variant_sets": "Variants",
    "x_measure": "X variable",
    "roles": "Roles",
    "groups": "Grouping order",
    "aggregate.pooled": "Weight by N",
    "cell_statistic": "Cell statistic",
    "index_column": "Index column",
    "comparison": "Compare to reference",
    "time_zone": "Time zone",
    "filters": "Filters",
    "location_filter": "Schema locations",
    "factor_variables": "Joined factors",
    "level_groups": "Combines",
}


def unclassified_fields() -> tuple[list[str], list[str]]:
    """``(missing, stale)``: PlotSpec fields with no class, and entries naming
    no field. Drives ``test_every_plot_field_is_classified``."""
    expected: set[str] = set()
    for f in fields(PlotSpec):
        if not f.init:
            continue
        if f.name in _SPLIT_BLOCKS:
            expected.update(f"{f.name}.{sub.name}" for sub in fields(_SPLIT_BLOCKS[f.name]))
        else:
            expected.add(f.name)
    known = set(DESIGN_FIELDS)
    return sorted(expected - known), sorted(known - expected)


def design_paths() -> list[str]:
    return [path for path, cls in DESIGN_FIELDS.items() if cls is SyncClass.DESIGN]


def _value_at(spec: PlotSpec, path: str) -> Any:
    head, _, tail = path.partition(".")
    value = getattr(spec, head)
    return getattr(value, tail) if tail else value


def _jsonable(value: Any) -> Any:
    """A design value as plain JSON, through the spec's own serialisers."""
    if isinstance(value, Enum):
        return str(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable(asdict(value))
    return value


def design_of(spec: PlotSpec) -> dict[str, Any]:
    """The design fields of ``spec`` as JSON, keyed by path."""
    return {path: _jsonable(_value_at(spec, path)) for path in design_paths()}


def design_fingerprint(spec: PlotSpec) -> str:
    """A short stable digest of :func:`design_of`, for "has it diverged?"."""
    text = json.dumps(design_of(spec), sort_keys=True, default=str)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class DesignDifference:
    """One design field that differs between two specs."""

    path: str
    label: str
    a: str
    b: str

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "label": self.label, "a": self.a, "b": self.b}


def _summary(path: str, value: Any) -> str:
    """Short human text for a design value (the banner's two sides)."""
    if path == "roles" and isinstance(value, dict):
        return ", ".join(f"{k}={v}" for k, v in sorted(value.items())) or "(none)"
    if path == "groups" and isinstance(value, list):
        # Innermost first in the spec; read outermost-first, as nested ticks do.
        return " × ".join(reversed([str(v) for v in value])) or "(none)"
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return ", ".join(value) or "(none)"
    if value in (None, [], {}):
        return "(none)"
    if isinstance(value, bool):
        return "on" if value else "off"
    if isinstance(value, (str, int, float)):
        return str(value)
    text = json.dumps(value, sort_keys=True, default=str)
    return text if len(text) <= 80 else text[:77] + "…"


def design_diff(a: PlotSpec, b: PlotSpec) -> list[DesignDifference]:
    """Every design field where ``a`` and ``b`` differ, in table order.

    Roles compare as a mapping, so the order the roles were assigned in is not
    a difference. Everything else compares by its JSON form.
    """
    da, db = design_of(a), design_of(b)
    out = []
    for path in design_paths():
        va, vb = da[path], db[path]
        same = (
            dict(va or {}) == dict(vb or {})
            if path == "roles"
            else json.dumps(va, sort_keys=True, default=str) == json.dumps(vb, sort_keys=True, default=str)
        )
        if not same:
            out.append(DesignDifference(path, LABELS.get(path, path), _summary(path, va), _summary(path, vb)))
    Log.debug(
        "design diff: %d field(s) differ%s",
        len(out),
        f" ({', '.join(d.path for d in out)})" if out else "",
        layer=LAYER,
    )
    return out


@dataclass(frozen=True)
class SyncNote:
    """A display setting that pointed at a factor the new design dropped."""

    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "message": self.message}


def copy_design(source: PlotSpec, target: PlotSpec) -> tuple[PlotSpec, list[SyncNote]]:
    """``target`` with every design field taken from ``source`` (one way,
    wholesale — no merging). Display settings stay ``target``'s.

    A display setting that names a factor no longer in the right role is
    cleared, with a note: the colour must be a grouping layer, and
    "show sample" and its colour must name collapsed keys.
    """
    changes: dict[str, Any] = {}
    for path in design_paths():
        head, _, tail = path.partition(".")
        if tail:
            block = changes.get(head, getattr(target, head))
            changes[head] = replace(block, **{tail: getattr(getattr(source, head), tail)})
        else:
            changes[head] = getattr(source, head)
    result = replace(target, **changes)
    notes: list[SyncNote] = []
    grouped = {name for name, role in result.roles.items() if role is Role.GROUP}
    collapsed = {name for name, role in result.roles.items() if role is Role.COLLAPSE}
    if result.color and result.color not in grouped:
        notes.append(SyncNote("color", f"colour by {result.color!r} cleared: it is no longer a grouping layer"))
        result = replace(result, color=None)
    kept = [name for name in result.show_sample if name in collapsed]
    if kept != list(result.show_sample):
        dropped = [n for n in result.show_sample if n not in collapsed]
        notes.append(SyncNote("show_sample", f"show sample for {dropped!r} cleared: no longer collapsed"))
        result = replace(result, show_sample=kept)
    if result.sample_color and result.sample_color not in result.roles:
        notes.append(SyncNote("sample_color", f"sample colour {result.sample_color!r} cleared"))
        result = replace(result, sample_color=None)
    Log.info(
        "design sync: %d design field(s) copied, %d display note(s)%s",
        len(design_paths()),
        len(notes),
        "".join(f"; {n.message}" for n in notes),
        layer=LAYER,
    )
    return result, notes
