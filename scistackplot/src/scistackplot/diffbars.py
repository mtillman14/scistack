"""
Difference bars: a bar joining two marks of one panel, labelled ("*" by
default) — how a statistical test's result is drawn. The ONE owner of what a
bar names, which panel and which two ticks it resolves to, and (Stage 2)
where it is placed. Plan: ``.claude/plan-difference-bars.md``.

**What a bar names (D1).** ``match`` is the panel's ITERATE + FACET values
(``ResolvedPlot.panel_factors``), as text (``panels.panel_key_text``): a test
result belongs to one set of data, so "pre vs post differs for subject 01"
does not appear in subject 02's figure. (Per-panel overrides match FACET
values only; the two are deliberately different.) The ends ``a`` and ``b``
are ``{x layer: level text}``, never slot numbers, so a bar survives a
filter, a reorder or a new level. The pair is unordered.

**Mark = tick.** Since colour is paint every mark has its own leaf x
position (``render.base.MARK_SPAN``: nothing dodges), so the bar, box or
sample point a user clicks is always one tick, and a bar's end is a tick.

**Inert bars (D3).** A bar whose panel is not in this figure, or whose end
is not on this panel's axis, is kept in the spec, drawn nowhere and logged.

Readers ask this module; none of them match a bar to a panel or a tick
themselves.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from scistacklog import Log

from .panels import panel_key_text

if TYPE_CHECKING:  # pragma: no cover
    from .resolved import Panel, ResolvedPlot
    from .shape import Shape
    from .spec import PlotSpec

LAYER = "scistackplot"

#: The label a new bar carries.
DEFAULT_LABEL = "*"


@dataclass(frozen=True)
class DifferenceBar:
    """One bar between two ticks of one panel.

    - ``match``: the panel's ITERATE + FACET values as text,
      ``{factor: level}``; ``{}`` is the one panel of a figure with neither.
    - ``a``, ``b``: the two ends, ``{x layer: level text}`` over EVERY x
      layer (a nested axis names each layer). Unordered.
    - ``label``: drawn over the bar's middle.
    """

    match: dict[str, str] = field(default_factory=dict)
    a: dict[str, str] = field(default_factory=dict)
    b: dict[str, str] = field(default_factory=dict)
    label: str = DEFAULT_LABEL

    def problem(self) -> str | None:
        """Why this bar can never be drawn anywhere, or None."""
        if not self.a or not self.b:
            return "an end names no tick"
        if set(self.a) != set(self.b):
            return (
                f"its ends name different x layers ({sorted(self.a)} vs {sorted(self.b)})"
            )
        if self.a == self.b:
            return "both ends are the same tick"
        return None

    def same_pair(self, other: "DifferenceBar") -> bool:
        """Same panel and the same two ends, in either order."""
        return self.match == other.match and (
            (self.a == other.a and self.b == other.b)
            or (self.a == other.b and self.b == other.a)
        )

    def matches_panel(self, values: dict[str, Any]) -> bool:
        """Exact match on the panel's ITERATE + FACET values: the same
        factors, the same text. A partial key never matches."""
        if set(self.match) != set(values):
            return False
        return all(self.match[name] == panel_key_text(values[name]) for name in values)

    def describe(self) -> str:
        """``muscle=SOL: pre–post "*"`` — for logs and notes."""
        where = ", ".join(f"{name}={text}" for name, text in self.match.items())
        return (
            f"{where + ': ' if where else ''}{_end_text(self.a)}–{_end_text(self.b)} "
            f"{self.label!r}"
        )

    def to_dict(self) -> dict:
        return {
            "match": dict(self.match),
            "a": dict(self.a),
            "b": dict(self.b),
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "DifferenceBar":
        label = raw.get("label")
        return cls(
            match=_text_dict(raw.get("match")),
            a=_text_dict(raw.get("a")),
            b=_text_dict(raw.get("b")),
            label=DEFAULT_LABEL if label is None else str(label),
        )

    @classmethod
    def for_panel(
        cls,
        values: dict[str, Any],
        a: dict[str, str],
        b: dict[str, str],
        label: str = DEFAULT_LABEL,
    ) -> "DifferenceBar":
        """A bar on the panel with ITERATE + FACET values *values* (raw,
        turned into text here — the only place that does it). *a* and *b*
        are :class:`Endpoint` ``values``, already text."""
        return cls(
            match={str(name): panel_key_text(value) for name, value in values.items()},
            a=dict(a),
            b=dict(b),
            label=label,
        )


def _text_dict(raw: Any) -> dict[str, str]:
    return {str(name): str(text) for name, text in (raw or {}).items()}


def _end_text(end: dict[str, str]) -> str:
    return " · ".join(end.values())


# ---------------------------------------------------------------------------
# Where a figure can carry bars


#: The kinds that draw one mark per categorical tick, so a tick is something
#: to point at. Today the same membership as ``spec.OVERLAY_KINDS``, but a
#: different question (an overlay also needs a sample; a bar does not).
DIFFERENCE_BAR_KINDS: tuple[str, ...] = (
    "bar",
    "box",
    "violin",
    "scatter",
    "strip",
    "spaghetti",
)


def unavailable(spec: "PlotSpec", shape: "Shape") -> str | None:
    """Why this figure cannot carry difference bars, or None.

    The one statement, shipped in ``capabilities`` so the GUI greys the
    section out with it. Whether the figure has TWO ticks to join is a
    property of the drawn figure, not of the spec; :func:`slot_endpoints`
    answers it.
    """
    from .shape import Shape

    if str(spec.kind) not in DIFFERENCE_BAR_KINDS:
        return (
            f"A {spec.kind} plot has no tick-by-tick marks to join; difference "
            f"bars are for bar, box, violin, scatter, strip and spaghetti."
        )
    if spec.x_measure is not None:
        return "An x-y plot has no ticks to join."
    if shape is not Shape.SCALAR:
        return f"The drawn shape is {shape}; difference bars need one value per row."
    return None



def carries_difference_bars(resolved: "ResolvedPlot") -> bool:
    """Whether this DRAWN figure can carry bars — the renderers' guard (the
    spec-level reason for the GUI is :func:`unavailable`)."""
    return str(resolved.kind) in DIFFERENCE_BAR_KINDS and resolved.spec.x_measure is None


# ---------------------------------------------------------------------------
# Ticks


@dataclass(frozen=True)
class Endpoint:
    """One tick a bar can end on."""

    #: Index into ``ResolvedPlot.x_order`` (spacers included).
    slot: int
    #: Where the tick is drawn (``render.base.x_positions``).
    position: float
    #: ``{x layer: level text}`` — what a :class:`DifferenceBar` end stores.
    values: dict[str, str]
    #: The key the panel frame's ``__x`` holds for this tick, as text.
    x_text: str

    @property
    def identity(self) -> tuple[str, ...]:
        return tuple(self.values.values())


def slot_endpoints(resolved: "ResolvedPlot") -> list[Endpoint]:
    """Every tick of this figure a bar can end on, in drawn order.

    The ONE place an end's text is spelled: a nested axis reads its leaf's
    layer values from ``XPlan.leaf_values`` (never by splitting the key); a
    single layer spells its level ``str(v)``, which is what
    ``render.base.x_positions`` matches ``__x`` by. Empty when the figure has
    no x layer (one unlabelled tick, nothing to join) or no level order (a
    numeric x from a measure or an index).
    """
    from .render.base import x_positions

    layers = list(resolved.x_layers)
    order = resolved.x_order
    if not layers or order is None:
        return []
    positions, _ = x_positions(pd.Series(list(order), dtype=object), resolved)
    plan = resolved.x_plan

    if plan is not None:
        if len(plan.leaf_values) != len(plan.order):
            Log.warn(
                "difference bars: the x plan records %d leaf value(s) for %d tick(s); "
                "no tick can carry a bar",
                len(plan.leaf_values),
                len(plan.order),
                layer=LAYER,
            )
            return []
        found = []
        for slot, (key, leaf) in enumerate(zip(plan.order, plan.leaf_values)):
            if leaf is None:  # a spacer
                continue
            if len(leaf) != len(layers):
                Log.warn(
                    "difference bars: tick %r has %d layer value(s) for %d layer(s) %s; skipped",
                    key,
                    len(leaf),
                    len(layers),
                    layers,
                    layer=LAYER,
                )
                continue
            found.append(
                Endpoint(
                    slot=slot,
                    position=float(positions[slot]),
                    values=dict(zip(layers, leaf)),
                    x_text=str(key),
                )
            )
        return found

    if len(layers) != 1:
        return []
    return [
        Endpoint(
            slot=slot,
            position=float(positions[slot]),
            values={layers[0]: str(level)},
            x_text=str(level),
        )
        for slot, level in enumerate(order)
    ]


# ---------------------------------------------------------------------------
# Bars -> ticks


@dataclass(frozen=True)
class ResolvedBar:
    """A bar resolved onto one panel: its two ends, left one first."""

    bar: DifferenceBar
    left: Endpoint
    right: Endpoint


@dataclass(frozen=True)
class Unresolved:
    """A bar that matched a panel but cannot be drawn on it, and why."""

    bar: DifferenceBar
    reason: str


def panel_values(resolved: "ResolvedPlot", panel: "Panel") -> dict[str, Any]:
    """The panel's ITERATE + FACET values — what ``DifferenceBar.match`` names."""
    return {**resolved.figure_key, **panel.key}


def bars_for_panel(
    resolved: "ResolvedPlot", panel: "Panel", endpoints: list[Endpoint] | None = None
) -> tuple[list[ResolvedBar], list[Unresolved]]:
    """The bars drawn on *panel*, and those naming it that cannot be drawn.

    The ONLY resolver. A bar is drawn when its ends are two ticks of this
    figure's axis that this panel has a mark at (a ragged grid leaves a tick
    empty in one panel). The same pair given twice (in either order) is drawn
    once, with the LAST entry's label, and a WARN says so.
    """
    values = panel_values(resolved, panel)
    candidates = [bar for bar in resolved.spec.difference_bars if bar.matches_panel(values)]
    if not candidates:
        return [], []
    endpoints = slot_endpoints(resolved) if endpoints is None else endpoints
    by_identity = {endpoint.identity: endpoint for endpoint in endpoints}
    layers = list(resolved.x_layers)
    occupied = _occupied_x(panel)

    drawn: list[ResolvedBar] = []
    unresolved: list[Unresolved] = []
    for bar in candidates:
        reason = bar.problem()
        if reason is None and set(bar.a) != set(layers):
            reason = f"its ends name x layers {sorted(bar.a)}, the axis has {layers}"
        ends: list[Endpoint] = []
        if reason is None:
            for end in (bar.a, bar.b):
                found = by_identity.get(tuple(end[name] for name in layers))
                if found is None:
                    reason = f"{_end_text(end)!r} is not a tick of this figure"
                    break
                if found.x_text not in occupied:
                    reason = f"{_end_text(end)!r} has no mark in this panel"
                    break
                ends.append(found)
        if reason is not None:
            unresolved.append(Unresolved(bar, reason))
            continue
        left, right = sorted(ends, key=lambda end: end.position)
        duplicate = next(
            (index for index, seen in enumerate(drawn) if seen.bar.same_pair(bar)), None
        )
        if duplicate is not None:
            Log.warn(
                "difference bars: %s is given more than once; the last label is used",
                bar.describe(),
                layer=LAYER,
            )
            drawn[duplicate] = ResolvedBar(bar, left, right)
            continue
        drawn.append(ResolvedBar(bar, left, right))
    return drawn, unresolved


def _occupied_x(panel: "Panel") -> set[str]:
    """The ticks this panel draws something at, as ``__x`` text (marks and
    "Show sample" points both count)."""
    from .resolved import X

    found: set[str] = set()
    for frame in (panel.frame, panel.sample):
        if frame is not None and not frame.empty and X in frame.columns:
            found.update(str(value) for value in frame[X].dropna().unique())
    return found


def not_in_figure(resolved: "ResolvedPlot") -> list[DifferenceBar]:
    """The bars that name no panel of THIS figure — kept in the spec, inert
    here (they may be another figure's). For the log line and the GUI's
    "not in this figure" group."""
    keys = [panel_values(resolved, panel) for panel in resolved.panels]
    return [
        bar
        for bar in resolved.spec.difference_bars
        if not any(bar.matches_panel(values) for values in keys)
    ]


def log_figure(resolved: "ResolvedPlot") -> None:
    """One INFO per figure, only when the spec has bars: how many resolve
    onto a panel here, which cannot and why, and how many name another
    figure. WARN for a bar that can never draw anywhere."""
    if not resolved.spec.difference_bars:
        return
    endpoints = slot_endpoints(resolved)
    drawn: list[str] = []
    failed: list[str] = []
    for panel in resolved.panels:
        bars, unresolved = bars_for_panel(resolved, panel, endpoints)
        drawn.extend(item.bar.describe() for item in bars)
        failed.extend(f"{item.bar.describe()}: {item.reason}" for item in unresolved)
    for bar in resolved.spec.difference_bars:
        problem = bar.problem()
        if problem is not None:
            Log.warn(
                "difference bars: %s can never be drawn (%s)",
                bar.describe(),
                problem,
                layer=LAYER,
            )
    elsewhere = not_in_figure(resolved)
    label = ", ".join(f"{k}={v}" for k, v in resolved.figure_key.items())
    Log.info(
        "difference bars%s: %d resolved (%s), %d unresolved%s, %d not in this figure, "
        "%d tick(s) to join",
        f" [{label}]" if label else "",
        len(drawn),
        "; ".join(drawn) or "none",
        len(failed),
        f" ({'; '.join(failed)})" if failed else "",
        len(elsewhere),
        len(endpoints),
        layer=LAYER,
    )


# ---------------------------------------------------------------------------
# Placement (plan D5)
#
# Everything is worked out in AXIS units (log10 on a log axis, where both
# backends draw a segment straight) and turned back into data units at the
# end. A size in points becomes axis units through the panel's measured
# height: ``u = height_pt / (top - bottom)`` points per unit. The top the
# bars need depends on ``u``, which depends on the top, so the top is found
# by iterating to a fixed point (:func:`place_group`).

#: Clearance between whatever is below and a bar's line (or a leg's foot).
GAP_PT = 3.0
#: A bar's line width.
LINE_PT = 1.0
#: Between the line and the bottom of its label's box.
LABEL_GAP_PT = 1.0
#: Above the topmost label, to the axis' top.
PAD_PT = 2.0
#: A box plot's outlier marker radius (matplotlib's default flier markersize
#: 6 pt; plotly's outlier points are the same size in px = pt).
BOX_FLIER_PT = 3.0
#: Half the width of a strip plot's jitter, in ticks (``mpl._draw_points``,
#: ``rng.uniform(-0.15, 0.15)``).
STRIP_JITTER = 0.15
#: The fixed point stops when the top moves less than this fraction of the
#: range; the cap only guards a figure too short for its bars.
TOLERANCE = 1e-9
MAX_ITERATIONS = 200
#: WARN when the bars take more than this share of a panel's height.
CROWDED_FRACTION = 0.5
#: An unmeasured label's box: width per character and height, per point of
#: font size (DejaVu Sans, with its descent — the undecided fallback only;
#: the export measures).
EST_CHAR_WIDTH = 0.62
EST_LINE_HEIGHT = 1.2


@dataclass(frozen=True)
class Obstacles:
    """Everything a bar must clear, as parallel arrays (a "Show sample"
    overlay can be thousands of points; loops per point per bar per
    iteration would be seconds).

    Each entry is a segment from ``(x0, y0)`` to ``(x1, y1)`` in axis units —
    a flat box when ``y0 == y1``, a point when ``x0 == x1`` — plus ``reach``
    in points beyond it in every direction (a marker's radius, half a
    line's width)."""

    x0: np.ndarray
    x1: np.ndarray
    y0: np.ndarray
    y1: np.ndarray
    reach: np.ndarray

    @classmethod
    def empty(cls) -> "Obstacles":
        nothing = np.zeros(0, dtype=float)
        return cls(nothing, nothing, nothing, nothing, nothing)

    @classmethod
    def build(cls, rows: list[tuple[float, float, float, float, float]]) -> "Obstacles":
        if not rows:
            return cls.empty()
        array = np.asarray(rows, dtype=float)
        keep = ~np.isnan(array[:, :4]).any(axis=1)
        array = array[keep]
        return cls(array[:, 0], array[:, 1], array[:, 2], array[:, 3], array[:, 4])

    def __len__(self) -> int:
        return len(self.x0)

    def plus(self, other: "Obstacles") -> "Obstacles":
        return Obstacles(
            np.concatenate([self.x0, other.x0]),
            np.concatenate([self.x1, other.x1]),
            np.concatenate([self.y0, other.y0]),
            np.concatenate([self.y1, other.y1]),
            np.concatenate([self.reach, other.reach]),
        )

    def to_axis(self, log: bool) -> "Obstacles":
        """In axis units: log10 on a log axis, dropping what cannot be drawn
        there (a non-positive end)."""
        if not log:
            return self
        keep = (self.y0 > 0) & (self.y1 > 0)
        return Obstacles(
            self.x0[keep],
            self.x1[keep],
            np.log10(self.y0[keep]),
            np.log10(self.y1[keep]),
            self.reach[keep],
        )

    def top_over(self, a: float, b: float, *, per_unit_y: float, per_unit_x: float) -> float | None:
        """The highest thing over ``[a, b]`` (x), in axis units, reaches
        included; None when nothing is there."""
        if not len(self):
            return None
        pad = self.reach / per_unit_x
        hit = (self.x1 + pad >= a) & (self.x0 - pad <= b)
        if not hit.any():
            return None
        x0, x1 = self.x0[hit], self.x1[hit]
        y0, y1 = self.y0[hit], self.y1[hit]
        span = x1 - x0
        sloped = span > 0
        top = np.maximum(y0, y1)
        if sloped.any():
            # A sloped segment clipped to [a, b]: its highest point is at one
            # of the clipped ends (it is straight).
            s0, s1, t0, t1 = x0[sloped], x1[sloped], y0[sloped], y1[sloped]
            left = np.clip(a, s0, s1)
            right = np.clip(b, s0, s1)
            at_left = t0 + (left - s0) / (s1 - s0) * (t1 - t0)
            at_right = t0 + (right - s0) / (s1 - s0) * (t1 - t0)
            top[sloped] = np.maximum(at_left, at_right)
        return float((top + self.reach[hit] / per_unit_y).max())


@dataclass(frozen=True)
class PanelFrame:
    """A panel's drawing box, as laid out (measured by the export)."""

    height_pt: float
    width_pt: float
    #: The x axis' drawn range, data units.
    x_range: tuple[float, float]

    @property
    def per_unit_x(self) -> float:
        span = self.x_range[1] - self.x_range[0]
        return self.width_pt / span if span > 0 else 1.0


@dataclass(frozen=True)
class BarSizes:
    """Every size placement uses, in points."""

    #: The label's font size (``TextSizes.differences``).
    label_font_pt: float
    gap_pt: float = GAP_PT
    line_pt: float = LINE_PT
    label_gap_pt: float = LABEL_GAP_PT
    pad_pt: float = PAD_PT
    #: ``{label: (width_pt, height_pt)}`` measured by matplotlib; a label not
    #: here is estimated (:func:`estimate_label_box`).
    label_boxes: dict[str, tuple[float, float]] = field(default_factory=dict)

    def label_box(self, label: str) -> tuple[float, float]:
        found = self.label_boxes.get(label)
        return found if found is not None else estimate_label_box(label, self.label_font_pt)

    @property
    def measured(self) -> bool:
        return bool(self.label_boxes)


def estimate_label_box(label: str, font_pt: float) -> tuple[float, float]:
    """An unmeasured label's ``(width, height)`` in points — for the
    undecided preview only."""
    return (EST_CHAR_WIDTH * font_pt * max(len(label), 1), EST_LINE_HEIGHT * font_pt)


def figure_sizes(resolved: "ResolvedPlot", label_boxes: dict | None = None) -> BarSizes:
    """The sizes for *resolved*: the label font from the one text-size owner."""
    from .textsize import resolve_sizes

    return BarSizes(
        label_font_pt=resolve_sizes(resolved.spec.style).differences,
        label_boxes=dict(label_boxes or {}),
    )


@dataclass(frozen=True)
class PlacedBar:
    """One bar as drawn, in DATA units: the line from ``left`` to ``right``
    at ``y``, a leg down to each foot, the label's box from ``label_y``
    (its bottom, centred on the bar) to ``label_top``."""

    bar: DifferenceBar
    left: float
    right: float
    y: float
    left_foot: float
    right_foot: float
    label_y: float
    label_top: float

    @property
    def middle(self) -> float:
        return (self.left + self.right) / 2.0

    def to_dict(self) -> dict:
        return {
            "bar": self.bar.to_dict(),
            "left": self.left,
            "right": self.right,
            "y": self.y,
            "left_foot": self.left_foot,
            "right_foot": self.right_foot,
            "label_y": self.label_y,
            "label_top": self.label_top,
        }


@dataclass(frozen=True)
class PanelProblem:
    """One panel to place bars on: its bars, what they must clear, its box."""

    bars: list[ResolvedBar]
    obstacles: Obstacles
    frame: PanelFrame


@dataclass(frozen=True)
class GroupPlacement:
    """The answer for panels sharing one y range."""

    #: Per problem, the bars drawn, in placement order.
    placed: list[list[PlacedBar]]
    #: Per problem, the bars that do not fit under a typed top (D6).
    unfit: list[list[ResolvedBar]]
    #: The shared range the panels draw, data units.
    limits: tuple[float, float]
    iterations: int
    converged: bool
    #: The share of the panel height between the data's top and the axis'
    #: top — the room the bars took.
    bar_fraction: float


def placement_order(bars: list[ResolvedBar]) -> list[ResolvedBar]:
    """Narrowest span first, then leftmost: short comparisons sit low and
    long ones arch over them — the usual reading."""
    return sorted(
        bars, key=lambda item: (item.right.position - item.left.position, item.left.position)
    )


def _place_once(
    problem: PanelProblem,
    *,
    bottom: float,
    top: float,
    sizes: BarSizes,
    pinned: bool,
) -> tuple[list[tuple[ResolvedBar, float, float, float, float, float]], list[ResolvedBar], float | None]:
    """Place one panel's bars for an axis ending at *top* (axis units).

    Returns the placed bars as ``(item, y, left_foot, right_foot,
    label_bottom, label_top)`` in axis units, the bars that do not fit under
    a pinned top, and the top the placed bars need (None with none placed).
    """
    frame = problem.frame
    per_y = frame.height_pt / (top - bottom)
    per_x = frame.per_unit_x
    obstacles = problem.obstacles
    placed = []
    unfit: list[ResolvedBar] = []
    need: float | None = None
    half_line = sizes.line_pt / 2.0

    def height(a: float, b: float) -> float | None:
        return obstacles.top_over(a, b, per_unit_y=per_y, per_unit_x=per_x)

    for item in placement_order(problem.bars):
        left, right = item.left.position, item.right.position
        width_pt, height_pt = sizes.label_box(item.bar.label)
        middle = (left + right) / 2.0
        half_label = (width_pt / 2.0) / per_x
        above_line = (half_line + sizes.label_gap_pt) / per_y

        # The line clears everything over its span, including ticks BETWEEN
        # its ends; the label clears everything under its own width, which
        # may be wider than the span.
        floors = []
        under_span = height(left, right)
        if under_span is not None:
            floors.append(under_span)
        under_label = height(middle - half_label, middle + half_label)
        if under_label is not None:
            floors.append(under_label - above_line)
        floor = max(floors) if floors else bottom
        y = floor + (sizes.gap_pt + half_line) / per_y
        label_bottom = y + above_line
        label_top = label_bottom + height_pt / per_y

        feet = []
        for x in (left, right):
            under = height(x, x)
            foot = (
                min(y, under + sizes.gap_pt / per_y)
                if under is not None
                else max(bottom, y - 2 * sizes.gap_pt / per_y)
            )
            feet.append(foot)

        if pinned and label_top > top:
            unfit.append(item)
            continue
        # What a later bar must clear: this bar's whole LEVEL — line plus
        # label height across the full span (its legs are under it) — and the
        # label's own width when that is wider than the span. Reserving only
        # the label's width let a bar sharing an end tick sit a gap above this
        # line with a zero-length leg: nothing overlapped, but the pair read
        # as a staircase (2026-09-27). Inclusive ends, so a bar sharing an end
        # tick always goes on the next level.
        obstacles = obstacles.plus(
            Obstacles.build(
                [
                    (left, right, label_top, label_top, 0.0),
                    (middle - half_label, middle + half_label, label_top, label_top, 0.0),
                ]
            )
        )
        placed.append((item, y, feet[0], feet[1], label_bottom, label_top))
        wanted = label_top + sizes.pad_pt / per_y
        need = wanted if need is None else max(need, wanted)
    return placed, unfit, need


def place_group(
    problems: list[PanelProblem],
    *,
    limits: tuple[float, float],
    pinned_top: bool,
    sizes: BarSizes,
    log: bool = False,
    top_floor: float | None = None,
) -> GroupPlacement:
    """Place the bars of panels that share one y range, and the range.

    *limits* is the range the panels draw without bars (data units); its
    top is kept when *pinned_top* (a typed Max wins: bars above it are
    ``unfit``, D6), otherwise raised to the lowest top that holds every
    bar. *top_floor* (data units) raises it further — another figure of a
    fan-out sharing this range needs more (D7).

    The top ``T`` and the scale ``u = H / (T - bottom)`` depend on each
    other, so ``T`` is iterated: ``T(n+1) = max(T_data, need(T(n)))``.
    Every placement offset is ``c / u = c (T - bottom) / H`` with ``c`` in
    points, so ``need`` only grows with ``T`` and the sequence only rises;
    it settles while the bars' points are fewer than the panel's.
    """
    to_axis = (lambda v: math.log10(v)) if log else (lambda v: v)
    to_data = (lambda v: 10.0**v) if log else (lambda v: v)
    bottom, data_top = to_axis(limits[0]), to_axis(limits[1])
    if data_top <= bottom:
        # A flat range (every value equal, nothing padded): no scale to turn
        # points into units. One unit of room keeps the arithmetic finite.
        Log.warn(
            "difference bars: the y range %s is empty; placing over one unit above it",
            limits,
            layer=LAYER,
        )
        data_top = bottom + 1.0
    axis_problems = [
        PanelProblem(p.bars, p.obstacles.to_axis(log), p.frame) for p in problems
    ]

    def run(top: float, pinned: bool):
        results = [
            _place_once(p, bottom=bottom, top=top, sizes=sizes, pinned=pinned)
            for p in axis_problems
        ]
        needs = [need for _, _, need in results if need is not None]
        return results, (max(needs) if needs else None)

    iterations = 1
    converged = True
    if pinned_top:
        top = data_top
        results, _ = run(top, pinned=True)
    else:
        top = data_top
        if top_floor is not None and (not log or top_floor > 0):
            top = max(top, to_axis(top_floor))
        results, need = run(top, pinned=False)
        converged = False
        for iterations in range(1, MAX_ITERATIONS + 1):
            wanted = max(top, need) if need is not None else top
            if wanted - top <= TOLERANCE * max(top - bottom, 1e-300):
                converged = True
                break
            top = wanted
            results, need = run(top, pinned=False)
        if not converged:
            Log.warn(
                "difference bars: the axis top did not settle after %d iterations "
                "(the bars need more height than the panel has); drawn at %.6g",
                MAX_ITERATIONS,
                to_data(top),
                layer=LAYER,
            )

    placed = [
        [
            PlacedBar(
                bar=item.bar,
                left=item.left.position,
                right=item.right.position,
                y=to_data(y),
                left_foot=to_data(left_foot),
                right_foot=to_data(right_foot),
                label_y=to_data(label_bottom),
                label_top=to_data(label_top),
            )
            for item, y, left_foot, right_foot, label_bottom, label_top in result
        ]
        for result, _, _ in results
    ]
    unfit = [list(result_unfit) for _, result_unfit, _ in results]
    fraction = (top - data_top) / (top - bottom) if top > bottom else 0.0
    return GroupPlacement(
        placed=placed,
        unfit=unfit,
        limits=(limits[0], to_data(top)),
        iterations=iterations,
        converged=converged,
        bar_fraction=max(fraction, 0.0),
    )


# ---------------------------------------------------------------------------
# A resolved figure -> problems


def panel_obstacles(resolved: "ResolvedPlot", panel: "Panel") -> Obstacles:
    """Everything *panel* draws, as obstacles in DATA units — the marks and
    the "Show sample" overlay, each with the reach its renderer gives it.

    The reach rules, per kind (what both renderers draw):

    - bar: ``MARK_SPAN`` wide, up to its error bar's top (from zero on a
      linear axis, so a negative bar's top is 0);
    - box / violin: ``MARK_SPAN`` wide, up to the highest value (whiskers and
      fliers never pass it; matplotlib's violin and plotly's
      ``spanmode: "hard"`` stop at the data); a box's flier adds its radius;
    - strip: every point, jittered ``±STRIP_JITTER``, plus the marker radius;
    - scatter: every point plus the marker radius;
    - spaghetti: every point at its line's offset plus the marker radius,
      and every segment of every run plus half the line width;
    - overlay: every point at ``sample_positions`` plus its radius, and the
      joining segments when joined.
    """
    from .render.base import (
        mark_width,
        sample_dropped_reason,
        sample_positions,
        sample_runs,
        series_runs,
        x_positions,
    )
    from .spec import PlotKind
    from .weights import sample_weight, spaghetti_weight

    kind = resolved.kind
    encoding = resolved.encoding
    style = resolved.spec.style
    frame = panel.frame
    half = mark_width() / 2.0
    marker_radius = math.sqrt(max(float(style.marker_size), 0.0)) / 2.0
    rows: list[tuple[float, float, float, float, float]] = []

    if (
        not frame.empty
        and encoding.x in frame.columns
        and encoding.y in frame.columns
    ):
        positions, _ = x_positions(frame[encoding.x], resolved)
        values = pd.to_numeric(frame[encoding.y], errors="coerce").to_numpy(dtype=float)
        if kind is PlotKind.BAR:
            tops = values.copy()
            if encoding.has_error and encoding.y_high in frame.columns:
                high = pd.to_numeric(frame[encoding.y_high], errors="coerce").to_numpy(
                    dtype=float
                )
                tops = np.fmax(tops, high)
            if not style.log_y:
                tops = np.fmax(tops, 0.0)
            rows.extend((p - half, p + half, t, t, 0.0) for p, t in zip(positions, tops))
        elif kind in (PlotKind.BOX, PlotKind.VIOLIN):
            reach = BOX_FLIER_PT if kind is PlotKind.BOX else 0.0
            by_slot = (
                pd.DataFrame({"p": positions, "v": values}).dropna().groupby("p")["v"].max()
            )
            rows.extend((p - half, p + half, v, v, reach) for p, v in by_slot.items())
        elif kind is PlotKind.STRIP:
            rows.extend(
                (p - STRIP_JITTER, p + STRIP_JITTER, v, v, marker_radius)
                for p, v in zip(positions, values)
            )
        elif kind is PlotKind.SPAGHETTI:
            weight = spaghetti_weight(style)
            offsets = resolved.series_offsets or {}
            for series_id, run in series_runs(frame, resolved):
                xs, _ = x_positions(run[encoding.x], resolved)
                xs = xs + offsets.get(str(series_id), 0.0)
                ys = pd.to_numeric(run[encoding.y], errors="coerce").to_numpy(dtype=float)
                rows.extend(_polyline(xs, ys, weight.marker_pt / 2.0, weight.line_pt / 2.0, True))
        else:  # scatter, and anything else drawn as points
            rows.extend((p, p, v, v, marker_radius) for p, v in zip(positions, values))

    sample = panel.sample
    if sample is not None and not sample.empty and sample_dropped_reason(panel, resolved) is None:
        weight = sample_weight(style)
        for run in sample_runs(sample, resolved):
            xs = sample_positions(run.rows, resolved, run.identity)
            ys = pd.to_numeric(run.rows[encoding.y], errors="coerce").to_numpy(dtype=float)
            joined = resolved.sample_join and len(run.rows) > 1
            rows.extend(_polyline(xs, ys, weight.marker_pt / 2.0, weight.line_pt / 2.0, joined))
    return Obstacles.build(rows)


def _polyline(
    xs: np.ndarray, ys: np.ndarray, marker_reach: float, line_reach: float, joined: bool
) -> list[tuple[float, float, float, float, float]]:
    """Points (and, when joined, the segments between them in x order)."""
    order = np.argsort(xs, kind="stable")
    xs, ys = xs[order], ys[order]
    found = [(x, x, y, y, marker_reach) for x, y in zip(xs, ys)]
    if joined:
        found.extend(
            (xs[i], xs[i + 1], ys[i], ys[i + 1], line_reach) for i in range(len(xs) - 1)
        )
    return found


@dataclass(frozen=True)
class FigurePlacement:
    """Every panel's bars and drawn range, keyed by index in ``resolved.panels``."""

    bars: dict[int, list[PlacedBar]]
    unfit: dict[int, list[ResolvedBar]]
    limits: dict[int, tuple[float, float]]
    #: One line per placed group, for the log.
    notes: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return sum(len(bars) for bars in self.bars.values())

    def top(self) -> float | None:
        """The highest drawn top — what a sibling figure sharing the range
        must reach (D7)."""
        tops = [high for _, high in self.limits.values()]
        return max(tops) if tops else None


def range_key(resolved: "ResolvedPlot", panel: "Panel") -> tuple:
    """Which y range a panel draws, as a comparable key: its values in the
    y-limit scope (``ResolvedPlot.y_scope`` — ITERATE and FACET factors) plus
    the ends typed for it (the figure's or its own override's).

    Two panels with the same key draw the same range, in this figure or in
    another figure of the fan-out; that is what "kept in sync" means, and
    why bars raising one must raise the other (D7). The ONE statement of it.
    """
    from .panels import override_for
    from .ylimits import pinned_ends, scope_key

    values = panel_values(resolved, panel)
    scope = list(resolved.y_scope)
    return (
        scope_key({name: values.get(name) for name in scope}, scope),
        pinned_ends(resolved.spec.y_axis, override_for(resolved.spec, panel.key)),
    )


def placement_groups(resolved: "ResolvedPlot") -> list[list[int]]:
    """Panels placed together because they draw one range.

    All of them when they share one axis (``render.base.shares_y_axis``:
    matplotlib ties the axes, so one ``set_ylim`` moves every panel);
    otherwise by :func:`range_key`, so two panels that draw the same range
    on separate axes are still raised together.
    """
    from .render.base import shares_y_axis

    indices = list(range(len(resolved.panels)))
    if shares_y_axis(resolved) and indices:
        return [indices]
    groups: dict[tuple, list[int]] = {}
    for index in indices:
        groups.setdefault(range_key(resolved, resolved.panels[index]), []).append(index)
    return list(groups.values())


def place_figure(
    resolved: "ResolvedPlot",
    frames: dict[int, PanelFrame],
    sizes: BarSizes,
    *,
    top_floors: dict[tuple, float] | None = None,
) -> FigurePlacement:
    """Place every panel's bars in *resolved* on its measured *frames*.

    A panel without bars keeps its range unless it shares an axis with one
    that has them. A panel with no range (autoscaled: nothing computed and
    nothing typed) cannot be placed on and is logged. A typed top (figure
    or the panel's own, ``ylimits.pinned_ends``) wins (D6).

    *top_floors* (``{range_key: top}``, data units) are the tops other
    figures of the fan-out need for a range this figure also draws
    (:func:`sibling_floors`, D7); a group is raised to the highest of its
    panels' floors even when it has no bars of its own.
    """
    from .panels import override_for
    from .render.base import panel_y_limits
    from .ylimits import pinned_ends

    endpoints = slot_endpoints(resolved)
    log = bool(resolved.spec.style.log_y)
    bars: dict[int, list[PlacedBar]] = {}
    unfit: dict[int, list[ResolvedBar]] = {}
    limits: dict[int, tuple[float, float]] = {}
    notes: list[str] = []

    for group in placement_groups(resolved):
        resolved_bars = {
            index: bars_for_panel(resolved, resolved.panels[index], endpoints)[0]
            for index in group
        }
        with_bars = [index for index in group if resolved_bars[index]]
        for index in group:
            found = panel_y_limits(resolved, resolved.panels[index])
            if found is not None:
                limits[index] = tuple(found)
        floors = [
            (top_floors or {}).get(range_key(resolved, resolved.panels[index]))
            for index in group
        ]
        floors = [floor for floor in floors if floor is not None]
        top_floor = max(floors) if floors else None
        if not with_bars and top_floor is None:
            continue
        drawable = [index for index in group if index in limits and index in frames]
        missing = [index for index in with_bars if index not in drawable]
        for index in missing:
            Log.warn(
                "difference bars: panel %s has %s; its %d bar(s) are not drawn",
                resolved.panels[index].title or "(unfaceted)",
                "no y range" if index not in limits else "no measured frame",
                len(resolved_bars[index]),
                layer=LAYER,
            )
        if not drawable:
            continue
        shared = limits[drawable[0]]
        if log and shared[0] <= 0:
            Log.warn(
                "difference bars: log axis range %s starts at or below zero; not placed",
                shared,
                layer=LAYER,
            )
            continue
        panel = resolved.panels[drawable[0]]
        _, typed_top = pinned_ends(resolved.spec.y_axis, override_for(resolved.spec, panel.key))
        problems = [
            PanelProblem(
                bars=resolved_bars[index] if index in with_bars else [],
                obstacles=panel_obstacles(resolved, resolved.panels[index])
                if index in with_bars
                else Obstacles.empty(),
                frame=frames[index],
            )
            for index in drawable
        ]
        result = place_group(
            problems,
            limits=shared,
            pinned_top=typed_top is not None,
            sizes=sizes,
            log=log,
            top_floor=top_floor,
        )
        for position, index in enumerate(drawable):
            limits[index] = result.limits
            if result.placed[position]:
                bars[index] = result.placed[position]
            if result.unfit[position]:
                unfit[index] = result.unfit[position]
        names = ", ".join(resolved.panels[i].title or "(unfaceted)" for i in drawable)
        n_unfit = sum(len(u) for u in result.unfit)
        notes.append(
            f"{names}: {sum(len(p) for p in result.placed)} placed, {n_unfit} unfit, "
            f"top {shared[1]:.6g} -> {result.limits[1]:.6g} "
            f"({result.iterations} iteration(s), bars {result.bar_fraction:.0%} of the height)"
            + (f", raised to at least {top_floor:.6g} for other figures" if top_floor is not None else "")
        )
        Log.debug(
            "difference bars: panels %s, heights %s pt, labels %s",
            names,
            [round(frames[i].height_pt, 1) for i in drawable],
            "measured" if sizes.measured else "estimated",
            layer=LAYER,
        )
        if result.bar_fraction > CROWDED_FRACTION:
            Log.warn(
                "difference bars: %s take %.0f%% of the panel height; the figure is "
                "short for %d bar(s)",
                names,
                100 * result.bar_fraction,
                sum(len(p) for p in result.placed),
                layer=LAYER,
            )
        if n_unfit:
            Log.warn(
                "difference bars: %d bar(s) on %s do not fit under the Max you set (%.6g) "
                "and are not drawn",
                n_unfit,
                names,
                shared[1],
                layer=LAYER,
            )
    return FigurePlacement(bars=bars, unfit=unfit, limits=limits, notes=notes)


# ---------------------------------------------------------------------------
# Other figures of the fan-out (D7)


def figure_has_bars(spec: "PlotSpec", figure_key: dict[str, Any]) -> bool:
    """Whether some bar names a panel of the figure with ITERATE values
    *figure_key* — so ``reduce`` builds only the siblings that can raise a
    shared range, never the whole fan-out."""
    texts = {name: panel_key_text(value) for name, value in figure_key.items()}
    return any(
        all(bar.match.get(name) == text for name, text in texts.items())
        for bar in spec.difference_bars
    )


def sibling_floors(
    resolved: "ResolvedPlot",
    frames: dict[int, PanelFrame],
    sizes: BarSizes,
    *,
    lay_out=None,
) -> dict[tuple, float]:
    """The tops OTHER figures' bars need, per :func:`range_key` this figure
    also draws: ``{range_key: top}`` in data units, for ``place_figure``.

    The cheap way (user decision 2026-09-27): each sibling
    (``ResolvedPlot.difference_siblings``) is placed on THIS figure's
    measured *frames*, cell by cell — same size and same grid means the same
    panel heights, give or take a fraction of a point, which ``PAD_PT``
    absorbs. Only a sibling whose grid or tick count differs is really laid
    out, through *lay_out(sibling) -> FigurePlacement | None* (the renderer
    supplies it; None skips the sibling with a WARN).
    """
    siblings = list(getattr(resolved, "difference_siblings", None) or [])
    if not siblings:
        return {}
    wanted = {range_key(resolved, panel) for panel in resolved.panels}
    by_cell = {
        (panel.grid_row, panel.grid_col): frames[index]
        for index, panel in enumerate(resolved.panels)
        if index in frames
    }
    floors: dict[tuple, float] = {}
    for sibling in siblings:
        label = ", ".join(f"{k}={v}" for k, v in sibling.figure_key.items()) or "(figure)"
        same_grid = (
            (sibling.grid_rows, sibling.grid_cols) == (resolved.grid_rows, resolved.grid_cols)
            and len(sibling.x_order or []) == len(resolved.x_order or [])
            and all((p.grid_row, p.grid_col) in by_cell for p in sibling.panels)
        )
        if same_grid:
            sibling_frames = {
                index: by_cell[(panel.grid_row, panel.grid_col)]
                for index, panel in enumerate(sibling.panels)
            }
            placement = place_figure(sibling, sibling_frames, sizes)
            how = "estimated on this figure's panels"
        else:
            placement = lay_out(sibling) if lay_out is not None else None
            how = "laid out (its grid differs)"
            if placement is None:
                Log.warn(
                    "difference bars: sibling figure %s has a different grid and could "
                    "not be laid out; its bars do not raise this figure",
                    label,
                    layer=LAYER,
                )
                continue
        for index, panel_bars in placement.bars.items():
            key = range_key(sibling, sibling.panels[index])
            if not panel_bars or key not in wanted or index not in placement.limits:
                continue
            top = placement.limits[index][1]
            if top > floors.get(key, -math.inf):
                floors[key] = top
                Log.info(
                    "difference bars: figure %s needs a top of %.6g for a range this "
                    "figure shares (%s)",
                    label,
                    top,
                    how,
                    layer=LAYER,
                )
    return floors


# ---------------------------------------------------------------------------
# What the GUI reads (``layout.meta.difference_bars``)


def target_bounds(endpoints: list[Endpoint]) -> dict[int, tuple[float, float]]:
    """``{slot: (x0, x1)}``: the stretch of x that picks each tick — a click
    or hover anywhere on its bar, box or sample points lands inside. Half-way
    to the neighbouring tick, and never more than half a tick either side."""
    positions = sorted({end.position for end in endpoints if not math.isnan(end.position)})
    bounds: dict[int, tuple[float, float]] = {}
    for end in endpoints:
        if math.isnan(end.position):
            continue
        at = positions.index(end.position)
        left = (end.position - positions[at - 1]) / 2.0 if at > 0 else 0.5
        right = (positions[at + 1] - end.position) / 2.0 if at + 1 < len(positions) else 0.5
        bounds[end.slot] = (end.position - min(left, 0.5), end.position + min(right, 0.5))
    return bounds


def difference_meta(
    resolved: "ResolvedPlot",
    placement: FigurePlacement | None,
    axes: dict[int, tuple[str, str]],
    *,
    estimated: bool,
) -> dict:
    """``layout.meta.difference_bars``: everything Plot Studio shows or picks
    from, decided here so the panel never computes it.

    Per panel: its ``match`` (what a new bar stores), the axis names a click
    reports, the ticks a click can pick (``targets``: position, ``[x0, x1]``,
    the ``values`` an end stores, the text shown), the bars drawn with their
    geometry, the bars that cannot be drawn and why, and the bars that do not
    fit under a typed Max. Plus the bars that name another figure.
    """
    endpoints = slot_endpoints(resolved)
    bounds = target_bounds(endpoints)
    layers = list(resolved.x_layers)
    placement = placement or FigurePlacement(bars={}, unfit={}, limits={})
    panels = []
    for index, panel in enumerate(resolved.panels):
        _, unresolved = bars_for_panel(resolved, panel, endpoints)
        occupied = _occupied_x(panel)
        x_axis, y_axis = axes.get(index, ("", ""))
        panels.append(
            {
                "index": index,
                "match": {
                    name: panel_key_text(value)
                    for name, value in panel_values(resolved, panel).items()
                },
                "display_title": resolved.text.panel_title(panel.key),
                "xaxis": x_axis,
                "yaxis": y_axis,
                "targets": [
                    {
                        "slot": end.slot,
                        "position": end.position,
                        "x0": bounds[end.slot][0],
                        "x1": bounds[end.slot][1],
                        "values": dict(end.values),
                        "x_text": end.x_text,
                        "label": " · ".join(
                            resolved.text.level(layer, end.values[layer]) for layer in layers
                        ),
                    }
                    for end in endpoints
                    if end.x_text in occupied and end.slot in bounds
                ],
                "bars": [bar.to_dict() for bar in placement.bars.get(index, [])],
                "unresolved": [
                    {"bar": item.bar.to_dict(), "reason": item.reason} for item in unresolved
                ],
                "unfit": [item.bar.to_dict() for item in placement.unfit.get(index, [])],
                "y_limits": list(placement.limits[index]) if index in placement.limits else None,
            }
        )
    return {
        "panels": panels,
        "not_in_figure": [bar.to_dict() for bar in not_in_figure(resolved)],
        # Whether placement used measured panels and labels (the export's
        # decisions) or the preview's estimate.
        "estimated": estimated,
    }
