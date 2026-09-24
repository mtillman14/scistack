"""
``ResolvedPlot`` — a spec plus data, reduced to exactly what a renderer draws.

This intermediate is the reason the interactive plotly view and the exported
matplotlib figure cannot drift apart. Compiling ``PlotSpec`` straight to each
renderer would mean writing the aggregation, the error-band definition, and the
facet ordering twice, in two libraries, and discovering the divergence in a
figure rather than in a test. Here all of that happens once, above the renderer
split; the renderers become dumb translators of panel frames plus encodings,
and the semantics are tested against golden ``ResolvedPlot`` fixtures with no
rendering involved.

It is also the integration point for a future MATLAB renderer: a third backend
is a new leaf, not a redesign.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from .aliases import DisplayText
from .spec import PlotKind, PlotSpec

#: Canonical column names inside a panel frame. Renderers address these, never
#: the user's original column names, so a renderer never needs the spec to know
#: which column is the x axis.
X = "__x"
Y = "__y"
Y_LOW = "__y_low"
Y_HIGH = "__y_high"
COLOR = "__color"
SERIES = "__series"
#: The UNCOLOURED part of a series identity — the grouping layers that are
#: neither the colour nor the sample. A line or band per level of these is
#: told apart by a dash style (docs/claude/grouping-and-collapse.md, D4),
#: which is what a scientist reads off a printed figure without hovering.
DASH = "__dash"
#: The overlay's own colour level (``PlotSpec.sample_color``): the shown key
#: that colours a "Show sample" point, beside ``__color`` — the MARK's level,
#: which paints the point when it has no colour of its own. Two columns
#: because the two colourings are independent by construction (a shown key
#: is collapsed, a coloured layer groups; no factor is both).
SAMPLE_COLOR = "__sample_color"
#: On a spaghetti figure, the id of the LINE an overlay point belongs to —
#: the marks' ``__series`` (the lines layer, plus the units when a collapsed
#: key is drawn one line each) recomposed on the overlay rows, so the point
#: is placed at its line's ``series_offsets`` shift rather than on the tick.
SAMPLE_LINE = "__line"
#: The RUN a joined point belongs to — the bracket a polyline stays inside.
#: A line spans the innermost tick only and never crosses a bracket (every
#: tick layer above the innermost, ``roles.GroupingLayers.brackets``; user
#: decision 2026-09-21), so a spaghetti's marks carry the brackets' values
#: composed, and a "Show sample" point the brackets plus, on a spaghetti, its
#: ``__line``. Renderers and the generated code draw one polyline per
#: ``(__series, __run)``; the OFFSET stays keyed by ``__series`` alone, so a
#: subject keeps its slot in every bracket. Empty when nothing splits the runs.
RUN = "__run"
Z = "__z"
#: The one x level a categorical figure has when NO tick layer was chosen —
#: a scalar measure grouped by its colour alone, or not grouped at all. The
#: mark frame and the overlay frame both write it as their ``__x``
#: (``reduce._panel_frame`` / ``reduce._overlay_frame``), and
#: ``ResolvedPlot.x_order`` lists it as the axis' single level so both
#: renderers read the axis as categorical (``render.base.is_categorical_x``)
#: — a "Show sample" overlay only draws on a categorical axis, and
#: ``x_positions`` would coerce the label to NaN on a numeric one.
UNLABELLED_X = ""

#: The dash cycle, in plotly's names; :data:`MPL_DASHES` is the matplotlib
#: spelling of the same six. One owner for both renderers and for the
#: generated seaborn code (``codegen``), which restates it as ``dashes=``.
#: Six is what stays distinguishable; past that the figure warns and
#: suggests Separate panels (``reduce._dash_styles``).
DASH_CYCLE: tuple[str, ...] = (
    "solid",
    "dash",
    "dot",
    "dashdot",
    "longdash",
    "longdashdot",
)
MPL_DASHES: dict[str, Any] = {
    "solid": "-",
    "dash": "--",
    "dot": ":",
    "dashdot": "-.",
    "longdash": (0, (8, 3)),
    "longdashdot": (0, (8, 3, 2, 3)),
}


@dataclass(frozen=True)
class Encoding:
    """Which canonical columns are populated, and what they mean."""

    x: str | None = X
    y: str | None = Y
    color: str | None = None
    y_low: str | None = None
    y_high: str | None = None
    series: str | None = None
    #: The dash-style key column (:data:`DASH`), when uncoloured grouping
    #: layers split a line or a band.
    dash: str | None = None
    z: str | None = None

    @property
    def has_error(self) -> bool:
        return self.y_low is not None and self.y_high is not None


@dataclass(frozen=True)
class Labels:
    x: str = ""
    y: str = ""
    color: str | None = None
    #: What the dash styles tell apart — the uncoloured series layers'
    #: display names, outermost first — for the legend's second block.
    dash: str | None = None
    #: The overlay's own colour key (``PlotSpec.sample_color``), display
    #: name, for the legend's block of overlay levels.
    sample: str | None = None
    title: str | None = None


@dataclass
class Panel:
    """One subplot: a tidy frame plus the facet values that identify it."""

    frame: pd.DataFrame
    #: Where this panel sits in the grid. Assigned once, in ``reduce``, so the
    #: renderers never have to re-derive a layout (and never disagree about it).
    grid_row: int = 0
    grid_col: int = 0
    #: All facet values keyed by factor name (empty when unfaceted).
    key: dict[str, Any] = field(default_factory=dict)
    #: This panel's y range, or None to autoscale.
    #:
    #: **The authority** — ``ResolvedPlot.y_limits`` is derived from these and
    #: exists only for the case where they all agree. Per panel rather than per
    #: figure because ``PlotSpec.y_axis.scope`` may separate limits by a FACET
    #: factor, which is what "autoscale each panel" means.
    y_limits: tuple[float, float] | None = None
    #: "Show sample" overlay rows for this panel, or None: ``__x`` (the
    #: same leaf key the marks use), ``__y``, ``__color`` when coloured,
    #: ``__series`` (the shown keys composed, outermost first — one line's
    #: identity when joined) and the shown key columns themselves, for hover.
    #: Built by ``reduce._overlay_frame``; drawn after the marks.
    sample: pd.DataFrame | None = None

    @property
    def title(self) -> str:
        """
        What names this panel: the facet VALUES only.

        The key is already obvious from the figure — every panel in a grid is
        faceted by the same factor, so repeating "ColName=" on all 13 subplots
        is noise. (``ResolvedPlot.figure_label`` keeps ``key=value``: there the
        figures are separate files and the key is not otherwise visible.)

        Renderers draw this as the panel's **y-axis title**, not as a caption
        above it (``render.base.panel_y_title``) — the axis title is room the
        panel already spends, so the name costs the grid no height. The property
        keeps its name because it is also the panel's identity in logs, in
        ``to_dict`` and in the GUI.
        """
        return " · ".join(str(v) for v in self.key.values())


@dataclass
class ResolvedPlot:
    """
    One figure, fully reduced.

    ``resolve()`` returns a LIST of these — one per combination of the spec's
    ITERATE factors, which is the interactive equivalent of the pipeline's
    ``for_each`` fan-out over iterated schema keys.
    """

    kind: PlotKind
    panels: list[Panel]
    encoding: Encoding
    labels: Labels
    spec: PlotSpec
    #: The ITERATE factor values that select this figure out of the fan-out.
    figure_key: dict[str, Any] = field(default_factory=dict)
    x_order: list[Any] | None = None
    #: Set when several factors share the x axis: the composed leaf order plus
    #: the spans each higher layer covers. ``x_order`` mirrors ``x_plan.order``
    #: so every existing consumer keeps working; renderers read this only to
    #: draw the group labels and brackets beneath the ticks.
    x_plan: Any = None
    #: The factors sharing the x axis, outermost first: ``x_plan`` depth ``d``
    #: is ``x_layers[d]`` and the last entry is the tick labels' own layer.
    #: With ``color_factor``, how a renderer knows which labelled row repeats
    #: the legend (``StyleOptions.hide_legend_ticks``).
    x_layers: list[str] = field(default_factory=list)
    #: The factor the marks are painted by, as resolved, or None.
    color_factor: str | None = None
    color_order: list[Any] | None = None
    #: Subplot grid shape, decided in ``reduce`` from FacetOptions.
    grid_rows: int = 1
    grid_cols: int = 1
    #: Headers for rule-defined rows/columns (empty when the panels just flow).
    row_labels: list[str] = field(default_factory=list)
    col_labels: list[str] = field(default_factory=list)
    #: Human-readable notes about placement decisions the user did not ask for —
    #: a panel that spilled out of its ruled cell, a grid that had to grow, a
    #: panel that matched no rule. The layout never silently disobeys a rule;
    #: it says what it did instead. Surfaced in the GUI's Layout section.
    layout_notes: list[str] = field(default_factory=list)
    #: The whole figure's y range — set only when every panel shares it, and
    #: ``None`` when they differ. Derived from the panels in ``reduce``, never
    #: computed separately, so the two can never disagree. A renderer reads the
    #: None as "give each panel its own axis" (``render.base.shares_y_axis``).
    y_limits: tuple[float, float] | None = None
    #: The factors that separated the limits, after ineligible ones were
    #: dropped. Echoed back so the GUI can say WHY the axis reads as it does.
    y_scope: list[str] = field(default_factory=list)
    #: Every factor that separates PANELS, as resolved: the figure's ITERATE
    #: keys in fan-out order, then its FACET factors. The factors a y-limit
    #: scope may name — offered by the GUI from HERE rather than from
    #: `spec.roles`, because a schema key promoted to ITERATE or a facet the
    #: table defaulted is a panel factor the spec never mentions.
    panel_factors: list[str] = field(default_factory=list)
    #: Set when the data was reduced for transport (see reduce.MAX_TRANSPORT_POINTS).
    downsampled_from: int | None = None
    #: Notes about the FIGURE SET rather than about this figure's layout — at
    #: present, schema keys promoted to ITERATE because a nested key iterates.
    #: Identical on every figure of a fan-out (it describes the fan-out), which
    #: is why the GUI reads it from the first one.
    fanout_notes: list[str] = field(default_factory=list)
    #: SPAGHETTI only: the horizontal shift each series (subject) keeps at every
    #: x position, keyed by its ``SERIES`` id. Decided once per FIGURE, not per
    #: panel, so a subject sits at the same offset in every facet — and by one
    #: function (:func:`scistackplot.spaghetti.series_offsets`) so both
    #: renderers and the generated code place the same marker in the same spot.
    series_offsets: dict[str, float] = field(default_factory=dict)
    #: ``{dash id: dash name}`` for the whole figure (:data:`DASH_CYCLE`),
    #: decided once in ``reduce`` so every panel and both renderers draw the
    #: same level in the same style, and the legend can list them.
    dash_styles: dict[str, str] = field(default_factory=dict)
    #: "Show sample" (``PlotSpec.show_sample``): the shown keys, outermost
    #: first — the identity of one overlay point — empty when no overlay is
    #: drawn. ``sample_join`` says whether the points sharing that identity
    #: are joined across x, ``sample_join_reason`` why (``roles.overlay_join``),
    #: and ``sample_offsets`` the per-identity horizontal shift inside its
    #: mark's slot (``spaghetti.overlay_offsets``), decided once per figure
    #: like ``series_offsets`` so a subject sits in the same place in every
    #: panel and both renderers and the generated code agree.
    sample_shown: list[str] = field(default_factory=list)
    sample_join: bool = False
    sample_join_reason: str = ""
    sample_offsets: dict[str, float] = field(default_factory=dict)
    #: The shown key colouring the overlay (``roles.overlay_color``), None
    #: when a point takes its mark's colour; and that key's levels in
    #: declared order across the WHOLE figure — the same rule as
    #: ``color_order``, so subject 03 is the same colour in every panel
    #: (``render.base.sample_palette_for`` indexes it, never a panel's own
    #: enumeration).
    sample_color: str | None = None
    sample_color_order: list[Any] = field(default_factory=list)
    #: How every level and name READS in this figure (``aliases.DisplayText``:
    #: the plot's aliases over the project's). Renderers draw text through
    #: this and never stringify a level themselves; every identity above
    #: (keys, orders, offsets) stays raw. Empty reads everything raw.
    text: DisplayText = field(default_factory=DisplayText)
    #: What the GUI's Labels section offers for this figure (``aliases.labelable``):
    #: the measure and every factor drawn as text, each with its alias key,
    #: name and levels as ``{raw, text, origin}``. Shipped as
    #: ``layout.meta.labelable``; the panel never derives the list.
    labelable: list[dict] = field(default_factory=list)

    @property
    def figure_label(self) -> str:
        """Human-readable identifier for this figure within the fan-out."""
        if not self.figure_key:
            return self.labels.title or ""
        return ", ".join(f"{k}={v}" for k, v in self.figure_key.items())

    @property
    def row_count(self) -> int:
        return sum(len(p.frame) for p in self.panels)

    def to_dict(self) -> dict:
        """JSON-serializable form — used by the GUI transport and by tests."""
        return {
            "kind": str(self.kind),
            "figure_key": {k: _jsonable(v) for k, v in self.figure_key.items()},
            "figure_label": self.figure_label,
            "encoding": {
                "x": self.encoding.x,
                "y": self.encoding.y,
                "color": self.encoding.color,
                "y_low": self.encoding.y_low,
                "y_high": self.encoding.y_high,
                "series": self.encoding.series,
                "dash": self.encoding.dash,
                "z": self.encoding.z,
            },
            "labels": {
                "x": self.labels.x,
                "y": self.labels.y,
                "color": self.labels.color,
                "dash": self.labels.dash,
                "sample": self.labels.sample,
                "title": self.labels.title,
            },
            "grid": {
                "rows": self.grid_rows,
                "cols": self.grid_cols,
                "row_labels": list(self.row_labels),
                "col_labels": list(self.col_labels),
                "layout_notes": list(self.layout_notes),
            },
            "x_order": [_jsonable(v) for v in (self.x_order or [])] or None,
            "x_groups": [
                {
                    "label": group.label,
                    "depth": group.depth,
                    "start": group.start,
                    "end": group.end,
                }
                for group in (self.x_plan.groups if self.x_plan else [])
            ],
            "color_order": [_jsonable(v) for v in (self.color_order or [])] or None,
            "y_limits": list(self.y_limits) if self.y_limits else None,
            "y_scope": list(self.y_scope),
            "panel_factors": list(self.panel_factors),
            "downsampled_from": self.downsampled_from,
            "fanout_notes": list(self.fanout_notes),
            "series_offsets": dict(self.series_offsets),
            "dash_styles": dict(self.dash_styles),
            "sample": {
                "shown": list(self.sample_shown),
                "join": self.sample_join,
                "join_reason": self.sample_join_reason,
                "offsets": dict(self.sample_offsets),
                "color": self.sample_color,
                "color_order": [_jsonable(v) for v in self.sample_color_order],
            },
            "panels": [
                {
                    "key": {k: _jsonable(v) for k, v in panel.key.items()},
                    "title": panel.title,
                    # The TEXT the renderers draw; `title` above is the identity
                    # (facet layout rules and logs match it raw).
                    "display_title": self.text.panel_title(panel.key),
                    "grid_row": panel.grid_row,
                    "grid_col": panel.grid_col,
                    # Per panel, because the figure-level value is absent
                    # exactly when the panels differ — which is the case the
                    # interactive view most needs to draw correctly.
                    "y_limits": list(panel.y_limits) if panel.y_limits else None,
                    "rows": _frame_records(panel.frame),
                    "sample": (
                        _frame_records(panel.sample) if panel.sample is not None else None
                    ),
                }
                for panel in self.panels
            ],
        }


def _frame_records(frame: pd.DataFrame) -> list[dict]:
    return [
        {key: _jsonable(value) for key, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float)):
        return value
    # numpy scalars and arrays, pandas NA, Timestamps, ...
    if hasattr(value, "tolist"):
        return value.tolist()
    if pd.isna(value):
        return None
    return str(value)
