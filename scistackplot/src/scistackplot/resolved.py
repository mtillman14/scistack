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
Z = "__z"


@dataclass(frozen=True)
class Encoding:
    """Which canonical columns are populated, and what they mean."""

    x: str | None = X
    y: str | None = Y
    color: str | None = None
    y_low: str | None = None
    y_high: str | None = None
    series: str | None = None
    z: str | None = None

    @property
    def has_error(self) -> bool:
        return self.y_low is not None and self.y_high is not None


@dataclass(frozen=True)
class Labels:
    x: str = ""
    y: str = ""
    color: str | None = None
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
    #: Set when the data was reduced for transport (see reduce.MAX_TRANSPORT_POINTS).
    downsampled_from: int | None = None
    #: Notes about the FIGURE SET rather than about this figure's layout — at
    #: present, schema keys promoted to ITERATE because a nested key iterates.
    #: Identical on every figure of a fan-out (it describes the fan-out), which
    #: is why the GUI reads it from the first one.
    fanout_notes: list[str] = field(default_factory=list)

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
                "z": self.encoding.z,
            },
            "labels": {
                "x": self.labels.x,
                "y": self.labels.y,
                "color": self.labels.color,
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
            "downsampled_from": self.downsampled_from,
            "fanout_notes": list(self.fanout_notes),
            "panels": [
                {
                    "key": {k: _jsonable(v) for k, v in panel.key.items()},
                    "title": panel.title,
                    "grid_row": panel.grid_row,
                    "grid_col": panel.grid_col,
                    # Per panel, because the figure-level value is absent
                    # exactly when the panels differ — which is the case the
                    # interactive view most needs to draw correctly.
                    "y_limits": list(panel.y_limits) if panel.y_limits else None,
                    "rows": _frame_records(panel.frame),
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
