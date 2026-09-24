"""
How heavy the points and lines of a "Show sample" overlay and of a spaghetti
are drawn — the ONE owner.

``StyleOptions.sample_weight`` / ``StyleOptions.line_weight`` are
multipliers (user, 2026-09-24): one number scales the marker DIAMETER and the
line WIDTH together, so a bolder sample keeps its proportions. ``1.0`` is the
look both had before the knobs existed. Every reader — ``render_matplotlib``,
the plotly preview and the generated seaborn code — takes its sizes from
:func:`sample_weight` / :func:`spaghetti_weight`; none holds a width of its
own (the spaghetti's 1.2 pt used to be restated in both renderers, and the
export drew seaborn's defaults instead).

Pure, no matplotlib import. See docs/claude/show-sample-overlay.md
("Mark weights").
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from .spec import PlotKind, StyleOptions

#: The overlay's points against the marks: smaller (by AREA) than a scatter
#: point, so the marks stay the figure and the points the evidence behind it.
SAMPLE_MARKER_FRACTION = 0.45
#: The overlay's joining lines at 1x, in points (and px in the preview).
SAMPLE_LINE_WIDTH = 1.0
#: A spaghetti's own polylines at 1x.
SPAGHETTI_LINE_WIDTH = 1.2
#: The plotly preview draws a marker of ``d`` pt at ``d x 4/3`` px — the
#: 96/72 pt-to-px ratio. Pinned so 1x reproduces the preview as it was (8 px
#: markers where matplotlib draws 6 pt). Lines are 1 pt = 1 px, as before.
PLOTLY_PX_PER_PT = 4.0 / 3.0


def weight_problem(value: object) -> str | None:
    """Why *value* is not a usable weight, or None. ``roles.validate`` refuses
    with this sentence; the resolvers below never see a bad value."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return f"must be a number, got {type(value).__name__}"
    if not math.isfinite(value) or value <= 0:
        return f"must be a positive multiplier (1 = default), got {value!r}"
    return None


@dataclass(frozen=True)
class ResolvedWeight:
    """One drawn element's sizes at its weight."""

    weight: float
    #: Marker diameter in points (matplotlib ``plot(markersize=)``).
    marker_pt: float
    #: Line width in points (and px in the preview).
    line_pt: float

    @property
    def marker_area(self) -> float:
        """matplotlib ``scatter(s=)``: an AREA in pt²."""
        return self.marker_pt**2

    @property
    def marker_px(self) -> float:
        """plotly ``marker.size``: a diameter in px."""
        return self.marker_pt * PLOTLY_PX_PER_PT

    def to_dict(self) -> dict:
        return {
            "weight": self.weight,
            "marker_pt": round(self.marker_pt, 3),
            "line_pt": round(self.line_pt, 3),
        }

    def describe(self) -> str:
        return f"{self.weight:g}x (marker {self.marker_pt:.1f} pt, line {self.line_pt:.2f} pt)"


def sample_weight(style: StyleOptions) -> ResolvedWeight:
    """The "Show sample" overlay's points and joining lines."""
    w = float(style.sample_weight)
    return ResolvedWeight(
        weight=w,
        marker_pt=math.sqrt(style.marker_size * SAMPLE_MARKER_FRACTION) * w,
        line_pt=SAMPLE_LINE_WIDTH * w,
    )


def spaghetti_weight(style: StyleOptions) -> ResolvedWeight:
    """A spaghetti's own markers and polylines. The marker is a scatter
    point's size (``marker_size`` is an area, so its square root)."""
    w = float(style.line_weight)
    return ResolvedWeight(
        weight=w,
        marker_pt=math.sqrt(style.marker_size) * w,
        line_pt=SPAGHETTI_LINE_WIDTH * w,
    )


def mark_weights_meta(resolved) -> dict:
    """``layout.meta.mark_weights`` for the GUI: which knob applies to this
    figure and what it resolves to. Python decides applicability, so the
    panel never tests the kind itself."""
    style = resolved.spec.style
    return {
        "sample": {
            "applies": bool(resolved.sample_shown),
            **sample_weight(style).to_dict(),
        },
        "lines": {
            "applies": resolved.kind is PlotKind.SPAGHETTI,
            **spaghetti_weight(style).to_dict(),
        },
    }


def describe_weights(resolved) -> str:
    """The log fragment naming the weights that apply ("" when none does)."""
    style = resolved.spec.style
    parts = []
    if resolved.kind is PlotKind.SPAGHETTI:
        parts.append(f"lines {spaghetti_weight(style).describe()}")
    if resolved.sample_shown:
        parts.append(f"sample {sample_weight(style).describe()}")
    return "; ".join(parts)
