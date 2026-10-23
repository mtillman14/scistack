"""Regression lines — how a plot DRAWS a fit. The fit itself is not defined here.

The numbers (slope, intercept, R², p, the confidence band) belong to
``csvstats.regression.linear_fit``, the one owner of the formula, as the
error bars' belong to ``csvstats.descriptive`` (``spread.py``). The exported
code calls the same function, so a figure's line and its saved copy cannot
disagree.

This module owns the drawing policy, once for both renderers and the export:

- **Which points one line goes through** (``RegressionOptions.fit_by``): one
  fit per colour level, or one per panel through every point. Uncoloured,
  the two are the same. A fit is per PANEL: facets never pool.
- **Where the line runs**: over its own points' x range, never extrapolated.
- **The annotation** (:data:`STATS_TEXT_SOURCE`, executed here and emitted
  verbatim by ``codegen``): ``R² = 0.42, p = 0.003, n = 25``; p below 0.001
  reads ``p < 0.001``. No slope: on a datetime x it would be per
  millisecond (user, 2026-10-22). Several fits in one panel each get a line
  of text in their own colour, prefixed with their level, stacked from the
  chosen corner (``RegressionOptions.stats_corner``).
- **A fit that cannot be made** (fewer than 3 points, or no spread in x) draws
  no line and no text, and is reported in ``layout_notes`` and the log.

The x values are axis numbers: a datetime x arrives as wall-clock ms
(``temporal``), which ``linear_fit`` centres before fitting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd
from csvstats.regression import LinearFit, linear_fit
from scistacklog import Log

from .spec import FitBy, RegressionOptions, StatsCorner

LAYER = "scistackplot"

#: Points along a fitted line: two would draw the line, the band curves.
FIT_POINTS = 50
#: The annotation's distance from its panel's corner, in points.
ANNOTATION_INSET_PT = 4.0
#: One annotation line's height, in multiples of its font size.
ANNOTATION_LINE_SPACING = 1.3
#: The fitted line's width (pt) and the band's fill opacity.
FIT_LINE_WIDTH = 1.5
BAND_ALPHA = 0.2

# The annotation text. ONE source: annotation_text executes it, codegen emits
# it verbatim. Raw, so the generated file keeps the escape (ASCII-only).
STATS_TEXT_SOURCE = r'''
def _scistack_fit_text(r_squared, p_value, n, label=None):
    """One fit's annotation: the scistackplot.fits rule."""
    if p_value != p_value:
        p_text = "p = n/a"
    elif p_value < 0.001:
        p_text = "p < 0.001"
    else:
        p_text = f"p = {p_value:.3f}"
    r_text = "n/a" if r_squared != r_squared else f"{r_squared:.2f}"
    text = f"R² = {r_text}, {p_text}, n = {n}"
    return f"{label}: {text}" if label else text
'''

_NAMESPACE: dict = {}


def annotation_text(fit: LinearFit, label: str | None = None) -> str:
    """The annotation one fit draws (see :data:`STATS_TEXT_SOURCE`)."""
    if "_scistack_fit_text" not in _NAMESPACE:
        exec(STATS_TEXT_SOURCE, _NAMESPACE)  # noqa: S102 - our own constant
    return _NAMESPACE["_scistack_fit_text"](fit.r_squared, fit.p_value, fit.n, label)


@dataclass
class PanelFit:
    """One fitted line of one panel."""

    #: The colour level the fit is drawn for, or None (pooled / uncoloured).
    level: Any
    fit: LinearFit
    #: The line (and band) sampled over the points' own x range.
    xs: tuple[float, ...] = ()
    ys: tuple[float, ...] = ()
    low: tuple[float, ...] | None = None
    high: tuple[float, ...] | None = None
    #: The annotation as drawn ("" when the fit does not exist).
    text: str = ""

    def to_dict(self) -> dict:
        """JSON-safe: a missing number (NaN) is None, never ``NaN``."""
        numbers = {
            key: (None if isinstance(value, float) and value != value else value)
            for key, value in self.fit.to_dict().items()
        }
        return {
            "level": None if self.level is None else str(self.level),
            **numbers,
            "text": self.text,
        }


def panel_fits(
    frame: pd.DataFrame,
    options: RegressionOptions,
    *,
    x: str,
    y: str,
    color: str | None,
    color_order: list[Any],
    label_of: Callable[[Any], str],
    panel: str = "",
) -> tuple[list[PanelFit], list[str]]:
    """``(fits, notes)`` for one panel's points.

    ``label_of`` turns a colour level into its display text (the figure's
    aliases). ``notes`` say which fits could not be made, for the figure's
    ``layout_notes``.
    """
    if color and color in frame.columns and options.fit_by is FitBy.COLOR:
        present = {str(v) for v in frame[color].dropna().unique()}
        levels = [lvl for lvl in color_order if str(lvl) in present]
        groups = [(lvl, frame[frame[color].astype(str) == str(lvl)]) for lvl in levels]
    else:
        groups = [(None, frame)]

    fits: list[PanelFit] = []
    notes: list[str] = []
    labelled = len(groups) > 1
    for level, rows in groups:
        xs = pd.to_numeric(rows[x], errors="coerce").to_numpy(dtype=float)
        ys = pd.to_numeric(rows[y], errors="coerce").to_numpy(dtype=float)
        fit = linear_fit(xs, ys)
        where = " · ".join(part for part in (panel, label_of(level) if level is not None else "") if part)
        if not fit.ok:
            notes.append(f"No line for {where or 'the panel'}: {fit.reason}.")
            Log.info("regression fit skipped (%s): %s", where or "panel", fit.reason, layer=LAYER)
            fits.append(PanelFit(level=level, fit=fit))
            continue
        line_x = np.linspace(fit.x_min, fit.x_max, FIT_POINTS)
        low, high = fit.band(line_x)
        text = annotation_text(fit, label_of(level) if (labelled and level is not None) else None)
        Log.info(
            "regression fit (%s): n=%d slope=%.6g intercept=%.6g R²=%.4f p=%.4g",
            where or "panel", fit.n, fit.slope, fit.intercept, fit.r_squared, fit.p_value,
            layer=LAYER,
        )
        fits.append(
            PanelFit(
                level=level,
                fit=fit,
                xs=tuple(float(v) for v in line_x),
                ys=tuple(float(v) for v in fit.predict(line_x)),
                low=tuple(float(v) for v in low) if options.ci_band else None,
                high=tuple(float(v) for v in high) if options.ci_band else None,
                text=text,
            )
        )
    return fits, notes


@dataclass(frozen=True)
class Corner:
    """Where a panel's annotation block is anchored, in axes fraction."""

    x: float
    y: float
    ha: str
    va: str
    #: +1 / -1: which way the inset and each further line move.
    dx: int
    dy: int


_CORNERS = {
    StatsCorner.UPPER_LEFT: Corner(0.0, 1.0, "left", "top", 1, -1),
    StatsCorner.UPPER_RIGHT: Corner(1.0, 1.0, "right", "top", -1, -1),
    StatsCorner.LOWER_LEFT: Corner(0.0, 0.0, "left", "bottom", 1, 1),
    StatsCorner.LOWER_RIGHT: Corner(1.0, 0.0, "right", "bottom", -1, 1),
}


def corner(options: RegressionOptions) -> Corner:
    return _CORNERS[options.stats_corner]


def annotation_offsets(index: int, count: int, font_pt: float, where: Corner) -> tuple[float, float]:
    """``(dx, dy)`` in points from the corner for annotation line ``index``
    of ``count``. Lines read top to bottom in fit order from either corner:
    at the bottom the LAST line sits on the inset."""
    step = font_pt * ANNOTATION_LINE_SPACING
    row = index if where.dy < 0 else count - 1 - index
    return (where.dx * ANNOTATION_INSET_PT, where.dy * (ANNOTATION_INSET_PT + row * step))


def drawn(fits: list[PanelFit]) -> list[PanelFit]:
    """The fits that draw anything."""
    return [f for f in fits if f.fit.ok]


__all__ = [
    "ANNOTATION_INSET_PT",
    "ANNOTATION_LINE_SPACING",
    "BAND_ALPHA",
    "FIT_LINE_WIDTH",
    "FIT_POINTS",
    "PanelFit",
    "STATS_TEXT_SOURCE",
    "annotation_offsets",
    "annotation_text",
    "corner",
    "drawn",
    "panel_fits",
]

