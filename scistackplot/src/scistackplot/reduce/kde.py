"""Kernel density estimates — the one owner of the KDE kind's curve.

User request (2026-11-01): a smoothed distribution that shows finer
differences than a histogram's bins can. Plan:
`.claude/plan-histogram-special-bins-and-kde.md`.

- **The curve** (:data:`KDE_SOURCE`): a Gaussian kernel with Scott's
  bandwidth (``std(ddof=1) * n ** -1/5``) times
  ``KdeOptions.bandwidth_adjust``, on :data:`GRID_POINTS` evenly spaced points
  from ``min - cut * bw`` to ``max + cut * bw``. This is exactly scipy's
  ``gaussian_kde`` with ``bw_method="scott"``, as seaborn uses it, written in
  pure numpy. This module executes it and ``codegen`` emits it verbatim, so the
  exported curve is the previewed curve (the ``bins.EDGES_SOURCE`` bargain).
- **Normalisation**: per colour level within a panel (area 1), like a
  histogram's percent, so two groups of different sizes compare by shape.
- **No curve** for a level with fewer than two finite values or no spread:
  nothing is drawn, and a layout note says why.

Values are axis numbers. A datetime measure arrives as wall-clock ms
(``temporal.apply_temporal``), so its bandwidth is in ms on a date x axis.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scistacklog import Log

LAYER = "scistackplot"

#: Points along each curve: seaborn's default ``gridsize``.
GRID_POINTS = 200
#: The curve's line width (pt) and, when filled, the fill's opacity. Stated,
#: because seaborn's and plotly's defaults differ. Both renderers and the
#: export read these.
LINE_WIDTH = 1.5
FILL_ALPHA = 0.25
#: The y axis title a KDE draws unless the user typed one (reduce, codegen).
Y_TITLE = "Density"

KDE_SOURCE = '''
def _scistack_kde(values, bandwidth_adjust, cut, gridsize):
    """(xs, density) of a Gaussian KDE (Scott's bandwidth x adjust), or None
    with fewer than two finite values or no spread: the scistackplot.reduce.kde rule."""
    import numpy as np

    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if values.size < 2:
        return None
    std = float(np.std(values, ddof=1))
    if not std > 0:
        return None
    bandwidth = std * values.size ** (-1.0 / 5.0) * float(bandwidth_adjust)
    xs = np.linspace(values.min() - cut * bandwidth, values.max() + cut * bandwidth, int(gridsize))
    density = np.zeros(xs.size)
    for start in range(0, values.size, 4096):
        chunk = values[start:start + 4096]
        z = (xs[:, None] - chunk[None, :]) / bandwidth
        density += np.exp(-0.5 * z * z).sum(axis=1)
    density /= values.size * bandwidth * np.sqrt(2.0 * np.pi)
    return xs, density
'''

_NAMESPACE: dict = {}


def _kde_function():
    if "_scistack_kde" not in _NAMESPACE:
        exec(KDE_SOURCE, _NAMESPACE)  # noqa: S102 - our own constant
    return _NAMESPACE["_scistack_kde"]


def kde_problem(bandwidth_adjust: float, cut: float) -> str | None:
    """Why these settings cannot draw a curve, or None. ``roles`` refuses the
    spec with it and the capability report shows it beside the controls."""
    if not bandwidth_adjust > 0 or not np.isfinite(bandwidth_adjust):
        return f"bandwidth adjust {bandwidth_adjust!r} must be greater than zero."
    if not cut >= 0 or not np.isfinite(cut):
        return f"cut {cut!r} must be zero or more."
    return None


def curve_table(
    frame: pd.DataFrame,
    options,
    *,
    value: str,
    color: str | None,
    color_order: list[Any],
) -> tuple[pd.DataFrame, list[str]]:
    """One panel's curves: a row per (colour level, grid point), columns
    ``X``, ``Y``, ``COUNT`` (the level's n) and the colour column, in colour
    order. Also returns a note for each level that drew nothing."""
    from .resolved import COUNT, X, Y

    columns = [X, Y, COUNT, *([color] if color else [])]
    if color and color in frame.columns:
        present = {str(v) for v in frame[color].dropna().unique()}
        levels = [lvl for lvl in color_order if str(lvl) in present]
        levels += [lvl for lvl in frame[color].dropna().unique() if str(lvl) not in {str(x) for x in levels}]
        groups = [(lvl, frame[frame[color].astype(str) == str(lvl)]) for lvl in levels]
    else:
        groups = [(None, frame)]
    kde = _kde_function()
    parts, notes = [], []
    for level, rows in groups:
        values = pd.to_numeric(rows[value], errors="coerce").to_numpy(dtype=float)
        n = int(np.isfinite(values).sum())
        drawn = kde(values, options.bandwidth_adjust, options.cut, GRID_POINTS)
        if drawn is None:
            who = f"{level}" if level is not None else "The data"
            notes.append(
                f"{who}: no density curve — it needs at least two different values (has {n})."
            )
            continue
        xs, density = drawn
        part = pd.DataFrame({X: xs, Y: density, COUNT: n})
        if color:
            part[color] = level
        parts.append(part)
    if not parts:
        return pd.DataFrame(columns=columns), notes
    return pd.concat(parts, ignore_index=True)[columns], notes


def density_limits(tops: list[float]) -> tuple[float, float]:
    """``(0, top * 1.05)``: the curves from zero to 5% above the highest
    point, stated in the preview and the export alike."""
    finite = [t for t in tops if t == t]
    top = max(finite) if finite else 0.0
    return (0.0, top * 1.05 if top > 0 else 1.0)


def x_range(frames: list[pd.DataFrame]) -> tuple[float, float] | None:
    """The figure's x range: exactly the curves' extent, every panel."""
    from .resolved import X

    lows = [float(f[X].min()) for f in frames if X in f.columns and not f.empty]
    highs = [float(f[X].max()) for f in frames if X in f.columns and not f.empty]
    if not lows:
        return None
    return (min(lows), max(highs))


def log_curves(n_panels: int, options, notes: list[str]) -> None:
    Log.info(
        "kde: %d panel(s), bandwidth x%g, cut %g, %s%s",
        n_panels,
        options.bandwidth_adjust,
        options.cut,
        "filled" if options.fill else "lines",
        f", {len(notes)} level(s) without a curve" if notes else "",
        layer=LAYER,
    )
