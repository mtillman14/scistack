"""
Renderer protocol and the layout maths both renderers share.

A renderer translates a :class:`~scistackplot.resolved.ResolvedPlot` into a
backend figure. It performs **no** data reduction — every aggregation, ordering
and error band was decided in ``reduce.resolve``. Keeping renderers dumb is
what stops the interactive view and the exported figure from disagreeing.
"""

from __future__ import annotations

import math
from typing import Any, Protocol

import numpy as np
import pandas as pd

from ..resolved import ResolvedPlot


class Renderer(Protocol):
    """Anything that can draw a ResolvedPlot."""

    def render(self, resolved: ResolvedPlot) -> Any:  # pragma: no cover - protocol
        ...


def grid_shape(resolved: ResolvedPlot) -> tuple[int, int]:
    """Rows and columns of the subplot grid, as decided in ``reduce``."""
    return max(1, resolved.grid_rows), max(1, resolved.grid_cols)


def panel_position(resolved: ResolvedPlot, panel_index: int) -> tuple[int, int]:
    """Zero-based (row, column) of a panel. Layout is not the renderer's job."""
    panel = resolved.panels[panel_index]
    return panel.grid_row, panel.grid_col


def occupied_cells(resolved: ResolvedPlot) -> set[tuple[int, int]]:
    """Every grid cell that holds a panel — the basis for the axis rules."""
    return {(p.grid_row, p.grid_col) for p in resolved.panels}


def shows_x_labels(resolved: ResolvedPlot, row: int, col: int) -> bool:
    """
    Whether this cell carries the x tick labels AND the x axis title.

    ONE rule for both, deliberately: "nothing directly below" rather than
    "bottom row", because a wrapped grid's last row is usually partial and the
    panels above those empty cells are the bottom of their own column. The two
    used to drift — the title followed this rule while the tick labels were
    re-applied to every panel.
    """
    return (row + 1, col) not in occupied_cells(resolved)


def shows_y_labels(resolved: ResolvedPlot, row: int, col: int) -> bool:
    """Same idea on the other axis: nothing directly to the left.

    **Unless the panels are on different scales**, in which case every panel
    keeps its numbers. Hiding them is only honest when the hidden numbers would
    have been identical; a grid of independently-scaled panels labelled down the
    left column only reads as one shared scale, which is precisely the misread
    that per-panel limits exist to enable in the first place.
    """
    if not shares_y_axis(resolved):
        return True
    return (row, col - 1) not in occupied_cells(resolved)


def panel_y_title(resolved: ResolvedPlot, panel, *, leftmost: bool) -> str:
    """The y-axis title one panel carries.

    Keyed off the panel's own facet KEY rather than off the spec's roles, so a
    FACET factor that resolved to a single keyless panel reads as the ordinary
    plot it is instead of as a grid of one.

    ONE rule for both renderers, and the reason a faceted figure has no subplot
    captions: **the facet values ARE the y-axis title**. A caption above every
    panel costs a strip of vertical room in each row of the grid — the axis
    title is room the panel was already spending, so the same information
    arrives for free and the panels get the height back.

    It follows that a faceted panel labels its axis wherever it sits: the text
    identifies THIS panel, so the "leftmost only" rule (which exists to stop a
    shared label being repeated) does not apply to it.
    """
    if panel is not None and panel.key:
        return panel.title
    return resolved.labels.y if leftmost else ""


def is_categorical_x(resolved: ResolvedPlot) -> bool:
    """
    Whether the x axis is a set of discrete positions rather than a number line.

    A factor on x is categorical; a 1-D index or a second measure is numeric.
    """
    if resolved.x_order is None:
        return False
    return not all(_is_number(value) for value in resolved.x_order)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, np.integer, np.floating)) and not isinstance(
        value, bool
    )


def x_positions(
    values: pd.Series, resolved: ResolvedPlot
) -> tuple[np.ndarray, list[str] | None]:
    """
    Map x values to plotting positions.

    Returns ``(positions, tick_labels)``. ``tick_labels`` is None for a numeric
    axis; for a categorical axis it is the ordered level labels, and positions
    are their indices — which is what puts "01, 02, … 10" in the right order
    instead of pandas' lexicographic 1, 10, 2.
    """
    if not is_categorical_x(resolved):
        return pd.to_numeric(values, errors="coerce").to_numpy(dtype=float), None

    order = [str(v) for v in (resolved.x_order or [])]
    lookup = {label: position for position, label in enumerate(order)}
    positions = np.array(
        [lookup.get(str(v), np.nan) for v in values], dtype=float
    )
    return positions, order


def color_groups(
    frame: pd.DataFrame, resolved: ResolvedPlot
) -> list[tuple[Any, pd.DataFrame]]:
    """Split a panel frame into colour series, in declared level order."""
    color_column = resolved.encoding.color
    if not color_column or color_column not in frame.columns:
        return [(None, frame)]

    order = resolved.color_order or []
    groups: list[tuple[Any, pd.DataFrame]] = []
    seen = set()
    for level in order:
        subset = frame[frame[color_column].astype(str) == str(level)]
        if len(subset):
            groups.append((level, subset))
            seen.add(str(level))
    # Anything the declared order missed (shouldn't happen, but never drop data).
    for level in frame[color_column].dropna().unique():
        if str(level) not in seen:
            groups.append((level, frame[frame[color_column] == level]))
    return groups


def legend_levels(resolved: ResolvedPlot) -> list[Any]:
    """
    The colour levels a legend would list, in drawn order.

    Read off the PANELS rather than off ``color_order`` because a legend must
    describe what is actually on the figure: a colour factor can arrive with
    levels that this figure never draws (a filter removed them, or an ITERATE
    slice only contains one of them), and those must not be counted.
    """
    color_column = resolved.encoding.color
    if not color_column:
        return []
    present: dict[str, Any] = {}
    for panel in resolved.panels:
        if color_column not in panel.frame.columns:
            continue
        # unique(), not color_groups(): this is only a level count, and masking
        # every panel once per declared level would walk the whole figure's data
        # a second time on the export path (which is not downsampled).
        for value in panel.frame[color_column].dropna().unique():
            present.setdefault(str(value), value)

    # Declared order first, then anything it missed — the same ordering rule
    # ``color_groups`` uses, so the legend lists the series in drawn order.
    ordered = [
        present[str(level)]
        for level in (resolved.color_order or [])
        if str(level) in present
    ]
    seen = {str(level) for level in ordered}
    ordered.extend(value for key, value in present.items() if key not in seen)
    return ordered


def shares_y_axis(resolved: ResolvedPlot) -> bool:
    """Whether every panel draws the same y range, so one axis can serve them.

    ONE rule, shared by both renderers — the same bargain as
    :func:`shows_legend`. It is *derived*, not configured: the user says which
    factors separate limits (``PlotSpec.y_axis.scope``) and whether the panels
    end up agreeing follows from that plus the data.

    It matters beyond the range itself, which is why it is a function rather
    than an inline check. A shared axis also hides the inner panels' tick
    labels: right when they genuinely share a scale, and a silent lie the moment
    they do not — a grid of panels at different scales with numbers on only the
    left column reads as one scale.
    """
    return resolved.y_limits is not None


def panel_y_limits(resolved: ResolvedPlot, panel) -> tuple[float, float] | None:
    """The range one panel draws, falling back to the figure's.

    The fallback matters for a HEATMAP, whose panels carry no y limits at all,
    and for any panel a scope left without a group of its own.
    """
    return panel.y_limits or resolved.y_limits


def drawable_limits(
    limits: tuple[float, float], *, log: bool
) -> tuple[float, float] | None:
    """``limits`` an axis of this type can actually hold, in DATA units.

    A non-positive end on a log axis cannot be drawn: matplotlib ignores it
    with a warning and plotly takes ``log10`` of it and gets NaN. `ylimits`
    never produces one (its floor is the smallest positive drawn value) but a
    hand-typed one can arrive, so a floor at or below zero is dropped to a
    decade under the ceiling, and a range with no positive part is None — let
    the renderer autoscale rather than draw an empty axis.
    """
    if not log:
        return limits
    low, high = limits
    if high <= 0:
        return None
    if low <= 0:
        low = high / 10.0
    return (low, high)


def axis_range(
    limits: tuple[float, float], *, log: bool
) -> tuple[float, float] | None:
    """``limits`` in the units plotly's ``range`` wants for this axis type.

    Limits are computed and carried in DATA units everywhere (`Panel.y_limits`
    is what the user reads back in the GUI). matplotlib's ``set_ylim`` takes
    them as they are; plotly's ``range`` on a ``type: "log"`` axis is in
    **log10 units** — ``[0.95, 105]`` handed over verbatim asked for
    10^0.95 .. 10^105 and the figure came back empty.
    """
    drawable = drawable_limits(limits, log=log)
    if drawable is None or not log:
        return drawable
    return (math.log10(drawable[0]), math.log10(drawable[1]))


def shows_legend(resolved: ResolvedPlot) -> bool:
    """
    Whether this figure gets a legend at all.

    ONE rule, shared by both renderers and mirrored by the generated seaborn
    code (``codegen``): the legend exists only to tell colour series apart, so
    a single level makes it pure noise — it restates the one thing every mark
    on the figure already has in common, and it costs the panels width.
    """
    return len(legend_levels(resolved)) > 1


#: A colour-blind-safe qualitative palette, used when the spec names none.
#: Okabe–Ito, which stays distinguishable in greyscale print.
DEFAULT_PALETTE = (
    "#0072B2",
    "#D55E00",
    "#009E73",
    "#CC79A7",
    "#E69F00",
    "#56B4E9",
    "#F0E442",
    "#000000",
)


def palette_color(index: int, palette: tuple[str, ...] = DEFAULT_PALETTE) -> str:
    return palette[index % len(palette)]


def palette_for(resolved: ResolvedPlot, level: Any, fallback: int) -> str:
    """The colour a level carries — the same one in every panel.

    Indexed by the level's position in the **declared** order, never by how many
    groups this particular panel happened to draw. :func:`color_groups` omits a
    level with no rows in the panel it is splitting, so enumerating its output
    handed the *next* level that level's colour: one series drawn in two colours
    across a facet grid, with the legend agreeing with only some of the panels.
    A figure wrong in a way that looks like data.

    ``fallback`` covers a level the declared order never mentioned — the same
    "never drop data" case ``color_groups`` ends with.
    """
    order = resolved.color_order or []
    for position, candidate in enumerate(order):
        if str(candidate) == str(level):
            return palette_color(position)
    return palette_color(fallback)
