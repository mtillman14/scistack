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
from scistacklog import Log

from ..resolved import DASH_CYCLE, SAMPLE_COLOR, SAMPLE_LINE, RUN, SERIES, ResolvedPlot
from ..roles import overlay_in_legend
from ..spec import PlotKind

LAYER = "scistackplot"


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


def series_groups(
    subset: pd.DataFrame, resolved: ResolvedPlot
) -> list[tuple[Any, pd.DataFrame]]:
    """``(series id, rows)`` per polyline / band, or one group for the lot.

    Which rows form ONE line is a single decision for every renderer; it was
    a byte-identical ``_series_groups`` in ``mpl`` and ``plotly_`` until
    2026-09-23, free to drift so the two backends drew different lines.
    """
    series_column = resolved.encoding.series
    if series_column and series_column in subset.columns:
        return list(subset.groupby(series_column, sort=False))
    return [(None, subset)]


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


def dash_levels(resolved: ResolvedPlot) -> list[str]:
    """The dash ids a legend would list, in drawn order — the figure-wide
    ``dash_styles`` (``reduce._dash_styles``), restricted to what the panels
    actually draw, for the same reason :func:`legend_levels` reads the
    panels."""
    column = resolved.encoding.dash
    if not column or not resolved.dash_styles:
        return []
    if len(resolved.dash_styles) > len(DASH_CYCLE):
        # Past the cycle the styles repeat, so a list of them names nothing a
        # reader could match to a line; `reduce._dash_styles` has already
        # warned and pointed at Separate panels. The lines still dash.
        return []
    present: set[str] = set()
    for panel in resolved.panels:
        if column in panel.frame.columns:
            present.update(str(v) for v in panel.frame[column].dropna().unique())
    return [sid for sid in resolved.dash_styles if sid in present]


def dash_style(resolved: ResolvedPlot, rows: pd.DataFrame) -> str:
    """The dash name for a run of rows (one series), ``"solid"`` when the
    figure has no dash layer."""
    column = resolved.encoding.dash
    if not column or column not in rows.columns or rows.empty:
        return "solid"
    return resolved.dash_styles.get(str(rows[column].iloc[0]), "solid")


def shows_legend(resolved: ResolvedPlot) -> bool:
    """
    Whether this figure gets a legend at all.

    ONE rule, shared by both renderers and mirrored by the generated seaborn
    code (``codegen``): the legend exists only to tell series apart — by
    colour, by dash style, or by the overlay's own colour — so a single level
    of each makes it pure noise:
    it restates the one thing every mark on the figure already has in common,
    and it costs the panels width.
    """
    return (
        len(legend_levels(resolved)) > 1
        or len(dash_levels(resolved)) > 1
        or len(sample_legend_levels(resolved)) > 1
    )


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


# ---------------------------------------------------------------------------
# Mark placement, and the "Show sample" overlay — one arithmetic for both
# renderers
# ---------------------------------------------------------------------------

#: The fraction of the gap between two categorical positions that the ONE
#: mark at that position occupies. plotly's ``bargap`` / ``boxgap`` of 0.2
#: is the same statement (``plotly_.render``).
#:
#: One mark, never a dodged row of them: since 2026-09-21 colour is paint
#: (``roles.GroupingLayers``) — the coloured layer is a tick layer like any
#: other, so every colour level already has its own x position and there is
#: nothing to dodge. plotly's ``*mode: "group"`` would still reserve a slot
#: per colour TRACE at every position; ``MARK_OFFSET_GROUP`` puts every
#: trace in the same slot so the bars draw at ``MARK_SPAN`` wide.
MARK_SPAN = 0.8
MARK_OFFSET_GROUP = "marks"


def mark_width() -> float:
    """The width of the mark at a categorical position — :data:`MARK_SPAN`.
    A function rather than the constant so the two renderers, the overlay
    and the tests read one name for "how wide is a bar"."""
    return MARK_SPAN

#: How the overlay's points draw against the marks they sit on: the mark's
#: colour with a dark edge so they read on top of a bar of the same hue, a
#: little more transparent, and smaller — the marks are the figure, the
#: points are the evidence behind it.
SAMPLE_ALPHA = 0.7
SAMPLE_MARKER_FRACTION = 0.45
SAMPLE_EDGE_COLOR = "#333333"
SAMPLE_LINE_WIDTH = 1.0


#: The overlay's OWN palette (``PlotSpec.sample_color``): one colour per
#: level of the shown key that colours the points, separate from the marks'
#: :data:`DEFAULT_PALETTE` so subjects on top of intervention-group bars
#: read as a second family. Twenty entries — a subject key has more levels
#: than a grouping layer does; past twenty the colours repeat and
#: ``reduce`` warns. (matplotlib's tab20, as hex.)
SAMPLE_PALETTE = (
    "#1f77b4",
    "#aec7e8",
    "#ff7f0e",
    "#ffbb78",
    "#2ca02c",
    "#98df8a",
    "#d62728",
    "#ff9896",
    "#9467bd",
    "#c5b0d5",
    "#8c564b",
    "#c49c94",
    "#e377c2",
    "#f7b6d2",
    "#7f7f7f",
    "#c7c7c7",
    "#bcbd22",
    "#dbdb8d",
    "#17becf",
    "#9edae5",
)


def sample_palette_for(resolved: ResolvedPlot, level: Any, fallback: int) -> str:
    """The colour an overlay level carries — the same one in every panel.

    Indexed by the level's position in ``ResolvedPlot.sample_color_order``,
    never by a panel's own enumeration, for the reason :func:`palette_for`
    gives: a subject absent from one panel must not shift every other
    subject's colour there.
    """
    for position, candidate in enumerate(resolved.sample_color_order):
        if str(candidate) == str(level):
            return palette_color(position, SAMPLE_PALETTE)
    return palette_color(fallback, SAMPLE_PALETTE)


def sample_legend_levels(resolved: ResolvedPlot) -> list[Any]:
    """The overlay's colour levels a legend would list, in drawn order —
    empty when the overlay takes its mark's colour. Read off the panels'
    sample frames, like :func:`legend_levels` off their mark frames, so a
    level this figure never draws is not listed."""
    if not overlay_in_legend(resolved.spec, resolved.sample_color):
        return []
    present: dict[str, Any] = {}
    for panel in resolved.panels:
        sample = getattr(panel, "sample", None)
        if sample is None or SAMPLE_COLOR not in sample.columns:
            continue
        for value in sample[SAMPLE_COLOR].dropna().unique():
            present.setdefault(str(value), value)
    ordered = [
        present[str(level)] for level in resolved.sample_color_order if str(level) in present
    ]
    seen = {str(level) for level in ordered}
    ordered.extend(value for key, value in present.items() if key not in seen)
    return ordered


def series_runs(subset: pd.DataFrame, resolved: ResolvedPlot) -> list[tuple[Any, pd.DataFrame]]:
    """``(series id, rows)`` per spaghetti RUN — one polyline each.

    A run is one series (``__series``: the subject's line) inside one bracket
    (``__run``, the tick layers above the innermost — ``resolved.RUN``,
    written by ``reduce._panel_frame`` on a nested axis): a spaghetti line
    spans the innermost tick only, so subject 01 under ``[speed | session]``
    draws ``pre → post`` once per speed rather than one line across all
    four positions. The id returned is the series alone — the OFFSET
    (``ResolvedPlot.series_offsets``) and the legend are keyed by it. The
    one seam both renderers read; the export passes ``units=_run``.
    """
    series_column = resolved.encoding.series
    if not series_column or series_column not in subset.columns:
        return [(None, subset)]
    if RUN in subset.columns:
        return [
            (series_id, rows)
            for (series_id, _run), rows in subset.groupby([series_column, RUN], sort=False)
        ]
    return list(subset.groupby(series_column, sort=False))


def sample_series(subset: pd.DataFrame, resolved: ResolvedPlot) -> list[tuple[Any, pd.DataFrame]]:
    """``(identity, rows)`` per overlay RUN — one polyline when joined — or a
    single unnamed group when the frame carries no identity column.

    A run is one identity (``__series``, the shown keys composed) inside one
    bracket (``__run``: the tick layers above the innermost and, on a
    spaghetti, the line the point sits on — ``resolved.RUN``): a
    joined line spans the innermost tick only and never crosses a bracket,
    so subject 01 yields one run per session when session is the bracket.
    The identity returned is ``__series`` alone, because that is what the
    OFFSET is keyed by (``ResolvedPlot.sample_offsets``): the subject keeps
    its slot in every bracket. The one seam both renderers read.
    """
    if SERIES not in subset.columns:
        return [(None, subset)]
    if RUN in subset.columns:
        return [
            (identity, rows)
            for (identity, _run), rows in subset.groupby([SERIES, RUN], sort=False)
        ]
    return list(subset.groupby(SERIES, sort=False))


def sample_groups(
    sample: pd.DataFrame, resolved: ResolvedPlot
) -> list[tuple[Any, pd.DataFrame]]:
    """How a panel's overlay rows split into runs that share one colour and
    are joined into one line: ``(colour level, rows)``.

    ONE rule, both renderers and the generated code: with the overlay
    coloured by its own key (``ResolvedPlot.sample_color``) the rows are NOT
    split by the marks' colour first — an identity's line runs across the
    colour levels, from the ``pre`` tick to the ``post`` tick, and its own
    colour is what makes that unambiguous. Without it a line crossing two
    mark colours would have no colour to be, so the rows split per mark
    level as they always did and the points take the mark's colour. Either
    way the colour level returned is the one to PAINT with; the position is
    always the row's own mark's (:func:`sample_positions`).
    """
    if resolved.sample_color and SAMPLE_COLOR in sample.columns:
        order = [str(v) for v in resolved.sample_color_order]
        groups = list(sample.groupby(SAMPLE_COLOR, sort=False))
        return sorted(
            groups,
            key=lambda item: order.index(str(item[0])) if str(item[0]) in order else len(order),
        )
    return color_groups(sample, resolved)


def sample_paint(resolved: ResolvedPlot, level: Any, fallback: int) -> str:
    """The colour a :func:`sample_groups` run is painted: the overlay's own
    palette when it has one, else its mark's."""
    if resolved.sample_color:
        return sample_palette_for(resolved, level, fallback)
    return palette_for(resolved, level, fallback)


def sample_positions(
    rows: pd.DataFrame,
    resolved: ResolvedPlot,
    identity: Any,
) -> np.ndarray:
    """x positions of overlay rows: the mark's position, plus the identity's
    own offset (``ResolvedPlot.sample_offsets``, ``spaghetti.overlay_offsets``).

    The mark's position is its tick — every colour level has its own tick
    now that colour is paint, so there is no dodge slot to add — except on
    a **spaghetti**, where the mark is a point on a LINE shifted by
    ``series_offsets``: each row is placed at ITS OWN line's shift
    (``SAMPLE_LINE``), so a subject's trials sit on that subject's line, and
    the identity offset was scaled to the inter-line spacing by
    ``reduce._overlay_offsets``. Per row, not per run, so a joined line lands
    each of its points on the right mark whatever run it belongs to.
    """
    positions, _ = x_positions(rows[resolved.encoding.x], resolved)
    if resolved.kind is PlotKind.SPAGHETTI and SAMPLE_LINE in rows.columns:
        offsets = resolved.series_offsets or {}
        positions = positions + np.array(
            [offsets.get(str(line), 0.0) for line in rows[SAMPLE_LINE]], dtype=float
        )
    return positions + resolved.sample_offsets.get(str(identity), 0.0)


def sample_dropped_reason(panel, resolved: ResolvedPlot) -> str | None:
    """Why a panel's "Show sample" overlay draws NOTHING, or ``None`` when it
    draws. One owner for both renderers' guard, and the one place that says
    so out loud: an overlay the plan built (``reduce`` logged its levels and
    timed ``sample_panels``) and a renderer then discarded is a silent data
    loss the log must show. An empty overlay is not one (a panel whose rows
    all filtered away); a NON-categorical axis is — ``x_positions`` has no
    slots to place points in — and since ``reduce._unlabelled_x_order`` gave
    the tick-less figure its one level, no categorical kind should reach it.
    """
    sample = getattr(panel, "sample", None)
    if sample is None or sample.empty:
        return None
    if not is_categorical_x(resolved):
        reason = (
            f"x axis is not categorical (x_order={resolved.x_order!r}, "
            f"x={resolved.encoding.x!r}, colour={resolved.encoding.color!r})"
        )
        Log.warn(
            "sample overlay panel %s: %d point(s) NOT drawn — %s",
            panel.title or "unfaceted",
            len(sample),
            reason,
            layer=LAYER,
        )
        return reason
    return None


def sample_hover(rows: pd.DataFrame, resolved: ResolvedPlot) -> list[str]:
    """One label per overlay row naming its shown keys — ``subject=01 · trial=2``."""
    shown = [name for name in resolved.sample_shown if name in rows.columns]
    if not shown:
        return [""] * len(rows)
    return [
        " · ".join(f"{name}={record[name]}" for name in shown)
        for record in rows[shown].astype(str).to_dict(orient="records")
    ]
