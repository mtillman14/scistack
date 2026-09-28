"""
Renderer protocol and the layout maths both renderers share.

A renderer translates a :class:`~scistackplot.resolved.ResolvedPlot` into a
backend figure. It performs **no** data reduction — every aggregation, ordering
and error band was decided in ``reduce.resolve``. Keeping renderers dumb is
what stops the interactive view and the exported figure from disagreeing.
"""

from __future__ import annotations

import functools
import math
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np
import pandas as pd
from scistacklog import Log

from ..panels import (
    Y_TITLES_FIRST_COLUMN,
    PanelOverride,
    override_for,
    panel_key_text,
    unmatched,
)
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


def ruled_bracket_depths(plan_depth: int, shown: set[int]) -> set[int]:
    """The bracket rows (``x_plan`` depths) that draw their rules.

    A rule sits ABOVE its own label and spans the labels above it — the deeper
    bracket rows and, nearest the axis, the tick row (depth ``plan_depth``).
    So a row is ruled only when its own labels show AND some row nearer the
    axis shows labels for the rule to bracket. With the tick labels hidden
    (``hide_legend_ticks``: the legend names them), the innermost rules
    bracketed nothing (spec/images/bars_wrong_horz_lines.png).

    ``shown`` holds every depth, tick row included, with at least one label
    drawn. ONE owner for both backends: the export draws by it and the preview
    drops the same rules.
    """
    return {
        depth
        for depth in range(plan_depth)
        if depth in shown
        and any(nearer in shown for nearer in range(depth + 1, plan_depth + 1))
    }


#: Clear space below the tick labels before the first bracket row, and between
#: bracket rows, in points. Points, not a fraction of the axes: a fraction of
#: a short panel is smaller than the tick labels, which is how brackets came to
#: sit on top of them (spec/images/graph2.png).
X_GROUP_GAP_PT = 4.0

#: Between a bracket's rule and its label, in points.
X_GROUP_RULE_GAP_PT = 2.0


@dataclass(frozen=True)
class BracketGeometry:
    """Where the bracket rows hang below an axes, in points from its bottom edge.

    ONE owner for both backends. The export MEASURES ``tick_depth_pt`` (how far
    the drawn tick labels reach below the axes) and ``row_height_pt`` (the
    tallest fitted bracket label), so a label rotated to 45 or 90 degrees —
    whose depth is its length, not its font size — pushes every row beneath it
    down. The preview draws from the same numbers (1 pt = 1 px) instead of
    placing brackets at a fixed fraction of the figure, which left them on top
    of rotated tick labels.
    """

    tick_depth_pt: float
    row_height_pt: float

    @property
    def step_pt(self) -> float:
        """One bracket row, label and the gaps around its rule."""
        return self.row_height_pt + X_GROUP_RULE_GAP_PT + X_GROUP_GAP_PT

    def rule_pt(self, depth: int, plan_depth: int) -> float:
        """Below the axes, the rule of the row at ``depth`` (depth 0 is the
        outermost layer, furthest below; deeper layers sit nearer the axis)."""
        return self.tick_depth_pt + X_GROUP_GAP_PT + (plan_depth - depth - 1) * self.step_pt

    def label_pt(self, depth: int, plan_depth: int) -> float:
        """Below the axes, the TOP of the labels of the row at ``depth``."""
        return self.rule_pt(depth, plan_depth) + X_GROUP_RULE_GAP_PT

    def title_pad_pt(self, plan_depth: int) -> float:
        """From the bottom of the tick labels to the axis title: clear of every row."""
        return X_GROUP_GAP_PT + plan_depth * self.step_pt

    def bottom_pt(self, plan_depth: int) -> float:
        """Below the axes, the bottom of the outermost row's labels."""
        return self.tick_depth_pt + plan_depth * self.step_pt

    def describe(self) -> str:
        return (
            f"tick labels reach {self.tick_depth_pt:.1f}pt below the axes, "
            f"bracket rows {self.step_pt:.1f}pt apart"
        )


@dataclass(frozen=True)
class GridReach:
    """How far the panels' text reaches OUTSIDE their axes, in points.

    Measured by the export (``mpl._grid_reach``: axes box vs its tight box, so
    tick labels at any rotation, bracket rows and axis titles all count), and
    read by the preview to size its margins and the gaps between cells (1 pt
    = 1 px). A gap sized as a fraction of the figure was the wrong unit: text
    has a size in points whatever the figure's width, so the right panel's y
    title ran into the left panel on a narrow figure, and 90 degree tick
    labels on an inner row reached the row below.

    ``outer`` is text in the figure margin (column 0's left side, the bottom
    row's underside); ``inner`` is text drawn into a gap between two cells.
    """

    left_outer_pt: float
    left_inner_pt: float
    below_outer_pt: float
    below_inner_pt: float

    def describe(self) -> str:
        return (
            f"left {self.left_outer_pt:.1f}pt (margin) / {self.left_inner_pt:.1f}pt "
            f"(between columns), below {self.below_outer_pt:.1f}pt (margin) / "
            f"{self.below_inner_pt:.1f}pt (between rows)"
        )


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
    return is_leftmost(resolved, row, col)


def is_leftmost(resolved: ResolvedPlot, row: int, col: int) -> bool:
    """Whether nothing sits directly to the left of this cell: the first
    column, including a panel in a partial wrapped row whose left cell is
    empty. The one "first column" rule, for the tick numbers
    (:func:`shows_y_labels`) and ``StyleOptions.y_titles`` (:func:`shows_panel_y_title`)."""
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
    shared label being repeated) does not apply to it — unless the user asks
    for it (``StyleOptions.y_titles``) or hides/replaces this panel's title
    (``PanelOverride``; :func:`shows_panel_y_title`). A hidden title is ``""``,
    which measures to nothing, so the column gap it took is given back
    (``mpl._grid_reach`` -> ``GridReach`` -> plotly ``_frame``).
    """
    if panel is not None and panel.key:
        override = override_for(resolved.spec, panel.key)
        if not shows_panel_y_title(resolved, panel, override):
            return ""
        if override is not None and override.y_label is not None:
            # Exactly as typed: no aliases.
            return override.y_label
        # The TEXT of the facet values (aliased); `panel.title` stays the raw
        # identity the layout rules match.
        return resolved.text.panel_title(panel.key)
    return resolved.labels.y if leftmost else ""


def shows_panel_y_title(
    resolved: ResolvedPlot, panel, override: PanelOverride | None = None
) -> bool:
    """Whether a FACETED panel draws its y title (plan D5/D6).

    The panel's own ``y_label_hidden`` wins when set (``False`` forces the
    title on against the grid toggle); otherwise ``StyleOptions.y_titles``
    decides: ``every_panel``, or ``first_column`` = :func:`is_leftmost`.
    """
    if override is not None and override.y_label_hidden is not None:
        return not override.y_label_hidden
    if resolved.spec.style.y_titles == Y_TITLES_FIRST_COLUMN:
        return is_leftmost(resolved, panel.grid_row, panel.grid_col)
    return True


def panel_override_meta(resolved: ResolvedPlot) -> dict:
    """What the GUI's Panels section shows, decided here (``layout.meta``).

    ``match`` is each panel's facet values as TEXT (``panels.panel_key_text``),
    ready to be written back as ``PanelOverride.match`` — the GUI never turns a
    value into text itself. ``y_title`` is the title drawn ("" when hidden),
    ``y_limits`` the range drawn, ``override`` the matched entry or None.
    ``unmatched`` lists overrides for panels not in this figure (inert).
    """
    at_left = {
        id(panel): shows_y_labels(resolved, panel.grid_row, panel.grid_col)
        for panel in resolved.panels
    }
    panels = []
    for panel in resolved.panels:
        if not panel.key:
            continue
        override = override_for(resolved.spec, panel.key)
        panels.append(
            {
                "match": {name: panel_key_text(value) for name, value in panel.key.items()},
                "display_title": resolved.text.panel_title(panel.key),
                "y_title": panel_y_title(resolved, panel, leftmost=at_left[id(panel)]),
                "grid_row": panel.grid_row,
                "grid_col": panel.grid_col,
                "y_limits": list(panel.y_limits) if panel.y_limits else None,
                "override": override.to_dict() if override is not None else None,
            }
        )
    return {
        "panels": panels,
        "unmatched": [
            override.to_dict()
            for override in unmatched(resolved.spec, [panel.key for panel in resolved.panels])
        ],
        "y_titles": resolved.spec.style.y_titles,
        "shares_y": shares_y_axis(resolved),
    }


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


@functools.lru_cache(maxsize=64)
def mark_palette(name: str | None, n: int) -> tuple[str, ...]:
    """The marks' colours for ``StyleOptions.palette`` over ``n`` colour
    levels — ONE owner for the preview (:func:`palette_for`) and the export
    (``codegen._mark_palette`` hands seaborn the same name, and seaborn
    resolves it the same way: ``sns.color_palette(name, n)``).

    ``None`` is :data:`DEFAULT_PALETTE`. A name is resolved by seaborn, so a
    colormap (``"viridis"``) is sampled over exactly the ``n`` levels the
    export's ``hue_order`` has. Until 2026-09-26 the preview ignored the
    name and always drew the default, while the export honoured it.
    A name seaborn refuses, or no seaborn at all, falls back to the default
    and says so (once per name, via the cache).
    """
    if not name:
        return DEFAULT_PALETTE
    try:
        import seaborn as sns
    except ImportError:
        Log.warn(
            "palette %r needs seaborn to resolve; drawing the default palette "
            "(the export, which uses seaborn, will differ)",
            name,
            layer=LAYER,
        )
        return DEFAULT_PALETTE
    try:
        colors = tuple(sns.color_palette(name, max(n, 1)).as_hex())
    except (ValueError, KeyError) as exc:
        Log.warn(
            "palette %r is not a seaborn/matplotlib palette (%s); drawing the default palette",
            name,
            exc,
            layer=LAYER,
        )
        return DEFAULT_PALETTE
    Log.debug("palette %r over %d level(s): %s", name, n, list(colors), layer=LAYER)
    return colors


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

    A PINNED colour (``resolved.colors``: the plot's ``colors`` over the
    project's ``[colors]``; with no colour layer, the single mark colour) wins.
    Every other level keeps its palette colour at its declared position, so
    pinning one level never moves another's.
    """
    pinned = resolved.colors.mark(level)
    if pinned is not None:
        return pinned
    order = resolved.color_order or []
    # Over the declared levels: a colormap is sampled per level, as the
    # export's `hue_order` samples it (mark_palette).
    palette = mark_palette(resolved.spec.style.palette, len(order))
    for position, candidate in enumerate(order):
        if str(candidate) == str(level):
            return palette_color(position, palette)
    return palette_color(fallback, palette)


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

#: The fill opacity of a box and a violin body — lighter than a bar, whose
#: fill is ``StyleOptions.alpha``, because the median / quartile lines
#: inside them must read through.
BOX_FILL_ALPHA = 0.6
VIOLIN_FILL_ALPHA = 0.55


def fill_alpha(kind: PlotKind, style) -> float | None:
    """The opacity of a mark's FILL — ONE owner for matplotlib, plotly and
    the export (``codegen``), or ``None`` for a kind with no fill.

    Until 2026-09-26 each side had its own: matplotlib drew bars at
    ``style.alpha`` and boxes / violins at literals, plotly drew opaque bars
    and its own half-transparent box fill, and the export drew every fill
    opaque.
    """
    if kind is PlotKind.BAR:
        return style.alpha
    if kind is PlotKind.BOX:
        return BOX_FILL_ALPHA
    if kind is PlotKind.VIOLIN:
        return VIOLIN_FILL_ALPHA
    return None


#: How the overlay's points draw against the marks they sit on: the mark's
#: colour with a dark edge so they read on top of a bar of the same hue, a
#: little more transparent, and smaller — the marks are the figure, the
#: points are the evidence behind it. Their SIZE and line width are
#: ``scistackplot.weights``' (``StyleOptions.sample_weight``).
SAMPLE_ALPHA = 0.7
SAMPLE_EDGE_COLOR = "#333333"


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
    A pinned colour (``resolved.colors.sample``) wins, as in :func:`palette_for`.
    """
    pinned = resolved.colors.sample(level)
    if pinned is not None:
        return pinned
    for position, candidate in enumerate(resolved.sample_color_order):
        if str(candidate) == str(level):
            return palette_color(position, SAMPLE_PALETTE)
    return palette_color(fallback, SAMPLE_PALETTE)


#: Levels listed per factor in the GUI's Colours section. Past this the list
#: says it is cut: 500 subjects are pinned in the project config, not
#: clicked one by one.
MAX_COLORABLE_LEVELS = 200


def colorable(resolved: ResolvedPlot) -> list[dict]:
    """What the GUI's Colours section offers for one figure — Python's list,
    so the panel never works out what is painted or in which colour.

    One entry per painted factor: the marks' colour layer (role
    ``"colour"``), the overlay's own key (``"sample colour"``), or — with no
    colour layer — one ``"marks"`` entry for the single mark colour. Each
    level is ``{raw, text, hex, origin}``: the colour DRAWN (through
    :func:`palette_for` / :func:`sample_palette_for`, the one owner of it)
    and where it came from, ``plot`` / ``project`` / ``palette``. ``key``
    is the thing a pin is written under (``MarkColors.entry_key``: a
    grouping column's ``Var.Column``); None for the single mark colour.

    Also WARNs, once per call, when a pin paints two levels of one factor
    the same (``colors.warn_duplicates``).
    """
    from ..colors import PALETTE, warn_duplicates

    if resolved.spec.kind is PlotKind.HEATMAP:
        return []  # a colormap, not marks: out of scope (plan decision 4)
    pins = resolved.colors
    text = resolved.text
    entries: list[dict] = []
    painted: list[tuple[str, Any, str, str]] = []

    def entry(factor: str, role: str, levels: list[Any], paint, pinned) -> dict:
        rows = []
        for index, level in enumerate(levels[:MAX_COLORABLE_LEVELS]):
            hex_ = paint(resolved, level, index)
            found = pinned(factor, level)
            origin = found[1] if found else PALETTE
            painted.append((factor, level, hex_, origin))
            rows.append(
                {
                    "raw": "" if level is None else str(level),
                    "text": text.level(factor, level),
                    "hex": hex_,
                    "origin": origin,
                }
            )
        return {
            "factor": factor,
            "key": pins.entry_key(factor),
            "role": role,
            "name": text.name(factor, factor),
            "levels": rows,
            "truncated": len(levels) > MAX_COLORABLE_LEVELS,
        }

    if pins.color is not None:
        entries.append(
            entry(
                pins.color,
                "colour",
                list(resolved.color_order or []),
                palette_for,
                pins.pinned,
            )
        )
    else:
        single = pins.single
        entries.append(
            {
                "factor": None,
                "key": None,
                "role": "marks",
                "name": "Marks",
                "levels": [
                    {
                        "raw": "",
                        "text": "All marks",
                        "hex": palette_for(resolved, None, 0),
                        "origin": single[1] if single else PALETTE,
                    }
                ],
                "truncated": False,
            }
        )
    if pins.sample_color is not None and resolved.sample_color_order:
        entries.append(
            entry(
                pins.sample_color,
                "sample colour",
                list(resolved.sample_color_order),
                sample_palette_for,
                pins.pinned,
            )
        )
    warn_duplicates(painted)
    return entries


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
    """The overlay's OWN colour levels, in ``sample_color_order``:
    ``(level, rows)``. Only meaningful with ``ResolvedPlot.sample_color``;
    :func:`sample_runs` is the seam renderers read."""
    order = [str(v) for v in resolved.sample_color_order]
    groups = list(sample.groupby(SAMPLE_COLOR, sort=False))
    return sorted(
        groups,
        key=lambda item: order.index(str(item[0])) if str(item[0]) in order else len(order),
    )


#: The line joining overlay points that sit on marks of DIFFERENT colours
#: (the coloured layer is the one the line spans, e.g. ``pre`` → ``post``
#: with ``session`` coloured) when the overlay takes its marks' colour: the
#: points keep their mark's colour, the line between them has none to be,
#: so it is drawn in this neutral grey.
SAMPLE_CROSS_LINE_COLOR = "#808080"

#: matplotlib gid of the points drawn over a neutral crossing line — they
#: belong to that line's run (read back by ``tests/plot_geometry.py``).
SAMPLE_LINE_POINTS_GID = "sample-line-points"


@dataclass(frozen=True)
class SampleRun:
    """One overlay run — a polyline when joined — as every renderer draws it.

    ``level`` is the run's paint level (the overlay's own level, or the mark
    level every point shares; ``None`` when the run crosses mark colours),
    ``line_color`` the colour of the line joining the points, and
    ``point_colors`` one colour per row of ``rows``, in ``rows`` order.
    """

    identity: Any
    rows: pd.DataFrame
    level: Any
    line_color: str
    point_colors: tuple[str, ...]

    @property
    def uniform(self) -> bool:
        """Every point, and the line, in one colour."""
        return all(color == self.line_color for color in self.point_colors)


def sample_runs(sample: pd.DataFrame, resolved: ResolvedPlot) -> list[SampleRun]:
    """How a panel's overlay rows split into runs and what colour each point
    and line is painted — ONE rule, both renderers (the generated code
    restates it in ``codegen._sample_draw_lines``).

    A run is :func:`sample_series`: one identity inside one bracket. It is
    never split by the marks' colour when joined, so a line spans the innermost tick
    whichever layer is coloured:

    * with the overlay's own colour (``ResolvedPlot.sample_color``) the runs
      split by that key's level and point and line take its colour;
    * points only (``sample_join`` off): split per mark colour, each run in
      its mark's colour and level (so plotly's legend group still hides a
      level's points);
    * otherwise each POINT takes its own mark's colour, and the line takes
      that colour too when every point shares it (the coloured layer is a
      bracket, or depth-less) — else, when the coloured layer is the one the
      line spans, it is drawn in :data:`SAMPLE_CROSS_LINE_COLOR`. Splitting
      per mark colour here (the rule until 2026-09-26) left every run one
      point long, so Lines / Auto (lines) drew no line at all.

    The position is always the row's own mark's (:func:`sample_positions`).
    """
    runs: list[SampleRun] = []
    if resolved.sample_color and SAMPLE_COLOR in sample.columns:
        for index, (level, subset) in enumerate(sample_groups(sample, resolved)):
            color = sample_palette_for(resolved, level, index)
            for identity, rows in sample_series(subset, resolved):
                runs.append(SampleRun(identity, rows, level, color, (color,) * len(rows)))
        return runs
    color_column = resolved.encoding.color
    if not color_column or color_column not in sample.columns:
        color = palette_for(resolved, None, 0)
        return [
            SampleRun(identity, rows, None, color, (color,) * len(rows))
            for identity, rows in sample_series(sample, resolved)
        ]
    if not resolved.sample_join:
        # Points only: no line to cross anything, so the runs split per mark
        # colour and each keeps its level — plotly's `legendgroup` then ties
        # the points to their mark's legend entry (hide 01, hide its points).
        for index, (level, subset) in enumerate(color_groups(sample, resolved)):
            color = palette_for(resolved, level, index)
            for identity, rows in sample_series(subset, resolved):
                runs.append(SampleRun(identity, rows, level, color, (color,) * len(rows)))
        return runs
    # A level's colour exactly as the per-level split painted it (the
    # fallback index is its position among the panel's levels).
    paint = {
        str(level): palette_for(resolved, level, index)
        for index, (level, _) in enumerate(color_groups(sample, resolved))
    }
    crossing = 0
    for identity, rows in sample_series(sample, resolved):
        levels = rows[color_column].tolist()
        colors = tuple(paint.get(str(level), palette_for(resolved, level, 0)) for level in levels)
        if len({str(level) for level in levels}) == 1:
            runs.append(SampleRun(identity, rows, levels[0], colors[0], colors))
        else:
            crossing += 1
            runs.append(SampleRun(identity, rows, None, SAMPLE_CROSS_LINE_COLOR, colors))
    if crossing:
        Log.debug(
            "sample overlay: %d of %d run(s) cross the marks' colour %r — "
            "points in their mark's colour, line in %s",
            crossing,
            len(runs),
            color_column,
            SAMPLE_CROSS_LINE_COLOR,
            layer=LAYER,
        )
    return runs


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
