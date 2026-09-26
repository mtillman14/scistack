"""
Plotly renderer — the interactive path.

Emits a plain ``{"data": [...], "layout": {...}}`` dict rather than a
``plotly.graph_objects.Figure``. That is deliberate: the consumer is plotly.js
running inside a VS Code webview, which wants JSON. Building it directly means
the interactive path needs no plotly Python package at all, keeps the payload
inspectable in tests, and avoids shipping a second figure object across the
JSON-RPC boundary only to serialize it anyway.

``plotly`` remains an optional extra for users who want a Figure in a notebook
(:func:`to_figure`).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scistacklog import Log

from ..figsize import describe_size
from ..paper import (
    PAPER,
    describe_paper,
    plotly_axis_style,
    plotly_error_style,
    plotly_layout_style,
)
from ..resolved import ResolvedPlot
from ..roles import overlay_in_legend
from ..spec import PlotKind
from ..textsize import resolve_sizes
from ..weights import mark_weights_meta, sample_weight, spaghetti_weight
from ..xaxis import LEAF_SEPARATOR
from .base import (
    MARK_OFFSET_GROUP,
    SAMPLE_ALPHA,
    SAMPLE_EDGE_COLOR,
    series_groups,
    color_groups,
    dash_levels,
    dash_style,
    grid_shape,
    is_categorical_x,
    legend_levels,
    palette_for,
    panel_position,
    axis_range,
    fill_alpha,
    panel_y_limits,
    panel_y_title,
    shares_y_axis,
    shows_legend,
    ruled_bracket_depths,
    shows_x_labels,
    shows_y_labels,
    sample_hover,
    sample_legend_levels,
    sample_dropped_reason,
    sample_positions,
    sample_runs,
    series_runs,
    x_positions,
)

LAYER = "scistackplot"

#: Right margin with no legend in it — just room for the last x tick label.
BARE_RIGHT_MARGIN = 20


def render(
    resolved: ResolvedPlot,
    *,
    decisions: dict | None = None,
    fixed_size_px: tuple[float, float] | None = None,
) -> dict:
    """Build a plotly.js figure dict.

    ``decisions`` (``mpl.layout_decisions``) are applied as they are — see
    :func:`_apply_decisions`. ``fixed_size_px`` draws at that size instead of
    filling the pane.
    """
    with Log.timer("render_plotly", layer=LAYER, extra=str(resolved.kind)):
        n_rows, n_cols = grid_shape(resolved)
        style = resolved.spec.style
        sizes = resolve_sizes(style)
        traces: list[dict] = []
        legend_on = shows_legend(resolved)
        # The same phrase render_matplotlib logs, so preview and export compare.
        Log.debug("preview %s", describe_paper(), layer=LAYER)
        if not legend_on and resolved.encoding.color:
            Log.debug(
                "legend omitted: %d colour level(s) drawn for %r",
                len(legend_levels(resolved)),
                resolved.labels.color,
                layer=LAYER,
            )
        layout: dict[str, Any] = {
            "showlegend": legend_on,
            "legend": {
                "title": {
                    "text": _legend_title(resolved),
                    "font": {"size": sizes.legend_title_for(sizes.legend)},
                },
                "font": {"size": sizes.legend},
                # Stated, not defaulted: outside the plotting area on the right
                # and vertically centred, which is exactly where the matplotlib
                # export puts it. The margin below reserves the room it sits in
                # — at plotly's default right margin the legend was drawn into
                # 20px of space and clipped.
                "x": 1.02,
                "xanchor": "left",
                "y": 0.5,
                "yanchor": "middle",
            },
            "margin": {
                "l": 60,
                "r": _right_margin(resolved) if legend_on else BARE_RIGHT_MARGIN,
                "t": 40,
                # Each nested group layer needs a label row below the ticks, or
                # the brackets are drawn off the bottom of the figure.
                "b": 50 + 28 * (resolved.x_plan.depth if resolved.x_plan else 0),
            },
            "hovermode": "closest",
            "annotations": [],
            # The same number as the export's font.size, read as px here. The
            # GUI adds the colour; it must not add a size, or the setting would
            # only be visible after a save.
            "font": {"size": sizes.base, "color": PAPER.text},
            # The export's paper (paper.py): white, a black frame with outward
            # ticks, no grid. Left to plotly.js's defaults the preview drew a
            # grey grid and a zero line and no frame, so it was never the
            # figure a save produces.
            **plotly_layout_style(),
            # The grid shape travels with the figure so the panel can size it:
            # 4 rows of subplots need more height than 1, and only the renderer
            # knows how the panels were laid out. The GUI also reads `rows`/
            # `cols` back as the EFFECTIVE grid — that is how "I set 2 columns"
            # shows the computed row count — and `layout_notes` is how a spilled
            # panel gets told to the user instead of just being logged.
            "meta": {
                "rows": n_rows,
                "cols": n_cols,
                "panels": len(resolved.panels),
                "layout_notes": list(resolved.layout_notes),
                # The factors a y-limit scope may name, AS RESOLVED (promoted
                # or defaulted ones included) — the GUI builds its checkboxes
                # from this, never from `spec.roles`.
                "panel_factors": list(resolved.panel_factors),
                "y_scope": list(resolved.y_scope),
                # The size the SAVED figure will have. The preview fills its
                # pane regardless, so this is how the panel states what the
                # export produces — the same numbers `render_mpl` logs.
                "figure_size": describe_size(style.width, style.height),
                # Every text size as the export resolves it, fixed ones listed —
                # the GUI shows a derived size as the placeholder of its box.
                "text_sizes": sizes.to_dict(),
                # Which weight knob applies here and what it resolves to
                # (weights.mark_weights_meta): the GUI shows a knob only when
                # this says so, never by testing the kind itself.
                "mark_weights": mark_weights_meta(resolved),
                # What the Labels section offers (aliases.labelable): the
                # measure and every factor this figure draws as text.
                "labelable": list(resolved.labelable),
            },
        }
        if resolved.labels.title:
            layout["title"] = {"text": resolved.labels.title, "font": {"size": sizes.title}}

        if resolved.kind is PlotKind.BAR:
            # Stated, never inferred — the same rule as `orientation: "v"` on
            # the trace. plotly's defaults happen to agree today, but a figure
            # whose bar layout depends on which defaults the bundled plotly.js
            # ships is a figure that can change without anyone editing it.
            # 0.2 is also exactly what the matplotlib path draws
            # (`mpl._draw_bars`: width = MARK_SPAN = 0.8, one bar per tick).
            #
            # "group" mode would still reserve one slot per colour TRACE at
            # every x — a narrow, off-centre bar wherever only one level has a
            # bar, which since colour became paint is everywhere. Every mark
            # trace therefore shares `offsetgroup` (`_panel_traces`), so the
            # single bar at each position draws full width.
            layout["barmode"] = "group"
            layout["bargap"] = 0.2
            layout["bargroupgap"] = 0.0
        elif resolved.kind is PlotKind.BOX:
            # Same rule as the bars (`mpl._draw_distribution`: box width 0.85
            # of the 0.8 mark span). plotly's default boxmode is "overlay",
            # which would be right for disjoint x sets too, but "group" with
            # one shared `offsetgroup` states the geometry instead of relying
            # on the traces never sharing an x.
            layout["boxmode"] = "group"
            layout["boxgap"] = 0.2
            layout["boxgroupgap"] = 0.15
        elif resolved.kind is PlotKind.VIOLIN:
            # As for boxes; a violin is 0.9 of the mark span on the mpl path.
            layout["violinmode"] = "group"
            layout["violingap"] = 0.2
            layout["violingroupgap"] = 0.1

        positions = [
            panel_position(resolved, index) for index in range(len(resolved.panels))
        ]
        seen_legend: set[str] = set()

        for index, panel in enumerate(resolved.panels):
            row, col = positions[index]
            slot = row * n_cols + col + 1
            x_axis = "x" if slot == 1 else f"x{slot}"
            y_axis = "y" if slot == 1 else f"y{slot}"

            traces.extend(
                _panel_traces(
                    panel.frame, resolved, x_axis, y_axis, seen_legend, legend_on
                )
            )
            traces.extend(_sample_traces(panel, resolved, x_axis, y_axis, seen_legend, legend_on))
            _add_axes(
                layout,
                resolved,
                slot,
                row,
                col,
                n_rows,
                n_cols,
                bottom=shows_x_labels(resolved, row, col),
                leftmost=shows_y_labels(resolved, row, col),
                y_limits=panel_y_limits(resolved, panel),
                panel=panel,
            )
            # No panel-title annotation: a facet is named by its y-axis title
            # (base.panel_y_title), which costs the grid no vertical room.
            _add_x_groups(layout, resolved, row, col, n_rows, n_cols, slot)

        if legend_on and len(dash_levels(resolved)) > 1:
            traces.extend(_dash_legend_traces(resolved))

        # The font the decisions were measured in (matplotlib's default),
        # so the preview's glyphs are the ones the export was fitted to.
        layout["font"]["family"] = PREVIEW_FONT_FAMILY
        if decisions is not None:
            _apply_decisions(layout, resolved, decisions)
        if fixed_size_px is not None:
            width, height = (round(float(v)) for v in fixed_size_px)
            layout.update(width=width, height=height, autosize=False)
            layout["meta"]["fixed_size"] = [width, height]
        return {"data": traces, "layout": layout}


#: Vertical room, in paper fraction, for one row of nested group labels.
X_GROUP_ROW = 0.045

#: Gap left at each end of a bracket rule, in category slots, so adjacent
#: brackets read as separate spans rather than one continuous rule.
X_GROUP_INSET = 0.05


def _add_x_groups(layout, resolved, row, col, n_rows, n_cols, slot) -> None:
    """Label and bracket each higher x layer beneath the tick labels.

    Horizontally in the panel's OWN x-axis coordinates — category serial
    numbers, which are exactly the indices of ``plan.order`` (the axis states
    that order as ``categoryarray``, and a positional axis puts its levels at
    the same integers). So a bracket moves with the ticks it names when the
    user zooms or pans: plotly clips the rule to the axis range and hides a
    label whose anchor leaves it, just as it drops the ticks. In paper
    coordinates they stayed put, and a zoom onto one pair of bars still showed
    every group's label under it.

    Vertically in PAPER coordinates from the cell's own domain, so the brackets
    hang below the axis (outside any data range) under the panel they describe.

    Only under panels that show tick labels: repeating "stim | sham" under every
    row of a grid is the same noise ``shows_x_labels`` already suppresses for
    the ticks themselves.
    """
    plan = resolved.x_plan
    if not plan or not plan.groups or not shows_x_labels(resolved, row, col):
        return

    _x0, y0, _cell_width, _cell_height = _cell(
        row, col, n_rows, n_cols, _x_depth(resolved)
    )
    x_ref = "x" if slot == 1 else f"x{slot}"
    Log.debug(
        "x groups: %d bracket(s) on %s in axis coordinates over %d slot(s)",
        len(plan.groups), x_ref, len(plan.order), layer=LAYER,
    )

    for group in plan.groups:
        # A leaf at index i spans [i - 0.5, i + 0.5] on the axis.
        left = group.start - 0.5
        right = group.end + 0.5
        # Deeper layers sit closer to the axis; depth 0 is furthest below.
        rows_below = plan.depth - group.depth
        y = y0 - X_GROUP_ROW * rows_below - 0.03

        layout["annotations"].append(
            {
                # Tagged so `_apply_decisions` can find this bracket label
                # (a valid plotly annotation attribute, used for templates).
                "name": f"{X_GROUP_TAG}:{group.depth}:{group.start}",
                "text": group.label,
                "x": (left + right) / 2.0,
                "y": y,
                "xref": x_ref,
                "yref": "paper",
                "showarrow": False,
                # The one owner's bracket size (textsize), which the matplotlib
                # side draws the same label at. It was 0.8 x here and "small"
                # (0.833 x) there: two owners of one number.
                "font": {"size": resolve_sizes(resolved.spec.style).groups},
                "xanchor": "center",
                "yanchor": "top",
            }
        )
        if not group.label:
            # A bracket with no label draws no rule: a bare line names nothing.
            continue
        layout.setdefault("shapes", []).append(
            {
                # Same tag as its label, so `_apply_decisions` can drop the
                # rule when the export blanks that label (hide_legend_ticks).
                "name": f"{X_GROUP_TAG}:{group.depth}:{group.start}",
                "type": "line",
                "xref": x_ref,
                "yref": "paper",
                "x0": left + X_GROUP_INSET,
                "x1": right - X_GROUP_INSET,
                "y0": y + 0.008,
                "y1": y + 0.008,
                "line": {"color": "#888888", "width": 1},
            }
        )


def to_figure(resolved: ResolvedPlot):
    """Wrap :func:`render` in a ``plotly.graph_objects.Figure`` (optional extra)."""
    try:
        import plotly.graph_objects as go
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "to_figure() needs plotly (pip install scistackplot[interactive]). "
            "render() returns a plain figure dict with no such requirement."
        ) from exc
    return go.Figure(render(resolved))


# ---------------------------------------------------------------------------


def _panel_traces(
    frame: pd.DataFrame,
    resolved: ResolvedPlot,
    x_axis: str,
    y_axis: str,
    seen_legend: set[str],
    legend_on: bool = True,
) -> list[dict]:
    if frame.empty:
        return []

    encoding = resolved.encoding
    kind = resolved.kind
    traces: list[dict] = []

    if kind is PlotKind.HEATMAP:
        matrix = np.asarray(frame[encoding.z].iloc[0], dtype=float)
        return [
            {
                "type": "heatmap",
                "z": matrix.tolist(),
                "xaxis": x_axis.replace("x", "x"),
                "yaxis": y_axis,
                "colorscale": "Viridis",
            }
        ]

    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        color = palette_for(resolved, level, index)
        label = resolved.text.color_level(level) if level is not None else resolved.labels.y
        show_legend = legend_on and level is not None and label not in seen_legend
        if show_legend:
            seen_legend.add(label)

        base = {
            "name": label,
            "legendgroup": label,
            "showlegend": show_legend,
            "xaxis": x_axis,
            "yaxis": y_axis,
        }
        if _positional_x(resolved):
            # A linear axis of level indices (see `_positional_x`): every mark
            # is placed by index so the overlay can shift beside it, and the
            # hover names the level the index stands for.
            positions, _ = x_positions(subset[encoding.x], resolved)
            x_values = [None if np.isnan(v) else float(v) for v in positions]
            hover = _level_hover(subset, resolved, base)
        else:
            x_values = _values(subset[encoding.x])
            hover = {}

        if kind in (PlotKind.SCATTER, PlotKind.STRIP):
            traces.append(
                {
                    **base,
                    "type": "scatter",
                    "mode": "markers",
                    "x": x_values,
                    "y": _values(subset[encoding.y]),
                    "marker": {"color": color, "size": 8, "opacity": resolved.spec.style.alpha},
                    **hover,
                }
            )
        elif kind is PlotKind.SPAGHETTI:
            traces.extend(_spaghetti_traces(subset, resolved, base, color))
        elif kind is PlotKind.LINE:
            traces.extend(_line_traces(subset, resolved, base, color))
        elif kind is PlotKind.BAND:
            traces.extend(_band_traces(subset, resolved, base, color))
        elif kind is PlotKind.BAR:
            error = None
            if encoding.has_error:
                centre = np.asarray(_values(subset[encoding.y]), dtype=float)
                error = {
                    "type": "data",
                    "symmetric": False,
                    "array": (
                        np.asarray(_values(subset[encoding.y_high]), dtype=float) - centre
                    ).tolist(),
                    "arrayminus": (
                        centre - np.asarray(_values(subset[encoding.y_low]), dtype=float)
                    ).tolist(),
                    # The export's error-bar ink and caps (paper.py).
                    **plotly_error_style(),
                }
            traces.append(
                {
                    **base,
                    "type": "bar",
                    # Stated, never inferred: plotly picks an orientation from
                    # which of x/y it recognises, so a panel whose x came out
                    # numeric could silently draw sideways.
                    "orientation": "v",
                    # One slot for every colour trace: colour is paint, each
                    # level has its own x, nothing dodges (see `render`).
                    "offsetgroup": MARK_OFFSET_GROUP,
                    "x": x_values,
                    "y": _values(subset[encoding.y]),
                    # The fill opacity every side shares (base.fill_alpha); on
                    # the marker, so the error bars stay opaque as in matplotlib.
                    "marker": {"color": color, "opacity": fill_alpha(kind, resolved.spec.style)},
                    **({"error_y": error} if error else {}),
                    **hover,
                }
            )
        elif kind in (PlotKind.BOX, PlotKind.VIOLIN):
            traces.append(
                {
                    **base,
                    "type": "box" if kind is PlotKind.BOX else "violin",
                    "orientation": "v",  # see the bar trace above
                    "offsetgroup": MARK_OFFSET_GROUP,
                    "x": x_values,
                    "y": _values(subset[encoding.y]),
                    "marker": {"color": color},
                    "line": {"color": color},
                    # Stated: plotly's own default fill is half-transparent.
                    "fillcolor": _rgba(color, fill_alpha(kind, resolved.spec.style)),
                    "boxpoints": "outliers" if kind is PlotKind.BOX else None,
                    # A violin spans exactly its data, as matplotlib's does.
                    # plotly's default ("soft") runs the KDE two bandwidths
                    # past the extremes — tails the y limits, computed from the
                    # data, would clip.
                    **({"spanmode": "hard"} if kind is PlotKind.VIOLIN else {}),
                }
            )

    return traces


def _positional_x(resolved: ResolvedPlot) -> bool:
    """Whether this figure's x axis is drawn as NUMERIC positions of its levels.

    SPAGHETTI shifts each series a fraction of a tick sideways
    (``ResolvedPlot.series_offsets``), and a "Show sample" overlay shifts its
    points beside their marks (``sample_offsets``) and may join them
    with lines. plotly's category axis cannot take either: a number in a
    category trace is stringified and becomes a NEW category, so ``1.15``
    would draw as its own tick. The axis is therefore linear, with the levels'
    indices as ``tickvals`` and their labels as ``ticktext``, and an explicit
    ``range`` of ``[-0.5, n - 0.5]`` — the range a category axis has by
    default, which is also the geometry :func:`_add_x_groups` assumes when it
    places brackets at ``i / n``. The marks (bars, boxes) are placed by index
    too, one per position, exactly as on a category axis.
    """
    return (
        resolved.kind is PlotKind.SPAGHETTI or bool(resolved.sample_shown)
    ) and is_categorical_x(resolved)


def _level_hover(subset, resolved: ResolvedPlot, base: dict) -> dict:
    """Hover text naming the LEVEL for a mark drawn at an index position —
    a nested leaf as ``stim · pre`` — so the hover never shows the index.

    Carried in ``customdata``, never ``text``: plotly PAINTS a bar trace's
    ``text`` onto the bars (``textposition`` defaults to ``auto``), so a
    hover-only string in ``text`` labelled every bar the moment the axis
    turned positional under a "Show sample" overlay. ``customdata`` is
    hover-only for every trace type.
    """
    labels = [
        str(v).replace(LEAF_SEPARATOR, " · ") for v in subset[resolved.encoding.x].to_numpy()
    ]
    return {
        "customdata": labels,
        "hovertemplate": "%{customdata}<br>%{y}<extra>" + str(base["name"]) + "</extra>",
    }


def _legend_title(resolved: ResolvedPlot) -> str:
    """The marks' colour key, and the overlay's own colour key when its
    levels are listed too — the same ``a / b`` title ``mpl._apply_legend``
    writes."""
    blocks = (
        resolved.labels.color,
        resolved.labels.sample if len(sample_legend_levels(resolved)) > 1 else None,
    )
    return " / ".join(t for t in blocks if t)


def _sample_traces(
    panel,
    resolved: ResolvedPlot,
    x_axis: str,
    y_axis: str,
    seen_legend: set[str] | None = None,
    legend_on: bool = True,
) -> list[dict]:
    """The "Show sample" overlay for one panel: markers, or lines+markers when
    joined, placed by ``base.sample_positions`` on the marks they belong to
    (the same arithmetic ``mpl._draw_sample`` uses).

    Runs and paint are ``base.sample_runs``. In the mark's colour the points
    are never in the legend, and ``legendgroup`` ties a run to its colour
    level so hiding a level hides its points too; a run that crosses the
    marks' colours (neutral line, each marker its own mark's colour) has no
    one level and joins the y label's group.
    With the overlay's own colour (``ResolvedPlot.sample_color``) each level
    gets ONE legend entry (the first trace that draws it, figure-wide via
    ``seen_legend``) and its own ``sample:`` legend group, so clicking a
    subject hides that subject's points and lines in every panel.
    """
    sample = getattr(panel, "sample", None)
    if sample is None or sample.empty or sample_dropped_reason(panel, resolved):
        return []
    seen = seen_legend if seen_legend is not None else set()
    own_color = bool(resolved.sample_color)
    # Listed only when the one rule says so (roles.overlay_in_legend).
    listed = overlay_in_legend(resolved.spec, resolved.sample_color)
    weight = sample_weight(resolved.spec.style)
    traces: list[dict] = []
    for run in sample_runs(sample, resolved):
        level, rows, identity = run.level, run.rows, run.identity
        # The overlay's own colour key when it has one, else the mark's
        # colour — none when the run crosses the marks' colours.
        label = (
            (resolved.text.sample_level(level) if own_color else resolved.text.color_level(level))
            if level is not None
            else resolved.labels.y
        )
        legend_group = f"sample:{label}" if own_color else label
        positions = sample_positions(rows, resolved, identity)
        order = np.argsort(positions, kind="stable")
        hover = sample_hover(rows, resolved)
        levels = [
            str(v).replace(LEAF_SEPARATOR, " · ") for v in rows[resolved.encoding.x].to_numpy()
        ]
        show_legend = listed and legend_on and legend_group not in seen
        if show_legend:
            seen.add(legend_group)
        traces.append(
            {
                "type": "scatter",
                "mode": "lines+markers" if resolved.sample_join and len(rows) > 1 else "markers",
                "name": label if own_color else (str(identity) if identity is not None else label),
                "legendgroup": legend_group,
                "showlegend": show_legend,
                # After every mark entry whatever panel first drew it.
                "legendrank": 2000,
                "xaxis": x_axis,
                "yaxis": y_axis,
                "x": [None if np.isnan(v) else float(v) for v in positions[order]],
                "y": _values(rows[resolved.encoding.y].iloc[order]),
                "marker": {
                    # One colour, or each point its own mark's when the
                    # run crosses the marks' colours (base.sample_runs).
                    "color": run.line_color
                    if run.uniform
                    else [run.point_colors[i] for i in order],
                    "size": weight.marker_px,
                    "line": {"color": SAMPLE_EDGE_COLOR, "width": 0.5},
                },
                "line": {"color": run.line_color, "width": weight.line_pt},
                "opacity": SAMPLE_ALPHA,
                # Hover-only (see `_level_hover` on why not `text`).
                "customdata": [
                    f"{levels[i]}<br>{hover[i]}" if hover[i] else levels[i] for i in order
                ],
                "hovertemplate": "%{customdata}<br>%{y}<extra>" + label + "</extra>",
            }
        )
    # The one line that tells "no lines" apart: joined runs vs one-point runs
    # (a run is one identity inside one bracket — base.sample_runs — never
    # split by the marks' colour), and what decided the join.
    joined = sum(1 for t in traces if t["mode"] == "lines+markers")
    Log.debug(
        "sample overlay panel %s: %d run(s), %d joined, %d single-point; "
        "join=%s (%s), own colour=%r, mark colour=%r",
        panel.title or "unfaceted",
        len(traces),
        joined,
        sum(1 for t in traces if len(t["x"]) == 1),
        resolved.sample_join,
        resolved.sample_join_reason,
        resolved.sample_color,
        resolved.encoding.color,
        layer=LAYER,
    )
    return traces


def _x_ticks(resolved: ResolvedPlot) -> dict:
    """Explicit tick placement for an x axis that needs it (else nothing).

    A nested axis is keyed by composed leaf keys the user must never see; the
    ticks show the innermost layer's value, with the layers above it drawn as
    brackets (see :func:`_add_x_groups`). Spacer positions get no tick.

    A positional axis (:func:`_positional_x`) places the same ticks at the
    levels' integer indices and pins the range a category axis would have had,
    so the brackets' ``i / n`` arithmetic still lands under the right leaves.
    """
    plan = resolved.x_plan
    if _positional_x(resolved):
        order = list(plan.order) if plan else [str(v) for v in resolved.x_order or []]
        labels = (
            list(plan.tick_labels)
            if plan
            else [resolved.text.x_tick(v) for v in resolved.x_order or []]
        )
        return {
            "tickmode": "array",
            "tickvals": list(range(len(order))),
            "ticktext": labels,
            "range": [-0.5, len(order) - 0.5],
        }
    if plan:
        return {
            "tickmode": "array",
            "tickvals": list(plan.order),
            "ticktext": list(plan.tick_labels),
        }
    if is_categorical_x(resolved):
        # A plain category axis names its ticks by the category VALUES, which
        # stay raw (the traces are placed by them). Aliased text, when any
        # tick reads differently, goes on as ticktext over those same values.
        order = [str(v) for v in resolved.x_order or []]
        labels = [resolved.text.x_tick(v) for v in resolved.x_order or []]
        if labels != order:
            return {"tickmode": "array", "tickvals": order, "ticktext": labels}
    return {}


def _spaghetti_traces(subset, resolved, base, color) -> list[dict]:
    """Markers + one polyline per series per bracket (``base.series_runs``),
    at index-plus-offset positions.

    Same placement as ``mpl._draw_spaghetti``: tick index from ``x_order``,
    plus the figure-wide per-series offset, sorted by position so a line runs
    left to right whatever order the rows arrived in.
    """
    encoding = resolved.encoding
    offsets = resolved.series_offsets or {}
    weight = spaghetti_weight(resolved.spec.style)

    traces = []
    for position, (series_id, rows) in enumerate(series_runs(subset, resolved)):
        positions, _ = x_positions(rows[encoding.x], resolved)
        positions = positions + offsets.get(str(series_id), 0.0)
        order = np.argsort(positions, kind="stable")
        # A nested axis keys rows by a composed leaf; show it as "stim · pre".
        labels = [
            str(v).replace(LEAF_SEPARATOR, " · ")
            for v in rows[encoding.x].to_numpy()[order]
        ]
        traces.append(
            {
                **base,
                "showlegend": base["showlegend"] and position == 0,
                "type": "scatter",
                "mode": "lines+markers",
                "x": [None if np.isnan(v) else float(v) for v in positions[order]],
                "y": _values(rows[encoding.y].iloc[order]),
                "line": {"color": color, "width": weight.line_pt},
                "marker": {"color": color, "size": weight.marker_px},
                "opacity": resolved.spec.style.alpha,
                # The axis shows level labels, but the x values are indices;
                # hover names the level and the series so neither is lost.
                # Hover-only (see `_level_hover` on why not `text`).
                "customdata": [
                    f"{label}<br>{series_id}" if series_id is not None else label
                    for label in labels
                ],
                "hovertemplate": "%{customdata}<br>%{y}<extra>" + str(base["name"]) + "</extra>",
            }
        )
    return traces


def _line_traces(subset, resolved, base, color) -> list[dict]:
    """One trace per polyline; only the first carries the legend entry. An
    uncoloured grouping layer is told apart by dash style (``dash_style``),
    the same style in every panel and colour."""
    encoding = resolved.encoding
    traces = []
    for position, (series_id, rows) in enumerate(series_groups(subset, resolved)):
        traces.append(
            {
                **base,
                "showlegend": base["showlegend"] and position == 0,
                "type": "scatter",
                "mode": "lines",
                "x": _values(rows[encoding.x]),
                "y": _values(rows[encoding.y]),
                "line": {"color": color, "width": 1.5, "dash": dash_style(resolved, rows)},
                "opacity": resolved.spec.style.alpha,
                "hovertext": str(series_id) if series_id is not None else None,
            }
        )
    return traces


def _band_traces(subset, resolved, base, color) -> list[dict]:
    """One band per series (an uncoloured grouping layer), each its own fill
    and a centre line in that series' dash style; the first carries the
    legend entry."""
    encoding = resolved.encoding
    traces = []
    for position, (series_id, rows) in enumerate(series_groups(subset, resolved)):
        x_values = _values(rows[encoding.x])
        if encoding.has_error:
            traces.append(
                {
                    **base,
                    "showlegend": False,
                    "type": "scatter",
                    "mode": "lines",
                    "x": x_values + x_values[::-1],
                    "y": _values(rows[encoding.y_high]) + _values(rows[encoding.y_low])[::-1],
                    "fill": "toself",
                    "fillcolor": _rgba(color, 0.22),
                    "line": {"width": 0},
                    "hoverinfo": "skip",
                }
            )
        traces.append(
            {
                **base,
                "showlegend": base["showlegend"] and position == 0,
                "type": "scatter",
                "mode": "lines",
                "x": x_values,
                "y": _values(rows[encoding.y]),
                "line": {"color": color, "width": 2, "dash": dash_style(resolved, rows)},
                "hovertext": str(series_id) if series_id is not None else None,
            }
        )
    return traces


def _dash_legend_traces(resolved) -> list[dict]:
    """One empty trace per dash id so the legend lists the dash styles — in
    neutral grey, since the style is what they tell apart. Their own legend
    group, after the colour entries."""
    return [
        {
            "name": resolved.text.dash_id(sid),
            "legendgroup": f"dash:{sid}",
            "showlegend": True,
            "type": "scatter",
            "mode": "lines",
            "x": [None],
            "y": [None],
            "hoverinfo": "skip",
            "line": {"color": "#555555", "width": 2, "dash": resolved.dash_styles[sid]},
        }
        for sid in dash_levels(resolved)
    ]


def _add_axes(
    layout,
    resolved,
    slot,
    row,
    col,
    n_rows,
    n_cols,
    *,
    bottom: bool = True,
    leftmost: bool = True,
    y_limits: tuple[float, float] | None = None,
    panel=None,
) -> None:
    x_key = "xaxis" if slot == 1 else f"xaxis{slot}"
    y_key = "yaxis" if slot == 1 else f"yaxis{slot}"
    x_anchor = "y" if slot == 1 else f"y{slot}"
    y_anchor = "x" if slot == 1 else f"x{slot}"

    x0, y0, cell_width, cell_height = _cell(
        row, col, n_rows, n_cols, _x_depth(resolved)
    )
    sizes = resolve_sizes(resolved.spec.style)

    layout[x_key] = {
        **plotly_axis_style(),
        "domain": [x0, x0 + cell_width],
        "anchor": x_anchor,
        # Tick labels and the axis title share ONE rule (base.shows_x_labels).
        "showticklabels": bottom,
        "title": {
            "text": resolved.labels.x if bottom else "",
            "font": {"size": sizes.x_label},
        },
        # The fitted size replaces this when decisions are applied.
        "tickfont": {"size": sizes.x_ticks},
        # A nested axis is keyed by composed leaf keys the user must never see;
        # the ticks show the innermost layer's value, with the layers above it
        # drawn as brackets (see _add_x_groups). Spacer positions get no tick.
        # A positional axis (_positional_x) says the same thing in indices:
        # its traces carry level INDICES plus a fractional offset, so the ticks
        # are placed at the integers and named from the plan or the order.
        **_x_ticks(resolved),
        # The category order, STATED. Left unsaid, plotly orders a categorical
        # axis by first appearance in the traces, and the trace order is
        # whatever `_summarize(..., sort=False)` left behind — i.e. database row
        # order. Three things follow from saying it instead:
        #
        #  * the declared level order (`[schema_keys]` -> ScidbSource._ordered
        #    -> FactorInfo.levels -> x_order) actually reaches the figure. It
        #    did not before, for a flat axis as much as a nested one;
        #  * SPACER categories get positions. They hold no data by construction
        #    (xaxis.SPACER_PREFIX), so they appear in no trace and plotly never
        #    learned they existed — which is why a nested axis drew as one
        #    undifferentiated run of bars with no gap between groups;
        #  * `_add_x_groups` places its brackets at `group.start /
        #    len(plan.order)`, arithmetic that assumes the spacers occupy slots.
        #    With them missing every bracket sat left of the bars it named.
        #
        # Categorical only: `is_categorical_x` is false for a 1-D sample index
        # or a joined x measure, and forcing a numeric axis into categories
        # would turn a number line into evenly spaced ticks.
        **(
            {
                "categoryorder": "array",
                "categoryarray": [str(v) for v in resolved.x_order],
            }
            if is_categorical_x(resolved) and not _positional_x(resolved)
            else {}
        ),
        # "category", stated, whenever the axis holds levels — and not left to
        # plotly's auto-detection, which reads an array of NUMERIC-LOOKING
        # STRINGS as a linear axis. Zero-padded schema keys ("01", "02", …) are
        # exactly that, and on a linear axis `categoryarray` is ignored: the
        # declared order would be silently dropped again for precisely the keys
        # this project pads. log_x still wins, since a log category axis is not
        # a thing either backend can draw.
        "type": (
            "log"
            if resolved.spec.style.log_x
            else (
                "category"
                if is_categorical_x(resolved) and not _positional_x(resolved)
                else "-"
            )
        ),
        # Upright, always. Plotly rotates category tick labels towards vertical
        # once a cell is too narrow for them, so the same figure reads
        # differently at two facet counts. Fixed at 0; automargin buys the room.
        "tickangle": 0,
        "automargin": True,
    }
    layout[y_key] = {
        **plotly_axis_style(),
        "domain": [y0, y0 + cell_height],
        "anchor": y_anchor,
        "showticklabels": leftmost,
        # Tick labels and the axis title do NOT share a rule here (unlike x):
        # a faceted panel's title names that panel, so it is drawn even where
        # the shared tick labels are suppressed. See base.panel_y_title.
        "title": {
            "text": panel_y_title(resolved, panel, leftmost=leftmost),
            "font": {"size": sizes.y_label},
        },
        "tickfont": {"size": sizes.y_ticks},
        "type": "log" if resolved.spec.style.log_y else "-",
        # No automargin here, deliberately, even though the x axes use it: an
        # inner column's title is rotated text drawn into X_GAP (which is sized
        # for it), and plotly's automargin answers a crowded subplot axis by
        # growing the FIGURE's left margin — it would take back across the whole
        # width the room this change just gave the panels.
    }
    # Link the axes when the spec asks for shared scales, so panning/zooming one
    # subplot moves them all — and so hiding tick labels stays truthful.
    if slot != 1:
        if resolved.spec.facet.share_x:
            layout[x_key]["matches"] = "x"
        # `matches` on y is DERIVED, exactly as matplotlib's sharey is: linking
        # panels that hold different ranges would make zooming one rescale the
        # rest, silently discarding the per-panel limits below.
        if shares_y_axis(resolved):
            layout[y_key]["matches"] = "y"
    if y_limits and resolved.kind is not PlotKind.HEATMAP:
        span = axis_range(y_limits, log=resolved.spec.style.log_y)
        if span is not None:
            layout[y_key]["range"] = list(span)


#: Approximate width of one legend character at the webview's font size, in px.
#: The renderer emits JSON and never measures text, so the strip is sized from
#: the label lengths — generously, because too wide only costs plot area while
#: too narrow clips the level names.
LEGEND_CHAR_PX = 8
#: Swatch, padding and the gap between the panels and the legend.
LEGEND_FIXED_PX = 48
#: The preview's own cap (the export now narrows its legend to
#: mpl.LEGEND_BUDGET or moves it below the panels; the preview follows in
#: stage 4 of .claude/plan-tick-label-legibility.md), in pixels against
#: the default figure width.
MAX_LEGEND_PX = 320


def _right_margin(resolved: ResolvedPlot) -> int:
    """Room on the right for the legend, sized from the longest entry."""
    entries = [resolved.text.color_level(level) for level in legend_levels(resolved)]
    entries.extend(resolved.text.dash_id(sid) for sid in dash_levels(resolved))
    entries.append(resolved.labels.color or "")
    entries.append(resolved.labels.dash or "")
    longest = max((len(text) for text in entries), default=0)
    return int(min(MAX_LEGEND_PX, LEGEND_FIXED_PX + LEGEND_CHAR_PX * longest))


#: Space between subplot cells, as a fraction of the figure. The vertical gap is
#: still the larger of the two — x tick labels hang below a cell — but it used
#: to be 0.14 because a panel TITLE also sat above the next row and at 0.06 the
#: two collided. Facet names moved onto the y axis (base.panel_y_title), so that
#: strip is no longer spent on captions and the panels keep the height.
X_GAP = 0.06
Y_GAP = 0.09


def _x_depth(resolved: ResolvedPlot) -> int:
    """Rows of nested x-group labels that hang below a cell (0 when flat)."""
    return resolved.x_plan.depth if resolved.x_plan else 0


def _gaps(n_rows: int, n_cols: int, x_depth: int = 0) -> tuple[float, float]:
    """
    Gap between cells, never more than half the figure in total.

    One function so every consumer of the layout agrees about how much room
    there is between two cells — the y-axis titles of the inner columns are
    drawn into the horizontal gap, and the x tick labels into the vertical one.

    ``x_depth`` is why the vertical gap is not just a constant: a nested x axis
    draws a row of group labels and brackets under each panel that shows tick
    labels (:func:`_add_x_groups`), and in a grid that is INSIDE the gap rather
    than in the figure's bottom margin. Sizing the gap from the same number the
    brackets are placed with is what stops them landing on the row below.
    """
    return (
        min(X_GAP, 0.5 / (n_cols - 1)) if n_cols > 1 else 0.0,
        min(Y_GAP + X_GROUP_ROW * x_depth, 0.5 / (n_rows - 1)) if n_rows > 1 else 0.0,
    )


def _cell(
    row, col, n_rows, n_cols, x_depth: int = 0
) -> tuple[float, float, float, float]:
    """
    (x0, y0, width, height) of one grid cell, in paper coordinates.

    The gaps are a fraction of the FIGURE, so they must be clamped against the
    cell count: at a fixed Y_GAP of 0.14 a 9-row grid spends 1.12 of its 1.0 on
    gaps, the cell height goes negative, and every panel's y domain runs
    backwards — panels invert and overlap. Capping the total gap at half the
    figure keeps every cell at least ``0.5 / n`` tall, and the extra rows are
    absorbed by the figure's pixel height instead (the GUI sizes it from
    ``layout.meta.rows``).
    """
    x_gap, y_gap = _gaps(n_rows, n_cols, x_depth)
    cell_width = (1.0 - x_gap * (n_cols - 1)) / n_cols
    cell_height = (1.0 - y_gap * (n_rows - 1)) / n_rows
    # Plotly's y domain runs bottom-up; our rows run top-down.
    x0 = col * (cell_width + x_gap)
    y0 = (n_rows - row - 1) * (cell_height + y_gap)
    # The last cell's edge is 1.0 by construction, but only exactly so in real
    # arithmetic; plotly rejects a domain above 1, so trim the float residue.
    return x0, y0, min(cell_width, 1.0 - x0), min(cell_height, 1.0 - y0)


def _values(series: pd.Series) -> list:
    """Series -> a JSON-safe list (numpy scalars are not JSON serializable)."""
    return [None if pd.isna(v) else (v.item() if hasattr(v, "item") else v) for v in series]


def _rgba(hex_color: str, alpha: float) -> str:
    hex_color = hex_color.lstrip("#")
    r, g, b = (int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


# ---------------------------------------------------------------------------
# Applying the export's decisions (mpl.layout_decisions) to the preview

#: matplotlib's default font first: the decisions were measured in it. Arial
#: is the fallback because it is NARROWER than DejaVu Sans, so a label fitted
#: in DejaVu still fits; Verdana, wider, would not.
PREVIEW_FONT_FAMILY = "DejaVu Sans, Arial, sans-serif"

#: ``name`` prefix of a bracket-label annotation (``_add_x_groups``).
X_GROUP_TAG = "x-group"

#: The preview is drawn at 1 pt = 1 px — the convention ``text.base`` already
#: follows (`layout.font.size`), so a decision in points is one in pixels.
PX_PER_IN = 72.0


def _html(text: str) -> str:
    return str(text).replace("\n", "<br>")


def _apply_decisions(layout: dict, resolved: ResolvedPlot, decisions: dict) -> None:
    """Draw what the export decided, rather than deciding again.

    Tick labels (stripped, wrapped, thinned), their angle and font; bracket
    labels' font and wrapping; the legend's font, wrapped title and place
    (right, or below the panels) with the margin it needs. Anything the
    decisions do not cover is left as the renderer drew it.
    """
    width_px = float(decisions.get("width_in") or resolved.spec.style.width) * PX_PER_IN
    height_px = float(decisions.get("height_in") or resolved.spec.style.height) * PX_PER_IN
    ticks = decisions.get("ticks")
    if ticks is not None and ticks.rows:
        texts = [_html(t) for t in ticks.rows[0]]
        applied = 0
        for key, axis in layout.items():
            if not (key.startswith("xaxis") and isinstance(axis, dict)):
                continue
            if len(axis.get("ticktext") or []) == len(texts):
                axis["ticktext"] = texts
            elif len(axis.get("categoryarray") or []) == len(texts):
                axis.update(
                    tickmode="array", tickvals=list(axis["categoryarray"]), ticktext=texts
                )
            else:
                Log.debug(
                    "preview %s: %d tick(s) here, %d decided — left as drawn",
                    key, len(axis.get("ticktext") or axis.get("categoryarray") or []),
                    len(texts), layer=LAYER,
                )
                continue
            # matplotlib turns counter-clockwise; plotly's angle is clockwise.
            axis["tickangle"] = -int(ticks.rotation)
            axis["tickfont"] = {"size": ticks.font_pt}
            applied += 1
        Log.debug("preview: tick decision applied to %d x axis/axes", applied, layer=LAYER)

    brackets = decisions.get("brackets")
    plan = resolved.x_plan
    if brackets is not None and plan is not None and plan.groups:
        depths = sorted({g.depth for g in plan.groups})
        fitted: dict[str, str] = {}
        for depth, row in zip(depths, brackets.rows):
            spans = [g for g in plan.groups if g.depth == depth]
            for group, text in zip(spans, row):
                fitted[f"{X_GROUP_TAG}:{group.depth}:{group.start}"] = text
        for note in layout.get("annotations", []):
            name = note.get("name", "")
            if name in fitted:
                note["text"] = _html(fitted[name])
                note["font"] = {**note.get("font", {}), "size": brackets.font_pt}
        # A bracket whose label the export blanked loses its rule too, as does
        # a row with nothing shown above it to bracket — the export's rule
        # (base.ruled_bracket_depths, drawn by mpl._draw_x_groups).
        shown = {
            depth
            for depth, row in zip(depths, brackets.rows)
            if any(text for text in row)
        }
        if ticks is None or not ticks.rows or any(ticks.rows[0]):
            shown.add(plan.depth)
        ruled = ruled_bracket_depths(plan.depth, shown)
        blank = {
            name
            for name, text in fitted.items()
            if not text or int(name.split(":")[1]) not in ruled
        }
        if blank and layout.get("shapes"):
            kept = [s for s in layout["shapes"] if s.get("name") not in blank]
            Log.debug(
                "preview: %d bracket rule(s) dropped (blank label or nothing shown above)",
                len(layout["shapes"]) - len(kept),
                layer=LAYER,
            )
            layout["shapes"] = kept

    legend = decisions.get("legend")
    if legend is not None and layout.get("showlegend"):
        spec_legend = layout.setdefault("legend", {})
        spec_legend["font"] = {"size": legend["font_pt"]}
        title = spec_legend.setdefault("title", {})
        if "wrap_title" in legend["steps"] and not legend["below"]:
            title["text"] = " /<br>".join(
                part for part in str(title.get("text", "")).split(" / ")
            )
        title["font"] = {"size": legend.get("title_font_pt", legend["font_pt"])}
        margin = layout.setdefault("margin", {})
        if legend["below"]:
            spec_legend.update(
                orientation="h", x=0.5, xanchor="center", y=0.0, yanchor="bottom",
                yref="container",
            )
            margin["r"] = BARE_RIGHT_MARGIN
            margin["b"] = margin.get("b", 50) + round(legend["height_frac"] * height_px)
        else:
            margin["r"] = max(BARE_RIGHT_MARGIN, round(legend["width_frac"] * width_px))

    layout["meta"]["label_fit"] = {
        "decided_at_in": [decisions.get("width_in"), decisions.get("height_in")],
        "ticks": ticks.describe() if ticks is not None else None,
        "fits": ticks.fits if ticks is not None else True,
        "legend": legend,
    }
