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

from ..resolved import ResolvedPlot
from ..spec import PlotKind
from .base import (
    color_groups,
    grid_shape,
    legend_levels,
    palette_for,
    panel_position,
    axis_range,
    panel_y_limits,
    panel_y_title,
    shares_y_axis,
    shows_legend,
    shows_x_labels,
    shows_y_labels,
)

LAYER = "scistackplot"

#: Right margin with no legend in it — just room for the last x tick label.
BARE_RIGHT_MARGIN = 20


def render(resolved: ResolvedPlot) -> dict:
    """Build a plotly.js figure dict."""
    with Log.timer("render_plotly", layer=LAYER, extra=str(resolved.kind)):
        n_rows, n_cols = grid_shape(resolved)
        traces: list[dict] = []
        legend_on = shows_legend(resolved)
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
                "title": {"text": resolved.labels.color or ""},
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
            },
        }
        if resolved.labels.title:
            layout["title"] = {"text": resolved.labels.title}

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

        return {"data": traces, "layout": layout}


#: Vertical room, in paper fraction, for one row of nested group labels.
X_GROUP_ROW = 0.045


def _add_x_groups(layout, resolved, row, col, n_rows, n_cols, slot) -> None:
    """Label and bracket each higher x layer beneath the tick labels.

    Drawn in PAPER coordinates from the cell's own domain, so the brackets sit
    under the panel they describe in a facet grid — a data-coordinate
    annotation would be clipped by the axis range and would move when the user
    zooms.

    Only under panels that show tick labels: repeating "stim | sham" under every
    row of a grid is the same noise ``shows_x_labels`` already suppresses for
    the ticks themselves.
    """
    plan = resolved.x_plan
    if not plan or not plan.groups or not shows_x_labels(resolved, row, col):
        return

    x0, y0, cell_width, _cell_height = _cell(
        row, col, n_rows, n_cols, _x_depth(resolved)
    )
    positions = max(1, len(plan.order))

    for group in plan.groups:
        # Leaf index -> paper x. Centre of a leaf cell is (i + 0.5) / n.
        left = x0 + cell_width * (group.start / positions)
        right = x0 + cell_width * ((group.end + 1) / positions)
        # Deeper layers sit closer to the axis; depth 0 is furthest below.
        rows_below = plan.depth - group.depth
        y = y0 - X_GROUP_ROW * rows_below - 0.03

        layout["annotations"].append(
            {
                "text": group.label,
                "x": (left + right) / 2.0,
                "y": y,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 10},
                "xanchor": "center",
                "yanchor": "top",
            }
        )
        layout.setdefault("shapes", []).append(
            {
                "type": "line",
                "xref": "paper",
                "yref": "paper",
                # Inset slightly so adjacent brackets read as separate spans
                # rather than one continuous rule.
                "x0": left + cell_width * 0.004,
                "x1": right - cell_width * 0.004,
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
        label = str(level) if level is not None else resolved.labels.y
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
        x_values = _values(subset[encoding.x])

        if kind in (PlotKind.SCATTER, PlotKind.STRIP):
            traces.append(
                {
                    **base,
                    "type": "scatter",
                    "mode": "markers",
                    "x": x_values,
                    "y": _values(subset[encoding.y]),
                    "marker": {"color": color, "size": 8, "opacity": resolved.spec.style.alpha},
                }
            )
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
                }
            traces.append(
                {
                    **base,
                    "type": "bar",
                    # Stated, never inferred: plotly picks an orientation from
                    # which of x/y it recognises, so a panel whose x came out
                    # numeric could silently draw sideways.
                    "orientation": "v",
                    "x": x_values,
                    "y": _values(subset[encoding.y]),
                    "marker": {"color": color},
                    **({"error_y": error} if error else {}),
                }
            )
        elif kind in (PlotKind.BOX, PlotKind.VIOLIN):
            traces.append(
                {
                    **base,
                    "type": "box" if kind is PlotKind.BOX else "violin",
                    "orientation": "v",  # see the bar trace above
                    "x": x_values,
                    "y": _values(subset[encoding.y]),
                    "marker": {"color": color},
                    "line": {"color": color},
                    "boxpoints": "outliers" if kind is PlotKind.BOX else None,
                    # A violin spans exactly its data, as matplotlib's does.
                    # plotly's default ("soft") runs the KDE two bandwidths
                    # past the extremes — tails the y limits, computed from the
                    # data, would clip.
                    **({"spanmode": "hard"} if kind is PlotKind.VIOLIN else {}),
                }
            )

    return traces


def _line_traces(subset, resolved, base, color) -> list[dict]:
    """One trace per polyline; only the first carries the legend entry."""
    encoding = resolved.encoding
    series_column = encoding.series
    if series_column and series_column in subset.columns:
        groups = list(subset.groupby(series_column, sort=False))
    else:
        groups = [(None, subset)]

    traces = []
    for position, (series_id, rows) in enumerate(groups):
        traces.append(
            {
                **base,
                "showlegend": base["showlegend"] and position == 0,
                "type": "scatter",
                "mode": "lines",
                "x": _values(rows[encoding.x]),
                "y": _values(rows[encoding.y]),
                "line": {"color": color, "width": 1.5},
                "opacity": resolved.spec.style.alpha,
                "hovertext": str(series_id) if series_id is not None else None,
            }
        )
    return traces


def _band_traces(subset, resolved, base, color) -> list[dict]:
    encoding = resolved.encoding
    x_values = _values(subset[encoding.x])
    traces = []
    if encoding.has_error:
        traces.append(
            {
                **base,
                "showlegend": False,
                "type": "scatter",
                "mode": "lines",
                "x": x_values + x_values[::-1],
                "y": _values(subset[encoding.y_high]) + _values(subset[encoding.y_low])[::-1],
                "fill": "toself",
                "fillcolor": _rgba(color, 0.22),
                "line": {"width": 0},
                "hoverinfo": "skip",
            }
        )
    traces.append(
        {
            **base,
            "type": "scatter",
            "mode": "lines",
            "x": x_values,
            "y": _values(subset[encoding.y]),
            "line": {"color": color, "width": 2},
        }
    )
    return traces


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

    layout[x_key] = {
        "domain": [x0, x0 + cell_width],
        "anchor": x_anchor,
        # Tick labels and the axis title share ONE rule (base.shows_x_labels).
        "showticklabels": bottom,
        "title": {"text": resolved.labels.x if bottom else ""},
        # A nested axis is keyed by composed leaf keys the user must never see;
        # the ticks show the innermost layer's value, with the layers above it
        # drawn as brackets (see _add_x_groups). Spacer positions get no tick.
        **(
            {
                "tickmode": "array",
                "tickvals": list(resolved.x_plan.order),
                "ticktext": list(resolved.x_plan.tick_labels),
            }
            if resolved.x_plan
            else {}
        ),
        "type": "log" if resolved.spec.style.log_x else "-",
        # Upright, always. Plotly rotates category tick labels towards vertical
        # once a cell is too narrow for them, so the same figure reads
        # differently at two facet counts. Fixed at 0; automargin buys the room.
        "tickangle": 0,
        "automargin": True,
    }
    layout[y_key] = {
        "domain": [y0, y0 + cell_height],
        "anchor": y_anchor,
        "showticklabels": leftmost,
        # Tick labels and the axis title do NOT share a rule here (unlike x):
        # a faceted panel's title names that panel, so it is drawn even where
        # the shared tick labels are suppressed. See base.panel_y_title.
        "title": {"text": panel_y_title(resolved, panel, leftmost=leftmost)},
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
#: Same cap as the matplotlib path (mpl.MAX_LEGEND_FRACTION), in pixels against
#: the default figure width.
MAX_LEGEND_PX = 320


def _right_margin(resolved: ResolvedPlot) -> int:
    """Room on the right for the legend, sized from the longest entry."""
    entries = [str(level) for level in legend_levels(resolved)]
    entries.append(resolved.labels.color or "")
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
