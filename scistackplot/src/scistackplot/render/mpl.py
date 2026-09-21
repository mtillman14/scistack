"""
matplotlib renderer — the export and pipeline path.

Returns a ``matplotlib.figure.Figure``, which is exactly what a scidb
``plot_`` endpoint must return (the framework saves and closes it). This is
also the renderer whose output the generated seaborn/matplotlib code is
expected to reproduce.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scistacklog import Log

from ..figsize import aspect_name
from ..resolved import MPL_DASHES, ResolvedPlot
from ..spec import PlotKind
from ..table import natural_sort_key
from .base import (
    SAMPLE_ALPHA,
    SAMPLE_EDGE_COLOR,
    SAMPLE_LINE_WIDTH,
    SAMPLE_MARKER_FRACTION,
    color_groups,
    dash_levels,
    dash_style,
    grid_shape,
    is_categorical_x,
    legend_levels,
    mark_width,
    palette_for,
    panel_position,
    drawable_limits,
    panel_y_limits,
    panel_y_title,
    shares_y_axis,
    shows_legend,
    shows_x_labels,
    shows_y_labels,
    sample_dropped_reason,
    sample_groups,
    sample_hover,
    sample_legend_levels,
    sample_paint,
    sample_palette_for,
    sample_positions,
    sample_series,
    series_runs,
    x_positions,
)

LAYER = "scistackplot"


def render(resolved: ResolvedPlot):
    """Draw ``resolved`` and return the Figure (caller owns closing it)."""
    import matplotlib

    if matplotlib.get_backend().lower() not in ("agg", "template"):
        # Rendering happens inside a server process and inside for_each; a GUI
        # backend there either warns or blocks. Agg is the only safe default.
        matplotlib.use("Agg", force=False)
    import matplotlib.pyplot as plt

    with Log.timer("render_mpl", layer=LAYER, extra=str(resolved.kind)):
        style = resolved.spec.style
        # rc_context, not rcParams: text reads the size at CREATION, so this
        # has to be open while every label, tick and legend entry is made —
        # and it must close afterwards, because this runs inside a server and
        # inside for_each, where a leaked rcParam would resize someone else's
        # figure. One key: every other text size in matplotlib is relative to
        # font.size, so this scales ticks, labels, legend and title together.
        with plt.rc_context({"font.size": style.font_size}):
            n_rows, n_cols = grid_shape(resolved)
            # The size the file will have (before bbox_inches="tight" trims the
            # margins). Stated in the log because the preview never shows it: a
            # figure that "came out squashed" is diagnosed here, not in the GUI.
            Log.info(
                "figure size %.2f x %.2f in (%s), font %gpt, %d x %d panel grid",
                style.width,
                style.height,
                aspect_name(style.width, style.height),
                style.font_size,
                n_rows,
                n_cols,
                layer=LAYER,
            )
            fig, axes = plt.subplots(
                n_rows,
                n_cols,
                figsize=(style.width, style.height),
                squeeze=False,
                sharex=resolved.spec.facet.share_x,
                # DERIVED, not configured. matplotlib's sharey ties the axes
                # together, so one panel's autoscale drags every other panel with
                # it — exactly wrong once `y_axis.scope` asks for per-panel ranges,
                # and the set_ylim below would be silently overruled by whichever
                # panel was drawn last.
                sharey=shares_y_axis(resolved),
            )

            used: set[tuple[int, int]] = set()
            # (row, col) -> panel, so the cosmetics pass can ask a CELL for its
            # panel's limits. It walks the grid rather than the panel list (blank
            # cells need hiding too), and the two orders are not the same.
            at_cell: dict[tuple[int, int], Any] = {}
            for index, panel in enumerate(resolved.panels):
                row, col = panel_position(resolved, index)
                if (row, col) in used or not (0 <= row < n_rows and 0 <= col < n_cols):
                    # reduce._assign_grid guarantees one panel per cell inside the
                    # reported grid. If that ever breaks, say so — the old silent
                    # clamp drew two panels onto one axes, which looks like bad data
                    # rather than a layout bug.
                    Log.warn(
                        "panel %r wants cell (%d,%d) in a %dx%d grid, which is "
                        "occupied or out of range — clamping",
                        panel.title,
                        row,
                        col,
                        n_rows,
                        n_cols,
                        layer=LAYER,
                    )
                row = min(max(row, 0), n_rows - 1)
                col = min(max(col, 0), n_cols - 1)
                ax = axes[row][col]
                used.add((row, col))
                at_cell[(row, col)] = panel
                _draw_panel(ax, panel.frame, resolved)
                _draw_sample(ax, panel, resolved)
                # No subplot caption: a faceted panel is named by its y-axis title
                # instead (base.panel_y_title), which buys back the row of vertical
                # space a title costs in every row of the grid.

            # Blank out grid cells no panel landed in (a wrapped grid's remainder).
            for row in range(n_rows):
                for col in range(n_cols):
                    if (row, col) not in used:
                        axes[row][col].set_visible(False)

            _apply_axes_cosmetics(fig, axes, resolved, n_rows, n_cols, at_cell)

            if resolved.labels.title:
                fig.suptitle(resolved.labels.title)
            # tight_layout is told how much width the legend took. A FIGURE legend
            # is invisible to tight_layout, so laying the axes out across the whole
            # width put the legend on top of the rightmost panels in the exported
            # PNG while the interactive plotly view kept it outside — the same
            # figure reading two different ways depending on how you looked at it.
            reserved = _apply_legend(fig, resolved)
            fig.tight_layout(rect=(0.0, 0.0, 1.0 - reserved, 1.0))
            return fig


# ---------------------------------------------------------------------------


def _draw_panel(ax, frame: pd.DataFrame, resolved: ResolvedPlot) -> None:
    if frame.empty:
        ax.text(
            0.5,
            0.5,
            "no data",
            ha="center",
            va="center",
            transform=ax.transAxes,
            color="#888888",
        )
        return

    kind = resolved.kind
    if kind is PlotKind.HEATMAP:
        _draw_heatmap(ax, frame, resolved)
    elif kind in (PlotKind.SCATTER, PlotKind.STRIP):
        _draw_points(ax, frame, resolved, jitter=kind is PlotKind.STRIP)
    elif kind is PlotKind.SPAGHETTI:
        _draw_spaghetti(ax, frame, resolved)
    elif kind is PlotKind.LINE:
        _draw_lines(ax, frame, resolved)
    elif kind is PlotKind.BAND:
        _draw_band(ax, frame, resolved)
    elif kind is PlotKind.BAR:
        _draw_bars(ax, frame, resolved)
    elif kind in (PlotKind.BOX, PlotKind.VIOLIN):
        _draw_distribution(ax, frame, resolved, violin=kind is PlotKind.VIOLIN)


def _draw_points(ax, frame, resolved, *, jitter: bool) -> None:
    style = resolved.spec.style
    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        positions, _ = x_positions(subset[resolved.encoding.x], resolved)
        if jitter and is_categorical_x(resolved):
            rng = np.random.default_rng(abs(hash(str(level))) % (2**32))
            positions = positions + rng.uniform(-0.15, 0.15, size=len(positions))
        ax.scatter(
            positions,
            subset[resolved.encoding.y].to_numpy(dtype=float),
            s=style.marker_size,
            alpha=style.alpha,
            color=palette_for(resolved, level, index),
            label=str(level) if level is not None else None,
        )


def _draw_spaghetti(ax, frame, resolved) -> None:
    """Markers plus one polyline per series (subject) across the x positions
    of ONE bracket (``base.series_runs``: a line spans the innermost tick
    only, so a nested axis gets one polyline per series per bracket).

    The x axis is categorical, so each series is placed at its tick INDEX plus
    the figure-wide offset ``ResolvedPlot.series_offsets`` gave it — the same
    number at every position, so the line stays parallel to its neighbours and
    ends exactly on its own markers. Rows arrive in database order and are
    sorted by position here, or a subject's "post" could be joined back to its
    "pre" from the wrong side.
    """
    style = resolved.spec.style
    encoding = resolved.encoding
    offsets = resolved.series_offsets or {}
    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        color = palette_for(resolved, level, index)
        for position, (series_id, rows) in enumerate(series_runs(subset, resolved)):
            positions, _ = x_positions(rows[encoding.x], resolved)
            positions = positions + offsets.get(str(series_id), 0.0)
            order = np.argsort(positions, kind="stable")
            ax.plot(
                positions[order],
                rows[encoding.y].to_numpy(dtype=float)[order],
                color=color,
                alpha=style.alpha,
                linewidth=1.2,
                marker="o",
                # scatter's `s` is an area in pt²; plot's markersize is a
                # diameter in pt. Same visual size as the scatter kinds.
                markersize=float(np.sqrt(style.marker_size)),
                label=str(level) if (level is not None and position == 0) else None,
            )


def _series_groups(subset, resolved) -> list[tuple]:
    """``(series id, rows)`` per polyline / band, or one group for the lot."""
    series_column = resolved.encoding.series
    if series_column and series_column in subset.columns:
        return list(subset.groupby(series_column, sort=False))
    return [(None, subset)]


def _linestyle(resolved, rows):
    return MPL_DASHES[dash_style(resolved, rows)]


def _draw_lines(ax, frame, resolved) -> None:
    style = resolved.spec.style
    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        color = palette_for(resolved, level, index)
        for position, (_, line_rows) in enumerate(_series_groups(subset, resolved)):
            positions, _ = x_positions(line_rows[resolved.encoding.x], resolved)
            ax.plot(
                positions,
                line_rows[resolved.encoding.y].to_numpy(dtype=float),
                color=color,
                alpha=style.alpha,
                linewidth=1.4,
                # An uncoloured grouping layer is told apart by dash style —
                # the same style in every panel and colour (`dash_styles`).
                linestyle=_linestyle(resolved, line_rows),
                # Only the first line of a colour group carries the legend entry,
                # otherwise a 200-trial plot produces a 200-entry legend.
                label=str(level) if (level is not None and position == 0) else None,
            )
    _dash_legend_handles(ax, resolved)


def _draw_band(ax, frame, resolved) -> None:
    """One band per series (an uncoloured grouping layer), each its own fill
    and a centre line in that series' dash style."""
    encoding = resolved.encoding
    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        color = palette_for(resolved, level, index)
        for position, (_, rows) in enumerate(_series_groups(subset, resolved)):
            positions, _ = x_positions(rows[encoding.x], resolved)
            centre = rows[encoding.y].to_numpy(dtype=float)
            ax.plot(
                positions,
                centre,
                color=color,
                linewidth=1.8,
                linestyle=_linestyle(resolved, rows),
                label=str(level) if (level is not None and position == 0) else None,
            )
            if encoding.has_error:
                ax.fill_between(
                    positions,
                    rows[encoding.y_low].to_numpy(dtype=float),
                    rows[encoding.y_high].to_numpy(dtype=float),
                    color=color,
                    alpha=0.22,
                    linewidth=0,
                )
    _dash_legend_handles(ax, resolved)


def _dash_legend_handles(ax, resolved) -> None:
    """Empty neutral-grey lines, one per dash id, so `_apply_legend` (which
    gathers every axes' labelled handles) lists the dash styles after the
    colours. Drawn on every panel; the legend de-duplicates by label."""
    ids = dash_levels(resolved)
    if len(ids) < 2:
        return
    for sid in ids:
        ax.plot([], [], color="#555555", linewidth=2, linestyle=MPL_DASHES[resolved.dash_styles[sid]], label=sid)


def _draw_bars(ax, frame, resolved) -> None:
    """One bar per x position, painted by its colour level.

    No dodge: colour is paint (``roles.GroupingLayers``), so every colour
    level already has its own tick and the colour groups here are disjoint
    sets of x positions — each bar sits ON its tick, ``mark_width`` wide,
    whichever level paints it. ``test_colour_is_paint.py`` holds the
    geometry to that.
    """
    encoding = resolved.encoding
    width = mark_width()

    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        positions, ticks = x_positions(subset[encoding.x], resolved)
        centre = subset[encoding.y].to_numpy(dtype=float)
        error = None
        if encoding.has_error:
            low = subset[encoding.y_low].to_numpy(dtype=float)
            high = subset[encoding.y_high].to_numpy(dtype=float)
            error = np.vstack([centre - low, high - centre])
        ax.bar(
            positions,
            centre,
            width=width,
            yerr=error,
            capsize=3,
            color=palette_for(resolved, level, index),
            alpha=resolved.spec.style.alpha,
            label=str(level) if level is not None else None,
        )
        if ticks is not None:
            ax.set_xticks(range(len(ticks)))
            ax.set_xticklabels(ticks)


def _x_levels(frame: pd.DataFrame, resolved: ResolvedPlot) -> list[Any]:
    """The x slots a distribution is drawn at, in order.

    ``x_order`` carries the table's level order (declared ``[schema_keys]``
    first). Without one, natural sort — the same fallback
    ``reduce._level_rank`` uses — so "1, 2, 10" never renders as
    "1, 10, 2", which plain ``key=str`` did.
    """
    if resolved.x_order:
        return list(resolved.x_order)
    return sorted(
        frame[resolved.encoding.x].dropna().unique().tolist(), key=natural_sort_key
    )


def _draw_distribution(ax, frame, resolved, *, violin: bool) -> None:
    """Box or violin, one per x position, painted by its colour level (no
    dodge — see ``_draw_bars``)."""
    encoding = resolved.encoding
    width = mark_width()
    order = _x_levels(frame, resolved)

    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        datasets: list[np.ndarray] = []
        positions: list[float] = []
        for slot, level_value in enumerate(order):
            values = subset[subset[encoding.x].astype(str) == str(level_value)][
                encoding.y
            ].to_numpy(dtype=float)
            values = values[~np.isnan(values)]
            if values.size:
                datasets.append(values)
                positions.append(float(slot))
        if not datasets:
            continue

        color = palette_for(resolved, level, index)
        if violin:
            parts = ax.violinplot(
                datasets, positions=positions, widths=width * 0.9, showmeans=True
            )
            for body in parts["bodies"]:
                body.set_facecolor(color)
                body.set_alpha(0.55)
        else:
            drawn = ax.boxplot(
                datasets,
                positions=positions,
                widths=width * 0.85,
                patch_artist=True,
                manage_ticks=False,
            )
            for box in drawn["boxes"]:
                box.set_facecolor(color)
                box.set_alpha(0.6)
        if level is not None:
            # Boxes carry no legend handle of their own; a proxy patch does.
            ax.plot([], [], color=color, linewidth=6, label=str(level))

    ax.set_xticks(range(len(order)))
    ax.set_xticklabels([str(v) for v in order])


def _draw_sample(ax, panel, resolved: ResolvedPlot) -> None:
    """The "Show sample" overlay: one point per row of ``panel.sample`` inside
    the mark it belongs to, joined into a line per identity when
    ``ResolvedPlot.sample_join`` says so.

    Placement is ``base.sample_positions`` — the row's own mark's position
    (its tick; on a spaghetti its line's shift) plus the identity's offset —
    so the points sit in the bar, box or line they were averaged into.
    Colour is ``base.sample_groups`` / ``sample_paint``: the mark's, or the
    overlay's own key's (``ResolvedPlot.sample_color``), in which case a
    line runs across the mark colours and the legend lists the levels
    (``_sample_legend_handles``). Drawn after the marks, on top.
    """
    sample = getattr(panel, "sample", None)
    if sample is None or sample.empty or sample_dropped_reason(panel, resolved):
        return
    style = resolved.spec.style
    size = style.marker_size * SAMPLE_MARKER_FRACTION
    for index, (level, subset) in enumerate(sample_groups(sample, resolved)):
        color = sample_paint(resolved, level, index)
        for identity, rows in sample_series(subset, resolved):
            positions = sample_positions(rows, resolved, identity)
            order = np.argsort(positions, kind="stable")
            values = rows[resolved.encoding.y].to_numpy(dtype=float)[order]
            if resolved.sample_join and len(rows) > 1:
                ax.plot(
                    positions[order],
                    values,
                    color=color,
                    alpha=SAMPLE_ALPHA,
                    linewidth=SAMPLE_LINE_WIDTH,
                    marker="o",
                    markersize=float(np.sqrt(size)),
                    markeredgecolor=SAMPLE_EDGE_COLOR,
                    markeredgewidth=0.5,
                    zorder=3,
                )
            else:
                ax.scatter(
                    positions[order],
                    values,
                    s=size,
                    color=color,
                    alpha=SAMPLE_ALPHA,
                    edgecolors=SAMPLE_EDGE_COLOR,
                    linewidths=0.5,
                    zorder=3,
                )
    _sample_legend_handles(ax, resolved)


def _sample_legend_handles(ax, resolved: ResolvedPlot) -> None:
    """Empty markers, one per overlay colour level, so `_apply_legend` lists
    the overlay's levels after the marks' — the dash-style block's pattern.
    Only when the overlay has its own colour: painted in the mark's colour
    the points add nothing a legend could say."""
    levels = sample_legend_levels(resolved)
    if len(levels) < 2:
        return
    for index, level in enumerate(levels):
        ax.plot(
            [],
            [],
            color=sample_palette_for(resolved, level, index),
            linestyle="-" if resolved.sample_join else "none",
            marker="o",
            markeredgecolor=SAMPLE_EDGE_COLOR,
            markeredgewidth=0.5,
            label=str(level),
        )


def _draw_heatmap(ax, frame, resolved) -> None:
    matrix = frame[resolved.encoding.z].iloc[0]
    image = ax.imshow(np.asarray(matrix, dtype=float), aspect="auto", origin="lower")
    ax.figure.colorbar(image, ax=ax, fraction=0.046, pad=0.04)


def _apply_axes_cosmetics(fig, axes, resolved: ResolvedPlot, n_rows, n_cols, at_cell) -> None:
    style = resolved.spec.style
    for row in range(n_rows):
        for col in range(n_cols):
            ax = axes[row][col]
            if not ax.get_visible():
                continue
            # ONE rule decides both the axis title and the tick labels (see
            # base.shows_x_labels). They used to drift: the title followed
            # "nothing below" while the categorical block further down
            # re-applied set_xticklabels to every panel, so tick labels came
            # back everywhere.
            bottom = shows_x_labels(resolved, row, col)
            leftmost = shows_y_labels(resolved, row, col)
            panel = at_cell.get((row, col))
            ax.set_xlabel(resolved.labels.x if bottom else "")
            # The facet values, when this is a facet — see base.panel_y_title.
            ax.set_ylabel(panel_y_title(resolved, panel, leftmost=leftmost))
            if style.log_x:
                ax.set_xscale("log")
            if style.log_y:
                ax.set_yscale("log")
            limits = panel_y_limits(resolved, panel) if panel else resolved.y_limits
            if limits and resolved.kind is not PlotKind.HEATMAP:
                # Data units, but never a non-positive end on a log axis
                # (base.drawable_limits) — matplotlib would ignore it, silently.
                limits = drawable_limits(limits, log=style.log_y)
                if limits:
                    ax.set_ylim(*limits)
            if resolved.x_plan:
                # A nested axis is keyed by composed leaf keys the user must
                # never see: ticks show the innermost layer, and the layers
                # above it become brackets under the axis.
                plan = resolved.x_plan
                ax.set_xticks(range(len(plan.order)))
                ax.set_xticklabels(plan.tick_labels)
                if bottom:
                    _draw_x_groups(ax, plan)
            elif is_categorical_x(resolved) and resolved.kind in (
                PlotKind.SCATTER,
                PlotKind.STRIP,
                PlotKind.SPAGHETTI,
            ):
                order = [str(v) for v in (resolved.x_order or [])]
                ax.set_xticks(range(len(order)))
                ax.set_xticklabels(order)

            # tick_params, NOT set_visible() on the Text objects: with
            # sharex/sharey, get_xticklabels() regenerates the tick list and the
            # new labels take their visibility from the axis's labelbottom
            # param — so per-Text visibility silently reverts. This must also
            # come LAST, after every set_xticklabels above (here and in the
            # _draw_* helpers), so the rule wins rather than being overwritten.
            # rotation=0 alongside it: panel content stays upright at every grid
            # size, matching the plotly path's tickangle (a figure must not read
            # differently just because it gained a facet).
            ax.tick_params(labelbottom=bottom, labelleft=leftmost)
            ax.tick_params(axis="x", rotation=0)


#: Height of one nested-group label row, as a fraction of the axes height.
X_GROUP_ROW = 0.07


def _draw_x_groups(ax, plan) -> None:
    """Label and bracket each higher x layer beneath the tick labels.

    Blended coordinates — x in DATA space (leaf positions are data positions on
    a categorical axis) and y in AXES space (a fixed distance below the axis
    regardless of the measure's range). The alternative, data coordinates for
    both, would put the brackets at a y that moves with the data.
    """
    from matplotlib.transforms import blended_transform_factory

    transform = blended_transform_factory(ax.transData, ax.transAxes)
    for group in plan.groups:
        rows_below = plan.depth - group.depth
        y = -0.10 - X_GROUP_ROW * rows_below
        ax.plot(
            [group.start - 0.35, group.end + 0.35],
            [y + 0.02, y + 0.02],
            transform=transform,
            color="#888888",
            linewidth=0.8,
            clip_on=False,
        )
        ax.text(
            group.centre,
            y,
            group.label,
            transform=transform,
            ha="center",
            va="top",
            # Relative, so it follows StyleOptions.font_size like every other label.
            fontsize="small",
            clip_on=False,
        )


#: Breathing room between the panels and the legend strip, as a fraction of the
#: figure width.
LEGEND_PAD = 0.02

#: Widest the legend strip may get, however long the level names are: past this
#: the labels have eaten the figure, and truncating the panels is worse than
#: truncating the legend.
MAX_LEGEND_FRACTION = 0.4


def _apply_legend(fig, resolved: ResolvedPlot) -> float:
    """
    Draw the legend to the right of the panels; return the width it claimed.

    The return value is a fraction of the figure width, for ``tight_layout``'s
    ``rect`` — see the call site. Zero means no legend was drawn, which is the
    answer for a single colour level (``base.shows_legend``) as well as for no
    colour at all.
    """
    if not shows_legend(resolved):
        Log.debug(
            "legend omitted: %d colour level(s) drawn for %r",
            len(legend_levels(resolved)),
            resolved.labels.color,
            layer=LAYER,
        )
        return 0.0
    # Every VISIBLE axes, not just the first: with facets, a level can be
    # absent from panel 1 and present in panel 5, and reading one panel's
    # handles would drop it from the legend of a figure that draws it.
    unique: dict[str, Any] = {}
    for ax in fig.axes:
        if not ax.get_visible():
            continue
        handles, labels = ax.get_legend_handles_labels()
        for handle, label in zip(handles, labels, strict=False):
            unique.setdefault(label, handle)
    if not unique:
        return 0.0
    # The overlay's block goes LAST, in its levels' order. matplotlib lists
    # an axes' Line2D handles before its bar containers, so gathered as-is
    # the subjects would precede the bars they sit on.
    sample_labels = [str(level) for level in sample_legend_levels(resolved)]
    if len(sample_labels) > 1:
        unique = {
            **{k: v for k, v in unique.items() if k not in sample_labels},
            **{k: unique[k] for k in sample_labels if k in unique},
        }

    # One title naming every block: the colours, the dash styles, and the
    # overlay's own colour key when its levels are listed too.
    blocks = (
        resolved.labels.color,
        resolved.labels.dash,
        resolved.labels.sample if len(sample_legend_levels(resolved)) > 1 else None,
    )
    title = " / ".join(t for t in blocks if t) or None
    legend = fig.legend(
        unique.values(),
        unique.keys(),
        title=title,
        loc="center right",
        frameon=False,
    )
    return _legend_width_fraction(fig, legend, unique.keys(), title)


def _legend_width_fraction(fig, legend, labels, title) -> float:
    """How much of the figure's width the legend needs, measured if possible."""
    figure_width = fig.get_size_inches()[0] or 1.0
    try:
        inches = legend.get_window_extent(fig.canvas.get_renderer()).width / fig.dpi
    except Exception:  # a backend without a usable renderer
        # Estimate rather than reserve nothing: a wrong-by-a-little strip still
        # keeps the legend off the panels, an unmeasured one does not.
        longest = max((len(str(text)) for text in [*labels, title or ""]), default=0)
        inches = 0.55 + 0.085 * longest
        Log.debug(
            "legend width not measurable; estimating %.2fin from %d labels",
            inches,
            len(list(labels)),
            layer=LAYER,
        )
    return min(MAX_LEGEND_FRACTION, inches / figure_width + LEGEND_PAD)
