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

from ..resolved import ResolvedPlot
from ..spec import PlotKind
from .base import (
    color_groups,
    grid_shape,
    is_categorical_x,
    legend_levels,
    palette_for,
    panel_position,
    panel_y_limits,
    shares_y_axis,
    shows_legend,
    shows_x_labels,
    shows_y_labels,
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
        n_rows, n_cols = grid_shape(resolved)
        style = resolved.spec.style
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
            if panel.key:
                ax.set_title(panel.title, fontsize=10)

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


def _draw_lines(ax, frame, resolved) -> None:
    style = resolved.spec.style
    series_column = resolved.encoding.series
    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        color = palette_for(resolved, level, index)
        if series_column and series_column in subset.columns:
            series_groups = list(subset.groupby(series_column, sort=False))
        else:
            series_groups = [(None, subset)]
        for position, (_, line_rows) in enumerate(series_groups):
            positions, _ = x_positions(line_rows[resolved.encoding.x], resolved)
            ax.plot(
                positions,
                line_rows[resolved.encoding.y].to_numpy(dtype=float),
                color=color,
                alpha=style.alpha,
                linewidth=1.4,
                # Only the first line of a colour group carries the legend entry,
                # otherwise a 200-trial plot produces a 200-entry legend.
                label=str(level) if (level is not None and position == 0) else None,
            )


def _draw_band(ax, frame, resolved) -> None:
    encoding = resolved.encoding
    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        color = palette_for(resolved, level, index)
        positions, _ = x_positions(subset[encoding.x], resolved)
        centre = subset[encoding.y].to_numpy(dtype=float)
        ax.plot(
            positions,
            centre,
            color=color,
            linewidth=1.8,
            label=str(level) if level is not None else None,
        )
        if encoding.has_error:
            ax.fill_between(
                positions,
                subset[encoding.y_low].to_numpy(dtype=float),
                subset[encoding.y_high].to_numpy(dtype=float),
                color=color,
                alpha=0.22,
                linewidth=0,
            )


def _draw_bars(ax, frame, resolved) -> None:
    encoding = resolved.encoding
    groups = color_groups(frame, resolved)
    n_groups = max(len(groups), 1)
    width = 0.8 / n_groups

    for index, (level, subset) in enumerate(groups):
        positions, ticks = x_positions(subset[encoding.x], resolved)
        offset = (index - (n_groups - 1) / 2) * width
        centre = subset[encoding.y].to_numpy(dtype=float)
        error = None
        if encoding.has_error:
            low = subset[encoding.y_low].to_numpy(dtype=float)
            high = subset[encoding.y_high].to_numpy(dtype=float)
            error = np.vstack([centre - low, high - centre])
        ax.bar(
            positions + offset,
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


def _draw_distribution(ax, frame, resolved, *, violin: bool) -> None:
    """Box or violin, dodged by colour level when one is assigned."""
    encoding = resolved.encoding
    groups = color_groups(frame, resolved)
    n_groups = max(len(groups), 1)
    width = 0.8 / n_groups
    order = resolved.x_order or sorted(frame[encoding.x].dropna().unique().tolist(), key=str)

    for index, (level, subset) in enumerate(groups):
        offset = (index - (n_groups - 1) / 2) * width
        datasets: list[np.ndarray] = []
        positions: list[float] = []
        for slot, level_value in enumerate(order):
            values = subset[subset[encoding.x].astype(str) == str(level_value)][
                encoding.y
            ].to_numpy(dtype=float)
            values = values[~np.isnan(values)]
            if values.size:
                datasets.append(values)
                positions.append(slot + offset)
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
            ax.set_xlabel(resolved.labels.x if bottom else "")
            ax.set_ylabel(resolved.labels.y if leftmost else "")
            if style.log_x:
                ax.set_xscale("log")
            if style.log_y:
                ax.set_yscale("log")
            panel = at_cell.get((row, col))
            limits = panel_y_limits(resolved, panel) if panel else resolved.y_limits
            if limits and resolved.kind is not PlotKind.HEATMAP:
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
            fontsize=9,
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

    legend = fig.legend(
        unique.values(),
        unique.keys(),
        title=resolved.labels.color,
        loc="center right",
        frameon=False,
    )
    return _legend_width_fraction(fig, legend, unique.keys(), resolved.labels.color)


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
