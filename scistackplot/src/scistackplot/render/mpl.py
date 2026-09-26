"""
matplotlib renderer — the export and pipeline path.

Returns a ``matplotlib.figure.Figure``, which is exactly what a scidb
``plot_`` endpoint must return (the framework saves and closes it). This is
also the renderer whose output the generated seaborn/matplotlib code is
expected to reproduce.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

import numpy as np
import pandas as pd
from scistacklog import Log

from ..figsize import aspect_name
from ..resolved import MPL_DASHES, ResolvedPlot
from ..spec import PlotKind
from ..table import natural_sort_key
from ..paper import PAPER, apply_paper_axes, describe_paper, figure_rc_params
from ..textsize import ResolvedSizes, resolve_sizes
from ..weights import describe_weights, sample_weight, spaghetti_weight
from ..ticklabels import (
    BRACKET_POLICY,
    TICK_POLICY,
    LabelFit,
    LabelRow,
    Measure,
    fit_labels,
    label_block,
)
from .base import (
    SAMPLE_ALPHA,
    SAMPLE_EDGE_COLOR,
    series_groups,
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
    fill_alpha,
    panel_y_limits,
    panel_y_title,
    shares_y_axis,
    shows_legend,
    ruled_bracket_depths,
    shows_x_labels,
    shows_y_labels,
    sample_dropped_reason,
    SAMPLE_LINE_POINTS_GID,
    sample_hover,
    sample_legend_levels,
    sample_palette_for,
    sample_positions,
    sample_runs,
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
        # figure. Every size is stated in points by the one owner
        # (textsize.rc_params), so nothing here depends on a relative name.
        # The paper (background, frame, ticks, grid) is stated alongside them
        # by its own owner, paper.figure_rc_params, instead of inherited.
        sizes = resolve_sizes(style)
        with plt.rc_context(figure_rc_params(sizes)):
            n_rows, n_cols = grid_shape(resolved)
            # The size the file will have, exactly (figure_file.write_figure
            # never trims). Stated in the log because the preview never shows it: a
            # figure that "came out squashed" is diagnosed here, not in the GUI.
            Log.info(
                "figure size %.2f x %.2f in (%s), %d x %d panel grid, text %s%s, %s",
                style.width,
                style.height,
                aspect_name(style.width, style.height),
                n_rows,
                n_cols,
                sizes.describe(),
                (f", marks {w}" if (w := describe_weights(resolved)) else ""),
                describe_paper(),
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
            # tight_layout is told how much room the legend took. A FIGURE legend
            # is invisible to tight_layout, so laying the axes out across the whole
            # width put the legend on top of the rightmost panels in the exported
            # PNG while the interactive plotly view kept it outside — the same
            # figure reading two different ways depending on how you looked at it.
            legend = _apply_legend(fig, resolved)
            labelled = _labelled_cells(axes, resolved, n_rows, n_cols)
            rect = _arrange(fig, labelled, resolved, legend)
            fig.tight_layout(rect=rect)
            # The data's labels outrank the legend: if, with the legend at the
            # right, the x labels still cannot fit, the legend gives the width
            # back and goes below the panels (spec/images/graph2.png).
            if legend is not None and not legend.below:
                overlap = _x_labels_crowded(fig, labelled, resolved)
                if overlap is not None:
                    legend = _legend_below(
                        fig,
                        legend,
                        f"at the right it left the x labels overlapping by {overlap:.1f}pt",
                    )
                    rect = _arrange(fig, labelled, resolved, legend)
                    fig.tight_layout(rect=rect)
            setattr(fig, LEGEND_ATTR, legend.describe() if legend is not None else None)
            # After a layout pass: the room each label has is the laid-out
            # panel width (spec/images/graph1.png, graph2.png).
            _fit_x_labels(fig, [ax for _, _, ax in labelled], resolved, rect)
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
            label=resolved.text.color_level(level) if level is not None else None,
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
    weight = spaghetti_weight(style)
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
                linewidth=weight.line_pt,
                marker="o",
                # A diameter in pt (weights.spaghetti_weight): at 1x the same
                # visual size as the scatter kinds' `s` area.
                markersize=weight.marker_pt,
                label=(
                    resolved.text.color_level(level)
                    if (level is not None and position == 0)
                    else None
                ),
            )


def _linestyle(resolved, rows):
    return MPL_DASHES[dash_style(resolved, rows)]


def _draw_lines(ax, frame, resolved) -> None:
    style = resolved.spec.style
    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        color = palette_for(resolved, level, index)
        for position, (_, line_rows) in enumerate(series_groups(subset, resolved)):
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
                label=(
                    resolved.text.color_level(level)
                    if (level is not None and position == 0)
                    else None
                ),
            )
    _dash_legend_handles(ax, resolved)


def _draw_band(ax, frame, resolved) -> None:
    """One band per series (an uncoloured grouping layer), each its own fill
    and a centre line in that series' dash style."""
    encoding = resolved.encoding
    for index, (level, subset) in enumerate(color_groups(frame, resolved)):
        color = palette_for(resolved, level, index)
        for position, (_, rows) in enumerate(series_groups(subset, resolved)):
            positions, _ = x_positions(rows[encoding.x], resolved)
            centre = rows[encoding.y].to_numpy(dtype=float)
            ax.plot(
                positions,
                centre,
                color=color,
                linewidth=1.8,
                linestyle=_linestyle(resolved, rows),
                label=(
                    resolved.text.color_level(level)
                    if (level is not None and position == 0)
                    else None
                ),
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
        ax.plot([], [], color="#555555", linewidth=2, linestyle=MPL_DASHES[resolved.dash_styles[sid]], label=resolved.text.dash_id(sid))


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
            capsize=PAPER.error_cap,
            ecolor=PAPER.error_bar_color,
            error_kw={"elinewidth": PAPER.error_bar_width, "capthick": PAPER.error_cap_width},
            color=palette_for(resolved, level, index),
            alpha=fill_alpha(PlotKind.BAR, resolved.spec.style),
            label=resolved.text.color_level(level) if level is not None else None,
        )
        if ticks is not None:
            ax.set_xticks(range(len(ticks)))
            ax.set_xticklabels([resolved.text.x_tick(t) for t in ticks])


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
                body.set_alpha(fill_alpha(PlotKind.VIOLIN, resolved.spec.style))
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
                box.set_alpha(fill_alpha(PlotKind.BOX, resolved.spec.style))
        if level is not None:
            # Boxes carry no legend handle of their own; a proxy patch does.
            ax.plot([], [], color=color, linewidth=6, label=resolved.text.color_level(level))

    ax.set_xticks(range(len(order)))
    ax.set_xticklabels([resolved.text.x_tick(v) for v in order])


def _draw_sample(ax, panel, resolved: ResolvedPlot) -> None:
    """The "Show sample" overlay: one point per row of ``panel.sample`` inside
    the mark it belongs to, joined into a line per identity when
    ``ResolvedPlot.sample_join`` says so.

    Placement is ``base.sample_positions`` — the row's own mark's position
    (its tick; on a spaghetti its line's shift) plus the identity's offset —
    so the points sit in the bar, box or line they were averaged into.
    Runs and colours are ``base.sample_runs``: the overlay's own key's
    (``ResolvedPlot.sample_color``, the legend lists the levels —
    ``_sample_legend_handles``), or each point its mark's, with a line that
    crosses mark colours drawn neutral under points painted one by one.
    Drawn after the marks, on top.
    """
    sample = getattr(panel, "sample", None)
    if sample is None or sample.empty or sample_dropped_reason(panel, resolved):
        return
    weight = sample_weight(resolved.spec.style)
    for run in sample_runs(sample, resolved):
        positions = sample_positions(run.rows, resolved, run.identity)
        order = np.argsort(positions, kind="stable")
        values = run.rows[resolved.encoding.y].to_numpy(dtype=float)[order]
        joined = resolved.sample_join and len(run.rows) > 1
        if joined and run.uniform:
            ax.plot(
                positions[order],
                values,
                color=run.line_color,
                alpha=SAMPLE_ALPHA,
                linewidth=weight.line_pt,
                marker="o",
                markersize=weight.marker_pt,
                markeredgecolor=SAMPLE_EDGE_COLOR,
                markeredgewidth=0.5,
                zorder=3,
            )
            continue
        if joined:
            # The points sit on marks of different colours: the line (the
            # run, zorder 3) in the neutral colour, the points on top of it
            # each in its own mark's colour (gid: part of the line's run).
            ax.plot(
                positions[order],
                values,
                color=run.line_color,
                alpha=SAMPLE_ALPHA,
                linewidth=weight.line_pt,
                zorder=3,
            )
        ax.scatter(
            positions[order],
            values,
            s=weight.marker_area,
            color=[run.point_colors[i] for i in order],
            alpha=SAMPLE_ALPHA,
            edgecolors=SAMPLE_EDGE_COLOR,
            linewidths=0.5,
            zorder=3.1 if joined else 3,
            gid=SAMPLE_LINE_POINTS_GID if joined else None,
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
            label=resolved.text.sample_level(level),
        )


def _draw_heatmap(ax, frame, resolved) -> None:
    matrix = frame[resolved.encoding.z].iloc[0]
    image = ax.imshow(np.asarray(matrix, dtype=float), aspect="auto", origin="lower")
    ax.figure.colorbar(image, ax=ax, fraction=0.046, pad=0.04)


def _apply_axes_cosmetics(fig, axes, resolved: ResolvedPlot, n_rows, n_cols, at_cell) -> None:
    style = resolved.spec.style
    # matplotlib has ONE rc key for both axis titles (axes.labelsize = the x
    # title's size), so the y title is sized here.
    y_label_pt = resolve_sizes(style).y_label
    for row in range(n_rows):
        for col in range(n_cols):
            ax = axes[row][col]
            if not ax.get_visible():
                continue
            # Explicitly, not only through the rc: ticks are made lazily, and a
            # save outside the rc_context would build them from whatever rc is
            # in force then (paper.apply_paper_axes).
            apply_paper_axes(ax)
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
            ax.set_ylabel(
                panel_y_title(resolved, panel, leftmost=leftmost), fontsize=y_label_pt
            )
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
                # The brackets are drawn after layout (`_fit_x_labels`): they
                # sit a measured distance below the FITTED tick labels.
            elif is_categorical_x(resolved) and resolved.kind in (
                PlotKind.SCATTER,
                PlotKind.STRIP,
                PlotKind.SPAGHETTI,
            ):
                order = list(resolved.x_order or [])
                ax.set_xticks(range(len(order)))
                ax.set_xticklabels([resolved.text.x_tick(v) for v in order])

            # tick_params, NOT set_visible() on the Text objects: with
            # sharex/sharey, get_xticklabels() regenerates the tick list and the
            # new labels take their visibility from the axis's labelbottom
            # param — so per-Text visibility silently reverts. This must also
            # come LAST, after every set_xticklabels above (here and in the
            # _draw_* helpers), so the rule wins rather than being overwritten.
            # rotation=0 alongside it: upright is the starting point. Whether a
            # label must shrink, wrap, rotate or thin is decided once for the
            # whole figure after layout (`_fit_x_labels`), so every panel still
            # reads the same way.
            ax.tick_params(labelbottom=bottom, labelleft=leftmost)
            ax.tick_params(axis="x", rotation=0)


# ---------------------------------------------------------------------------
# Fitting the x labels — scistackplot.ticklabels decides, this measures and
# applies. Runs after a first layout pass, because the room a label has is the
# laid-out axes width, which nothing knows before then.

#: Clear space below the tick labels before the first bracket row, and between
#: bracket rows, in points. Points, not a fraction of the axes: a fraction of
#: a short panel is smaller than the tick labels, which is how brackets came to
#: sit on top of them (spec/images/graph2.png).
X_GROUP_GAP_PT = 4.0

#: Between a bracket's rule and its label, in points.
X_GROUP_RULE_GAP_PT = 2.0

#: Brackets overhang their outer leaves by this much, in leaf widths.
X_GROUP_OVERHANG = 0.35

#: Attribute on a rendered Figure holding ``{"ticks": LabelFit, "brackets":
#: LabelFit | None}`` — what the x-label fit decided for it.
LABEL_FIT_ATTR = "scistackplot_label_fit"


@dataclass
class _XTicks:
    """One labelled axes' ticks, as drawn BEFORE fitting — every refit starts
    from these, never from an already-stripped or thinned set."""

    ax: Any
    locs: list[float]
    labels: list[str]


def _pt(fig, pixels: float) -> float:
    return float(pixels) * 72.0 / fig.dpi


def _text_measure(fig) -> Measure:
    """``ticklabels.Measure`` from the figure's own renderer: the real extents
    of the font the file is drawn in, not an estimate."""
    from matplotlib.font_manager import FontProperties

    renderer = fig.canvas.get_renderer()
    line_heights: dict[float, float] = {}

    def measure(text: str, font_pt: float) -> tuple[float, float]:
        prop = FontProperties(size=font_pt)
        width, height, _descent = renderer.get_text_width_height_descent(
            text, prop, ismath=False
        )
        # A drawn line is never shorter than "lp" — matplotlib's Text layout
        # sizes every line by it, so "BL" (no descender) is drawn as tall as
        # "lp". Measured by its own glyphs, a 90-degree "BL" looked thinner
        # than the strip it is drawn in, and neighbours were packed into
        # each other (graph1: BL / MID24).
        if font_pt not in line_heights:
            line_heights[font_pt] = renderer.get_text_width_height_descent(
                "lp", prop, ismath=False
            )[1]
        return _pt(fig, width), _pt(fig, max(height, line_heights[font_pt]))

    return measure


def _labelled_cells(axes, resolved: ResolvedPlot, n_rows, n_cols) -> list[tuple[int, int, Any]]:
    """``(row, col, ax)`` of every visible panel that carries x labels
    (``base.shows_x_labels`` — the same rule the cosmetics pass used)."""
    return [
        (row, col, axes[row][col])
        for row in range(n_rows)
        for col in range(n_cols)
        if axes[row][col].get_visible() and shows_x_labels(resolved, row, col)
    ]


def _legend_duplicate_depths(resolved: ResolvedPlot) -> set[int]:
    """The x layers (``x_layers`` indices) whose labels the legend already
    gives — only when ``StyleOptions.hide_legend_ticks`` asks (opt-in)."""
    if not resolved.spec.style.hide_legend_ticks or not shows_legend(resolved):
        return set()
    return {
        depth
        for depth, name in enumerate(resolved.x_layers)
        if name == resolved.color_factor
    }


def _collect_x_ticks(
    labelled: list[Any], resolved: ResolvedPlot, *, quiet: bool = False
) -> list[_XTicks]:
    """The categorical tick labels of each labelled axes. A numeric x axis is
    left to matplotlib's locator, which already spaces its ticks."""
    if resolved.kind is PlotKind.HEATMAP or not (
        resolved.x_plan or is_categorical_x(resolved)
    ):
        return []
    hide = (len(resolved.x_layers) - 1) in _legend_duplicate_depths(resolved)
    if hide and not quiet:
        Log.info(
            "x tick labels hidden: %r is the colour layer and the legend lists it",
            resolved.color_factor,
            layer=LAYER,
        )
    collected = []
    for ax in labelled:
        # Every tick, unfiltered: a FixedFormatter labels by tick INDEX, so
        # dropping one would shift every label after it.
        locs = [float(v) for v in ax.get_xticks()]
        labels = [
            str(text) for text in ax.xaxis.get_major_formatter().format_ticks(locs)
        ]
        collected.append(_XTicks(ax, locs, [""] * len(labels) if hide else labels))
    return collected


def _positions_pt(fig, ax, xs) -> list[float]:
    """Data x positions on ``ax`` as points from the figure's left edge."""
    if not len(xs):
        return []
    # get_xlim() first: autoscaling is deferred, and transData reads the view
    # limits WITHOUT bringing them up to date. Measured on a stale range, the
    # labels were fitted to room they did not have (graph1 overlapped) or
    # denied room they had (a 20pt label shrank to 19pt).
    ax.get_xlim()
    pixels = ax.transData.transform([(float(x), 0.0) for x in xs])[:, 0]
    return [_pt(fig, value) for value in pixels]


def _fit_x_ticks(fig, ticks: list[_XTicks], resolved: ResolvedPlot):
    """One :class:`LabelFit` for every labelled panel, measured on the
    current layout. Returns ``(fit, rows)``; rows are kept for the log."""
    plan = resolved.x_plan
    # Thinning keeps each innermost bracket's first and last label.
    spans = (
        [(g.start, g.end) for g in plan.groups if g.depth == plan.depth - 1]
        if plan and plan.groups
        else []
    )
    # Spans index `plan.order`; only meaningful when the ticks ARE that order.
    rows = [
        LabelRow(
            item.labels,
            _positions_pt(fig, item.ax, item.locs),
            groups=spans if plan and len(item.locs) == len(plan.order) else [],
        )
        for item in ticks
    ]
    for item, row in zip(ticks, rows):
        Log.debug(
            "x tick fit input: xlim=%s axes width %.1fpt, %d tick(s) at %s…%spt",
            tuple(round(v, 3) for v in item.ax.get_xlim()),
            _pt(fig, item.ax.get_window_extent(fig.canvas.get_renderer()).width),
            len(row.positions),
            round(row.positions[0], 1) if row.positions else "-",
            round(row.positions[-1], 1) if row.positions else "-",
            layer=LAYER,
        )
    style = resolved.spec.style
    sizes = resolve_sizes(style)
    policy = replace(
        TICK_POLICY,
        pin_rotation=style.tick_rotation,
        pin_font_pt=sizes.x_ticks if sizes.is_pinned("x_ticks") else None,
        pin_every=style.tick_every,
    )
    fit = fit_labels(rows, sizes.x_ticks, _text_measure(fig), policy)
    return fit, rows


def _apply_x_fit(ticks: list[_XTicks], fit: LabelFit) -> None:
    """Draw the decision. A rotated label is right-anchored at its tick with
    its centre line through it — the geometry ``ticklabels`` fitted."""
    rotated = fit.rotation != 0
    for item, texts in zip(ticks, fit.rows):
        item.ax.set_xticks(item.locs)
        item.ax.set_xticklabels(
            list(texts),
            fontsize=fit.font_pt,
            rotation=fit.rotation,
            ha="right" if rotated else "center",
            va="center" if rotated else "top",
            rotation_mode="anchor" if rotated else "default",
            multialignment="center",
        )


def _same_decision(a: LabelFit, b: LabelFit) -> bool:
    return (a.rows, a.font_pt, a.rotation, a.every) == (
        b.rows,
        b.font_pt,
        b.rotation,
        b.every,
    )


def _log_fit(what: str, fit: LabelFit, rows: list[LabelRow], measure) -> None:
    """INFO: what was decided and why; WARN when it still overlaps."""
    gaps: list[float] = []
    for row in rows:
        shown = [at for at, label in zip(row.positions, row.labels) if label]
        gaps.extend(b - a for a, b in zip(shown, shown[1:]))
    widest = max(
        (
            label_block(label, fit.font_pt, measure)[0]
            for row in rows
            for label in row.labels
            if label
        ),
        default=0.0,
    )
    Log.info(
        "%s: %d panel row(s), %d position(s), narrowest slot %.1fpt, widest "
        "label %.1fpt at the fitted font -> %s",
        what,
        len(rows),
        max((len(row.labels) for row in rows), default=0),
        min(gaps, default=0.0),
        widest,
        fit.describe(),
        layer=LAYER,
    )
    if not fit.fits:
        Log.warn(
            "%s still overlap by %.1fpt after every allowed step — widen the "
            "figure, lower the font size or show fewer positions (names are "
            "never thinned, only numbered labels are)",
            what,
            fit.worst_overlap_pt,
            layer=LAYER,
        )


def _tick_label_depth_pt(fig, ax, renderer) -> float:
    """How far below the axes' bottom edge its tick labels reach, in points.
    Measured, so a rotated or wrapped label pushes the brackets down with it."""
    bottoms = [
        text.get_window_extent(renderer).y0
        for text in ax.xaxis.get_ticklabels()
        if text.get_visible() and text.get_text()
    ]
    if not bottoms:
        return 0.0
    return max(0.0, _pt(fig, ax.get_window_extent(renderer).y0 - min(bottoms)))


def _draw_x_groups(fig, ticks: list[_XTicks], resolved: ResolvedPlot) -> LabelFit | None:
    """Label and bracket each higher x layer beneath the tick labels.

    x in DATA space (leaf positions are data positions on a categorical axis);
    y a fixed number of POINTS below the measured bottom of the tick labels,
    one row per layer. The labels are fitted like the ticks
    (``ticklabels.BRACKET_POLICY``: shrink and wrap, never rotate or thin). The
    axis title, when there is one, is pushed below the last row.
    """
    from matplotlib.lines import Line2D
    from matplotlib.transforms import blended_transform_factory, offset_copy

    plan = resolved.x_plan
    if not plan or not plan.groups or not ticks:
        return None
    hidden = _legend_duplicate_depths(resolved)
    fig.draw_without_rendering()
    renderer = fig.canvas.get_renderer()
    measure = _text_measure(fig)
    depths = sorted({group.depth for group in plan.groups})
    by_depth = {d: [g for g in plan.groups if g.depth == d] for d in depths}

    rows = [
        LabelRow(
            ["" if depth in hidden else g.label for g in by_depth[depth]],
            _positions_pt(fig, item.ax, [g.centre for g in by_depth[depth]]),
        )
        for item in ticks
        for depth in depths
    ]
    sizes = resolve_sizes(resolved.spec.style)
    policy = replace(
        BRACKET_POLICY,
        pin_font_pt=sizes.groups if sizes.is_pinned("groups") else None,
    )
    fit = fit_labels(rows, sizes.groups, measure, policy)
    _log_fit("x bracket labels", fit, rows, measure)
    row_height = max(
        (
            label_block(text, fit.font_pt, measure)[1]
            for row in fit.rows
            for text in row
            if text
        ),
        default=0.0,
    )
    step = row_height + X_GROUP_RULE_GAP_PT + X_GROUP_GAP_PT

    fitted = iter(fit.rows)
    for item in ticks:
        ax = item.ax
        below = _tick_label_depth_pt(fig, ax, renderer)
        blended = blended_transform_factory(ax.transData, ax.transAxes)
        texts_by_depth = {depth: next(fitted) for depth in depths}
        shown = {depth for depth, texts in texts_by_depth.items() if any(texts)}
        if any(item.labels):
            shown.add(plan.depth)
        ruled = ruled_bracket_depths(plan.depth, shown)
        for depth in depths:
            texts = texts_by_depth[depth]
            if depth in shown and depth not in ruled:
                Log.debug(
                    "x bracket row %r: nothing shown above it, rules omitted",
                    resolved.x_layers[depth] if depth < len(resolved.x_layers) else depth,
                    layer=LAYER,
                )
            # Deeper layers sit closer to the axis; depth 0 is furthest below.
            top = below + X_GROUP_GAP_PT + (plan.depth - depth - 1) * step
            rule = offset_copy(blended, fig=fig, y=-top, units="points")
            under = offset_copy(
                blended, fig=fig, y=-(top + X_GROUP_RULE_GAP_PT), units="points"
            )
            if not any(texts):
                # A row with no label shown (hide_legend_ticks: the legend
                # names it) draws no rules either: a bare line names nothing.
                Log.debug(
                    "x bracket row %r: no label shown, rules omitted",
                    resolved.x_layers[depth] if depth < len(resolved.x_layers) else depth,
                    layer=LAYER,
                )
            for group, text in zip(by_depth[depth], texts):
                if not text:
                    continue
                if depth in ruled:
                    # add_artist, not plot(): a plotted line would join the data
                    # limits and could move the ticks the labels were fitted to.
                    ax.add_artist(
                        Line2D(
                            [group.start - X_GROUP_OVERHANG, group.end + X_GROUP_OVERHANG],
                            [0.0, 0.0],
                            transform=rule,
                            color="#888888",
                            linewidth=0.8,
                            clip_on=False,
                        )
                    )
                ax.text(
                    group.centre,
                    0.0,
                    text,
                    transform=under,
                    ha="center",
                    va="top",
                    multialignment="center",
                    fontsize=fit.font_pt,
                    clip_on=False,
                )
        ax.xaxis.labelpad = X_GROUP_GAP_PT + plan.depth * step
    return fit


def _shared_x_title(
    fig, labelled: list[tuple[int, int, Any]], resolved: ResolvedPlot, *, y: float = 0.0
) -> float:
    """One x title under the whole figure when several COLUMNS would each
    repeat it; returns the fraction of the figure height it reserves.

    Each column's copy was centred under its own panel, so a title wider than
    a panel ran into its neighbour's (spec/images/graph2.png). A single
    column keeps the ordinary per-axes title. ``y`` is where it sits (a
    figure fraction): above a legend that is below the panels. Safe to call
    again with a new ``y`` — matplotlib keeps one supxlabel per figure.
    """
    title = resolved.labels.x
    if not title or len({col for _, col, _ in labelled}) < 2:
        return 0.0
    for _, _, ax in labelled:
        ax.set_xlabel("")
    text = fig.supxlabel(
        title, y=y + 0.01, va="bottom", fontsize=resolve_sizes(resolved.spec.style).x_label
    )
    height = text.get_window_extent(fig.canvas.get_renderer()).height
    figure_height = fig.get_size_inches()[1] * fig.dpi or 1.0
    reserved = (height + 2 * X_GROUP_GAP_PT * fig.dpi / 72.0) / figure_height
    Log.debug("x title %r drawn once for %d columns", title, len(labelled), layer=LAYER)
    return min(0.25, reserved)


def _fit_x_labels(fig, labelled: list[Any], resolved: ResolvedPlot, rect) -> None:
    """Fit the tick labels, lay out again, confirm, then draw the brackets.

    Two measurements, because fitting changes the layout it was measured on:
    a rotated label deepens the bottom margin, which can narrow the panels a
    little. The second pass refits on the new widths; if the decision holds,
    that is the answer.
    """
    ticks = _collect_x_ticks(labelled, resolved)
    if not ticks:
        return
    fit, rows = _fit_x_ticks(fig, ticks, resolved)
    _apply_x_fit(ticks, fit)
    fig.tight_layout(rect=rect)
    refit, rows = _fit_x_ticks(fig, ticks, resolved)
    if not _same_decision(fit, refit):
        Log.debug("x tick labels refitted after layout: %s", refit.describe(), layer=LAYER)
        _apply_x_fit(ticks, refit)
        fig.tight_layout(rect=rect)
        fit = refit
    _log_fit("x tick labels", fit, rows, _text_measure(fig))
    brackets = _draw_x_groups(fig, ticks, resolved)
    if brackets is not None:
        fig.tight_layout(rect=rect)
    # The decisions travel with the Figure, so a caller (a test today, the
    # GUI's "labels still overlap" notice later) can read what was decided
    # instead of re-deriving it from the drawing.
    setattr(fig, LABEL_FIT_ATTR, {"ticks": fit, "brackets": brackets})
    if fit.fits:
        _verify_x_ticks(fig, ticks, fit)


def _verify_x_ticks(fig, ticks: list[_XTicks], fit: LabelFit) -> None:
    """Check the DRAWN labels against the decision, and WARN if they collide.

    The fit is only as good as the positions and extents it was given; a
    wrong measurement (a stale axis range, a font the renderer substitutes)
    would otherwise surface only as an unreadable PNG. Upright and 90 degree
    labels only, where a label's bounding box is the text itself.
    """
    if fit.rotation not in (0, 90):
        return
    fig.draw_without_rendering()
    renderer = fig.canvas.get_renderer()
    for item in ticks:
        drawn = sorted(
            (
                text.get_window_extent(renderer)
                for text in item.ax.xaxis.get_ticklabels()
                if text.get_visible() and text.get_text()
            ),
            key=lambda box: box.x0,
        )
        worst = max(
            (_pt(fig, a.x1 - b.x0) for a, b in zip(drawn, drawn[1:])), default=0.0
        )
        if worst > 0.5:
            Log.warn(
                "x tick labels were fitted (%s) but the drawn labels overlap by "
                "%.1fpt — the measurement disagrees with the drawing",
                fit.describe(),
                worst,
                layer=LAYER,
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
    sample_labels = [resolved.text.sample_level(level) for level in sample_legend_levels(resolved)]
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
#: Breathing room between the panels and the legend strip, as a fraction of the
#: figure width.
LEGEND_PAD = 0.02

#: Share of the figure width a legend at the right may take. Past it the
#: legend is narrowed (title wrapped, shorter line samples, smaller text) and,
#: if still too wide, moved below the panels. The old cap was 0.4, and at it a
#: 14-subject legend took half of spec/images/graph2.png.
LEGEND_BUDGET = 0.30

#: Widest a legend BELOW the panels may be, as a share of the figure width;
#: columns are added until it is.
LEGEND_BELOW_WIDTH = 0.95

#: Tallest a legend below the panels should be, as a share of the figure
#: height, before its text shrinks (and past which the log WARNs).
LEGEND_BELOW_HEIGHT = 0.4

#: matplotlib's defaults, and the shortened line samples of the narrowing step.
LEGEND_HANDLES = {"handlelength": 2.0, "handletextpad": 0.8}
LEGEND_SHORT_HANDLES = {"handlelength": 1.0, "handletextpad": 0.4}

#: Attribute on a rendered Figure describing where its legend went and why.
LEGEND_ATTR = "scistackplot_legend"


@dataclass
class _Legend:
    """The drawn legend and what it reserves. One per figure."""

    artist: Any
    handles: list[Any]
    labels: list[str]
    #: The title's blocks ("session", "subject"), joined per placement.
    blocks: list[str]
    font_pt: float
    below: bool = False
    #: Fractions of the figure the layout keeps free for it.
    width_frac: float = 0.0
    height_frac: float = 0.0
    columns: int = 1
    steps: list[str] = field(default_factory=list)
    reason: str = ""
    #: The title's size: fixed (``text.legend_title``) or the entries' size.
    title_pt: float | None = None
    #: The resolved text sizes the legend was fitted with (textsize owner).
    text: ResolvedSizes | None = None

    def describe(self) -> dict:
        return {
            "below": self.below,
            "font_pt": self.font_pt,
            "title_font_pt": self.title_pt if self.title_pt is not None else self.font_pt,
            "columns": self.columns,
            "width_frac": round(self.width_frac, 3),
            "height_frac": round(self.height_frac, 3),
            "steps": list(self.steps),
            "reason": self.reason,
        }


def _legend_entries(fig, resolved: ResolvedPlot):
    """``(handles, labels, title blocks)`` for the figure legend, or None."""
    if not shows_legend(resolved):
        Log.debug(
            "legend omitted: %d colour level(s) drawn for %r",
            len(legend_levels(resolved)),
            resolved.labels.color,
            layer=LAYER,
        )
        return None
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
        return None
    # The overlay's block goes LAST, in its levels' order. matplotlib lists
    # an axes' Line2D handles before its bar containers, so gathered as-is
    # the subjects would precede the bars they sit on.
    sample_labels = [resolved.text.sample_level(level) for level in sample_legend_levels(resolved)]
    if len(sample_labels) > 1:
        unique = {
            **{k: v for k, v in unique.items() if k not in sample_labels},
            **{k: unique[k] for k in sample_labels if k in unique},
        }
    # One title naming every block: the colours, the dash styles, and the
    # overlay's own colour key when its levels are listed too.
    blocks = [
        t
        for t in (
            resolved.labels.color,
            resolved.labels.dash,
            resolved.labels.sample if len(sample_labels) > 1 else None,
        )
        if t
    ]
    return list(unique.values()), list(unique.keys()), blocks


def _legend_title(blocks: list[str], wrapped: bool) -> str | None:
    """``"a / b"``, or one block per line (``"a /\\nb"``) when wrapped — the
    title is usually the legend's widest line."""
    if not blocks:
        return None
    return " /\n".join(blocks) if wrapped else " / ".join(blocks)


def _draw_legend(
    fig, handles, labels, title, font_pt, *, title_pt=None, below=False, columns=1,
    handle_style=None,
):
    style = handle_style or LEGEND_HANDLES
    kwargs = (
        {"loc": "lower center", "bbox_to_anchor": (0.5, 0.0), "ncol": columns}
        if below
        else {"loc": "center right"}
    )
    return fig.legend(
        handles,
        labels,
        title=title,
        frameon=False,
        fontsize=font_pt,
        # Unfixed, the title follows the entries through every shrink.
        title_fontsize=title_pt if title_pt is not None else font_pt,
        **style,
        **kwargs,
    )


def _legend_size(fig, artist, labels, title) -> tuple[float, float]:
    """``(width, height)`` of a drawn legend as fractions of the figure."""
    width_px = fig.get_size_inches()[0] * fig.dpi or 1.0
    height_px = fig.get_size_inches()[1] * fig.dpi or 1.0
    try:
        box = artist.get_window_extent(fig.canvas.get_renderer())
        return box.width / width_px, box.height / height_px
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
        return inches * fig.dpi / width_px, 0.0


def _legend_ladder(sizes: ResolvedSizes) -> list[float]:
    """The entry sizes the legend fit may try, largest first.

    A FIXED legend size (``text.legend``) is the whole ladder: the fit may
    still wrap the title, shorten the handles or move the legend below, but
    never shrinks what the user set. Otherwise it steps down to the tick
    labels' floor (``LabelPolicy.font_floor``).
    """
    base = sizes.legend
    if sizes.is_pinned("legend"):
        return [base]
    floor = TICK_POLICY.font_floor(base)
    ladder = [base]
    while ladder[-1] - TICK_POLICY.font_step_pt > floor + 1e-6:
        ladder.append(round(ladder[-1] - TICK_POLICY.font_step_pt, 3))
    if floor < base - 1e-6:
        ladder.append(floor)
    return ladder


def _apply_legend(fig, resolved: ResolvedPlot) -> _Legend | None:
    """Draw the legend to the right of the panels, narrowed to fit
    :data:`LEGEND_BUDGET`; below the panels when it cannot be.

    The narrowing steps, least destructive first: wrap the title at " / ",
    shorten the line samples, shrink the text (never below the tick labels'
    floor, ``LabelPolicy.font_floor``, and never when the size is fixed —
    :func:`_legend_ladder`). None when there is no legend
    (``base.shows_legend``).
    """
    entries = _legend_entries(fig, resolved)
    if entries is None:
        return None
    handles, labels, blocks = entries
    text = resolve_sizes(resolved.spec.style)
    sizes = _legend_ladder(text)
    base = sizes[0]

    # (step name, wrapped title, handle style, font) in the order tried.
    attempts = [("as_is", False, LEGEND_HANDLES, base)]
    if len(blocks) > 1:
        attempts.append(("wrap_title", True, LEGEND_HANDLES, base))
    wrap = len(blocks) > 1
    attempts.append(("short_handles", wrap, LEGEND_SHORT_HANDLES, base))
    attempts.extend(("shrink", wrap, LEGEND_SHORT_HANDLES, size) for size in sizes[1:])

    steps: list[str] = []
    width = 1.0
    for step, wrapped, style, size in attempts:
        title = _legend_title(blocks, wrapped)
        title_pt = text.legend_title_for(size)
        artist = _draw_legend(
            fig, handles, labels, title, size, title_pt=title_pt, handle_style=style
        )
        width, _ = _legend_size(fig, artist, labels, title)
        width += LEGEND_PAD
        if step != "as_is" and step not in steps:
            steps.append(step)
        Log.debug(
            "legend at the right, %s: %.0f%% of the width (budget %.0f%%)",
            step, 100 * width, 100 * LEGEND_BUDGET, layer=LAYER,
        )
        if width <= LEGEND_BUDGET:
            legend = _Legend(
                artist, handles, labels, blocks, size,
                width_frac=width, steps=steps, title_pt=title_pt, text=text,
            )
            Log.info(
                "legend at the right: %d entr(ies), %.0f%% of the width, font %gpt, "
                "title %gpt%s",
                len(labels), 100 * width, size, title_pt,
                f" ({', '.join(steps)})" if steps else "",
                layer=LAYER,
            )
            return legend
        artist.remove()

    return _legend_below(
        fig,
        _Legend(None, handles, labels, blocks, base, steps=steps, text=text),
        f"{100 * width:.0f}% of the width even narrowed (budget {100 * LEGEND_BUDGET:.0f}%)"
        + (" — its font is fixed, so it was not shrunk" if text.is_pinned("legend") else ""),
    )


def _legend_below(fig, legend: _Legend, reason: str) -> _Legend:
    """Move the legend below the panels.

    As many columns as fit :data:`LEGEND_BELOW_WIDTH` (fewest rows), at the
    full font; if that is still taller than :data:`LEGEND_BELOW_HEIGHT`, the
    text shrinks towards the tick labels' floor (unless it is fixed). The
    height it reserves is the height it has — never capped, since a capped
    reservation is a legend drawn over the panels — and a figure too small
    for it is said so.
    """
    if legend.artist is not None:
        legend.artist.remove()
    text = legend.text
    sizes = _legend_ladder(text)
    base = sizes[0]
    title = _legend_title(legend.blocks, False)

    artist = None
    for size in sizes:
        title_pt = text.legend_title_for(size)
        for columns in range(len(legend.labels), 0, -1):
            if artist is not None:
                artist.remove()
            artist = _draw_legend(
                fig, legend.handles, legend.labels, title, size,
                title_pt=title_pt, below=True, columns=columns,
            )
            width, height = _legend_size(fig, artist, legend.labels, title)
            if width <= LEGEND_BELOW_WIDTH or columns == 1:
                break
        if height <= LEGEND_BELOW_HEIGHT:
            break
    pad = X_GROUP_GAP_PT / 72.0 / (fig.get_size_inches()[1] or 1.0)
    moved = _Legend(
        artist,
        legend.handles,
        legend.labels,
        legend.blocks,
        size,
        below=True,
        height_frac=height + pad,
        columns=columns,
        steps=[*legend.steps, "below", *(["shrink"] if size < base else [])],
        reason=reason,
        title_pt=title_pt,
        text=text,
    )
    Log.info(
        "legend moved below the panels: %s — %d entr(ies) in %d column(s), "
        "font %gpt, title %gpt, %.0f%% of the height",
        reason, len(legend.labels), columns, size, title_pt, 100 * moved.height_frac,
        layer=LAYER,
    )
    if height > LEGEND_BELOW_HEIGHT:
        Log.warn(
            "the legend needs %.0f%% of the figure height even at %gpt — the "
            "figure is too small for %d entries; enlarge it, lower the legend "
            "font, or untick Show sample > Show in legend",
            100 * height, size, len(legend.labels),
            layer=LAYER,
        )
    return moved


def _arrange(fig, labelled, resolved: ResolvedPlot, legend: _Legend | None) -> tuple:
    """The ``tight_layout`` rect leaving room for the legend and a shared x
    title (which sits just above a legend that is below the panels)."""
    below = legend.height_frac if legend is not None and legend.below else 0.0
    title = _shared_x_title(fig, labelled, resolved, y=below)
    right = legend.width_frac if legend is not None and not legend.below else 0.0
    return (0.0, below + title, 1.0 - right, 1.0)


def _x_labels_crowded(fig, labelled, resolved: ResolvedPlot) -> float | None:
    """How far the x tick labels would still overlap on the current layout,
    or None when they fit. A dry run — nothing on the axes is changed."""
    ticks = _collect_x_ticks([ax for _, _, ax in labelled], resolved, quiet=True)
    if not ticks:
        return None
    fit, _ = _fit_x_ticks(fig, ticks, resolved)
    return None if fit.fits else fit.worst_overlap_pt


# ---------------------------------------------------------------------------
# The decisions, for a renderer that cannot measure text (the plotly preview)


def layout_decisions(
    resolved: ResolvedPlot,
    *,
    width_in: float | None = None,
    height_in: float | None = None,
) -> dict:
    """The export's label and legend decisions for ``resolved`` at a size.

    Lays the figure out exactly as the export does, at ``width_in x
    height_in`` (the spec's own size when omitted), and reads back what
    was decided: ``{"ticks": LabelFit | None, "brackets": LabelFit | None,
    "legend": dict | None, "width_in", "height_in"}``. The plotly preview
    applies these rather than estimating text of its own, so what the preview
    shows and what the file gets cannot come from two different rules.
    """
    import matplotlib.pyplot as plt

    style = resolved.spec.style
    width = float(width_in or style.width)
    height = float(height_in or style.height)
    target = resolved
    if (width, height) != (style.width, style.height):
        target = replace(
            resolved,
            spec=replace(resolved.spec, style=replace(style, width=width, height=height)),
        )
    with Log.timer("layout_decisions", layer=LAYER, extra=f"{width:.2f}x{height:.2f}in"):
        fig = render(target)
        try:
            fits = getattr(fig, LABEL_FIT_ATTR, None) or {}
            return {
                "ticks": fits.get("ticks"),
                "brackets": fits.get("brackets"),
                "legend": getattr(fig, LEGEND_ATTR, None),
                "width_in": width,
                "height_in": height,
            }
        finally:
            plt.close(fig)
