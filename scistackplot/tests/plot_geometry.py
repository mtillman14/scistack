"""
What a rendered figure DRAWS, read back as data — the test-side twin of
``render.base``.

Both renderers place marks by the same arithmetic, and the argument for a
plot is only as good as what lands on the canvas. Pixel baselines would pin
that but break on every matplotlib release; the geometry — which x position
each mark sits at, how tall it is, where its error bar ends, what the ticks
say — is what a reader's eye takes in, and it is stable. This module reads it
back from a matplotlib Figure and from a plotly payload in ONE shape, so a
test can say "these two figures draw the same thing" and mean it.

Every x is reported as the LABEL of the tick the mark sits on (the resolved
``x_order`` entry) plus the mark's offset from that tick, so a category axis
(plotly strings) and an index axis (matplotlib positions) read alike.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from scistackplot import PlotKind
from scistackplot.resolved import ResolvedPlot
from scistackplot.xaxis import is_spacer


@dataclass(frozen=True, order=True)
class Mark:
    """One drawn mark, or one drawn point of a mark.

    ``x`` is the tick label the mark sits on; ``offset`` its shift from that
    tick (0 for a bar on its tick, the series shift for a spaghetti point);
    ``y`` the height / value / median; ``low`` / ``high`` the error-bar ends
    (bars) or the box edges (boxes), ``None`` where the kind has none;
    ``width`` the drawn width where the kind has one.
    """

    panel: int
    x: str
    offset: float
    y: float
    low: float | None = None
    high: float | None = None
    width: float | None = None


def _r(value: float, digits: int = 6) -> float:
    return round(float(value), digits)


def _label_at(resolved: ResolvedPlot, position: float) -> tuple[str, float]:
    order = [str(v) for v in (resolved.x_order or [])]
    index = int(round(float(position)))
    if not order:
        return str(index), _r(position - index)
    if not 0 <= index < len(order):
        raise AssertionError(f"mark at {position} is off the axis {order}")
    return order[index], _r(position - index)


# --- matplotlib ----------------------------------------------------------------


def mpl_marks(figure, resolved: ResolvedPlot) -> list[Mark]:
    """The marks a matplotlib figure drew, one entry per bar / box / violin /
    point, sorted."""
    kind = resolved.kind
    marks: list[Mark] = []
    axes = [ax for ax in figure.axes if ax.get_visible()]
    for panel, ax in enumerate(axes):
        if kind is PlotKind.BAR:
            marks.extend(_mpl_bars(ax, panel, resolved))
        elif kind in (PlotKind.BOX, PlotKind.VIOLIN):
            marks.extend(_mpl_boxes(ax, panel, resolved, violin=kind is PlotKind.VIOLIN))
        elif kind in (PlotKind.SCATTER, PlotKind.STRIP):
            marks.extend(_mpl_points(ax, panel, resolved))
        elif kind is PlotKind.SPAGHETTI:
            marks.extend(_mpl_spaghetti(ax, panel, resolved))
        else:  # pragma: no cover - the categorical kinds are what this is for
            raise NotImplementedError(kind)
    return sorted(marks)


def _mpl_bars(ax, panel, resolved) -> list[Mark]:
    from matplotlib.collections import LineCollection
    from matplotlib.patches import Rectangle

    # The vertical error segments, keyed by their x.
    ends: dict[float, tuple[float, float]] = {}
    for collection in ax.collections:
        if not isinstance(collection, LineCollection):
            continue
        for segment in collection.get_segments():
            (x0, y0), (x1, y1) = segment[0], segment[-1]
            if abs(x0 - x1) < 1e-9:
                ends[_r(x0)] = (_r(min(y0, y1)), _r(max(y0, y1)))
    marks = []
    for patch in ax.patches:
        if not isinstance(patch, Rectangle) or patch.get_width() == 0:
            continue
        centre = patch.get_x() + patch.get_width() / 2
        label, offset = _label_at(resolved, centre)
        low, high = ends.get(_r(centre), (None, None))
        marks.append(
            Mark(panel, label, offset, _r(patch.get_height()), low, high, _r(patch.get_width()))
        )
    return marks


def _mpl_boxes(ax, panel, resolved, *, violin: bool) -> list[Mark]:
    from matplotlib.collections import PolyCollection
    from matplotlib.patches import PathPatch

    marks = []
    if violin:
        bodies = [c for c in ax.collections if isinstance(c, PolyCollection)]
        for body in bodies:
            extents = body.get_paths()[0].get_extents()
            centre = (extents.x0 + extents.x1) / 2
            label, offset = _label_at(resolved, centre)
            marks.append(
                Mark(panel, label, offset, _r((extents.y0 + extents.y1) / 2),
                     _r(extents.y0), _r(extents.y1), _r(extents.x1 - extents.x0))
            )
        return marks
    for patch in ax.patches:
        if not isinstance(patch, PathPatch):
            continue
        extents = patch.get_path().get_extents()
        centre = (extents.x0 + extents.x1) / 2
        label, offset = _label_at(resolved, centre)
        # A box's patch spans q1..q3; the median line shares its x.
        median = next(
            (
                _r(line.get_ydata()[0])
                for line in ax.lines
                if len(line.get_xdata()) == 2
                and abs(line.get_ydata()[0] - line.get_ydata()[1]) < 1e-12
                and extents.x0 - 1e-9 <= line.get_xdata()[0] <= extents.x1 + 1e-9
                and extents.y0 - 1e-9 <= line.get_ydata()[0] <= extents.y1 + 1e-9
            ),
            _r((extents.y0 + extents.y1) / 2),
        )
        marks.append(
            Mark(panel, label, offset, median, _r(extents.y0), _r(extents.y1),
                 _r(extents.x1 - extents.x0))
        )
    return marks


def _mpl_points(ax, panel, resolved) -> list[Mark]:
    from matplotlib.collections import PathCollection

    marks = []
    for collection in ax.collections:
        if not isinstance(collection, PathCollection):
            continue
        for x, y in collection.get_offsets():
            label, offset = _label_at(resolved, x)
            marks.append(Mark(panel, label, offset, _r(y)))
    return marks


def _mpl_spaghetti(ax, panel, resolved) -> list[Mark]:
    marks = []
    for line in ax.lines:
        if line.get_marker() != "o":
            continue
        for x, y in zip(line.get_xdata(), line.get_ydata(), strict=True):
            label, offset = _label_at(resolved, x)
            marks.append(Mark(panel, label, offset, _r(y)))
    return marks


def mpl_tick_labels(figure) -> list[list[str]]:
    """The x tick texts per visible axes, spacers dropped."""
    return [
        [t.get_text() for t in ax.get_xticklabels() if t.get_text()]
        for ax in figure.axes
        if ax.get_visible()
    ]


def mpl_bracket_labels(figure) -> list[str]:
    """The nested-axis group labels drawn under the ticks (``ax.text``)."""
    return sorted(
        text.get_text() for ax in figure.axes if ax.get_visible() for text in ax.texts
    )


# --- plotly ----------------------------------------------------------------------


def plotly_marks(payload: dict, resolved: ResolvedPlot) -> list[Mark]:
    """The marks a plotly payload draws — the same shape as :func:`mpl_marks`."""
    kind = resolved.kind
    marks: list[Mark] = []
    axes = sorted({trace.get("xaxis", "x") for trace in payload["data"]})
    for trace in payload["data"]:
        if trace.get("legendrank") == 2000:
            continue  # the "Show sample" overlay (`plotly_._sample_traces`), not a mark
        panel = axes.index(trace.get("xaxis", "x"))
        xs = [_plotly_x(resolved, value) for value in trace["x"]]
        if kind is PlotKind.BAR and trace["type"] == "bar":
            ys = [float(v) for v in trace["y"]]
            error = trace.get("error_y")
            for index, ((label, offset), y) in enumerate(zip(xs, ys, strict=True)):
                low = high = None
                if error:
                    low = _r(y - float(error["arrayminus"][index]))
                    high = _r(y + float(error["array"][index]))
                marks.append(Mark(panel, label, offset, _r(y), low, high))
        elif kind in (PlotKind.BOX, PlotKind.VIOLIN) and trace["type"] in ("box", "violin"):
            by_x: dict[tuple[str, float], list[float]] = {}
            for (label, offset), y in zip(xs, trace["y"], strict=True):
                by_x.setdefault((label, offset), []).append(float(y))
            for (label, offset), values in by_x.items():
                values = sorted(values)
                q1, median, q3 = np.percentile(values, [25, 50, 75])
                marks.append(Mark(panel, label, offset, _r(median), _r(q1), _r(q3)))
        elif kind in (PlotKind.SCATTER, PlotKind.STRIP, PlotKind.SPAGHETTI) and trace["type"] == "scatter":
            for (label, offset), y in zip(xs, trace["y"], strict=True):
                marks.append(Mark(panel, label, offset, _r(y)))
    return sorted(marks)


def _plotly_x(resolved: ResolvedPlot, value: Any) -> tuple[str, float]:
    if isinstance(value, str):
        return value, 0.0
    return _label_at(resolved, float(value))


def plotly_tick_labels(payload: dict) -> list[list[str]]:
    """The x tick texts per x axis, spacers dropped, in axis order."""
    layout = payload["layout"]
    keys = sorted(k for k in layout if k.startswith("xaxis"))
    out = []
    for key in keys:
        axis = layout[key]
        if "ticktext" in axis:
            out.append([t for t in axis["ticktext"] if t])
        elif "categoryarray" in axis:
            out.append([str(v) for v in axis["categoryarray"] if not is_spacer(str(v))])
        else:
            out.append([])
    return out


def plotly_bracket_labels(payload: dict) -> list[str]:
    return sorted(a["text"] for a in payload["layout"].get("annotations", []))


# --- comparison helpers ------------------------------------------------------------


def without_error(marks: list[Mark]) -> list[Mark]:
    """The marks with their spread dropped — for kinds whose two backends
    express it differently (a box's quartiles vs its drawn patch)."""
    return sorted(Mark(m.panel, m.x, m.offset, m.y, None, None, None) for m in marks)


def positions_only(marks: list[Mark]) -> list[tuple[int, str, float]]:
    """``(panel, tick label, y)`` — what a reader compares across backends."""
    return sorted((m.panel, m.x, m.y) for m in marks)


def on_tick(marks: list[Mark]) -> list[Mark]:
    """The marks with their offset snapped to the tick — for a strip's jitter."""
    return sorted(Mark(m.panel, m.x, 0.0, m.y, m.low, m.high, m.width) for m in marks)


def despaced(positions, resolved: ResolvedPlot) -> list[float]:
    """Preview x positions with the nested axis's SPACER slots taken out —
    the positions seaborn's export draws at.

    The preview's nested axis leaves an empty slot between groups
    (``xaxis.plan_x_axis``); the export cannot (``codegen._nested_x_args``
    drops the spacers from ``order=``), so a mark at preview position 3.2
    (two leaves, a spacer, then the third leaf) is at 2.2 in the export.
    A test comparing the two maps the preview through this first. Offsets
    inside a tick are kept.
    """
    order = [str(v) for v in (resolved.x_order or [])]
    leaves_before: list[int] = []
    count = 0
    for key in order:
        leaves_before.append(count)
        if not is_spacer(key):
            count += 1
    out = []
    for x in positions:
        tick = int(round(float(x)))
        out.append(_r(leaves_before[tick] + (float(x) - tick)) if order else _r(x))
    return out


# --- the "Show sample" overlay's runs -------------------------------------------


@dataclass(frozen=True, order=True)
class Run:
    """One drawn "Show sample" run — the points ONE polyline joins, or a set
    of lone points when nothing is joined — each point as ``(tick label,
    offset, y)`` in drawn order. A joined line spans the innermost tick only
    (``render.base.sample_series``), so a subject under a bracket reads back
    as one run per bracket."""

    panel: int
    joined: bool
    points: tuple[tuple[str, float, float], ...]


def _run_points(resolved, xs, ys) -> tuple[tuple[str, float, float], ...]:
    points = []
    for x, y in zip(xs, ys, strict=True):
        label, offset = _label_at(resolved, x)
        points.append((label, offset, _r(y)))
    return tuple(points)


def mpl_sample_runs(figure, resolved: ResolvedPlot) -> list[Run]:
    """The overlay runs a matplotlib figure draws (``mpl._draw_sample``:
    ``ax.plot`` with markers when joined, ``ax.scatter`` otherwise — both at
    zorder 3, above the marks). Legend proxies carry no data and are skipped."""
    from matplotlib.collections import PathCollection

    runs: list[Run] = []
    for panel, ax in enumerate(ax for ax in figure.axes if ax.get_visible()):
        for line in ax.lines:
            if line.get_zorder() != 3 or len(line.get_xdata()) == 0:
                continue
            runs.append(Run(panel, True, _run_points(resolved, line.get_xdata(), line.get_ydata())))
        for collection in ax.collections:
            if not isinstance(collection, PathCollection) or collection.get_zorder() != 3:
                continue
            offsets = collection.get_offsets()
            runs.append(Run(panel, False, _run_points(resolved, offsets[:, 0], offsets[:, 1])))
    return sorted(runs)


def plotly_sample_runs(payload: dict, resolved: ResolvedPlot) -> list[Run]:
    """The overlay runs a plotly payload draws (``plotly_._sample_traces``:
    one trace per run, ``legendrank`` 2000) — the same shape as
    :func:`mpl_sample_runs`."""
    runs: list[Run] = []
    axes = sorted({trace.get("xaxis", "x") for trace in payload["data"]})
    for trace in payload["data"]:
        if trace.get("legendrank") != 2000:
            continue
        panel = axes.index(trace.get("xaxis", "x"))
        xs = [float(v) for v in trace["x"]]
        ys = [float(v) for v in trace["y"]]
        runs.append(Run(panel, trace["mode"] == "lines+markers", _run_points(resolved, xs, ys)))
    return sorted(runs)


def run_brackets(run: Run, n_layers: int) -> set[str]:
    """The bracket part of every tick a run touches — the composed leaf key
    minus its innermost layer — so ``len(run_brackets(...)) == 1`` says the
    run never left its bracket."""
    from scistackplot.xaxis import LEAF_SEPARATOR

    return {
        LEAF_SEPARATOR.join(label.split(LEAF_SEPARATOR)[: n_layers - 1]) for label, _, _ in run.points
    }
