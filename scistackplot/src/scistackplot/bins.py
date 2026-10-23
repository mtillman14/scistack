"""Histogram bins — the one owner (docs/claude/histogram-and-regression.md).

Three things a histogram decides, each once:

- **The edges** (:func:`bin_edges`). ONE source, :data:`EDGES_SOURCE`: this
  module executes it and ``codegen`` emits it verbatim, so the exported
  figure bins exactly where the preview does (the ``MPL_DATE_AXIS_SOURCE``
  bargain). Pure numpy; no scistackplot import.

  * ``auto``: numpy's ``"auto"`` estimator (seaborn's default too).
  * ``count``: that many equal bins over the range.
  * ``width``: bins that wide, the first edge at the multiple of the width at
    or below the range's start — so a 1-day width starts at midnight wall
    clock, and a width of 5 starts at a multiple of 5. A datetime width is a
    fixed duration (``"1h"``, ``"7D"``, ``"30min"``; no months, which have no
    fixed length — user, 2026-10-22). 7-day bins therefore start on
    Thursdays (1970-01-01 was one).

  The range is the values' own extent unless ``range_min`` / ``range_max``
  pin an end. Edges are decided once per FIGURE, over every panel's values,
  so the panels of a figure compare bin for bin. A figure of a fan-out
  decides its own, as the exported endpoint (which sees one figure's rows)
  must.

- **The heights** (:func:`bin_table`): count, percent or density, normalised
  per colour level within a panel (seaborn ``common_norm=False``; user,
  2026-10-22) so two groups of different sizes compare by shape.

- **How colour levels share a panel**: layered (overlaid), stacked (in
  REVERSED colour order, the last level at the bottom, as seaborn stacks)
  or dodged (each bin split into one slot per colour level, in colour order).

Values are axis numbers: a datetime measure arrives as wall-clock ms
(``temporal.apply_temporal``), so a datetime histogram is a numeric one with
a date axis and a width in ms.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scistacklog import Log

from .spec import DEFAULT_BIN_COUNT, BinMode, HistMultiple, HistogramOptions, HistStat

LAYER = "scistackplot"

#: The most bins a figure may have. A width too small for the range would
#: otherwise ask for millions of bars and stall the panel.
MAX_BINS = 1000

#: The fill opacity of a histogram bar (``render.base.fill_alpha``): layered
#: histograms overlap, so they must show through each other.
FILL_ALPHA = 0.6
#: The bar outline: stated, because seaborn's and plotly's defaults differ.
EDGE_COLOR = "#ffffff"
EDGE_WIDTH = 0.5


class BinError(ValueError):
    """Bin settings that cannot be applied as written; the message names the fix."""


# The edges. ONE source: bin_edges executes it, codegen emits it verbatim.
EDGES_SOURCE = '''
def _scistack_bin_edges(values, mode, count, width, lo, hi, max_bins):
    """Histogram bin edges: the scistackplot.bins rule."""
    import math

    import numpy as np

    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if lo is None:
        lo = float(values.min()) if values.size else None
    if hi is None:
        hi = float(values.max()) if values.size else None
    if lo is None or hi is None:
        return np.array([], dtype=float)
    if hi < lo:
        lo, hi = hi, lo
    if mode == "width":
        start = math.floor(lo / width) * width
        n = max(1, int(math.ceil((hi - start) / width - 1e-9)))
        if n > max_bins:
            raise ValueError(
                f"a bin width of {width:g} makes {n} bins; at most {max_bins} fit"
            )
        edges = start + width * np.arange(n + 1, dtype=float)
        edges[-1] = max(edges[-1], hi)
        return edges
    inside = values[(values >= lo) & (values <= hi)]
    bins = int(count) if mode == "count" else "auto"
    edges = np.histogram_bin_edges(inside, bins=bins, range=(lo, hi))
    if len(edges) - 1 > max_bins:
        edges = np.histogram_bin_edges(inside, bins=max_bins, range=(lo, hi))
    return edges
'''

_NAMESPACE: dict = {}


def _edges_function():
    if "_scistack_bin_edges" not in _NAMESPACE:
        exec(EDGES_SOURCE, _NAMESPACE)  # noqa: S102 - our own constant
    return _NAMESPACE["_scistack_bin_edges"]


@dataclass(frozen=True)
class BinEdges:
    """One figure's bins."""

    edges: tuple[float, ...]
    mode: BinMode
    #: The width when every bin has it (count / auto / width), else None.
    width: float | None
    #: How many finite values decided them.
    n_values: int = 0
    notes: tuple[str, ...] = ()
    #: The arguments the edges came from, as the generated code passes them.
    call: tuple = ()

    @property
    def n_bins(self) -> int:
        return max(len(self.edges) - 1, 0)

    def describe(self) -> str:
        if not self.edges:
            return f"{self.mode}: no values to bin"
        width = f", width {self.width:g}" if self.width is not None else ""
        return (
            f"{self.mode}: {self.n_bins} bin(s) over [{self.edges[0]:g}, "
            f"{self.edges[-1]:g}]{width}, from {self.n_values} value(s)"
        )

    def to_dict(self) -> dict:
        return {
            "mode": str(self.mode),
            "edges": list(self.edges),
            "width": self.width,
            "n_bins": self.n_bins,
            "n_values": self.n_values,
            "notes": list(self.notes),
        }


def parse_width(value: Any, *, temporal: bool) -> float | None:
    """A bin width as an axis number (ms on a datetime axis), or None.

    Raises :class:`BinError` naming the fix for a width that is not one.
    """
    if value is None or value == "":
        return None
    if temporal:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            raise BinError(
                f"bin width {value!r}: on a datetime axis type a duration such as "
                f"'30min', '1h', '1D' or '7D'."
            )
        text = str(value).strip()
        try:
            delta = pd.Timedelta(text)
        except (TypeError, ValueError) as exc:
            raise BinError(
                f"bin width {text!r} is not a duration. Use a fixed length such as "
                f"'30min', '1h', '1D' or '7D' (months and years have no fixed length)."
            ) from exc
        width = delta.total_seconds() * 1000.0
    else:
        try:
            width = float(value)
        except (TypeError, ValueError) as exc:
            raise BinError(f"bin width {value!r} is not a number.") from exc
    if not width > 0 or not math.isfinite(width):
        raise BinError(f"bin width {value!r} must be greater than zero.")
    return width


def parse_bound(value: Any, *, temporal: bool) -> float | None:
    """A range end as an axis number: ISO date text on a datetime axis (a
    wall clock in the axis's zone, ``temporal.parse_limit``), a number
    otherwise. Raises :class:`BinError` for text that is neither."""
    if value is None or value == "":
        return None
    if temporal:
        from .temporal import parse_limit

        parsed = parse_limit(value)
        if parsed is None:
            raise BinError(f"range end {value!r} is not a date (try '2026-07-01 12:00').")
        return parsed
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise BinError(f"range end {value!r} is not a number.") from exc


def histogram_problem(options: HistogramOptions, *, temporal: bool) -> str | None:
    """Why these settings cannot bin, or None. ``roles.validate`` refuses the
    spec with it and the capability report shows it beside the controls."""
    try:
        _arguments(options, temporal=temporal)
    except BinError as exc:
        return str(exc)
    return None


def edge_arguments(options: HistogramOptions, *, temporal: bool) -> tuple:
    """``(mode, count, width, lo, hi, max_bins)`` exactly as
    ``_scistack_bin_edges`` takes them — what the generated code passes,
    so it bins over its own figure's values by the preview's settings."""
    mode, count, width, lo, hi, _notes = _arguments(options, temporal=temporal)
    return (str(mode), count, width, lo, hi, MAX_BINS)


def _arguments(options: HistogramOptions, *, temporal: bool) -> tuple:
    """``(mode, count, width, lo, hi, notes)``: the options as the edge rule
    takes them. A width mode with no width falls back to auto, with a note
    (a preset carries the mode but never the variable's own width)."""
    notes: list[str] = []
    mode = options.bins
    count = options.bin_count if options.bin_count is not None else DEFAULT_BIN_COUNT
    if mode is BinMode.COUNT and not 1 <= int(count) <= MAX_BINS:
        raise BinError(f"bin count {count} must be between 1 and {MAX_BINS}.")
    width = parse_width(options.bin_width, temporal=temporal) if mode is BinMode.WIDTH else None
    if mode is BinMode.WIDTH and width is None:
        notes.append("No bin width is set, so the bins are chosen automatically.")
        mode = BinMode.AUTO
    lo = parse_bound(options.range_min, temporal=temporal)
    hi = parse_bound(options.range_max, temporal=temporal)
    if lo is not None and hi is not None and hi < lo:
        raise BinError(f"range {options.range_min!r} .. {options.range_max!r} ends before it starts.")
    return mode, int(count), width, lo, hi, notes


def bin_edges(values: Any, options: HistogramOptions, *, temporal: bool = False) -> BinEdges:
    """The figure's bin edges over ``values`` (every panel's, axis numbers)."""
    mode, count, width, lo, hi, notes = _arguments(options, temporal=temporal)
    array = np.asarray(values, dtype=float)
    finite = int(np.isfinite(array).sum())
    try:
        edges = _edges_function()(array, str(mode), count, width, lo, hi, MAX_BINS)
    except ValueError as exc:
        raise BinError(f"{exc}. Use a wider bin, or fewer bins.") from exc
    if mode is BinMode.AUTO and len(edges) - 1 == MAX_BINS:
        notes.append(f"Automatic bins capped at {MAX_BINS}.")
    widths = np.diff(edges)
    same = widths.size and np.allclose(widths, widths[0], rtol=1e-9, atol=0.0)
    result = BinEdges(
        edges=tuple(float(e) for e in edges),
        mode=mode,
        width=float(widths[0]) if same else None,
        n_values=finite,
        notes=tuple(notes),
        call=(str(mode), count, width, lo, hi, MAX_BINS),
    )
    Log.info("histogram bins: %s", result.describe(), layer=LAYER)
    for note in notes:
        Log.info("histogram bins: %s", note, layer=LAYER)
    return result


def _heights(counts: np.ndarray, edges: np.ndarray, stat: HistStat) -> np.ndarray:
    """Bar heights from counts, normalised within ONE colour level's counts."""
    counts = counts.astype(float)
    total = counts.sum()
    if stat is HistStat.COUNT:
        return counts
    if total <= 0:
        return np.zeros_like(counts)
    if stat is HistStat.PERCENT:
        return counts / total * 100.0
    return counts / (total * np.diff(edges))


def y_title(stat: HistStat) -> str:
    """The y axis title a histogram draws unless the user typed one."""
    return {HistStat.COUNT: "Count", HistStat.PERCENT: "Percent", HistStat.DENSITY: "Density"}[stat]


def bin_table(
    frame: pd.DataFrame,
    edges: BinEdges,
    options: HistogramOptions,
    *,
    value: str,
    color: str | None,
    color_order: list[Any],
) -> pd.DataFrame:
    """One panel's bars: a row per (colour level, bin).

    ``frame`` is the panel frame the scatter kinds would draw (``value`` the
    column holding the binned numbers, ``color`` the colour column or None).
    Columns: ``X`` (bar centre), ``X_LOW`` / ``X_HIGH`` (bar edges after any
    dodge), ``Y_BASE`` (0 unless stacked), ``Y`` (bar top), ``COUNT`` and the
    colour column. Empty bins are kept, so a stacked level sits on the right
    base everywhere.
    """
    from .resolved import COUNT, X, X_HIGH, X_LOW, Y, Y_BASE

    columns = [X, X_LOW, X_HIGH, Y_BASE, Y, COUNT, *([color] if color else [])]
    if not edges.edges:
        return pd.DataFrame(columns=columns)
    edge_array = np.asarray(edges.edges, dtype=float)
    lefts, rights = edge_array[:-1], edge_array[1:]

    if color and color in frame.columns:
        present = {str(v) for v in frame[color].dropna().unique()}
        levels = [lvl for lvl in color_order if str(lvl) in present]
        levels += [lvl for lvl in frame[color].dropna().unique() if str(lvl) not in {str(x) for x in levels}]
        groups = [(lvl, frame[frame[color].astype(str) == str(lvl)]) for lvl in levels]
        slots = [str(lvl) for lvl in color_order] or [str(lvl) for lvl in levels]
    else:
        groups = [(None, frame)]
        slots = []

    heights: dict[str, np.ndarray] = {}
    counts: dict[str, np.ndarray] = {}
    for level, rows in groups:
        values = pd.to_numeric(rows[value], errors="coerce").to_numpy(dtype=float)
        values = values[np.isfinite(values)]
        found, _ = np.histogram(values, bins=edge_array)
        counts[str(level)] = found
        heights[str(level)] = _heights(found, edge_array, options.stat)

    bases = {key: np.zeros(len(lefts)) for key in heights}
    if options.multiple is HistMultiple.STACK and len(groups) > 1:
        running = np.zeros(len(lefts))
        for level, _ in reversed(groups):
            bases[str(level)] = running.copy()
            running = running + heights[str(level)]

    parts = []
    for level, _rows in groups:
        key = str(level)
        lo, hi = lefts, rights
        if options.multiple is HistMultiple.DODGE and level is not None and len(slots) > 1:
            index = slots.index(key) if key in slots else len(slots) - 1
            slot = (rights - lefts) / len(slots)
            lo, hi = lefts + index * slot, lefts + (index + 1) * slot
        part = pd.DataFrame(
            {
                X: (lo + hi) / 2.0,
                X_LOW: lo,
                X_HIGH: hi,
                Y_BASE: bases[key],
                Y: bases[key] + heights[key],
                COUNT: counts[key],
            }
        )
        if color:
            part[color] = level
        parts.append(part)
    return pd.concat(parts, ignore_index=True)[columns]


def histogram_limits(tops: list[float]) -> tuple[float, float]:
    """``(0, top)`` for bars whose tallest top is ``max(tops)``: matplotlib's
    own autoscale for bars (a sticky zero, a 5% margin above), so the export
    and the generated code, which autoscale, draw the preview's range."""
    finite = [t for t in tops if t == t]
    top = max(finite) if finite else 0.0
    return (0.0, top * 1.05 if top > 0 else 1.0)
