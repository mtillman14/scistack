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

- **The special slots** (user, 2026-11-01): single-value bins
  (``HistogramOptions.exact_values``, e.g. 0) LEFT of the range and a NaN bar
  (``show_nan``) RIGHT of it, each in its own hatched slot with its own tick.

The slots, heights, arrangement and ticks are ONE source, :data:`BARS_SOURCE`,
executed here and emitted verbatim by ``codegen`` beside the edges. Every
reader — ``reduce``, both renderers, the export, the GUI's readout — reads
the bar table (``bin_table``) or :class:`BinEdges`; none recomputes a slot.

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

from .spec import DEFAULT_BIN_COUNT, BinMode, HistogramOptions, HistStat

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

# The bars: which slots there are, what each holds, how colour levels share
# them, and the ticks naming the special slots. ONE source, like the edges:
# `bin_table` / `BinEdges` execute it and codegen emits it verbatim, so the
# preview, matplotlib and the exported figure place every bar by one rule.
#
# Special slots (user, 2026-11-01): a value in `exact` (e.g. 0) leaves the
# regular bins for a bar of its own LEFT of the range, ascending, after a
# one-bin gap; the NaN values get a bar RIGHT of the range. A slot is one
# regular bin wide (the median width). Neither has a width on the data axis,
# so neither has a density: under the density stat they are not drawn (the
# exact values still stay out of the regular bins).
BARS_SOURCE = '''
def _scistack_specials(exact, nan, stat):
    """The special slots drawn: (exact values, NaN slot?). None under density."""
    if stat == "density":
        return (), False
    return tuple(exact), bool(nan)


def _scistack_regular(values, exact):
    """Which values the regular bins hold: finite, and equal to no exact
    value (an exact value matches by equality: 0.0 is 0, 1e-9 is not)."""
    import numpy as np

    values = np.asarray(values, dtype=float)
    keep = np.isfinite(values)
    for value in exact:
        keep &= values != float(value)
    return keep


def _scistack_slots(edges, exact, nan, stat):
    """Every bar slot, left to right: (lefts, rights, kinds, labels).
    kinds: "exact" | "range" | "nan"; labels name the special slots."""
    import numpy as np

    exact, nan = _scistack_specials(exact, nan, stat)
    edges = np.asarray(edges, dtype=float)
    if edges.size >= 2:
        lo, hi = float(edges[0]), float(edges[-1])
        width = float(np.median(np.diff(edges)))
    else:
        lo = hi = 0.0
        width = 1.0
    lefts, rights, kinds, labels = [], [], [], []
    for i, value in enumerate(exact):
        left = lo - width * (len(exact) + 1 - i)
        lefts.append(left)
        rights.append(left + width)
        kinds.append("exact")
        labels.append(f"= {value:g}")
    for left, right in zip(edges[:-1], edges[1:]):
        lefts.append(float(left))
        rights.append(float(right))
        kinds.append("range")
        labels.append("")
    if nan:
        lefts.append(hi + width)
        rights.append(hi + 2 * width)
        kinds.append("nan")
        labels.append("NaN")
    return np.array(lefts), np.array(rights), kinds, labels


def _scistack_bar_heights(values, edges, exact, nan, stat):
    """One colour level's (counts, heights) over _scistack_slots' slots.
    A value equal to an exact value is counted in its own slot, never in a
    regular bin. Percent is of every value drawn: regular (in range), exact
    and, when shown, NaN; density is the regular bins' alone."""
    import numpy as np

    values = np.asarray(values, dtype=float)
    missing = ~np.isfinite(values)
    edges = np.asarray(edges, dtype=float)
    regular = (
        np.histogram(values[_scistack_regular(values, exact)], bins=edges)[0]
        if edges.size >= 2
        else np.zeros(0, dtype=int)
    )
    drawn_exact, drawn_nan = _scistack_specials(exact, nan, stat)
    counts = np.array(
        [int(np.sum(values == float(v))) for v in drawn_exact]
        + [int(c) for c in regular]
        + ([int(missing.sum())] if drawn_nan else []),
        dtype=int,
    )
    if stat == "count":
        return counts, counts.astype(float)
    if stat == "percent":
        total = counts.sum()
        return counts, (counts / total * 100.0 if total > 0 else np.zeros(len(counts)))
    total = regular.sum()
    heights = regular / (total * np.diff(edges)) if total > 0 else np.zeros(len(regular))
    return counts, heights.astype(float)


def _scistack_arrange(heights, lefts, rights, multiple, slot_names):
    """How colour levels share each slot. heights: {level: array}, in colour
    order; slot_names: every colour level of the figure (a dodge keeps a
    level's place when a panel lacks another). Returns {level: (lows, highs,
    bases)}: stacked in REVERSED colour order, the last level at the bottom,
    as seaborn stacks; dodged one sub-slot per level, in colour order."""
    import numpy as np

    levels = list(heights)
    bases = {level: np.zeros(len(lefts)) for level in levels}
    if multiple == "stack" and len(levels) > 1:
        running = np.zeros(len(lefts))
        for level in reversed(levels):
            bases[level] = running.copy()
            running = running + heights[level]
    out = {}
    for level in levels:
        lows, highs = lefts, rights
        if multiple == "dodge" and level is not None and len(slot_names) > 1:
            key = str(level)
            index = slot_names.index(key) if key in slot_names else len(slot_names) - 1
            slot = (rights - lefts) / len(slot_names)
            lows, highs = lefts + index * slot, lefts + (index + 1) * slot
        out[level] = (lows, highs, bases[level])
    return out


def _scistack_hist_ticks(lefts, rights, kinds, labels):
    """(values, labels) for an axis with special slots: round-number ticks
    over the regular range only, plus one tick per special slot. None when
    there is no special slot (the axis keeps its own ticks)."""
    import math

    import numpy as np

    if all(kind == "range" for kind in kinds):
        return None
    ticks, names = [], []
    regular = [(lo, hi) for lo, hi, kind in zip(lefts, rights, kinds) if kind == "range"]
    if regular:
        lo, hi = regular[0][0], regular[-1][1]
        raw = (hi - lo) / 5 if hi > lo else 1.0
        magnitude = 10 ** math.floor(math.log10(raw))
        step = next(m * magnitude for m in (1, 2, 2.5, 5, 10) if m * magnitude >= raw)
        first = math.ceil(lo / step - 1e-9) * step
        for tick in first + step * np.arange(int((hi - first) / step + 1e-9) + 1):
            ticks.append(float(tick))
            names.append(f"{round(float(tick), 10):g}")
    for lo, hi, kind, label in zip(lefts, rights, kinds, labels):
        if kind != "range":
            ticks.append((lo + hi) / 2.0)
            names.append(label)
    return ticks, names
'''

# "Show sample" on a histogram or KDE: a RUG (user, 2026-11-01,
# `.claude/plan-histogram-rug.md`). One tick per sample value at its exact x
# (a value of a single-value bin sits under that bar's slot; a NaN has no x),
# one row per colour level, shaded light to dark in the level's own colour by
# CROWDING: how many of the level's ticks share its 1/RUG_SLOTS of the axis.
# A fraction of the axis, not of the screen, so the preview, the saved image
# and the export shade alike at any size. ONE source, emitted verbatim.
RUG_SOURCE = '''
def _scistack_rug_positions(values, exact, lefts, rights, kinds):
    """Each value's tick x: its own value, or the centre of its single-value
    slot (_scistack_slots); NaN (no tick) for a non-finite value."""
    import numpy as np

    values = np.asarray(values, dtype=float)
    xs = np.where(np.isfinite(values), values, np.nan)
    centres = [(lo + hi) / 2.0 for lo, hi, kind in zip(lefts, rights, kinds) if kind == "exact"]
    for value, centre in zip(exact, centres):
        xs[values == float(value)] = centre
    return xs


def _scistack_rug_counts(xs, lo, hi, slots):
    """Each tick's crowding: how many ticks share its 1/slots of [lo, hi].
    0 for a tick with no x or outside the range (not drawn)."""
    import numpy as np

    xs = np.asarray(xs, dtype=float)
    span = (hi - lo) if hi > lo else 1.0
    inside = np.isfinite(xs) & (xs >= lo) & (xs <= hi)
    index = np.clip(((np.where(inside, xs, lo) - lo) / span * slots).astype(int), 0, slots - 1)
    found = np.bincount(index[inside], minlength=slots)
    return np.where(inside, found[index], 0)


def _scistack_rug_shade(counts, top, shades):
    """A shade index 0 (lightest: one tick) .. shades - 1 (the level's most
    crowded spot, ``top``)."""
    import numpy as np

    counts = np.asarray(counts, dtype=float)
    if top <= 1:
        return np.zeros(counts.shape, dtype=int)
    return np.clip(np.rint((counts - 1) / (top - 1) * (shades - 1)), 0, shades - 1).astype(int)


def _scistack_rug_color(base, shade, shades):
    """The level's colour mixed with white: 25% at the lightest shade, the
    full colour at the darkest. base: "#rrggbb" or "rgb(r, g, b)"."""
    text = str(base).strip()
    if text.startswith("rgb"):
        r, g, b = (int(float(p)) for p in text[text.index("(") + 1:text.index(")")].split(",")[:3])
    else:
        text = text.lstrip("#")
        r, g, b = (int(text[i:i + 2], 16) for i in (0, 2, 4))
    a = 0.25 + 0.75 * (shade / (shades - 1) if shades > 1 else 1.0)
    mix = lambda c: int(round(255 * (1 - a) + c * a))
    return f"#{mix(r):02x}{mix(g):02x}{mix(b):02x}"
'''

#: The rug's crowding window: 1/RUG_SLOTS of the x axis (about a pixel of a
#: 6-inch-wide axis). Its shades, quantised so plotly draws one path per
#: shade, not one shape per value. Each row's height (axes fraction, rows
#: stacked upward from the bottom, one per colour level) and tick width (pt).
RUG_SLOTS = 400
RUG_SHADES = 8
RUG_ROW_HEIGHT = 0.03
RUG_LINE_WIDTH = 1.0

_NAMESPACE: dict = {}


def _source_function(name: str):
    if name not in _NAMESPACE:
        exec(EDGES_SOURCE, _NAMESPACE)  # noqa: S102 - our own constant
        exec(BARS_SOURCE, _NAMESPACE)  # noqa: S102 - our own constant
        exec(RUG_SOURCE, _NAMESPACE)  # noqa: S102 - our own constant
    return _NAMESPACE[name]


def _edges_function():
    return _source_function("_scistack_bin_edges")


@dataclass(frozen=True)
class BinEdges:
    """One figure's bins, and its special slots (``BARS_SOURCE``)."""

    edges: tuple[float, ...]
    mode: BinMode
    #: The width when every bin has it (count / auto / width), else None.
    width: float | None
    #: How many finite values decided them (the exact values left out).
    n_values: int = 0
    notes: tuple[str, ...] = ()
    #: The arguments the edges came from, as the generated code passes them.
    call: tuple = ()
    #: The single-value bins in effect (``HistogramOptions.exact_values``,
    #: once applicable: numbers on a numeric axis), ascending.
    exact: tuple[float, ...] = ()
    #: Whether the NaN slot is in effect: asked for, and the figure has NaNs.
    nan: bool = False
    #: The figure's NaN values, every panel — reported whether or not the NaN
    #: slot is drawn, so the GUI offers the toggle exactly when it matters.
    n_nan: int = 0
    #: The stat: under density no special slot is drawn (``_scistack_specials``).
    stat: HistStat = HistStat.COUNT

    @property
    def n_bins(self) -> int:
        return max(len(self.edges) - 1, 0)

    def slots(self) -> tuple:
        """``(lefts, rights, kinds, labels)``: every bar slot, special ones
        included — the one layout ``bin_table``, the x range and the ticks read."""
        return _source_function("_scistack_slots")(self.edges, self.exact, self.nan, str(self.stat))

    def x_range(self) -> tuple[float, float] | None:
        """Exactly the outer slot edges (seaborn's sticky edges), or None."""
        lefts, rights, _kinds, _labels = self.slots()
        if not len(lefts):
            return None
        return (float(lefts.min()), float(rights.max()))

    def ticks(self) -> tuple[list[float], list[str]] | None:
        """The x ticks when a special slot is drawn, else None (the axis
        keeps its own)."""
        return _source_function("_scistack_hist_ticks")(*self.slots())

    def describe(self) -> str:
        if not self.edges and not self.exact:
            return f"{self.mode}: no values to bin"
        width = f", width {self.width:g}" if self.width is not None else ""
        span = f" over [{self.edges[0]:g}, {self.edges[-1]:g}]" if self.edges else ""
        exact = f", exact {list(self.exact)}" if self.exact else ""
        nan = f", NaN slot ({self.n_nan})" if self.nan else (f", {self.n_nan} NaN not drawn" if self.n_nan else "")
        return (
            f"{self.mode}: {self.n_bins} bin(s){span}{width}, from {self.n_values} "
            f"value(s){exact}{nan}"
        )

    def to_dict(self) -> dict:
        _lefts, _rights, kinds, labels = self.slots()
        return {
            "mode": str(self.mode),
            "edges": list(self.edges),
            "width": self.width,
            "n_bins": self.n_bins,
            "n_values": self.n_values,
            "notes": list(self.notes),
            "exact": list(self.exact),
            "nan": self.nan,
            "n_nan": self.n_nan,
            "special": [label for kind, label in zip(kinds, labels) if kind != "range"],
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


#: Why a special slot is not drawn — the notes `bin_edges` adds.
EXACT_TEMPORAL_NOTE = "Single-value bins apply to numbers, not dates; they are ignored here."
NAN_TEMPORAL_NOTE = "Missing times are not drawn as a bar on a date axis."
DENSITY_SPECIAL_NOTE = (
    "A single-value or NaN bar has no width, so it has no density: it is not "
    "drawn. Switch to count or percent to see it."
)


def special_arguments(options: HistogramOptions, *, temporal: bool) -> tuple:
    """``(exact, show_nan, notes)``: the special slots the options ask for,
    once applicable — numbers on a numeric axis only. Density is decided in
    the source itself (``_scistack_specials``), so the export decides it the
    same way."""
    notes: list[str] = []
    exact = tuple(float(v) for v in options.exact_values)
    show_nan = options.show_nan
    if temporal:
        if exact:
            notes.append(EXACT_TEMPORAL_NOTE)
        exact, show_nan = (), False
    return exact, show_nan, notes


def bin_edges(values: Any, options: HistogramOptions, *, temporal: bool = False) -> BinEdges:
    """The figure's bins over ``values`` — every panel's, axis numbers, NaN
    included (they are counted for the NaN slot, never binned). The exact
    values are taken out before the edges are decided, so the regular bins
    span only the values they hold."""
    mode, count, width, lo, hi, notes = _arguments(options, temporal=temporal)
    exact, show_nan, special_notes = special_arguments(options, temporal=temporal)
    notes.extend(special_notes)
    array = np.asarray(values, dtype=float)
    missing = ~np.isfinite(array)
    n_nan = int(missing.sum())
    regular = array[_source_function("_scistack_regular")(array, exact)]
    if temporal and n_nan:
        notes.append(NAN_TEMPORAL_NOTE)
    if options.stat is HistStat.DENSITY and (exact or (show_nan and n_nan)):
        notes.append(DENSITY_SPECIAL_NOTE)
    try:
        edges = _edges_function()(regular, str(mode), count, width, lo, hi, MAX_BINS)
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
        n_values=int(regular.size),
        notes=tuple(notes),
        call=(str(mode), count, width, lo, hi, MAX_BINS),
        exact=exact,
        nan=bool(show_nan and n_nan),
        n_nan=n_nan,
        stat=options.stat,
    )
    Log.info("histogram bins: %s", result.describe(), layer=LAYER)
    for value in exact:
        Log.info(
            "histogram bins: exact %g -> %d value(s) of %d",
            value, int((array == value).sum()), array.size, layer=LAYER,
        )
    for note in notes:
        Log.info("histogram bins: %s", note, layer=LAYER)
    return result


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
    """One panel's bars: a row per (colour level, slot).

    ``frame`` is the panel frame the scatter kinds would draw (``value`` the
    column holding the binned numbers, NaN rows kept; ``color`` the colour
    column or None). Columns: ``X`` (bar centre), ``X_LOW`` / ``X_HIGH`` (bar
    edges after any dodge), ``Y_BASE`` (0 unless stacked), ``Y`` (bar top),
    ``COUNT``, ``BIN_KIND`` / ``BIN_LABEL`` and the colour column. The slots,
    heights and arrangement are ``BARS_SOURCE``'s. Empty bins are kept, so a
    stacked level sits on the right base everywhere.
    """
    from .resolved import BIN_KIND, BIN_LABEL, COUNT, X, X_HIGH, X_LOW, Y, Y_BASE

    columns = [X, X_LOW, X_HIGH, Y_BASE, Y, COUNT, BIN_KIND, BIN_LABEL, *([color] if color else [])]
    lefts, rights, kinds, labels = edges.slots()
    if not len(lefts):
        return pd.DataFrame(columns=columns)

    if color and color in frame.columns:
        present = {str(v) for v in frame[color].dropna().unique()}
        levels = [lvl for lvl in color_order if str(lvl) in present]
        levels += [lvl for lvl in frame[color].dropna().unique() if str(lvl) not in {str(x) for x in levels}]
        groups = [(lvl, frame[frame[color].astype(str) == str(lvl)]) for lvl in levels]
        slot_names = [str(lvl) for lvl in color_order] or [str(lvl) for lvl in levels]
    else:
        groups = [(None, frame)]
        slot_names = []

    heights_of = _source_function("_scistack_bar_heights")
    heights: dict[Any, np.ndarray] = {}
    counts: dict[Any, np.ndarray] = {}
    for level, rows in groups:
        values = pd.to_numeric(rows[value], errors="coerce").to_numpy(dtype=float)
        counts[level], heights[level] = heights_of(
            values, edges.edges, edges.exact, edges.nan, str(edges.stat)
        )
        # The denominator a percent bar is out of (and the export's `_hist_w`
        # restates): every value this level DRAWS, special slots included.
        drawn = [(label or "range", int(c)) for label, c in zip(labels, counts[level])]
        special = {name: c for name, c in drawn if name != "range"}
        Log.debug(
            "histogram bars: level %s: %d value(s) drawn (%d in the regular bins%s) of %d row(s), stat=%s",
            level, int(counts[level].sum()), sum(c for name, c in drawn if name == "range"),
            "".join(f", {name}: {c}" for name, c in special.items()), values.size, edges.stat,
            layer=LAYER,
        )
    arranged = _source_function("_scistack_arrange")(
        heights, lefts, rights, str(options.multiple), slot_names
    )

    parts = []
    for level, _rows in groups:
        lo, hi, base = arranged[level]
        part = pd.DataFrame(
            {
                X: (lo + hi) / 2.0,
                X_LOW: lo,
                X_HIGH: hi,
                Y_BASE: base,
                Y: base + heights[level],
                COUNT: counts[level],
                BIN_KIND: kinds,
                BIN_LABEL: labels,
            }
        )
        if color:
            part[color] = level
        parts.append(part)
    return pd.concat(parts, ignore_index=True)[columns]


def rug_tables(
    samples: list[pd.DataFrame | None],
    edges: BinEdges | None,
    x_range: tuple[float, float] | None,
    *,
    value: str,
    color: str | None,
    color_order: list[Any],
) -> tuple[list[pd.DataFrame | None], dict[str, int]]:
    """Every panel's rug (``RUG_SOURCE``) from its "Show sample" rows.

    ``samples`` are the panels' overlay frames (``value`` the sample value,
    ``color`` the colour column or None); ``edges`` the histogram's bins
    (None on a KDE: no single-value slots); ``x_range`` the figure's axis
    (``render.base.univariate_x_range``'s), so every panel counts in the same
    slots. Crowding is counted per panel and colour level; the darkest shade
    is the level's most crowded spot over the WHOLE figure, so a shade means
    the same in every panel. Returns the rug frames (``X``, ``Y`` the value,
    ``COUNT``, ``RUG_SHADE``, ``RUG_ROW``, the colour column and the sample's
    other columns, for hover) and ``{level: darkest count}``."""
    from .resolved import COUNT, RUG_ROW, RUG_SHADE, X, Y

    if x_range is None:
        return [None for _ in samples], {}
    lo, hi = x_range
    exact = edges.exact if edges is not None else ()
    lefts, rights, kinds, _labels = edges.slots() if edges is not None else ([], [], [], [])
    positions = _source_function("_scistack_rug_positions")
    crowding = _source_function("_scistack_rug_counts")
    shade_of = _source_function("_scistack_rug_shade")
    rows = {str(level): index for index, level in enumerate(color_order)}

    counted: list[pd.DataFrame | None] = []
    tops: dict[str, int] = {}
    dropped = 0
    for sample in samples:
        if sample is None:
            counted.append(None)
            continue
        out = sample.copy()
        out[Y] = pd.to_numeric(out[value], errors="coerce")
        out[X] = positions(out[Y].to_numpy(dtype=float), exact, lefts, rights, kinds)
        out[COUNT] = 0
        groups = out.groupby(color, sort=False) if color and color in out.columns else [(None, out)]
        for level, part in groups:
            counts = crowding(part[X].to_numpy(dtype=float), lo, hi, RUG_SLOTS)
            out.loc[part.index, COUNT] = counts
            key = str(level)
            tops[key] = max(tops.get(key, 0), int(counts.max()) if len(counts) else 0)
        dropped += int((out[COUNT] == 0).sum())
        counted.append(out[out[COUNT] > 0])

    frames: list[pd.DataFrame | None] = []
    for out in counted:
        if out is None:
            frames.append(None)
            continue
        levels = out[color].astype(str) if color and color in out.columns else pd.Series("None", index=out.index)
        out = out.copy()
        shades = np.zeros(len(out), dtype=int)
        for level in levels.unique():
            mask = (levels == level).to_numpy()
            shades[mask] = shade_of(out[COUNT].to_numpy()[mask], tops.get(level, 1), RUG_SHADES)
        out[RUG_SHADE] = shades
        out[RUG_ROW] = [rows.get(level, 0) for level in levels]
        frames.append(out.reset_index(drop=True))
    Log.info(
        "rug: %d tick(s) over %d panel(s), crowding per 1/%d of [%g, %g]; darkest %s%s",
        sum(len(f) for f in frames if f is not None),
        len(samples),
        RUG_SLOTS,
        lo,
        hi,
        tops or "none",
        f"; {dropped} value(s) with no x or outside the range not drawn" if dropped else "",
        layer=LAYER,
    )
    return frames, tops


def rug_color(base: str, shade: int) -> str:
    """A rug tick's colour: the level's colour at this shade (``RUG_SOURCE``)."""
    return _source_function("_scistack_rug_color")(base, int(shade), RUG_SHADES)


def histogram_limits(tops: list[float]) -> tuple[float, float]:
    """``(0, top)`` for bars whose tallest top is ``max(tops)``: matplotlib's
    own autoscale for bars (a sticky zero, a 5% margin above), so the export
    and the generated code, which autoscale, draw the preview's range."""
    finite = [t for t in tops if t == t]
    top = max(finite) if finite else 0.0
    return (0.0, top * 1.05 if top > 0 else 1.0)
