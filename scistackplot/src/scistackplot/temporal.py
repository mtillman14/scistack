"""Datetimes in plots — the one owner (docs/claude/plot-datetimes.md).

A datetime column follows the same rules as every other data type (user,
2026-10-22): it is a measure (y now; x once continuous x-vs-y plots exist) or
a factor (group, colour, facet, iterate, filter).

**The time zone mode** (``PlotSpec.time_zone``) decides how values that hold
several zones share one axis:

- ``"local"`` (default): each value at its own wall clock, so a time of day
  compares across sites;
- a zone label (``"UTC"``, ``"Europe/Berlin"``, ``"+02:00"``;
  scicanonicalhash's vocabulary): every aware value converted to it, so
  instants line up. A naive value has no zone to convert from and is placed
  at its own clock time (counted in the notes).

**A measure** becomes one float per value: wall-clock milliseconds since
1970-01-01 (:func:`wall_clock_ms`), done once in :func:`apply_temporal`, the
fourth derived table. Every reducer, the y-limits and the sample overlay
then run on ordinary numbers; plotly reads milliseconds on a ``date`` axis
as they are, and matplotlib gets a locator/formatter in milliseconds
(:data:`MPL_DATE_AXIS_SOURCE`, executed by the renderer and emitted verbatim
by codegen, so the export draws the preview's ticks).

**A factor** keeps a fixed IDENTITY label per value: the value in its own
zone (:func:`level_labels`, applied in ``LongTable.from_frame``), ordered by
time. Filters, facet rules, colours and aliases key on it whatever the mode;
"Convert to <zone>" only changes how a level READS (:func:`display_levels`,
the lowest layer of ``aliases.display_text``).

Known limit: in a converted mode a DST fall-back hour repeats, so two instants
in it share a wall clock. UTC as the target has no such hour.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scicanonicalhash import is_missing_datetime, utc_ns, zone_from_label, zone_label
from scistacklog import Log

LAYER = "scistackplot"

#: ``PlotSpec.time_zone`` for "each value at its own wall clock".
LOCAL = "local"

#: How many leading values decide whether an object column is temporal.
_SCAN = 50

_NS_PER_MS = 1_000_000

#: The reason a datetime measure offers no bar (user, 2026-10-22).
BAR_REASON = (
    "A bar is drawn from zero, which for a datetime is 1970; "
    "use points, a box or a violin."
)
LOG_REASON = "A datetime axis cannot be logarithmic."
PERCENT_REASON = "A % change of a datetime has no meaning; use a difference."


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def is_temporal_value(value: Any) -> bool:
    """A present datetime (``datetime``, ``pd.Timestamp``, ``np.datetime64``)."""
    if isinstance(value, np.datetime64):
        return not np.isnat(value)
    return isinstance(value, datetime.datetime) and not is_missing_datetime(value)


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    if isinstance(value, float) and value != value:
        return True
    return is_missing_datetime(value)


def has_temporal_values(series: pd.Series, skip: tuple = ()) -> bool:
    """Whether any of a column's first present values is a datetime — the
    cheap test before anything walks the whole column. ``skip``: values that
    name a missing value (``table.MISSING_LEVEL``), passed over like NaN."""
    seen = 0
    for value in series:
        if _is_missing(value) or (isinstance(value, str) and value in skip):
            continue
        if is_temporal_value(value):
            return True
        seen += 1
        if seen >= _SCAN:
            return False
    return False


def is_temporal_column(series: pd.Series) -> bool:
    """Whether a column holds datetimes: a datetime64 dtype (naive or aware),
    or an object column whose present values (the first few) are all
    datetimes. An all-missing object column is not temporal."""
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return True
    if series.dtype != object:
        return False
    seen = 0
    for value in series:
        if _is_missing(value):
            continue
        if not is_temporal_value(value):
            return False
        seen += 1
        if seen >= _SCAN:
            break
    return seen > 0


# ---------------------------------------------------------------------------
# The mode
# ---------------------------------------------------------------------------


def normalize_mode(mode: Any) -> str:
    """``PlotSpec.time_zone`` as stored: ``"local"`` or a zone label. Raises
    ``ValueError`` naming a zone no time-zone database knows."""
    text = str(mode or "").strip()
    if not text or text.lower() == LOCAL:
        return LOCAL
    zone_from_label(text)  # validates
    return text


def zone_title(mode: str) -> str:
    """What an axis or a note calls the mode: ``local time`` or the zone."""
    return "local time" if mode == LOCAL else mode


def _as_timestamp(value: Any) -> pd.Timestamp:
    return pd.Timestamp(value)


def _wall_ms(ts: pd.Timestamp) -> float:
    """A Timestamp's own wall clock, as ms since 1970-01-01."""
    naive = ts.tz_localize(None) if ts.tzinfo is not None else ts
    return naive.value / _NS_PER_MS  # .value is ns whatever the unit


@dataclass
class TemporalReport:
    """What converting one column did: counts for the figure's notes."""

    n_values: int = 0
    n_naive: int = 0
    n_aware: int = 0
    zones: tuple[str, ...] = ()
    mode: str = LOCAL

    def notes(self, what: str) -> list[str]:
        out = []
        if self.mode != LOCAL and self.n_naive:
            out.append(
                f"{self.n_naive} value(s) of {what} have no time zone; placed at "
                f"their own clock time, not converted to {self.mode}"
            )
        if self.mode == LOCAL and len(self.zones) > 1:
            out.append(
                f"{what} holds {len(self.zones)} time zones ({', '.join(self.zones)}); "
                f"each value is placed at its own local time"
            )
        return out


def wall_clock_ms(series: pd.Series, mode: str = LOCAL) -> tuple[np.ndarray, TemporalReport]:
    """THE conversion: datetimes -> float64 wall-clock ms in ``mode``.

    NaT / missing -> NaN. A naive value is its own wall clock in every mode.
    """
    mode = normalize_mode(mode)
    report = TemporalReport(mode=mode)
    target = None if mode == LOCAL else zone_from_label(mode)

    # Vectorised: a datetime64 column (one zone or none).
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        s = series
        tz = s.dt.tz
        if tz is not None:
            if target is not None:
                s = s.dt.tz_convert(target)
            report.zones = (zone_label(pd.Timestamp(0, tz=tz)) or "UTC",)
            report.n_aware = int(s.notna().sum())
            s = s.dt.tz_localize(None)
        else:
            report.n_naive = int(s.notna().sum())
        ns = s.astype("datetime64[ns]").to_numpy().view(np.int64).astype("float64")
        ns[s.isna().to_numpy()] = np.nan
        report.n_values = len(s)
        return ns / _NS_PER_MS, report

    out = np.full(len(series), np.nan, dtype="float64")
    zones: dict[str, None] = {}
    for i, value in enumerate(series.to_numpy()):
        if _is_missing(value) or not is_temporal_value(value):
            continue
        ts = _as_timestamp(value)
        label = zone_label(ts)
        if label is None:
            report.n_naive += 1
        else:
            report.n_aware += 1
            zones[label] = None
            if target is not None:
                ts = ts.tz_convert(target)
        out[i] = _wall_ms(ts)
    report.n_values = len(series)
    report.zones = tuple(zones)
    return out, report


def from_axis_ms(value: float) -> pd.Timestamp:
    """An axis number back to the naive wall-clock Timestamp it stands for."""
    return pd.Timestamp(int(round(value * _NS_PER_MS)))


def parse_limit(value: Any) -> float | None:
    """A y-limit of a temporal axis -> axis ms. ISO text (``2026-07-01
    12:00``) is read as a wall clock in the axis's zone; a number is taken
    as axis ms already; ``""``/None -> None."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    try:
        ts = pd.Timestamp(str(value))
    except (TypeError, ValueError):
        return None
    return _wall_ms(ts)


# ---------------------------------------------------------------------------
# The axis
# ---------------------------------------------------------------------------

#: ``(span ms at least, strftime, d3 format)``: the tick label format for an
#: axis spanning that much, widest first. ONE table read by both renderers.
_FORMATS = (
    (2 * 365 * 86_400_000, "%Y", "%Y"),
    (60 * 86_400_000, "%Y-%m", "%Y-%m"),
    (2 * 86_400_000, "%Y-%m-%d", "%Y-%m-%d"),
    (2 * 3_600_000, "%m-%d %H:%M", "%m-%d %H:%M"),
    (2 * 60_000, "%H:%M", "%H:%M"),
    (2_000, "%H:%M:%S", "%H:%M:%S"),
    (0, "%H:%M:%S.%f", "%H:%M:%S.%L"),
)
HOVER_STRFTIME = "%Y-%m-%d %H:%M:%S"
HOVER_D3 = "%Y-%m-%d %H:%M:%S.%L"


def tick_format(span_ms: float) -> tuple[str, str]:
    """``(strftime, d3)`` for an axis spanning ``span_ms``."""
    span = abs(float(span_ms)) if span_ms == span_ms else 0.0
    for floor, strf, d3 in _FORMATS:
        if span >= floor:
            return strf, d3
    return _FORMATS[-1][1], _FORMATS[-1][2]


@dataclass(frozen=True)
class TemporalAxis:
    """A datetime y axis, carried on ``ResolvedPlot.temporal``."""

    mode: str = LOCAL
    #: The tick format, chosen from the data's span (:func:`tick_format`).
    strftime: str = "%Y-%m-%d"
    d3: str = "%Y-%m-%d"
    #: Notes for the figure (naive values in a converted mode, several zones).
    notes: tuple[str, ...] = ()

    @property
    def title_suffix(self) -> str:
        return f" ({zone_title(self.mode)})"

    def plotly_axis(self) -> dict:
        """The keys a plotly y axis needs to read ms as dates."""
        return {"type": "date", "tickformat": self.d3, "hoverformat": HOVER_D3}

    def format(self, value: float) -> str:
        """One axis number as tick text (strftime), for logs and tests."""
        if value != value:
            return ""
        return from_axis_ms(value).strftime(self.strftime)

    def to_dict(self) -> dict:
        return {"mode": self.mode, "strftime": self.strftime, "d3": self.d3,
                "notes": list(self.notes)}


# The matplotlib date axis in ms. ONE source: render_matplotlib executes it
# (:func:`apply_mpl_date_axis`) and codegen emits it verbatim
# (:func:`mpl_date_axis_code`), so the exported figure's ticks are the
# preview's. Pure matplotlib; no scistackplot import.
MPL_DATE_AXIS_SOURCE = '''
def _scistack_date_axis(axis, strftime):
    """Read an axis of wall-clock ms since 1970 as dates."""
    import datetime as _dt

    import matplotlib.dates as _mdates
    import matplotlib.ticker as _mticker

    _epoch = _dt.datetime(1970, 1, 1)

    def _to_dt(ms):
        return _epoch + _dt.timedelta(milliseconds=float(ms))

    class _MsDateLocator(_mticker.Locator):
        def __init__(self):
            self._auto = _mdates.AutoDateLocator()

        def __call__(self):
            lo, hi = self.axis.get_view_interval()
            return self.tick_values(lo, hi)

        def tick_values(self, vmin, vmax):
            if not (vmin == vmin and vmax == vmax):
                return []
            if vmin == vmax:
                return [vmin]
            lo, hi = sorted((vmin, vmax))
            days = self._auto.tick_values(_to_dt(lo), _to_dt(hi))
            return [d * 86_400_000.0 for d in days]

    axis.set_major_locator(_MsDateLocator())
    axis.set_major_formatter(
        _mticker.FuncFormatter(lambda ms, _pos: _to_dt(ms).strftime(strftime))
    )
'''

_MPL_NAMESPACE: dict = {}


def apply_mpl_date_axis(axis, temporal: TemporalAxis) -> None:
    """Make a matplotlib ``Axis`` (``ax.yaxis``) read ms as dates."""
    if "_scistack_date_axis" not in _MPL_NAMESPACE:
        exec(MPL_DATE_AXIS_SOURCE, _MPL_NAMESPACE)  # noqa: S102 - our own constant
    _MPL_NAMESPACE["_scistack_date_axis"](axis, temporal.strftime)


def mpl_date_axis_code(axes: str, temporal: TemporalAxis) -> list[str]:
    """Generated-code lines: define the helper, then apply it to each axes'
    y axis in ``axes`` (an expression: ``g.axes.flat``)."""
    body = MPL_DATE_AXIS_SOURCE.strip("\n").splitlines()
    return [
        *body,
        f"for _ax in {axes}:",
        f"    _scistack_date_axis(_ax.yaxis, {temporal.strftime!r})",
    ]


def conversion_code(column: str, mode: str) -> list[str]:
    """Generated-code lines turning ``df[column]`` into axis ms exactly as
    :func:`wall_clock_ms` does (the export must draw what the preview draws)."""
    mode = normalize_mode(mode)
    lines: list[str] = []
    if mode == LOCAL:
        target = "None"
    else:
        tz = zone_from_label(mode)
        if isinstance(tz, datetime.timezone):
            # A fixed offset (or UTC): stated exactly; pandas' own parsing of
            # "+02:00" is not something every version agrees on.
            minutes = int(tz.utcoffset(None).total_seconds() // 60)
            lines.append("import datetime")
            target = f"datetime.timezone(datetime.timedelta(minutes={minutes}))"
        else:
            target = repr(mode)
    return [
        *lines,
        f"def _wall_ms(_v, _target={target}):",
        "    if _v is None or (isinstance(_v, float) and _v != _v) or pd.isna(_v):",
        "        return float('nan')",
        "    _ts = pd.Timestamp(_v)",
        "    if _ts.tzinfo is not None:",
        "        if _target is not None:",
        "            _ts = _ts.tz_convert(_target)",
        "        _ts = _ts.tz_localize(None)",
        "    return _ts.value / 1e6",
        f"df = df.assign(**{{{column!r}: df[{column!r}].map(_wall_ms).astype(float)}})",
    ]


def factor_label_code(column: str, values: dict, strftime: str, missing: "str | None") -> list[str]:
    """Generated-code lines giving a datetime factor column its identity
    labels (:func:`level_labels`) — the preview's levels, so the export's ticks,
    legend, filters and orders match. The labels the preview made are a
    literal (keyed by each value's ISO text); a value the preview never saw is
    formatted the same way (wall clock + zone abbreviation)."""
    known = {pd.Timestamp(ts).isoformat(): label for label, ts in values.items()}
    var = "_levels_" + "".join(ch if ch.isalnum() else "_" for ch in str(column))
    return [
        f"# {column}: datetimes as the preview labels them, in time order",
        f"{var} = {known!r}",
        f"def {var}_of(_v):",
        "    if _v is None or (isinstance(_v, float) and _v != _v) or pd.isna(_v):",
        f"        return {missing!r}",
        "    _ts = pd.Timestamp(_v)",
        f"    _known = {var}.get(_ts.isoformat())",
        "    if _known is not None:",
        "        return _known",
        f"    _text = _ts.tz_localize(None).strftime({strftime!r}) if _ts.tzinfo else _ts.strftime({strftime!r})",
        "    return f'{_text} {_ts.tzname()}' if _ts.tzinfo else _text",
        f"df[{column!r}] = df[{column!r}].map({var}_of)",
    ]


# ---------------------------------------------------------------------------
# Factors
# ---------------------------------------------------------------------------


def _precision(values: list[pd.Timestamp]) -> str:
    """The strftime every level of a column shares: as fine as the finest
    value needs, no finer."""
    def own(ts):
        return ts.tz_localize(None) if ts.tzinfo is not None else ts

    walls = [own(ts) for ts in values]
    if any(ts.nanosecond or ts.microsecond for ts in walls):
        return "%Y-%m-%d %H:%M:%S.%f"
    if any(ts.second for ts in walls):
        return "%Y-%m-%d %H:%M:%S"
    if any(ts.hour or ts.minute for ts in walls):
        return "%Y-%m-%d %H:%M"
    return "%Y-%m-%d"


def _label(ts: pd.Timestamp, strftime: str, zone_text: str | None) -> str:
    text = ts.strftime(strftime)
    if ts.nanosecond and strftime.endswith(".%f"):
        text += f"{ts.nanosecond:03d}"
    return f"{text} {zone_text}" if zone_text else text


def _zone_text(ts: pd.Timestamp, full: bool) -> str | None:
    if ts.tzinfo is None:
        return None
    return zone_label(ts) if full else (ts.tzname() or zone_label(ts))


def _sort_key(ts: pd.Timestamp) -> int:
    return utc_ns(ts)


@dataclass
class TemporalLevels:
    """A datetime factor's identity labels."""

    #: One label per row (None where the value is missing).
    labels: list[str | None]
    #: Levels in time order.
    order: list[str]
    #: ``{label: the Timestamp it names}`` (the first value read so).
    values: dict[str, pd.Timestamp] = field(default_factory=dict)
    strftime: str = "%Y-%m-%d"


def level_labels(series: pd.Series) -> TemporalLevels:
    """THE identity label of each value of a datetime factor: the value in
    its own zone, at the precision the column needs, with the zone's
    abbreviation (``2026-07-01 14:00 CEST``) — or its full label when two
    different values would otherwise read the same."""
    stamps: list[pd.Timestamp | None] = [
        None if _is_missing(v) or not is_temporal_value(v) else _as_timestamp(v)
        for v in series.to_numpy()
    ]
    present = [ts for ts in stamps if ts is not None]
    strftime = _precision(present) if present else "%Y-%m-%d"

    def build(full: bool):
        labels, values, clash = [], {}, False
        for ts in stamps:
            if ts is None:
                labels.append(None)
                continue
            text = _label(ts.tz_localize(None) if ts.tzinfo else ts, strftime, _zone_text(ts, full))
            prior = values.get(text)
            if prior is not None and (utc_ns(prior), zone_label(prior)) != (utc_ns(ts), zone_label(ts)):
                clash = True
            values.setdefault(text, ts)
            labels.append(text)
        return labels, values, clash

    labels, values, clash = build(full=False)
    if clash:
        labels, values, _ = build(full=True)
    order = sorted(values, key=lambda text: (_sort_key(values[text]), text))
    return TemporalLevels(labels=labels, order=order, values=values, strftime=strftime)


def display_levels(values: dict[str, pd.Timestamp], strftime: str, mode: str) -> dict[str, str]:
    """``{identity label: text}`` for a converted mode; ``{}`` in local mode
    (identity labels already read as each value's own time). Levels that
    convert to one text keep their original zone in brackets, so two marks
    never read the same."""
    mode = normalize_mode(mode)
    if mode == LOCAL or not values:
        return {}
    target = zone_from_label(mode)
    out: dict[str, str] = {}
    for label, ts in values.items():
        if ts.tzinfo is None:
            continue  # nothing to convert from; reads as it is
        shown = ts.tz_convert(target)
        out[label] = _label(shown.tz_localize(None), strftime, _zone_text(shown, False))
    texts: dict[str, list[str]] = {}
    for label, text in out.items():
        texts.setdefault(text, []).append(label)
    for text, labels in texts.items():
        if len(labels) > 1:
            for label in labels:
                out[label] = f"{text} (from {zone_label(values[label])})"
    return out


# ---------------------------------------------------------------------------
# The derived table
# ---------------------------------------------------------------------------


def temporal_measures(table) -> list[str]:
    return [m.name for m in table.measures if getattr(m, "temporal", False)]


def apply_temporal(spec, table):
    """The fourth derived table: a temporal y measure rewritten to axis ms in
    ``spec.time_zone``. Returns ``(table, TemporalAxis | None)``; the table
    is unchanged (same object) when the y measure is not temporal, so a plot
    without datetimes pays nothing.

    The measure keeps ``temporal=True`` on its ``MeasureInfo`` (now holding
    numbers), which is how capability, validation, compare and the renderers
    know the axis is time.
    """
    from dataclasses import replace

    measure = spec.y_measure
    if measure not in temporal_measures(table) or measure not in table.frame.columns:
        return table, None
    column = table.frame[measure]
    if pd.api.types.is_float_dtype(column.dtype):  # already converted (derived twice)
        ms, report = column.to_numpy(dtype="float64"), TemporalReport(mode=normalize_mode(spec.time_zone))
    else:
        ms, report = wall_clock_ms(column, spec.time_zone)
    frame = table.frame.copy()
    frame[measure] = ms
    finite = ms[np.isfinite(ms)]
    span = float(finite.max() - finite.min()) if finite.size else 0.0
    strftime, d3 = tick_format(span)
    axis = TemporalAxis(
        mode=normalize_mode(spec.time_zone),
        strftime=strftime,
        d3=d3,
        notes=tuple(report.notes(measure)),
    )
    Log.info(
        "temporal: %s -> wall-clock ms in %s (%d value(s): %d aware in %s, %d naive), "
        "tick format %r",
        measure, zone_title(axis.mode), report.n_values, report.n_aware,
        list(report.zones) or "no zone", report.n_naive, strftime, layer=LAYER,
    )
    return replace(table, frame=frame), axis


def time_zone_summary(spec, table) -> dict:
    """What the Plot Studio's Time zone control needs: whether a datetime
    is in play (the plotted measure, or any factor), the current mode, and
    the zones the data holds (offered first in the zone picker)."""
    measure = spec.y_measure if spec.measures else None
    temporal_measure = bool(measure) and measure in temporal_measures(table)
    factors = [f.name for f in table.factors if getattr(f, "temporal", False)]
    zones: dict[str, None] = {}
    if temporal_measure and measure in table.frame.columns:
        column = table.frame[measure]
        if pd.api.types.is_datetime64_any_dtype(column.dtype):
            if column.dt.tz is not None:
                zones[zone_label(pd.Timestamp(0, tz=column.dt.tz)) or "UTC"] = None
        else:
            for value in column.to_numpy():
                if is_temporal_value(value):
                    label = zone_label(pd.Timestamp(value))
                    if label:
                        zones[label] = None
    for name in factors:
        for ts in (table.factor(name).temporal_values or {}).values():
            label = zone_label(ts)
            if label:
                zones[label] = None
    try:
        mode = normalize_mode(spec.time_zone)
        problem = None
    except ValueError as exc:
        mode, problem = spec.time_zone, str(exc)
    return {
        "applies": temporal_measure or bool(factors),
        "measure": temporal_measure,
        "factors": factors,
        "mode": mode,
        "zones": sorted(zones),
        "problem": problem,
    }
