"""
Y-axis limits, split by a chosen set of factors.

The question this answers is not "what is the range of this panel" — that is
easy, and it is what every panel already knew. It is **"what range should this
panel share with which other panels"**, and that is a property of the whole
figure set rather than of any one figure.

Which makes it awkward, because the interactive path deliberately builds one
figure at a time (``reduce.resolve_one``): a scope of ``[]`` — one range for
every panel of every figure — needs data from figures nobody has asked for.
Building them to find out would cost exactly what Stage 3 just removed.

So the limits are computed **off the table, not off the figures**, before any
reduction happens, in one pass:

* the drawn extent of a line/scatter/box/violin is the raw extent of the data,
  so for those it is a min/max over the unexploded arrays — 48 rows of numpy
  work for a figure set that would otherwise be 17 million;
* a band or a bar draws ``centre ± spread``, which genuinely needs the
  reduction — but only its extremes, so it is one groupby on the sample index,
  with no panel frames, no series keys and no sorting.

Both cover the whole fan-out at once, which is the point: paging through thirty
subjects must not recompute anything.
"""

from __future__ import annotations

from typing import Any, Iterable

import numpy as np
import pandas as pd
from scistacklog import Log

from .shape import Shape
from .spec import ErrorBand, PlotKind, PlotSpec, Role, Statistic
from .table import LongTable

LAYER = "scistackplot"

#: Fraction of the data range left as breathing room at each end. Matches what
#: ``reduce._shared_limits`` has always done, so switching to a scope does not
#: silently re-pad every existing figure.
PAD_FRACTION = 0.05

#: The key of the single group when nothing separates the limits.
GLOBAL_KEY: tuple = ()


def eligible_scope(
    scope: Iterable[str],
    roles: dict[str, Role],
    table: LongTable,
) -> list[str]:
    """The requested scope, less anything that cannot separate a y axis.

    Only ITERATE (one figure per level) and FACET (one panel per level) split
    panels apart. A COLOR or FREE factor lives *inside* a panel, so asking to
    separate limits by it asks one axis for two ranges — there is no figure that
    could satisfy it.

    Dropped entries are logged rather than raised on: a scope is a user's
    checkbox state, and a factor that was FACET a moment ago and is COLOR now
    should quietly stop separating limits instead of replacing the figure with
    an error. The GUI offers only eligible factors anyway; this is the guard for
    a spec that arrives from TOML, from an older session, or from a role the
    user has since changed.
    """
    wanted = list(dict.fromkeys(scope))
    if not wanted:
        return []

    allowed = {
        name
        for name, role in roles.items()
        if role in (Role.ITERATE, Role.FACET) and table.has_factor(name)
    }
    kept = [name for name in wanted if name in allowed]

    dropped = [name for name in wanted if name not in allowed]
    if dropped:
        Log.warn(
            "y-limit scope ignores %s: only factors that separate PANELS "
            "(iterate, facet) can separate y limits — %s. Limits will be shared "
            "across %s.",
            dropped,
            {name: str(roles.get(name, "no role")) for name in dropped},
            dropped,
            layer=LAYER,
        )
    return kept


def limits_by_scope(
    table: LongTable,
    spec: PlotSpec,
    scope: list[str],
) -> dict[tuple, tuple[float, float]]:
    """``{scope values: (low, high)}`` for every group the scope names.

    ``scope`` must already be :func:`eligible_scope`-filtered. An empty scope
    returns exactly one entry, keyed ``()`` — one range for the whole dataset.

    The frame is whatever the caller hands over, which for the resolve path is
    the **post-variant, post-filter** table: limits must describe what will be
    drawn, so a variant selection that removes half the data has to move them.
    """
    frame = table.frame
    measure = spec.y_measure
    if measure not in frame.columns or frame.empty:
        return {}

    present = [name for name in scope if name in frame.columns]
    if _needs_reduction(spec):
        extents = _aggregated_extents(frame, spec, table, present)
    else:
        extents = _raw_extents(frame, measure, present)

    limits = {key: _padded(low, high) for key, (low, high) in extents.items()}
    Log.debug(
        "y limits over %s: %d group(s)",
        present or "the whole dataset",
        len(limits),
        layer=LAYER,
    )
    return limits


def limits_for(
    limits: dict[tuple, tuple[float, float]],
    key: dict[str, Any],
    scope: list[str],
    y_axis,
) -> tuple[float, float] | None:
    """The limits one panel draws: its group's range, with overrides applied.

    ``key`` is everything identifying the panel — its figure's ITERATE values
    merged with its own FACET values — and the scope picks the part that
    matters. A panel whose group is missing (a combination the data does not
    hold) falls back to the global range if there is one, because a panel with
    no limits at all silently autoscales and would be the one panel on the page
    that cannot be compared with the others.

    An override end always wins; both ends override means the data is never
    consulted, which is what lets a manual range be set on an empty figure.
    """
    if y_axis.is_manual:
        # _ordered here too: BOTH ends typed by hand is exactly the case where a
        # swapped pair reaches an axis untouched, because this path never
        # consults the data and so never passed through the ordering below.
        return _ordered(float(y_axis.minimum), float(y_axis.maximum))

    group = tuple(key.get(name) for name in scope)
    found = limits.get(group)
    if found is None and scope:
        found = limits.get(GLOBAL_KEY)
    if found is None:
        # Nothing computed and no global fallback: with one end pinned there is
        # still nothing to pin it against, so let the renderer autoscale rather
        # than invent the other end.
        return None

    low, high = found
    if y_axis.minimum is not None:
        low = float(y_axis.minimum)
    if y_axis.maximum is not None:
        high = float(y_axis.maximum)
    return _ordered(low, high)


def describe(scope: list[str], y_axis) -> str:
    """One line naming the rule, for the panel and the log.

    A number on an axis that cannot be traced to a rule is indistinguishable
    from a bug — "why is this 0.61?" has to have an answer the user can read.
    """
    if y_axis.is_manual:
        return "set by hand"
    rule = "the same everywhere" if not scope else f"per {', '.join(scope)}"
    if y_axis.minimum is not None:
        rule += ", floor set by hand"
    if y_axis.maximum is not None:
        rule += ", ceiling set by hand"
    return rule


# ---------------------------------------------------------------------------
# Extents
# ---------------------------------------------------------------------------


def _needs_reduction(spec: PlotSpec) -> bool:
    """Whether the drawn extent differs from the raw extent.

    Only for the kinds that draw a summary: a band or a bar draws
    ``centre ± spread``, which can sit outside the data (a mean plus an SD) and
    inside it (a mean of anything). Everything else draws the observations, or
    box statistics that lie within them.
    """
    return spec.kind in (PlotKind.BAND, PlotKind.BAR) and (
        spec.aggregate.error is not ErrorBand.NONE
    )


def _raw_extents(
    frame: pd.DataFrame, measure: str, scope: list[str]
) -> dict[tuple, tuple[float, float]]:
    """Min/max per group, **without exploding anything**.

    A 1-D measure is one array per cell, and the extent of a group of arrays is
    the extent of their per-cell extents. Computing that cell by cell is 24
    numpy reductions over arrays that already exist, where exploding first would
    build 8.9 million rows to answer the same question.
    """
    values = frame[measure]
    lows, highs = _cell_extents(values)

    usable = ~(np.isnan(lows) | np.isnan(highs))
    if not usable.any():
        return {}

    if not scope:
        return {GLOBAL_KEY: (float(lows[usable].min()), float(highs[usable].max()))}

    keys = _scope_keys(frame, scope)
    extents: dict[tuple, tuple[float, float]] = {}
    for key, low, high, ok in zip(keys, lows, highs, usable, strict=True):
        if not ok:
            continue
        seen = extents.get(key)
        extents[key] = (
            (low, high) if seen is None else (min(seen[0], low), max(seen[1], high))
        )
    # The global entry always exists as a fallback for a panel whose own group
    # has no data (`limits_for`), and it is free: these are the same numbers.
    if extents:
        extents[GLOBAL_KEY] = (
            min(low for low, _ in extents.values()),
            max(high for _, high in extents.values()),
        )
    return extents


def _cell_extents(values: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """Per-row (min, max) of a measure column that may hold scalars or arrays."""
    numeric = pd.to_numeric(values, errors="coerce")
    if not numeric.isna().all():
        # A plain scalar column: its own values are the extents, and this is one
        # vectorized cast rather than a Python loop.
        column = numeric.to_numpy(dtype=float, na_value=np.nan)
        return column, column

    lows = np.full(len(values), np.nan)
    highs = np.full(len(values), np.nan)
    for position, value in enumerate(values.to_numpy()):
        array = _as_array(value)
        # An all-NaN cell stays NaN rather than going through nanmin, which
        # warns and returns NaN anyway — the caller drops it either way, and a
        # RuntimeWarning per empty trial is noise in a real run.
        if array is None or not array.size or np.isnan(array).all():
            continue
        lows[position] = np.nanmin(array)
        highs[position] = np.nanmax(array)
    return lows, highs


def _as_array(value: Any) -> np.ndarray | None:
    """One cell as floats, or None when it holds nothing numeric."""
    if value is None or isinstance(value, (str, bytes)):
        return None
    if isinstance(value, (list, tuple, np.ndarray)):
        try:
            return np.asarray(value, dtype="float64")
        except (TypeError, ValueError):
            return None
    try:
        return np.asarray([float(value)])
    except (TypeError, ValueError):
        return None


def _aggregated_extents(
    frame: pd.DataFrame,
    spec: PlotSpec,
    table: LongTable,
    scope: list[str],
) -> dict[tuple, tuple[float, float]]:
    """Extents of ``centre ± spread`` — the band a BAND/BAR actually draws.

    This is the expensive branch and it is deliberately narrow: it reduces to
    the same statistic ``reduce._summarize`` does, grouped by everything that
    separates an x position, and then takes extremes. No panel frames, no series
    keys, no sorting, and one pass for the whole fan-out rather than one per
    figure.
    """
    measure = spec.y_measure
    working = frame
    index_column = spec.index_column or table.index_column

    if table.shape_of(measure) is Shape.SERIES_1D and not table.measure(measure).exploded:
        working, index_column = _explode_for_limits(frame, measure, index_column)

    # What separates one drawn point from another: the scope (which separates
    # panels), plus the x position and the colour within a panel.
    grouping = [
        name
        for name in dict.fromkeys(
            [
                *scope,
                *[n for n, r in spec.roles.items() if r in (Role.X, Role.COLOR)],
                *( [index_column] if index_column else [] ),
            ]
        )
        if name in working.columns
    ]
    values = pd.to_numeric(working[measure], errors="coerce")
    working = working.assign(**{measure: values}).dropna(subset=[measure])
    if working.empty:
        return {}
    if not grouping:
        centre, low, high = _summary_bounds(working[measure], spec)
        return {GLOBAL_KEY: (float(min(low, centre)), float(max(high, centre)))}

    grouped = working.groupby(grouping, dropna=False, sort=False)[measure]
    centre = grouped.median() if spec.aggregate.statistic is Statistic.MEDIAN else grouped.mean()
    low, high = _spread(grouped, centre, spec)

    bounds = pd.DataFrame({"low": low, "high": high}).reset_index()
    if not scope:
        return {GLOBAL_KEY: (float(bounds["low"].min()), float(bounds["high"].max()))}

    by_scope = bounds.groupby(scope, dropna=False, sort=False)
    extents = {
        _as_key(key): (float(part["low"].min()), float(part["high"].max()))
        for key, part in by_scope
    }
    if extents:
        extents[GLOBAL_KEY] = (
            min(low for low, _ in extents.values()),
            max(high for _, high in extents.values()),
        )
    return extents


def _explode_for_limits(
    frame: pd.DataFrame, measure: str, index_column: str | None
) -> tuple[pd.DataFrame, str]:
    """A minimal explode: the measure, the index, and the factors — nothing else.

    Deliberately not ``reduce._explode_1d``: that one logs at INFO (it is the
    headline cost of a resolve) and raises when the index column already exists.
    Here an existing index column just means the caller pre-exploded.
    """
    column = index_column or "index"
    if column in frame.columns:
        return frame, column
    working = frame.copy()
    working[column] = working[measure].map(
        lambda v: list(range(len(v))) if isinstance(v, (list, tuple, np.ndarray)) else []
    )
    return working.explode([measure, column], ignore_index=True), column


def _spread(grouped, centre, spec: PlotSpec):
    """``(low, high)`` per group — the same definitions ``reduce._summarize`` uses.

    Kept deliberately parallel to that function: two definitions of an error
    band would put the limits and the drawing at odds, and the symptom would be
    a band clipped by its own axis.
    """
    error = spec.aggregate.error
    if error is ErrorBand.IQR:
        return grouped.quantile(0.25), grouped.quantile(0.75)
    sd = grouped.std(ddof=1).fillna(0.0)
    count = grouped.count()
    if error is ErrorBand.SD:
        spread = sd
    elif error is ErrorBand.SEM:
        spread = sd / np.sqrt(count.where(count > 0, 1))
    else:  # CI95
        spread = 1.96 * sd / np.sqrt(count.where(count > 0, 1))
    return centre - spread, centre + spread


def _summary_bounds(values: pd.Series, spec: PlotSpec):
    """The ungrouped case: one centre and one band over everything."""
    centre = (
        values.median() if spec.aggregate.statistic is Statistic.MEDIAN else values.mean()
    )
    error = spec.aggregate.error
    if error is ErrorBand.IQR:
        return centre, values.quantile(0.25), values.quantile(0.75)
    sd = values.std(ddof=1)
    sd = 0.0 if pd.isna(sd) else sd
    count = max(len(values), 1)
    if error is ErrorBand.SD:
        spread = sd
    elif error is ErrorBand.SEM:
        spread = sd / np.sqrt(count)
    else:
        spread = 1.96 * sd / np.sqrt(count)
    return centre, centre - spread, centre + spread


# ---------------------------------------------------------------------------
# Keys and padding
# ---------------------------------------------------------------------------


def _scope_keys(frame: pd.DataFrame, scope: list[str]) -> list[tuple]:
    """One key tuple per row, matching what a panel's ``key`` will produce."""
    columns = [frame[name].to_numpy() for name in scope]
    return list(zip(*columns, strict=True)) if len(columns) > 1 else [
        (value,) for value in columns[0]
    ]


def _as_key(key: Any) -> tuple:
    return key if isinstance(key, tuple) else (key,)


def _ordered(low: float, high: float) -> tuple[float, float]:
    """Bounds in the order an axis wants them.

    A hand-typed minimum above the computed maximum is a typo, not an inverted
    axis: matplotlib would silently flip the axis and the figure would read
    upside down with nothing to say why.

    Swapping is reported rather than done quietly — the user typed one of those
    two numbers and the figure is about to disagree with it, so the log is the
    only place that can say which way round the axis actually ended up.
    """
    if low <= high:
        return (low, high)
    Log.warn(
        "y limits arrived inverted (%s above %s) — drawing them the other way "
        "round rather than flipping the axis",
        low,
        high,
        layer=LAYER,
    )
    return (high, low)


def _padded(low: float, high: float) -> tuple[float, float]:
    if low == high:
        pad = abs(low) * PAD_FRACTION or 1.0
        return (low - pad, high + pad)
    pad = (high - low) * PAD_FRACTION
    return (low - pad, high + pad)
