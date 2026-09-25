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
* a band or a bar draws ``centre ± spread`` over the sample, and a collapse
  chain draws the pre-collapsed rows (the sample, for every kind); both
  genuinely need the reduction — but only its extremes,
  so it is one grouped pass with no panel frames and no sorting.

Both cover the whole fan-out at once, which is the point: paging through thirty
subjects must not recompute anything.

**The one rule that keeps limits honest** (2026-09-14): the statistic is
computed at the granularity it is *drawn* at — one ``centre ± spread`` per
panel, per colour, per position, where a panel is one combination of every
ITERATE and FACET factor — and the scope only decides how those per-panel
extents are *combined*. It used to be computed at scope granularity instead:
unticking ``subject`` pooled every subject into one mean ± SEM whose range
collapsed around the grand mean, and every subject's own band fell outside it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd
from scistacklog import Log

from .numeric import coerce_numeric
from .shape import Shape
from .spec import ErrorBand, PlotKind, PlotSpec, Role, Statistic
from .table import LongTable

LAYER = "scistackplot"

#: Fraction of the data range left as breathing room at each end. Matches what
#: ``reduce._shared_limits`` has always done, so switching to a scope does not
#: silently re-pad every existing figure. On a log axis the same fraction is
#: applied to the log10 range.
PAD_FRACTION = 0.05

#: The key of the single group when nothing separates the limits.
GLOBAL_KEY: tuple = ()


@dataclass(frozen=True)
class ExtentMode:
    """What the y limits depend on *beyond* the rows a plan selected.

    A plan is keyed by the data question (measure, roles, filters, variants —
    ``reduce._plan_cache_key``) and deliberately NOT by the plot kind, because
    a kind switch must not re-run the variants, filters and fan-out grouping.
    But the limits DO depend on the kind: a band draws ``centre ± spread``,
    which sits outside the raw data, and a bar rises from zero. This is the
    part of the answer that the plan cannot share across kinds, spelled out as
    a hashable key so ``reduce`` can memoise the limits per mode instead of
    either rebuilding the plan or — as it did until 2026-09-14 — drawing a
    band inside a line plot's limits.
    """

    #: BAND/BAR: the drawn extent is ``centre ± spread`` (just the centre with
    #: no error band) — never the sample rows themselves.
    summary: bool
    #: Any COLLAPSE role: the collapse chain runs before anything is drawn,
    #: so the drawn values are means, not the observations. Raw extents would
    #: only be loose, but it is one path.
    collapse: bool
    #: BAR on a linear axis: bars rise from zero, so zero is always in view.
    #: Never on a log axis — there is no zero to rise from, and folding it in
    #: would put log10(0) on the axis.
    from_zero: bool
    #: Log y axis: only positive values can be drawn, and padding is geometric.
    log: bool
    #: "Show sample" (``PlotSpec.show_sample``): the overlay's points are drawn
    #: too, at a granularity the marks' chain never reaches (raw cycles over a
    #: bar of subject means), so the limits must include them. The checked
    #: names, when the kind can carry an overlay at all; ``()`` otherwise.
    overlay: tuple[str, ...] = ()

    @classmethod
    def for_spec(cls, spec: PlotSpec, roles: dict[str, Role]) -> "ExtentMode":
        from .roles import has_sample, overlay_unavailable

        sample = has_sample(roles)
        return cls(
            # With or without a band: a bar/band draws the sample's CENTRE,
            # never the sample rows. Gating this on `error is not NONE` (until
            # 2026-09-24) sent a band-less bar down the raw path — its limits
            # spanned the per-sample means, looser than the bars at both ends,
            # and the exported figure (fitted to the bars) disagreed.
            # `spread_bounds` / `position_stats` return centre..centre for NONE.
            summary=spec.kind in (PlotKind.BAND, PlotKind.BAR),
            collapse=sample,
            from_zero=spec.kind is PlotKind.BAR and not spec.style.log_y,
            log=bool(spec.style.log_y),
            # The shape is not known here; `_reduced_extents` skips a 1-D
            # measure itself. A key that only misses the memo is cheap — a
            # figure drawn inside limits that ignore its points is not.
            overlay=(
                tuple(spec.show_sample)
                if spec.show_sample
                and overlay_unavailable(spec, roles, Shape.SCALAR) is None
                else ()
            ),
        )

    @property
    def reduces(self) -> bool:
        """Whether the drawn extent differs from the raw extent at all."""
        return self.summary or self.collapse

    def describe(self) -> str:
        parts = ["summary" if self.summary else "raw"]
        if self.collapse:
            parts.append("collapsed")
        if self.from_zero:
            parts.append("from zero")
        if self.log:
            parts.append("log")
        if self.overlay:
            parts.append("sample overlay " + ", ".join(self.overlay))
        return ", ".join(parts)


def eligible_scope(
    scope: Iterable[str],
    roles: dict[str, Role],
    table: LongTable,
) -> list[str]:
    """The requested scope, less anything that cannot separate a y axis.

    Only ITERATE (one figure per level) and FACET (one panel per level) split
    panels apart. A GROUP or COLLAPSE factor lives *inside* a panel, so asking
    to separate limits by it asks one axis for two ranges — there is no figure
    that could satisfy it.

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


def panel_factors(roles: dict[str, Role], frame: pd.DataFrame) -> list[str]:
    """Every factor that puts data in a *different panel* — ITERATE and FACET
    — in role order, restricted to what the frame still holds.

    This is the granularity a statistic is drawn at, and therefore the
    granularity it must be computed at for the limits. The scope is always a
    subset of it (``eligible_scope``), so folding panel extents into scope
    groups is a projection of the panel key.
    """
    return [
        name
        for name, role in roles.items()
        if role in (Role.ITERATE, Role.FACET) and name in frame.columns
    ]


def limits_by_scope(
    table: LongTable,
    spec: PlotSpec,
    scope: list[str],
    roles: dict[str, Role],
) -> dict[tuple, tuple[float, float]]:
    """``{scope values: (low, high)}`` for every group the scope names.

    ``scope`` must already be :func:`eligible_scope`-filtered, and ``roles``
    are the COMPLETED roles (``roles.complete_roles``) — a defaulted colour or
    facet is as real to the drawing as a declared one, and must be as real
    here. An empty scope returns exactly one entry, keyed ``()`` — one range
    for the whole dataset.

    The frame is whatever the caller hands over, which for the resolve path is
    the **post-variant, post-filter** table: limits must describe what will be
    drawn, so a variant selection that removes half the data has to move them.

    This is the pandas reference; ``reducer.NumpyReducer.y_extents`` answers
    the nested 1-D cases over ndarray cells and is held to it by
    ``scistackplotdb/tests/test_reducer_parity.py``.
    """
    frame = table.frame
    measure = spec.y_measure
    if measure not in frame.columns or frame.empty:
        return {}

    mode = ExtentMode.for_spec(spec, roles)
    present = [name for name in scope if name in frame.columns]
    # A 2-D measure never runs the collapse chain (its panel is `matrix_mean`,
    # which pools), so a collapsed factor on a heatmap leaves the raw extents
    # — loose, but the matrices' own — rather than a chain that cannot average
    # object cells.
    if mode.reduces and table.shape_of(measure) is not Shape.MATRIX_2D:
        extents = _reduced_extents(frame, spec, table, roles, present, mode)
    else:
        extents = _raw_extents(frame, measure, present, mode)
    return finish_extents(extents, present, mode, source="pandas")


def finish_extents(
    extents: dict[tuple, tuple[float, float]],
    scope: list[str],
    mode: ExtentMode,
    *,
    source: str,
    panels: int | None = None,
) -> dict[tuple, tuple[float, float]]:
    """Per-scope-group extents -> the limits the panels draw.

    One tail for both reducers: the global fallback entry, zero for a bar
    chart, padding, and the log line. Kept here rather than in each reducer so
    the two cannot pad differently.
    """
    if not extents:
        Log.debug("y limits (%s, %s): no drawable values", mode.describe(), source, layer=LAYER)
        return {}
    folded = dict(extents)
    if scope:
        # The global entry always exists as a fallback for a panel whose own
        # group has no data (`limits_for`), and it is free: same numbers.
        folded[GLOBAL_KEY] = (
            min(low for low, _ in extents.values()),
            max(high for _, high in extents.values()),
        )
    if mode.from_zero:
        folded = {key: (min(low, 0.0), max(high, 0.0)) for key, (low, high) in folded.items()}

    limits = {key: _padded(low, high, mode) for key, (low, high) in folded.items()}
    Log.info(
        "y limits (%s, %s): %d group(s) over %s%s",
        mode.describe(),
        source,
        len(limits) - (1 if scope else 0),
        scope or "the whole dataset",
        f" from {panels} panel(s)" if panels is not None else "",
        layer=LAYER,
    )
    # Capped: one line per group was 95,585 calls (~7 s) for an 80-field
    # fan-out of 1,195 figures (2026-09-25). The first few say what the rule
    # did; the INFO line above already has the count.
    for key, (low, high) in list(limits.items())[:_DEBUG_GROUPS_SHOWN]:
        Log.debug("  y limits %s: %.6g to %.6g", key or "(global)", low, high, layer=LAYER)
    if len(limits) > _DEBUG_GROUPS_SHOWN:
        Log.debug(
            "  y limits: … %d more group(s) not listed",
            len(limits) - _DEBUG_GROUPS_SHOWN,
            layer=LAYER,
        )
    return limits


#: How many per-group limits the DEBUG log lists before summarising the rest.
_DEBUG_GROUPS_SHOWN = 20


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

    group = scope_key({name: key.get(name) for name in scope}, scope)
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
# Keys
# ---------------------------------------------------------------------------


def hashable(value: Any) -> Any:
    """A group-key component: NaN as None, so every missing value is ONE group.

    pandas' ``groupby(dropna=False)`` keeps a NaN level as one group but hands
    it back as ``nan``, and ``nan != nan`` — so a NaN-keyed entry written by
    the extents could never be found by ``limits_for``. Both sides go through
    this, so a missing level is a level like any other.
    """
    if isinstance(value, float) and value != value:
        return None
    return value


def scope_key(values: dict[str, Any], scope: list[str]) -> tuple:
    """The dict key of one scope group, from a panel's identifying values."""
    return tuple(hashable(values.get(name)) for name in scope)


def _as_key(key: Any) -> tuple:
    key = key if isinstance(key, tuple) else (key,)
    return tuple(hashable(v) for v in key)


# ---------------------------------------------------------------------------
# Extents
# ---------------------------------------------------------------------------


def merge_extent(
    extents: dict[tuple, tuple[float, float]], key: tuple, low: float, high: float
) -> None:
    """Widen ``extents[key]`` to include ``(low, high)``."""
    seen = extents.get(key)
    extents[key] = (
        (low, high) if seen is None else (min(seen[0], low), max(seen[1], high))
    )


def pair_extent(
    low: np.ndarray, high: np.ndarray, mode: ExtentMode
) -> tuple[float, float] | None:
    """The extent of a set of drawn ``(low, high)`` pairs, or None if nothing
    in it can be drawn.

    On a log axis the floor is the smallest POSITIVE drawn value: a band whose
    lower edge crosses zero is drawn clipped there anyway, so its floor is
    wherever it re-enters the positive half-plane — its centre, if the whole
    lower edge is below zero. Never a negative number, which the axis would
    refuse, and never an invented one.
    """
    low = np.asarray(low, dtype=float)
    high = np.asarray(high, dtype=float)
    if mode.log:
        floor = np.where(low > 0, low, np.where(high > 0, high, np.nan))
        ceiling = np.where(high > 0, high, np.nan)
    else:
        floor, ceiling = low, high
    ok = ~(np.isnan(floor) | np.isnan(ceiling))
    if not ok.any():
        return None
    return float(floor[ok].min()), float(ceiling[ok].max())


def _raw_extents(
    frame: pd.DataFrame, measure: str, scope: list[str], mode: ExtentMode
) -> dict[tuple, tuple[float, float]]:
    """Min/max per group, **without exploding anything**.

    A 1-D measure is one array per cell, and the extent of a group of arrays is
    the extent of their per-cell extents. Computing that cell by cell is 24
    numpy reductions over arrays that already exist, where exploding first would
    build 8.9 million rows to answer the same question.
    """
    values = frame[measure]
    lows, highs = _cell_extents(values, mode)

    usable = ~(np.isnan(lows) | np.isnan(highs))
    dropped = int((~usable).sum())
    if dropped:
        # INFO, not WARN: an empty trial is legitimate. But 17,640 of 21,420
        # cells setting no limit is the whole story of a figure drawn on the
        # wrong scale, and it was invisible until this line existed.
        Log.info(
            "y limits: %d of %d cell(s) of %r hold nothing numeric and set no limit",
            dropped,
            len(lows),
            measure,
            layer=LAYER,
        )
    if not usable.any():
        return {}

    if not scope:
        return {GLOBAL_KEY: (float(lows[usable].min()), float(highs[usable].max()))}

    # One groupby, not a Python loop over rows: 928,720 melted rows took
    # ~11.5 s row by row (2026-09-25, SymmetryTable, 80 fields). `observed`
    # keeps an unused category from becoming an all-NaN group; `dropna=False`
    # keeps a missing level as one group, which `_as_key` maps to None exactly
    # as `hashable` does for `limits_for`.
    work = pd.DataFrame({name: frame[name].to_numpy() for name in scope})
    work["__low"] = lows
    work["__high"] = highs
    folded = (
        work[usable]
        .groupby(scope, dropna=False, sort=False, observed=True)
        .agg(low=("__low", "min"), high=("__high", "max"))
    )
    return {
        _as_key(key): (float(low), float(high))
        for key, low, high in zip(
            folded.index, folded["low"].to_numpy(), folded["high"].to_numpy(), strict=True
        )
    }


def _cell_extents(values: pd.Series, mode: ExtentMode) -> tuple[np.ndarray, np.ndarray]:
    """Per-row (min, max) of a measure column that may hold scalars or arrays.

    Decided per CELL, not per column. A melted struct variable is one column
    holding every field, and fields differ: `GAITRiteLoaded` keeps 9 per-trial
    scalars next to 42 per-step arrays (2026-09-14). Deciding once for the
    column — "some cell is a scalar, so cast the column" — turned every array
    cell into NaN, so 42 of 51 panels had no extent of their own, fell back to
    the global range, agreed with each other, and were drawn LINKED on the 9
    scalar fields' scale.

    The scalar cells still go through one vectorised cast; only the cells that
    cast produced NaN for are visited in Python.
    """
    numeric = coerce_numeric(values)
    column = numeric.to_numpy(dtype=float, na_value=np.nan)
    if mode.log:
        column = np.where(column > 0, column, np.nan)
    lows = column.copy()
    highs = column.copy()

    raw = values.to_numpy()
    for position in np.flatnonzero(np.isnan(numeric.to_numpy(dtype=float, na_value=np.nan))):
        array = _as_array(raw[position])
        if array is None or not array.size:
            continue
        if mode.log:
            array = array[array > 0]
        # An all-NaN cell stays NaN rather than going through nanmin, which
        # warns and returns NaN anyway — the caller drops it either way, and a
        # RuntimeWarning per empty trial is noise in a real run.
        if not array.size or np.isnan(array).all():
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


def _reduced_extents(
    frame: pd.DataFrame,
    spec: PlotSpec,
    table: LongTable,
    roles: dict[str, Role],
    scope: list[str],
    mode: ExtentMode,
) -> dict[tuple, tuple[float, float]]:
    """Extents of what a reducing plot draws, at the granularity it draws it.

    Mirrors the figure path step for step — explode, the collapse chain
    (``roles.collapse_steps``: the pre-collapse, leaving the sample), then ``centre ± spread`` per panel, per mark — and
    only then folds the result into scope groups. The panel factors are ALWAYS
    in the grouping whether or not the scope names them: the scope decides
    which panels share a range, never what statistic each panel draws.

    Deliberately narrow all the same: no panel frames, no composed keys, no
    sorting, and one pass for the whole fan-out rather than one per figure.
    """
    # Imported here: `reduce` imports this module.
    from .reduce import _collapse_levels
    from .roles import collapse_steps, overlay_steps

    measure = spec.y_measure
    working = frame
    index_column = spec.index_column or table.index_column

    if table.shape_of(measure) is Shape.SERIES_1D and not table.measure(measure).exploded:
        working, index_column = _explode_for_limits(frame, measure, index_column)

    values = coerce_numeric(working[measure])
    working = working.assign(**{measure: values}).dropna(subset=[measure])
    if working.empty:
        return {}
    # The "Show sample" points, from the rows BEFORE the marks' chain — the
    # same cut `reduce._build_figure` draws — folded by scope like any raw
    # extent and unioned into whatever the marks' own extent comes to below.
    overlay_extents: dict[tuple, tuple[float, float]] = {}
    if mode.overlay and table.shape_of(measure) is Shape.SCALAR:
        overlay = overlay_steps(spec, roles, table)
        if overlay is not None:
            shown = _collapse_levels(
                working, overlay.averaged, spec, table, index_column,
                pooled=spec.aggregate.pooled,
            )
            overlay_extents = _raw_extents(shown, measure, scope, mode)
            Log.debug(
                "y limits: sample overlay (%s) over %d row(s) -> %s",
                " x ".join(overlay.shown), len(shown), overlay_extents, layer=LAYER,
            )
    if mode.collapse:
        steps = collapse_steps(spec, roles, table)
        working = _collapse_levels(working, steps.pre, spec, table, index_column)
        working = _collapse_levels(working, steps.final, spec, table, index_column)

    panels = panel_factors(roles, working)
    if not mode.summary:
        # What the chain left is drawn as it is — the sample rows, by every
        # kind that does not summarise them: the extent is the values',
        # folded by scope. `scope` ⊆ `panels` ⊆ the chain's kept columns.
        return _union_extents(_raw_extents(working, measure, scope, mode), overlay_extents)

    # What separates one drawn mark from another: the panel, plus every
    # grouping layer within it (tick, series and colour alike), plus the
    # position of a 1-D measure. The rows sharing all of those are the sample.
    grouping = [
        name
        for name in dict.fromkeys(
            [
                *panels,
                *[n for n, r in roles.items() if r is Role.GROUP],
                *([index_column] if index_column else []),
            ]
        )
        if name in working.columns
    ]
    if not grouping:
        centre, low, high = _summary_bounds(working[measure], spec)
        extent = pair_extent(
            np.asarray([min(low, centre)]), np.asarray([max(high, centre)]), mode
        )
        return _union_extents({GLOBAL_KEY: extent} if extent else {}, overlay_extents)

    grouped = working.groupby(grouping, dropna=False, sort=False)[measure]
    centre = grouped.median() if spec.aggregate.statistic is Statistic.MEDIAN else grouped.mean()
    low, high = spread_bounds(grouped, centre, spec)
    # The centre line is drawn too, and a MEAN with an IQR band can sit outside
    # its own quartiles on skewed data — the band was covered, the line not.
    bounds = pd.DataFrame(
        {"low": np.minimum(low, centre), "high": np.maximum(high, centre)}
    ).reset_index()
    return _union_extents(_fold(bounds, scope, mode), overlay_extents)


def _union_extents(
    base: dict[tuple, tuple[float, float]], extra: dict[tuple, tuple[float, float]]
) -> dict[tuple, tuple[float, float]]:
    """``base`` widened by ``extra`` per scope key (``merge_extent``)."""
    if not extra:
        return base
    merged = dict(base)
    for key, (low, high) in extra.items():
        merge_extent(merged, key, low, high)
    return merged


def _fold(
    bounds: pd.DataFrame, scope: list[str], mode: ExtentMode
) -> dict[tuple, tuple[float, float]]:
    """``(low, high)`` rows with their scope columns -> one extent per group."""
    if not scope:
        extent = pair_extent(bounds["low"].to_numpy(), bounds["high"].to_numpy(), mode)
        return {GLOBAL_KEY: extent} if extent else {}
    extents: dict[tuple, tuple[float, float]] = {}
    for key, part in bounds.groupby(scope, dropna=False, sort=False):
        extent = pair_extent(part["low"].to_numpy(), part["high"].to_numpy(), mode)
        if extent:
            extents[_as_key(key)] = extent
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


def spread_bounds(grouped, centre, spec: PlotSpec):
    """``(low, high)`` per group — THE definition of an error band in pandas.

    ``reduce._summarize`` (the drawing) calls this too, so there is exactly one
    place that says what ``± SEM`` means; the numpy twin is
    ``series_stats.position_stats``, held to it by the parity suite.
    """
    error = spec.aggregate.error
    if error is ErrorBand.NONE:
        return centre, centre
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
    if error is ErrorBand.NONE:
        return centre, centre, centre
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


def _padded(low: float, high: float, mode: ExtentMode) -> tuple[float, float]:
    if mode.log:
        # Geometric: 5 % of the DECADES, so a 0.01-100 axis gets the same
        # breathing room at both ends instead of a floor pushed below zero.
        lo, hi = np.log10(low), np.log10(high)
        pad = (hi - lo) * PAD_FRACTION if hi > lo else 0.05
        return (float(10 ** (lo - pad)), float(10 ** (hi + pad)))
    if low == high:
        pad = abs(low) * PAD_FRACTION or 1.0
        return (low - pad, high + pad)
    pad = (high - low) * PAD_FRACTION
    return (low - pad, high + pad)
