"""The reduction contract: the per-SAMPLE work of a plot, behind one seam.

Every plot over a 1-D measure has a handful of steps that touch every sample
rather than every record — the y extents, one row per sample for a line, the
collapse chain's means at each position, mean ± error over the sample at each
position. Everything else in a resolve is per record or per drawn point
and is cheap. Those steps live behind this protocol so that HOW they are done
can change without the rest of ``reduce`` knowing.

Two implementations:

* :class:`PandasReducer` — the reference. It *calls* the functions the reduce
  path has always used (``_explode_1d``, ``_collapse_levels``,
  ``_summarize``, ``ylimits``), so its answers are the original answers by
  construction. It is the oracle every other reducer is tested against
  (``scistackplotdb/tests/test_reducer_parity.py``) and the reducer an
  in-memory CSV/DataFrame table gets by default. Keep it that way: a reference
  that starts computing its own version of a statistic has quietly become a
  second definition, and two definitions of an error band put the drawing and
  its axis at odds.
* :class:`NumpyReducer` — the fast one, over ndarray cells (``series_stats``).
  Same answers, no 174-million-row frame: measured 12–40× faster than DuckDB
  SQL for the same reductions and ~50× faster than the reference on the real
  data (``.claude/plan-plot-minimal-load-examples.md`` §8). Attached by
  ``scistackplotdb.ScidbSource``; it has no database dependency, so any source
  whose cells are arrays may use it.

A DuckDB-SQL reducer existed briefly (2026-09-13) and lost every measurement to
numpy once the fetch stopped boxing samples; it is gone. DuckDB SELECTS rows,
numpy REDUCES them.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import numpy as np
import pandas as pd
from scistacklog import Log

from .spec import PlotKind, PlotSpec, Role
from .table import LongTable
from .ylimits import hashable

LAYER = "scistackplot"


@runtime_checkable
class Reducer(Protocol):
    """Per-sample reductions a plot needs.

    Every method takes the frame it should reduce (already variant-selected
    and filtered by the caller — the reducer never re-derives a filter) and the
    spec that says how. It must honour the frame's *rows*: the set of records
    the caller has selected is the caller's decision.
    """

    def y_extents(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        table: LongTable,
        scope: list[str],
        roles: dict[str, Role],
    ) -> dict[tuple, tuple[float, float]]:
        """``{scope values: (low, high)}`` per group, plus the global key.

        Raw (min/max of what is drawn) or aggregated (centre ± spread, the band
        a BAND/BAR draws) according to the spec — the same rule
        ``ylimits.limits_by_scope`` applies.

        ``roles`` are the COMPLETED roles: the statistic is computed at the
        granularity it is drawn at — every ITERATE and FACET factor, declared
        or defaulted — and the scope only folds those panel extents together.
        """
        ...

    def explode_series(
        self,
        frame: pd.DataFrame,
        measure: str,
        index_column: str,
        table: LongTable,
        *,
        max_points: int | None = None,
    ) -> tuple[pd.DataFrame, str, int]:
        """``(frame, index_column, total)``: one row per sample of a 1-D measure.

        ``total`` is the row count before ``max_points`` striding; when
        ``max_points`` is given the returned frame is what ``downsample`` would
        have made of the full explode, so a reducer may keep only the kept
        rows' labels rather than gather every sample's.
        """
        ...

    def collapse_series(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        roles: dict[str, Role],
        index_column: str,
        table: LongTable,
    ) -> tuple[pd.DataFrame, str]:
        """The collapse chain of a 1-D measure for a kind that draws the
        sample itself (line, spaghetti): ``steps.pre``
        (``roles.collapse_steps``), mean per position at each step, returned
        EXPLODED (one row per sample level x kept factor combination x
        position) — ``_collapse_levels`` over the explode."""
        ...

    def summarize_series(
        self,
        group: pd.DataFrame,
        spec: PlotSpec,
        roles: dict[str, Role],
        index_column: str,
        table: LongTable,
    ) -> pd.DataFrame:
        """A BAND/BAR panel frame for one panel's rows of a 1-D measure:
        ``X`` (position), ``COLOR`` and ``SERIES`` if any, ``Y``, ``Y_LOW``,
        ``Y_HIGH`` — centre ± spread over the SAMPLE at every position,
        computed over ALL samples. The pre-collapse (``steps.pre``) is applied
        first, as the drawing path always has."""
        ...

    def downsample(
        self, frame: pd.DataFrame, max_points: int, index_column: str | None
    ) -> pd.DataFrame:
        """At most ~``max_points`` rows, by striding — preserving trace shape."""
        ...

    def matrix_mean(self, group: pd.DataFrame, measure: str) -> pd.DataFrame:
        """One row holding the elementwise mean of a group's 2-D matrices."""
        ...


class PandasReducer:
    """The reference: the original in-memory reductions, exactly.

    Delegates to the existing functions rather than restating them. This is the
    default for in-memory sources and the oracle every faster reducer is tested
    against.
    """

    def y_extents(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        table: LongTable,
        scope: list[str],
        roles: dict[str, Role],
    ) -> dict[tuple, tuple[float, float]]:
        from dataclasses import replace

        from .ylimits import limits_by_scope

        return limits_by_scope(replace(table, frame=frame), spec, scope, roles)

    def explode_series(
        self,
        frame: pd.DataFrame,
        measure: str,
        index_column: str,
        table: LongTable,
        *,
        max_points: int | None = None,
    ) -> tuple[pd.DataFrame, str, int]:
        # Imported here, not at module top: reduce imports this module.
        from .reduce import _downsample, _explode_1d

        exploded, column = _explode_1d(frame, measure, index_column)
        total = len(exploded)
        if max_points is not None and total > max_points:
            exploded = _downsample(exploded, max_points, column)
        return exploded, column, total

    def collapse_series(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        roles: dict[str, Role],
        index_column: str,
        table: LongTable,
    ) -> tuple[pd.DataFrame, str]:
        from .reduce import _collapse_levels, _explode_1d
        from .roles import collapse_steps

        steps = collapse_steps(spec, roles, table)
        exploded, column = _explode_1d(frame, spec.y_measure, index_column)
        collapsed = _collapse_levels(exploded, steps.pre, spec, table, column)
        collapsed = _collapse_levels(collapsed, steps.final, spec, table, column)
        return collapsed, column

    def summarize_series(
        self,
        group: pd.DataFrame,
        spec: PlotSpec,
        roles: dict[str, Role],
        index_column: str,
        table: LongTable,
    ) -> pd.DataFrame:
        from .reduce import _collapse_levels, _explode_1d, _summarize_exploded
        from .roles import collapse_steps, grouping_layers

        steps = collapse_steps(spec, roles, table)
        layers = grouping_layers(spec, table, roles, spec.kind)
        exploded, column = _explode_1d(group, spec.y_measure, index_column)
        collapsed = _collapse_levels(exploded, steps.pre, spec, table, column)
        return _summarize_exploded(
            collapsed, spec, layers.color, column, series_layers=layers.series
        )

    def downsample(
        self, frame: pd.DataFrame, max_points: int, index_column: str | None
    ) -> pd.DataFrame:
        from .reduce import _downsample

        return _downsample(frame, max_points, index_column)

    def matrix_mean(self, group: pd.DataFrame, measure: str) -> pd.DataFrame:
        from .reduce import _matrix_frame

        return _matrix_frame(group, measure)


class NumpyReducer(PandasReducer):
    """The reference's answers over ndarray cells, without the long frame.

    Takes over the four 1-D operations (``series_stats``); everything else —
    scalars, 2-D matrices, a table that arrives already exploded, the raw
    (non-aggregated) extents, which the reference already does cell-by-cell in
    numpy — falls through to the reference. A partly-migrated reducer is never
    a partly-WRONG one.
    """

    # ---- y extents -------------------------------------------------------

    def y_extents(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        table: LongTable,
        scope: list[str],
        roles: dict[str, Role],
    ) -> dict[tuple, tuple[float, float]]:
        from .roles import collapse_steps, grouping_layers
        from .ylimits import (
            ExtentMode,
            finish_extents,
            merge_extent,
            pair_extent,
            panel_factors,
            scope_key,
        )

        measure = spec.y_measure
        mode = ExtentMode.for_spec(spec, roles)
        if not _nested_series(frame, table, measure) or not mode.reduces:
            return super().y_extents(frame, spec, table, scope, roles)

        # Computed at the granularity it is DRAWN at — per panel (every
        # ITERATE and FACET factor, whether or not the scope names it), per
        # mark (colour and series), per position — through the SAME chain
        # `summarize_series` runs for a panel, and only then folded into the
        # scope groups. The scope decides which panels share a range, never
        # what a panel draws (ylimits module docstring, 2026-09-14).
        present = [s for s in scope if s in frame.columns]
        panels = panel_factors(roles, frame)
        steps = collapse_steps(spec, roles, table)
        layers = grouping_layers(spec, table, roles, spec.kind)
        with Log.timer("y_extents(numpy)", layer=LAYER, extra=f"{measure}, {mode.describe()}") as timer:
            with timer.phase("cells"):
                arrays = _cells(frame[measure])
            with timer.phase("stats"):
                bounds: dict[tuple, tuple[float, float]] = {}
                panel_count = 0
                for key, rows in _group_rows(frame, panels):
                    panel_count += 1
                    group = frame.iloc[rows].reset_index(drop=True)
                    members = [arrays[i] for i in rows]
                    group_key = scope_key(dict(zip(panels, key, strict=True)), present)
                    if mode.summary:
                        kept, series = _chain(group, members, steps.pre, table)
                        for _, centre, low, high, count in _stats_by_mark(
                            kept, series, layers, spec
                        ):
                            ok = count > 0
                            # The centre line is drawn too (MEAN + IQR can put
                            # it outside its own quartiles).
                            extent = pair_extent(
                                np.minimum(low, centre)[ok], np.maximum(high, centre)[ok], mode
                            )
                            if extent:
                                merge_extent(bounds, group_key, *extent)
                    else:
                        # What the chain left is drawn as it is: the sample
                        # rows, one line per sample level (UNIT_KINDS).
                        _, series = _chain(group, members, [*steps.pre, *steps.final], table)
                        for values in series:
                            extent = pair_extent(values, values, mode)
                            if extent:
                                merge_extent(bounds, group_key, *extent)
        return finish_extents(bounds, present, mode, source="numpy", panels=panel_count)

    # ---- explode ---------------------------------------------------------

    def explode_series(
        self,
        frame: pd.DataFrame,
        measure: str,
        index_column: str,
        table: LongTable,
        *,
        max_points: int | None = None,
    ) -> tuple[pd.DataFrame, str, int]:
        from .series_stats import explode

        if index_column in frame.columns:
            # The reference raises for this; raising the same error is right.
            return super().explode_series(
                frame, measure, index_column, table, max_points=max_points
            )
        with Log.timer("explode_series(numpy)", layer=LAYER, extra=measure) as timer:
            with timer.phase("cells"):
                arrays = _cells(frame[measure])
            # The stride the reference's `_downsample` would apply to the full
            # explode, computed from the total before any row's labels exist.
            with timer.phase("explode"):
                row, pos, val, total, stride = explode(arrays, max_points=max_points)
            with timer.phase("labels"):
                # Only the KEPT rows' labels are gathered — for a strided line
                # that is 20 k gathers, not 174 M.
                carried = frame.drop(columns=[measure]).take(row).reset_index(drop=True)
                carried[measure] = val
                carried[index_column] = pos
                exploded = carried[[*frame.columns, index_column]]
        Log.info(
            "exploded 1-D measure %r: %d row(s) -> %d sample(s) [numpy]",
            measure,
            len(frame),
            total,
            layer=LAYER,
        )
        if stride > 1:
            # Same line `_downsample` writes, so a log reads the same whichever
            # reducer strided.
            Log.warn(
                "downsampled %d row(s) to %d for transport (stride=%d)",
                total,
                len(exploded),
                stride,
                layer=LAYER,
            )
        return exploded, index_column, total

    # ---- the collapse chain ----------------------------------------------

    def collapse_series(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        roles: dict[str, Role],
        index_column: str,
        table: LongTable,
    ) -> tuple[pd.DataFrame, str]:
        from .roles import collapse_steps

        measure = spec.y_measure
        if index_column in frame.columns or not _nested_series(frame, table, measure):
            return super().collapse_series(frame, spec, roles, index_column, table)
        steps = collapse_steps(spec, roles, table)
        keys = [*steps.pre, *steps.final]
        if not keys:
            # Nothing to average — the sample is drawn as it is (schema-level
            # parity, 2026-09-19). The reference's answer is then its plain
            # explode, every column carried; `_chain` would keep only the
            # factor columns, so the two reducers would disagree on shape.
            exploded, column, _total = self.explode_series(
                frame, measure, index_column, table
            )
            return exploded, column
        with Log.timer("collapse_series(numpy)", layer=LAYER, extra=measure) as timer:
            with timer.phase("cells"):
                arrays = _cells(frame[measure])
            with timer.phase("means"):
                kept, series = _chain(frame.reset_index(drop=True), arrays, keys, table)
                pieces = []
                assert len(kept) == len(series), "one kept row per collapsed cell"
                # By position, not `itertuples`: with every factor collapsed
                # the kept frame has one row and NO columns, and itertuples
                # (which zips over columns) yields nothing for it.
                for position, values in enumerate(series):
                    ok = ~np.isnan(values)
                    piece = pd.DataFrame(
                        {name: kept[name].iloc[position] for name in kept.columns},
                        index=range(int(ok.sum())),
                    )
                    piece[index_column] = np.flatnonzero(ok)
                    piece[measure] = values[ok]
                    pieces.append(piece)
            collapsed = (
                pd.concat(pieces, ignore_index=True)
                if pieces
                else pd.DataFrame(columns=[*kept.columns, index_column, measure])
            )
        Log.debug(
            "collapse %s: %d row(s) -> %d row(s) [numpy]",
            keys,
            len(frame),
            len(collapsed),
            layer=LAYER,
        )
        return collapsed, index_column

    # ---- band / bar ------------------------------------------------------

    def summarize_series(
        self,
        group: pd.DataFrame,
        spec: PlotSpec,
        roles: dict[str, Role],
        index_column: str,
        table: LongTable,
    ) -> pd.DataFrame:
        from .resolved import COLOR, DASH, SERIES, X, Y, Y_HIGH, Y_LOW
        from .roles import collapse_steps, grouping_layers

        measure = spec.y_measure
        if index_column in group.columns or not _nested_series(group, table, measure):
            return super().summarize_series(group, spec, roles, index_column, table)
        steps = collapse_steps(spec, roles, table)
        layers = grouping_layers(spec, table, roles, spec.kind)
        color = layers.color if layers.color in group.columns else None
        has_series = any(name in group.columns for name in layers.series)
        has_dash = any(
            name in group.columns for name in layers.series if name != color
        )
        with Log.timer("summarize_series(numpy)", layer=LAYER, extra=measure) as timer:
            with timer.phase("cells"):
                arrays = _cells(group[measure])
            with timer.phase("collapse"):
                kept, series = _chain(group.reset_index(drop=True), arrays, steps.pre, table)
            with timer.phase("stats"):
                pieces = []
                for (level, sid, did), centre, low, high, count in _stats_by_mark(
                    kept, series, layers, spec
                ):
                    ok = count > 0
                    piece = pd.DataFrame({X: np.flatnonzero(ok)})
                    if color:
                        piece[COLOR] = level
                    if has_series:
                        piece[SERIES] = sid
                    if has_dash:
                        piece[DASH] = did
                    piece[Y] = centre[ok]
                    piece[Y_LOW] = low[ok]
                    piece[Y_HIGH] = high[ok]
                    pieces.append(piece)
                columns = [
                    X,
                    *([COLOR] if color else []),
                    *([SERIES] if has_series else []),
                    *([DASH] if has_dash else []),
                    Y,
                    Y_LOW,
                    Y_HIGH,
                ]
                out = (
                    pd.concat(pieces, ignore_index=True)
                    if pieces
                    else pd.DataFrame(columns=columns)
                )
                if spec.kind is PlotKind.BAND:
                    out = out.sort_values(X, kind="stable")
        return out.reset_index(drop=True)


def _chain(
    frame: pd.DataFrame,
    arrays: list[np.ndarray],
    keys: list[str],
    table: LongTable,
) -> tuple[pd.DataFrame, list[np.ndarray]]:
    """``reduce._collapse_levels`` over ndarray cells: average ``keys`` away
    one at a time, each grouping on every other factor still present, mean
    per position. Returns the surviving factor columns (one row per cell)
    and the cells. ``frame`` must have a fresh RangeIndex (positions index
    ``arrays``). The pandas reference is what ``test_reducer_parity`` holds
    this to."""
    from .series_stats import position_mean

    factor_columns = [name for name in table.factor_names if name in frame.columns]
    kept = frame[factor_columns]
    for key in keys:
        if key not in kept.columns:
            continue
        keep = [name for name in kept.columns if name != key]
        rows_out: list[tuple] = []
        arrays_out: list[np.ndarray] = []
        for group_key, rows in _group_rows(kept, keep):
            mean, _count = position_mean([arrays[i] for i in rows])
            rows_out.append(group_key)
            arrays_out.append(mean)
        # An explicit index: with every factor collapsed `keep` is empty and
        # `DataFrame([()], columns=[])` would be zero rows, not one.
        kept = pd.DataFrame(rows_out, columns=keep, index=range(len(rows_out)))
        arrays = arrays_out
    return kept, arrays


def _stats_by_mark(kept: pd.DataFrame, series: list[np.ndarray], layers, spec: PlotSpec):
    """Centre ± spread across the sample at each position, per MARK — one
    (colour level, series id) pair — in first-seen order, the order pandas'
    ``groupby(sort=False)`` gives the reference. Yields
    ``((level, sid, dash id), centre, low, high, count)``; the dash id is the
    series id over the UNCOLOURED layers (``reduce._series_key``)."""
    color = layers.color if layers.color in kept.columns else None
    series_layers = [name for name in layers.series if name in kept.columns]
    dash_layers = [name for name in series_layers if name != color]
    marks: dict[tuple, list[np.ndarray]] = {}
    levels = kept[color].to_numpy() if color else None
    # Outermost first, as `reduce._series_key` composes it.
    id_columns = [kept[name].to_numpy() for name in reversed(series_layers)]
    dash_columns = [kept[name].to_numpy() for name in reversed(dash_layers)]
    for position, values in enumerate(series):
        level = hashable(levels[position]) if color else None
        sid = " | ".join(str(column[position]) for column in id_columns) if id_columns else None
        did = " | ".join(str(column[position]) for column in dash_columns) if dash_columns else None
        marks.setdefault((level, sid, did), []).append(values)
    for key, members in marks.items():
        centre, low, high, count = _stats(members, spec)
        yield key, centre, low, high, count


# ---- helpers --------------------------------------------------------------


def _nested_series(frame: pd.DataFrame, table: LongTable, measure: str) -> bool:
    """Whether ``measure`` is a 1-D series still nested one cell per row."""
    from .shape import Shape

    if measure not in frame.columns or frame.empty:
        return False
    if table.shape_of(measure) is not Shape.SERIES_1D:
        return False
    if table.measure(measure).exploded:
        return False
    return not (table.index_column is not None and table.index_column in frame.columns)


def _cells(values: pd.Series) -> list[np.ndarray]:
    from .series_stats import cell_arrays

    return cell_arrays(values.to_numpy())


def _stats(arrays: list[np.ndarray], spec: PlotSpec):
    from .series_stats import position_stats

    return position_stats(arrays, spec.aggregate.statistic, spec.aggregate.error)


def _group_rows(frame: pd.DataFrame, columns: list[str]):
    """``(key tuple, row positions)`` per distinct combination of ``columns``,
    in order of first appearance — pandas' ``groupby(sort=False)`` order."""
    if not columns:
        yield (), np.arange(len(frame))
        return
    keys = [
        tuple(hashable(v) for v in key)
        for key in zip(*[frame[c].to_numpy() for c in columns], strict=True)
    ]
    order: dict[tuple, list[int]] = {}
    for position, key in enumerate(keys):
        order.setdefault(key, []).append(position)
    for key, rows in order.items():
        yield key, np.asarray(rows)


#: The reducer used when a table names none — every in-memory source.
DEFAULT_REDUCER: Reducer = PandasReducer()


def reducer_for(table: LongTable) -> Reducer:
    """The reducer a table carries, or the pandas reference."""
    found: Any = getattr(table, "reducer", None)
    return found if found is not None else DEFAULT_REDUCER
