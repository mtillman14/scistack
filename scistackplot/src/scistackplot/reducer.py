"""The reduction contract: the per-SAMPLE work of a plot, behind one seam.

Every plot over a 1-D measure has a handful of steps that touch every sample
rather than every record — the y extents, one row per sample for a line, the
mean across an AGGREGATE factor at each position, mean ± error across replicates
at each position. Everything else in a resolve is per record or per drawn point
and is cheap. Those steps live behind this protocol so that HOW they are done
can change without the rest of ``reduce`` knowing.

Two implementations:

* :class:`PandasReducer` — the reference. It *calls* the functions the reduce
  path has always used (``_explode_1d``, ``_collapse_aggregates``,
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
        """The AGGREGATE collapse of a 1-D measure: mean per position across
        every AGGREGATE factor, returned EXPLODED (one row per kept factor
        combination per position) — ``_collapse_aggregates`` over the explode."""
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
        ``X`` (position), ``COLOR`` if any, ``Y``, ``Y_LOW``, ``Y_HIGH`` —
        centre ± spread across replicates at every position, computed over ALL
        samples. The AGGREGATE collapse is applied first, as the drawing path
        always has."""
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
        from .reduce import _collapse_aggregates, _explode_1d

        exploded, column = _explode_1d(frame, spec.y_measure, index_column)
        return _collapse_aggregates(exploded, spec, roles, column), column

    def summarize_series(
        self,
        group: pd.DataFrame,
        spec: PlotSpec,
        roles: dict[str, Role],
        index_column: str,
        table: LongTable,
    ) -> pd.DataFrame:
        from .reduce import _collapse_aggregates, _explode_1d, _summarize_exploded

        exploded, column = _explode_1d(group, spec.y_measure, index_column)
        collapsed = _collapse_aggregates(exploded, spec, roles, column)
        color = next((n for n, r in roles.items() if r is Role.COLOR), None)
        return _summarize_exploded(collapsed, spec, color, column)

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
        # colour, per position — through the SAME two stages `summarize_series`
        # runs for a panel, and only then folded into the scope groups. The
        # scope decides which panels share a range, never what a panel draws
        # (ylimits module docstring, 2026-09-14).
        present = [s for s in scope if s in frame.columns]
        panels = panel_factors(roles, frame)
        color = next((n for n, r in roles.items() if r is Role.COLOR and n in frame.columns), None)
        with Log.timer("y_extents(numpy)", layer=LAYER, extra=f"{measure}, {mode.describe()}") as timer:
            with timer.phase("cells"):
                arrays = _cells(frame[measure])
            with timer.phase("stats"):
                bounds: dict[tuple, tuple[float, float]] = {}
                panel_count = 0
                for key, rows in _group_rows(frame, panels):
                    panel_count += 1
                    group = frame.iloc[rows]
                    series, series_color = self._panel_series(
                        group, [arrays[i] for i in rows], roles, color
                    )
                    group_key = scope_key(dict(zip(panels, key, strict=True)), present)
                    if mode.summary:
                        for _, centre, low, high, count in _series_stats(series, series_color, spec):
                            ok = count > 0
                            # The centre line is drawn too (MEAN + IQR can put
                            # it outside its own quartiles).
                            extent = pair_extent(
                                np.minimum(low, centre)[ok], np.maximum(high, centre)[ok], mode
                            )
                            if extent:
                                merge_extent(bounds, group_key, *extent)
                    else:
                        # AGGREGATE on a non-summary kind: the collapsed means
                        # are drawn as they are.
                        for mean in series:
                            extent = pair_extent(mean, mean, mode)
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

    # ---- AGGREGATE collapse ---------------------------------------------

    def collapse_series(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        roles: dict[str, Role],
        index_column: str,
        table: LongTable,
    ) -> tuple[pd.DataFrame, str]:
        from .series_stats import position_mean

        measure = spec.y_measure
        if index_column in frame.columns or not _nested_series(frame, table, measure):
            return super().collapse_series(frame, spec, roles, index_column, table)
        keep = [
            name
            for name, role in roles.items()
            if role is not Role.AGGREGATE and name in frame.columns
        ]
        with Log.timer("collapse_series(numpy)", layer=LAYER, extra=measure) as timer:
            with timer.phase("cells"):
                arrays = _cells(frame[measure])
            with timer.phase("means"):
                pieces = []
                for key, rows in _group_rows(frame, keep):
                    mean, count = position_mean([arrays[i] for i in rows])
                    ok = count > 0
                    piece = pd.DataFrame({name: value for name, value in zip(keep, key)}, index=range(int(ok.sum())))
                    piece[index_column] = np.flatnonzero(ok)
                    piece[measure] = mean[ok]
                    pieces.append(piece)
            collapsed = (
                pd.concat(pieces, ignore_index=True)
                if pieces
                else pd.DataFrame(columns=[*keep, index_column, measure])
            )
        Log.debug(
            "aggregate over %s: %d row(s) -> %d row(s) [numpy]",
            [n for n, r in roles.items() if r is Role.AGGREGATE],
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
        from .resolved import COLOR, X, Y, Y_HIGH, Y_LOW

        measure = spec.y_measure
        if index_column in group.columns or not _nested_series(group, table, measure):
            return super().summarize_series(group, spec, roles, index_column, table)
        color = next((n for n, r in roles.items() if r is Role.COLOR and n in group.columns), None)
        with Log.timer("summarize_series(numpy)", layer=LAYER, extra=measure) as timer:
            with timer.phase("cells"):
                arrays = _cells(group[measure])
            with timer.phase("collapse"):
                series, series_color = self._panel_series(group, arrays, roles, color)
            with timer.phase("stats"):
                pieces = []
                for level, centre, low, high, count in _series_stats(series, series_color, spec):
                    ok = count > 0
                    piece = pd.DataFrame({X: np.flatnonzero(ok)})
                    if color:
                        piece[COLOR] = level
                    piece[Y] = centre[ok]
                    piece[Y_LOW] = low[ok]
                    piece[Y_HIGH] = high[ok]
                    pieces.append(piece)
                columns = [X, *([COLOR] if color else []), Y, Y_LOW, Y_HIGH]
                out = (
                    pd.concat(pieces, ignore_index=True)
                    if pieces
                    else pd.DataFrame(columns=columns)
                )
                if spec.kind is PlotKind.BAND:
                    out = out.sort_values(X, kind="stable")
        return out.reset_index(drop=True)

    def _panel_series(
        self,
        group: pd.DataFrame,
        arrays: list[np.ndarray],
        roles: dict[str, Role],
        color: str | None,
    ) -> tuple[list[np.ndarray], list[Any]]:
        """Stage 1 of a BAND/BAR panel — the AGGREGATE collapse.

        One mean series per kept factor combination, each tagged with its
        colour level. Without AGGREGATE roles every row is its own series.
        Shared by ``summarize_series`` (the drawing) and ``y_extents`` (the
        limits) so the two cannot collapse differently.
        """
        from .series_stats import position_mean

        aggregated = [n for n, r in roles.items() if r is Role.AGGREGATE and n in group.columns]
        if not aggregated:
            return arrays, (list(group[color].to_numpy()) if color else [None] * len(arrays))
        keep = [
            name
            for name, role in roles.items()
            if role is not Role.AGGREGATE and name in group.columns
        ]
        series: list[np.ndarray] = []
        series_color: list[Any] = []
        for key, rows in _group_rows(group, keep):
            mean, count = position_mean([arrays[i] for i in rows])
            series.append(np.where(count > 0, mean, np.nan))
            series_color.append(key[keep.index(color)] if color else None)
        return series, series_color


def _series_stats(series: list[np.ndarray], series_color: list[Any], spec: PlotSpec):
    """Stage 2 of a BAND/BAR panel — centre ± spread across the replicates of
    each colour level, in first-seen order. Yields
    ``(level, centre, low, high, count)`` per level."""
    for level in dict.fromkeys(series_color):
        members = [s for s, c in zip(series, series_color, strict=True) if c == level]
        centre, low, high, count = _stats(members, spec)
        yield level, centre, low, high, count


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
