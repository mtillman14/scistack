"""The reduction contract: what a source can do on a plot's behalf.

Four operations cover every plot kind over every stored shape
(``.claude/plan-duckdb-native-reduction.md`` §3). They are the parts of a
resolve that touch every SAMPLE rather than every record, which is where a
plot over 174 million samples spent 350 s (§2 there) — and they are exactly the
operations a columnar engine performs natively, in one pass, without building a
Python object per value.

So the contract lives here, as a protocol with a pandas reference, and a source
that can push it down (scidb -> DuckDB) overrides it. Sources that cannot (a
CSV, an in-memory frame) keep the reference, which is what keeps ``scistackplot``
standalone: nothing above this line knows which one it got.

**Stage 1 of the plan is this file, unchanged in behaviour.** ``PandasReducer``'s
methods *call* the functions the reduce path already used — nothing is
reimplemented — so its answers are today's answers by construction. Every later
stage is tested against it. Keep that property: a PandasReducer that starts
computing its own version of a statistic has quietly become a second definition,
and two definitions of an error band put the drawing and its axis at odds
(``ylimits._spread``).
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import pandas as pd

from .spec import PlotSpec
from .table import LongTable


@runtime_checkable
class Reducer(Protocol):
    """Per-sample reductions a plot needs, as a source can best perform them.

    Every method takes the frame it should reduce (already variant-selected
    and filtered by the caller — the reducer never re-derives a filter) and the
    spec that says how. A pushed-down implementation is free to ignore the
    frame's *values* and re-query, but must honour its *rows*: the set of
    records the caller has selected is the caller's decision.
    """

    def y_extents(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        table: LongTable,
        scope: list[str],
    ) -> dict[tuple, tuple[float, float]]:
        """``{scope values: (low, high)}`` per group, plus the global key.

        Raw (min/max of what is drawn) or aggregated (centre ± spread, the band
        a BAND/BAR draws) according to the spec — the same rule
        ``ylimits.limits_by_scope`` applies.
        """
        ...

    def explode_series(
        self,
        frame: pd.DataFrame,
        measure: str,
        index_column: str,
        table: LongTable,
    ) -> tuple[pd.DataFrame, str]:
        """One row per sample of a 1-D measure, with a positional index column.

        ``table`` is the table the frame came from — a pushed-down reducer needs
        it to locate the rows in storage, the same way ``y_extents`` does.
        """
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
    """The reference: today's in-memory reductions, exactly.

    Delegates to the existing functions rather than restating them. This is the
    fallback every source inherits and the oracle every pushed-down reducer is
    tested against.
    """

    def y_extents(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        table: LongTable,
        scope: list[str],
    ) -> dict[tuple, tuple[float, float]]:
        from dataclasses import replace

        from .ylimits import limits_by_scope

        return limits_by_scope(replace(table, frame=frame), spec, scope)

    def explode_series(
        self,
        frame: pd.DataFrame,
        measure: str,
        index_column: str,
        table: LongTable,
    ) -> tuple[pd.DataFrame, str]:
        # Imported here, not at module top: reduce imports this module.
        from .reduce import _explode_1d

        return _explode_1d(frame, measure, index_column)

    def downsample(
        self, frame: pd.DataFrame, max_points: int, index_column: str | None
    ) -> pd.DataFrame:
        from .reduce import _downsample

        return _downsample(frame, max_points, index_column)

    def matrix_mean(self, group: pd.DataFrame, measure: str) -> pd.DataFrame:
        from .reduce import _matrix_frame

        return _matrix_frame(group, measure)


#: The reducer used when a table names none — every in-memory source, and any
#: scidb table built before its source learned to push down.
DEFAULT_REDUCER: Reducer = PandasReducer()


def reducer_for(table: LongTable) -> Reducer:
    """The reducer a table carries, or the pandas reference."""
    found: Any = getattr(table, "reducer", None)
    return found if found is not None else DEFAULT_REDUCER
