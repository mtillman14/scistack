"""``DuckDBReducer`` — the plot's per-sample reductions, done in DuckDB.

Stages 2-3 of ``.claude/plan-duckdb-native-reduction.md``: ``y_extents`` and
``explode_series`` in SQL. ``downsample`` and ``matrix_mean`` still fall
through to the pandas reference and are later stages.

**The design, in one sentence:** the frame decides WHICH rows and WHICH groups;
DuckDB reduces the VALUES. The two are joined on ``record_id`` (plus the field
column, for a dict variable). That split is not a convenience — it is forced.
The grouping columns a plot needs (a scope factor, the X and COLOR factors) are
frame columns that may have no DuckDB column behind them at all: a synthetic
``Variant`` factor, a ``ColName`` from the melt, a level group. So the SQL can
never re-derive the groups from the database. It takes the group labels from the
frame as a registered view, and does the one thing the frame cannot do cheaply:
touch every sample.

**Parity is the contract**, per §4 of the plan: every statistic here has its
pandas twin named, and ``tests/test_reducer_parity.py`` asserts equality on a
fixture built to hit the places two implementations disagree (NaN, ragged
lengths, n=1). Change a function here and the test says whether pandas agrees.

Why the value columns are addressed by name from the frame rather than by a
stored ``(table, column)`` on the table object: a stacked table holds SEVERAL
variables in one value column, told apart by ``VARIABLE_COLUMN``; a melted one
holds several DuckDB columns told apart by ``ColName``. The frame row is the
only thing that knows both, so the row is where the address comes from.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scistacklog import Log
from scistackplot import LongTable, PlotSpec
from scistackplot.reducer import PandasReducer
from scistackplot.shape import Shape
from scistackplot.spec import ErrorBand, Role, Statistic
from scistackplot.variants import VARIABLE_COLUMN
from scistackplot.ylimits import GLOBAL_KEY, PAD_FRACTION

from .load import data_columns_for, table_name_for

LAYER = "scistackplotdb"

#: The registered view name the group-label frame is visible under.
_GROUPS_VIEW = "_plot_groups"

#: The melted field factor — one DuckDB column per level. Imported lazily from
#: ``source`` would be a cycle; the name is stable and pinned by test.
FIELD_FACTOR = "ColName"


class DuckDBReducer(PandasReducer):
    """Push ``y_extents`` and ``explode_series`` down to DuckDB.

    Subclassing the reference (rather than the bare protocol) is deliberate:
    an operation this class has not yet taken over falls through to the exact
    pandas code it would otherwise have been compared against, so a partly
    migrated reducer is never a partly WRONG one. ``downsample`` and
    ``matrix_mean`` are still inherited.
    """

    def __init__(self, db) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # explode_series
    # ------------------------------------------------------------------

    def explode_series(
        self,
        frame: pd.DataFrame,
        measure: str,
        index_column: str,
        table: LongTable,
    ) -> tuple[pd.DataFrame, str]:
        """One row per sample — the same frame ``reduce._explode_1d`` builds,
        produced by ``UNNEST`` and returned columnar.

        **Identical output, different cost.** Stage 3 of the plan first proposed
        striding INSIDE this query (``WHERE pos % stride = 0``). That would have
        changed the figure: ``_downsample`` strides the exploded frame by ROW
        (``iloc[::stride]``), and the exploded frame is record-major, so for a
        line plot the stride walks ACROSS records and keeps different positions
        of each. Only a band/bar — which collapses per position BEFORE the
        stride — is unaffected. So this method reproduces the explode exactly
        and leaves striding where it is; the win is that 174 M samples come
        back as one numpy buffer through ``.df()`` instead of 174 M boxed Python
        floats through ``fetchall()``/``pandas.explode`` (153 s on 2026-09-13).

        Contract, matched to ``_explode_1d`` and pinned by parity test:

        * every non-measure column of ``frame`` is carried through unchanged;
        * rows are ordered record-major (the frame's row order), then by
          position within the record — ``ORDER BY __row, pos``;
        * the index is 0-based (``pos - 1``; ``WITH ORDINALITY`` is 1-based);
        * NaN samples are dropped (``dropna(subset=[measure])``), and their
          positions are NOT renumbered — the gap is preserved;
        * the measure and index columns are numeric.

        Falls back to the reference when a row cannot be addressed in DuckDB or
        the index column already exists (the reference raises for that, and
        raising the same error is the right thing).
        """
        if index_column in frame.columns:
            return super().explode_series(frame, measure, index_column, table)
        addressed = self._address_rows(frame, table, measure)
        if addressed is None:
            Log.info(
                "explode_series(%s): rows not addressable in DuckDB — pandas fallback",
                measure,
                layer=LAYER,
            )
            return super().explode_series(frame, measure, index_column, table)

        with Log.timer("explode_series(duckdb)", layer=LAYER, extra=measure) as timer:
            with timer.phase("unnest"):
                pieces = []
                pairs = (
                    addressed[["__table", "__column"]].drop_duplicates().itertuples(index=False)
                )
                for tbl, col in pairs:
                    mask = (addressed["__table"] == tbl) & (addressed["__column"] == col)
                    view = addressed.loc[mask, ["__row", "record_id"]].reset_index(drop=True)
                    sql = (
                        f"SELECT g.__row, v.pos - 1 AS __pos, v.val AS __val "
                        f'FROM "{tbl}" t '
                        f"JOIN {_GROUPS_VIEW} g ON g.record_id = t.record_id, "
                        f'UNNEST(t."{col}") WITH ORDINALITY AS v(val, pos) '
                        # dropna(subset=[measure]): a NaN sample leaves a gap.
                        f"WHERE v.val IS NOT NULL AND NOT isnan(v.val)"
                    )
                    pieces.append(
                        self._db._duck._fetchdf_with_frame(sql, view, view=_GROUPS_VIEW)
                    )
            with timer.phase("assemble"):
                samples = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame(
                    {"__row": [], "__pos": [], "__val": []}
                )
                # Record-major, then position — the order pandas.explode gives.
                samples = samples.sort_values(["__row", "__pos"], kind="stable")
                # Carry every other column through by re-indexing the original
                # frame at each sample's source row. `take` is a positional
                # gather — one vectorised copy, no per-row Python.
                carried = frame.drop(columns=[measure]).take(
                    samples["__row"].to_numpy(dtype=np.int64)
                )
                carried = carried.reset_index(drop=True)
                carried[measure] = samples["__val"].to_numpy(dtype=float)
                carried[index_column] = samples["__pos"].to_numpy(dtype=np.int64)
                # Match `_explode_1d`'s column order: the frame's columns, with
                # the measure where it was, then the index appended.
                order = [c for c in frame.columns] + [index_column]
                exploded = carried[order]

        Log.info(
            "exploded 1-D measure %r: %d row(s) -> %d sample(s) (x%d) [duckdb]",
            measure,
            len(frame),
            len(exploded),
            round(len(exploded) / len(frame)) if len(frame) else 0,
            layer=LAYER,
        )
        return exploded, index_column

    # ------------------------------------------------------------------
    # y_extents
    # ------------------------------------------------------------------

    def y_extents(
        self,
        frame: pd.DataFrame,
        spec: PlotSpec,
        table: LongTable,
        scope: list[str],
    ) -> dict[tuple, tuple[float, float]]:
        measure = spec.y_measure
        if measure not in frame.columns or frame.empty:
            return {}

        addressed = self._address_rows(frame, table, measure)
        if addressed is None:
            # A row this reducer cannot locate in DuckDB (an exploded table, a
            # CSV-joined x measure, a value that was never stored) is answered
            # the way it always was. Correct beats fast.
            Log.info(
                "y_extents(%s): rows not addressable in DuckDB — pandas fallback",
                measure,
                layer=LAYER,
            )
            return super().y_extents(frame, spec, table, scope)

        present = [name for name in scope if name in frame.columns]
        with Log.timer("y_extents(duckdb)", layer=LAYER, extra=measure) as timer:
            if _needs_reduction(spec):
                with timer.phase("aggregated"):
                    extents = self._aggregated_extents(
                        frame, addressed, spec, table, present
                    )
            else:
                with timer.phase("raw"):
                    extents = self._raw_extents(
                        frame, addressed, table, measure, present
                    )
        limits = {key: _padded(low, high) for key, (low, high) in extents.items()}
        Log.debug(
            "y limits over %s: %d group(s) [duckdb]",
            present or "the whole dataset",
            len(limits),
            layer=LAYER,
        )
        return limits

    # ------------------------------------------------------------------
    # addressing: frame row -> (table, column, record_id)
    # ------------------------------------------------------------------

    def _address_rows(
        self, frame: pd.DataFrame, table: LongTable, measure: str
    ) -> "pd.DataFrame | None":
        """One row per frame row: ``__table``, ``__column``, ``record_id``.

        ``None`` when the rows are STRUCTURALLY not in DuckDB — no
        ``record_id`` column, an already-exploded table, a variable with no
        data table — and the caller falls back to pandas, which is then the
        honest reducer for that shape.

        Anything else raises. This used to wrap ``table_name_for`` in
        ``except Exception: return None``, and on 2026-09-13 that turned a
        closed connection (``ConnectionException: Connection already closed!``
        — the GUI had released its per-request hold before ``resolve`` ran)
        into "rows not addressable … pandas fallback": a correct plot, 375 s
        of ``y_limits`` and 110 s of ``explode`` later, with the reducer
        believed to be working. A fault in reaching the database is not a
        property of the rows; it must surface as the fault it is
        (scidb.log 18:02:26, .claude/plan-plot-minimal-load-examples.md §0).
        """
        if "record_id" not in frame.columns:
            return None
        already_exploded = table.measure(measure).exploded or (
            table.index_column is not None and table.index_column in frame.columns
        )
        if already_exploded:
            # Already one row per sample: the values ARE the frame, and DuckDB
            # holds them nested. Pandas is the honest reducer for this shape.
            return None

        # Which variable does each row come from? A stacked table says per row;
        # otherwise it is the table's own name.
        if VARIABLE_COLUMN in frame.columns:
            variables = frame[VARIABLE_COLUMN].astype(str)
        else:
            variables = pd.Series([table.name] * len(frame), index=frame.index)

        tables: dict[str, str] = {}
        single_column: dict[str, str] = {}
        for variable in variables.unique():
            # Neither lookup raises for an unknown variable: `table_name_for`
            # falls back to "<name>_data" and `data_columns_for` returns []
            # for a table that does not exist. So an exception here is the
            # database itself being unreachable, and it propagates.
            tables[variable] = table_name_for(self._db, variable)
            columns = data_columns_for(self._db, variable)
            if not columns:
                Log.warn(
                    "reducer: variable %r has no data table in DuckDB — its rows "
                    "are not addressable, pandas will reduce them",
                    variable,
                    layer=LAYER,
                )
                return None
            single_column[variable] = columns[0]

        # Which DuckDB column? Melted -> the field factor names it; else the
        # variable's (first) data column, exactly as `_named_frame` chose it.
        if FIELD_FACTOR in frame.columns:
            columns_per_row = frame[FIELD_FACTOR].astype(str)
        else:
            columns_per_row = variables.map(single_column)

        return pd.DataFrame(
            {
                "__row": np.arange(len(frame)),
                "__table": variables.map(tables).to_numpy(),
                "__column": columns_per_row.to_numpy(),
                "record_id": frame["record_id"].astype(str).to_numpy(),
            }
        )

    # ------------------------------------------------------------------
    # raw: MIN/MAX
    # ------------------------------------------------------------------

    def _raw_extents(
        self,
        frame: pd.DataFrame,
        addressed: pd.DataFrame,
        table: LongTable,
        measure: str,
        scope: list[str],
    ) -> dict[tuple, tuple[float, float]]:
        """Min/max per scope group — ``list_min``/``list_max`` for arrays.

        pandas twin: ``ylimits._raw_extents`` (``np.nanmin``/``np.nanmax`` per
        cell, then min/max across cells).

        **NaN is stored as a real NaN double, not NULL** — scidb writes arrays
        through ``pa.array(..., pa.list_(pa.float64()))`` (``database.py:1605``),
        which preserves NaN. DuckDB orders NaN ABOVE every number, so a bare
        ``list_max`` over a list holding one NaN returns NaN where ``nanmax``
        would skip it. Every reduction here therefore drops NaN first with
        ``list_filter``; an all-NaN list then becomes an empty one, whose
        ``list_min`` is NULL, which the outer ``MIN`` skips — an all-NaN cell
        contributing nothing, exactly as pandas has it.
        """
        shape = table.shape_of(measure)
        if shape is Shape.SCALAR:
            value = "CASE WHEN isnan(v.val) THEN NULL ELSE v.val END"
            lo, hi = f"MIN({value})", f"MAX({value})"
        elif shape is Shape.MATRIX_2D:
            row_min = "list_min(list_filter(r, x -> NOT isnan(x)))"
            row_max = "list_max(list_filter(r, x -> NOT isnan(x)))"
            lo = f"MIN(list_min(list_filter(list_transform(v.val, r -> {row_min}), x -> x IS NOT NULL)))"
            hi = f"MAX(list_max(list_filter(list_transform(v.val, r -> {row_max}), x -> x IS NOT NULL)))"
        else:
            clean = "list_filter(v.val, x -> NOT isnan(x))"
            lo, hi = f"MIN(list_min({clean}))", f"MAX(list_max({clean}))"

        # The scope labels ride along in the view so the query can GROUP BY them.
        labels = frame[scope].reset_index(drop=True) if scope else None
        rows = self._reduce_by_group(addressed, scope, lo, hi, group_labels=labels)
        return _extents_from_rows(rows, scope)

    # ------------------------------------------------------------------
    # aggregated: centre ± spread per drawn point, extremes per scope
    # ------------------------------------------------------------------

    def _aggregated_extents(
        self,
        frame: pd.DataFrame,
        addressed: pd.DataFrame,
        spec: PlotSpec,
        table: LongTable,
        scope: list[str],
    ) -> dict[tuple, tuple[float, float]]:
        """Extents of ``centre ± spread`` — the band a BAND/BAR draws.

        pandas twin: ``ylimits._aggregated_extents``. The grouping is identical
        by construction (scope + X + COLOR + the sample position); the
        statistics map per the plan's §4 table:

            mean()          -> avg
            median()        -> quantile_cont(0.5)
            std(ddof=1)     -> stddev_samp, COALESCE(...,0) for n=1
            quantile(q)     -> quantile_cont(q)
            SEM / CI95      -> sd / sqrt(GREATEST(count,1)), x1.96 for CI95
        """
        measure = spec.y_measure
        shape = table.shape_of(measure)
        group_names = [
            name
            for name in dict.fromkeys(
                [*scope, *[n for n, r in spec.roles.items() if r in (Role.X, Role.COLOR)]]
            )
            if name in frame.columns
        ]
        # 1-D: the sample POSITION is part of what separates a drawn point.
        by_position = shape is Shape.SERIES_1D

        centre_sql, low_sql, high_sql = _spread_sql(spec)
        group_labels = frame[group_names].reset_index(drop=True) if group_names else None

        rows = self._reduce_by_group(
            addressed,
            scope,
            low_sql,
            high_sql,
            inner_groups=group_names,
            group_labels=group_labels,
            by_position=by_position,
            centre_sql=centre_sql,
        )
        return _extents_from_rows(rows, scope)

    # ------------------------------------------------------------------
    # the query
    # ------------------------------------------------------------------

    def _reduce_by_group(
        self,
        addressed: pd.DataFrame,
        scope: list[str],
        low_sql: str,
        high_sql: str,
        *,
        inner_groups: "list[str] | None" = None,
        group_labels: "pd.DataFrame | None" = None,
        by_position: bool = False,
        centre_sql: "str | None" = None,
    ) -> list[tuple]:
        """Run the reduction, one ``(table, column)`` pair at a time.

        A frame may span several DuckDB tables/columns (a stacked or melted
        variable). Each pair is one query; the results are merged in Python,
        which is cheap — there are as many rows as groups, not as samples.
        """
        results: list[tuple] = []
        pairs = addressed[["__table", "__column"]].drop_duplicates().itertuples(index=False)
        for tbl, col in pairs:
            mask = (addressed["__table"] == tbl) & (addressed["__column"] == col)
            view = addressed.loc[mask, ["__row", "record_id"]].reset_index(drop=True)
            if group_labels is not None:
                view = pd.concat(
                    [view, group_labels.loc[mask.to_numpy()].reset_index(drop=True)],
                    axis=1,
                )
            results.extend(
                self._run(
                    tbl, col, view, scope, low_sql, high_sql,
                    inner_groups=inner_groups or [],
                    by_position=by_position,
                    centre_sql=centre_sql,
                )
            )
        return results

    def _run(
        self,
        tbl: str,
        col: str,
        view: pd.DataFrame,
        scope: list[str],
        low_sql: str,
        high_sql: str,
        *,
        inner_groups: list[str],
        by_position: bool,
        centre_sql: "str | None",
    ) -> list[tuple]:
        scope_select = "".join(f', g."{s}"' for s in scope)
        scope_group = ", ".join(f'g."{s}"' for s in scope)

        if centre_sql is None:
            # RAW: one reduction over the values, grouped by scope only.
            sql = (
                f"SELECT {low_sql}, {high_sql}{scope_select} "
                f'FROM "{tbl}" t '
                f"JOIN {_GROUPS_VIEW} g ON g.record_id = t.record_id, "
                f'LATERAL (SELECT t."{col}" AS val) v '
                + (f"GROUP BY {scope_group}" if scope else "")
            )
            return self._db._duck._fetchall_with_frame(sql, view, view=_GROUPS_VIEW)

        # AGGREGATED: per drawn point (inner groups + position), then extremes
        # per scope. Two levels of GROUP BY in one statement.
        inner = list(dict.fromkeys([*scope, *inner_groups]))
        inner_cols = "".join(f', g."{n}"' for n in inner)
        inner_group = ", ".join(f'g."{n}"' for n in inner)
        if by_position:
            source = (
                f'FROM "{tbl}" t '
                f"JOIN {_GROUPS_VIEW} g ON g.record_id = t.record_id, "
                f'UNNEST(t."{col}") WITH ORDINALITY AS v(val, pos) '
            )
            inner_group = f"{inner_group}, v.pos" if inner_group else "v.pos"
        else:
            source = (
                f'FROM "{tbl}" t '
                f"JOIN {_GROUPS_VIEW} g ON g.record_id = t.record_id, "
                f'LATERAL (SELECT CASE WHEN isnan(t."{col}") THEN NULL '
                f'ELSE t."{col}" END AS val) v '
            )
        # NaN as well as NULL: pandas drops both (`to_numeric` + `dropna`), and
        # `avg` over a NaN is NaN. See `_raw_extents` on why NaN is really there.
        per_point = (
            f"SELECT {centre_sql} AS centre, {low_sql} AS low, {high_sql} AS high"
            f"{inner_cols} {source} WHERE v.val IS NOT NULL AND NOT isnan(v.val) "
            + (f"GROUP BY {inner_group}" if inner_group else "")
        )
        # Extremes of the per-point bounds, per scope — exactly the pandas
        # twin's `bounds["low"].min()` / `bounds["high"].max()` over the grouped
        # frame. (pandas consults the CENTRE only in its ungrouped branch,
        # `_summary_bounds`; here every row of `per_point` is a real group, so the
        # centre is never part of the extreme. `centre` is still selected: it is
        # what the grouped branch is computing, and a future reader debugging a
        # band can see it.)
        scope_cols = ", ".join(f'"{s}"' for s in scope)
        if inner_group:
            lo, hi = "MIN(low)", "MAX(high)"
        else:
            # The one ungrouped case (a scalar measure with no scope, X, or
            # COLOR): pandas goes through `_summary_bounds` and takes
            # min(low, centre) / max(high, centre). Same here, so the two paths
            # are the same shape rather than merely the same number.
            lo, hi = "MIN(LEAST(low, centre))", "MAX(GREATEST(high, centre))"
        outer = (
            f"SELECT {lo}, {hi}"
            + (f", {scope_cols}" if scope else "")
            + f" FROM ({per_point}) p"
            + (f" GROUP BY {scope_cols}" if scope else "")
        )
        return self._db._duck._fetchall_with_frame(outer, view, view=_GROUPS_VIEW)


# ----------------------------------------------------------------------
# statistics -> SQL (the §4 table, one place)
# ----------------------------------------------------------------------


def _spread_sql(spec: PlotSpec) -> tuple[str, str, str]:
    """``(centre, low, high)`` SQL over ``v.val`` — pandas twins in the comments."""
    if spec.aggregate.statistic is Statistic.MEDIAN:
        centre = "quantile_cont(v.val, 0.5)"  # pandas .median(): linear interp
    else:
        centre = "avg(v.val)"  # pandas .mean()
    error = spec.aggregate.error
    if error is ErrorBand.IQR:
        # pandas .quantile(0.25/0.75): linear interpolation -> quantile_cont.
        return centre, "quantile_cont(v.val, 0.25)", "quantile_cont(v.val, 0.75)"
    # pandas .std(ddof=1).fillna(0.0): sample std, 0 when n=1 (DuckDB gives NULL).
    sd = "COALESCE(stddev_samp(v.val), 0.0)"
    # pandas count.where(count > 0, 1)
    n = "GREATEST(count(v.val), 1)"
    if error is ErrorBand.SD:
        spread = sd
    elif error is ErrorBand.SEM:
        spread = f"{sd} / sqrt({n})"
    else:  # CI95 — same 1.96 constant as ylimits._spread, not a t-distribution
        spread = f"1.96 * {sd} / sqrt({n})"
    return centre, f"{centre} - {spread}", f"{centre} + {spread}"


def _needs_reduction(spec: PlotSpec) -> bool:
    """Mirror of ``ylimits._needs_reduction`` — kept identical, tested identical."""
    from scistackplot.spec import PlotKind

    return spec.kind in (PlotKind.BAND, PlotKind.BAR) and (
        spec.aggregate.error is not ErrorBand.NONE
    )


def _padded(low: float, high: float) -> tuple[float, float]:
    """Mirror of ``ylimits._padded``."""
    if low == high:
        pad = abs(low) * PAD_FRACTION or 1.0
        return (low - pad, high + pad)
    pad = (high - low) * PAD_FRACTION
    return (low - pad, high + pad)


def _extents_from_rows(
    rows: list[tuple], scope: list[str]
) -> dict[tuple, tuple[float, float]]:
    """``(low, high, *scope_values)`` rows -> the extents dict + global key.

    Several ``(table, column)`` queries may each contribute a row for the same
    scope key (a melted variable's fields all live in one group); they are
    merged by min/max here. Pandas twin: the key merge in ``_raw_extents``.
    """
    extents: dict[tuple, tuple[float, float]] = {}
    for row in rows:
        low, high = row[0], row[1]
        if low is None or high is None:
            continue
        low, high = float(low), float(high)
        if np.isnan(low) or np.isnan(high):
            continue
        # Scope values come back as the frame stored them (strings, or None
        # for a missing level — DuckDB keeps a NULL group as pandas keeps a
        # dropna=False one), so the key matches a panel's without translation.
        key = tuple(row[2:]) if scope else GLOBAL_KEY
        seen = extents.get(key)
        extents[key] = (
            (low, high) if seen is None else (min(seen[0], low), max(seen[1], high))
        )
    if extents and scope:
        # The global entry always exists as a fallback for a panel whose own
        # group has no data — same as the pandas path.
        extents[GLOBAL_KEY] = (
            min(low for low, _ in extents.values()),
            max(high for _, high in extents.values()),
        )
    return extents
