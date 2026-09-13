"""The reducer contract: a pushed-down reducer must equal the pandas reference.

This is Stage 1 of ``.claude/plan-duckdb-native-reduction.md``: the FIXTURE and
the ORACLE, before any SQL exists. ``PandasReducer`` delegates to the functions
the reduce path used before the refactor, so its answers are the old answers by
construction; these tests pin that the refactor changed nothing, and give every
later stage the thing it is measured against.

The fixture is built to hit the edges the plan's §4 parity table names — the
places two implementations of a statistic disagree:

* **ragged 1-D lengths** (a stride is per record, not per table);
* **NaN inside an array** (``list_min`` vs ``nanmin`` must agree on skipping);
* **an n=1 group** (``std(ddof=1)`` is NaN there; pandas fills 0.0);
* **an all-NaN cell** (contributes nothing, must not poison a group);
* **two 2-D shapes** (the heatmap mismatch warning, "use the first").

Shapes covered: DOUBLE (scalar), DOUBLE[] (1-D), DOUBLE[][] (2-D) — the
user's stated constraint. Kinds covered: every ``PlotKind``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from scidb import BaseVariable, configure_database
from scidb.database import _local
from scistackplot import PlotKind, PlotSpec, Role, resolve
from scistackplot.reducer import DEFAULT_REDUCER, PandasReducer, Reducer, reducer_for
from scistackplot.spec import Aggregation, ErrorBand, Statistic

from scistackplotdb import ScidbSource

SCHEMA = ["subject", "trial"]


class Scalar(BaseVariable):
    """DOUBLE."""

    schema_version = 1


class Series(BaseVariable):
    """DOUBLE[] — ragged, with NaNs, one subject with a single trial."""

    schema_version = 1


class Matrix(BaseVariable):
    """DOUBLE[][] — two records of one shape, one of another."""

    schema_version = 1


@pytest.fixture
def parity_db(tmp_path):
    db = configure_database(tmp_path / "parity.duckdb", SCHEMA)
    rng = np.random.default_rng(7)

    # Scalar: three subjects, two of them with three trials, one (s3) with ONE
    # trial — the n=1 group where sample std is undefined.
    for subject, trials in (("s1", 3), ("s2", 3), ("s3", 1)):
        for trial in range(1, trials + 1):
            Scalar.save(float(rng.normal(10.0, 2.0)), subject=subject, trial=str(trial))

    # 1-D: ragged lengths, a NaN mid-array in s1/t1, an all-NaN cell in s2/t3.
    lengths = {("s1", "1"): 12, ("s1", "2"): 9, ("s1", "3"): 15,
               ("s2", "1"): 10, ("s2", "2"): 10, ("s2", "3"): 6,
               ("s3", "1"): 8}
    for (subject, trial), n in lengths.items():
        arr = rng.normal(0.0, 1.0, size=n)
        if (subject, trial) == ("s1", "1"):
            arr[4] = np.nan
        if (subject, trial) == ("s2", "3"):
            arr[:] = np.nan
        Series.save(arr, subject=subject, trial=trial)

    # 2-D: s1 has two 3x4 matrices, s2 one 2x2 — the shape-mismatch case.
    Matrix.save(rng.normal(size=(3, 4)), subject="s1", trial="1")
    Matrix.save(rng.normal(size=(3, 4)), subject="s1", trial="2")
    Matrix.save(rng.normal(size=(2, 2)), subject="s2", trial="1")

    yield db
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


@pytest.fixture
def source(parity_db) -> ScidbSource:
    return ScidbSource(parity_db)


@pytest.fixture(autouse=True)
def _fresh_plan_cache():
    """The plan cache is keyed on ``id(table)`` and is module-global.

    A spy that expects to see ``y_extents`` called would be defeated by a
    cached plan for a recycled table id from an earlier test. Clear on both
    sides so each test's spy observes a real plan build.
    """
    from scistackplot.reduce import clear_plan_cache

    clear_plan_cache()
    yield
    clear_plan_cache()


# ---------------------------------------------------------------------------
# The contract itself
# ---------------------------------------------------------------------------


class TestContract:
    def test_pandas_reducer_satisfies_the_protocol(self):
        assert isinstance(PandasReducer(), Reducer)

    def test_a_table_with_no_reducer_gets_the_pandas_reference(self, parity_db):
        """`reducer=None` on a table means the pandas reference.

        Written in Stage 1 against the default `source`, which then carried no
        reducer. Stage 2 made `ScidbSource` attach a `DuckDBReducer` by default
        (its own tests pin that), so the "no reducer" case now has to be asked
        for explicitly — `pushdown=False` is the in-tree way a table ends up
        with none.
        """
        table = ScidbSource(parity_db, pushdown=False).get_table(["Scalar"])
        assert table.reducer is None
        assert reducer_for(table) is DEFAULT_REDUCER

    def test_a_table_carries_the_reducer_its_source_set(self, source):
        """The seam every later stage uses: the source decides."""
        table = source.get_table(["Scalar"])
        sentinel = PandasReducer()
        table.reducer = sentinel
        assert reducer_for(table) is sentinel


# ---------------------------------------------------------------------------
# Every kind resolves through the reducer, for every shape it applies to
# ---------------------------------------------------------------------------

#: (kind, measure, roles, aggregate) — one row per cell of the plan's §3 table
#: that is a real combination. Heatmap is the only 2-D kind; box/violin/strip
#: are observation kinds; band/bar summarise.
KIND_CASES = [
    ("line-1d", PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.FREE}, None),
    ("scatter-scalar", PlotKind.SCATTER, "Scalar", {"subject": Role.X, "trial": Role.FREE}, None),
    ("band-1d-sd", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD)),
    ("band-1d-sem", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SEM)),
    ("band-1d-ci95", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.CI95)),
    ("band-1d-iqr-median", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEDIAN, error=ErrorBand.IQR)),
    ("bar-scalar-sd", PlotKind.BAR, "Scalar", {"subject": Role.X, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD)),
    ("box-scalar", PlotKind.BOX, "Scalar", {"subject": Role.X, "trial": Role.FREE}, None),
    # A 1-D measure's x axis IS its sample index (roles.py:265), so no factor may
    # hold X; the distribution is across samples, per colour.
    ("box-1d", PlotKind.BOX, "Series", {"subject": Role.COLOR, "trial": Role.FREE}, None),
    ("violin-scalar", PlotKind.VIOLIN, "Scalar", {"subject": Role.X, "trial": Role.FREE}, None),
    ("strip-scalar", PlotKind.STRIP, "Scalar", {"subject": Role.X, "trial": Role.FREE}, None),
    # FREE, not AGGREGATE: `_matrix_frame` is where matrices are averaged, and
    # it is only reached when no factor is AGGREGATE — an AGGREGATE role sends 2-D
    # cells into `_collapse_aggregates`, whose pandas `.mean()` cannot average
    # object cells and raises. A real gap, pre-existing, surfaced by this
    # fixture; recorded for Stage 4 (plan §10). Every existing heatmap test
    # uses FREE for the same reason.
    ("heatmap-2d", PlotKind.HEATMAP, "Matrix", {"subject": Role.ITERATE, "trial": Role.FREE}, None),
]


def _spec(kind, measure, roles, aggregate) -> PlotSpec:
    kwargs = dict(measures=[measure], roles=roles, kind=kind)
    if aggregate is not None:
        kwargs["aggregate"] = aggregate
    return PlotSpec(**kwargs)


class _Spy:
    """A reducer that records every call and forwards to the reference.

    Proves each kind actually ROUTES through the contract: a kind that reduces
    inline somewhere else would pass a value-parity test while bypassing the
    seam entirely, which is the one way Stage 2 could silently do nothing.
    """

    def __init__(self):
        self.calls: list[str] = []
        self._ref = PandasReducer()

    def y_extents(self, *a, **k):
        self.calls.append("y_extents")
        return self._ref.y_extents(*a, **k)

    def explode_series(self, *a, **k):
        self.calls.append("explode_series")
        return self._ref.explode_series(*a, **k)

    def downsample(self, *a, **k):
        self.calls.append("downsample")
        return self._ref.downsample(*a, **k)

    def matrix_mean(self, *a, **k):
        self.calls.append("matrix_mean")
        return self._ref.matrix_mean(*a, **k)


@pytest.mark.parametrize("label,kind,measure,roles,aggregate", KIND_CASES, ids=[c[0] for c in KIND_CASES])
class TestEveryKindRoutesThroughTheReducer:
    def test_resolves_and_calls_y_extents(self, source, label, kind, measure, roles, aggregate):
        table = source.get_table([measure])
        spy = _Spy()
        table.reducer = spy
        figures = resolve(_spec(kind, measure, roles, aggregate), table)
        assert figures, f"{label}: no figure"
        assert "y_extents" in spy.calls, f"{label}: y limits bypassed the reducer"

    def test_1d_kinds_explode_through_the_reducer(self, source, label, kind, measure, roles, aggregate):
        if measure != "Series":
            pytest.skip("only 1-D measures explode")
        table = source.get_table([measure])
        spy = _Spy()
        table.reducer = spy
        resolve(_spec(kind, measure, roles, aggregate), table)
        assert "explode_series" in spy.calls, f"{label}: explode bypassed the reducer"

    def test_2d_kinds_average_through_the_reducer(self, source, label, kind, measure, roles, aggregate):
        if measure != "Matrix":
            pytest.skip("only 2-D measures average matrices")
        table = source.get_table([measure])
        spy = _Spy()
        table.reducer = spy
        resolve(_spec(kind, measure, roles, aggregate), table)
        assert "matrix_mean" in spy.calls, f"{label}: matrix mean bypassed the reducer"


class TestDownsampleRoutesThroughTheReducer:
    def test_transport_budget_calls_downsample(self, source):
        """Only when the figure exceeds the budget — exactly as before."""
        table = source.get_table(["Series"])
        spy = _Spy()
        table.reducer = spy
        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.FREE}, None)
        resolve(spec, table, max_points=10)
        assert "downsample" in spy.calls

    def test_full_resolution_never_downsamples(self, source):
        table = source.get_table(["Series"])
        spy = _Spy()
        table.reducer = spy
        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.FREE}, None)
        resolve(spec, table, max_points=None)
        assert "downsample" not in spy.calls


# ---------------------------------------------------------------------------
# The oracle: the reference equals the pre-refactor inline functions, exactly
# ---------------------------------------------------------------------------


class TestReferenceEqualsTheOriginals:
    """``PandasReducer`` must be a pure delegation, byte-for-byte.

    Compared against the underlying functions directly, on the frames the
    resolve path would hand over. If a future edit makes the reference compute
    something of its own, one of these fails.
    """

    def test_y_extents_raw(self, source):
        from scistackplot.ylimits import limits_by_scope

        table = source.get_table(["Series"])
        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.FACET, "trial": Role.FREE}, None)
        expected = limits_by_scope(table, spec, ["subject"])
        assert PandasReducer().y_extents(table.frame, spec, table, ["subject"]) == expected

    @pytest.mark.parametrize("error", [ErrorBand.SD, ErrorBand.SEM, ErrorBand.CI95, ErrorBand.IQR])
    def test_y_extents_aggregated(self, source, error):
        from scistackplot.ylimits import limits_by_scope

        table = source.get_table(["Series"])
        stat = Statistic.MEDIAN if error is ErrorBand.IQR else Statistic.MEAN
        spec = _spec(
            PlotKind.BAND, "Series", {"subject": Role.FACET, "trial": Role.AGGREGATE},
            Aggregation(statistic=stat, error=error),
        )
        expected = limits_by_scope(table, spec, ["subject"])
        got = PandasReducer().y_extents(table.frame, spec, table, ["subject"])
        assert got.keys() == expected.keys()
        for key in expected:
            assert got[key] == pytest.approx(expected[key], nan_ok=True)

    def test_y_extents_scalar(self, source):
        from scistackplot.ylimits import limits_by_scope

        table = source.get_table(["Scalar"])
        spec = _spec(PlotKind.SCATTER, "Scalar", {"subject": Role.FACET, "trial": Role.FREE}, None)
        expected = limits_by_scope(table, spec, ["subject"])
        assert PandasReducer().y_extents(table.frame, spec, table, ["subject"]) == expected

    def test_explode_series(self, source):
        from scistackplot.reduce import _explode_1d

        table = source.get_table(["Series"])
        frame = table.frame
        a, ia = _explode_1d(frame, "Series", "index")
        b, ib = PandasReducer().explode_series(frame, "Series", "index", table)
        assert ia == ib
        pd.testing.assert_frame_equal(a, b)

    def test_downsample(self, source):
        from scistackplot.reduce import _downsample, _explode_1d

        frame, idx = _explode_1d(source.get_table(["Series"]).frame, "Series", "index")
        pd.testing.assert_frame_equal(
            _downsample(frame, 7, idx), PandasReducer().downsample(frame, 7, idx)
        )

    def test_matrix_mean(self, source):
        from scistackplot.reduce import _matrix_frame

        frame = source.get_table(["Matrix"]).frame
        s1 = frame[frame["subject"] == "s1"]
        a = _matrix_frame(s1, "Matrix")
        b = PandasReducer().matrix_mean(s1, "Matrix")
        np.testing.assert_array_equal(a.iloc[0, 0], b.iloc[0, 0])

    def test_matrix_mean_mismatch_takes_the_first(self, source, caplog):
        """The heatmap shape-mismatch rule survives the delegation."""
        import logging

        frame = source.get_table(["Matrix"]).frame
        with caplog.at_level(logging.WARNING, logger="scistackplot"):
            out = PandasReducer().matrix_mean(frame, "Matrix")
        assert out.iloc[0, 0].shape == (3, 4)
        assert "differing shapes" in "\n".join(r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Fixture edge cases the parity table needs to be true
# ---------------------------------------------------------------------------


class TestFixtureHitsTheEdges:
    """If the fixture stops exercising an edge, the parity tests stop meaning
    anything for it. These pin the fixture, not the code."""

    def test_series_is_ragged(self, source):
        lengths = {len(v) for v in source.get_table(["Series"]).frame["Series"]}
        assert len(lengths) > 1

    def test_series_has_a_nan_and_an_all_nan_cell(self, source):
        cells = [np.asarray(v, dtype=float) for v in source.get_table(["Series"]).frame["Series"]]
        assert any(np.isnan(c).any() and not np.isnan(c).all() for c in cells)
        assert any(np.isnan(c).all() for c in cells)

    def test_scalar_has_an_n_equals_1_group(self, source):
        counts = source.get_table(["Scalar"]).frame.groupby("subject").size()
        assert (counts == 1).any()

    def test_matrix_has_two_shapes(self, source):
        shapes = {np.asarray(v).shape for v in source.get_table(["Matrix"]).frame["Matrix"]}
        assert len(shapes) == 2


# ---------------------------------------------------------------------------
# A pre-existing gap the fixture surfaced — pinned red, for Stage 4
# ---------------------------------------------------------------------------


class TestAggregateOnA2DMeasure:
    """``AGGREGATE`` over a DOUBLE[][] measure has never worked.

    Averaging matrices lives in ``_matrix_frame`` (elementwise mean), reached
    from ``_panel_frame`` — but only when no factor holds AGGREGATE. Give one
    the AGGREGATE role and the 2-D cells go through ``_collapse_aggregates``
    first, whose pandas ``groupby(...).mean()`` cannot average object cells:
    ``TypeError: agg function failed [how->mean, dtype->object]``.

    Found by this fixture on 2026-09-13; no earlier heatmap test used
    AGGREGATE. It is NOT fixed in Stage 1 (behaviour-preserving by definition).
    Stage 4 rebuilds the 2-D reduction and should turn this green — at which
    point the ``xfail`` must be REMOVED, not left to pass silently
    (``strict=True`` makes an unexpected pass a failure).
    """

    @pytest.mark.xfail(
        raises=TypeError,
        strict=True,
        reason="AGGREGATE routes DOUBLE[][] into _collapse_aggregates' scalar mean; Stage 4",
    )
    def test_aggregate_over_trials_averages_matrices(self, source):
        table = source.get_table(["Matrix"])
        spec = _spec(
            PlotKind.HEATMAP, "Matrix", {"subject": Role.ITERATE, "trial": Role.AGGREGATE}, None
        )
        resolve(spec, table)


# ---------------------------------------------------------------------------
# Stage 2: DuckDBReducer.y_extents == the pandas reference
# ---------------------------------------------------------------------------


@pytest.fixture
def pandas_source(parity_db) -> ScidbSource:
    """The same database, reductions kept in pandas — the oracle side."""
    return ScidbSource(parity_db, pushdown=False)


@pytest.fixture
def duckdb_source(parity_db) -> ScidbSource:
    """The same database, reductions pushed to DuckDB — the side under test."""
    return ScidbSource(parity_db, pushdown=True)


def _extents(source, kind, measure, roles, aggregate, scope):
    """Resolve far enough to get `y_extents` called, capturing what it returns."""
    from scistackplot.reducer import reducer_for
    from scistackplot.roles import complete_roles
    from scistackplot.ylimits import eligible_scope

    table = source.get_table([measure])
    spec = _spec(kind, measure, roles, aggregate)
    completed = complete_roles(spec, table)
    present = eligible_scope(scope, completed, table)
    return reducer_for(table).y_extents(table.frame, spec, table, present)


def _assert_extents_equal(got: dict, expected: dict, label: str) -> None:
    assert set(got) == set(expected), f"{label}: group keys differ\n  got {sorted(got)}\n  want {sorted(expected)}"
    for key in expected:
        assert got[key] == pytest.approx(expected[key], rel=1e-9, abs=1e-9), (
            f"{label}: extents differ at {key!r}: got {got[key]}, want {expected[key]}"
        )


class TestDuckDBSourceAttachesTheReducer:
    def test_pushdown_source_tables_carry_a_duckdb_reducer(self, duckdb_source):
        from scistackplotdb.reducer import DuckDBReducer

        assert isinstance(duckdb_source.get_table(["Scalar"]).reducer, DuckDBReducer)

    def test_pushdown_off_leaves_the_pandas_reference(self, pandas_source):
        assert pandas_source.get_table(["Scalar"]).reducer is None
        assert reducer_for(pandas_source.get_table(["Scalar"])) is DEFAULT_REDUCER

    def test_one_reducer_per_source(self, duckdb_source):
        a = duckdb_source.get_table(["Scalar"]).reducer
        b = duckdb_source.get_table(["Series"]).reducer
        assert a is b


#: (label, kind, measure, roles, aggregate, scope). Raw AND aggregated modes,
#: every error band, every shape, scoped and unscoped, including the fixture's
#: n=1 subject and the all-NaN cell.
EXTENT_CASES = [
    # ---- raw, 1-D --------------------------------------------------------
    ("raw-1d-global", PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.FREE}, None, []),
    ("raw-1d-by-subject", PlotKind.LINE, "Series", {"subject": Role.FACET, "trial": Role.FREE}, None, ["subject"]),
    # ---- raw, scalar -----------------------------------------------------
    ("raw-scalar-global", PlotKind.SCATTER, "Scalar", {"subject": Role.X, "trial": Role.FREE}, None, []),
    ("raw-scalar-by-subject", PlotKind.SCATTER, "Scalar", {"subject": Role.FACET, "trial": Role.FREE}, None, ["subject"]),
    # ---- raw, 2-D --------------------------------------------------------
    ("raw-2d-global", PlotKind.HEATMAP, "Matrix", {"subject": Role.ITERATE, "trial": Role.FREE}, None, []),
    ("raw-2d-by-subject", PlotKind.HEATMAP, "Matrix", {"subject": Role.ITERATE, "trial": Role.FREE}, None, ["subject"]),
    # ---- aggregated, 1-D, every band, global and scoped -------------------
    ("agg-1d-sd", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD), []),
    ("agg-1d-sd-by-subject", PlotKind.BAND, "Series", {"subject": Role.FACET, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD), ["subject"]),
    ("agg-1d-sem", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SEM), []),
    ("agg-1d-ci95", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.CI95), []),
    ("agg-1d-iqr-median", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEDIAN, error=ErrorBand.IQR), []),
    ("agg-1d-iqr-median-by-subject", PlotKind.BAND, "Series", {"subject": Role.FACET, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEDIAN, error=ErrorBand.IQR), ["subject"]),
    # ---- aggregated, scalar (bar), incl. the n=1 subject ------------------
    ("agg-scalar-sd", PlotKind.BAR, "Scalar", {"subject": Role.X, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD), []),
    ("agg-scalar-sem-by-subject", PlotKind.BAR, "Scalar", {"subject": Role.FACET, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SEM), ["subject"]),
    ("agg-scalar-iqr-median", PlotKind.BAR, "Scalar", {"subject": Role.X, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEDIAN, error=ErrorBand.IQR), []),
]


@pytest.mark.parametrize(
    "label,kind,measure,roles,aggregate,scope", EXTENT_CASES, ids=[c[0] for c in EXTENT_CASES]
)
def test_duckdb_y_extents_equal_pandas(
    pandas_source, duckdb_source, label, kind, measure, roles, aggregate, scope
):
    """The §4 parity table, one row at a time, over the edge-case fixture."""
    expected = _extents(pandas_source, kind, measure, roles, aggregate, scope)
    got = _extents(duckdb_source, kind, measure, roles, aggregate, scope)
    _assert_extents_equal(got, expected, label)


class TestDuckDBExtentsActuallyRanInDuckDB:
    """Parity would also pass if the pushdown silently fell back to pandas."""

    def test_raw_path_logs_the_duckdb_timer(self, duckdb_source, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger="scistackplotdb"):
            _extents(duckdb_source, PlotKind.LINE, "Series",
                     {"subject": Role.COLOR, "trial": Role.FREE}, None, [])
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "[timing] y_extents(duckdb)" in text
        assert "raw=" in text
        assert "pandas fallback" not in text

    def test_aggregated_path_logs_the_duckdb_timer(self, duckdb_source, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger="scistackplotdb"):
            _extents(duckdb_source, PlotKind.BAND, "Series",
                     {"subject": Role.COLOR, "trial": Role.AGGREGATE},
                     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD), [])
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "[timing] y_extents(duckdb)" in text
        assert "aggregated=" in text
        assert "pandas fallback" not in text

    def test_no_payload_column_is_fetched_into_python(self, duckdb_source, monkeypatch):
        """The whole point: the reduction must not pull the arrays out.

        Spied on `_fetchall` (the boxing path). The reducer's own queries go
        through `_fetchall_with_frame`; anything selecting the Series column
        through plain `_fetchall` during y_extents is the old behaviour leaking.
        """
        table = duckdb_source.get_table(["Series"])  # the load itself is allowed
        seen: list[str] = []
        original = duckdb_source._db._duck._fetchall

        def spy(sql, params=None):
            seen.append(sql)
            return original(sql, params)

        monkeypatch.setattr(duckdb_source._db._duck, "_fetchall", spy)
        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.FREE}, None)
        reducer_for(table).y_extents(table.frame, spec, table, [])
        payload_selects = [s for s in seen if "SELECT t.record_id" in s and '"Series"' in s]
        assert payload_selects == [], f"y_extents re-fetched the payload: {payload_selects}"


class TestDuckDBExtentsFallBackHonestly:
    def test_exploded_table_falls_back_to_pandas(self, duckdb_source, caplog):
        """A pre-exploded frame's values ARE the frame; DuckDB holds them nested."""
        import logging

        from dataclasses import replace

        from scistackplot.reduce import _explode_1d

        table = duckdb_source.get_table(["Series"])
        frame, idx = _explode_1d(table.frame, "Series", "index")
        exploded = replace(table, frame=frame, index_column=idx)
        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.FREE}, None)
        with caplog.at_level(logging.INFO, logger="scistackplotdb"):
            got = reducer_for(exploded).y_extents(frame, spec, exploded, [])
        assert "pandas fallback" in "\n".join(r.getMessage() for r in caplog.records)
        # And the fallback is still the right answer.
        expected = PandasReducer().y_extents(frame, spec, exploded, [])
        _assert_extents_equal(got, expected, "exploded-fallback")

    def test_frame_without_record_id_falls_back(self, duckdb_source):
        table = duckdb_source.get_table(["Scalar"])
        frame = table.frame.drop(columns=["record_id"])
        spec = _spec(PlotKind.SCATTER, "Scalar", {"subject": Role.X, "trial": Role.FREE}, None)
        got = reducer_for(table).y_extents(frame, spec, table, [])
        expected = PandasReducer().y_extents(frame, spec, table, [])
        _assert_extents_equal(got, expected, "no-record-id-fallback")


class TestDuckDBExtentsThroughResolve:
    """End to end: the figure a pushdown source produces has the same y limits."""

    @pytest.mark.parametrize("label,kind,measure,roles,aggregate", KIND_CASES, ids=[c[0] for c in KIND_CASES])
    def test_resolved_panel_limits_match(
        self, pandas_source, duckdb_source, label, kind, measure, roles, aggregate
    ):
        spec = _spec(kind, measure, roles, aggregate)
        a = resolve(spec, pandas_source.get_table([measure]))
        b = resolve(spec, duckdb_source.get_table([measure]))
        assert len(a) == len(b), label
        for fa, fb in zip(a, b, strict=True):
            for pa, pb in zip(fa.panels, fb.panels, strict=True):
                la, lb = getattr(pa, "y_limits", None), getattr(pb, "y_limits", None)
                if la is None and lb is None:
                    continue
                assert la == pytest.approx(lb, rel=1e-9, abs=1e-9), f"{label}: {la} vs {lb}"


# ---------------------------------------------------------------------------
# Stage 3: DuckDBReducer.explode_series == the pandas reference, frame for frame
# ---------------------------------------------------------------------------


class TestDuckDBExplodeEqualsPandas:
    """The explode is reproduced EXACTLY — same rows, same order, same dtypes.

    Not "same figure": `_collapse_aggregates` and `_downsample` run on this
    frame afterwards and both are order-sensitive (`_downsample` strides by row,
    and the exploded frame is record-major), so a frame that differed only in
    row order would draw a different line. `assert_frame_equal` is the contract.
    """

    def _both(self, pandas_source, duckdb_source, measure="Series"):
        from scistackplot.reducer import reducer_for

        p = pandas_source.get_table([measure])
        d = duckdb_source.get_table([measure])
        a, ia = reducer_for(p).explode_series(p.frame, measure, "index", p)
        b, ib = reducer_for(d).explode_series(d.frame, measure, "index", d)
        return (a, ia), (b, ib)

    def test_frames_are_identical(self, pandas_source, duckdb_source):
        (a, ia), (b, ib) = self._both(pandas_source, duckdb_source)
        assert ia == ib == "index"
        pd.testing.assert_frame_equal(
            a.reset_index(drop=True), b.reset_index(drop=True), check_dtype=False
        )

    def test_nan_samples_leave_a_gap_not_a_renumbering(self, duckdb_source):
        """s1/t1 has NaN at position 4: positions must run 0,1,2,3,5,... ."""
        from scistackplot.reducer import reducer_for

        d = duckdb_source.get_table(["Series"])
        b, _ = reducer_for(d).explode_series(d.frame, "Series", "index", d)
        row = b[(b["subject"] == "s1") & (b["trial"] == "1")]
        positions = row["index"].tolist()
        assert 4 not in positions
        assert positions == sorted(positions)
        assert positions[:4] == [0, 1, 2, 3]
        assert positions[4] == 5

    def test_all_nan_cell_contributes_no_rows(self, duckdb_source):
        """s2/t3 is all NaN: the record vanishes from the exploded frame."""
        from scistackplot.reducer import reducer_for

        d = duckdb_source.get_table(["Series"])
        b, _ = reducer_for(d).explode_series(d.frame, "Series", "index", d)
        assert b[(b["subject"] == "s2") & (b["trial"] == "3")].empty

    def test_ragged_lengths_are_preserved_per_record(self, pandas_source, duckdb_source):
        (a, _), (b, _) = self._both(pandas_source, duckdb_source)
        pa_ = a.groupby(["subject", "trial"]).size()
        pb_ = b.groupby(["subject", "trial"]).size()
        pd.testing.assert_series_equal(pa_, pb_)

    def test_order_is_record_major_then_position(self, duckdb_source):
        """What `_downsample`'s `iloc[::stride]` depends on."""
        from scistackplot.reducer import reducer_for

        d = duckdb_source.get_table(["Series"])
        b, _ = reducer_for(d).explode_series(d.frame, "Series", "index", d)
        # Within each record the index must be strictly increasing, and the
        # records must appear in the original frame's row order.
        rec_order = list(dict.fromkeys(zip(b["subject"], b["trial"])))
        frame_order = list(zip(d.frame["subject"], d.frame["trial"]))
        frame_order = [k for k in frame_order if k in set(rec_order)]
        assert rec_order == frame_order
        for _, part in b.groupby(["subject", "trial"], sort=False):
            assert part["index"].is_monotonic_increasing

    def test_explode_actually_ran_in_duckdb(self, duckdb_source, caplog):
        import logging

        from scistackplot.reducer import reducer_for

        d = duckdb_source.get_table(["Series"])
        with caplog.at_level(logging.INFO, logger="scistackplotdb"):
            reducer_for(d).explode_series(d.frame, "Series", "index", d)
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "[timing] explode_series(duckdb)" in text
        assert "unnest=" in text
        assert "[duckdb]" in text
        assert "pandas fallback" not in text

    def test_samples_never_pass_through_fetchall(self, duckdb_source, monkeypatch):
        """The columnar path is the point: `.df()`, never `fetchall()`.

        `fetchall` boxes a Python float per sample — the exact cost this stage
        removes. Any UNNEST reaching it during the explode is the old behaviour
        leaking back in through a different door.
        """
        from scistackplot.reducer import reducer_for

        d = duckdb_source.get_table(["Series"])
        seen: list[str] = []
        original = duckdb_source._db._duck._fetchall

        def spy(sql, params=None):
            seen.append(sql)
            return original(sql, params)

        monkeypatch.setattr(duckdb_source._db._duck, "_fetchall", spy)
        reducer_for(d).explode_series(d.frame, "Series", "index", d)
        assert not [s for s in seen if "UNNEST" in s], seen

    def test_existing_index_column_raises_like_the_reference(self, duckdb_source):
        from scistackplot.reducer import reducer_for

        d = duckdb_source.get_table(["Series"])
        frame = d.frame.assign(index=0)
        with pytest.raises(ValueError, match="already exists"):
            reducer_for(d).explode_series(frame, "Series", "index", d)


class TestDuckDBExplodeThroughResolve:
    """End to end: the drawn frames agree for every 1-D kind.

    This is what makes the record-major order argument above matter — the
    downsampled, collapsed panel frames must match, not just the explode.
    """

    @pytest.mark.parametrize(
        "label,kind,measure,roles,aggregate",
        [c for c in KIND_CASES if c[2] == "Series"],
        ids=[c[0] for c in KIND_CASES if c[2] == "Series"],
    )
    def test_panel_frames_match(
        self, pandas_source, duckdb_source, label, kind, measure, roles, aggregate
    ):
        spec = _spec(kind, measure, roles, aggregate)
        for max_points in (None, 25):
            a = resolve(spec, pandas_source.get_table([measure]), max_points=max_points)
            b = resolve(spec, duckdb_source.get_table([measure]), max_points=max_points)
            assert len(a) == len(b), label
            for fa, fb in zip(a, b, strict=True):
                assert len(fa.panels) == len(fb.panels), label
                for pa_, pb_ in zip(fa.panels, fb.panels, strict=True):
                    pd.testing.assert_frame_equal(
                        pa_.frame.reset_index(drop=True),
                        pb_.frame.reset_index(drop=True),
                        check_dtype=False,
                        check_like=True,
                    )
