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

        `ScidbSource` attaches the numpy reducer by default (its own tests pin
        that), so the "no reducer" case has to be asked for explicitly —
        `fast=False` is the in-tree way a table ends up with none.
        """
        table = ScidbSource(parity_db, fast=False).get_table(["Scalar"])
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

    def collapse_series(self, *a, **k):
        self.calls.append("collapse_series")
        return self._ref.collapse_series(*a, **k)

    def summarize_series(self, *a, **k):
        self.calls.append("summarize_series")
        return self._ref.summarize_series(*a, **k)

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

    def test_1d_kinds_reduce_through_the_reducer(self, source, label, kind, measure, roles, aggregate):
        """Which per-sample operation a 1-D kind takes is part of the contract:
        a SCALAR kind collapses each vector to one value first, BAND/BAR
        summarise from the cells, an AGGREGATE role collapses, and everything
        else explodes.

        A SCALAR kind is the exception, and it is not a reducer operation at
        all: ``collapse.apply_collapse`` turns each vector into ONE value over
        the cells, before any panel work, so the measure the panel path sees is
        scalar and there is no per-sample work left to route. That is a
        different operation from ``Reducer.collapse_series``, which is the
        AGGREGATE collapse — a mean per sample POSITION across aggregated
        factors, whose result is still 1-D. Confusing the two is easy: they
        share a word and neither name says which.

        This branch used to demand ``explode_series`` for every non-BAND/BAR
        kind, which went stale the moment scalar kinds began implying a
        collapse. It asks ``collapses`` now rather than re-deriving the rule.
        """
        if measure != "Series":
            pytest.skip("only 1-D measures have per-sample work")
        from scistackplot.collapse import collapses

        table = source.get_table([measure])
        spec = _spec(kind, measure, roles, aggregate)
        spy = _Spy()
        table.reducer = spy
        resolve(spec, table)
        if collapses(spec, table):
            # The vectors are gone by the time the panel path runs. What must
            # NOT happen is per-sample work on top of a collapse already done —
            # that would be the samples being read twice, once per meaning.
            assert not ({"explode_series", "collapse_series", "summarize_series"}
                        & set(spy.calls)), (
                f"{label}: the kind implies a vector-to-scalar collapse, so no "
                f"per-sample reducer work should remain ({spy.calls})"
            )
            return
        if kind in (PlotKind.BAND, PlotKind.BAR):
            expected = "summarize_series"
        elif Role.AGGREGATE in roles.values():
            expected = "collapse_series"
        else:
            expected = "explode_series"
        assert expected in spy.calls, f"{label}: {expected} bypassed the reducer ({spy.calls})"
        assert "explode_series" not in spy.calls or expected == "explode_series", (
            f"{label}: a 1-D {kind} should never explode ({spy.calls})"
        )

    def test_2d_kinds_average_through_the_reducer(self, source, label, kind, measure, roles, aggregate):
        if measure != "Matrix":
            pytest.skip("only 2-D measures average matrices")
        table = source.get_table([measure])
        spy = _Spy()
        table.reducer = spy
        resolve(_spec(kind, measure, roles, aggregate), table)
        assert "matrix_mean" in spy.calls, f"{label}: matrix mean bypassed the reducer"


class TestDownsampleRoutesThroughTheReducer:
    def test_transport_budget_strides_inside_the_explode(self, source):
        """A LINE's stride is applied by `explode_series` itself (so only the
        kept rows' labels are ever gathered); the figure reports the total."""
        table = source.get_table(["Series"])
        spy = _Spy()
        table.reducer = spy
        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.FREE}, None)
        figures = resolve(spec, table, max_points=10)
        assert "explode_series" in spy.calls
        assert "downsample" not in spy.calls
        assert figures[0].downsampled_from and figures[0].row_count <= 10 + 1

    def test_a_collapsed_line_downsamples_after_collapsing(self, source):
        """With an AGGREGATE role the exploded frame is the collapsed one, and
        the transport stride runs over THAT — after the mean, never before."""
        table = source.get_table(["Series"])
        spy = _Spy()
        table.reducer = spy
        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE}, None)
        resolve(spec, table, max_points=10)
        assert spy.calls.index("collapse_series") < spy.calls.index("downsample")

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
        from scistackplot.roles import complete_roles
        from scistackplot.ylimits import limits_by_scope

        table = source.get_table(["Series"])
        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.FACET, "trial": Role.FREE}, None)
        roles = complete_roles(spec, table)
        expected = limits_by_scope(table, spec, ["subject"], roles)
        assert PandasReducer().y_extents(table.frame, spec, table, ["subject"], roles) == expected

    @pytest.mark.parametrize("error", [ErrorBand.SD, ErrorBand.SEM, ErrorBand.CI95, ErrorBand.IQR])
    def test_y_extents_aggregated(self, source, error):
        from scistackplot.roles import complete_roles
        from scistackplot.ylimits import limits_by_scope

        table = source.get_table(["Series"])
        stat = Statistic.MEDIAN if error is ErrorBand.IQR else Statistic.MEAN
        spec = _spec(
            PlotKind.BAND, "Series", {"subject": Role.FACET, "trial": Role.AGGREGATE},
            Aggregation(statistic=stat, error=error),
        )
        roles = complete_roles(spec, table)
        expected = limits_by_scope(table, spec, ["subject"], roles)
        got = PandasReducer().y_extents(table.frame, spec, table, ["subject"], roles)
        assert got.keys() == expected.keys()
        for key in expected:
            assert got[key] == pytest.approx(expected[key], nan_ok=True)

    def test_y_extents_scalar(self, source):
        from scistackplot.roles import complete_roles
        from scistackplot.ylimits import limits_by_scope

        table = source.get_table(["Scalar"])
        spec = _spec(PlotKind.SCATTER, "Scalar", {"subject": Role.FACET, "trial": Role.FREE}, None)
        roles = complete_roles(spec, table)
        expected = limits_by_scope(table, spec, ["subject"], roles)
        assert PandasReducer().y_extents(table.frame, spec, table, ["subject"], roles) == expected

    def test_explode_series(self, source):
        from scistackplot.reduce import _explode_1d

        table = source.get_table(["Series"])
        frame = table.frame
        a, ia = _explode_1d(frame, "Series", "index")
        b, ib, total = PandasReducer().explode_series(frame, "Series", "index", table)
        assert ia == ib
        assert total == len(a)
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
# AGGREGATE over a 2-D measure — a gap the fixture surfaced, closed by ndarray cells
# ---------------------------------------------------------------------------


class TestAggregateOnA2DMeasure:
    """``AGGREGATE`` over a DOUBLE[][] measure.

    Pinned RED on 2026-09-13 (strict xfail): with list cells, the AGGREGATE
    role sent the matrices through ``_collapse_aggregates``' pandas
    ``groupby(...).mean()``, which cannot average object cells holding lists
    (``TypeError: agg function failed``). The same day the fetch started
    handing cells over as ndarrays (``test_load_fetch.py``) and the xfail
    XPASSed: the object-column mean sums ndarrays elementwise. So the test
    now asserts the answer, not the absence of an error — one figure per
    subject, and the averaged matrix is the elementwise mean of that subject's
    matrices.
    """

    def test_aggregate_over_trials_averages_matrices(self, source):
        from scistackplot.resolved import Z

        table = source.get_table(["Matrix"])
        spec = _spec(
            PlotKind.HEATMAP, "Matrix", {"subject": Role.ITERATE, "trial": Role.AGGREGATE}, None
        )
        figures = resolve(spec, table)
        assert [f.figure_key["subject"] for f in figures] == ["s1", "s2"]

        frame = table.frame
        s1 = [np.asarray(v, dtype=float) for v in frame.loc[frame["subject"] == "s1", "Matrix"]]
        assert len(s1) == 2
        expected = np.mean(np.stack(s1), axis=0)
        got = np.asarray(figures[0].panels[0].frame[Z].iloc[0], dtype=float)
        np.testing.assert_allclose(got, expected)


# ---------------------------------------------------------------------------
# NumpyReducer == the pandas reference
# ---------------------------------------------------------------------------
#
# The side under test changed on 2026-09-13: a DuckDB-SQL reducer was measured
# against numpy over the loaded cells and lost every reduction by 12-40x
# (.claude/plan-plot-minimal-load-examples.md §8), so the fast reducer is now
# `scistackplot.NumpyReducer` and touches no database at all. The oracle and
# the fixture are unchanged; every test below asks the same question of the new
# implementation.


@pytest.fixture
def pandas_source(parity_db) -> ScidbSource:
    """The same database, reductions kept in pandas — the oracle side."""
    return ScidbSource(parity_db, fast=False)


@pytest.fixture
def numpy_source(parity_db) -> ScidbSource:
    """The same database, reductions in numpy — the side under test."""
    return ScidbSource(parity_db, fast=True)


def _extents(source, kind, measure, roles, aggregate, scope):
    """Resolve far enough to get `y_extents` called, capturing what it returns."""
    from scistackplot.reducer import reducer_for
    from scistackplot.roles import complete_roles
    from scistackplot.ylimits import eligible_scope

    table = source.get_table([measure])
    spec = _spec(kind, measure, roles, aggregate)
    completed = complete_roles(spec, table)
    present = eligible_scope(scope, completed, table)
    return reducer_for(table).y_extents(table.frame, spec, table, present, completed)


def _assert_extents_equal(got: dict, expected: dict, label: str) -> None:
    assert set(got) == set(expected), f"{label}: group keys differ\n  got {sorted(got)}\n  want {sorted(expected)}"
    for key in expected:
        assert got[key] == pytest.approx(expected[key], rel=1e-9, abs=1e-9), (
            f"{label}: extents differ at {key!r}: got {got[key]}, want {expected[key]}"
        )


def _sorted_panel(frame: pd.DataFrame) -> pd.DataFrame:
    """A panel frame in a canonical row order for comparison.

    BAR panels are not sorted by either reducer (only BAND sorts by X), and the
    reference's groupby(sort=False) order depends on record order in a way the
    numpy path does not reproduce; the VALUES are the contract."""
    from scistackplot.resolved import COLOR, X

    keys = [c for c in (X, COLOR) if c in frame.columns]
    return frame.sort_values(keys, kind="stable").reset_index(drop=True) if keys else frame


class TestScidbSourceAttachesTheReducer:
    def test_fast_source_tables_carry_the_numpy_reducer(self, numpy_source):
        from scistackplot.reducer import NumpyReducer

        assert isinstance(numpy_source.get_table(["Scalar"]).reducer, NumpyReducer)

    def test_fast_off_leaves_the_pandas_reference(self, pandas_source):
        assert pandas_source.get_table(["Scalar"]).reducer is None
        assert reducer_for(pandas_source.get_table(["Scalar"])) is DEFAULT_REDUCER

    def test_one_reducer_per_source(self, numpy_source):
        a = numpy_source.get_table(["Scalar"]).reducer
        b = numpy_source.get_table(["Series"]).reducer
        assert a is b

    def test_the_numpy_reducer_satisfies_the_protocol(self):
        from scistackplot.reducer import NumpyReducer

        assert isinstance(NumpyReducer(), Reducer)


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
    # ---- aggregated, 1-D, replicates FREE (a band across trials) ----------
    ("agg-1d-free-sd", PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.FREE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD), []),
    ("agg-1d-free-iqr-by-subject", PlotKind.BAND, "Series", {"subject": Role.FACET, "trial": Role.FREE},
     Aggregation(statistic=Statistic.MEDIAN, error=ErrorBand.IQR), ["subject"]),
    # ---- collapsed, 1-D, non-summary kind (means drawn as they are) --------
    ("collapsed-1d-line", PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE}, None, []),
    ("collapsed-1d-line-by-subject", PlotKind.LINE, "Series", {"subject": Role.FACET, "trial": Role.AGGREGATE}, None, ["subject"]),
    # ---- panel factor NOT in the scope: computed per panel, folded globally --
    ("agg-1d-sem-facet-unscoped", PlotKind.BAND, "Series", {"subject": Role.FACET, "trial": Role.FREE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SEM), []),
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
def test_numpy_y_extents_equal_pandas(
    pandas_source, numpy_source, label, kind, measure, roles, aggregate, scope
):
    """The §4 parity table, one row at a time, over the edge-case fixture."""
    expected = _extents(pandas_source, kind, measure, roles, aggregate, scope)
    got = _extents(numpy_source, kind, measure, roles, aggregate, scope)
    _assert_extents_equal(got, expected, label)


class TestNumpyExtentsActuallyRanInNumpy:
    """Parity would also pass if the fast path silently deferred to pandas."""

    def test_aggregated_path_logs_the_numpy_timer_and_never_explodes(self, numpy_source, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger="scistackplot"):
            _extents(numpy_source, PlotKind.BAND, "Series",
                     {"subject": Role.COLOR, "trial": Role.AGGREGATE},
                     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD), [])
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "[timing] y_extents(numpy)" in text
        assert "stats=" in text
        assert "exploded 1-D measure" not in text

    def test_raw_path_is_the_reference_cell_by_cell(self, numpy_source, caplog):
        """Raw extents were already per-cell numpy in the reference; the fast
        reducer defers rather than duplicating them."""
        import logging

        with caplog.at_level(logging.INFO, logger="scistackplot"):
            _extents(numpy_source, PlotKind.LINE, "Series",
                     {"subject": Role.COLOR, "trial": Role.FREE}, None, [])
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "y_extents(numpy)" not in text
        assert "exploded 1-D measure" not in text


class TestNumpyExtentsDeferHonestly:
    def test_exploded_table_gets_the_reference_answer(self, numpy_source):
        """A pre-exploded frame's values ARE the frame; the reference reduces it."""
        from dataclasses import replace

        from scistackplot.reduce import _explode_1d

        table = numpy_source.get_table(["Series"])
        frame, idx = _explode_1d(table.frame, "Series", "index")
        exploded = replace(table, frame=frame, index_column=idx)
        spec = _spec(PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.FREE},
                     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD))
        from scistackplot.roles import complete_roles

        roles = complete_roles(spec, exploded)
        got = reducer_for(exploded).y_extents(frame, spec, exploded, [], roles)
        expected = PandasReducer().y_extents(frame, spec, exploded, [], roles)
        _assert_extents_equal(got, expected, "exploded-defer")


class TestNumpyExtentsThroughResolve:
    """End to end: the figure a fast source produces has the same y limits."""

    @pytest.mark.parametrize("label,kind,measure,roles,aggregate", KIND_CASES, ids=[c[0] for c in KIND_CASES])
    def test_resolved_panel_limits_match(
        self, pandas_source, numpy_source, label, kind, measure, roles, aggregate
    ):
        spec = _spec(kind, measure, roles, aggregate)
        a = resolve(spec, pandas_source.get_table([measure]))
        b = resolve(spec, numpy_source.get_table([measure]))
        assert len(a) == len(b), label
        for fa, fb in zip(a, b, strict=True):
            for pa, pb in zip(fa.panels, fb.panels, strict=True):
                la, lb = getattr(pa, "y_limits", None), getattr(pb, "y_limits", None)
                if la is None and lb is None:
                    continue
                assert la == pytest.approx(lb, rel=1e-9, abs=1e-9), f"{label}: {la} vs {lb}"


# ---------------------------------------------------------------------------
# NumpyReducer.explode_series == the pandas reference, frame for frame
# ---------------------------------------------------------------------------


class TestNumpyExplodeEqualsPandas:
    """The explode is reproduced EXACTLY — same rows, same order.

    Not "same figure": `_collapse_aggregates` and `_downsample` run on this
    frame afterwards and both are order-sensitive (`_downsample` strides by row,
    and the exploded frame is record-major), so a frame that differed only in
    row order would draw a different line. `assert_frame_equal` is the contract.
    """

    def _both(self, pandas_source, numpy_source, measure="Series", max_points=None):
        p = pandas_source.get_table([measure])
        n = numpy_source.get_table([measure])
        a = reducer_for(p).explode_series(p.frame, measure, "index", p, max_points=max_points)
        b = reducer_for(n).explode_series(n.frame, measure, "index", n, max_points=max_points)
        return a, b

    def test_frames_are_identical(self, pandas_source, numpy_source):
        (a, ia, ta), (b, ib, tb) = self._both(pandas_source, numpy_source)
        assert ia == ib == "index"
        assert ta == tb == len(a)
        pd.testing.assert_frame_equal(
            a.reset_index(drop=True), b.reset_index(drop=True), check_dtype=False
        )

    def test_a_transport_budget_strides_exactly_as_downsample_would(self, pandas_source, numpy_source):
        """The reference explodes then `_downsample`s; numpy strides the kept
        indices before gathering any label. Same rows, or a line plot moves."""
        (a, _, ta), (b, _, tb) = self._both(pandas_source, numpy_source, max_points=25)
        assert ta == tb
        assert len(a) == len(b) < ta
        pd.testing.assert_frame_equal(
            a.reset_index(drop=True), b.reset_index(drop=True), check_dtype=False
        )

    def test_nan_samples_leave_a_gap_not_a_renumbering(self, numpy_source):
        """s1/t1 has NaN at position 4: positions must run 0,1,2,3,5,... ."""
        n = numpy_source.get_table(["Series"])
        b, _, _ = reducer_for(n).explode_series(n.frame, "Series", "index", n)
        s1t1 = b[(b["subject"] == "s1") & (b["trial"] == "1")]["index"].tolist()
        assert s1t1 == [0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11]

    def test_an_all_nan_cell_contributes_no_rows(self, numpy_source):
        n = numpy_source.get_table(["Series"])
        b, _, _ = reducer_for(n).explode_series(n.frame, "Series", "index", n)
        assert b[(b["subject"] == "s2") & (b["trial"] == "3")].empty

    def test_ragged_lengths_are_preserved_per_record(self, pandas_source, numpy_source):
        (a, _, _), (b, _, _) = self._both(pandas_source, numpy_source)
        pa_ = a.groupby(["subject", "trial"]).size()
        pb_ = b.groupby(["subject", "trial"]).size()
        pd.testing.assert_series_equal(pa_, pb_)

    def test_order_is_record_major_then_position(self, numpy_source):
        """What `_downsample`'s `iloc[::stride]` depends on."""
        n = numpy_source.get_table(["Series"])
        b, _, _ = reducer_for(n).explode_series(n.frame, "Series", "index", n)
        rec_order = list(dict.fromkeys(zip(b["subject"], b["trial"])))
        frame_order = list(zip(n.frame["subject"], n.frame["trial"]))
        frame_order = [k for k in frame_order if k in set(rec_order)]
        assert rec_order == frame_order
        for _, part in b.groupby(["subject", "trial"], sort=False):
            assert part["index"].is_monotonic_increasing

    def test_explode_actually_ran_in_numpy(self, numpy_source, caplog):
        import logging

        n = numpy_source.get_table(["Series"])
        with caplog.at_level(logging.INFO, logger="scistackplot"):
            reducer_for(n).explode_series(n.frame, "Series", "index", n)
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "[timing] explode_series(numpy)" in text
        assert "[numpy]" in text

    def test_existing_index_column_raises_like_the_reference(self, numpy_source):
        n = numpy_source.get_table(["Series"])
        frame = n.frame.assign(index=0)
        with pytest.raises(ValueError, match="already exists"):
            reducer_for(n).explode_series(frame, "Series", "index", n)


# ---------------------------------------------------------------------------
# collapse_series and summarize_series == the pandas reference
# ---------------------------------------------------------------------------


class TestNumpyCollapseEqualsPandas:
    """The AGGREGATE collapse of a 1-D measure: mean per position over the
    aggregated factors, one exploded row per kept combination per position."""

    def test_collapsed_frames_agree(self, pandas_source, numpy_source):
        from scistackplot.roles import complete_roles

        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE}, None)
        p = pandas_source.get_table(["Series"])
        n = numpy_source.get_table(["Series"])
        roles = complete_roles(spec, p)
        a, ia = reducer_for(p).collapse_series(p.frame, spec, roles, "index", p)
        b, ib = reducer_for(n).collapse_series(n.frame, spec, roles, "index", n)
        assert ia == ib
        key = ["subject", "index"]
        pd.testing.assert_frame_equal(
            a.sort_values(key).reset_index(drop=True)[[*key, "Series"]],
            b.sort_values(key).reset_index(drop=True)[[*key, "Series"]],
            check_dtype=False,
        )

    def test_a_position_only_some_trials_reach_is_the_mean_of_those(self, numpy_source):
        """s1's trials are 12, 9 and 15 samples long: position 13 is the mean
        of ONE trial, position 10 of two — never NaN-poisoned, never padded."""
        from scistackplot.roles import complete_roles

        spec = _spec(PlotKind.LINE, "Series", {"subject": Role.COLOR, "trial": Role.AGGREGATE}, None)
        n = numpy_source.get_table(["Series"])
        roles = complete_roles(spec, n)
        b, _ = reducer_for(n).collapse_series(n.frame, spec, roles, "index", n)
        s1 = b[b["subject"] == "s1"].set_index("index")["Series"]
        assert s1.index.max() == 14
        cells = {row.trial: np.asarray(row.Series) for row in n.frame[n.frame["subject"] == "s1"].itertuples()}
        assert s1.loc[13] == pytest.approx(cells["3"][13])
        assert s1.loc[10] == pytest.approx(np.mean([cells["1"][10], cells["3"][10]]))


SUMMARY_CASES = [
    ("band-sd-agg", PlotKind.BAND, {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD)),
    ("band-sd-free", PlotKind.BAND, {"subject": Role.COLOR, "trial": Role.FREE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD)),
    ("band-sem-free-nocolor", PlotKind.BAND, {"subject": Role.FREE, "trial": Role.FREE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SEM)),
    ("band-ci95-agg", PlotKind.BAND, {"subject": Role.COLOR, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.CI95)),
    ("band-iqr-median-free", PlotKind.BAND, {"subject": Role.COLOR, "trial": Role.FREE},
     Aggregation(statistic=Statistic.MEDIAN, error=ErrorBand.IQR)),
    ("band-none-free", PlotKind.BAND, {"subject": Role.COLOR, "trial": Role.FREE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.NONE)),
    ("bar-sd-free", PlotKind.BAR, {"subject": Role.COLOR, "trial": Role.FREE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD)),
    ("band-facet-agg", PlotKind.BAND, {"subject": Role.FACET, "trial": Role.AGGREGATE},
     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD)),
]


@pytest.mark.parametrize("label,kind,roles,aggregate", SUMMARY_CASES, ids=[c[0] for c in SUMMARY_CASES])
class TestNumpySummarizeEqualsPandas:
    """centre ± spread per position, from the cells, equals explode + groupby."""

    def test_panel_frames_agree(self, pandas_source, numpy_source, label, kind, roles, aggregate):
        from scistackplot.roles import complete_roles

        spec = _spec(kind, "Series", roles, aggregate)
        p = pandas_source.get_table(["Series"])
        n = numpy_source.get_table(["Series"])
        completed = complete_roles(spec, p)
        a = reducer_for(p).summarize_series(p.frame, spec, completed, "index", p)
        b = reducer_for(n).summarize_series(n.frame, spec, completed, "index", n)
        assert list(a.columns) == list(b.columns), label
        pd.testing.assert_frame_equal(_sorted_panel(a), _sorted_panel(b), check_dtype=False)

    def test_summarize_never_explodes(self, numpy_source, caplog, label, kind, roles, aggregate):
        import logging

        from scistackplot.roles import complete_roles

        spec = _spec(kind, "Series", roles, aggregate)
        n = numpy_source.get_table(["Series"])
        with caplog.at_level(logging.INFO, logger="scistackplot"):
            reducer_for(n).summarize_series(n.frame, spec, complete_roles(spec, n), "index", n)
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "[timing] summarize_series(numpy)" in text
        assert "exploded 1-D measure" not in text


class TestNumpySummarizeEdges:
    def test_one_replicate_has_zero_spread(self, numpy_source):
        """s3 has ONE trial: std(ddof=1) is NaN there and pandas filled 0.0."""
        from scistackplot.resolved import COLOR, Y, Y_HIGH, Y_LOW
        from scistackplot.roles import complete_roles

        spec = _spec(PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.FREE},
                     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD))
        n = numpy_source.get_table(["Series"])
        out = reducer_for(n).summarize_series(n.frame, spec, complete_roles(spec, n), "index", n)
        s3 = out[out[COLOR] == "s3"]
        assert len(s3) == 8
        np.testing.assert_allclose(s3[Y_LOW], s3[Y])
        np.testing.assert_allclose(s3[Y_HIGH], s3[Y])

    def test_the_all_nan_cell_is_not_a_replicate(self, numpy_source):
        """s2/t3 is all NaN: at every position s2's count is 2, not 3."""
        from scistackplot.resolved import COLOR, X, Y
        from scistackplot.roles import complete_roles

        spec = _spec(PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.FREE},
                     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD))
        n = numpy_source.get_table(["Series"])
        out = reducer_for(n).summarize_series(n.frame, spec, complete_roles(spec, n), "index", n)
        s2 = out[out[COLOR] == "s2"].set_index(X)[Y]
        frame = n.frame
        t1 = np.asarray(frame[(frame["subject"] == "s2") & (frame["trial"] == "1")]["Series"].iloc[0])
        t2 = np.asarray(frame[(frame["subject"] == "s2") & (frame["trial"] == "2")]["Series"].iloc[0])
        assert s2.loc[0] == pytest.approx((t1[0] + t2[0]) / 2)
        assert len(s2) == 10


class TestBandThroughResolveIsSummarisedBeforeStriding:
    """The fidelity fix that came with the numpy path, for BOTH reducers.

    The old figure order was explode -> stride -> summarise: at scale a band was
    the mean ± SD of one sample in 8,707. Now BAND/BAR panels are summarised
    over every sample and the transport stride is applied to the SUMMARY.
    """

    @pytest.fixture
    def band(self):
        return _spec(PlotKind.BAND, "Series", {"subject": Role.COLOR, "trial": Role.FREE},
                     Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD))

    def test_a_budget_thins_the_summary_not_the_samples(self, numpy_source, band):
        full = resolve(band, numpy_source.get_table(["Series"]))[0].panels[0].frame
        thin = resolve(band, numpy_source.get_table(["Series"]), max_points=10)[0]
        thinned = thin.panels[0].frame
        assert 0 < len(thinned) < len(full)
        assert thin.downsampled_from == len(full)
        # Every kept row is a row of the full summary, values untouched.
        merged = thinned.merge(full, on=list(thinned.columns), how="inner")
        assert len(merged) == len(thinned)

    def test_both_reducers_agree_at_a_budget(self, pandas_source, numpy_source, band):
        a = resolve(band, pandas_source.get_table(["Series"]), max_points=10)[0].panels[0].frame
        b = resolve(band, numpy_source.get_table(["Series"]), max_points=10)[0].panels[0].frame
        pd.testing.assert_frame_equal(_sorted_panel(a), _sorted_panel(b), check_dtype=False)

    def test_the_figure_never_explodes(self, numpy_source, band, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger="scistackplot"):
            resolve(band, numpy_source.get_table(["Series"]), max_points=10)
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "exploded 1-D measure" not in text
        assert "summarize_series(numpy)" in text


class TestNumpyThroughResolve:
    """End to end: the drawn frames agree for every 1-D kind, at full
    resolution and at a transport budget."""

    @pytest.mark.parametrize(
        "label,kind,measure,roles,aggregate",
        [c for c in KIND_CASES if c[2] == "Series"],
        ids=[c[0] for c in KIND_CASES if c[2] == "Series"],
    )
    def test_panel_frames_match(
        self, pandas_source, numpy_source, label, kind, measure, roles, aggregate
    ):
        spec = _spec(kind, measure, roles, aggregate)
        for max_points in (None, 25):
            a = resolve(spec, pandas_source.get_table([measure]), max_points=max_points)
            b = resolve(spec, numpy_source.get_table([measure]), max_points=max_points)
            assert len(a) == len(b), label
            for fa, fb in zip(a, b, strict=True):
                assert len(fa.panels) == len(fb.panels), label
                assert fa.downsampled_from == fb.downsampled_from, label
                for pa_, pb_ in zip(fa.panels, fb.panels, strict=True):
                    pd.testing.assert_frame_equal(
                        _sorted_panel(pa_.frame),
                        _sorted_panel(pb_.frame),
                        check_dtype=False,
                        check_like=True,
                    )


# ---------------------------------------------------------------------------
# The reduction never touches the database
# ---------------------------------------------------------------------------


class TestReductionNeedsNoDatabase:
    """Why the GUI can release its DuckDB hold before `resolve` (plot_service
    `_load`): every reduction runs over the loaded cells. Close the connection
    after the load and the whole figure still resolves."""

    @pytest.mark.parametrize("label,kind,measure,roles,aggregate", KIND_CASES, ids=[c[0] for c in KIND_CASES])
    def test_resolves_with_the_connection_closed(
        self, numpy_source, parity_db, label, kind, measure, roles, aggregate
    ):
        table = numpy_source.get_table([measure])
        parity_db._duck.close()
        try:
            figures = resolve(_spec(kind, measure, roles, aggregate), table, max_points=25)
            assert figures and all(len(f.panels) for f in figures), label
        finally:
            parity_db._duck.reopen()
