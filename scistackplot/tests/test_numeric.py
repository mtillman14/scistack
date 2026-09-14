"""``coerce_numeric``: the ``to_numeric`` that survives ndarray cells.

Found 2026-09-13: once scistackplotdb's fetch handed 1-D cells over as ndarrays,
``DataFrame.explode`` produced ``np.float64`` objects per sample and
``pd.to_numeric`` raised ``len() of unsized object`` from inside pandas. These
pin the helper on the shapes that broke, and that the explode/ylimits paths
give the same answer from ndarray cells as from list cells.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from scistackplot.numeric import coerce_numeric
from scistackplot.reduce import _explode_1d


class TestCoerceNumeric:
    def test_numpy_scalars_after_an_explode(self):
        exploded = pd.Series([np.float64(1.0), np.float64(np.nan), np.float64(3.0)], dtype=object)
        out = coerce_numeric(exploded)
        assert out.dtype == np.float64
        np.testing.assert_array_equal(out.to_numpy(), [1.0, np.nan, 3.0])

    def test_python_floats_and_none(self):
        out = coerce_numeric(pd.Series([1.0, None, 3.0], dtype=object))
        np.testing.assert_array_equal(out.to_numpy(), [1.0, np.nan, 3.0])

    def test_numpy_ints_from_an_index_column(self):
        out = coerce_numeric(pd.Series([np.int64(0), np.int64(1)], dtype=object))
        assert out.dtype == np.float64
        np.testing.assert_array_equal(out.to_numpy(), [0.0, 1.0])

    def test_a_numeric_column_is_returned_as_is(self):
        series = pd.Series([1.0, 2.0])
        assert coerce_numeric(series) is series

    def test_strings_coerce_to_nan_like_to_numeric(self):
        out = coerce_numeric(pd.Series(["1.5", "x", None], dtype=object))
        np.testing.assert_array_equal(out.to_numpy(), [1.5, np.nan, np.nan])

    def test_array_cells_coerce_to_nan_never_to_a_matrix(self):
        """`_cell_extents` relies on a cell column coming back all-NaN — the
        signal that it must look inside the cells. Same-length arrays must not
        be stacked into a 2-D block on the way."""
        cells = pd.Series([np.array([1.0, 2.0]), np.array([3.0, 4.0])], dtype=object)
        out = coerce_numeric(cells)
        assert out.isna().all()
        assert len(out) == 2

    def test_mixed_scalars_and_arrays_keep_the_scalars(self):
        mixed = pd.Series([np.float64(2.0), np.array([1.0, 2.0])], dtype=object)
        out = coerce_numeric(mixed)
        np.testing.assert_array_equal(out.to_numpy(), [2.0, np.nan])

    def test_booleans_go_the_slow_path_not_the_float_cast(self):
        out = coerce_numeric(pd.Series([True, False], dtype=object))
        np.testing.assert_array_equal(out.to_numpy(), [1.0, 0.0])


def _object_column(cells):
    out = np.empty(len(cells), dtype=object)
    for i, c in enumerate(cells):
        out[i] = c
    return out


class TestExplodeFromNdarrayCells:
    """The exploded frame is the same whether the cells were lists or arrays."""

    @pytest.fixture
    def pair(self):
        lists = [[1.0, float("nan"), 3.0], [4.0, 5.0], [float("nan")] * 2, [7.0]]
        subjects = ["a", "a", "b", "b"]
        as_lists = pd.DataFrame({"subject": subjects, "m": _object_column(lists)})
        as_arrays = pd.DataFrame(
            {"subject": subjects, "m": _object_column([np.asarray(v) for v in lists])}
        )
        return as_lists, as_arrays

    def test_frames_agree(self, pair):
        as_lists, as_arrays = pair
        a, ia = _explode_1d(as_lists, "m", "index")
        b, ib = _explode_1d(as_arrays, "m", "index")
        assert ia == ib == "index"
        pd.testing.assert_frame_equal(
            a.reset_index(drop=True), b.reset_index(drop=True), check_dtype=False
        )

    def test_nan_samples_are_dropped_and_positions_kept(self, pair):
        _, as_arrays = pair
        exploded, _ = _explode_1d(as_arrays, "m", "index")
        assert exploded["m"].notna().all()
        # s1 row 0 loses position 1; the gap is preserved, not renumbered.
        first = exploded[exploded["subject"] == "a"]["index"].tolist()
        assert first == [0, 2, 0, 1]
        assert exploded["m"].dtype == np.float64
