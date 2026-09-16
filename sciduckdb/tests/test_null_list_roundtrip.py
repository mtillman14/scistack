"""NULL list elements must come back as NaN, never as 0.

DuckDB returns a LIST that holds NULL elements as a ``numpy.ma.MaskedArray``.
``np.asarray`` on a masked array drops the mask and returns DuckDB's fill
buffer — zeros — which is how a NaN saved from MATLAB reached the next
function as ``0`` (``GAITRiteLoaded.L_StepLengths_GR``, 2026-09-15; plan
``.claude/plan-null-list-elements-to-nan.md``). Every load path funnels
through ``_storage_to_python``, so that is where the mask is honoured.

Contract pinned here: NULL in storage ≡ NaN in Python. Integer/bool array
columns that hold a NULL upcast to float64, the only dtype that can carry it.
"""

import numpy as np
import pandas as pd
import pytest

from sciduckdb import SciDuck
from sciduckdb.sciduckdb import (
    _storage_to_python,
    count_nan_array_elements,
    count_null_list_elements,
)


@pytest.fixture
def duck():
    db = SciDuck(":memory:", dataset_schema=["subject", "trial"])
    yield db
    db.close()


# ---------------------------------------------------------------------------
# The deserialiser itself (the one owner)
# ---------------------------------------------------------------------------


class TestStorageToPython:
    def test_masked_float_array_restores_nan_not_zero(self):
        masked = np.ma.MaskedArray([0.0, 0.5, 0.4], mask=[True, False, False])
        out = _storage_to_python(masked, {"python_type": "ndarray", "numpy_dtype": "float64"})
        assert not isinstance(out, np.ma.MaskedArray)
        assert np.isnan(out[0]), out
        np.testing.assert_array_equal(out[1:], [0.5, 0.4])

    def test_masked_int_array_upcasts_to_float64(self):
        masked = np.ma.MaskedArray([1, 0, 3], mask=[False, True, False])
        out = _storage_to_python(masked, {"python_type": "ndarray", "numpy_dtype": "int64"})
        assert out.dtype == np.float64
        assert np.isnan(out[1])
        assert out[0] == 1 and out[2] == 3

    def test_unmasked_int_array_keeps_dtype(self):
        out = _storage_to_python(
            np.array([1, 2, 3]), {"python_type": "ndarray", "numpy_dtype": "int64"}
        )
        assert out.dtype == np.int64

    def test_restoration_is_idempotent(self):
        """SciDuck.load restores a cell twice (_restore_types, then the
        multi_column branch). The second pass sees a plain float array whose
        declared dtype is still int64 — casting NaN to int64 yields INT_MIN, so
        the upcast must survive re-restoration."""
        meta = {"python_type": "ndarray", "numpy_dtype": "int64"}
        once = _storage_to_python(
            np.ma.MaskedArray([1, 0, 3], mask=[False, True, False]), meta
        )
        twice = _storage_to_python(once, meta)
        assert twice.dtype == np.float64
        assert np.isnan(twice[1])
        assert twice[0] == 1 and twice[2] == 3

    def test_nan_is_never_cast_to_an_integer(self):
        out = _storage_to_python(
            np.array([1.0, np.nan]), {"python_type": "ndarray", "numpy_dtype": "int64"}
        )
        assert out.dtype == np.float64
        assert np.isnan(out[1])

    def test_2d_without_nan_takes_the_original_path(self):
        """Matrix shape through this branch is load-bearing, so NaN-free data
        must come out exactly as it did before the NaN restoration existed."""
        rows = np.empty(4, dtype=object)
        for i in range(4):
            rows[i] = np.array([3 * i, 3 * i + 1, 3 * i + 2])
        out = _storage_to_python(
            rows, {"python_type": "ndarray", "numpy_dtype": "int64", "ndim": 2}
        )
        assert out.shape == (4, 3), out
        assert out.dtype == np.int64
        np.testing.assert_array_equal(out[3], [9, 10, 11])

    def test_masked_2d_rows(self):
        rows = np.empty(2, dtype=object)
        rows[0] = np.ma.MaskedArray([1.0, 0.0], mask=[False, True])
        rows[1] = np.array([3.0, 4.0])
        out = _storage_to_python(
            rows, {"python_type": "ndarray", "numpy_dtype": "float64", "ndim": 2}
        )
        assert out.shape == (2, 2)
        assert np.isnan(out[0, 1])
        assert out[1, 1] == 4.0

    def test_list_of_ndarray_column(self):
        cells = [np.ma.MaskedArray([0.0, 2.0], mask=[True, False]), np.array([5.0])]
        out = _storage_to_python(
            cells,
            {"python_type": "list", "contains_ndarray": True, "ndarray_dtype": "float64"},
        )
        assert np.isnan(out[0][0]) and out[0][1] == 2.0
        assert out[1][0] == 5.0


class TestCounters:
    def test_count_null_list_elements(self):
        cells = pd.Series(
            [
                np.ma.MaskedArray([0.0, 1.0], mask=[True, False]),
                np.array([1.0, 2.0]),
                None,
            ],
            dtype=object,
        )
        assert count_null_list_elements(cells) == 1

    def test_count_nan_array_elements(self):
        cells = np.array(
            [np.array([np.nan, 1.0]), [np.nan, np.nan], np.array([1, 2]), "x"],
            dtype=object,
        )
        assert count_nan_array_elements(cells) == 3


# ---------------------------------------------------------------------------
# Through DuckDB: save → load
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_dict_float_array_with_nan(self, duck):
        duck.save(
            "arr", {"force": np.array([np.nan, 0.5, 0.4, np.nan])}, subject="S01", trial="1"
        )
        loaded = duck.load("arr", subject="S01", trial="1")
        f = loaded["force"]
        assert f.dtype == np.float64
        assert np.isnan(f[0]) and np.isnan(f[3]), f
        assert not np.any(f == 0.0), f
        np.testing.assert_array_equal(f[1:3], [0.5, 0.4])

    def test_dataframe_array_cell_column_with_nan(self, duck):
        """One DuckDB row per DataFrame row, an array in each cell — the
        GAITRiteLoaded shape."""
        original = pd.DataFrame(
            {
                "steps": [np.array([np.nan, 0.49, 0.45]), np.array([0.51, np.nan])],
                "label": ["a", "b"],
            }
        )
        duck.save("gr", original, subject="S01", trial="1")
        loaded = duck.load("gr", subject="S01", trial="1")
        assert isinstance(loaded, pd.DataFrame)
        s0, s1 = loaded["steps"].iloc[0], loaded["steps"].iloc[1]
        assert np.isnan(s0[0]) and s0[1] == 0.49 and s0[2] == 0.45, s0
        assert s1[0] == 0.51 and np.isnan(s1[1]), s1

    def test_2d_array_with_nan(self, duck):
        m = np.array([[1.0, np.nan], [np.nan, 4.0]])
        duck.save("mat", {"m": m}, subject="S01", trial="1")
        out = duck.load("mat", subject="S01", trial="1")["m"]
        assert out.shape == (2, 2)
        assert np.isnan(out[0, 1]) and np.isnan(out[1, 0])
        assert out[0, 0] == 1.0 and out[1, 1] == 4.0

    def test_int_array_with_sql_null_upcasts(self, duck):
        """A NULL written by SQL into a BIGINT[] column — no in-memory NaN
        could have produced it — still reloads as NaN, in float64."""
        duck.save("ints", {"ids": np.array([1, 2, 3], dtype=np.int64)}, subject="S01", trial="1")
        duck._execute('UPDATE "ints" SET ids = [1, NULL, 3]')
        out = duck.load("ints", subject="S01", trial="1")["ids"]
        assert out.dtype == np.float64
        assert out[0] == 1 and np.isnan(out[1]) and out[2] == 3

    def test_nan_free_arrays_untouched(self, duck):
        duck.save("clean", {"v": np.array([0.0, 1.0, 2.0])}, subject="S01", trial="1")
        out = duck.load("clean", subject="S01", trial="1")["v"]
        np.testing.assert_array_equal(out, [0.0, 1.0, 2.0])
        assert not np.any(np.isnan(out))
