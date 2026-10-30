"""The ``row_list`` column kind (sciduckdb.row_lists).

A row_list column holds one value per SOURCE ROW of a record whose table had
several rows at one schema location (``on_duplicate="combine"``). Contract:

- a one-element cell loads as the plain value, several as a list in row order;
- a scalar saved into a row_list column is stored as ``[v]`` (sticky kind);
- a stored scalar column converts in place, every old value loading unchanged;
- row_list and a genuine array column never share a column.

Plan: ``.claude/plan-combine-duplicate-locations.md``.
"""

import math

import duckdb
import numpy as np
import pandas as pd
import pytest

from sciduckdb import (
    RowListTypeError,
    convert_column_to_row_list,
    infer_data_columns,
    row_list_change,
    row_list_column_type,
    row_list_meta,
    storage_to_python,
    storage_to_python_column,
    unwrap_row_list,
)
from sciduckdb.sciduckdb import (
    _python_to_storage,
    bulk_df_to_storage_rows,
    record_schema_mismatch,
    value_to_storage_row,
)


class _Duck:
    """The two SciDuck calls convert_column_to_row_list makes, on a bare
    in-memory connection."""

    def __init__(self):
        self.con = duckdb.connect(":memory:")

    def _execute(self, sql, params=None):
        return self.con.execute(sql, params or [])

    def _fetchall(self, sql, params=None):
        return self.con.execute(sql, params or []).fetchall()


# ---------------------------------------------------------------------------
# Element typing
# ---------------------------------------------------------------------------


class TestColumnType:
    def test_ints_are_bigint(self):
        assert row_list_column_type([7, [5, 12]])[0] == "BIGINT[]"

    def test_float_anywhere_is_double(self):
        assert row_list_column_type([7, [5, 5.5]])[0] == "DOUBLE[]"

    def test_missing_among_ints_is_double(self):
        assert row_list_column_type([7, [5, np.nan]])[0] == "DOUBLE[]"

    def test_strings(self):
        ddb, meta = row_list_column_type(["A", ["A", "B"], None])
        assert ddb == "VARCHAR[]"
        assert meta == row_list_meta("VARCHAR")

    def test_bools(self):
        assert row_list_column_type([True, [True, False]])[0] == "BOOLEAN[]"

    def test_numpy_scalars(self):
        assert row_list_column_type([np.int64(1), [np.float64(2.5)]])[0] == "DOUBLE[]"

    def test_mixed_classes_refused(self):
        with pytest.raises(RowListTypeError):
            row_list_column_type([5, ["x", "y"]], "ward")

    def test_bool_and_number_refused(self):
        with pytest.raises(RowListTypeError):
            row_list_column_type([True, [1, 2]])

    def test_datetime_refused(self):
        with pytest.raises(RowListTypeError, match="datetime"):
            row_list_column_type([[pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02")]])


# ---------------------------------------------------------------------------
# Storage and load
# ---------------------------------------------------------------------------


class TestRoundTripValues:
    meta = row_list_meta("DOUBLE")

    def test_scalar_is_wrapped(self):
        assert _python_to_storage(7.0, self.meta) == [7.0]

    def test_list_kept_in_order(self):
        assert _python_to_storage([5, 12], self.meta) == [5.0, 12.0]

    def test_missing_element_is_null(self):
        assert _python_to_storage([5, np.nan], self.meta) == [5.0, None]

    def test_missing_scalar_is_null_cell(self):
        assert _python_to_storage(np.nan, self.meta) is None

    def test_one_element_loads_as_scalar(self):
        assert storage_to_python([7.0], self.meta) == 7.0

    def test_several_load_as_list(self):
        assert storage_to_python(np.array([5.0, 12.0]), self.meta) == [5.0, 12.0]

    def test_null_cell_loads_nan(self):
        assert math.isnan(storage_to_python(None, self.meta))

    def test_one_null_element_loads_nan(self):
        assert math.isnan(unwrap_row_list([None], self.meta))

    def test_masked_element_is_nan(self):
        masked = np.ma.MaskedArray([5.0, 0.0], mask=[False, True])
        out = unwrap_row_list(masked, self.meta)
        assert out[0] == 5.0 and math.isnan(out[1])

    def test_strings(self):
        meta = row_list_meta("VARCHAR")
        assert _python_to_storage(["A", "B"], meta) == ["A", "B"]
        assert unwrap_row_list(["A"], meta) == "A"
        assert unwrap_row_list(["A", None], meta) == ["A", None]
        assert unwrap_row_list(None, meta) is None

    def test_int_elements(self):
        meta = row_list_meta("BIGINT")
        assert unwrap_row_list([3], meta) == 3
        assert isinstance(unwrap_row_list([3], meta), int)

    def test_bool_elements(self):
        meta = row_list_meta("BOOLEAN")
        assert unwrap_row_list([True, False], meta) == [True, False]

    def test_value_to_storage_row_single_column(self):
        dtype_meta = {"mode": "single_column", "columns": {"value": self.meta}}
        assert value_to_storage_row(7.0, dtype_meta) == [[7.0]]

    def test_bulk_dataframe_rows(self):
        dtype_meta = {
            "mode": "dataframe",
            "columns": {"stay": self.meta, "age": {"python_type": "float"}},
            "df_columns": ["stay", "age"],
        }
        s01 = pd.DataFrame({"stay": pd.Series([[5.0, 12.0]], dtype=object), "age": [60.0]})
        s02 = pd.DataFrame({"stay": [7.0], "age": [45.0]})
        rows = bulk_df_to_storage_rows([s01, s02], ["r1", "r2"], dtype_meta)
        assert rows == [("r1", [5.0, 12.0], 60.0), ("r2", [7.0], 45.0)]

    def test_column_load_unwraps_per_cell(self):
        series = pd.Series([np.array([7.0]), np.array([5.0, 12.0]), None], name="stay")
        out = storage_to_python_column(series, self.meta)
        assert out.iloc[0] == 7.0
        assert out.iloc[1] == [5.0, 12.0]
        assert math.isnan(out.iloc[2])

    def test_genuine_one_element_array_stays_array(self):
        out = storage_to_python(
            np.array([7.0]), {"python_type": "ndarray", "numpy_dtype": "float64"}
        )
        assert isinstance(out, np.ndarray) and out.shape == (1,)


# ---------------------------------------------------------------------------
# infer_data_columns / validation
# ---------------------------------------------------------------------------


class TestInference:
    def test_dataframe_mode_row_list_column(self):
        s01 = pd.DataFrame({"stay": pd.Series([[5, 12]], dtype=object), "age": [60.0]})
        s02 = pd.DataFrame({"stay": [7], "age": [45.0]})
        types, meta = infer_data_columns(
            s01, more_frames=[s02], row_list_columns=["stay"]
        )
        assert types == {"stay": "BIGINT[]", "age": "DOUBLE"}
        assert meta["columns"]["stay"] == row_list_meta("BIGINT")

    def test_single_column_mode_any_name_marks_the_column(self):
        types, meta = infer_data_columns(
            7.0, row_list_columns=["stay_days"], more_values=[[5.0, 12.0]]
        )
        assert types == {"value": "DOUBLE[]"}
        assert meta["columns"]["value"] == row_list_meta("DOUBLE")

    def test_without_row_list_columns_unchanged(self):
        types, _ = infer_data_columns(7.0)
        assert types == {"value": "DOUBLE"}

    def test_validation_accepts_scalar_and_list(self):
        ref = {"value": "DOUBLE[]"}
        assert record_schema_mismatch(ref, {"value": "DOUBLE"}, row_list_columns=["value"]) is None
        assert record_schema_mismatch(ref, {"value": "DOUBLE[]"}, row_list_columns=["value"]) is None
        assert record_schema_mismatch(ref, {"value": "DOUBLE[][]"}, row_list_columns=["value"])


# ---------------------------------------------------------------------------
# What a save does to a stored column
# ---------------------------------------------------------------------------


class TestRowListChange:
    def test_neither_is_row_list(self):
        assert row_list_change({"python_type": "float"}, "DOUBLE", {"python_type": "float"}, "DOUBLE") is None

    def test_scalar_column_converts(self):
        assert row_list_change(
            {"python_type": "float"}, "DOUBLE", row_list_meta("DOUBLE"), "DOUBLE[]"
        ) == ("convert", "DOUBLE")

    def test_int_column_with_fractions_converts_to_double(self):
        assert row_list_change(
            {"python_type": "int"}, "BIGINT", row_list_meta("DOUBLE"), "DOUBLE[]"
        ) == ("convert", "DOUBLE")

    def test_scalars_into_row_list_are_same(self):
        assert row_list_change(
            row_list_meta("DOUBLE"), "DOUBLE[]", {"python_type": "int"}, "BIGINT"
        ) == ("same", "DOUBLE")

    def test_row_list_elements_widen(self):
        assert row_list_change(
            row_list_meta("BIGINT"), "BIGINT[]", row_list_meta("DOUBLE"), "DOUBLE[]"
        ) == ("alter", "DOUBLE")

    def test_text_column_takes_numbers_as_text(self):
        assert row_list_change(
            {"python_type": "str"}, "VARCHAR", row_list_meta("DOUBLE"), "DOUBLE[]"
        ) == ("convert", "VARCHAR")

    def test_numbers_column_refuses_text(self):
        assert row_list_change(
            {"python_type": "float"}, "DOUBLE", row_list_meta("VARCHAR"), "VARCHAR[]"
        )[0] == "refuse"

    def test_genuine_array_column_refused_both_ways(self):
        nd = {"python_type": "ndarray", "numpy_dtype": "float64", "ndim": 1}
        assert row_list_change(nd, "DOUBLE[]", row_list_meta("DOUBLE"), "DOUBLE[]")[0] == "refuse"
        assert row_list_change(row_list_meta("DOUBLE"), "DOUBLE[]", nd, "DOUBLE[]")[0] == "refuse"


class TestConvertInPlace:
    @pytest.mark.parametrize(
        "ddb_type, values, elem",
        [
            ("DOUBLE", [7.0, None, 1.5], "DOUBLE"),
            ("BIGINT", [7, None, 3], "BIGINT"),
            ("VARCHAR", ["A", None, "B"], "VARCHAR"),
            ("BOOLEAN", [True, None, False], "BOOLEAN"),
        ],
    )
    def test_every_old_value_loads_unchanged(self, ddb_type, values, elem):
        duck = _Duck()
        duck._execute(f'CREATE TABLE "T_data" (record_id VARCHAR PRIMARY KEY, "c" {ddb_type})')
        for i, v in enumerate(values):
            duck._execute('INSERT INTO "T_data" VALUES (?, ?)', [f"r{i}", v])
        n = convert_column_to_row_list(duck, "T_data", "c", elem)
        assert n == sum(v is not None for v in values)
        stored = duck._fetchall('SELECT "c" FROM "T_data" ORDER BY record_id')
        meta = row_list_meta(elem)
        for (cell,), original in zip(stored, values):
            loaded = unwrap_row_list(cell, meta)
            if original is None:
                assert loaded is None or (isinstance(loaded, float) and math.isnan(loaded))
            else:
                assert loaded == original

    def test_bigint_row_list_widens(self):
        duck = _Duck()
        duck._execute('CREATE TABLE "T_data" (record_id VARCHAR PRIMARY KEY, "c" BIGINT[])')
        duck._execute('INSERT INTO "T_data" VALUES (\'r\', [5, 12])')
        convert_column_to_row_list(duck, "T_data", "c", "DOUBLE", from_row_list=True)
        types = duck._fetchall(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'T_data' AND column_name = 'c'"
        )
        assert types[0][0] == "DOUBLE[]"

    def test_failed_transaction_rolls_back(self):
        duck = _Duck()
        duck._execute('CREATE TABLE "T_data" (record_id VARCHAR PRIMARY KEY, "c" DOUBLE)')
        duck._execute('INSERT INTO "T_data" VALUES (\'r\', 7.0)')
        duck._execute("BEGIN TRANSACTION")
        convert_column_to_row_list(duck, "T_data", "c", "DOUBLE")
        duck._execute("ROLLBACK")
        types = duck._fetchall(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'T_data' AND column_name = 'c'"
        )
        assert types[0][0] == "DOUBLE"
