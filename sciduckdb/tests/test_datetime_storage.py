"""Datetime storage: ``<col>`` TIMESTAMP_NS (UTC) + ``<col>__tz`` zone label.

Owner: ``sciduckdb.datetimes`` (design: docs/claude/datetime-roundtrip.md).
NULL zone = naive; a NaT is NULL in both columns; zones are per value; the
companion exists from a datetime column's first save; ``dtype_meta["columns"]``
never lists it.
"""

import datetime

import numpy as np
import pandas as pd
import pytest

from sciduckdb import (
    DatetimeStorageError,
    SciDuck,
    column_type_change,
    infer_data_columns,
    merge_tz_columns,
    storage_column_names,
    stored_tz_companions,
    tz_column_name,
)
from sciduckdb.datetimes import (
    datetime_from_storage,
    datetime_to_storage,
    restore_datetime_series,
)
from sciduckdb.sciduckdb import _storage_signature, value_to_storage_row


@pytest.fixture
def duck():
    db = SciDuck(":memory:", dataset_schema=["subject"])
    yield db
    db.close()


class TestTyping:
    @pytest.mark.parametrize(
        "value",
        [
            pd.Timestamp("2026-01-01"),
            pd.NaT,
            datetime.datetime(2026, 1, 1),
            np.datetime64("2026-01-01"),
            np.datetime64("NaT"),
            pd.Timestamp("2026-01-01", tz="Europe/Berlin"),
        ],
        ids=["Timestamp", "NaT", "datetime", "datetime64", "datetime64-NaT", "aware"],
    )
    def test_scalar_gets_instant_and_companion(self, value):
        types, meta = infer_data_columns(value)
        assert types == {"value": "TIMESTAMP_NS", "value__tz": "VARCHAR"}
        assert meta["columns"] == {"value": {"python_type": "datetime"}}

    def test_dataframe_columns_in_storage_order(self):
        frame = pd.DataFrame({"a": [1.0], "t": [pd.Timestamp("2026-01-01")], "b": ["x"]})
        types, meta = infer_data_columns(frame)
        assert list(types) == ["a", "t", "t__tz", "b"]
        assert list(meta["columns"]) == ["a", "t", "b"]
        assert storage_column_names(meta) == ["a", "t", "t__tz", "b"]

    def test_all_nat_columns_are_datetime(self):
        for col in (pd.to_datetime([None, None]), pd.Series([pd.NaT, pd.NaT], dtype=object)):
            types, meta = infer_data_columns(pd.DataFrame({"t": col}))
            assert types == {"t": "TIMESTAMP_NS", "t__tz": "VARCHAR"}
            assert meta["columns"]["t"]["python_type"] == "datetime"

    @pytest.mark.parametrize("unit", ["ns", "us", "s"])
    def test_datetime64_column_records_its_unit(self, unit):
        col = pd.Series(pd.to_datetime(["2026-01-01"])).dt.as_unit(unit)
        for c in (col, col.dt.tz_localize("UTC")):
            _, meta = infer_data_columns(pd.DataFrame({"t": c}))
            assert meta["columns"]["t"] == {"python_type": "datetime", "unit": unit}

    def test_dict_record(self):
        types, meta = infer_data_columns({"start": pd.Timestamp("2026-01-01"), "n": 1})
        assert types == {"start": "TIMESTAMP_NS", "start__tz": "VARCHAR", "n": "BIGINT"}

    def test_collision_refused(self):
        frame = pd.DataFrame({"t": [pd.Timestamp("2026-01-01")], "t__tz": ["x"]})
        with pytest.raises(DatetimeStorageError):
            infer_data_columns(frame)

    def test_temporal_types_are_not_interchangeable(self):
        assert column_type_change("TIMESTAMP", "TIMESTAMP_NS") == "refuse"
        assert column_type_change("TIMESTAMP_NS", "INTERVAL") == "refuse"
        assert column_type_change("TIMESTAMP_NS", "TIMESTAMP_NS") == "same"
        assert _storage_signature("TIMESTAMP_NS") != _storage_signature("TIMESTAMP")


class TestValues:
    def test_naive_is_wall_clock_no_zone(self):
        assert datetime_to_storage(pd.Timestamp("2026-07-01 12:00:00.000000005")) == (
            "2026-07-01 12:00:00.000000005", None,
        )

    def test_aware_is_utc_plus_label(self):
        berlin = pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin")
        assert datetime_to_storage(berlin) == ("2026-07-01 12:00:00.000000000", "Europe/Berlin")

    def test_fixed_offset(self):
        v = datetime.datetime(2026, 1, 1, 2, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        assert datetime_to_storage(v) == ("2026-01-01 00:00:00.000000000", "+02:00")

    @pytest.mark.parametrize("missing", [None, pd.NaT, np.datetime64("NaT"), float("nan")])
    def test_missing(self, missing):
        assert datetime_to_storage(missing) == (None, None)

    def test_before_epoch(self):
        assert datetime_to_storage(datetime.datetime(1900, 1, 1))[0] == "1900-01-01 00:00:00.000000000"

    def test_out_of_range(self):
        with pytest.raises(DatetimeStorageError, match="range"):
            datetime_to_storage(datetime.datetime(2300, 1, 1))

    def test_not_a_datetime(self):
        with pytest.raises(DatetimeStorageError, match="float"):
            datetime_to_storage(1.5)

    def test_from_storage(self):
        assert datetime_from_storage(None, None) is pd.NaT
        assert datetime_from_storage(pd.Timestamp("2026-07-01 12:00"), None) == pd.Timestamp("2026-07-01 12:00")
        back = datetime_from_storage(pd.Timestamp("2026-07-01 12:00"), "Europe/Berlin")
        assert back == pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin")
        assert str(back.tzinfo) == "Europe/Berlin"

    def test_value_row_has_both_columns(self):
        _, meta = infer_data_columns({"t": pd.Timestamp("2026-01-01", tz="UTC"), "n": 2})
        assert value_to_storage_row(
            {"t": pd.Timestamp("2026-01-01", tz="UTC"), "n": 2}, meta
        ) == ["2026-01-01 00:00:00.000000000", "UTC", 2]


class TestRestoreSeries:
    def _instants(self, *texts):
        # As a TIMESTAMP_NS fetch returns them: ns, whatever pandas defaults to.
        return pd.Series(pd.to_datetime(list(texts))).astype("datetime64[ns]")

    def test_no_labels_is_naive(self):
        out = restore_datetime_series(self._instants("2026-01-01", None), [None, None])
        assert str(out.dtype) == "datetime64[ns]"

    @pytest.mark.parametrize("labels", [[None, None], ["Asia/Tokyo", None]])
    def test_saved_unit_restored(self, labels):
        out = restore_datetime_series(self._instants("2026-01-01", None), labels, unit="us")
        assert out.dt.unit == "us"

    def test_one_zone_is_tz_dtype(self):
        out = restore_datetime_series(self._instants("2026-01-01", None), ["Asia/Tokyo", None])
        assert str(out.dt.tz) == "Asia/Tokyo"
        assert out.isna().tolist() == [False, True]

    def test_mixed_is_object(self):
        out = restore_datetime_series(
            self._instants("2026-01-01", "2026-01-01", "2026-01-01"),
            ["Asia/Tokyo", "UTC", None],
        )
        assert out.dtype == object
        assert [str(getattr(v, "tzinfo", None)) for v in out] == ["Asia/Tokyo", "UTC", "None"]

    def test_merge_drops_companion(self):
        df = pd.DataFrame({
            "t": pd.to_datetime(["2026-01-01 00:00"]),
            "t__tz": ["Europe/Berlin"],
            "x": [1.0],
        })
        meta = {"columns": {"t": {"python_type": "datetime"}, "x": {"python_type": "float"}}}
        merge_tz_columns(df, meta)
        assert list(df.columns) == ["t", "x"]
        assert df["t"].iloc[0] == pd.Timestamp("2026-01-01 01:00", tz="Europe/Berlin")

    def test_missing_companion_loads_naive_with_warning(self, caplog):
        df = pd.DataFrame({"t": pd.to_datetime(["2026-01-01"])})
        with caplog.at_level("WARNING"):
            merge_tz_columns(df, {"columns": {"t": {"python_type": "datetime"}}})
        assert df["t"].dt.tz is None
        assert any("t__tz" in r.message for r in caplog.records)


class TestStoredSchema:
    def test_companion_recognised_from_types(self):
        assert stored_tz_companions(
            {"t": "TIMESTAMP_NS", "t__tz": "VARCHAR", "site__tz": "VARCHAR", "x": "DOUBLE"}
        ) == {"t__tz": "t"}

    def test_name(self):
        assert tz_column_name("t") == "t__tz"


class TestSciDuckRoundTrip:
    def test_scalar(self, duck):
        value = pd.Timestamp("2026-07-01 14:00:00.000000001", tz="Europe/Berlin")
        duck.save("when", value, subject="S01")
        loaded = duck.load("when", subject="S01")
        assert loaded == value and str(loaded.tzinfo) == "Europe/Berlin"
        assert loaded.nanosecond == 1

    def test_nat_scalar(self, duck):
        duck.save("when", pd.NaT, subject="S01")
        assert duck.load("when", subject="S01") is pd.NaT

    def test_dataframe_mixed(self, duck):
        values = [
            pd.Timestamp("2026-01-01", tz="Asia/Tokyo"),
            pd.Timestamp("2026-01-01"),
            pd.NaT,
        ]
        duck.save("frame", pd.DataFrame({"t": pd.Series(values, dtype=object)}), subject="S01")
        loaded = duck.load("frame", subject="S01")["t"].tolist()
        assert str(loaded[0].tzinfo) == "Asia/Tokyo" and loaded[0] == values[0]
        assert loaded[1] == values[1] and loaded[1].tzinfo is None
        assert loaded[2] is pd.NaT
