"""MATLAB datetimes through the bridge (docs/claude/datetime-roundtrip.md).

MATLAB sends a datetime array as int64 ns from 1970-01-01 (convertTo
'epochtime', 1e9 ticks/s: wall clock when unzoned, else the UTC instant) +
a NaT mask + its ONE TimeZone; ``datetimes_from_matlab`` builds the pandas
values. The way back, ``datetimes_to_matlab``, hands over the same three
(ticks as raw little-endian bytes: MATLAB's from_python makes numbers double,
which cannot hold ~1.8e18 exactly).

Runs entirely in Python without MATLAB: the arrays are what
+scidb/+internal/datetime_to_python.m sends, and ``_matlab_rebuild``
reproduces what datetime_from_python.m does with the reply.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import datetime

import numpy as np
import pandas as pd
import pytest
from scicanonicalhash import canonical_hash
from scidb.database import configure_database
from scimatlab.bridge import (
    datetimes_from_matlab,
    datetimes_to_matlab,
    is_datetime_value,
    matlab_zone_label,
    numpy_scalar_to_python,
    register_matlab_variable,
    save_batch_bridge,
)

SCHEMA = ["subject"]


@pytest.fixture
def db(tmp_path):
    import scifor as _scifor

    _scifor.set_schema([])
    db = configure_database(tmp_path / "dt.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


def _ticks(*texts, tz=None):
    """What MATLAB's convertTo('epochtime', 1e9) sends for these values."""
    ts = pd.to_datetime(list(texts))
    if tz:
        ts = ts.tz_localize(tz).tz_convert("UTC").tz_localize(None)
    return ts.astype("datetime64[ns]").to_numpy().view(np.int64)


def _matlab_rebuild(desc):
    """datetime_from_python.m: typecast(ns_bytes) + NaT mask + TimeZone."""
    ns = np.frombuffer(desc["ns_bytes"], dtype="<i8")
    assert (ns == desc["ns"]).all()
    return ns, desc["nat"], desc["tz"]


class TestZoneLabel:
    @pytest.mark.parametrize(
        "tz,label",
        [
            ("", None),
            ("UTC", "UTC"),
            ("Z", "UTC"),
            ("UTCLeapSeconds", "UTC"),
            ("Europe/Berlin", "Europe/Berlin"),
            ("+02:00", "+02:00"),
            ("+0200", "+02:00"),
            ("-05", "-05:00"),
            ("+00:00", "UTC"),
        ],
    )
    def test_matlab_spellings(self, tz, label):
        assert matlab_zone_label(tz) == label

    def test_unknown_zone_refused(self):
        with pytest.raises(ValueError, match="Mars/Olympus"):
            matlab_zone_label("Mars/Olympus")


class TestFromMatlab:
    def test_unzoned_scalar_is_naive_wall_clock(self):
        v = datetimes_from_matlab(_ticks("2026-07-01 12:00:00.000000007"), [False], "", "scalar")
        assert v == pd.Timestamp("2026-07-01 12:00:00.000000007")
        assert v.tzinfo is None and v.nanosecond == 7

    def test_zoned_scalar_keeps_its_zone(self):
        ticks = _ticks("2026-07-01 14:00", tz="Europe/Berlin")
        v = datetimes_from_matlab(ticks, [False], "Europe/Berlin", "scalar")
        assert v == pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin")
        assert str(v.tzinfo) == "Europe/Berlin"

    def test_nat_scalar(self):
        assert datetimes_from_matlab(np.zeros(1, np.int64), [True], "", "scalar") is pd.NaT
        assert datetimes_from_matlab(np.zeros(1, np.int64), [True], "UTC", "scalar") is pd.NaT

    def test_column_is_a_datetime_series(self):
        s = datetimes_from_matlab(
            _ticks("2026-01-01", "2026-01-02", tz="Asia/Tokyo"), [False, True], "Asia/Tokyo", "column"
        )
        assert isinstance(s, pd.Series)
        assert str(s.dt.tz) == "Asia/Tokyo"
        assert s.isna().tolist() == [False, True]

    def test_list_is_one_value_per_record(self):
        vals = datetimes_from_matlab(_ticks("2026-01-01", "2026-01-02"), [False, False], "", "list")
        assert vals == [pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02")]

    def test_scalar_needs_one_value(self):
        with pytest.raises(ValueError):
            datetimes_from_matlab(_ticks("2026-01-01", "2026-01-02"), [False, False], "", "scalar")

    def test_hash_matches_the_python_value(self):
        """A MATLAB save and a Python save of one value are one record."""
        berlin = pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin")
        from_matlab = datetimes_from_matlab(
            _ticks("2026-07-01 14:00", tz="Europe/Berlin"), [False], "Europe/Berlin", "scalar"
        )
        assert canonical_hash(from_matlab) == canonical_hash(berlin)


class TestToMatlab:
    def test_scalar(self):
        desc = datetimes_to_matlab(pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin"))
        assert desc["ok"] and desc["scalar"] and desc["tz"] == "Europe/Berlin"
        ns, nat, _ = _matlab_rebuild(desc)
        assert ns.tolist() == _ticks("2026-07-01 12:00").tolist()
        assert nat.tolist() == [False]

    def test_naive_and_nat(self):
        for v in (pd.NaT, np.datetime64("NaT")):
            desc = datetimes_to_matlab(v)
            assert desc["ok"] and desc["scalar"] and desc["nat"].tolist() == [True] and desc["tz"] == ""
        desc = datetimes_to_matlab(datetime.datetime(2026, 1, 1, 0, 0, 0, 5))
        assert desc["tz"] == "" and desc["ns"].tolist() == [_ticks("2026-01-01")[0] + 5000]

    def test_datetime64_scalar(self):
        desc = datetimes_to_matlab(np.datetime64("2026-01-01T00:00:00.000000001"))
        assert desc["ok"] and desc["ns"].tolist() == [_ticks("2026-01-01")[0] + 1]

    @pytest.mark.parametrize("unit", ["ns", "us", "s"])
    def test_series_any_unit(self, unit):
        s = pd.Series(pd.to_datetime(["2026-01-01 10:00", None])).dt.as_unit(unit)
        desc = datetimes_to_matlab(s.dt.tz_localize(datetime.timezone(datetime.timedelta(hours=2))))
        assert desc["ok"] and not desc["scalar"] and desc["tz"] == "+02:00"
        assert desc["nat"].tolist() == [False, True]
        assert desc["ns"][0] == _ticks("2026-01-01 08:00")[0]

    def test_all_nat_aware_series_keeps_zone(self):
        s = pd.Series(pd.to_datetime([None, None])).dt.tz_localize("Europe/Berlin")
        assert datetimes_to_matlab(s)["tz"] == "Europe/Berlin"

    def test_mixed_zones_refused_with_reason(self):
        mixed = pd.Series(
            [pd.Timestamp("2026-01-01", tz="UTC"), pd.Timestamp("2026-01-01")], dtype=object
        )
        desc = datetimes_to_matlab(mixed)
        assert not desc["ok"] and "zones" in desc["reason"]

    def test_round_trip_through_matlab_shape(self):
        """to_matlab -> (MATLAB rebuild) -> from_matlab gives the value back."""
        for v in (
            pd.Timestamp("2026-07-01 14:00:00.123456789", tz="America/New_York"),
            pd.Timestamp("1900-01-01 00:00:00.000000001"),
            pd.NaT,
        ):
            ns, nat, tz = _matlab_rebuild(datetimes_to_matlab(v))
            back = datetimes_from_matlab(ns, nat, tz, "scalar")
            assert (back is pd.NaT) if v is pd.NaT else (back == v and str(back.tzinfo) == str(v.tzinfo))


class TestDispatchHelpers:
    def test_is_datetime_value(self):
        assert is_datetime_value(pd.Timestamp("2026-01-01"))
        assert is_datetime_value(pd.NaT)
        assert is_datetime_value(np.datetime64("2026-01-01"))
        assert not is_datetime_value(1.5)

    def test_numpy_scalar_never_unwraps_datetime64(self):
        """.item() of a ns datetime64 is a bare int (MATLAB: a double)."""
        assert numpy_scalar_to_python(np.datetime64("2026-01-01T00:00:00.000000001")) == [False, None]
        assert numpy_scalar_to_python(np.int64(3)) == [True, 3]


class TestSaveFromTable:
    def test_datetime_data_column_saves_one_record_per_row(self, db):
        """BaseVariable.save_from_table's datetime data column: the list
        datetime_to_python(..., 'list') returns, unchanged by the bridge."""
        cls = register_matlab_variable("MatlabWhen")
        values = datetimes_from_matlab(
            _ticks("2026-07-01 14:00", "2026-07-02 14:00", tz="Europe/Berlin"),
            [False, True], "Europe/Berlin", "list",
        )
        rids = save_batch_bridge(
            "MatlabWhen", values, ["subject"], [np.array([1.0, 2.0])], {}, None, None
        ).split("\n")
        assert len(rids) == 2 and all(rids)
        first = cls.load(subject=1).data
        assert first == pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin")
        assert str(first.tzinfo) == "Europe/Berlin"
        assert cls.load(subject=2).data is pd.NaT
        assert db.stored_data_column_types(cls.table_name()) == {
            "value": "TIMESTAMP_NS", "value__tz": "VARCHAR",
        }
