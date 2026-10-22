"""Datetimes round-trip: naive, tz-aware (per value), NaT.

Regression for 2026-10-22: ``When.save(pd.Timestamp(...), subject="S01")``
raised ``ValueError: Unserializable data type: <class 'pandas.Timestamp'>``
from the record_id hash; no datetime scalar could be saved at all.

Agreed design (.claude/plan-datetime-roundtrip.md, docs/claude/
datetime-roundtrip.md):
- D1 datetime.datetime / pd.Timestamp / np.datetime64 holding one value hash
  the same; NaT has one spelling (owner scicanonicalhash.datetimes).
- D3/D4 every datetime column is stored as ``<col>`` TIMESTAMP_NS (UTC
  instant, or the wall clock of a naive value) + ``<col>__tz`` VARCHAR (the
  value's zone label, NULL = naive), the companion existing from the first
  save; zones are per value; naive and aware may mix (owner
  sciduckdb.datetimes).
- D5 TIMESTAMP_NS, not TIMESTAMPTZ: nanoseconds survive.
"""

import datetime
import zoneinfo

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, ColumnTypeChangedError, configure_database
from scicanonicalhash import canonical_hash
from sciduckdb import DatetimeStorageError, tz_column_name

SCHEMA = ["subject"]


class When(BaseVariable):
    pass


class WhenFrame(BaseVariable):
    pass


class WhenDict(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "datetimes.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


def _load_one(cls, subject):
    rec = cls.load(subject=subject)  # one match -> one BaseVariable
    return rec.data


def _stored(db, cls):
    return db.stored_data_column_types(cls.table_name())


def _same_value(a, b):
    """Equal as values AND in zone (a Timestamp compares instants only)."""
    if a is pd.NaT or b is pd.NaT:
        return a is b
    return a == b and str(getattr(a, "tzinfo", None)) == str(getattr(b, "tzinfo", None))


# ---------------------------------------------------------------------------
# Scalar per schema level (the user's case)
# ---------------------------------------------------------------------------

SCALARS = {
    "S01": pd.Timestamp("2026-01-01 10:00:00.123456789"),
    "S02": pd.NaT,
    "S03": datetime.datetime(2026, 1, 3, 12, 0),
    "S04": np.datetime64("2026-01-04T08:30"),
    "S05": pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin"),
    "S06": pd.Timestamp("2026-07-01 08:00", tz="America/New_York"),
    "S07": datetime.datetime(2026, 1, 7, tzinfo=datetime.timezone(datetime.timedelta(hours=2))),
    "S08": pd.Timestamp("2026-01-08", tz="UTC"),
    "S09": np.datetime64("NaT"),
}


@pytest.mark.parametrize("subject", list(SCALARS))
def test_scalar_round_trip_one_at_a_time(db, subject):
    value = SCALARS[subject]
    When.save(value, subject=subject)
    loaded = _load_one(When, subject)
    expected = pd.NaT if pd.isna(value) else pd.Timestamp(value)
    assert _same_value(loaded, expected), (value, loaded)
    assert canonical_hash(loaded) == canonical_hash(value)


def test_scalars_every_level_one_batch(db):
    """One save of every level: a NaT level among valid ones, naive and
    aware values in several zones, one variable."""
    db.save_batch(When, [(v, {"subject": s}) for s, v in SCALARS.items()])
    for subject, value in SCALARS.items():
        loaded = _load_one(When, subject)
        expected = pd.NaT if pd.isna(value) else pd.Timestamp(value)
        assert _same_value(loaded, expected), (subject, value, loaded)


def test_nat_only_variable(db):
    When.save(pd.NaT, subject="S01")
    assert _load_one(When, "S01") is pd.NaT
    assert _stored(db, When) == {"value": "TIMESTAMP_NS", "value__tz": "VARCHAR"}


def test_companion_exists_from_first_save_even_all_naive(db):
    When.save(pd.Timestamp("2026-01-01"), subject="S01")
    assert _stored(db, When) == {"value": "TIMESTAMP_NS", "value__tz": "VARCHAR"}
    # A later aware value fits the same table.
    When.save(pd.Timestamp("2026-01-02", tz="Europe/Berlin"), subject="S02")
    assert str(_load_one(When, "S02").tzinfo) == "Europe/Berlin"


def test_dtype_meta_never_lists_the_companion(db):
    When.save(pd.Timestamp("2026-01-01"), subject="S01")
    assert list(db.get_dtype_meta("When")["columns"]) == ["value"]


def test_stored_instant_is_utc_and_zone_is_kept(db):
    When.save(pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin"), subject="S01")
    rows = db._duck._fetchall(
        f'SELECT CAST("value" AS VARCHAR), "value__tz" FROM "{When.table_name()}"'
    )
    assert rows == [("2026-07-01 12:00:00", "Europe/Berlin")]


def test_nanoseconds_survive(db):
    value = pd.Timestamp("2026-01-01 00:00:00.000000007")
    When.save(value, subject="S01")
    loaded = _load_one(When, "S01")
    assert loaded.value == value.value
    assert loaded.nanosecond == 7


def test_resave_of_loaded_value_is_the_same_record(db):
    """Same hash after load -> same record_id -> "0 new rows"."""
    batch = [(v, {"subject": s}) for s, v in SCALARS.items()]
    first = db.save_batch(When, batch)
    reloaded = [(_load_one(When, s), {"subject": s}) for s in SCALARS]
    assert db.save_batch(When, reloaded) == first


def test_out_of_range_refused(db):
    with pytest.raises(DatetimeStorageError, match="outside the storable range"):
        When.save(datetime.datetime(3000, 1, 1), subject="S01")


def test_load_all_as_df_scalar(db):
    db.save_batch(When, [
        (pd.Timestamp("2026-01-01"), {"subject": "S01"}),
        (pd.NaT, {"subject": "S02"}),
        (pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin"), {"subject": "S03"}),
    ])
    df = db.load_all_as_df(When, layout="spread").set_index("subject")
    assert df.loc["S01", "When"] == pd.Timestamp("2026-01-01")
    assert df.loc["S02", "When"] is pd.NaT
    assert str(df.loc["S03", "When"].tzinfo) == "Europe/Berlin"
    assert "value__tz" not in df.columns and "When__tz" not in df.columns


# ---------------------------------------------------------------------------
# DataFrame columns
# ---------------------------------------------------------------------------


def test_naive_dataframe_column_with_nat(db):
    frame = pd.DataFrame({
        "t": pd.to_datetime(["2026-01-01 10:00", None, "2026-01-03 00:00"]),
        "x": [1.0, 2.0, 3.0],
    })
    WhenFrame.save(frame, subject="S01")
    loaded = _load_one(WhenFrame, "S01")
    assert list(loaded.columns) == ["t", "x"]
    assert loaded["t"].dtype == frame["t"].dtype  # unit restored (datetime_meta)
    pd.testing.assert_series_equal(loaded["t"], frame["t"])
    assert canonical_hash(loaded) == canonical_hash(frame)


@pytest.mark.parametrize("unit", ["ns", "us", "ms", "s"])
@pytest.mark.parametrize("tz", [None, "Europe/Berlin"])
def test_dataframe_column_unit_restored(db, unit, tz):
    """TIMESTAMP_NS loads as ns; a us column (pandas' default on some
    versions) must come back as us, or its hash (dtype included) changes and
    a re-save makes a new record."""
    t = pd.Series(pd.to_datetime(["2026-01-01 10:00:00", None])).dt.as_unit(unit)
    if tz:
        t = t.dt.tz_localize(tz)
    frame = pd.DataFrame({"t": t})
    first = db.save_batch(WhenFrame, [(frame, {"subject": "S01"})])
    loaded = _load_one(WhenFrame, "S01")
    assert loaded["t"].dtype == frame["t"].dtype
    assert canonical_hash(loaded) == canonical_hash(frame)
    assert db.save_batch(WhenFrame, [(loaded, {"subject": "S01"})]) == first


def test_aware_dataframe_column_one_zone(db):
    frame = pd.DataFrame({
        "t": pd.to_datetime(["2026-01-01 10:00", None]).tz_localize("Europe/Berlin"),
    })
    WhenFrame.save(frame, subject="S01")
    loaded = _load_one(WhenFrame, "S01")
    assert str(loaded["t"].dt.tz) == "Europe/Berlin"
    assert loaded["t"].iloc[0] == frame["t"].iloc[0]
    assert loaded["t"].iloc[1] is pd.NaT
    assert canonical_hash(loaded) == canonical_hash(frame)


def test_dataframe_column_mixing_zones_and_naive(db):
    """Per-value zones: one column holding two zones and a naive value."""
    values = [
        pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin"),
        pd.Timestamp("2026-07-01 08:00", tz="America/New_York"),
        pd.Timestamp("2026-07-01 12:00"),
        pd.NaT,
    ]
    frame = pd.DataFrame({"t": pd.Series(values, dtype=object)})
    WhenFrame.save(frame, subject="S01")
    loaded = _load_one(WhenFrame, "S01")["t"].tolist()
    for got, want in zip(loaded, values, strict=True):
        assert _same_value(got, want), (got, want)


def test_zones_differ_per_record(db):
    db.save_batch(WhenFrame, [
        (pd.DataFrame({"t": [pd.Timestamp("2026-01-01", tz="Europe/Berlin")]}), {"subject": "S01"}),
        (pd.DataFrame({"t": [pd.Timestamp("2026-01-01", tz="Asia/Tokyo")]}), {"subject": "S02"}),
        (pd.DataFrame({"t": [pd.Timestamp("2026-01-01")]}), {"subject": "S03"}),
    ])
    assert str(_load_one(WhenFrame, "S01")["t"].dt.tz) == "Europe/Berlin"
    assert str(_load_one(WhenFrame, "S02")["t"].dt.tz) == "Asia/Tokyo"
    assert _load_one(WhenFrame, "S03")["t"].dt.tz is None


def test_all_nat_datetime64_column_is_still_datetime(db):
    frame = pd.DataFrame({"t": pd.to_datetime([None, None])})
    WhenFrame.save(frame, subject="S01")
    assert _stored(db, WhenFrame) == {"t": "TIMESTAMP_NS", "t__tz": "VARCHAR"}
    WhenFrame.save(pd.DataFrame({"t": [pd.Timestamp("2026-01-01", tz="UTC")]}), subject="S02")
    assert str(_load_one(WhenFrame, "S02")["t"].dt.tz) == "UTC"


def test_empty_column_becomes_datetime_and_gains_its_zone_column(db):
    """A blank first save (undecided VARCHAR placeholder), datetimes later:
    the column turns TIMESTAMP_NS and the companion is added then."""
    WhenFrame.save(pd.DataFrame({"t": pd.Series([None], dtype=object)}), subject="S01")
    assert _stored(db, WhenFrame) == {"t": "VARCHAR"}
    WhenFrame.save(
        pd.DataFrame({"t": [pd.Timestamp("2026-01-02", tz="Europe/Berlin")]}), subject="S02"
    )
    assert _stored(db, WhenFrame) == {"t": "TIMESTAMP_NS", "t__tz": "VARCHAR"}
    assert str(_load_one(WhenFrame, "S02")["t"].dt.tz) == "Europe/Berlin"
    assert _load_one(WhenFrame, "S01")["t"].iloc[0] is pd.NaT


def test_numbers_into_a_datetime_column_refused(db):
    WhenFrame.save(pd.DataFrame({"t": [pd.Timestamp("2026-01-01")]}), subject="S01")
    with pytest.raises(ColumnTypeChangedError):
        WhenFrame.save(pd.DataFrame({"t": [1.5]}), subject="S02")


def test_companion_name_collision_refused(db):
    frame = pd.DataFrame({"t": [pd.Timestamp("2026-01-01")], "t__tz": ["mine"]})
    with pytest.raises(DatetimeStorageError, match="zone column"):
        WhenFrame.save(frame, subject="S01")


def test_a_plain_column_ending_in_tz_is_just_data(db):
    frame = pd.DataFrame({"site__tz": ["a", "b"], "x": [1.0, 2.0]})
    WhenFrame.save(frame, subject="S01")
    pd.testing.assert_frame_equal(_load_one(WhenFrame, "S01"), frame)


def test_load_all_as_df_dataframe_spread(db):
    db.save_batch(WhenFrame, [
        (pd.DataFrame({"t": [pd.Timestamp("2026-01-01", tz="Europe/Berlin")]}), {"subject": "S01"}),
        (pd.DataFrame({"t": [pd.NaT]}), {"subject": "S02"}),
    ])
    df = db.load_all_as_df(WhenFrame, layout="spread").set_index("subject")
    assert tz_column_name("t") not in df.columns
    assert df.loc["S01", "t"] == pd.Timestamp("2026-01-01", tz="Europe/Berlin")
    assert pd.isna(df.loc["S02", "t"])


# ---------------------------------------------------------------------------
# Dict (multi-column) records
# ---------------------------------------------------------------------------


def test_dict_record_with_datetime(db):
    value = {"start": pd.Timestamp("2026-01-01 09:00", tz="Europe/Berlin"), "n": 3}
    WhenDict.save(value, subject="S01")
    loaded = _load_one(WhenDict, "S01")
    assert _same_value(loaded["start"], value["start"])
    assert loaded["n"] == 3
    assert set(loaded) == {"start", "n"}


# ---------------------------------------------------------------------------
# Readers of the table schema
# ---------------------------------------------------------------------------


def test_inspector_lists_no_zone_column(db):
    from scidb.inspect import Inspector

    WhenFrame.save(pd.DataFrame({"t": [pd.Timestamp("2026-01-01")], "x": [1.0]}), subject="S01")
    detail = Inspector(db).variable("WhenFrame")
    assert "t__tz" not in detail.data_columns
    assert "t" in detail.data_columns


def test_zoneinfo_and_pytz_spellings_hash_alike(db):
    """A value saved with one tz library loads with zoneinfo; the label (and
    so the record) is the same."""
    berlin = datetime.datetime(2026, 7, 1, 14, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin"))
    When.save(berlin, subject="S01")
    loaded = _load_one(When, "S01")
    assert canonical_hash(loaded) == canonical_hash(berlin)
