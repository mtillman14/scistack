"""A date or datetime schema key round-trips idempotently.

A key may hold calendar days, exact timestamps, or both. A loaded value has its
original type, and saving it again finds the same address and the same record.
.claude/plan-distribute-replaces-combine.md Stage 6.
"""

import datetime
import zoneinfo

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.store.schema_values import from_datetime_key_text, schema_str
from sciduckdb import schema_str as sciduckdb_schema_str

CHICAGO = zoneinfo.ZoneInfo("America/Chicago")
VALUES = [
    datetime.date(2026, 7, 9),
    pd.Timestamp("2026-07-09 15:12:00"),
    pd.Timestamp("2026-07-09 15:12:00.000000001"),
    pd.Timestamp("2026-07-09 15:12:00", tz=CHICAGO),
    pd.Timestamp("2026-01-09 15:12:00", tz=CHICAGO),
    pd.Timestamp("2026-07-09 15:12:00", tz=datetime.timezone(datetime.timedelta(hours=2))),
]


class Measure(BaseVariable):
    pass


class Bout(BaseVariable):
    pass


class PerDate(BaseVariable):
    pass


class SourceTable(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "dates.duckdb", ["subject", "date"])
    yield database
    _scifor.set_schema([])
    database.close()


def _records(db):
    return db._duck.fetchall('SELECT COUNT(*) FROM "Measure_data"')[0][0]


def test_one_owner_of_the_spelling():
    assert schema_str is sciduckdb_schema_str


@pytest.mark.parametrize("value", VALUES, ids=str)
def test_text_and_value_round_trip(value):
    text = schema_str(value)
    restored = from_datetime_key_text(text)
    assert type(restored) is type(value)
    assert restored == value
    assert schema_str(restored) == text


@pytest.mark.parametrize("text", ["S01", "2026-13-45", "2026-07-09T25:00:00", "2026-07-09 15:12:00"])
def test_text_that_is_not_a_spelling_stays_text(text):
    assert from_datetime_key_text(text) == text


@pytest.mark.parametrize("value", VALUES, ids=str)
def test_a_loaded_key_has_its_type_and_saves_at_the_same_record(db, value):
    first = Measure.save(1.5, db=db, subject="A", date=value)
    loaded = Measure.load(db=db, subject="A")
    assert type(loaded.metadata["date"]) is type(value)
    assert loaded.metadata["date"] == value

    second = Measure.save(1.5, db=db, subject="A", date=loaded.metadata["date"])
    assert second == first
    assert _records(db) == 1
    assert Measure.load(db=db, subject="A", date=value).data == pytest.approx(1.5)


def test_every_type_holding_one_value_is_one_record(db):
    text = "2026-07-09T15:12:00"
    first = Measure.save(1.5, db=db, subject="A", date=pd.Timestamp(text))
    assert Measure.save(1.5, db=db, subject="A", date=np.datetime64(text)) == first
    assert Measure.save(1.5, db=db, subject="A", date=datetime.datetime(2026, 7, 9, 15, 12)) == first
    assert Measure.save(1.5, db=db, subject="A", date=text) == first
    assert _records(db) == 1


def test_a_day_and_midnight_are_two_addresses(db):
    Measure.save(1.0, db=db, subject="A", date=datetime.date(2026, 7, 9))
    Measure.save(2.0, db=db, subject="A", date=pd.Timestamp("2026-07-09"))
    assert _records(db) == 2
    assert Measure.load(db=db, subject="A", date=datetime.date(2026, 7, 9)).data == pytest.approx(1.0)
    assert Measure.load(db=db, subject="A", date=pd.Timestamp("2026-07-09")).data == pytest.approx(2.0)


def test_days_and_timestamps_may_mix_in_one_key(db):
    Measure.save(1.0, db=db, subject="A", date=datetime.date(2026, 7, 9))
    Measure.save(2.0, db=db, subject="A", date=pd.Timestamp("2026-07-10 08:00:00"))
    assert db.schema_key_loads_as("date") == "dates"
    frame = Measure.load(db=db, as_df=True, subject="A")
    assert {type(v) for v in frame["date"]} == {datetime.date, pd.Timestamp}


def test_one_text_value_makes_the_whole_key_text(db):
    """Per key, never per value: a column holds one kind."""
    Measure.save(1.0, db=db, subject="A", date=datetime.date(2026, 7, 9))
    Measure.save(2.0, db=db, subject="A", date="baseline")
    assert db.schema_key_loads_as("date") == "strings"
    assert isinstance(Measure.load(db=db, subject="A", date="2026-07-09").metadata["date"], str)


def test_for_each_files_a_column_of_timestamps_and_reads_it_back(tmp_path):
    db = configure_database(tmp_path / "bouts.duckdb", ["subject", "bout", "date"])
    try:
        SourceTable.save(np.arange(3.0), db=db, subject="A")
        stamps = [pd.Timestamp("2026-07-09 15:12", tz=CHICAGO), pd.Timestamp("2026-07-10", tz=CHICAGO)]

        def parse(x):
            return (
                pd.DataFrame({"bout": [1], "floor": [15]}),
                pd.DataFrame({"bout": [1, 1], "date": stamps, "score": [1.0, 2.0]}),
            )

        for_each(parse, {"x": SourceTable}, [Bout, PerDate], subject=["A"], db=db)
        records = PerDate.load(db=db, subject="A")
        assert {r.metadata["date"] for r in records} == set(stamps)

        before = db._duck.fetchall('SELECT COUNT(*) FROM "PerDate_data"')[0][0]
        for_each(parse, {"x": SourceTable}, [Bout, PerDate], subject=["A"], db=db)
        assert db._duck.fetchall('SELECT COUNT(*) FROM "PerDate_data"')[0][0] == before

        # A consumer iterating the date key finds each record.
        # A one-column table record reaches a function as {column: value}.
        seen = []

        def consume(y):
            seen.append(float(y["score"]))
            return 1.0

        class Consumed(BaseVariable):
            pass

        for_each(consume, {"y": PerDate}, [Consumed], subject=["A"], bout=[], date=[], db=db)
        assert sorted(seen) == [1.0, 2.0]
    finally:
        _scifor.set_schema([])
        db.close()
