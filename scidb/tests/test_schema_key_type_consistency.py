"""
A schema key's loaded values share ONE type.

Found by the example integration suite (2026-09-19): cycles "01".."10" came
back as "01".."09" plus the int 10, because the VARCHAR -> number restore
(`_from_schema_str`) was decided per VALUE. "10" round-trips through int and
"01" does not, so one column held both types — and a `set(cycle) == {...}`
comparison, a groupby, or a plot axis then saw ten levels that were not the
ten that were saved.

The rule is now per KEY (`DatabaseManager.schema_key_is_numeric`): declared
type first, else numeric only when EVERY stored value of the key round-trips.
"""

from __future__ import annotations

import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database


class Value(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "types.duckdb", ["subject", "cycle"])
    yield db
    _scifor.set_schema([])
    db.close()


def _cycles(db, **where):
    return Value.load(as_df=True, **where)["cycle"].tolist()


def test_one_zero_padded_value_keeps_the_whole_key_a_string(db):
    for cycle in ["01", "02", "09", "10", "11"]:
        Value.save(1.0, subject="s1", cycle=cycle)

    cycles = _cycles(db)
    assert sorted(cycles) == ["01", "02", "09", "10", "11"]
    assert {type(v) for v in cycles} == {str}, cycles


def test_plain_numbers_still_come_back_as_numbers(db):
    """The restore exists so `subject=1` saved as an int loads as an int; a
    key whose values ALL round-trip keeps that behaviour."""
    for subject in [1, 2, 10]:
        Value.save(1.0, subject=subject, cycle="01")

    subjects = Value.load(as_df=True)["subject"].tolist()
    assert sorted(subjects) == [1, 2, 10]
    assert {type(v) for v in subjects} == {int}


def test_the_answer_changes_when_a_padded_value_arrives_later(db):
    """The per-key decision is cached, and the cache must not outlive a save
    that changes the answer."""
    Value.save(1.0, subject="s1", cycle="1")
    Value.save(1.0, subject="s1", cycle="2")
    assert sorted(_cycles(db)) == [1, 2]

    Value.save(1.0, subject="s1", cycle="03")
    cycles = _cycles(db)
    assert sorted(cycles) == ["03", "1", "2"]
    assert {type(v) for v in cycles} == {str}


def test_a_declared_string_key_never_converts(tmp_path):
    _scifor.set_schema([])
    db = configure_database(
        tmp_path / "declared.duckdb",
        ["subject", "cycle"],
        schema_key_types={"cycle": "string"},
    )
    try:
        Value.save(1.0, subject="s1", cycle="7")
        cycles = _cycles(db)
        assert cycles == ["7"]
    finally:
        _scifor.set_schema([])
        db.close()


def test_single_record_load_agrees_with_the_frame(db):
    """`load()` (one record, metadata dict) and `load(as_df=True)` go through
    different code paths; both must apply the key's type."""
    for cycle in ["01", "10"]:
        Value.save(1.0, subject="s1", cycle=cycle)
    record = Value.load(subject="s1", cycle="10")
    assert record.metadata["cycle"] == "10"
    assert isinstance(record.metadata["cycle"], str)
