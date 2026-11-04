"""A function with several outputs that each return a DataFrame saves each
output with ITS OWN frame.

Found reading scifor's result assembly (2026-10-28, Stage 1 of
.claude/plan-for-columns-groups.md): when every output is a DataFrame of at
most one row, ``_results_to_output_dataframe`` spreads them, concatenating
all outputs side by side into one wide row. The output names then are not
columns of the result table, so ``_save_results`` takes its flatten branch
for EACH output and builds that output's record from EVERY data column.

for_columns split mode (one group per output, groups usually returning
small frames) is exactly this shape, so these pin what it relies on.
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, NotFoundError, configure_database, for_each
from scifor import MissingSchemaKeyError

SCHEMA = ["subject"]


class FramesIn(BaseVariable):
    pass


class FramesA(BaseVariable):
    pass


class FramesB(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "multi_output_frames.duckdb", SCHEMA)
    for s in (1, 2):
        FramesIn.save(np.arange(3.0) + s, db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def one_row_frames(x):
    return (
        pd.DataFrame({"a_mean": [float(np.mean(x))]}),
        pd.DataFrame({"b_max": [float(np.max(x))], "b_min": [float(np.min(x))]}),
    )


def one_row_frames_same_column(x):
    return (
        pd.DataFrame({"value": [float(np.mean(x))]}),
        pd.DataFrame({"value": [float(np.max(x))]}),
    )


def multi_row_frames(x):
    return (
        pd.DataFrame({"a": list(x)}),
        pd.DataFrame({"b": list(x * 10), "c": list(x * 100)}),
    )


def _only(var, db, subject):
    rec = var.load(db=db, subject=subject)
    assert not isinstance(rec, list), f"{var.__name__} at subject={subject}: {len(rec)} records"
    return rec.data


def test_one_row_frames_keep_their_own_columns(db):
    for_each(one_row_frames, inputs={"x": FramesIn}, outputs=[FramesA, FramesB], subject=[1, 2], db=db)

    a = _only(FramesA, db, 1)
    b = _only(FramesB, db, 1)
    assert list(a.columns) == ["a_mean"]
    assert list(b.columns) == ["b_max", "b_min"]
    assert a["a_mean"].iloc[0] == pytest.approx(2.0)
    assert b["b_max"].iloc[0] == pytest.approx(3.0)
    assert b["b_min"].iloc[0] == pytest.approx(1.0)


def test_one_row_frames_with_the_same_column_name(db):
    for_each(
        one_row_frames_same_column,
        inputs={"x": FramesIn},
        outputs=[FramesA, FramesB],
        subject=[1, 2],
        db=db,
    )

    assert _only(FramesA, db, 2)["value"].iloc[0] == pytest.approx(3.0)
    assert _only(FramesB, db, 2)["value"].iloc[0] == pytest.approx(4.0)


def test_multi_row_frames_keep_their_own_columns(db):
    for_each(multi_row_frames, inputs={"x": FramesIn}, outputs=[FramesA, FramesB], subject=[1, 2], db=db)

    a = _only(FramesA, db, 1)
    b = _only(FramesB, db, 1)
    assert list(a.columns) == ["a"]
    assert list(b.columns) == ["b", "c"]
    assert list(a["a"]) == pytest.approx([1.0, 2.0, 3.0])
    assert list(b["c"]) == pytest.approx([100.0, 200.0, 300.0])


# ---------------------------------------------------------------------------
# Each output files its rows by ITS OWN key columns (scistack.log 2026-11-04,
# parse_data: [subject, bout] and [subject, bout, date] from one call both saved
# per subject). .claude/plan-distribute-replaces-combine.md Stage 3.
# ---------------------------------------------------------------------------


@pytest.fixture
def leveled_db(tmp_path):
    db = configure_database(tmp_path / "leveled.duckdb", ["subject", "bout", "date"])
    for s in (1, 2):
        FramesIn.save(np.arange(3.0) + s, db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def subject_and_date_levels(x):
    per_bout = pd.DataFrame({"bout": [1, 2], "a": [10.0, 20.0]})
    per_date = pd.DataFrame(
        {
            "bout": [1, 1, 2],
            "date": ["2026-07-09", "2026-07-10", "2026-09-22"],
            "b": [1.0, 2.0, 3.0],
        }
    )
    return per_bout, per_date


def same_columns_at_two_levels(x):
    return (
        pd.DataFrame({"bout": [1], "value": [1.0]}),
        pd.DataFrame({"bout": [1], "date": ["2026-07-09"], "value": [2.0]}),
    )


def one_level_and_one_whole_table(x):
    return (
        pd.DataFrame({"bout": [1, 2], "a": [10.0, 20.0]}),
        pd.DataFrame({"b": [1.0, 2.0, 3.0]}),
    )


def blank_date(x):
    return (
        pd.DataFrame({"bout": [1], "a": [10.0]}),
        pd.DataFrame({"bout": [1, 1], "date": ["2026-07-09", None], "b": [1.0, 2.0]}),
    )


def _at(var, db, **keys):
    rec = var.load(db=db, **keys)
    assert not isinstance(rec, list), f"{var.__name__} at {keys}: {len(rec)} records"
    return rec.data


def test_two_outputs_save_at_their_own_levels(leveled_db):
    db = leveled_db
    for_each(
        subject_and_date_levels,
        inputs={"x": FramesIn},
        outputs=[FramesA, FramesB],
        subject=[1, 2],
        db=db,
    )

    for subject in (1, 2):
        assert len(FramesA.load(db=db, subject=subject)) == 2
        assert len(FramesB.load(db=db, subject=subject)) == 3

    assert _at(FramesA, db, subject=1, bout=2)["a"].iloc[0] == pytest.approx(20.0)
    b = _at(FramesB, db, subject=2, bout=1, date="2026-07-10")
    assert b["b"].iloc[0] == pytest.approx(2.0)
    # A schema key is an address, never data.
    assert list(b.columns) == ["b"]


def test_two_levels_keep_same_named_columns_apart(leveled_db):
    db = leveled_db
    for_each(
        same_columns_at_two_levels,
        inputs={"x": FramesIn},
        outputs=[FramesA, FramesB],
        subject=[1],
        db=db,
    )
    assert _at(FramesA, db, subject=1, bout=1)["value"].iloc[0] == pytest.approx(1.0)
    b = _at(FramesB, db, subject=1, bout=1, date="2026-07-09")
    assert b["value"].iloc[0] == pytest.approx(2.0)


def test_a_spreading_output_beside_a_whole_table(leveled_db):
    db = leveled_db
    for_each(
        one_level_and_one_whole_table,
        inputs={"x": FramesIn},
        outputs=[FramesA, FramesB],
        subject=[1],
        db=db,
    )
    assert len(FramesA.load(db=db, subject=1)) == 2
    assert list(_at(FramesB, db, subject=1)["b"]) == pytest.approx([1.0, 2.0, 3.0])


def test_a_blank_key_names_the_rows_and_saves_nothing(leveled_db):
    db = leveled_db
    with pytest.raises(MissingSchemaKeyError, match=r"FramesB at subject=1.*date: row\(s\) \[1\]"):
        for_each(
            blank_date,
            inputs={"x": FramesIn},
            outputs=[FramesA, FramesB],
            subject=[1],
            db=db,
        )
    with pytest.raises(NotFoundError):
        FramesA.load(db=db, subject=1)


def test_logs_name_the_function_not_the_wrapper(leveled_db, caplog):
    import logging

    with caplog.at_level(logging.INFO):
        for_each(
            subject_and_date_levels,
            inputs={"x": FramesIn},
            outputs=[FramesA, FramesB],
            subject=[1],
            db=leveled_db,
        )
    assert "for_each(fn)" not in caplog.text
    assert "for_each(subject_and_date_levels)" in caplog.text
    assert "FramesB: 3 record(s) filed by its own schema key column(s) ['bout', 'date']" in caplog.text
