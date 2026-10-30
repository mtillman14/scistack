"""``on_duplicate="combine"``: two rows at one schema location become one
record, a differing value the list of every row's value (scidb.combine; rule
scifor.combine_duplicate_rows; storage sciduckdb.row_lists).

The motivating shape: a CSV with a ``subject`` column, schema
``[subject, session]`` (no session column), one subject twice (two hospital
stays). Plan: ``.claude/plan-combine-duplicate-locations.md``.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest
import scifor as _scifor
from scifor import CombineMixedTypesError, PathInput

from scidb import (
    BaseVariable,
    DuplicateLocationError,
    OutputSaveError,
    configure_database,
    for_each,
)


class Stay(BaseVariable):
    pass


class Visit(BaseVariable):
    pass


class Doubled(BaseVariable):
    pass


def double(x):
    return x * 2


def read_table(path):
    return pd.read_csv(path)


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "combine.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()


def _stored_type(db, table, col):
    return db.stored_data_column_types(table)[col]


def _meta(db, type_name, col):
    return db.get_dtype_meta(type_name)["columns"][col]


def _value(db, cls, subject, col=None):
    var = cls.load(db=db, subject=subject)
    data = var.data
    if col is None:
        return data
    return data[col].iloc[0]


def _n_records(db, type_name) -> int:
    return db._duck._fetchall(
        "SELECT COUNT(*) FROM _record WHERE type = ?", [type_name]
    )[0][0]


# --- option ------------------------------------------------------------------


def test_invalid_on_duplicate_value(db):
    with pytest.raises(ValueError, match="on_duplicate"):
        Stay.save(pd.DataFrame({"subject": ["S1"], "x": [1.0]}), db=db, on_duplicate="merge")


def test_default_still_refuses(db):
    df = pd.DataFrame({"subject": ["S1", "S1"], "x": [5.0, 12.0]})
    with pytest.raises(DuplicateLocationError):
        Stay.save(df, db=db)


# --- single data column ------------------------------------------------------


def test_single_column_combines_and_unwraps(db):
    df = pd.DataFrame({"subject": ["S1", "S1", "S2"], "x": [5.0, 12.0, 7.0]})
    Stay.save(df, db=db, on_duplicate="combine")
    assert _n_records(db, "Stay") == 2
    assert _value(db, Stay, "S1") == [5.0, 12.0]
    assert _value(db, Stay, "S2") == 7.0
    assert _meta(db, "Stay", "value")["python_type"] == "row_list"


def test_combine_without_duplicates_stores_what_default_stores(db):
    df = pd.DataFrame({"subject": ["S1", "S2"], "x": [5.0, 7.0]})
    Stay.save(df, db=db, on_duplicate="combine")
    assert _stored_type(db, Stay.table_name(), "value") == "DOUBLE"
    assert _value(db, Stay, "S1") == 5.0


def test_identical_rows_one_record_scalar_column(db):
    df = pd.DataFrame({"subject": ["S1", "S1"], "x": [5.0, 5.0]})
    Stay.save(df, db=db, on_duplicate="combine")
    assert _n_records(db, "Stay") == 1
    assert _stored_type(db, Stay.table_name(), "value") == "DOUBLE"


def test_nan_handling(db):
    df = pd.DataFrame(
        {"subject": ["S1", "S1", "S2", "S2"], "x": [float("nan"), float("nan"), 5.0, float("nan")]}
    )
    Stay.save(df, db=db, on_duplicate="combine")
    assert math.isnan(_value(db, Stay, "S1"))
    s2 = _value(db, Stay, "S2")
    assert s2[0] == 5.0 and math.isnan(s2[1])


# --- several data columns ------------------------------------------------------


def test_multi_column_lists_and_collapses(db):
    df = pd.DataFrame(
        {
            "subject": ["S1", "S1", "S2"],
            "stay_days": [5, 12, 7],
            "age": [60, 60, 45],
            "ward": ["A", "B", "A"],
        }
    )
    Visit.save(df, db=db, on_duplicate="combine")
    table = Visit.table_name()
    assert _stored_type(db, table, "stay_days") == "BIGINT[]"
    assert _stored_type(db, table, "ward") == "VARCHAR[]"
    assert _stored_type(db, table, "age") == "BIGINT"
    assert _value(db, Visit, "S1", "stay_days") == [5, 12]
    assert _value(db, Visit, "S1", "ward") == ["A", "B"]
    assert _value(db, Visit, "S1", "age") == 60
    assert _value(db, Visit, "S2", "stay_days") == 7
    assert _value(db, Visit, "S2", "ward") == "A"


def test_three_rows_keep_alignment(db):
    df = pd.DataFrame(
        {"subject": ["S1"] * 3, "age": [60, 60, 61], "stay": [1, 2, 3]}
    )
    Visit.save(df, db=db, on_duplicate="combine")
    assert _value(db, Visit, "S1", "age") == [60, 60, 61]
    assert _value(db, Visit, "S1", "stay") == [1, 2, 3]


def test_uncombined_subject_keeps_its_record_id(db):
    """A location with one row is saved exactly as the default would."""
    base = pd.DataFrame({"subject": ["S2"], "stay": [7], "age": [45]})
    Visit.save(base, db=db)
    rid_before = Visit.load(db=db, subject="S2", introspect=True).record_id
    df = pd.DataFrame({"subject": ["S1", "S1", "S2"], "stay": [5, 12, 7], "age": [60, 60, 45]})
    Visit.save(df, db=db, on_duplicate="combine")
    rid_after = Visit.load(db=db, subject="S2", introspect=True).record_id
    assert rid_before == rid_after


def test_mixed_types_refused_nothing_written(db):
    df = pd.DataFrame({"subject": ["S1", "S1"], "x": [5, "x"], "y": [1, 2]})
    with pytest.raises(CombineMixedTypesError) as info:
        Visit.save(df, db=db, on_duplicate="combine")
    assert info.value.variable == "Visit"
    assert _n_records(db, "Visit") == 0


def test_int_and_float_combine(db):
    df = pd.DataFrame({"subject": ["S1", "S1"], "x": [5.0, 5.5], "y": [1, 1]})
    Visit.save(df, db=db, on_duplicate="combine")
    assert _value(db, Visit, "S1", "x") == [5.0, 5.5]


# --- lazy conversion -------------------------------------------------------


def test_d1_scenario_lazy_conversion(db):
    """Run 1 (error): 7 -> DOUBLE. Run 2 (combine): 7 and 8 -> the column is
    converted in place, the old record loads 7, the new one [7, 8]. Run 3
    (duplicate removed): 7, the column stays a row list."""
    table = Stay.table_name()
    Stay.save(pd.DataFrame({"subject": ["S1", "S2"], "x": [7.0, 3.0]}), db=db)
    assert _stored_type(db, table, "value") == "DOUBLE"
    old = Stay.load(db=db, subject="S2", introspect=True)

    Stay.save(
        pd.DataFrame({"subject": ["S1", "S1", "S2"], "x": [7.0, 8.0, 3.0]}),
        db=db,
        on_duplicate="combine",
    )
    assert _stored_type(db, table, "value") == "DOUBLE[]"
    assert _meta(db, "Stay", "value")["python_type"] == "row_list"
    assert _value(db, Stay, "S1") == [7.0, 8.0]
    # the stored record of S2 is untouched: same id, same value
    again = Stay.load(db=db, subject="S2", introspect=True)
    assert again.record_id == old.record_id
    assert again.data == 3.0
    every = Stay.load(db=db, subject="S1", version="all")
    values = sorted((v.data for v in every), key=lambda d: isinstance(d, list))
    assert values == [7.0, [7.0, 8.0]]

    Stay.save(pd.DataFrame({"subject": ["S1"], "x": [7.0]}), db=db)
    assert _stored_type(db, table, "value") == "DOUBLE[]"
    assert _value(db, Stay, "S1") == 7.0


def test_old_content_hashes_unchanged_by_conversion(db):
    Stay.save(pd.DataFrame({"subject": ["S1"], "x": [7.0]}), db=db)
    before = db._duck._fetchall(
        "SELECT record_id, content_hash FROM _record WHERE type = 'Stay'"
    )
    Stay.save(
        pd.DataFrame({"subject": ["S2", "S2"], "x": [1.0, 2.0]}),
        db=db,
        on_duplicate="combine",
    )
    after = dict(
        db._duck._fetchall("SELECT record_id, content_hash FROM _record WHERE type = 'Stay'")
    )
    for rid, h in before:
        assert after[rid] == h


def test_multi_column_lazy_conversion(db):
    table = Visit.table_name()
    Visit.save(pd.DataFrame({"subject": ["S1"], "stay": [5], "age": [60]}), db=db)
    Visit.save(
        pd.DataFrame({"subject": ["S2", "S2"], "stay": [1, 2], "age": [40, 40]}),
        db=db,
        on_duplicate="combine",
    )
    assert _stored_type(db, table, "stay") == "BIGINT[]"
    assert _stored_type(db, table, "age") == "BIGINT"
    assert _value(db, Visit, "S1", "stay") == 5
    assert _value(db, Visit, "S2", "stay") == [1, 2]


def test_int_row_list_widens_for_fractions(db):
    table = Stay.table_name()
    Stay.save(pd.DataFrame({"subject": ["S1", "S1"], "x": [1, 2]}), db=db, on_duplicate="combine")
    assert _stored_type(db, table, "value") == "BIGINT[]"
    Stay.save(pd.DataFrame({"subject": ["S2", "S2"], "x": [1.5, 2.5]}), db=db, on_duplicate="combine")
    assert _stored_type(db, table, "value") == "DOUBLE[]"
    assert _value(db, Stay, "S1") == [1.0, 2.0]


def test_row_list_and_array_column_never_mix(db):
    import numpy as np

    from scidb import ColumnTypeChangedError

    Stay.save(np.array([1.0, 2.0]), db=db, subject="S9")
    with pytest.raises(ColumnTypeChangedError):
        Stay.save(
            pd.DataFrame({"subject": ["S1", "S1"], "x": [1.0, 2.0]}),
            db=db,
            on_duplicate="combine",
        )


# --- variants and the backstop -------------------------------------------------


def test_variants_are_not_combined(db):
    db.save_batch(
        Stay,
        [
            (1.0, {"subject": "S1", "__branch_params": {"k": 1}, "k": 1}),
            (2.0, {"subject": "S1", "__branch_params": {"k": 2}, "k": 2}),
        ],
    )
    assert _n_records(db, "Stay") == 2


def test_variant_columns_keep_rows_apart_in_a_table(db):
    df = pd.DataFrame({"subject": ["S1", "S1"], "k": [1, 2], "x": [1.0, 2.0]})
    # k is neither a schema key nor the data column: it keeps the rows apart
    Stay.save_from_dataframe(df, "x", ["subject", "k"], db=db, on_duplicate="combine")
    assert _n_records(db, "Stay") == 2


def test_array_cells_are_not_combined_and_still_refused(db):
    import numpy as np

    with pytest.raises(DuplicateLocationError):
        db.save_batch(
            Stay,
            [
                (np.array([1.0, 2.0]), {"subject": "S1"}),
                (np.array([3.0]), {"subject": "S1"}),
            ],
        )


# --- for_each ------------------------------------------------------------------


def _csv(tmp_path, body):
    path = tmp_path / "stays.csv"
    path.write_text(body)
    return PathInput(str(path), name="StaysPath")


def test_for_each_flatten_combines(db, tmp_path):
    pi = _csv(tmp_path, "subject,days,ward\nS1,5,A\nS1,12,B\nS2,7,A\n")
    for_each(read_table, {"path": pi}, [Visit], on_duplicate="combine")
    assert _n_records(db, "Visit") == 2
    assert _value(db, Visit, "S1", "days") == [5, 12]
    assert _value(db, Visit, "S1", "ward") == ["A", "B"]
    assert _value(db, Visit, "S2", "days") == 7


def test_for_each_default_still_refuses(db, tmp_path):
    pi = _csv(tmp_path, "subject,days\nS1,5\nS1,12\n")
    with pytest.raises(OutputSaveError):
        for_each(read_table, {"path": pi}, [Visit])


def mixed_table(path):
    return pd.DataFrame(
        {"subject": ["S1", "S1", "S2"], "days": pd.Series([5, "x", 7], dtype=object)}
    )


def test_for_each_mixed_types_fail_the_save(db, tmp_path):
    pi = _csv(tmp_path, "unused\n1\n")
    with pytest.raises((OutputSaveError, CombineMixedTypesError)):
        for_each(mixed_table, {"path": pi}, [Visit], on_duplicate="combine")
    assert _n_records(db, "Visit") == 0


def test_for_each_normal_path_without_duplicates_unchanged(db):
    """combine mode on the normal (one value per combo) path stores what
    the default stores when no location repeats."""
    df = pd.DataFrame({"subject": ["S1", "S1", "S2"], "x": [5.0, 12.0, 7.0]})
    Stay.save(df.iloc[[0, 2]], db=db)

    for_each(
        double, {"x": Stay}, [Doubled], on_duplicate="combine", subject=["S1", "S2"]
    )
    assert _value(db, Doubled, "S1") == 10.0
    assert _stored_type(db, Doubled.table_name(), "value") == "DOUBLE"
