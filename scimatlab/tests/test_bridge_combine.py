"""``save_batch_bridge(on_duplicate="combine")`` — MATLAB's save_from_table
with two rows at one location (scidb.combine; plan
``.claude/plan-combine-duplicate-locations.md``).

The returned ids stay one per INPUT row (MATLAB splits them by line):
combined rows share their record's id. Runs in Python without MATLAB: the
arguments are what +scidb/BaseVariable.m save_from_table sends.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
import pandas as pd
import pytest
from scidb import DuplicateLocationError
from scidb.database import configure_database
from scimatlab.bridge import register_matlab_variable, save_batch_bridge


@pytest.fixture
def db(tmp_path):
    import scifor as _scifor

    _scifor.set_schema([])
    database = configure_database(tmp_path / "combine.duckdb", ["subject", "session"])
    yield database
    database.close()


def _save(values, subjects, **kwargs):
    return save_batch_bridge(
        "MatlabStay", values, ["subject"], [np.array(subjects, dtype=float)],
        {}, None, None, **kwargs,
    ).split("\n")


def test_scalar_rows_combine_ids_per_input_row(db):
    cls = register_matlab_variable("MatlabStay")
    rids = _save(np.array([5.0, 12.0, 7.0]), [1.0, 1.0, 2.0], on_duplicate="combine")
    assert len(rids) == 3 and all(rids)
    assert rids[0] == rids[1] != rids[2]
    assert cls.load(subject=1).data == [5.0, 12.0]
    assert cls.load(subject=2).data == 7.0


def test_default_refuses(db):
    register_matlab_variable("MatlabStay")
    with pytest.raises(DuplicateLocationError):
        _save(np.array([5.0, 12.0]), [1.0, 1.0])


def test_unique_rows_unchanged(db):
    cls = register_matlab_variable("MatlabStay")
    _save(np.array([5.0, 7.0]), [1.0, 2.0], on_duplicate="combine")
    assert db.stored_data_column_types(cls.table_name()) == {"value": "DOUBLE"}


def test_one_row_tables_combine(db):
    """The multi-data-column path: each row's data is a 1-row table
    (Strategy A vertcat + row heights)."""
    cls = register_matlab_variable("MatlabVisit")
    bulk = pd.DataFrame({"days": [5.0, 12.0, 7.0], "age": [60.0, 60.0, 45.0]})
    rids = save_batch_bridge(
        "MatlabVisit", bulk, ["subject"], [np.array([1.0, 1.0, 2.0])],
        {}, None, np.array([1, 1, 1]), on_duplicate="combine",
    ).split("\n")
    assert rids[0] == rids[1] != rids[2]
    s1 = cls.load(subject=1).data
    assert s1["days"].iloc[0] == [5.0, 12.0]
    assert s1["age"].iloc[0] == 60.0
    assert cls.load(subject=2).data["days"].iloc[0] == 7.0


def test_d1_sequence_through_the_bridge(db):
    cls = register_matlab_variable("MatlabStay")
    _save(np.array([7.0]), [1.0])
    _save(np.array([7.0, 8.0]), [1.0, 1.0], on_duplicate="combine")
    assert db.stored_data_column_types(cls.table_name()) == {"value": "DOUBLE[]"}
    assert cls.load(subject=1).data == [7.0, 8.0]
    _save(np.array([7.0]), [1.0])
    assert cls.load(subject=1).data == 7.0


def test_invalid_value(db):
    register_matlab_variable("MatlabStay")
    with pytest.raises(ValueError, match="on_duplicate"):
        _save(np.array([1.0]), [1.0], on_duplicate="merge")
