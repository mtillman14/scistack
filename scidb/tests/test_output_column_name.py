"""``column_name`` on an output type renames its result column, not its type.

One owner, ``scifor.output_column_name``, names the result columns and scidb
matches them back to the output types it saves. A type with ``column_name``
set must still save and reload as itself.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject"]
SUBJECTS = ["S01", "S02"]


class ColIn(BaseVariable):
    pass


class ColOutRenamed(BaseVariable):
    column_name = "doubled"


class ColOutPlain(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "output_column_name.duckdb", SCHEMA)
    for subject in SUBJECTS:
        ColIn.save(3.0, subject=subject)
    yield database
    _scifor.set_schema([])
    database.close()


def test_renamed_output_saves_as_its_type_and_names_the_column(db):
    result = for_each(lambda x: x * 2, {"x": ColIn}, [ColOutRenamed], subject=SUBJECTS)
    assert "doubled" in result.columns
    assert "ColOutRenamed" not in result.columns
    for subject in SUBJECTS:
        assert ColOutRenamed.load(subject=subject).data == pytest.approx(6.0)


def test_two_outputs_one_renamed_save_to_the_right_types(db):
    result = for_each(
        lambda x: (x * 2, x * 10),
        {"x": ColIn},
        [ColOutRenamed, ColOutPlain],
        subject=SUBJECTS,
    )
    assert {"doubled", "ColOutPlain"} <= set(result.columns)
    assert ColOutRenamed.load(subject="S01").data == pytest.approx(6.0)
    assert ColOutPlain.load(subject="S01").data == pytest.approx(30.0)


def test_unset_column_name_is_the_class_name():
    assert BaseVariable.column_name is None
    assert _scifor.output_column_name(ColOutPlain) == "ColOutPlain"
    assert _scifor.output_column_name(ColOutRenamed) == "doubled"


def test_same_call_runs_on_scifor_unchanged(db):
    """The drop-in direction: a scidb-shaped call (types in outputs, save=,
    db=) runs on scifor with a DataFrame input and names the column alike."""
    frame = db.load_all_as_df(ColIn, stringify_schema=True)
    _scifor.set_schema(SCHEMA)
    result = _scifor.for_each(
        lambda x: x * 2,
        {"x": frame},
        [ColOutRenamed],
        save=True,
        db=db,
        subject=SUBJECTS,
    )
    assert "doubled" in result.columns
    assert np.allclose(result["doubled"].to_numpy(dtype=float), 6.0)
