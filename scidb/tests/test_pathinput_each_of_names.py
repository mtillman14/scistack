"""An EachOf of differently named PathInputs on one argument records each
arm under its OWN name (2026-10-20).

The GUI binds two PathInput nodes drawn onto one port as
``EachOf(PathInput(.., name="pi_a"), PathInput(.., name="pi_b"))``.
``parameter_names`` names one declaration per argument, so it cannot speak
for both arms; the PathInput's name (its identity key) is the owner of
"which declared PathInput fed this argument".
"""

import pytest

import scifor as _scifor
from scidb import BaseVariable, EachOf, PathInput, configure_database, for_each

SCHEMA = ["subject"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "pi_each_of.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class FileValue(BaseVariable):
    pass


def read_value(filepath):
    return float(open(filepath).read())


def _files(tmp_path):
    (tmp_path / "a_1.txt").write_text("1")
    (tmp_path / "b_2.txt").write_text("2")
    return EachOf(
        PathInput("a_{subject}.txt", name="pi_a", root_folder=tmp_path),
        PathInput("b_{subject}.txt", name="pi_b", root_folder=tmp_path),
    )


def _recorded_names(db) -> set[str]:
    return {
        name
        for fn in db.get_aggregated_variants()["functions"].values()
        for param, name in fn["path_input_names"].items()
        if param == "filepath"
    }


def test_each_arm_records_its_own_name(db, tmp_path):
    for_each(read_value, inputs={"filepath": _files(tmp_path)}, outputs=[FileValue])
    assert _recorded_names(db) == {"pi_a", "pi_b"}
    assert FileValue.load(db=db, subject=1).data == 1.0
    assert FileValue.load(db=db, subject=2).data == 2.0


def test_parameter_names_cannot_relabel_an_arm(db, tmp_path):
    """The GUI may pass one declared name for the argument; the second arm
    still records as itself."""
    for_each(
        read_value,
        inputs={"filepath": _files(tmp_path)},
        outputs=[FileValue],
        parameter_names={"filepath": "pi_a"},
    )
    assert _recorded_names(db) == {"pi_a", "pi_b"}
