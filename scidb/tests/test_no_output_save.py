"""Saving a multi-output call with a NoOutput slot (Stage 1 of
.claude/plan-for-columns-groups.md): the skipped output gets no record at
that combination, the call's other outputs save, and an output that saved
nothing in the whole run is left with an EMPTY table, so SQL on its view
returns no rows instead of "does not exist".

The marker is internal; for_columns split mode will set it. These tests
return it from a plain multi-output function.
"""

import logging

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.exceptions import NotFoundError
from scifor.outputs import NoOutput

SCHEMA = ["subject"]


class NoOutIn(BaseVariable):
    pass


class NoOutA(BaseVariable):
    pass


class NoOutB(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "no_output.duckdb", SCHEMA)
    for s in (1, 2, 3):
        NoOutIn.save(np.arange(3.0) + s, db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def frames_skip_a_at_2(x):
    b = pd.DataFrame({"b": [float(np.max(x))]})
    if np.min(x) == 2.0:
        return NoOutput("no_data"), b
    return pd.DataFrame({"a": [float(np.mean(x))]}), b


def frames_never_a(x):
    return NoOutput("no_data"), pd.DataFrame({"b": [float(np.max(x))]})


def frames_always(x):
    return pd.DataFrame({"a": [float(np.mean(x))]}), pd.DataFrame({"b": [float(np.max(x))]})


def test_skipped_output_has_no_record_and_the_other_saves(db, caplog):
    with caplog.at_level(logging.INFO):
        for_each(
            frames_skip_a_at_2,
            inputs={"x": NoOutIn},
            outputs=[NoOutA, NoOutB],
            subject=[1, 2, 3],
            db=db,
        )

    assert NoOutA.load(db=db, subject=1).data["a"].iloc[0] == pytest.approx(2.0)
    assert NoOutA.load(db=db, subject=3).data["a"].iloc[0] == pytest.approx(4.0)
    with pytest.raises(NotFoundError):
        NoOutA.load(db=db, subject=2)
    for s, expected in ((1, 3.0), (2, 4.0), (3, 5.0)):
        assert NoOutB.load(db=db, subject=s).data["b"].iloc[0] == pytest.approx(expected)

    assert "output NoOutA: no data × 1 — subject=2" in caplog.text
    assert "[save] NoOutA: skipped at 1 combination(s) (1 no data)" in caplog.text


def test_output_skipped_everywhere_leaves_an_empty_table(db, caplog):
    with caplog.at_level(logging.INFO):
        for_each(
            frames_never_a,
            inputs={"x": NoOutIn},
            outputs=[NoOutA, NoOutB],
            subject=[1, 2, 3],
            db=db,
        )

    assert "[save] NoOutA: 0 records this run (3 no data); created empty table" in caplog.text
    view = db._duck._fetchdf(f'SELECT * FROM "{NoOutA.view_name()}"')
    assert len(view) == 0
    assert "subject" in view.columns
    assert db.load_all_as_df(NoOutA).empty
    assert len(db.load_all_as_df(NoOutB, layout="spread")) == 3


def test_a_later_run_with_data_rebuilds_the_empty_table(db):
    for_each(frames_never_a, inputs={"x": NoOutIn}, outputs=[NoOutA, NoOutB], subject=[1, 2, 3], db=db)
    for_each(frames_always, inputs={"x": NoOutIn}, outputs=[NoOutA, NoOutB], subject=[1, 2, 3], db=db)

    assert db.stored_data_column_types(NoOutA.table_name()) == {"a": "DOUBLE"}
    assert NoOutA.load(db=db, subject=2).data["a"].iloc[0] == pytest.approx(3.0)


def test_ensure_variable_table_is_idempotent(db):
    assert db.ensure_variable_table(NoOutA) is True
    assert db.ensure_variable_table(NoOutA) is False
