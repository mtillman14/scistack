"""
``as_table`` must survive full iteration mode.

When EVERY schema key is a ``for_each`` iteration key, scidb adds a ``__rid_*``
discriminator to each combo, and that is the only condition under which
``_normalize_variable_inputs`` runs. It unwrapped a variable input to its scalar
value (``df[var_name].iloc[0]``) without consulting ``as_table`` — so a function
that asked for the whole frame received a bare ``numpy.float64``, in that case
and in no other.

The narrowness is what made it survive: iterate one key short of the leaf and
the input arrives as a DataFrame exactly as asked. It became reachable in
ordinary use when scistackplot began promoting a nested ITERATE key's ancestors,
since "one figure per trial" then iterates the full schema depth.
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import pandas as pd
import pytest
from scidb.database import _local

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "session", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "as_table.duckdb", SCHEMA)
    yield database
    _scifor.set_schema([])
    database.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


class Speed(BaseVariable):
    pass


class Summary(BaseVariable):
    pass


def _seed():
    value = 0.0
    for subject in ["01", "02"]:
        for session in ["pre", "post"]:
            for trial in ["1", "2"]:
                value += 1.0
                Speed.save(value, subject=subject, session=session, trial=trial)


def test_as_table_input_stays_a_frame_when_every_key_iterates(db):
    _seed()
    seen: list[object] = []

    def summarize(df):
        seen.append(df)
        return float(len(df))

    for_each(
        summarize,
        inputs={"df": Speed},
        outputs=[Summary],
        as_table=["df"],
        subject=[],
        session=[],
        trial=[],
    )

    assert len(seen) == 8, "every (subject, session, trial) combination runs"
    assert all(isinstance(value, pd.DataFrame) for value in seen), (
        f"as_table asked for frames; got {[type(v).__name__ for v in seen]}"
    )
    # One record per location, and the schema keys arrive as columns — the
    # contract a plot_/stat_ endpoint body is written against.
    assert all(len(frame) == 1 for frame in seen)
    assert all(
        {"subject", "session", "trial"} <= set(frame.columns) for frame in seen
    )


def test_as_table_true_is_honoured_the_same_way(db):
    """`as_table=True` resolves to every loadable input, like scifor's rule."""
    _seed()
    seen: list[object] = []

    def summarize(df):
        seen.append(df)
        return float(len(df))

    for_each(
        summarize,
        inputs={"df": Speed},
        outputs=[Summary],
        as_table=True,
        subject=[],
        session=[],
        trial=[],
    )

    assert seen and all(isinstance(value, pd.DataFrame) for value in seen)


def test_without_as_table_the_scalar_unwrap_still_happens(db):
    """The normalization is right for its own case, and must keep working.

    A function that did not ask for a table gets the raw value it always got —
    this is the behaviour ``as_table`` is an exception to, not a bug.
    """
    _seed()
    seen: list[object] = []

    def double(df):
        seen.append(df)
        return float(df) * 2

    for_each(
        double,
        inputs={"df": Speed},
        outputs=[Summary],
        subject=[],
        session=[],
        trial=[],
    )

    assert seen and not any(isinstance(value, pd.DataFrame) for value in seen)
