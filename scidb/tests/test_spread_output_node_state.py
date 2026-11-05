"""A call that saves below the level it iterated still reaches green.

The bug (2026-11-05, ``combine_groups``, MATLAB): one call over the whole dataset
returned 882 rows that spread into 882 records. The predictor rebuilt the right
invocation id but paired it with the location the call iterated, where nothing was
ever saved, so the node read ``red — 1 expected invocation(s) not present`` after
every clean run.

What is pinned:

1. One call whose rows spread is green, with one expected pair per written location.
2. A per-subject call whose rows spread by a deeper key is green.
3. ``distribute``, which also saves one level below, is green.
4. The fix did not make these nodes green unconditionally: new input data and a
   partial run are still red.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.provenance.state import check_node_state


class SpreadRaw(BaseVariable):
    pass


class SpreadCombined(BaseVariable):
    pass


class SpreadByTrial(BaseVariable):
    pass


class SpreadDistributed(BaseVariable):
    pass


def combine_rows(table):
    """One row per subject, spread back out by the returned `subject` column."""
    return pd.DataFrame(
        {
            "subject": table["subject"].values,
            "row_number": np.arange(len(table), dtype=float),
        }
    )


def split_by_trial(raw):
    return pd.DataFrame({"trial": ["1", "2"], "score": [1.0, 2.0]})


def split_distributed(raw):
    return pd.DataFrame({"score": [1.0, 2.0]})


@pytest.fixture
def subject_db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "subject.duckdb", ["subject"])
    for subject in ("01", "02", "03"):
        SpreadRaw.save(np.array([1.0, 2.0]), subject=subject)
    yield database
    database.close()
    _scifor.set_schema([])


@pytest.fixture
def trial_db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "trial.duckdb", ["subject", "trial"])
    for subject in ("01", "02"):
        SpreadRaw.save(np.array([1.0, 2.0]), subject=subject)
    yield database
    database.close()
    _scifor.set_schema([])


def test_one_call_whose_rows_spread_is_green(subject_db):
    for_each(combine_rows, {"table": SpreadRaw}, [SpreadCombined], as_table=True)

    node = check_node_state(combine_rows, [SpreadCombined], db=subject_db)

    assert node["state"] == "green", node["counts"]
    assert node["counts"] == {"up_to_date": 3, "stale": 0, "missing": 0}
    assert {c["schema_combo"]["subject"] for c in node["combos"]} == {"01", "02", "03"}


def test_new_input_data_turns_a_spread_one_call_red(subject_db):
    for_each(combine_rows, {"table": SpreadRaw}, [SpreadCombined], as_table=True)
    SpreadRaw.save(np.array([1.0, 2.0]), subject="04")

    node = check_node_state(combine_rows, [SpreadCombined], db=subject_db)

    assert node["state"] == "red"
    assert node["counts"]["missing"] == 1


def test_per_subject_call_whose_rows_spread_is_green(trial_db):
    for_each(split_by_trial, {"raw": SpreadRaw}, [SpreadByTrial], subject=[])

    node = check_node_state(split_by_trial, [SpreadByTrial], db=trial_db)

    assert node["state"] == "green", node["counts"]
    assert node["counts"]["up_to_date"] == 4


def test_a_partial_per_subject_spread_run_is_red(trial_db):
    """Subject 01's outputs must not vouch for subject 02, which never ran."""
    for_each(split_by_trial, {"raw": SpreadRaw}, [SpreadByTrial], subject=["01"])

    node = check_node_state(split_by_trial, [SpreadByTrial], db=trial_db)

    assert node["state"] == "red"
    assert node["counts"]["missing"] == 1


def test_distribute_is_green(trial_db):
    for_each(
        split_distributed,
        {"raw": SpreadRaw},
        [SpreadDistributed],
        subject=[],
        distribute=True,
    )

    node = check_node_state(split_distributed, [SpreadDistributed], db=trial_db)

    assert node["state"] == "green", node["counts"]
