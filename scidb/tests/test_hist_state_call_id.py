"""Tests for node-state across multiple for_each configs of one function.

Historically this used an explicit ``call_id`` to keep call sites from blurring.
``call_id`` is gone: each config produces config-specific ``invocation_id``s, so
distinct call sites coexist automatically and node completeness is a pure
invocation-membership test (§9c). These tests verify that coexistence and the
partial-run ("you have input data not yet processed with these params")
detection still hold through the public ``check_node_state`` API.
"""

import numpy as np
import pytest
from scidb.state import check_node_state

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_state_call_id.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


def bandpass(signal, low_hz):
    return signal * low_hz


def _seed(db, subjects, sessions):
    for s in subjects:
        for sess in sessions:
            RawSignal.save(np.array([1.0, 2.0, 3.0]), db=db, subject=s, session=sess)


def test_two_configs_partial_each_union_is_red(db):
    """Two configs (different constants) each run on a subset → union red.

    Input: 2 subjects × 2 sessions = 4 RawSignal. low_hz=20 runs session A,
    low_hz=50 runs session B. Each config's live-derived expected set spans all
    4 input locations, so 4 of 8 expected invocations are present → any missing
    makes the node red (binary model — no grey).
    """
    _seed(db, subjects=["1", "2"], sessions=["A", "B"])

    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[Filtered],
        db=db,
        subject=["1", "2"],
        session=["A"],
    )
    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[Filtered],
        db=db,
        subject=["1", "2"],
        session=["B"],
    )

    state = check_node_state(bandpass, [Filtered], db=db)
    assert state["counts"]["up_to_date"] == 4, state
    assert state["counts"]["missing"] == 4, state
    assert state["state"] == "red", state


def test_two_configs_fully_run_is_green(db):
    """Both configs run over the full input grid → every expected invocation
    present → green (the two configs coexist, neither clobbers the other)."""
    _seed(db, subjects=["1", "2"], sessions=["A", "B"])

    for low_hz in (20, 50):
        for_each(
            bandpass,
            inputs={"signal": RawSignal, "low_hz": low_hz},
            outputs=[Filtered],
            db=db,
            subject=["1", "2"],
            session=["A", "B"],
        )

    state = check_node_state(bandpass, [Filtered], db=db)
    # 2 configs × 4 locations = 8 expected, all present.
    assert state["counts"]["up_to_date"] == 8, state
    assert state["counts"]["missing"] == 0, state
    assert state["state"] == "green", state


def test_config_partial_run_is_red(db):
    """One config run on a subset of available input → red (partial-run
    detection: subjects 2,3 exist in input but weren't processed → missing)."""
    _seed(db, subjects=["1", "2", "3"], sessions=["A"])

    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[Filtered],
        db=db,
        subject=["1"],
        session=["A"],
    )

    state = check_node_state(bandpass, [Filtered], db=db)
    assert state["counts"]["up_to_date"] == 1, state
    assert state["counts"]["missing"] == 2, state
    assert state["state"] == "red", state
