"""scidb.for_each(on_error="stop"): save the completed combos, then re-raise
the ORIGINAL exception (scifor owns the option; see
docs/claude/error-surfacing.md).

Without the save, a stopped run would drop every result computed before the
failure — scidb saves only after scifor's loop returns.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject"]


class StopIn(BaseVariable):
    pass


class StopOut(BaseVariable):
    pass


def third_fails(x):
    if float(np.asarray(x).ravel()[0]) == 3.0:
        raise ZeroDivisionError("subject three")
    return np.asarray(x) * 2


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "stop.duckdb", SCHEMA)
    for s in (1, 2, 3, 4):
        StopIn.save(np.array([float(s)]), db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def _saved_subjects(db):
    frame = db.load_all_as_df(StopOut, stringify_schema=True)
    return sorted(frame["subject"].tolist())


def test_stop_saves_completed_then_reraises_original(db):
    with pytest.raises(ZeroDivisionError, match="subject three") as info:
        for_each(
            third_fails,
            inputs={"x": StopIn},
            outputs=[StopOut],
            subject=[1, 2, 3, 4],
            on_error="stop",
            db=db,
        )

    stopped = _scifor.stopped_run_of(info.value)
    assert stopped is not None and stopped.completed == 2
    assert stopped.failure.user_frame.function == "third_fails"
    assert _saved_subjects(db) == ["1", "2"]  # 4 never ran; 1 and 2 kept


def test_continue_saves_everything_but_the_failure(db):
    for_each(third_fails, inputs={"x": StopIn}, outputs=[StopOut], subject=[1, 2, 3, 4], db=db)
    assert _saved_subjects(db) == ["1", "2", "4"]


def test_bad_on_error_value_rejected_before_any_work(db):
    with pytest.raises(ValueError, match="on_error"):
        for_each(
            third_fails,
            inputs={"x": StopIn},
            outputs=[StopOut],
            subject=[1],
            on_error="explode",
            db=db,
        )


def test_stop_halts_a_pipeline(db):
    from scidb.pipeline import _reset_pipeline_state

    _reset_pipeline_state()
    try:
        pipe = db.pipeline("stop_pipe")
        for_each(
            third_fails,
            {"x": StopIn},
            [StopOut],
            subject=[1, 2, 3, 4],
            on_error="stop",
            db=db,
        )
        with pytest.raises(ZeroDivisionError):
            pipe.run_all()
        assert _saved_subjects(db) == ["1", "2"]
    finally:
        _reset_pipeline_state()
