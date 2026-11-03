"""``ComboFailure.from_dict`` inverts ``to_dict`` (P5 stage 3).

MATLAB's loop sends its failure records (``+scifor/failure_from_mexception.m``,
the ``to_dict`` shape) to ``scifor.matlab_loop.assemble``, which rebuilds
them as ``ComboFailure`` so Python's summary code reads a MATLAB failure
like its own. A field lost here would vanish from the GUI's failure panel.
"""

from scifor.failures import ComboFailure, FailureFrame


def _failure() -> ComboFailure:
    frame = FailureFrame(file="/u/fn.m", line=3, function="fn", code="", origin="user")
    lib = FailureFrame(file="/m/toolbox/x.m", line=9, function="x", code="", origin="library")
    return ComboFailure(
        fn_name="fn", metadata={"subject": "1"}, stage="prepare_inputs",
        exc_type="test:bad", message="bad", frames=(frame, lib), user_frame=frame,
        no_data=False, blame="user_code", probable=True, remedy={"title": "t"},
    )


def test_round_trip():
    failure = _failure()
    assert ComboFailure.from_dict(failure.to_dict()) == failure


def test_unknown_stage_and_origin_read_safely():
    raw = _failure().to_dict()
    raw["stage"] = "elsewhere"
    raw["frames"][0]["origin"] = "???"
    back = ComboFailure.from_dict(raw)
    assert back.stage == "call"
    assert back.frames[0].origin == "user"


def test_no_user_frame():
    raw = {**_failure().to_dict(), "user_frame": None, "remedy": []}
    back = ComboFailure.from_dict(raw)
    assert back.user_frame is None and back.remedy is None
