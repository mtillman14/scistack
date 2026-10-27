"""The per-combo failure record (scifor.failures) and on_error="stop".

Pins:
- a raising fn yields a ComboFailure whose user_frame is the raising line in
  the user's file, with scifor's loop frames trimmed away
- a library (pandas) error is attributed to the user's call site
- input-preparation failures are recorded with stage="prepare_inputs"
- NoDataError groups are no_data and never stop a run
- one group per reason, combos capped on the wire, JSON-safe
- on_error="stop" re-raises the ORIGINAL exception with the partial result
- frame_origin is the only frame classifier (AST guard)
See docs/claude/error-surfacing.md.
"""

import ast
import inspect
import json
import logging
import pathlib

import pandas as pd
import pytest

import scifor
from scifor import ColumnSelection, for_each, set_schema, stopped_run_of
from scifor.failures import (
    FRAMES_MAX,
    MESSAGE_MAX_CHARS,
    WIRE_COMBOS_MAX,
    frame_origin,
)

THIS_FILE = __file__


def setup_function():
    set_schema([])


def _summary(events):
    (s,) = [e for e in events if e.get("event") == "summary"]
    return s


def _line_of(fn, needle):
    src, start = inspect.getsourcelines(fn)
    for i, text in enumerate(src):
        if needle in text:
            return start + i
    raise AssertionError(needle)


def _df(n=3):
    return pd.DataFrame({"subject": list(range(1, n + 1)), "value": [1.0] * n})


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


def test_user_frame_is_the_raising_line():
    set_schema(["subject"])
    events = []

    def broken(value):
        raise KeyError("missing_channel")  # RAISE-HERE

    for_each(broken, inputs={"value": _df(2)}, subject=[1, 2], _progress_fn=events.append)

    (group,) = _summary(events)["failures"]
    assert group["count"] == 2 and group["no_data"] is False
    assert group["exc_type"] == "KeyError"
    uf = group["user_frame"]
    assert uf["file"] == THIS_FILE
    assert uf["line"] == _line_of(broken, "RAISE-HERE")
    assert uf["function"] == "broken"
    example = group["example"]
    assert example["stage"] == "call"
    assert example["metadata"] == {"subject": "1"}
    # scifor's loop frames are trimmed: the traceback starts in user code.
    assert example["frames"][0]["origin"] != "framework"
    assert all(
        "scifor/foreach.py" not in f["file"].replace("\\", "/")
        for f in example["frames"]
    )


def test_library_error_is_attributed_to_the_user_call_site():
    set_schema(["subject"])
    events = []

    def bad_concat(value):
        return pd.concat([value, 3])  # CALL-SITE

    for_each(bad_concat, inputs={"value": _df(1)}, subject=[1], _progress_fn=events.append)

    (group,) = _summary(events)["failures"]
    frames = group["example"]["frames"]
    assert frames[-1]["origin"] == "library"  # pandas raised
    assert group["user_frame"]["line"] == _line_of(bad_concat, "CALL-SITE")


def test_combo_skip_event_carries_the_small_failure_form():
    set_schema(["subject"])
    events = []

    def broken(value):
        raise ValueError("nope")

    for_each(broken, inputs={"value": _df(1)}, subject=[1], _progress_fn=events.append)

    (skip,) = [e for e in events if e["event"] == "combo_skip"]
    assert skip["failure"]["reason"] == "ValueError: nope"
    assert skip["failure"]["no_data"] is False
    assert skip["failure"]["user_frame"]["file"] == THIS_FILE
    assert "frames" not in skip["failure"]  # tracebacks travel once, in the summary


def test_prepare_inputs_failure_has_stage_and_no_crash():
    set_schema(["subject"])
    events = []
    df = pd.DataFrame({"subject": [1], "a": [1.0]})

    for_each(
        lambda v: v,
        inputs={"v": ColumnSelection(df, ["no_such_column"])},
        subject=[1],
        _progress_fn=events.append,
    )

    (group,) = _summary(events)["failures"]
    assert group["example"]["stage"] == "prepare_inputs"
    assert group["no_data"] is False


def test_no_data_group_is_marked_and_never_warns(caplog):
    set_schema(["subject"])
    events = []
    df = pd.DataFrame({"subject": [1], "value": [1.0]})

    with caplog.at_level(logging.DEBUG, logger="scifor"):
        for_each(
            lambda value: value,
            inputs={"value": df},
            subject=[1, 2],
            _progress_fn=events.append,
        )

    s = _summary(events)
    (group,) = s["failures"]
    assert group["no_data"] is True and group["exc_type"] == "NoDataError"
    assert s["no_data"] == 1 and s["failed"] == 0
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_for_columns_failure_points_at_the_user_line():
    """ColumnFunctionError is raised by scifor `from` the first per-column
    error, so the record still finds the user's line."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1], "a": ["x"]})
    events = []

    def to_float(v):
        return float(v)  # COLUMN-LINE

    for_each(
        to_float,
        inputs={"v": ColumnSelection(df, ["a"], iterate=True)},
        subject=[1],
        _progress_fn=events.append,
    )

    (group,) = _summary(events)["failures"]
    assert group["exc_type"] == "ColumnFunctionError"
    assert group["user_frame"]["line"] == _line_of(to_float, "COLUMN-LINE")


def test_one_group_per_reason_capped_and_json_safe():
    set_schema(["subject"])
    n = WIRE_COMBOS_MAX + 5
    events = []

    def broken(value):
        raise ValueError("x" * (MESSAGE_MAX_CHARS + 100))

    for_each(broken, inputs={"value": _df(n)}, subject=list(range(1, n + 1)),
             _progress_fn=events.append)

    s = _summary(events)
    (group,) = s["failures"]
    assert group["count"] == n
    assert len(group["combos"]) == WIRE_COMBOS_MAX
    assert len(group["message"]) == MESSAGE_MAX_CHARS
    assert len(group["example"]["frames"]) <= FRAMES_MAX
    json.dumps(s["failures"])  # wire shape round-trips


def test_warn_and_summary_lines_name_the_user_frame(caplog):
    set_schema(["subject"])

    def broken(value):
        raise ValueError("boom")

    with caplog.at_level(logging.INFO, logger="scifor"):
        for_each(broken, inputs={"value": _df(1)}, subject=[1])

    text = "\n".join(r.getMessage() for r in caplog.records)
    assert f"at {THIS_FILE}:" in text
    failed = [r.getMessage() for r in caplog.records if r.getMessage().startswith("failed:")]
    assert failed and "in broken" in failed[0]


# ---------------------------------------------------------------------------
# frame_origin
# ---------------------------------------------------------------------------


def test_frame_origin_classifies():
    assert frame_origin(scifor.__file__) == "framework"
    assert frame_origin(pd.__file__) == "library"
    assert frame_origin(json.__file__) == "library"
    assert frame_origin(THIS_FILE) == "user"
    assert frame_origin("<string>") == "user"


def test_frame_origin_is_the_only_frame_classifier():
    """No other scifor/scidb/scistack_gui module walks tracebacks to decide
    what is user code (one owner — NOTE 4)."""
    root = pathlib.Path(scifor.__file__).resolve().parents[3]
    offenders = []
    for pkg in ("scifor/src", "scidb/src", "scihist/src", "scistack-gui/scistack_gui"):
        base = root / pkg
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            if path.name == "failures.py" and path.parent.name == "scifor":
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Attribute)
                    and node.attr == "extract_tb"
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "traceback"
                ):
                    offenders.append(str(path))
    assert offenders == []


# ---------------------------------------------------------------------------
# on_error
# ---------------------------------------------------------------------------


def test_on_error_rejects_unknown_values():
    with pytest.raises(ValueError, match="on_error"):
        for_each(lambda value: value, inputs={"value": _df(1)}, subject=[1], on_error="halt")


def _value_is_subject(n):
    """value == subject, so the function can tell combos apart."""
    return pd.DataFrame({"subject": list(range(1, n + 1)), "value": list(range(1, n + 1))})


def test_stop_reraises_original_with_partial_result():
    set_schema(["subject"])
    events = []

    def third_fails(value):
        if value == 3:
            raise ZeroDivisionError("third")  # STOP-LINE
        return float(value)

    with pytest.raises(ZeroDivisionError) as info:
        for_each(
            third_fails,
            inputs={"value": _value_is_subject(5)},
            subject=[1, 2, 3, 4, 5],
            on_error="stop",
            _progress_fn=events.append,
        )

    stopped = stopped_run_of(info.value)
    assert stopped is not None
    assert stopped.completed == 2
    assert list(stopped.partial_result["subject"]) == [1, 2]
    assert stopped.failure.user_frame.line == _line_of(third_fails, "STOP-LINE")
    # The traceback ends in the user's code, like vanilla Python.
    assert pathlib.Path(str(info.traceback[-1].path)) == pathlib.Path(THIS_FILE)
    s = _summary(events)
    assert s["stopped"] is True and s["completed"] == 2 and s["failed"] == 1
    # Combos 4 and 5 never ran.
    assert len([e for e in events if e["event"] == "combo_start"]) == 3


def test_stop_never_fires_on_no_data():
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 3], "value": [1.0, 3.0]})

    result = for_each(
        lambda value: value, inputs={"value": df}, subject=[1, 2, 3], on_error="stop"
    )

    assert list(result["subject"]) == [1, 3]


def test_continue_is_the_default_and_unchanged():
    set_schema(["subject"])

    def first_fails(value):
        if value == 1:
            raise ValueError("first")
        return value

    result = for_each(first_fails, inputs={"value": _value_is_subject(3)}, subject=[1, 2, 3])

    assert list(result["subject"]) == [2, 3]


def test_stop_inside_each_of_keeps_earlier_alternatives():
    set_schema(["subject"])
    good = _value_is_subject(2)
    bad = pd.DataFrame({"subject": [1, 2], "value": [-1, -1]})

    def positive(value):
        if value < 0:
            raise ValueError("negative")
        return value

    with pytest.raises(ValueError) as info:
        for_each(
            positive,
            inputs={"value": scifor.EachOf(good, bad)},
            subject=[1, 2],
            on_error="stop",
        )

    stopped = stopped_run_of(info.value)
    assert stopped is not None
    assert len(stopped.partial_result) == 2  # the first alternative's rows


# --- remedy: a structured popup an ActionableError may carry ----------------


class _RemedyError(scifor.ActionableError):
    remedy = {
        "kind": "demo",
        "title": "t",
        "explanation": "e",
        "items": [{"location": "subject=S1", "count": 2, "differing_columns": ["age"]}],
        "n_more": 0,
        "steps": [{"title": "s", "text": "x", "action": "open_schema_keys"}],
    }


def test_remedy_rides_the_record_and_the_cause_chain():
    from scifor.failures import FailureGroup, run_failure

    try:
        try:
            raise _RemedyError("refused")
        except _RemedyError as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as outer:
        failure = run_failure(outer, fn_name="f")
    assert failure.remedy == _RemedyError.remedy
    wire = FailureGroup(example=failure).to_dict()
    assert wire["remedy"] == _RemedyError.remedy
    json.dumps(wire)


def test_no_remedy_is_none_on_the_wire():
    from scifor.failures import FailureGroup, run_failure

    failure = run_failure(ValueError("x"), fn_name="f")
    assert FailureGroup(example=failure).to_dict()["remedy"] is None


def test_remedy_from_matlab_wire_undoes_jsonencode_quirks():
    """jsonencode writes a 1-element array as a bare object/string, and
    "no value" as []."""
    from scifor.failures import groups_from_wire

    raw = {
        "reason": "r",
        "remedy": {
            "kind": "demo",
            "title": "t",
            "items": {"location": "subject=S1", "count": 2, "differing_columns": "age"},
            "steps": {"title": "s", "text": "x", "action": "reveal_function"},
        },
    }
    (g,) = groups_from_wire([raw])
    assert g["remedy"]["items"] == [
        {"location": "subject=S1", "count": 2, "differing_columns": ["age"]}
    ]
    assert g["remedy"]["steps"][0]["action"] == "reveal_function"
    assert g["remedy"]["n_more"] == 0
    (none,) = groups_from_wire([{"reason": "r", "remedy": []}])
    assert none["remedy"] is None
