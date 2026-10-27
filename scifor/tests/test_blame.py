"""Who can fix a failure (scifor.failures.classify_blame) and the user-value seam.

Pins:
- each blame category from a real for_each run: user_code (the user's raise,
  a library call made from user code, a library function used directly as
  the function), setup (an ActionableError), user_value (UserValueError,
  ForColumnsError), no_data, internal (SciStack's own code)
- blame + blame_label travel on the wire (FailureGroup.to_dict) and survive
  groups_from_wire; a writer without the field gets the frame rule's answer
- user_value_seam: an unexpected exception while combining outputs becomes a
  UserValueError — precise when the values differ in kind across combos,
  probable (hedged, [seam_fallback] logged) otherwise; ActionableErrors pass
  through unchanged
- for_each's combine step is lexically inside the seam (AST guard)
- the journal block of a run carries the run-level error with its blame
See docs/claude/error-surfacing.md and docs/claude/logging-architecture.md.
"""

import ast
import json
import logging
import pathlib

import numpy as np
import pandas as pd
import pytest

import scifor.foreach as scifor_foreach
from scifor import ForColumnsError, NoDataError, for_each, set_schema
from scifor.failures import (
    ActionableError,
    FailureGroup,
    UserValueError,
    as_user_value_error,
    call_user_function,
    classify_blame,
    describe_value_kinds,
    failure_from_exception,
    groups_from_wire,
    run_failure,
    user_value_seam,
)
from scistacklog import Log


def setup_function():
    set_schema([])


def _summary(events):
    (s,) = [e for e in events if e.get("event") == "summary"]
    return s


def _df(n=2):
    return pd.DataFrame({"subject": list(range(1, n + 1)), "value": [1.0] * n})


def _only_group(fn, n=1):
    set_schema(["subject"])
    events = []
    for_each(fn, inputs={"value": _df(n)}, subject=list(range(1, n + 1)), _progress_fn=events.append)
    (group,) = _summary(events)["failures"]
    return group


# ---------------------------------------------------------------------------
# Categories, from real runs
# ---------------------------------------------------------------------------


def test_users_own_raise_is_user_code():
    def broken(value):
        raise KeyError("missing_channel")

    group = _only_group(broken)
    assert group["blame"] == "user_code"
    assert group["probable"] is False
    assert group["blame_label"] == "Error in your function broken"


def test_library_error_called_from_user_code_is_user_code():
    def bad_concat(value):
        return pd.concat([value, 3])

    assert _only_group(bad_concat)["blame"] == "user_code"


def test_library_function_used_directly_is_user_code():
    # The GUI wires library functions (pandas.read_csv) up as the function
    # itself, so the innermost non-library frame is call_user_function, which
    # marks where the user's function began.
    try:
        call_user_function(json.loads, "{not json")
    except ValueError as e:
        failure = failure_from_exception(e, fn_name="loads", metadata={}, stage="call")
    assert failure.blame == "user_code"


def test_actionable_error_is_setup():
    class BadConfig(ActionableError):
        pass

    def refuses(value):
        raise BadConfig("set schema_keys first")

    group = _only_group(refuses)
    assert group["blame"] == "setup"
    assert group["blame_label"] == "Problem with your setup or data"


def test_user_value_error_is_user_value_and_probable_is_kept():
    def returns_bad(value):
        raise UserValueError("bad shape", probable=True)

    group = _only_group(returns_bad)
    assert group["blame"] == "user_value"
    assert group["probable"] is True
    assert group["blame_label"] == "Probably fixable in your function returns_bad"


def test_for_columns_structural_error_is_a_user_value_error():
    assert issubclass(ForColumnsError, UserValueError)
    assert issubclass(ForColumnsError, ValueError)  # existing `except ValueError` still catch it


def test_no_data_is_no_data():
    try:
        raise NoDataError("No data for this combo after filtering.")
    except NoDataError as e:
        failure = failure_from_exception(e, fn_name="f", metadata={}, stage="prepare_inputs", no_data=True)
    assert failure.blame == "no_data"


def test_scistack_own_code_raising_is_internal():
    # scifor.failures._cap(None) raises TypeError inside scifor's own file.
    from scifor.failures import _cap

    try:
        _cap(None)
    except TypeError as e:
        failure = run_failure(e, fn_name="f")
    assert failure.blame == "internal"
    assert failure.to_dict()["blame"] == "internal"


def test_marker_is_read_from_the_exception_not_its_context():
    # A bug raised while handling an ActionableError is still a bug.
    from scifor.failures import _cap

    try:
        try:
            raise ActionableError("refused")
        except ActionableError:
            _cap(None)
    except TypeError as e:
        failure = run_failure(e, fn_name="f")
    assert failure.blame == "internal"


def test_classification_rule_is_logged_at_debug(caplog):
    def broken(value):
        raise KeyError("x")

    with caplog.at_level(logging.DEBUG, logger="scifor"):
        _only_group(broken)
    assert any("blame=user_code via innermost non-library frame is user code" in r.getMessage()
               for r in caplog.records)


# ---------------------------------------------------------------------------
# Wire
# ---------------------------------------------------------------------------


def test_blame_survives_groups_from_wire():
    def broken(value):
        raise KeyError("x")

    group = _only_group(broken)
    (back,) = groups_from_wire([group])
    assert back["blame"] == "user_code"
    assert back["blame_label"] == group["blame_label"]
    assert back["example"]["blame"] == "user_code"


def test_wire_without_blame_gets_the_frame_rule():
    frame = {"file": "/p/f.m", "line": 4, "function": "f", "code": "", "origin": "user"}
    base = {"reason": "E: m", "fn_name": "f", "count": 1, "combos": [], "example": {}}
    (with_frame,) = groups_from_wire([{**base, "user_frame": frame}])
    (without,) = groups_from_wire([{**base, "user_frame": []}])
    (no_data,) = groups_from_wire([{**base, "no_data": True}])
    assert with_frame["blame"] == "user_code"
    assert without["blame"] == "internal"
    assert no_data["blame"] == "no_data"


# ---------------------------------------------------------------------------
# The user-value seam
# ---------------------------------------------------------------------------


def test_describe_value_kinds_names_the_combos():
    msg = describe_value_kinds(
        [("subject=1", pd.DataFrame()), ("subject=2", pd.DataFrame()), ("subject=3", np.zeros(2))],
        fn_name="clean",
        output="Out",
    )
    assert "DataFrame for 2 combo(s) (subject=1; subject=2)" in msg
    assert "ndarray (1-D) for 1 combo(s) (subject=3)" in msg
    assert describe_value_kinds([("a", 1.0), ("b", 2.0)], fn_name="f") is None


def test_seam_passes_actionable_errors_through():
    err = ActionableError("already says what to do")
    assert as_user_value_error(err, stage="save", fn_name="f") is err


def test_seam_with_a_diagnosis_is_precise():
    with pytest.raises(UserValueError) as info:
        with user_value_seam("combine_outputs", fn_name="f", diagnose=lambda: "kinds differ"):
            raise TypeError("cannot concatenate")
    assert str(info.value) == "kinds differ"
    assert info.value.probable is False
    assert isinstance(info.value.__cause__, TypeError)


def test_seam_without_a_diagnosis_is_probable_and_logs_fallback(caplog):
    def user_fn():
        pass

    with caplog.at_level(logging.WARNING, logger="scifor"):
        with pytest.raises(UserValueError) as info:
            with user_value_seam("combine_outputs", fn_name="user_fn", fn=user_fn):
                raise TypeError("cannot concatenate")
    assert info.value.probable is True
    assert "SciStack couldn't combine the outputs of user_fn (defined at" in str(info.value)
    assert any("[seam_fallback] stage=combine_outputs fn=user_fn" in r.getMessage()
               for r in caplog.records)


def test_for_each_combine_failure_names_mixed_kinds(monkeypatch):
    set_schema(["subject"])

    calls = []

    def mixed(value):
        calls.append(value)
        return pd.DataFrame({"a": [1.0]}) if len(calls) == 1 else np.zeros(3)

    def boom(*args, **kwargs):
        raise TypeError("cannot combine")

    monkeypatch.setattr(scifor_foreach, "_results_to_output_dataframe", boom)
    with pytest.raises(UserValueError) as info:
        for_each(mixed, inputs={"value": _df(2)}, subject=[1, 2])
    assert "DataFrame for 1 combo(s) (subject=1)" in str(info.value)
    assert "ndarray (1-D) for 1 combo(s) (subject=2)" in str(info.value)
    assert run_failure(info.value, fn_name="mixed").blame == "user_value"


def test_combine_step_is_inside_the_seam():
    """Every call of the combine step in for_each sits inside a
    `with user_value_seam(...)` block, so a failure there can never be
    reported as SciStack's fault without the user-value check."""
    src = pathlib.Path(scifor_foreach.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    (fe,) = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "for_each"]

    def in_seam(node, ancestors):
        for a in ancestors:
            if isinstance(a, ast.With) and any(
                isinstance(i.context_expr, ast.Call)
                and getattr(i.context_expr.func, "id", "") == "user_value_seam"
                for i in a.items
            ):
                return True
        return False

    calls = []

    def walk(node, ancestors):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "_results_to_output_dataframe":
            calls.append(in_seam(node, ancestors))
        for child in ast.iter_child_nodes(node):
            walk(child, ancestors + [node])

    walk(fe, [])
    assert calls and all(calls)


# ---------------------------------------------------------------------------
# The run's journal block
# ---------------------------------------------------------------------------


def test_run_level_error_reaches_the_journal_with_its_blame(monkeypatch, tmp_path):
    set_schema(["subject"])
    seen = []
    monkeypatch.setattr(Log, "journal", classmethod(lambda cls, ev: seen.append(ev) or ""))

    def boom(*args, **kwargs):
        raise TypeError("cannot combine")

    monkeypatch.setattr(scifor_foreach, "_results_to_output_dataframe", boom)
    with pytest.raises(UserValueError):
        for_each(lambda value: 1.0, inputs={"value": _df(2)}, subject=[1, 2])
    (ev,) = seen
    assert ev["kind"] == "run"
    assert ev["completed"] == 2
    assert ev["error"]["blame"] == "user_value"
    assert ev["error"]["probable"] is True


def test_one_block_per_call_and_none_for_dry_run(monkeypatch):
    set_schema(["subject"])
    seen = []
    monkeypatch.setattr(Log, "journal", classmethod(lambda cls, ev: seen.append(ev) or ""))

    def broken(value):
        raise KeyError("x")

    for_each(broken, inputs={"value": _df(2)}, subject=[1, 2])
    for_each(broken, inputs={"value": _df(2)}, subject=[1, 2], dry_run=True)
    (ev,) = seen
    assert ev["name"] == "broken"
    assert ev["failed"] == 2
    (group,) = ev["failures"]
    assert group["blame"] == "user_code"


def test_failure_group_wire_carries_the_label():
    frame_failure = run_failure(ActionableError("x"), fn_name="f")
    wire = FailureGroup(example=frame_failure, combos=["s=1"]).to_dict()
    assert {"blame", "probable", "blame_label"} <= set(wire)
    assert classify_blame(ActionableError("x"), [])[0] == "setup"
