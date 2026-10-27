"""MATLAB's failure record has Python's shape (runs without MATLAB).

``+scifor/failure_from_mexception.m`` and ``+scifor/for_each.m``'s
``failure_groups`` emit the wire shape of Python's
``scifor.failures.ComboFailure.to_dict`` / ``FailureGroup.to_dict``, so the
GUI reads either language's failures without knowing which ran. This guard
parses the field names out of the .m source and holds them to the Python
keys; ``groups_from_wire`` is pinned on MATLAB ``jsonencode``'s quirks.
See docs/claude/error-surfacing.md.
"""

import re
from pathlib import Path

from scifor.failures import (
    ComboFailure,
    FailureFrame,
    FailureGroup,
    groups_from_wire,
)

_MATLAB = Path(__file__).parent.parent / "src" / "scimatlab" / "matlab"


def _struct_fields(text: str, anchor: str) -> set[str]:
    """Field names of the first ``struct( ... )`` call after *anchor*."""
    start = text.index(anchor)
    body = text[start : text.index(");", start)]
    return set(re.findall(r"'([a-z_]+)', ", body))


def _python_example() -> ComboFailure:
    frame = FailureFrame(file="/p/f.py", line=3, function="f", code="", origin="user")
    return ComboFailure(
        fn_name="f", metadata={}, stage="call", exc_type="E", message="m",
        frames=(frame,), user_frame=frame, no_data=False,
    )


def test_matlab_record_fields_match_python_combo_failure():
    text = (_MATLAB / "+scifor" / "failure_from_mexception.m").read_text()
    matlab = _struct_fields(text, "failure = struct(")
    python = set(_python_example().to_dict())
    # 'combo' is MATLAB's metadata string (its metadata struct is empty).
    assert matlab - {"combo"} == python


def test_matlab_frame_fields_match_python_frame():
    text = (_MATLAB / "+scifor" / "failure_from_mexception.m").read_text()
    matlab = set(re.findall(r"'([a-z_]+)', \{\}", text))
    assert matlab == set(_python_example().frames[0].to_dict())


def test_matlab_group_fields_match_python_group():
    text = (_MATLAB / "+scifor" / "for_each.m").read_text()
    matlab = _struct_fields(text, "groups{k} = struct(")
    python = set(FailureGroup(example=_python_example(), combos=["s=1"]).to_dict())
    assert matlab == python


def test_groups_from_wire_undoes_jsonencode_quirks():
    # jsonencode: a 1-element struct array -> object; [] -> []; a 1-element
    # cellstr -> a bare string.
    raw = {
        "reason": "MATLAB:undefinedFunction: nope",
        "fn_name": "f",
        "exc_type": "MATLAB:undefinedFunction",
        "message": "nope",
        "no_data": False,
        "count": 2,
        "combos": "subject=1",
        "user_frame": [],
        "example": {
            "frames": {"file": "/p/f.m", "line": 4, "function": "f", "code": "", "origin": "user"},
            "user_frame": {"file": "/p/f.m", "line": 4, "function": "f", "code": "", "origin": "user"},
        },
    }
    (g,) = groups_from_wire(raw)
    assert g["combos"] == ["subject=1"]
    assert g["user_frame"] is None
    assert g["example"]["frames"][0]["line"] == 4
    assert g["example"]["user_frame"]["file"] == "/p/f.m"


def test_groups_from_wire_drops_malformed_and_reads_none():
    assert groups_from_wire(None) == []
    assert groups_from_wire([{"no": "reason"}, 3]) == []


def test_python_groups_round_trip_unchanged():
    g = FailureGroup(example=_python_example(), combos=["s=1"]).to_dict()
    (back,) = groups_from_wire([g])
    assert back["user_frame"] == g["user_frame"]
    assert back["example"]["frames"] == g["example"]["frames"]
    assert back["combos"] == ["s=1"]


def test_on_error_is_validated_by_python_in_both_matlab_parsers():
    for rel in ("+scifor/for_each.m", "+scidb/for_each.m"):
        text = (_MATLAB / rel).read_text()
        assert 'case "on_error"' in text
        assert "py.scifor.validate_on_error(opts.on_error)" in text


def test_scidb_defers_the_stop_and_rethrows_after_saving():
    text = (_MATLAB / "+scidb" / "for_each.m").read_text()
    defer = text.index("'_defer_stop'")
    save = text.index("py_result_df = py.scimatlab.bridge.for_each_save(")
    rethrow = text.index("rethrow(stop_err)")
    assert defer < save < rethrow


def test_a_python_save_error_becomes_a_run_level_group_with_its_remedy():
    """scimatlab.bridge.run_failure_group_json — what +scidb/for_each.m's
    note_run_failure adds to iteration_tally when for_each_save raises — has
    the remedy, and survives MATLAB's jsondecode -> jsonencode round trip
    (1-element arrays as bare objects, null as [])."""
    import json

    from scidb import DuplicateLocationError, OutputSaveError
    from scimatlab.bridge import run_failure_group_json

    cause = DuplicateLocationError(
        "Demo",
        [{"location": {"subject": "S1"}, "count": 2, "differing_columns": ["age"]}],
        ["subject"],
    )
    exc = OutputSaveError("load_csv", ["Demo: 2 record(s) not saved"], 0, causes=[cause])
    group = json.loads(run_failure_group_json(exc, "load_csv"))
    assert group["count"] == 1
    assert group["blame"] == "setup"
    assert group["remedy"]["kind"] == "duplicate_location"

    # What MATLAB's jsonencode writes back for a 1-element list / null.
    remedy = dict(group["remedy"])
    remedy["items"] = {**remedy["items"][0], "differing_columns": "age"}
    remedy["steps"] = [{**s, "action": [] if s["action"] is None else s["action"]} for s in remedy["steps"]]
    (back,) = groups_from_wire([{**group, "remedy": remedy, "user_frame": []}])
    assert back["remedy"]["items"] == [
        {"location": "subject=S1", "count": 2, "differing_columns": ["age"]}
    ]
    assert [s["action"] for s in back["remedy"]["steps"]] == [None, "reveal_function"]


def test_run_failure_group_json_ignores_non_exceptions():
    from scimatlab.bridge import run_failure_group_json

    assert run_failure_group_json("not an exception", "f") == ""
