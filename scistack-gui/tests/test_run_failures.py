"""GUI side of the failure records (services.run_failures) and on_error.

Pins:
- run_done.failures carries the failed groups (no-data groups dropped), each
  tagged with the clicked node; the console names the user frame instead of
  "see log above"
- combo_skip relays the small `failure` form
- on_error from run_options reaches for_each; a stop marks run_done.stopped
- a run-level exception (not a stop) becomes a stage="run" group
- on_error_from_config: the one reader; unknown values fall back, logged
- code export emits on_error only when it is not the default
See docs/claude/error-surfacing.md.
"""

import logging
import threading
import time

import pytest

from scifor.failures import StoppedRun, attach_stopped_run, failure_from_exception
from scistack_gui.services.run_failures import RunFailures, on_error_from_config


def _wait_for_threads(prefix: str = "Thread-", timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not [t for t in threading.enumerate() if t.name.startswith(prefix)]:
            return
        time.sleep(0.05)


def _group(reason="KeyError: 'chan'", *, no_data=False, count=2, line=12):
    return {
        "reason": reason,
        "fn_name": "bandpass_filter",
        "exc_type": reason.split(":")[0],
        "message": reason.split(": ", 1)[1],
        "no_data": no_data,
        "count": count,
        "combos": [f"subject={i}" for i in range(1, count + 1)],
        "user_frame": None
        if no_data
        else {"file": "/proj/filters.py", "line": line, "function": "bandpass_filter",
              "code": "x = d['chan']", "origin": "user"},
        "example": {},
    }


def _fake_for_each(groups, *, raise_exc=None, seen=None):
    def fake(*args, **kwargs):
        if seen is not None:
            seen.append(kwargs)
        progress = kwargs["_progress_fn"]
        failed = sum(g["count"] for g in groups if not g["no_data"])
        no_data = sum(g["count"] for g in groups if g["no_data"])
        progress({
            "event": "combo_skip", "current": 1, "total": 3, "completed": 0,
            "skipped": 1, "metadata": {"subject": 1},
            "failure": {"reason": groups[0]["reason"], "no_data": False,
                        "user_frame": groups[0]["user_frame"]} if groups else None,
        })
        progress({
            "event": "summary", "current": 3, "total": 3, "completed": 1,
            "failed": failed, "no_data": no_data, "skipped": failed + no_data,
            "cancelled": False, "stopped": raise_exc is not None, "failures": groups,
        })
        if raise_exc is not None:
            raise raise_exc
        return None

    return fake


def _run(client, bp_node_id, monkeypatch, fake, run_options=None):
    from scistack_gui.api import run as run_api

    emitted: list[dict] = []
    monkeypatch.setattr(run_api, "push_message", lambda msg: emitted.append(msg))
    monkeypatch.setattr(run_api, "for_each", fake)
    body = {"function_name": "bandpass_filter", "variants": [], "node_id": bp_node_id}
    if run_options is not None:
        body["run_options"] = run_options
    r = client.post("/api/run", json=body)
    assert r.status_code == 200
    _wait_for_threads()
    (done,) = [m for m in emitted if m.get("type") == "run_done"]
    text = "".join(m.get("text", "") for m in emitted if m.get("type") == "run_output")
    return done, text, emitted


def test_run_done_carries_failed_groups_tagged_with_the_node(client, bp_node_id, monkeypatch):
    groups = [_group(), _group("NoDataError: No data", no_data=True, count=1)]
    done, text, emitted = _run(client, bp_node_id, monkeypatch, _fake_for_each(groups))

    assert done["success"] is False and done["stopped"] is False
    # Every covered node is listed; the no-data group is not a failure.
    assert list(done["failures"]["by_node"]) == [bp_node_id]
    (g,) = done["failures"]["by_node"][bp_node_id]
    assert done["failures"]["other"] == []
    assert g["node_id"] == bp_node_id
    assert g["reason"] == "KeyError: 'chan'" and g["count"] == 2
    assert "at /proj/filters.py:12 in bandpass_filter" in text
    assert "see log above" not in text

    (skip,) = [m for m in emitted if m.get("type") == "run_progress" and m["event"] == "combo_skip"]
    assert skip["failure"]["reason"] == "KeyError: 'chan'"


def test_on_error_reaches_for_each_and_stop_marks_run_done(client, bp_node_id, monkeypatch):
    seen: list[dict] = []
    exc = KeyError("chan")
    failure = failure_from_exception(exc, fn_name="bandpass_filter", metadata={"subject": 1}, stage="call")
    attach_stopped_run(exc, StoppedRun(failure=failure, partial_result=None, completed=1))

    done, text, _ = _run(
        client, bp_node_id, monkeypatch,
        _fake_for_each([_group(count=1)], raise_exc=exc, seen=seen),
        run_options={"on_error": "stop"},
    )

    assert seen and all(k["on_error"] == "stop" for k in seen)
    assert done["stopped"] is True and done["success"] is False
    # The stop is the summary's group, not a second one.
    assert len(done["failures"]["by_node"][bp_node_id]) == 1
    assert "Stopped at the first failure" in text


def test_default_on_error_is_continue(client, bp_node_id, monkeypatch):
    seen: list[dict] = []
    done, _, _ = _run(client, bp_node_id, monkeypatch, _fake_for_each([], seen=seen))
    # A clean run still names its node, so stale badges clear.
    assert done["failures"] == {"by_node": {bp_node_id: []}, "other": []}
    assert seen and all(k["on_error"] == "continue" for k in seen)


def test_run_level_exception_becomes_a_run_stage_group(client, bp_node_id, monkeypatch):
    done, text, _ = _run(
        client, bp_node_id, monkeypatch,
        _fake_for_each([], raise_exc=RuntimeError("save exploded")),
    )
    (g,) = done["failures"]["by_node"][bp_node_id]
    assert g["exc_type"] == "RuntimeError" and g["example"]["stage"] == "run"
    assert g["node_id"] == bp_node_id
    assert "save exploded" in text


def test_on_error_from_config_reads_and_falls_back(caplog):
    assert on_error_from_config(None) == "continue"
    assert on_error_from_config({}) == "continue"
    assert on_error_from_config({"on_error": "stop"}) == "stop"
    with caplog.at_level(logging.WARNING):
        assert on_error_from_config({"on_error": "explode"}) == "continue"
    assert "explode" in caplog.text


def test_collector_drops_no_data_and_renders_console():
    rf = RunFailures("r1")
    text = rf.add_summary(
        [_group(count=7), _group("NoDataError: none", no_data=True)],
        node_id="fn__x", fn_name="x", target="x", completed=3,
    )
    assert rf.failed_combos == 7
    wire = rf.to_wire(["fn__x", "fn__clean"])
    assert [g["node_id"] for g in wire["by_node"]["fn__x"]] == ["fn__x"]
    assert wire["by_node"]["fn__clean"] == []  # covered, ran clean
    assert "7 combo(s) failed (3 completed)" in text
    assert "(+4 more)" in text
    assert rf.add_summary([_group("NoDataError: none", no_data=True)],
                          node_id=None, fn_name="x", target="x", completed=0) == ""


@pytest.mark.parametrize("value, emitted", [("continue", False), ("stop", True)])
def test_python_export_emits_on_error_only_when_set(client, bp_node_id, value, emitted):
    """A node's on_error reaches the compiled pipeline AND the exported
    script (export matches the GUI run)."""
    from scistack_gui import pipeline_store
    from scistack_gui.db import get_db
    from scistack_gui.services.code_export_service import export_pipeline_to_code

    db = get_db()
    pipeline_store.update_node_config(db, bp_node_id, {"runOptions": {"on_error": value}})

    script = export_pipeline_to_code(db, "main")["script"]

    assert ("on_error='stop'" in script) is emitted


def test_compiled_pipeline_tags_steps_with_their_node(client, bp_node_id):
    from scistack_gui import pipeline_store
    from scistack_gui.db import get_db
    from scistack_gui.services.execution_service import (
        _discard_compiled,
        build_backend_pipeline,
    )

    db = get_db()
    pipeline_store.update_node_config(db, bp_node_id, {"runOptions": {"on_error": "stop"}})
    built: dict = {}
    try:
        pipe = build_backend_pipeline(db, "main", built)
        specs = [s for s in pipe.steps if s.name == "bandpass_filter"]
    finally:
        _discard_compiled(built)

    assert specs and all(s.tag == bp_node_id for s in specs)
    assert all(s.options["on_error"] == "stop" for s in specs)


# ---------------------------------------------------------------------------
# MATLAB: generator, .done marker, watcher (Stage 5)
# ---------------------------------------------------------------------------


def test_matlab_run_option_pairs_emit_on_error_only_when_stop(caplog):
    from scistack_gui.api.matlab_command import _format_run_option_pairs

    assert "'on_error', 'stop'" in _format_run_option_pairs({"on_error": "stop"}, "f")
    assert _format_run_option_pairs({"on_error": "continue"}, "f") == ""
    with caplog.at_level(logging.WARNING):
        _format_run_option_pairs({"on_error": "stop"}, "f")
    assert "not recognised" not in caplog.text  # on_error is not an unknown key


def test_matlab_failure_marker_carries_the_tally_failures():
    from scistack_gui.api.matlab_command import _run_marker_finish_lines

    lines = "\n".join(_run_marker_finish_lines("/tmp/x/study.duckdb", "r1", ok=False))
    assert "scidb.internal.iteration_tally('failures')" in lines


def test_done_marker_failures_reach_run_done_on_the_node(tmp_path, monkeypatch):
    """The exact JSON +scidb/run_marker.m writes (a spliced "failures" array,
    jsonencode's 1-element-array-as-object quirk included)."""
    import time as _time

    import scistack_gui.notify as notify_mod
    from scidb import run_markers
    from scistack_gui.matlab_run_watch import TrackedRun, _finish

    seen: list = []
    monkeypatch.setattr(notify_mod, "push_message", seen.append)
    monkeypatch.setattr("scistack_gui.api.run._notify_records_changed", lambda: None)

    markers = run_markers.marker_dir(tmp_path / "study.duckdb")
    markers.mkdir()
    (markers / "r1.done").write_text(
        '{"version":1,"run_id":"r1","ok":false,"interrupted":false,'
        '"identifier":"SciStack:iterationsFailed","message":"f: 1 of 2 failed","at":1.0,'
        '"failures":{"reason":"MATLAB:badsubscript: Index exceeds","fn_name":"f",'
        '"exc_type":"MATLAB:badsubscript","message":"Index exceeds","no_data":false,'
        '"count":1,"combos":"subject=2","user_frame":{"file":"/p/f.m","line":7,'
        '"function":"f","code":"","origin":"user"},"example":{"frames":'
        '{"file":"/p/f.m","line":7,"function":"f","code":"","origin":"user"}}}}\n',
        encoding="utf-8",
    )
    done = run_markers.read_done(markers, "r1")
    assert done is not None and len(done.failures) == 1

    run = TrackedRun(
        run_id="r1", db_path=str(tmp_path / "study.duckdb"), markers=str(markers),
        label="f", dispatched_at=_time.time(), node_id="fn__f",
    )
    _finish(run, success=False, error=done.message, unknown=False, failures=done.failures)

    (msg,) = [m for m in seen if m.get("type") == "run_done"]
    (g,) = msg["failures"]["by_node"]["fn__f"]
    assert g["user_frame"]["line"] == 7 and g["combos"] == ["subject=2"]
    text = "".join(m.get("text", "") for m in seen if m.get("type") == "run_output")
    assert "at /p/f.m:7 in f" in text


def test_a_save_refused_for_duplicate_locations_carries_its_remedy():
    """DuplicateLocationError inside OutputSaveError: run_done's group has
    the remedy the canvas opens as a popup (docs/claude/duplicate-location-vs-stale.md)."""
    from scidb import DuplicateLocationError, OutputSaveError

    cause = DuplicateLocationError(
        "Demo",
        [{"location": {"subject": "2455369"}, "count": 2, "differing_columns": ["age"]}],
        ["subject"],
    )
    exc = OutputSaveError("read_table", ["Demo: 2 record(s) not saved"], 0, causes=[cause])
    rf = RunFailures("r1")
    rf.add_exception(exc, node_id="fn__read", fn_name="read_table", target="read_table")
    (group,) = rf.to_wire(["fn__read"])["by_node"]["fn__read"]
    assert group["remedy"]["kind"] == "duplicate_location"
    assert group["remedy"]["items"][0]["location"] == "subject=2455369"
    assert group["blame"] == "setup"
