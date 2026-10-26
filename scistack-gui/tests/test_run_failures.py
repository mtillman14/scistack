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
