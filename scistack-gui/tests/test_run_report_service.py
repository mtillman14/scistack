"""The run report in the GUI (scistack_gui.services.run_report_service).

Every count and sentence is scidb's (scidb/tests/test_run_report.py); these
tests pin that the one handler reaches each part, that errors map to HTTP
statuses, and that ``run_done.reports`` names the stored run of a click.
"""

import threading
import time

from scistack_gui.services.run_report_service import reports_wire


def post(client, body):
    return client.post("/api/run-report", json=body)


def _latest(client):
    r = post(client, {"part": "list", "function_name": "bandpass_filter"})
    assert r.status_code == 200, r.text
    return r.json()["runs"]


def test_list_names_the_fixture_run(client):
    (run,) = _latest(client)
    assert run["function_name"] == "bandpass_filter"
    assert run["computed"] == 4 and run["failed"] == 0
    assert run["output_types"] == ["FilteredSignal"]
    assert run["status_line"] == "4 combinations: 4 computed."
    assert run["verdict"] == "ok"


def test_list_filters_by_the_nodes_outputs(client):
    body = {"part": "list", "function_name": "bandpass_filter"}
    assert len(post(client, {**body, "outputs": ["FilteredSignal"]}).json()["runs"]) == 1
    assert post(client, {**body, "outputs": ["Other"]}).json()["runs"] == []


def test_summary_part(client):
    run_id = _latest(client)[0]["run_id"]
    r = post(client, {"part": "summary", "run_id": run_id})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["summary"]["run_id"] == run_id
    assert body["settings"]["constants"] == {"low_hz": ["20"]}
    assert [c["variable"] for c in body["outputs"]] == ["FilteredSignal"]
    assert body["outputs"][0]["records"] == 4
    assert [(c["param"], c["variable"]) for c in body["inputs"]] == [("signal", "RawSignal")]


def test_data_part(client):
    run_id = _latest(client)[0]["run_id"]
    r = post(client, {"part": "data", "run_id": run_id})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total_rows"] == 4 and body["truncated"] is False
    assert body["status_counts"]["computed"] == 4
    assert body["schema_keys"] == ["subject", "session"]
    row = body["rows"][0]
    assert row["inputs"]["signal"]["shape_text"] == "10"
    assert set(row["outputs"]) == {"FilteredSignal"}
    one = post(client, {"part": "data", "run_id": run_id, "limit": 1}).json()
    assert len(one["rows"]) == 1 and one["truncated"] is True


def test_errors_map_to_statuses(client):
    assert post(client, {"part": "summary", "run_id": "nope"}).status_code == 404
    assert post(client, {"part": "bogus"}).status_code == 400
    assert post(client, {"part": "list"}).status_code == 400
    assert post(client, {"part": "data"}).status_code == 400


def test_reports_wire_keeps_run_order_and_drops_unrecorded_steps():
    wire = reports_wire([
        {"node_id": "n1", "function_name": "load", "run_ids": ["a"]},
        {"node_id": "n2", "function_name": "matlab_step", "run_ids": []},
        {"node_id": "n3", "function_name": "filter", "run_ids": ["b", None, "c"]},
        {"node_id": None, "function_name": "loose", "run_ids": None},
    ])
    assert wire == [
        {"node_id": "n1", "function_name": "load", "run_ids": ["a"]},
        {"node_id": "n3", "function_name": "filter", "run_ids": ["b", "c"]},
    ]


def _wait_for_threads(prefix: str = "Thread-", timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not [t for t in threading.enumerate() if t.name.startswith(prefix)]:
            return
        time.sleep(0.05)


def test_run_done_names_the_stored_run(client, bp_node_id, monkeypatch):
    """A real run through the run thread: scidb's ``run_recorded`` event is
    kept off the progress stream and its id lands in run_done.reports."""
    from scistack_gui.services import run_service as run_api

    emitted: list[dict] = []
    monkeypatch.setattr(run_api, "push_message", lambda msg: emitted.append(msg))
    r = client.post("/api/run", json={
        "function_name": "bandpass_filter", "variants": [], "node_id": bp_node_id,
    })
    assert r.status_code == 200, r.text
    _wait_for_threads()
    (done,) = [m for m in emitted if m.get("type") == "run_done"]
    assert not [
        m for m in emitted if m.get("type") == "run_progress" and m["event"] == "run_recorded"
    ]
    (entry,) = done["reports"]
    assert entry["node_id"] == bp_node_id
    assert entry["function_name"] == "bandpass_filter"
    newest = _latest(client)[0]
    assert entry["run_ids"] == [newest["run_id"]]


def test_matlab_runs_since_dispatch_become_reports(client, populated_db):
    """A MATLAB run is only watched from outside, so its stored runs are
    found by time and language (the bridge writes them)."""
    from scidb.provenance.run_summary import RunRecorder
    from scistack_gui.services.run_report_service import matlab_reports_since

    before = time.time() - 1
    assert matlab_reports_since(before, "n1") == []  # the fixture's run is Python

    def record(fn):
        r = RunRecorder(fn, language="matlab")
        r.observe({"event": "summary", "total": 1, "completed": 1, "skipped": 0, "no_data": 0})
        return r.finish(populated_db, output_types=["FilteredSignal"])

    first = record("load_m")
    assert matlab_reports_since(before, "n1") == [
        {"node_id": "n1", "function_name": "load_m", "run_ids": [first]},
    ]
    second = record("filter_m")
    # Two functions: a pipeline run, so no entry claims the node.
    assert matlab_reports_since(before, None) == [
        {"node_id": None, "function_name": "load_m", "run_ids": [first]},
        {"node_id": None, "function_name": "filter_m", "run_ids": [second]},
    ]
    assert matlab_reports_since(time.time() + 60, "n1") == []


def test_matlab_reports_never_raise_without_a_database():
    import scistack_gui.runtime.db as gui_db
    from scistack_gui.services.run_report_service import matlab_reports_since

    saved = gui_db._db
    gui_db._db = None
    try:
        assert matlab_reports_since(time.time(), "n1") == []
    finally:
        gui_db._db = saved
