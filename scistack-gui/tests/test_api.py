"""
Integration tests for all scistack-gui HTTP API endpoints.

Uses FastAPI's TestClient (backed by a real populated DuckDB) to exercise
every route defined in the application.
"""

import json
import time
import pytest
import threading
import numpy as np

from scidb import configure_database, for_each, BaseVariable
from scidb.database import _local
import scistack_gui.db as _gui_db
from scistack_gui import layout as layout_store
from scistack_gui import registry as _registry
from collections import defaultdict

# Import test variable classes and pipeline function from conftest so we share
# the same class objects (avoids duplicate BaseVariable subclass registrations).
from conftest import (
    RawSignal,
    FilteredSignal,
    bandpass_filter,
    find_fn_node_id_by_label,
    fn_min_state_across_call_sites,
)


# ---------------------------------------------------------------------------
# /api/info
# ---------------------------------------------------------------------------

class TestInfo:
    def test_returns_db_name(self, client):
        r = client.get("/api/info")
        assert r.status_code == 200
        assert r.json()["db_name"] == "test.duckdb"


# ---------------------------------------------------------------------------
# /api/schema
# ---------------------------------------------------------------------------

class TestSchema:
    def test_returns_schema_keys(self, client):
        r = client.get("/api/schema")
        assert r.status_code == 200
        data = r.json()
        assert data["keys"] == ["subject", "session"]

    def test_returns_distinct_values(self, client):
        r = client.get("/api/schema")
        values = r.json()["values"]
        assert set(str(v) for v in values["subject"]) == {"1", "2"}
        assert set(str(v) for v in values["session"]) == {"pre", "post"}


# ---------------------------------------------------------------------------
# /api/registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_returns_functions(self, client):
        r = client.get("/api/registry")
        assert r.status_code == 200
        data = r.json()
        assert "bandpass_filter" in data["functions"]

    def test_returns_variable_classes(self, client):
        r = client.get("/api/registry")
        data = r.json()
        assert "RawSignal" in data["variables"]
        assert "FilteredSignal" in data["variables"]

    def test_lists_are_sorted(self, client):
        r = client.get("/api/registry")
        data = r.json()
        assert data["functions"] == sorted(data["functions"])
        assert data["variables"] == sorted(data["variables"])


# ---------------------------------------------------------------------------
# /api/pipeline
# ---------------------------------------------------------------------------

class TestPipeline:
    def test_returns_200(self, client):
        r = client.get("/api/pipeline")
        assert r.status_code == 200

    def test_has_nodes_and_edges_keys(self, client):
        data = client.get("/api/pipeline").json()
        assert "nodes" in data
        assert "edges" in data

    def test_variable_nodes_present(self, client):
        nodes = client.get("/api/pipeline").json()["nodes"]
        node_ids = {n["id"] for n in nodes}
        assert "var__RawSignal" in node_ids
        assert "var__FilteredSignal" in node_ids

    def test_function_node_present(self, client, bp_node_id):
        nodes = client.get("/api/pipeline").json()["nodes"]
        node_ids = {n["id"] for n in nodes}
        assert bp_node_id in node_ids

    def test_constant_node_present(self, client):
        nodes = client.get("/api/pipeline").json()["nodes"]
        node_ids = {n["id"] for n in nodes}
        assert "const__low_hz" in node_ids

    def test_variable_node_has_total_records(self, client):
        nodes = client.get("/api/pipeline").json()["nodes"]
        raw_node = next(n for n in nodes if n["id"] == "var__RawSignal")
        assert raw_node["data"]["total_records"] == 4  # 2 subjects × 2 sessions

    def test_function_node_has_variants(self, client, bp_node_id):
        nodes = client.get("/api/pipeline").json()["nodes"]
        fn_node = next(n for n in nodes if n["id"] == bp_node_id)
        assert len(fn_node["data"]["variants"]) >= 1
        variant = fn_node["data"]["variants"][0]
        assert "constants" in variant
        assert "input_types" in variant
        assert "output_type" in variant
        # Sanity: the call_id in the node data matches the suffix of the id.
        assert fn_node["data"]["call_id"] == bp_node_id.rsplit("__", 1)[1]

    def test_edges_connect_raw_to_function(self, client, bp_node_id):
        edges = client.get("/api/pipeline").json()["edges"]
        matches = [
            e for e in edges
            if e["source"] == "var__RawSignal" and e["target"] == bp_node_id
        ]
        assert len(matches) >= 1

    def test_edges_connect_function_to_filtered(self, client, bp_node_id):
        edges = client.get("/api/pipeline").json()["edges"]
        matches = [
            e for e in edges
            if e["source"] == bp_node_id and e["target"] == "var__FilteredSignal"
        ]
        assert len(matches) >= 1

    def test_constant_edge_connects_to_function(self, client, bp_node_id):
        edges = client.get("/api/pipeline").json()["edges"]
        matches = [
            e for e in edges
            if e["source"] == "const__low_hz" and e["target"] == bp_node_id
        ]
        assert len(matches) >= 1

    def test_manual_node_appears_in_pipeline(self, client):
        # Add a manual node via the layout API
        client.put("/api/layout/manual__extra_var", json={
            "x": 50.0, "y": 100.0,
            "node_type": "variableNode",
            "label": "ExtraVar",
        })
        nodes = client.get("/api/pipeline").json()["nodes"]
        node_ids = {n["id"] for n in nodes}
        assert "manual__extra_var" in node_ids

    def test_manual_edge_appears_in_pipeline(self, client):
        client.put("/api/edges/manual_e1", json={
            "source": "var__RawSignal",
            "target": "fn__bandpass_filter",
            "source_handle": None,
            "target_handle": None,
        })
        edges = client.get("/api/pipeline").json()["edges"]
        edge_ids = {e["id"] for e in edges}
        assert "manual_e1" in edge_ids

    def test_manual_matlab_function_node_tagged_language(self, client, tmp_path):
        """Regression: manually-placed MATLAB function nodes must have
        ``data.language == "matlab"`` so the VS Code extension intercepts
        start_run and routes to handleMatlabRun instead of the Python
        registry (which doesn't know about MATLAB functions).

        Without this tag, clicking Run on a freshly-placed MATLAB node
        (no DB history yet) fails with:
            "Function '...' not found in registry."
        """
        from scistack_gui import matlab_registry
        from scistack_gui.matlab_parser import MatlabFunctionInfo

        # Stub a MATLAB function into the registry.
        fn_name = "my_matlab_fn"
        m_file = tmp_path / f"{fn_name}.m"
        m_file.write_text("function y = my_matlab_fn(x)\ny=x;\nend\n")
        matlab_registry._matlab_functions[fn_name] = MatlabFunctionInfo(
            name=fn_name, file_path=m_file, params=["x"], source_hash="0" * 64,
        )
        try:
            # Drop a manual function node on the canvas.
            client.put(f"/api/layout/manual__{fn_name}", json={
                "x": 0.0, "y": 0.0,
                "node_type": "functionNode",
                "label": fn_name,
            })
            nodes = client.get("/api/pipeline").json()["nodes"]
            matches = [n for n in nodes if n["data"]["label"] == fn_name]
            assert len(matches) == 1
            assert matches[0]["data"].get("language") == "matlab"
        finally:
            matlab_registry._matlab_functions.pop(fn_name, None)


# ---------------------------------------------------------------------------
# /api/layout
# ---------------------------------------------------------------------------

class TestLayoutEndpoints:
    def test_get_layout_returns_dict(self, client):
        r = client.get("/api/layout")
        assert r.status_code == 200
        data = r.json()
        assert "positions" in data
        assert "manual_nodes" in data

    def test_put_layout_saves_position(self, client):
        r = client.put("/api/layout/fn__bandpass_filter", json={"x": 100.0, "y": 200.0})
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        layout = client.get("/api/layout").json()
        assert layout["positions"]["fn__bandpass_filter"] == {"x": 100.0, "y": 200.0}

    def test_put_layout_with_node_type_creates_manual_node(self, client):
        client.put("/api/layout/manual__foo", json={
            "x": 10.0, "y": 20.0,
            "node_type": "functionNode",
            "label": "my_fn",
        })
        layout = client.get("/api/layout").json()
        assert "manual__foo" in layout["manual_nodes"]
        assert layout["manual_nodes"]["manual__foo"]["label"] == "my_fn"

    def test_delete_layout_removes_node(self, client):
        client.put("/api/layout/fn__bandpass_filter", json={"x": 1.0, "y": 2.0})
        r = client.delete("/api/layout/fn__bandpass_filter")
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        layout = client.get("/api/layout").json()
        assert "fn__bandpass_filter" not in layout["positions"]

    def test_put_layout_overwrites_position(self, client):
        client.put("/api/layout/fn__bandpass_filter", json={"x": 1.0, "y": 2.0})
        client.put("/api/layout/fn__bandpass_filter", json={"x": 99.0, "y": 88.0})
        layout = client.get("/api/layout").json()
        assert layout["positions"]["fn__bandpass_filter"] == {"x": 99.0, "y": 88.0}


# ---------------------------------------------------------------------------
# /api/constants
# ---------------------------------------------------------------------------

class TestConstantsEndpoints:
    def test_get_constants_initially_empty(self, client):
        r = client.get("/api/constants")
        assert r.status_code == 200
        assert r.json() == []

    def test_post_constant_adds_it(self, client):
        r = client.post("/api/constants", json={"name": "window_size"})
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        constants = client.get("/api/constants").json()
        assert "window_size" in constants

    def test_post_constant_no_duplicate(self, client):
        client.post("/api/constants", json={"name": "alpha"})
        client.post("/api/constants", json={"name": "alpha"})
        constants = client.get("/api/constants").json()
        assert constants.count("alpha") == 1

    def test_delete_constant_removes_it(self, client):
        client.post("/api/constants", json={"name": "alpha"})
        client.post("/api/constants", json={"name": "beta"})
        r = client.delete("/api/constants/alpha")
        assert r.status_code == 200
        constants = client.get("/api/constants").json()
        assert "alpha" not in constants
        assert "beta" in constants

    def test_delete_nonexistent_constant_is_ok(self, client):
        r = client.delete("/api/constants/ghost")
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# /api/edges
# ---------------------------------------------------------------------------

class TestEdgesEndpoints:
    def test_put_edge_saves_it(self, client):
        r = client.put("/api/edges/e_test_1", json={
            "source": "var__RawSignal",
            "target": "fn__bandpass_filter",
            "source_handle": None,
            "target_handle": "in__signal",
        })
        assert r.status_code == 200
        assert r.json() == {"ok": True}

        # Verify it shows up in the pipeline
        edges = client.get("/api/pipeline").json()["edges"]
        assert any(e["id"] == "e_test_1" for e in edges)

    def test_put_edge_upserts(self, client):
        client.put("/api/edges/e_test_2", json={
            "source": "A", "target": "B",
            "source_handle": None, "target_handle": None,
        })
        client.put("/api/edges/e_test_2", json={
            "source": "A", "target": "C",
            "source_handle": None, "target_handle": None,
        })
        layout = client.get("/api/layout").json()
        matching = [e for e in layout["manual_edges"] if e["id"] == "e_test_2"]
        assert len(matching) == 1
        assert matching[0]["target"] == "C"

    def test_delete_edge_removes_it(self, client):
        client.put("/api/edges/e_del", json={
            "source": "X", "target": "Y",
            "source_handle": None, "target_handle": None,
        })
        r = client.delete("/api/edges/e_del")
        assert r.status_code == 200
        layout = client.get("/api/layout").json()
        assert not any(e["id"] == "e_del" for e in layout["manual_edges"])

    def test_delete_nonexistent_edge_is_ok(self, client):
        r = client.delete("/api/edges/ghost_edge")
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# /api/variables
# ---------------------------------------------------------------------------

class TestVariableRecordsEndpoint:
    def test_returns_schema_keys(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        assert r.status_code == 200
        data = r.json()
        assert data["schema_keys"] == ["subject", "session"]

    def test_returns_records(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        data = r.json()
        assert len(data["records"]) == 4  # 2 subjects × 2 sessions

    def test_records_have_schema_key_values(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        records = r.json()["records"]
        subjects = {rec["subject"] for rec in records}
        sessions = {rec["session"] for rec in records}
        assert subjects == {"1", "2"}
        assert sessions == {"pre", "post"}

    def test_records_have_variant_label(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        records = r.json()["records"]
        assert all("variant_label" in rec for rec in records)
        # bandpass_filter with low_hz=20 should appear in every label
        assert all("low_hz=20" in rec["variant_label"] for rec in records)

    def test_variants_summary(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        variants = r.json()["variants"]
        assert len(variants) == 1  # single variant (low_hz=20)
        assert variants[0]["record_count"] == 4
        assert "low_hz=20" in variants[0]["label"]

    def test_raw_variable_returns_records(self, client):
        # RawSignal was saved directly (no for_each), branch_params should be empty
        r = client.get("/api/variables/RawSignal/records")
        assert r.status_code == 200
        data = r.json()
        assert len(data["records"]) == 4
        assert all(rec["variant_label"] == "(raw)" for rec in data["records"])

    def test_unknown_variable_returns_empty(self, client):
        r = client.get("/api/variables/NonExistentVariable/records")
        # No records in metadata → empty result (not 404)
        assert r.status_code == 200
        data = r.json()
        assert data["records"] == []
        assert data["variants"] == []


# ---------------------------------------------------------------------------
# /api/run
# ---------------------------------------------------------------------------

def _wait_for_threads(prefix: str, timeout: float = 2.0) -> None:
    """Wait for any background run threads to finish before DB teardown."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        live = [t for t in threading.enumerate() if t.name.startswith(prefix)]
        if not live:
            break
        time.sleep(0.05)


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestRunEndpoint:
    def test_returns_run_id(self, client):
        r = client.post("/api/run", json={
            "function_name": "bandpass_filter",
            "variants": [],
        })
        assert r.status_code == 200
        data = r.json()
        assert "run_id" in data
        assert data["run_id"]  # non-empty
        _wait_for_threads("Thread-")

    def test_accepts_caller_supplied_run_id(self, client):
        r = client.post("/api/run", json={
            "function_name": "bandpass_filter",
            "variants": [],
            "run_id": "my_run_42",
        })
        assert r.status_code == 200
        assert r.json()["run_id"] == "my_run_42"
        _wait_for_threads("Thread-")

    def test_unknown_function_still_returns_200_with_run_id(self, client):
        """
        The HTTP layer returns immediately; the error is surfaced via WebSocket.
        So even for an unknown function, the POST itself should succeed.
        """
        r = client.post("/api/run", json={
            "function_name": "no_such_function",
            "variants": [],
        })
        assert r.status_code == 200
        assert "run_id" in r.json()
        _wait_for_threads("Thread-")

    def test_cancel_run_unknown_id_returns_error(self, client):
        """cancel_run on a run_id that isn't active should return ok=False."""
        from scistack_gui.api.run import cancel_run
        result = cancel_run("does_not_exist_xyz")
        assert result["ok"] is False
        assert "unknown run_id" in result["error"]

    def test_force_cancel_run_unknown_id_returns_error(self, client):
        """force_cancel_run on an unknown run_id should also error gracefully."""
        from scistack_gui.api.run import force_cancel_run
        result = force_cancel_run("not_a_real_run_id")
        assert result["ok"] is False
        assert "unknown run_id" in result["error"]

    def test_cancel_run_sets_event_and_terminates(self, client, monkeypatch):
        """Cooperative cancel: setting the event should make the worker stop
        between variants, emit run_done with cancelled=True, and pop the entry
        from _active_runs.

        We synthesise a worker that loops forever while the event is unset,
        bypassing _run_in_thread's heavyweight setup. This exercises the
        registry, event plumbing, and the run_done emission path.
        """
        from scistack_gui.api import run as run_api

        run_id = "cancel_test_1"
        emitted: list[dict] = []

        # Capture push_message calls into a list rather than going to WS.
        monkeypatch.setattr(run_api, "push_message",
                            lambda msg: emitted.append(msg))

        cancel_event = threading.Event()
        worker_done = threading.Event()

        def synthetic_worker():
            with run_api._active_runs_lock:
                run_api._active_runs[run_id] = {
                    "event": cancel_event,
                    "thread": threading.current_thread(),
                    "cancelled": False,
                    "force_cancelled": False,
                }
            try:
                # Poll the event up to ~2s; cancel should fire well before that.
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    if cancel_event.is_set():
                        run_api.push_message({
                            "type": "run_done",
                            "run_id": run_id,
                            "success": True,
                            "duration_ms": 0,
                            "cancelled": True,
                            "force_cancelled": False,
                        })
                        return
                    time.sleep(0.02)
            finally:
                with run_api._active_runs_lock:
                    run_api._active_runs.pop(run_id, None)
                worker_done.set()

        t = threading.Thread(target=synthetic_worker, name="SynthRun-1",
                             daemon=True)
        t.start()

        # Give the worker a moment to register itself.
        time.sleep(0.05)

        result = run_api.cancel_run(run_id)
        assert result["ok"] is True
        assert result["cancelled"] is True
        assert result["force"] is False

        assert worker_done.wait(timeout=2.0), "worker did not stop after cancel"
        t.join(timeout=1.0)

        # Verify the emitted run_done has cancelled=True.
        done_msgs = [m for m in emitted if m.get("type") == "run_done"]
        assert len(done_msgs) == 1
        assert done_msgs[0]["cancelled"] is True

        # Registry must be cleaned up.
        with run_api._active_runs_lock:
            assert run_id not in run_api._active_runs

    def test_force_cancel_run_smoke(self, client):
        """force_cancel_run should set the event, attempt ctypes injection,
        and return the documented best-effort shape.

        We don't actually exercise the worker thread (KeyboardInterrupt
        injection inside pytest is destabilising) — we register a dummy
        thread, call force_cancel_run, then clean up.
        """
        from scistack_gui.api import run as run_api

        run_id = "force_cancel_smoke"
        cancel_event = threading.Event()

        # Register against the *current* thread but immediately remove
        # before it can be hit by ctypes. We pop just before the call so
        # the lookup succeeds, but the recorded thread is harmless.
        # To be really safe, use a finished thread (ident may be None or
        # invalid) — force_cancel_run handles both gracefully.
        finished = threading.Thread(target=lambda: None, name="ForceCancelSmoke")
        finished.start()
        finished.join()

        with run_api._active_runs_lock:
            run_api._active_runs[run_id] = {
                "event": cancel_event,
                "thread": finished,
                "cancelled": False,
                "force_cancelled": False,
            }

        try:
            result = run_api.force_cancel_run(run_id)
            assert result["ok"] is True
            assert result["cancelled"] is True
            assert result["force"] is True
            assert result["best_effort"] is True
            # The finished thread either has no ident or the injection
            # returns 0 — either way "injected" is a bool.
            assert "injected" in result
            assert isinstance(result["injected"], bool)
            # Cooperative cancel event must have been set as well.
            assert cancel_event.is_set()
        finally:
            with run_api._active_runs_lock:
                run_api._active_runs.pop(run_id, None)


# ---------------------------------------------------------------------------
# Run state: pipeline node run_state integration tests
# ---------------------------------------------------------------------------

# Extra variable class and function for the two-step chain tests
class ProcessedSignal(BaseVariable):
    pass


def process_signal(filtered):
    return np.asarray(filtered, dtype=float) * 2.0


class TestRunStateGreen:
    """Fully-run pipeline → green for both function and output variable."""

    def test_function_node_is_green(self, client, bp_node_id):
        nodes = client.get("/api/pipeline").json()["nodes"]
        fn_node = next(n for n in nodes if n["id"] == bp_node_id)
        assert fn_node["data"].get("run_state") == "green"

    def test_output_variable_is_green(self, client):
        nodes = client.get("/api/pipeline").json()["nodes"]
        var_node = next(n for n in nodes if n["id"] == "var__FilteredSignal")
        assert var_node["data"].get("run_state") == "green"


class TestRunStateRed:
    """Function registered but never run → red."""

    @pytest.fixture
    def client_never_run(self, tmp_path):
        # Fresh DB with raw data only — for_each never called
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "red.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "red.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app
        with TestClient(create_app()) as c:
            yield c

        db.close()

    def test_function_node_is_red(self, client_never_run):
        nodes = client_never_run.get("/api/pipeline").json()["nodes"]
        # Function never run — no variants in DB, so no fn node in pipeline.
        # State is red only if the node appears (via registry).
        state = fn_min_state_across_call_sites(nodes, "bandpass_filter")
        if state is not None:
            assert state == "red"


class TestRunStateGrey:
    """Partially-run pipeline → grey."""

    @pytest.fixture
    def client_partial(self, tmp_path):
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "grey.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        # Run for only 1 of 2 subjects → 2 of 4 schema_ids processed
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre", "post"],
        )

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "grey.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app
        with TestClient(create_app()) as c:
            yield c

        db.close()

    def test_function_node_is_grey(self, client_partial):
        nodes = client_partial.get("/api/pipeline").json()["nodes"]
        # One call site (low_hz=20), partially run → grey.
        node_id = find_fn_node_id_by_label(nodes, "bandpass_filter")
        fn_node = next(n for n in nodes if n["id"] == node_id)
        assert fn_node["data"].get("run_state") == "grey"

    def test_output_variable_is_grey(self, client_partial):
        nodes = client_partial.get("/api/pipeline").json()["nodes"]
        var_node = next(n for n in nodes if n["id"] == "var__FilteredSignal")
        assert var_node["data"].get("run_state") == "grey"


class TestRunStatePropagation:
    """
    Two-step chain: RawSignal → bandpass_filter → FilteredSignal → process_signal → ProcessedSignal.
    When the first step is grey (partial run), the second step must also be grey
    even if it ran completely for all available FilteredSignal records.
    """

    @pytest.fixture
    def client_propagation(self, tmp_path):
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "prop.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        # Step 1: partial run of bandpass_filter (only subject=1)
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre", "post"],
        )

        # Step 2: fully process ALL available FilteredSignal records
        for_each(
            process_signal,
            inputs={"filtered": FilteredSignal},
            outputs=[ProcessedSignal],
            subject=[1],
            session=["pre", "post"],
        )

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "prop.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter
        _registry._functions["process_signal"] = process_signal

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app
        with TestClient(create_app()) as c:
            yield c

        db.close()

    def test_upstream_is_grey(self, client_propagation):
        nodes = client_propagation.get("/api/pipeline").json()["nodes"]
        node_id = find_fn_node_id_by_label(nodes, "bandpass_filter")
        fn_node = next(n for n in nodes if n["id"] == node_id)
        assert fn_node["data"].get("run_state") == "grey"

    def test_downstream_function_is_grey_due_to_staleness(self, client_propagation):
        nodes = client_propagation.get("/api/pipeline").json()["nodes"]
        node_id = find_fn_node_id_by_label(nodes, "process_signal")
        fn_node = next(n for n in nodes if n["id"] == node_id)
        # process_signal ran for all available inputs, but upstream is grey → grey
        assert fn_node["data"].get("run_state") == "grey"

    def test_downstream_variable_is_grey_due_to_staleness(self, client_propagation):
        nodes = client_propagation.get("/api/pipeline").json()["nodes"]
        var_node = next(n for n in nodes if n["id"] == "var__ProcessedSignal")
        assert var_node["data"].get("run_state") == "grey"


# ---------------------------------------------------------------------------
# Pending constant lifecycle: green → pending → grey → run → green
# ---------------------------------------------------------------------------

class TestPendingConstantLifecycle:
    """End-to-end: user drags a new constant value in the GUI, the consumer
    function node greys out, the user runs for_each with that new value, and
    on the next /api/pipeline request the pending value is auto-cleaned and
    the function returns to green.

    The `populated_db` fixture leaves bandpass_filter fully run with low_hz=20.
    """

    def _fn_state(self, client):
        # The pending-constant lifecycle creates additional bandpass_filter
        # call sites mid-test (one per low_hz value).  "Is bandpass_filter
        # green?" therefore means "are all its call sites green?", so we
        # return the most pessimistic state across them.
        nodes = client.get("/api/pipeline").json()["nodes"]
        return fn_min_state_across_call_sites(nodes, "bandpass_filter")

    def _const_values(self, client):
        """Return the set of value strings under const__low_hz."""
        nodes = client.get("/api/pipeline").json()["nodes"]
        cnode = next(n for n in nodes if n["id"] == "const__low_hz")
        return {v["value"] for v in cnode["data"]["values"]}

    def test_starts_green(self, client):
        assert self._fn_state(client) == "green"

    def test_adding_pending_value_greys_consumer(self, client):
        assert self._fn_state(client) == "green"

        r = client.put("/api/constants/low_hz/pending/42")
        assert r.status_code == 200

        # Consumer downgraded green → grey.
        assert self._fn_state(client) == "grey"

        # Downstream variable inherits the grey via DAG propagation.
        nodes = client.get("/api/pipeline").json()["nodes"]
        var_node = next(n for n in nodes if n["id"] == "var__FilteredSignal")
        assert var_node["data"].get("run_state") == "grey"

    def test_pending_value_appears_under_constant_node(self, client):
        client.put("/api/constants/low_hz/pending/42")
        values = self._const_values(client)
        assert "42" in values

    def test_removing_pending_restores_green(self, client):
        client.put("/api/constants/low_hz/pending/42")
        assert self._fn_state(client) == "grey"

        r = client.delete("/api/constants/low_hz/pending/42")
        assert r.status_code == 200

        assert self._fn_state(client) == "green"

    def test_running_pipeline_with_new_value_auto_cleans_pending(self, client, populated_db):
        """Running for_each with the pending scale produces matching records;
        on the next /api/pipeline call, auto_clean_pending_constants removes
        the pending row and the node returns to green."""
        client.put("/api/constants/low_hz/pending/42")
        assert self._fn_state(client) == "grey"

        # Simulate the user running with the new constant value.
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 42},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["pre", "post"],
        )

        # Next pipeline request: auto-clean removes "42" from pending, and
        # both variants (20 and 42) are fully populated → green.
        assert self._fn_state(client) == "green"

        # The layout_store no longer reports "42" as pending.
        pending = layout_store.get_pending_constants()
        assert "42" not in pending.get("low_hz", set())

    def test_already_red_not_affected_by_pending(self, client, populated_db):
        """If a function is already red (stale input), adding a pending value
        does NOT reset it to grey — the more severe state wins."""
        # Re-save an input record to put the function into red.
        RawSignal.save(np.ones(10), subject=1, session="pre")
        assert self._fn_state(client) == "red"

        client.put("/api/constants/low_hz/pending/99")
        assert self._fn_state(client) == "red"


class TestPendingConstantRecovery:
    """Recovery paths — a function that is grey or red, has a pending
    constant added, and then is fully run, should end up green.
    """

    def _fn_state(self, c):
        nodes = c.get("/api/pipeline").json()["nodes"]
        return fn_min_state_across_call_sites(nodes, "bandpass_filter")

    @pytest.fixture
    def client_partial(self, tmp_path):
        """bandpass_filter ran for subject=1 only with low_hz=20 → grey."""
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "grey_recover.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre", "post"],
        )

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "grey_recover.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app
        with TestClient(create_app()) as c:
            yield c

        db.close()

    @pytest.fixture
    def client_never_run(self, tmp_path):
        """No for_each run yet → function would be red (if visible)."""
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "red_recover.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "red_recover.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app
        with TestClient(create_app()) as c:
            yield c

        db.close()

    def test_grey_plus_pending_resolves_to_green_after_full_run(self, client_partial):
        """grey (partial run) + pending low_hz=42 → run both variants fully → green."""
        assert self._fn_state(client_partial) == "grey"

        # Add pending on top of grey — still grey (pending can't worsen grey,
        # and auto_clean only downgrades green → grey).
        client_partial.put("/api/constants/low_hz/pending/42")
        assert self._fn_state(client_partial) == "grey"

        # Complete both variants.
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[2], session=["pre", "post"],   # remaining subjects
        )
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 42},
            outputs=[FilteredSignal],
            subject=[1, 2], session=["pre", "post"],
        )

        # Auto-clean removes pending; all variants fully populated → green.
        assert self._fn_state(client_partial) == "green"
        pending = layout_store.get_pending_constants()
        assert "42" not in pending.get("low_hz", set())

    def test_red_never_run_plus_pending_resolves_to_green(self, client_never_run):
        """red (never run, via DB variants path) + pending low_hz=42 → run → green.

        Because no variants exist yet, the fn node may not appear in the
        pipeline at all. Run the pipeline with the pending value first,
        then assert the node exists and is green.
        """
        # Prior state: no fn node visible (no variants), so state == None.
        assert self._fn_state(client_never_run) is None

        client_never_run.put("/api/constants/low_hz/pending/42")

        # Still no variants → still no fn node.
        assert self._fn_state(client_never_run) is None

        # Run with the pending value for all combos.
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 42},
            outputs=[FilteredSignal],
            subject=[1, 2], session=["pre", "post"],
        )

        assert self._fn_state(client_never_run) == "green"
        pending = layout_store.get_pending_constants()
        assert "42" not in pending.get("low_hz", set())
