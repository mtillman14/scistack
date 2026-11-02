"""
Tests for whole-pipeline MATLAB execution routing
(plan-matlab-pipeline-execution.md, Stages 1-4):

- matlab_command_service.generate_matlab_pipeline_command (Stage 1's
  service-layer wrapper — DB-backed resolution feeding the pure-string
  generator already unit-tested in test_matlab.py).
- execution_service.pipeline_has_matlab_steps + start_pipeline_run's
  MATLAB routing, both the VS Code/JSON-RPC signal path (Stage 2) and the
  standalone-sidecar path (Stage 3; MatlabSidecar itself is unit-tested
  separately in test_matlab_sidecar.py — here it's mocked at the
  get_sidecar() seam to verify start_pipeline_run wires it correctly).
- start_matlab_sidecar_run (Stage 4's fallback-ladder Tier 3, called from
  dagPanel.ts when the MathWorks terminal isn't available).

No real MATLAB environment in this sandbox — matlab_registry is just a
plain dict, so a fake registration exercises the real resolution code
paths without needing one (same approach as test_code_export.py).
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from scidb import BaseVariable
from scistack_gui.matlab import registry as matlab_registry
from scistack_gui.runtime.db import get_db
from scistack_gui.matlab.parser import MatlabFunctionInfo


def _wait_for_threads(prefix: str, timeout: float = 2.0) -> None:
    """Wait for any background run threads to finish before DB teardown
    (mirrors test_api.py's helper — a MATLAB-routed run must NOT spawn one
    of these; a Python-only run still does)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        live = [t for t in threading.enumerate() if t.name.startswith(prefix)]
        if not live:
            break
        time.sleep(0.05)


class MatlabOutput(BaseVariable):
    pass


def _register_fake_matlab_fn(name: str, params: list[str]) -> None:
    matlab_registry._matlab_functions[name] = MatlabFunctionInfo(
        name=name, file_path=Path(f"{name}.m"), params=params, source_hash="deadbeef",
    )


def _build_matlab_only_scope(client, pid: str, fn_name: str = "matlab_proc") -> None:
    """Mirrors test_code_export.py's TestMatlabCodeExport setup: a fresh
    scope with one MATLAB function node, wired and given a real target
    (constant edge + pending value) so derive_target_for_node resolves it."""
    client.put("/api/layout/mv_in", json={
        "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": pid,
    })
    client.put(f"/api/layout/mf_a", json={
        "x": 10, "y": 0, "node_type": "functionNode", "label": fn_name, "pipeline_id": pid,
    })
    client.put("/api/layout/mv_out", json={
        "x": 20, "y": 0, "node_type": "variableNode", "label": "MatlabOutput", "pipeline_id": pid,
    })
    client.put("/api/edges/e_in", json={
        "source": "mv_in", "target": "mf_a", "target_handle": "in__signal",
    })
    client.put("/api/edges/e_out", json={"source": "mf_a", "target": "mv_out"})
    client.put("/api/layout/mc_gain", json={
        "x": 5, "y": 5, "node_type": "parameterNode", "label": "gain", "pipeline_id": pid,
    })
    client.put("/api/edges/e_gain", json={
        "source": "mc_gain", "target": "mf_a", "target_handle": "in__gain",
    })
    client.put("/api/parameters/gain/pending/2.5")


class TestPipelineHasMatlabSteps:
    def test_pure_python_scope_is_false(self, client):
        from scistack_gui.services.execution_service import pipeline_has_matlab_steps

        assert pipeline_has_matlab_steps(get_db(), "main") is False

    def test_matlab_scope_is_true(self, client):
        from scistack_gui.services.execution_service import pipeline_has_matlab_steps

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            pid = client.post(
                "/api/pipelines", json={"name": "matlab_only"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, pid)
            assert pipeline_has_matlab_steps(get_db(), pid) is True
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)

    def test_used_child_pipeline_with_matlab_step_detected(self, client):
        """A MATLAB step buried in a `uses`-composed child scope must still
        be detected — build_backend_pipeline recursively compiles used
        pipelines too, so the routing check must match that closure."""
        from scistack_gui.services.execution_service import pipeline_has_matlab_steps

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            child_pid = client.post(
                "/api/pipelines", json={"name": "matlab_child"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, child_pid)

            parent_pid = client.post(
                "/api/pipelines", json={"name": "parent"}
            ).json()["pipeline_id"]
            r = client.post(
                f"/api/pipelines/{parent_pid}/uses",
                json={"child_pipeline_id": child_pid},
            )
            assert r.status_code == 200

            assert pipeline_has_matlab_steps(get_db(), parent_pid) is True
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestStartPipelineRunMatlabRouting:
    def test_matlab_pipeline_returns_host_execution_required(self, client):
        from scistack_gui.services.run_service import start_pipeline_run

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            pid = client.post(
                "/api/pipelines", json={"name": "matlab_only"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, pid)

            result = start_pipeline_run(
                pid, mode="all", run_id="rid_matlab", host_can_dispatch_matlab=True
            )
            assert result == {
                "run_id": "rid_matlab",
                "host_execution_required": True,
                "language": "matlab",
            }
            # No background thread spawned for the MATLAB-routed path.
            live = [t for t in threading.enumerate() if t.name.startswith("Thread-")]
            assert live == []
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)

    def test_matlab_pipeline_show_mode_rejected(self, client):
        from scistack_gui.services.run_service import start_pipeline_run

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            pid = client.post(
                "/api/pipelines", json={"name": "matlab_only"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, pid)

            with pytest.raises(ValueError, match="show"):
                start_pipeline_run(pid, mode="show", target="matlab_proc")
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)

    def test_pure_python_pipeline_unaffected(self, client):
        """The existing Python pipeline-run path (main, conftest-seeded
        bandpass_filter) must still spawn its normal background thread —
        this routing change must not regress it."""
        from scistack_gui.services.run_service import start_pipeline_run

        result = start_pipeline_run("main", mode="all", run_id="rid_python")
        assert result == {"run_id": "rid_python"}
        _wait_for_threads("Thread-")


class TestGenerateMatlabPipelineCommandService:
    """matlab_command_service.generate_matlab_pipeline_command — the
    DB-backed resolution layer feeding matlab_command_format's pure string
    generator (already unit-tested directly in test_matlab.py)."""

    def test_resolves_matlab_scope_into_pipeline_wrapped_script(self, client):
        from scistack_gui.services.matlab_command_service import (
            generate_matlab_pipeline_command,
        )

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            pid = client.post(
                "/api/pipelines", json={"name": "matlab_only"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, pid)

            result = generate_matlab_pipeline_command(pid, get_db(), {"mode": "all"})
            assert result["warnings"] == []
            cmd = result["command"]
            assert f"pipe = scidb.Pipeline('{pid}');" in cmd
            assert "scidb.for_each(@matlab_proc, " in cmd
            assert "'signal', RawSignal()" in cmd
            assert "'gain', 2.5" in cmd
            assert "{MatlabOutput()}" in cmd
            assert "pipe.run_all(" in cmd
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)

    def test_python_nodes_in_scope_reported_as_warnings(self, client):
        """A Python function node co-scoped with a MATLAB one must be
        excluded from the script (not silently mis-registered) and
        surfaced back to the caller as a warning."""
        from scistack_gui.services.matlab_command_service import (
            generate_matlab_pipeline_command,
        )

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            pid = client.post(
                "/api/pipelines", json={"name": "mixed"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, pid)
            # bandpass_filter is registered as a real Python function by
            # conftest — add it to the same scope, wired independently.
            client.put("/api/layout/pv_in", json={
                "x": 0, "y": 40, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": pid,
            })
            client.put("/api/layout/pf_a", json={
                "x": 10, "y": 40, "node_type": "functionNode", "label": "bandpass_filter", "pipeline_id": pid,
            })
            client.put("/api/layout/pv_out", json={
                "x": 20, "y": 40, "node_type": "variableNode", "label": "FilteredSignal", "pipeline_id": pid,
            })
            client.put("/api/edges/pe_in", json={
                "source": "pv_in", "target": "pf_a", "target_handle": "in__signal",
            })
            client.put("/api/edges/pe_out", json={"source": "pf_a", "target": "pv_out"})
            client.put("/api/layout/pc_low_hz", json={
                "x": 5, "y": 45, "node_type": "parameterNode", "label": "low_hz", "pipeline_id": pid,
            })
            client.put("/api/edges/pe_low_hz", json={
                "source": "pc_low_hz", "target": "pf_a", "target_handle": "in__low_hz",
            })
            client.put("/api/parameters/low_hz/pending/20")

            result = generate_matlab_pipeline_command(pid, get_db(), {"mode": "all"})
            assert len(result["warnings"]) == 1
            assert "bandpass_filter" in result["warnings"][0]
            cmd = result["command"]
            assert "@matlab_proc" in cmd
            assert "@bandpass_filter" not in cmd
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestStandaloneSidecarRouting:
    """start_pipeline_run's default (host_can_dispatch_matlab=False) path —
    the browser/standalone counterpart to TestStartPipelineRunMatlabRouting
    above, which exercises the VS Code/JSON-RPC host_can_dispatch_matlab=True
    signal path. MatlabSidecar itself is unit-tested in
    test_matlab_sidecar.py; here it's mocked at the get_sidecar() seam to
    verify start_pipeline_run's wiring (script generation -> sidecar ->
    run_output/run_done)."""

    def test_default_routes_through_sidecar_thread(self, client, monkeypatch):
        from scistack_gui.matlab import sidecar as matlab_sidecar
        from scistack_gui.services.run_service import start_pipeline_run

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            pid = client.post(
                "/api/pipelines", json={"name": "matlab_only"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, pid)

            class FakeSidecar:
                def __init__(self):
                    self.started = False
                    self.commands: list[str] = []

                def start(self):
                    self.started = True
                    return True

                def check_health(self, timeout=None):
                    return None  # a configured pyenv

                def run_command(self, command, on_line, timeout=None):
                    self.commands.append(command)
                    on_line("MATLAB output line\n")
                    return True

            fake = FakeSidecar()
            monkeypatch.setattr(matlab_sidecar, "get_sidecar", lambda: fake)

            result = start_pipeline_run(pid, mode="all", run_id="rid_sidecar")
            assert result == {"run_id": "rid_sidecar"}

            _wait_for_threads("Thread-")
            assert fake.started is True
            assert len(fake.commands) == 1
            assert "@matlab_proc" in fake.commands[0]
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)

    def test_matlab_unavailable_reports_error_without_crashing(self, client, monkeypatch):
        from scistack_gui.matlab import sidecar as matlab_sidecar
        from scistack_gui.services.run_service import start_pipeline_run

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            pid = client.post(
                "/api/pipelines", json={"name": "matlab_only"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, pid)

            class UnavailableSidecar:
                def start(self):
                    return False

            monkeypatch.setattr(
                matlab_sidecar, "get_sidecar", lambda: UnavailableSidecar()
            )

            result = start_pipeline_run(pid, mode="all", run_id="rid_unavailable")
            assert result == {"run_id": "rid_unavailable"}
            _wait_for_threads("Thread-")
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)

    def test_host_can_dispatch_matlab_skips_sidecar(self, client, monkeypatch):
        """The VS Code/JSON-RPC path (host_can_dispatch_matlab=True) must
        never touch the sidecar — dagPanel.ts owns dispatch there."""
        from scistack_gui.matlab import sidecar as matlab_sidecar
        from scistack_gui.services.run_service import start_pipeline_run

        _register_fake_matlab_fn("matlab_proc", ["signal", "gain"])
        try:
            pid = client.post(
                "/api/pipelines", json={"name": "matlab_only"}
            ).json()["pipeline_id"]
            _build_matlab_only_scope(client, pid)

            def boom():
                raise AssertionError("get_sidecar should not be called")

            monkeypatch.setattr(matlab_sidecar, "get_sidecar", boom)

            result = start_pipeline_run(
                pid, mode="all", run_id="rid_host", host_can_dispatch_matlab=True
            )
            assert result == {
                "run_id": "rid_host",
                "host_execution_required": True,
                "language": "matlab",
            }
        finally:
            matlab_registry._matlab_functions.pop("matlab_proc", None)


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestStartMatlabSidecarRun:
    """Stage 4's fallback-ladder Tier 3: start_matlab_sidecar_run, called
    from dagPanel.ts's dispatchMatlabCommand when the MathWorks terminal
    isn't available (works for BOTH a single-function command and a
    whole-pipeline command — this function is transport-agnostic, just
    text)."""

    def test_reports_unavailable_without_spawning_a_thread(self, monkeypatch):
        from scistack_gui.matlab import sidecar as matlab_sidecar
        from scistack_gui.services.run_service import start_matlab_sidecar_run

        monkeypatch.setattr(matlab_sidecar, "sidecar_capable", lambda: False)

        result = start_matlab_sidecar_run("disp('hi')", "rid_1")
        assert result == {"run_id": "rid_1", "sidecar_available": False}

        live = [t for t in threading.enumerate() if t.name.startswith("Thread-")]
        assert live == []

    def test_available_spawns_thread_and_drives_sidecar(self, monkeypatch):
        from scistack_gui.matlab import sidecar as matlab_sidecar
        from scistack_gui.services.run_service import start_matlab_sidecar_run

        monkeypatch.setattr(matlab_sidecar, "sidecar_capable", lambda: True)

        class FakeSidecar:
            def __init__(self):
                self.commands: list[str] = []

            def start(self):
                return True

            def check_health(self, timeout=None):
                return None  # a configured pyenv

            def run_command(self, command, on_line, timeout=None):
                self.commands.append(command)
                on_line("MATLAB output\n")
                return True

        fake = FakeSidecar()
        monkeypatch.setattr(matlab_sidecar, "get_sidecar", lambda: fake)

        result = start_matlab_sidecar_run(
            "disp('hi')", "rid_2", warnings=["a python node was excluded"]
        )
        assert result == {"run_id": "rid_2", "sidecar_available": True}

        _wait_for_threads("Thread-")
        assert fake.commands == ["disp('hi')"]

    def test_unhealthy_engine_refuses_before_running_anything(self, monkeypatch):
        """A MATLAB that launches but can't reach Python is a SETUP problem.
        Without the probe it fails on the first py.* call, deep inside
        configure_database, and reads as a pipeline error instead."""
        from scistack_gui.matlab import sidecar as matlab_sidecar
        from scistack_gui.services.run_service import start_matlab_sidecar_run

        monkeypatch.setattr(matlab_sidecar, "sidecar_capable", lambda: True)

        class UnhealthySidecar:
            def __init__(self):
                self.commands: list[str] = []

            def start(self):
                return True

            def check_health(self, timeout=None):
                return "MATLAB started, but its Python bridge (pyenv) is not configured."

            def run_command(self, command, on_line, timeout=None):
                self.commands.append(command)
                return True

        fake = UnhealthySidecar()
        monkeypatch.setattr(matlab_sidecar, "get_sidecar", lambda: fake)

        start_matlab_sidecar_run("disp('hi')", "rid_3")
        _wait_for_threads("Thread-")

        # The run is abandoned before MATLAB is asked to do any work.
        assert fake.commands == []


class TestDatabaseHandoffToMatlab:
    """MATLAB opens the same .duckdb file the GUI does.

    The JSON-RPC server drops its lock between requests, but nothing else
    does — in browser/standalone mode (FastAPI) the connection stays open
    for the life of the process, so a sidecar run started from there would
    find the database locked by *us* and die on its first
    ``scidb.configure_database`` call. ``external_db_access`` hands it
    over for the duration.
    """

    def test_lock_is_released_while_matlab_runs(self, monkeypatch, tmp_path):
        import duckdb
        from scistack_gui.runtime import db as db_mod
        from scistack_gui.matlab import sidecar as matlab_sidecar
        from scistack_gui.services.run_service import start_matlab_sidecar_run

        p = tmp_path / "handoff.duckdb"
        con = duckdb.connect(str(p))
        con.execute("CREATE TABLE _schema (schema_id INTEGER, subject INTEGER)")
        con.close()

        db_mod.init_db(p)
        assert db_mod._db_open is True

        monkeypatch.setattr(matlab_sidecar, "sidecar_capable", lambda: True)
        observed: dict = {}

        class FakeSidecar:
            def start(self):
                return True

            def check_health(self, timeout=None):
                return None

            def run_command(self, command, on_line, timeout=None):
                # This is what MATLAB does first. It must succeed.
                observed["db_open_during_run"] = db_mod._db_open
                try:
                    other = duckdb.connect(str(p))
                    other.close()
                    observed["matlab_could_open"] = True
                except Exception as exc:  # pragma: no cover - failure detail
                    observed["matlab_could_open"] = False
                    observed["error"] = str(exc)
                return True

        monkeypatch.setattr(matlab_sidecar, "get_sidecar", lambda: FakeSidecar())

        try:
            start_matlab_sidecar_run("disp('hi')", "rid_handoff")
            _wait_for_threads("Thread-")

            assert observed["db_open_during_run"] is False
            assert observed["matlab_could_open"] is True, observed.get("error")
            # …and we take it back afterwards.
            assert db_mod._db_open is True
        finally:
            if db_mod._db is not None:
                db_mod._db._duck.close()
            db_mod._db = None
            db_mod._db_path = None
            db_mod._db_open = False
            db_mod._db_refcount = 0
            db_mod._external_holder = None

    def test_concurrent_request_is_refused_not_raced(self):
        """A request arriving mid-run must be told MATLAB has the database,
        not quietly reopen it and steal the lock back from the run we just
        dispatched."""
        from scistack_gui.runtime import db as db_mod

        saved = (db_mod._db, db_mod._db_open, db_mod._db_refcount)
        db_mod._db, db_mod._db_open, db_mod._db_refcount = None, False, 0
        try:
            with db_mod.external_db_access("MATLAB"):
                with pytest.raises(db_mod.DatabaseLockedError) as excinfo:
                    db_mod.acquire_db_connection(timeout=0)
                assert "MATLAB" in str(excinfo.value)
            # Ownership is given back on exit.
            assert db_mod._external_holder is None
            # …and acquiring works again.
            db_mod.acquire_db_connection(timeout=0)
            assert db_mod._db_refcount == 1
        finally:
            db_mod._external_holder = None
            db_mod._db, db_mod._db_open, db_mod._db_refcount = saved
