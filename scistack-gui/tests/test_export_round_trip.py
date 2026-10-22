"""GUI → code → GUI: the exported script IS the canvas's pipeline.

plan-canvas-as-document §8a: "no test runs an exported script". This one does:

1. a step is built on the canvas (stored node + edges) and run from the GUI;
2. the canvas is exported to a plain Python script;
3. the script is run in a separate process (the way a user runs it), against
   the same database;
4. the script must record nothing new (same invocations, same records:
   ``0 new rows``), and the next canvas build must adopt nothing (every
   recorded wiring is claimed by the node that stated it).

The GUI run and the export render the SAME derivation (``build_backend_pipeline``
over the stored edges), so any difference between them is a bug in one of
the two renderers.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import numpy as np
import pytest

import scistack_gui.db as _gui_db
from scidb import configure_database
from scistack_gui import pipeline_store
from scistack_gui import registry as _registry

MODULE = "export_round_trip_steps"

SOURCE = textwrap.dedent(
    '''
    import numpy as np

    from scidb import BaseVariable


    class RoundTripRaw(BaseVariable):
        pass


    class RoundTripOut(BaseVariable):
        pass


    def scale_round_trip(signal, k):
        return np.asarray(signal, dtype=float) * float(k)
    '''
)


def _counts(db) -> dict:
    duck = db._duck
    return {
        "records": duck._fetchone("SELECT COUNT(*) FROM _record")[0],
        "invocations": duck._fetchone("SELECT COUNT(*) FROM _invocation")[0],
        "invocation_ids": sorted(r[0] for r in duck._fetchall("SELECT invocation_id FROM _invocation")),
    }


def _wait_for_runs(timeout: float = 30.0) -> None:
    from scistack_gui.api import run as run_api

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with run_api._active_runs_lock:
            busy = bool(run_api._active_runs)
        if not busy:
            return
        time.sleep(0.05)
    raise AssertionError("the GUI run did not finish")


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A one-step project in its own database: module on disk, raw data, a
    stored canvas node wired RoundTripRaw + k -> scale_round_trip -> RoundTripOut."""
    import importlib.util

    from fastapi.testclient import TestClient

    from scistack_gui.app import create_app

    module_path = tmp_path / f"{MODULE}.py"
    module_path.write_text(SOURCE)
    spec = importlib.util.spec_from_file_location(MODULE, module_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[MODULE] = mod
    spec.loader.exec_module(mod)

    db_path = tmp_path / "round_trip.duckdb"
    db = configure_database(db_path, ["subject", "session"])
    for subj in (1, 2):
        for sess in ("pre", "post"):
            mod.RoundTripRaw.save(np.arange(5.0), subject=subj, session=sess)
    _gui_db._db = db
    _gui_db._db_path = db_path
    pipeline_store._ensure_tables(db)
    _registry._functions["scale_round_trip"] = mod.scale_round_trip
    _registry._module_path = module_path

    from scistack_gui import layout as layout_store
    from scistack_gui.ids import in_handle, param_node_id, var_node_id

    fn = "fn__scale_round_trip__a1b2c3"
    layout_store.write_manual_node(fn, 100.0, 0.0, "functionNode", "scale_round_trip")
    layout_store.write_manual_node("var__RoundTripRaw", 0.0, 0.0, "variableNode", "RoundTripRaw")
    layout_store.write_manual_node("var__RoundTripOut", 200.0, 0.0, "variableNode", "RoundTripOut")
    layout_store.write_manual_node("param__k", 0.0, 80.0, "parameterNode", "k")
    pipeline_store.add_pending_constant(db, "k", "3")
    for edge in (
        {"id": "e_sig", "source": str(var_node_id("RoundTripRaw")), "target": fn, "targetHandle": in_handle("signal")},
        {"id": "e_k", "source": str(param_node_id("k")), "target": fn, "targetHandle": in_handle("k")},
        {"id": "e_out", "source": fn, "target": str(var_node_id("RoundTripOut"))},
    ):
        pipeline_store.write_manual_edge(db, edge)

    with TestClient(create_app()) as client:
        yield client, db, db_path, tmp_path, fn
    sys.modules.pop(MODULE, None)


def _function_node(client) -> dict:
    graph = client.get("/api/pipeline").json()
    nodes = [n for n in graph["nodes"] if n["data"].get("label") == "scale_round_trip"]
    assert len(nodes) == 1, [n["id"] for n in nodes]
    return nodes[0]


def test_an_exported_script_reproduces_the_gui_run(project):
    from scistack_gui.services.code_export_service import export_pipeline_to_code

    client, db, db_path, tmp_path, fn = project

    # 1. run from the GUI
    node = _function_node(client)
    r = client.post(
        "/api/run",
        json={"function_name": "scale_round_trip", "variants": [], "node_id": node["id"]},
    )
    assert r.status_code == 200, r.text
    _wait_for_runs()
    # The build after a GUI run gives a hand-placed node an id in the grammar
    # (it claimed history at dispatch), so the node is named after it.
    node = _function_node(client)
    after_gui = _counts(db)
    assert after_gui["records"] > 4, "the GUI run wrote its outputs"

    # 2. export
    result = export_pipeline_to_code(db, "main")
    assert result["warnings"] == []
    script = Path(result["path"])
    assert f"from {MODULE} import *" in result["script"]
    assert "for_each(scale_round_trip" in result["script"]

    # 3. run it as a user would: its own process, the database closed here.
    db.close()
    proc = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)},
    )
    assert proc.returncode == 0, f"the exported script failed:\n{proc.stdout}\n{proc.stderr}"

    # 4. nothing new recorded, nothing new adopted
    db2 = configure_database(db_path, ["subject", "session"])
    _gui_db._db = db2
    try:
        after_script = _counts(db2)
        assert after_script == after_gui, (
            "the exported script recorded new rows: it is not the call the GUI made"
        )
        graph = client.get("/api/pipeline").json()
        assert not [w for w in graph.get("warnings", []) if w.get("kind") == "adopted"], (
            "the script's run was not claimed by the node that states its wiring"
        )
        assert _function_node(client)["id"] == node["id"]
    finally:
        db2.close()
