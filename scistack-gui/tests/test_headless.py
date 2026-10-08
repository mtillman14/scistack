"""Opening a project without the GUI (portability Stage 3).

Export opens WITH discovery (the canvas is built with the code registry);
import opens WITHOUT, so an imported bundle's code never runs before the user
trusts it. Both go through bootstrap.open_or_create_project.
"""

from __future__ import annotations

from pathlib import Path

from scistack_gui import pipeline_store as ps
from scistack_gui.db import get_db

#: A project module with a uniquely named function. Whether it was IMPORTED is
#: read from the process (sys.modules, the registry), not from a side effect
#: at import: discovery's side-effect guard (find_top_level_side_effects)
#: refuses a module whose top level writes a file, so a marker-file tripwire
#: would never fire in either mode and prove nothing.
TRIPWIRE = "def tripwire_fn(x):\n    return x\n"


def _imported(root) -> bool:
    import sys

    from scistack_gui import registry

    path = str((root / "tripwire.py").resolve())
    in_modules = any(
        getattr(m, "__file__", None) and str(Path(m.__file__).resolve()) == path
        for m in list(sys.modules.values())
    )
    return in_modules or registry.lookup_function("tripwire_fn") is not None


def _project_with_tripwire(root):
    root.mkdir(parents=True, exist_ok=True)
    (root / "tripwire.py").write_text(TRIPWIRE)
    (root / "scistack.toml").write_text('modules = ["tripwire.py"]\n')
    return root


def test_open_for_import_imports_no_code(tmp_path):
    from scistack_gui.headless import open_for_import

    root = _project_with_tripwire(tmp_path)
    open_for_import(root / "study.duckdb", schema_keys=["subject"])

    assert not _imported(root), "import-mode open imported project code"
    assert (root / "study.duckdb").exists()


def test_open_for_import_pins_the_project_root(tmp_path):
    from scifor.pathinput import get_project_root

    from scistack_gui.headless import open_for_import

    root = _project_with_tripwire(tmp_path)
    open_for_import(root / "study.duckdb", schema_keys=["subject"])

    assert get_project_root() is not None
    assert get_project_root().resolve() == root.resolve()


def test_open_for_export_discovers(tmp_path):
    """The contrast: export mode imports the project's (own) code."""
    from scistack_gui.headless import open_for_export, open_for_import

    root = _project_with_tripwire(tmp_path)
    open_for_import(root / "study.duckdb", schema_keys=["subject"])
    assert not _imported(root)

    open_for_export(root / "study.duckdb")
    assert _imported(root), "export-mode open did not discover the project's code"


def test_a_canvas_imports_without_discovery_and_reports_what_it_deferred(
    client_with_variable_file, tmp_path
):
    from scistack_gui.headless import open_for_import
    from scistack_gui.services.pipeline_service import get_pipeline_graph
    from scistack_gui.services.portability_service import (
        export_pipeline,
        import_pipeline_document,
    )

    client = client_with_variable_file
    pid = client.post("/api/pipelines", json={"name": "loading"}).json()["pipeline_id"]
    client.put("/api/layout/mv_in", json={
        "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": pid,
    })
    client.put("/api/layout/mf_proc", json={
        "x": 10, "y": 0, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": pid,
    })
    client.put("/api/edges/e_in", json={"source": "mv_in", "target": "mf_proc"})
    client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
    client.put("/api/layout/pi_a", json={
        "x": 0, "y": 0, "node_type": "pathInputNode", "label": "gait_data", "pipeline_id": pid,
    })
    document = export_pipeline(get_db(), pid)

    target_root = _project_with_tripwire(tmp_path / "target")
    target_db = open_for_import(
        target_root / "target.duckdb", project=target_root, schema_keys=["subject"]
    )
    result = import_pipeline_document(target_db, document, discovered=False)

    assert result["ok"] is True
    assert result["unresolved_labels"] is None  # not checked: nothing discovered
    assert result["deferred"]["path_inputs"] == ["gait_data"]
    assert result["materialization_errors"] == []
    labels = {
        n.get("data", {}).get("label")
        for n in get_pipeline_graph(target_db, result["pipeline_id"])["nodes"]
    }
    assert {"RawSignal", "custom_proc", "gait_data"} <= labels
    assert not _imported(target_root), "import imported the target's code"
    assert any(p["pipeline_id"] == result["pipeline_id"] for p in ps.list_pipelines(target_db))


def test_the_gui_endpoint_and_the_headless_call_export_the_same_canvas(client):
    """One function behind both front ends: the HTTP export and a direct
    call produce the same canvas for the same pipeline."""
    from scistack_gui.services.portability_service import export_pipeline

    pid = client.post("/api/pipelines", json={"name": "loading"}).json()["pipeline_id"]
    client.put("/api/layout/mv_in", json={
        "x": 3, "y": 4, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": pid,
    })
    via_http = client.get(f"/api/pipelines/{pid}/export").json()
    direct = export_pipeline(get_db(), pid)

    import json

    def _strip(doc):
        doc = json.loads(json.dumps(doc.get("document", doc), default=str))
        doc.pop("exported_at", None)
        return doc

    assert _strip(via_http)["canvas"] == _strip(direct)["canvas"]
