"""A whole project through a .scistack bundle (portability Stage 4).

Export: the GUI section (every visible pipeline's canvas + global GUI state)
and the plots section, through scidb.bundle. Import: a NEW project, opened
without discovery, with every pipeline keeping its id -- the root canvas
``main`` filled, never forked into "main (imported)".
"""

from __future__ import annotations

import json
import zipfile

from scistack_gui import pipeline_store as ps
from scistack_gui.db import get_db

EXCLUDE_S02 = {"schemaSelection": {"exclude_levels": {"subject": ["S02"]}}}


def _node_by_label(db, pid, label):
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    graph = get_pipeline_graph(db, pid)
    matches = [n for n in graph["nodes"] if n.get("data", {}).get("label") == label]
    assert len(matches) == 1, f"expected one {label!r} node in {pid}, got {matches}"
    return matches[0]["id"]


def _build_source(client):
    from scistack_gui import layout as layout_store

    # Root canvas content.
    client.put("/api/layout/m_in", json={
        "x": 1, "y": 2, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": "main",
    })
    client.put("/api/layout/m_fn", json={
        "x": 30, "y": 2, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": "main",
    })
    client.put("/api/edges/m_e", json={"source": "m_in", "target": "m_fn"})
    client.put("/api/layout/m_fn/config", json={"config": EXCLUDE_S02})

    # A hypothesis tab with its own canvas.
    hyp = client.post("/api/hypotheses", json={"name": "faster gait"})
    assert hyp.status_code == 200, hyp.text
    hid = hyp.json()["pipeline_id"]
    client.put("/api/layout/h_fn", json={
        "x": 5, "y": 5, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": hid,
    })
    ps.update_hypothesis(get_db(), hid, research_question="Does stim speed gait up?")

    layout_store.write_note("variable:RawSignal", "from the force plate")
    return hid


def _export(tmp_path):
    from scidb.bundle import export_project

    from scistack_gui.headless import bundle_providers

    return export_project(tmp_path, get_db(), tmp_path / "out" / "study", providers=bundle_providers())


def test_the_bundle_carries_gui_and_plots_sections(client, tmp_path):
    _build_source(client)
    out = _export(tmp_path)
    with zipfile.ZipFile(out) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        gui = json.loads(zf.read("gui/gui.json"))
    assert "gui" in manifest["sections"]
    names = {p["name"] for p in gui["pipelines"]}
    assert "faster gait" in names
    assert gui["notes"] == {"variable:RawSignal": "from the force plate"}


def test_a_whole_project_imports_into_a_new_project(client, tmp_path):
    from scistack_gui import layout as layout_store
    from scistack_gui.headless import import_project_bundle

    hid = _build_source(client)
    source_pipelines = {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(get_db())}
    out = _export(tmp_path)

    target = tmp_path / "copy"
    report = import_project_bundle(out, target)
    db = get_db()

    assert report.sections["gui"]["pipelines"] == len(source_pipelines)
    assert {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(db)} == source_pipelines
    names = [p["name"] for p in ps.list_all_pipelines(db)]
    assert not any("(imported" in n for n in names), names

    # Root canvas: nodes, settings, position.
    fn = _node_by_label(db, "main", "custom_proc")
    assert ps.get_node_config(db, fn).get("schemaSelection") == EXCLUDE_S02["schemaSelection"]
    var = _node_by_label(db, "main", "RawSignal")
    pos = layout_store.read_positions_by_scope()["main"][var]
    assert (pos["x"], pos["y"]) == (1, 2)

    # The hypothesis tab: its own canvas and its fields.
    _node_by_label(db, hid, "custom_proc")
    hyp = {h["pipeline_id"]: h for h in ps.list_hypotheses(db)}[hid]
    assert hyp["research_question"] == "Does stim speed gait up?"

    assert layout_store.read_notes()["variable:RawSignal"] == "from the force plate"


def test_import_runs_none_of_the_bundles_code(client, tmp_path):
    """The new project's code is not imported while it is being created
    (the bundle has not been trusted)."""
    import sys

    from scistack_gui.headless import import_project_bundle

    _build_source(client)
    out = _export(tmp_path)
    target = tmp_path / "copy"
    before = set(sys.modules)

    import_project_bundle(out, target)

    new = [
        m for m in set(sys.modules) - before
        if str(target) in str(getattr(sys.modules[m], "__file__", "") or "")
    ]
    assert new == [], f"import loaded code from the new project: {new}"


# ---------------------------------------------------------------------------
# Stage 5b: the code travels as a COPY (source files), never imported
# ---------------------------------------------------------------------------


def _project_on_disk(tmp_path):
    import textwrap

    root = tmp_path / "src_proj"
    pkg = root / "src" / "gaitpkg"
    pkg.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname = "gaitpkg"\nversion = "0.1.0"\ndependencies = ["scidb"]\n'
    )
    (pkg / "__init__.py").write_text("")
    (pkg / "steps.py").write_text("def speed(x):\n    return x * 2\n")
    (pkg / "lookup.csv").write_text("a,b\n1,2\n")  # package data travels too
    (root / "analysis.py").write_text("def extra(x):\n    return x\n")
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "shared_fn.py").write_text("def shared_fn(x):\n    return x\n")
    (root / "scistack.toml").write_text(
        textwrap.dedent(
            f"""
            modules = [".", "{(shared / 'shared_fn.py').as_posix()}"]
            entities_file = "src/gaitpkg/scistack_entities.toml"
            """
        )
    )
    return root, shared


def test_the_code_is_copied_byte_for_byte_and_never_imported(tmp_path):
    import sys

    from scistack_gui.headless import (
        export_project_bundle,
        import_project_bundle,
        open_for_import,
    )

    root, shared = _project_on_disk(tmp_path)
    open_for_import(root / "study.duckdb", project=root, schema_keys=["subject"])
    out = export_project_bundle(root / "study.duckdb", tmp_path / "bundle")

    target = tmp_path / "copy"
    before = set(sys.modules)
    report = import_project_bundle(out, target)

    for rel in (
        "pyproject.toml",
        "src/gaitpkg/__init__.py",
        "src/gaitpkg/steps.py",
        "src/gaitpkg/lookup.csv",
        "src/gaitpkg/scistack_entities.toml",
        "analysis.py",
    ):
        assert (target / rel).read_bytes() == (root / rel).read_bytes(), rel
    assert not (target / "shared_fn.py").exists()
    assert any("shared_fn.py" in p for p in report.sections["code"]["external"])
    assert ".scistack-external.json" not in {p.name for p in target.rglob("*")}
    new = [
        m for m in set(sys.modules) - before
        if str(target) in str(getattr(sys.modules[m], "__file__", "") or "")
    ]
    assert new == [], f"import loaded code from the new project: {new}"
    # The environment check compared the declared dependencies.
    assert "missing" in report.sections["env"]
