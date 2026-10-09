"""Stage 8 front ends: ``scistack export/import/bundle-info`` (bundle_cli)
and the GUI's export handler.

One implementation behind both: the GUI's handler and the terminal export
write the same bundle; ``bundle_cli import`` (what the extension runs) and
``headless.import_project_bundle`` make the same project. Every offered
option and default is read from its owner (``ExportOptions``,
``import_project``'s signature), never restated.
"""

from __future__ import annotations

import argparse
import inspect
import json
import zipfile
from pathlib import Path

import pytest

from scistack_gui import pipeline_store as ps
from scistack_gui.db import get_db

from tests.test_bundle_project import _build_source, _export


@pytest.fixture
def pinned_root(tmp_path):
    """scifor's project root = tmp_path, as a GUI session pins it on load
    (the export reads the project's files from there)."""
    from scifor.pathinput import clear_project_root, set_project_root

    set_project_root(tmp_path)
    yield tmp_path
    clear_project_root()


def _parser():
    from scistack_gui.bundle_cli import add_bundle_subparsers

    parser = argparse.ArgumentParser()
    add_bundle_subparsers(parser.add_subparsers(dest="command"))
    return parser


def _bundle_contents(path: Path) -> dict:
    """Every member's bytes, the manifest minus its timestamp."""
    with zipfile.ZipFile(path) as zf:
        out = {n: zf.read(n) for n in zf.namelist() if n != "manifest.json"}
        manifest = json.loads(zf.read("manifest.json"))
    manifest.pop("exported_at", None)
    out["manifest.json"] = manifest
    return out


def _project_shape(db) -> dict:
    """What a user sees: pipelines and, per pipeline, its nodes by label and
    type with their settings, and the edges between labels."""
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    shape = {}
    for p in sorted(ps.list_pipelines(db), key=lambda p: p["pipeline_id"]):
        graph = get_pipeline_graph(db, p["pipeline_id"])
        label = {n["id"]: (n["type"], n.get("data", {}).get("label")) for n in graph["nodes"]}
        shape[p["pipeline_id"]] = {
            "name": p["name"],
            "nodes": sorted(
                (t, lbl, json.dumps(ps.get_node_config(db, nid) or {}, sort_keys=True))
                for nid, (t, lbl) in label.items()
            ),
            "edges": sorted(
                (label[e["source"]][1], label[e["target"]][1]) for e in graph["edges"]
                if e["source"] in label and e["target"] in label
            ),
        }
    return shape


def _files_under(root: Path) -> dict:
    """Relative path -> bytes for the project's own files: not the
    database, its log or lock, and not the layout file, which holds the node
    ids a snapshot import mints afresh (compared by label in _project_shape)."""
    skip = {".duckdb", ".wal", ".log", ".lock"}
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file()
        and p.suffix not in skip
        and not p.name.endswith(".layout.json")
        and ".scistack" not in p.relative_to(root).parts
    }


# ---------------------------------------------------------------------------
# Defaults come from their owners
# ---------------------------------------------------------------------------


def test_export_flags_default_to_export_options():
    from scidb.bundle import ExportOptions

    args = _parser().parse_args(["export"])
    defaults = ExportOptions()
    assert args.include_history is defaults.include_history
    assert args.include_data is defaults.include_data

    flipped = _parser().parse_args(["export", "--data", "--no-history"])
    assert flipped.include_data is True and flipped.include_history is False


def test_import_history_defaults_to_import_projects_default():
    from scidb.bundle import import_project

    default = inspect.signature(import_project).parameters["import_history"].default
    assert _parser().parse_args(["import", "b.scistack"]).import_history is default
    assert _parser().parse_args(["import", "b.scistack", "--no-history"]).import_history is False


def test_the_gui_offers_exactly_the_cli_options(client):
    from scistack_gui.bundle_cli import EXPORT_FLAGS, export_choices

    r = client.get("/api/bundles/export-options")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["choices"] == export_choices()
    assert [c["name"] for c in body["choices"]] == list(EXPORT_FLAGS)
    assert body["default_name"].endswith(body["extension"])


def test_pairs_parse_and_refuse_bad_input():
    from scistack_gui.bundle_cli import BundleCLIError, _parse_pairs

    assert _parse_pairs(["subject=participant", "session="], "--map", empty_is_none=True) == {
        "subject": "participant", "session": None,
    }
    with pytest.raises(BundleCLIError, match="NAME=VALUE"):
        _parse_pairs(["subject"], "--map", empty_is_none=True)
    with pytest.raises(BundleCLIError, match="twice"):
        _parse_pairs(["a=1", "a=2"], "--path-root", empty_is_none=False)
    with pytest.raises(BundleCLIError, match="no value"):
        _parse_pairs(["RAW="], "--path-root", empty_is_none=False)


# ---------------------------------------------------------------------------
# Export: the GUI handler and the terminal write the same bundle
# ---------------------------------------------------------------------------


def test_the_gui_handler_and_headless_export_write_the_same_bundle(client, pinned_root):
    from scistack_gui.headless import export_open_project

    _build_source(client)
    out_gui = pinned_root / "gui" / "study.scistack"
    r = client.post("/api/bundles/export", json={"out_path": str(out_gui), "options": None})
    assert r.status_code == 200, r.text
    assert r.json()["path"] == str(out_gui)
    assert r.json()["bytes"] == out_gui.stat().st_size

    out_headless = export_open_project(pinned_root / "cli" / "study.scistack")
    assert _bundle_contents(out_gui) == _bundle_contents(out_headless)


def test_the_gui_handler_honours_options_and_refuses_unknown_ones(client, pinned_root):
    _build_source(client)
    out = pinned_root / "nohist.scistack"
    r = client.post("/api/bundles/export", json={
        "out_path": str(out), "options": {"include_history": False},
    })
    assert r.status_code == 200, r.text
    with zipfile.ZipFile(out) as zf:
        sections = json.loads(zf.read("manifest.json"))["sections"]
    assert "history" not in sections

    bad = client.post("/api/bundles/export", json={
        "out_path": str(pinned_root / "x.scistack"), "options": {"include_everything": True},
    })
    assert bad.status_code == 400 and "unknown export option" in bad.text
    rel = client.post("/api/bundles/export", json={"out_path": "relative.scistack"})
    assert rel.status_code == 400 and "absolute" in rel.text


# ---------------------------------------------------------------------------
# Import: the CLI (what the extension runs) and headless make the same project
# ---------------------------------------------------------------------------


def test_cli_import_and_headless_import_make_the_same_project(client, tmp_path, capsys):
    from scistack_gui.bundle_cli import main
    from scistack_gui.headless import import_project_bundle

    _build_source(client)
    out = _export(tmp_path)

    # The SAME folder name under two parents: a bundle with no package name
    # takes it from the folder (init_project), so different names would make
    # different projects for a reason that has nothing to do with the route.
    via_cli = tmp_path / "cli" / "study"
    via_headless = tmp_path / "headless" / "study"

    capsys.readouterr()
    assert main(["import", str(out), "--into", str(via_cli), "--json"]) == 0
    answer = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert answer["ok"] is True and answer["check_code"] is None
    cli_shape = _project_shape(get_db())
    cli_report = answer["report"]

    report = import_project_bundle(out, via_headless)
    headless_shape = _project_shape(get_db())

    assert cli_shape == headless_shape
    cli_files, headless_files = _files_under(via_cli), _files_under(via_headless)
    assert sorted(cli_files) == sorted(headless_files)
    differing = {
        rel: (cli_files[rel].decode(errors="replace"), headless_files[rel].decode(errors="replace"))
        for rel in cli_files if cli_files[rel] != headless_files[rel]
    }
    assert differing == {}, f"files differ between the two imports: {differing}"
    # The reports agree once each project's own folder is named the same.
    as_json = json.dumps(report.to_dict()).replace(str(via_headless), "<root>")
    assert json.loads(json.dumps(cli_report).replace(str(via_cli), "<root>")) == (
        json.loads(as_json)
    )


def test_cli_import_errors_are_one_json_line(client, tmp_path, capsys):
    from scistack_gui.bundle_cli import main

    _build_source(client)
    out = _export(tmp_path)
    target = tmp_path / "taken"
    assert main(["import", str(out), "--into", str(target), "--json"]) == 0
    capsys.readouterr()

    assert main(["import", str(out), "--into", str(target), "--json"]) == 1
    answer = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert answer["ok"] is False and "NEW project" in answer["error"]


def test_check_code_needs_trust_and_writes_nothing_without_it(client, tmp_path, capsys, monkeypatch):
    from scistack_gui.bundle_cli import main

    _build_source(client)
    out = _export(tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    target = tmp_path / "untrusted"
    capsys.readouterr()
    assert main(["import", str(out), "--into", str(target), "--check-code", "--json"]) == 1
    answer = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "--trust" in answer["error"]
    assert not target.exists()


def test_check_code_lists_canvas_nodes_whose_code_is_missing(client, tmp_path, capsys):
    """The source canvas names ``custom_proc``, which no code defines: the
    trusted check (discovery on) reports it."""
    from scistack_gui.bundle_cli import main

    _build_source(client)
    out = _export(tmp_path)
    capsys.readouterr()
    assert main([
        "import", str(out), "--into", str(tmp_path / "checked"), "--check-code", "--trust", "--json",
    ]) == 0
    answer = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "custom_proc" in answer["check_code"]["unresolved_labels"]


def test_bundle_info_needs_no_project_and_runs_no_code(client, tmp_path, capsys):
    import sys

    from scistack_gui.bundle_cli import main

    _build_source(client)
    out = _export(tmp_path)
    before = set(sys.modules)
    capsys.readouterr()
    assert main(["bundle-info", str(out), "--json"]) == 0
    info = json.loads(capsys.readouterr().out.strip().splitlines()[-1])["info"]
    assert info["schema_keys"] == list(get_db().dataset_schema_keys)
    assert "gui" in info["sections"]
    new = [m for m in set(sys.modules) - before
           if str(tmp_path) in str(getattr(sys.modules[m], "__file__", "") or "")]
    assert new == []


def test_the_scistack_cli_mounts_the_same_commands():
    """``scistack export/import/bundle-info`` are bundle_cli's parsers."""
    pytest.importorskip("scistack")
    from scistack.__main__ import main

    with pytest.raises(SystemExit) as e:
        main(["import", "--help"])
    assert e.value.code == 0
