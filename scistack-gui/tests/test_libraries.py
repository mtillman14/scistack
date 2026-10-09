"""Library pipelines on the canvas: seed, lock, re-sync (portability Stage 10a).

A library ships a pipeline as a document (``scidb.library``). Listing the
library seeds it as a LIBRARY-OWNED pipeline (``services/library_service``):
derived ids, not a tab, read-only during user edits (``library_lock``), and
re-synced in place when the library changes. Placements of it on the
project's canvas belong to the project.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

from scistack_gui import pipeline_store as ps
from scistack_gui.db import get_db

LIB = "gaitlib_t10"


# ---------------------------------------------------------------------------
# Building a library on disk from a canvas
# ---------------------------------------------------------------------------


def _source_canvas(db) -> str:
    """A small submodule in THIS database to ship: EmgRaw -> lib fn ->
    Filtered. Returns its pipeline id."""
    pid = ps.create_pipeline(db, "preprocessing")
    ps.write_manual_node(db, "var__EmgRaw__src00001", "variableNode", "EmgRaw", pid)
    ps.write_manual_node(db, "fn__f__src00002", "functionNode", f"{LIB}.filter_emg", pid)
    ps.write_manual_node(db, "var__Filtered__src00003", "variableNode", "Filtered", pid)
    ps.write_manual_edge(db, {"id": "edge_src1", "source": "var__EmgRaw__src00001",
                              "target": "fn__f__src00002", "targetHandle": "in__signal"})
    ps.write_manual_edge(db, {"id": "edge_src2", "source": "fn__f__src00002",
                              "target": "var__Filtered__src00003", "sourceHandle": "out__Filtered"})
    return pid


def _document(db, pid: str, name: str = "preprocessing") -> dict:
    from scidb.library import make_document

    from scistack_gui.services import canvas_snapshot

    snap = canvas_snapshot.capture(db, [pid])
    return make_document(name, pid, [{"pipeline_id": pid, "name": name}], snap.to_dict())


def _write_library(root: Path, docs: dict, *, schema_keys=None, matlab: bool = False) -> Path:
    from scidb.library import document_bytes

    pkg = root / LIB
    (pkg / "pipelines").mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("")
    entities = 'variables = ["Filtered"]\n'
    if schema_keys:
        entities += f"\n[library]\nschema_keys = {json.dumps(schema_keys)}\n"
    (pkg / "scistack_entities.toml").write_text(entities)
    for old in (pkg / "pipelines").glob("*.json"):
        old.unlink()
    for name, doc in docs.items():
        (pkg / "pipelines" / f"{name}.json").write_bytes(document_bytes(doc))
    if matlab:
        m = pkg / "matlab" / f"+{LIB}"
        m.mkdir(parents=True, exist_ok=True)
        (m / "lowpass.m").write_text("function y = lowpass(x)\ny = x;\nend\n")
    return pkg


@pytest.fixture
def library(client, tmp_path, monkeypatch):
    """A library LIB on sys.path, listed in the project's scistack.toml,
    shipping the source canvas as 'preprocessing'. Yields
    ``(lib_root, document, source_pid)``."""
    from scidb import names
    from scifor.pathinput import clear_project_root, set_project_root

    from scistack_gui.config import add_package

    set_project_root(tmp_path)
    db = get_db()
    src = _source_canvas(db)
    doc = _document(db, src)
    lib_root = tmp_path / "libsrc"
    _write_library(lib_root, {"preprocessing": doc}, schema_keys=["participant", "session"])
    monkeypatch.syspath_prepend(str(lib_root))
    importlib.invalidate_caches()
    add_package(None, LIB, project=tmp_path)
    names.clear_cache()
    yield lib_root, doc, src
    from scidb import schema_order

    sys.modules.pop(LIB, None)
    names.clear_cache()
    schema_order.clear_cache()
    clear_project_root()


def _shape(db, pid: str) -> tuple:
    """Labels and edges between labels: the canvas a user sees."""
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    g = get_pipeline_graph(db, pid)
    label = {n["id"]: n["data"].get("label") for n in g["nodes"]}
    return (
        sorted(str(v) for v in label.values()),
        sorted((str(label.get(e["source"])), str(label.get(e["target"]))) for e in g["edges"]),
    )


def _root(doc) -> str:
    from scistack_gui.services.library_service import local_pipeline_ids

    return local_pipeline_ids(LIB, doc)[doc["root"]]


# ---------------------------------------------------------------------------
# Seed and re-sync
# ---------------------------------------------------------------------------


def test_a_listed_library_is_seeded_as_a_read_only_pipeline(library):
    from scistack_gui.services.library_service import sync_libraries

    _, doc, src = library
    db = get_db()
    report = sync_libraries(db)
    assert report["seeded"] == [f"{LIB}/preprocessing"]

    pid = _root(doc)
    owner = ps.library_owner(db, pid)
    assert owner["library"] == LIB and owner["pipeline_name"] == "preprocessing"
    assert pid not in {h["pipeline_id"] for h in ps.list_hypotheses(db)}  # never a tab
    assert _shape(db, pid) == _shape(db, src)
    assert all("__src" not in n for n in ps.get_manual_nodes(db, pid))  # derived ids


def test_an_unchanged_library_writes_nothing(library):
    from scistack_gui.services.library_service import sync_libraries

    db = get_db()
    sync_libraries(db)
    nodes = ps.get_manual_nodes(db, _root(library[1]))
    report = sync_libraries(db)
    assert report["unchanged"] == [f"{LIB}/preprocessing"] and not report["resynced"]
    assert ps.get_manual_nodes(db, _root(library[1])) == nodes


def test_an_upgraded_library_resyncs_in_place_and_keeps_placements(library):
    from scistack_gui.services.library_service import sync_libraries

    lib_root, doc, src = library
    db = get_db()
    sync_libraries(db)
    pid = _root(doc)
    before_ids = set(ps.get_manual_nodes(db, pid))
    use = ps.add_pipeline_use(db, "main", pid, {"key_map": {"participant": "subject"}})

    # The library gains a step.
    ps.write_manual_node(db, "fn__g__src00004", "functionNode", f"{LIB}.envelope", src)
    _write_library(lib_root, {"preprocessing": _document(db, src)}, schema_keys=["participant", "session"])
    report = sync_libraries(db)

    assert report["resynced"] == [f"{LIB}/preprocessing"]
    after = ps.get_manual_nodes(db, pid)
    assert before_ids <= set(after)  # the same derived ids
    assert f"{LIB}.envelope" in {m["label"] for m in after.values()}
    uses = {u["use_id"]: u for u in ps.get_pipeline_uses(db, "main")}
    assert uses[use]["child_pipeline_id"] == pid
    assert uses[use]["binding"] == {"key_map": {"participant": "subject"}}


def test_a_library_that_stops_shipping_a_pipeline_reports_it(library):
    from scistack_gui.services.library_service import sync_libraries

    lib_root, doc, _ = library
    db = get_db()
    sync_libraries(db)
    _write_library(lib_root, {}, schema_keys=["participant", "session"])
    report = sync_libraries(db)
    assert report["orphaned"] == [f"{LIB}/preprocessing"]
    assert ps.get_pipeline(db, _root(doc)) is not None  # never deleted


def test_a_broken_document_is_reported_not_fatal(library):
    from scistack_gui.services.library_service import sync_libraries

    lib_root, _, _ = library
    (lib_root / LIB / "pipelines" / "broken.json").write_text("{not json")
    report = sync_libraries(get_db())
    assert report["seeded"] == [f"{LIB}/preprocessing"]
    assert any("broken.json" in e for e in report["errors"])


# ---------------------------------------------------------------------------
# The lock
# ---------------------------------------------------------------------------


def _invoke(method: str, **params):
    """Call a handler the way both transports do (Handler.invoke), so the
    call is a USER EDIT exactly as in the GUI."""
    from scistack_gui.api.tables import ALL_HANDLERS

    h = next(h for h in ALL_HANDLERS if h.name == method)
    req = h.params(**params) if h.params is not None else None
    return h.invoke(req, get_db(), transport="rpc")


def test_user_edits_inside_a_library_pipeline_are_refused(library):
    from scistack_gui.library_lock import LibraryPipelineReadOnly
    from scistack_gui.services.library_service import sync_libraries

    db = get_db()
    sync_libraries(db)
    pid = _root(library[1])
    nodes = ps.get_manual_nodes(db, pid)
    fn = next(n for n, m in nodes.items() if m["type"] == "functionNode")

    refused = [
        ("put_layout", dict(node_id="fn__new__00000001", x=0, y=0, node_type="functionNode",
                            label="other_fn", pipeline_id=pid)),
        ("put_node_config", dict(node_id=fn, config={"schemaLevel": ["subject"]})),
        ("delete_layout", dict(node_id=fn)),
        ("put_edge", dict(edge_id="edge_x", source="var__Elsewhere__0000", target=fn, target_handle="in__other")),
        ("rename_pipeline", dict(pipeline_id=pid, name="mine")),
    ]
    for name, params in refused:
        with pytest.raises(LibraryPipelineReadOnly, match=LIB):
            _invoke(name, **params)
    assert ps.get_manual_nodes(db, pid) == nodes  # nothing changed


def test_the_project_still_places_binds_and_moves_it(library):
    from scistack_gui.services.library_service import sync_libraries

    db = get_db()
    sync_libraries(db)
    pid = _root(library[1])
    use = _invoke("add_pipeline_use", parent_pipeline_id="main", child_pipeline_id=pid,
                  binding=None, x=0, y=0)["use_id"]
    _invoke("update_use_binding", use_id=use, binding={"key_map": {"participant": "subject"}})
    fn = next(iter(ps.get_manual_nodes(db, pid)))
    _invoke("put_layout", node_id=fn, x=10, y=20, pipeline_id=pid)  # a move only
    assert {u["use_id"] for u in ps.get_pipeline_uses(db, "main")} >= {use}


def test_internal_writes_are_never_refused(library):
    """Outside a user edit (graph builds, scripts, seeding) the stores write."""
    from scistack_gui.services.library_service import sync_libraries

    db = get_db()
    sync_libraries(db)
    pid = _root(library[1])
    ps.write_manual_node(db, "var__X__internal1", "variableNode", "X", pid)
    assert "var__X__internal1" in ps.get_manual_nodes(db, pid)


def test_graduation_inside_an_unrelated_user_edit_is_not_refused(library):
    """A graph build during some edit on the project's canvas may graduate a
    seeded library node onto its history twin (migrate_node_config, which
    writes through the guarded update_node_config). That is bookkeeping, not
    an edit of the library pipeline: library_lock.internal lets it through."""
    from scistack_gui import library_lock
    from scistack_gui.services.library_service import sync_libraries

    db = get_db()
    sync_libraries(db)
    pid = _root(library[1])
    fn = next(n for n, m in ps.get_manual_nodes(db, pid).items() if m["type"] == "functionNode")
    with library_lock.user_edit("put_edge"):
        ps.migrate_node_config(db, fn, f"{fn}::{pid}")
        ps.rename_edge_endpoints(db, fn, f"{fn}::{pid}")


def test_every_guarded_write_checks_the_lock():
    """The pipeline_store writes that bypass the intent store each call the
    lock; the intent store's choke points do too."""
    import inspect

    from scistack_gui import intent_store

    for name in ps.GUARDED_WRITES:
        src = inspect.getsource(getattr(ps, name))
        assert "library_lock.check_" in src, f"pipeline_store.{name} never checks the lock"
    for name in ("put_statements", "clear_aspect", "delete_statements", "delete_scope"):
        src = inspect.getsource(getattr(intent_store, name))
        assert "library_lock" in src or "_check_deletion" in src, name


# ---------------------------------------------------------------------------
# Placing through a key map; listing; config
# ---------------------------------------------------------------------------


def test_placing_suggests_a_key_map_from_the_library_schema(library):
    from scistack_gui.services.library_service import suggest_key_map, sync_libraries

    db = get_db()
    sync_libraries(db)
    s = suggest_key_map(db, _root(library[1]))
    assert s["library_keys"] == ["participant", "session"]
    # 'session' is in both schemas; 'participant' is not in the project's.
    assert s["key_map"] == {}
    assert s["unmapped"] == ["participant"]


def test_the_libraries_list_and_the_handlers(library):
    db = get_db()
    out = _invoke("sync_libraries")
    assert out["seeded"] == [f"{LIB}/preprocessing"]
    libs = _invoke("list_libraries")["libraries"]
    (lib,) = [x for x in libs if x["library"] == LIB]
    assert lib["schema_keys"] == ["participant", "session"]
    assert lib["pipelines"][0]["seeded"] is True
    assert lib["pipelines"][0]["pipeline_id"] == _root(library[1])
    assert db is get_db()


def test_add_package_lists_once_and_refuses_bad_names(tmp_path):
    from scistack_gui.config import add_package, remove_package

    (tmp_path / "scistack.toml").write_text('modules = ["."]\n')
    add_package(None, "gait_tools", project=tmp_path)
    add_package(None, "gait_tools", project=tmp_path)
    text = (tmp_path / "scistack.toml").read_text()
    assert text.count("gait_tools") == 1 and "modules" in text  # other keys kept
    with pytest.raises(ValueError, match="import name"):
        add_package(None, "gait-tools", project=tmp_path)
    remove_package(None, "gait_tools", project=tmp_path)
    assert "gait_tools" not in (tmp_path / "scistack.toml").read_text()
    with pytest.raises(ValueError, match="not listed"):
        remove_package(None, "gait_tools", project=tmp_path)


def test_the_library_cli_lists_adds_and_removes(tmp_path, capsys, monkeypatch):
    from scistack_gui.library_cli import add_library_subparsers

    import argparse

    (tmp_path / "scistack.toml").write_text("")
    parser = argparse.ArgumentParser()
    add_library_subparsers(parser.add_subparsers(dest="command"))

    def run(*argv):
        args = parser.parse_args(["library", "--project", str(tmp_path), "--json", *argv])
        code = args._library_cmd(args)
        return code, json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    code, out = run("add", "no_such_lib_t10")
    assert code == 0
    (row,) = out["libraries"]
    assert row["library"] == "no_such_lib_t10" and row["installed"] is False
    code, out = run("remove", "no_such_lib_t10")
    assert code == 0 and out["libraries"] == []
    code, out = run("remove", "no_such_lib_t10")
    assert code == 1 and "not listed" in out["error"]


# ---------------------------------------------------------------------------
# MATLAB in a library
# ---------------------------------------------------------------------------


def test_a_librarys_matlab_functions_are_discovered_and_on_the_path(library, tmp_path):
    from scistack_gui.config import load_config
    from scistack_gui.matlab_parser import parse_matlab_function

    lib_root, doc, _ = library
    _write_library(lib_root, {"preprocessing": doc}, schema_keys=["participant", "session"],
                   matlab=True)
    config = load_config(tmp_path, tmp_path / "test.duckdb")
    mfile = lib_root / LIB / "matlab" / f"+{LIB}" / "lowpass.m"
    assert mfile.resolve() in {p.resolve() for p in config.matlab_sources}
    assert (lib_root / LIB / "matlab").resolve() in {p.resolve() for p in config.matlab_addpath}
    assert parse_matlab_function(mfile).name == f"{LIB}.lowpass"


# ---------------------------------------------------------------------------
# Bundles carry library pipelines by reference
# ---------------------------------------------------------------------------


def test_a_bundle_carries_library_pipelines_by_reference(library, tmp_path):
    from scidb.bundle import export_project

    from scistack_gui.headless import bundle_providers, import_project_bundle
    from scistack_gui.services.library_service import sync_libraries

    db = get_db()
    sync_libraries(db)
    pid = _root(library[1])
    ps.add_pipeline_use(db, "main", pid, {"key_map": {"participant": "subject"}})
    out = export_project(tmp_path, db, tmp_path / "out" / "study", providers=bundle_providers())

    import zipfile

    with zipfile.ZipFile(out) as zf:
        gui = json.loads(zf.read("gui/gui.json"))
    assert [r["pipeline_id"] for r in gui["library_pipelines"]] == [pid]
    assert pid not in {p["pipeline_id"] for p in gui["pipelines"]}
    assert all(n["pipeline_id"] != pid for n in gui["canvas"]["nodes"])  # content not carried

    report = import_project_bundle(out, tmp_path / "copy")
    new = get_db()
    assert report.sections["gui"]["library_pipelines"] == [f"{LIB}/preprocessing"]
    owner = ps.library_owner(new, pid)
    assert owner is not None and owner["definition_hash"] == ""  # empty until synced
    assert pid in {u["child_pipeline_id"] for u in ps.get_pipeline_uses(new, "main")}


def test_placing_lists_missing_requirements_and_declares_them_from_defaults(library, monkeypatch):
    """A Parameter / PathInput the library pipeline uses but the project
    does not declare is listed with the library's default; declaring writes
    it through the sidebar's own services (a PathInput without a root)."""
    from scistack_gui.services import library_service
    from scistack_gui.services.library_service import placement_check, sync_libraries

    lib_root, _, src = library
    db = get_db()
    ps.write_manual_node(db, "param__LIB_ONLY_T10__x1", "parameterNode", "LIB_ONLY_T10", src)
    ps.write_manual_node(db, "pathInput__LibRaw_T10__x2", "pathInputNode", "LibRaw_T10", src)
    _write_library(lib_root, {"preprocessing": _document(db, src)}, schema_keys=["participant", "session"])
    ent = lib_root / LIB / "scistack_entities.toml"
    ent.write_text(ent.read_text().replace(
        "\n[library]",
        '\n[parameters]\nLIB_ONLY_T10 = [3, 4]\n\n[path_inputs]\nLibRaw_T10 = ["{subject}/a.csv", "{subject}/b.csv"]\n\n[library]',
    ))
    sync_libraries(db)
    pid = _root(_document(db, src))

    check = placement_check(db, pid)
    assert check["missing_parameters"] == [{"name": "LIB_ONLY_T10", "default": [3, 4]}]
    assert check["missing_path_inputs"] == [
        {"name": "LibRaw_T10", "default": ["{subject}/a.csv", "{subject}/b.csv"]}
    ]

    calls = []
    monkeypatch.setattr(
        "scistack_gui.services.parameter_service.create_parameter",
        lambda name, values, *a, **k: calls.append(("param", name, values)) or {"ok": True},
    )
    monkeypatch.setattr(
        "scistack_gui.services.path_input_service.create_path_input",
        lambda name, template, root, alts=None: calls.append(("pi", name, template, root, alts)) or {"ok": True},
    )
    out = library_service.declare_requirements(db, pid)
    assert out == {"ok": True, "declared": ["LIB_ONLY_T10", "LibRaw_T10"], "failed": []}
    assert ("param", "LIB_ONLY_T10", [3, 4]) in calls
    assert ("pi", "LibRaw_T10", "{subject}/a.csv", None,
            [{"template": "{subject}/b.csv", "root_folder": None}]) in calls
