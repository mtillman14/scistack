"""Share as library (portability Stage 10b).

A submodule of a real on-disk project (own package + MATLAB sources) is
shared: the generated package carries the pipeline document with
``lib.fn`` labels, the Python modules with rewritten imports, the MATLAB
files with qualified calls, and the declarations (Parameters / PathInputs as
defaults). Listed back in a project, it seeds as a library pipeline.
"""

from __future__ import annotations

import importlib
import json
import sys
import textwrap

import pytest

from scistack_gui import pipeline_store as ps

PKG = "emgproj_t10b"
LIB = "emglib_t10b"


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A packaged project with a Python function (using a helper module of
    its own package), a MATLAB function (calling a MATLAB helper), a
    Parameter and a PathInput, opened WITH discovery; and a submodule 'emg'
    wiring them. Yields ``(root, db, pipeline_id)``."""
    from scistack_gui.headless import open_for_export, open_for_import
    from scistack_gui.ids import MANUAL_NODE_PREFIX

    root = tmp_path / "proj"
    pkg = root / "src" / PKG
    pkg.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "{PKG}"\nversion = "0.1.0"\ndependencies = ["scidb"]\n'
    )
    (pkg / "__init__.py").write_text("")
    (pkg / "util.py").write_text("def scale(x):\n    return x * 2\n")
    (pkg / "steps.py").write_text(textwrap.dedent(f"""\
        import json  # stdlib: no dependency
        from {PKG}.util import scale


        def filter_emg(signal, cutoff):
            return scale(signal)
        """))
    (pkg / "scistack_entities.toml").write_text(textwrap.dedent("""\
        variables = ["EmgRaw", "EmgFiltered", "EmgEnvelope"]

        [parameters]
        CUTOFF = 20

        [path_inputs]
        RawEmg = { template = "{subject}/emg.csv", root_folder = "/data/emg" }
        """))
    mdir = root / "matlab"
    mdir.mkdir()
    (mdir / "envelope_t10b.m").write_text(
        "function y = envelope_t10b(x)\n% uses smooth_t10b(x) in a comment\n"
        "y = smooth_t10b(abs(x));\nh = @smooth_t10b;\nend\n"
    )
    (mdir / "smooth_t10b.m").write_text("function y = smooth_t10b(x)\ny = x;\nend\n")
    (root / "scistack.toml").write_text(
        'modules = ["."]\nentities_file = "src/%s/scistack_entities.toml"\n\n'
        '[matlab]\nsources = ["matlab"]\n' % PKG
    )
    db_path = root / f"{PKG}.duckdb"
    open_for_import(db_path, project=root, schema_keys=["subject"])
    db = open_for_export(db_path, project=root)

    pid = ps.create_pipeline(db, "emg")
    nodes = {
        "raw": ("pathInputNode", "RawEmg"),
        "cut": ("parameterNode", "CUTOFF"),
        "fn": ("functionNode", "filter_emg"),
        "filt": ("variableNode", "EmgFiltered"),
        "env": ("functionNode", "envelope_t10b"),
        "out": ("variableNode", "EmgEnvelope"),
    }
    from scistack_gui import layout as layout_store

    ids = {}
    for i, (key, (ntype, label)) in enumerate(nodes.items()):
        ids[key] = f"{MANUAL_NODE_PREFIX[ntype]}__{label}__t10b{key}"
        ps.write_manual_node(db, ids[key], ntype, label, pid)
        # A drop also saves a position in the canvas: that is what PLACES a
        # declared Parameter / PathInput there (scope_filter), as in the GUI.
        layout_store.write_node_position(ids[key], 100.0 * i, 0.0, pipeline_id=pid)
    for i, (s, t, sh, th) in enumerate([
        ("raw", "fn", None, "in__signal"),
        ("cut", "fn", None, "in__cutoff"),
        ("fn", "filt", "out__EmgFiltered", None),
        ("filt", "env", None, "in__x"),
        ("env", "out", "out__EmgEnvelope", None),
    ]):
        ps.write_manual_edge(db, {"id": f"edge_t10b_{i}", "source": ids[s], "target": ids[t],
                                  "sourceHandle": sh, "targetHandle": th})
    ps.update_node_config(db, ids["fn"], {"schemaSelection": {"exclude_levels": {"subject": ["S02"]}}})
    yield root, db, pid
    for name in list(sys.modules):
        if name.split(".")[0] in (PKG, LIB):
            sys.modules.pop(name, None)


def _share(project, tmp_path):
    from scistack_gui.services.library_share import share_as_library

    root, db, pid = project
    return share_as_library(db, pid, tmp_path / "out", LIB)


def test_the_package_layout(project, tmp_path):
    report = _share(project, tmp_path)
    lib = tmp_path / "out" / "src" / LIB
    assert (tmp_path / "out" / "pyproject.toml").is_file()
    for rel in ("steps.py", "util.py", "__init__.py", "scistack_entities.toml",
                "pipelines/emg.json", f"matlab/+{LIB}/envelope_t10b.m",
                f"matlab/+{LIB}/smooth_t10b.m"):
        assert (lib / rel).is_file(), rel
    assert report.functions == {"filter_emg": f"{LIB}.filter_emg",
                                "envelope_t10b": f"{LIB}.envelope_t10b"}
    assert str(tmp_path / "out") in report.install and "pip install -e" in report.install


def test_python_imports_are_rewritten(project, tmp_path):
    report = _share(project, tmp_path)
    steps = (tmp_path / "out" / "src" / LIB / "steps.py").read_text()
    assert f"from {LIB}.util import scale" in steps
    assert PKG not in steps
    assert "import json" in steps
    assert any("util" in r for r in report.rewritten_imports)


def test_matlab_calls_are_qualified_but_declarations_and_comments_are_not(project, tmp_path):
    report = _share(project, tmp_path)
    text = (tmp_path / "out" / "src" / LIB / "matlab" / f"+{LIB}" / "envelope_t10b.m").read_text()
    assert text.startswith("function y = envelope_t10b(x)")  # declaration bare
    assert f"y = {LIB}.smooth_t10b(abs(x));" in text
    assert f"h = @{LIB}.smooth_t10b;" in text
    assert "% uses smooth_t10b(x) in a comment" in text  # comment untouched
    assert report.qualified_calls


def test_the_document_uses_lib_labels_and_no_locations(project, tmp_path):
    from scidb.library import read_document

    _share(project, tmp_path)
    doc = read_document((tmp_path / "out" / "src" / LIB / "pipelines" / "emg.json").read_bytes(), "emg")
    labels = {n["label"] for n in doc["canvas"]["nodes"]}
    assert {f"{LIB}.filter_emg", f"{LIB}.envelope_t10b", "EmgFiltered", "CUTOFF", "RawEmg"} <= labels
    assert "filter_emg" not in labels
    text = json.dumps(doc)
    assert "schemaSelection" not in text and "S02" not in text


def test_declarations_ship_as_defaults_without_roots(project, tmp_path):
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib

    _share(project, tmp_path)
    data = tomllib.loads((tmp_path / "out" / "src" / LIB / "scistack_entities.toml").read_text())
    assert {"EmgFiltered", "EmgEnvelope"} <= set(data["variables"])
    assert data["parameters"] == {"CUTOFF": 20}
    assert data["path_inputs"] == {"RawEmg": "{subject}/emg.csv"}  # no root_folder
    assert data["library"] == {"schema_keys": ["subject"]}


def test_refusals(project, tmp_path):
    from scidb.library import LibraryError

    from scistack_gui.services.library_share import ShareRefused, share_as_library

    root, db, pid = project
    full = tmp_path / "full"
    full.mkdir()
    (full / "x").write_text("")
    with pytest.raises(LibraryError, match="not empty"):
        share_as_library(db, pid, full, LIB)
    with pytest.raises(ShareRefused, match="already importable"):
        share_as_library(db, pid, tmp_path / "o2", "json")
    with pytest.raises(ValueError, match="Invalid project name"):
        share_as_library(db, pid, tmp_path / "o3", "Bad-Name")
    ps.write_manual_node(db, "fn__nowhere__t10bx", "functionNode", "nowhere_fn", pid)
    with pytest.raises(ShareRefused, match="nowhere_fn"):
        share_as_library(db, pid, tmp_path / "o4", LIB)


def test_the_shared_library_seeds_back_with_the_same_shape(project, tmp_path, monkeypatch):
    """Round trip: list the new library in the same project; its pipeline
    seeds as library-owned with the lib.fn labels and the same edges."""
    from scistack_gui.config import add_package
    from scistack_gui.services.library_service import placement_check, sync_libraries
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    root, db, pid = project
    _share(project, tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path / "out" / "src"))
    importlib.invalidate_caches()
    add_package(None, LIB, project=root)
    report = sync_libraries(db)
    assert report["seeded"] == [f"{LIB}/emg"], report

    lib_pid = next(r["pipeline_id"] for r in ps.list_library_pipelines(db)
                   if r["library"] == LIB and r["name"] == "emg")
    g = get_pipeline_graph(db, lib_pid)
    labels = sorted(str(n["data"].get("label")) for n in g["nodes"])
    assert f"{LIB}.filter_emg" in labels and f"{LIB}.envelope_t10b" in labels
    assert len(g["edges"]) == 5

    # CUTOFF and RawEmg are declared in this project: nothing to declare.
    check = placement_check(db, lib_pid)
    assert check["missing_parameters"] == [] and check["missing_path_inputs"] == []


def test_the_cli_create_command(project, tmp_path, capsys):
    import argparse

    from scistack_gui.library_cli import add_library_subparsers

    root, db, pid = project
    parser = argparse.ArgumentParser()
    add_library_subparsers(parser.add_subparsers(dest="command"))
    args = parser.parse_args([
        "library", "--project", str(root), "--json", "create", "--from-submodule", "emg",
        "--into", str(tmp_path / "cli_out"), "--name", LIB, "--db", str(root / f"{PKG}.duckdb"),
    ])
    capsys.readouterr()
    assert args._library_cmd(args) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["ok"] and out["report"]["library"] == LIB
    assert (tmp_path / "cli_out" / "src" / LIB / "pipelines" / "emg.json").is_file()


# ---------------------------------------------------------------------------
# Make my own copy (Stage 10c)
# ---------------------------------------------------------------------------


def _shared_and_listed(project, tmp_path, monkeypatch):
    from scistack_gui.config import add_package
    from scistack_gui.services.library_service import sync_libraries

    root, db, pid = project
    _share(project, tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path / "out" / "src"))
    importlib.invalidate_caches()
    add_package(None, LIB, project=root)
    sync_libraries(db)
    lib_pid = next(r["pipeline_id"] for r in ps.list_library_pipelines(db) if r["library"] == LIB)
    return root, db, lib_pid


def test_make_own_copy_rehomes_the_code_and_releases_the_pipelines(project, tmp_path, monkeypatch):
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib

    from scistack_gui.services.library_copy import make_own_copy

    root, db, lib_pid = _shared_and_listed(project, tmp_path, monkeypatch)
    nodes_before = ps.get_manual_nodes(db, lib_pid)

    report = make_own_copy(db, LIB)

    copied = root / "src" / PKG / LIB
    steps = (copied / "steps.py").read_text()
    assert f"from {PKG}.{LIB}.util import scale" in steps
    assert not (copied / "pipelines").exists() and not (copied / "matlab").exists()
    assert (root / "matlab" / f"+{LIB}" / "envelope_t10b.m").is_file()
    assert report.matlab_dir == str(root / "matlab" / f"+{LIB}")

    config = tomllib.loads((root / "scistack.toml").read_text())
    assert LIB not in config.get("packages", [])
    assert config["copied_libraries"] == [LIB]

    assert ps.library_owner(db, lib_pid) is None  # editable now
    assert ps.get_manual_nodes(db, lib_pid) == nodes_before  # same ids, same content
    assert report.released_pipelines == ["emg"]


def test_a_copied_librarys_functions_keep_their_names(project, tmp_path, monkeypatch):
    from scidb.names import function_name

    from scistack_gui import matlab_registry, registry
    from scistack_gui.services.library_copy import make_own_copy

    root, db, _ = _shared_and_listed(project, tmp_path, monkeypatch)
    make_own_copy(db, LIB)

    fn = registry.lookup_function(f"{LIB}.filter_emg")
    assert fn is not None
    assert getattr(fn, "__module__", "").startswith(f"{PKG}.{LIB}.")
    assert function_name(fn) == f"{LIB}.filter_emg"
    assert matlab_registry.is_matlab_function(f"{LIB}.envelope_t10b")


def test_edits_are_allowed_after_the_copy(project, tmp_path, monkeypatch):
    from scistack_gui import library_lock
    from scistack_gui.services.library_copy import make_own_copy

    root, db, lib_pid = _shared_and_listed(project, tmp_path, monkeypatch)
    make_own_copy(db, LIB)
    with library_lock.user_edit("rename_pipeline"):
        ps.rename_pipeline(db, lib_pid, "my emg")
    assert ps.get_pipeline(db, lib_pid)["name"] == "my emg"


def test_copy_refusals(project, tmp_path, monkeypatch):
    from scistack_gui.services.library_copy import CopyRefused, make_own_copy

    root, db, _ = _shared_and_listed(project, tmp_path, monkeypatch)
    with pytest.raises(CopyRefused, match="not a library"):
        make_own_copy(db, "json")
    (root / "src" / PKG / LIB).mkdir()
    with pytest.raises(CopyRefused, match="already exists"):
        make_own_copy(db, LIB)
