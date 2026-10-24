"""Share as library / publish / Make my own copy, at the service level
(``services/library_authoring``; portability Stage 10b,
``.claude/plan-library-fork-chain.md`` R1).

A submodule of a real on-disk project (own package + MATLAB sources) is
shared: the library is written to ``<project>/libs/<lib>/`` and carries the
pipeline document with ``lib.fn`` labels, the Python modules with rewritten
imports, the MATLAB files with qualified calls, and the declarations
(Parameters / PathInputs as defaults). The project then USES it: listed,
its pipeline owned by it with the same id, the canvas relabelled, still
editable (it is authored here). The end-to-end story across four projects
is ``test_library_fork_chain.py``.
"""

from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

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
        if name.split(".")[0] in (PKG, LIB, "emgcopy_t10b"):
            sys.modules.pop(name, None)
    for p in list(sys.path):  # authored libraries joined sys.path (O1)
        if str(root / "libs") in p:
            sys.path.remove(p)


def _share(project, tmp_path=None):
    from scistack_gui.services.library_authoring import share

    root, db, pid = project
    return share(db, pid, LIB)


def _lib(project) -> "Path":
    return project[0] / "libs" / LIB


def test_the_package_layout(project):
    report = _share(project)
    folder = _lib(project)
    lib = folder / "src" / LIB
    assert report.folder == folder.resolve()
    assert (folder / "pyproject.toml").is_file()
    for rel in ("steps.py", "util.py", "__init__.py", "scistack_entities.toml",
                "pipelines/emg.json", f"matlab/+{LIB}/envelope_t10b.m",
                f"matlab/+{LIB}/smooth_t10b.m"):
        assert (lib / rel).is_file(), rel
    assert report.functions == {"filter_emg": f"{LIB}.filter_emg",
                                "envelope_t10b": f"{LIB}.envelope_t10b"}
    assert "pip install -e" in report.install and LIB in report.install
    assert "from .steps import filter_emg" in (lib / "__init__.py").read_text()  # A9


def test_python_imports_are_rewritten(project):
    report = _share(project)
    steps = (_lib(project) / "src" / LIB / "steps.py").read_text()
    assert f"from {LIB}.util import scale" in steps
    assert PKG not in steps
    assert "import json" in steps
    assert any("util" in r for r in report.rewritten_imports)


def test_matlab_calls_are_qualified_but_declarations_and_comments_are_not(project):
    report = _share(project)
    text = (_lib(project) / "src" / LIB / "matlab" / f"+{LIB}" / "envelope_t10b.m").read_text()
    assert text.startswith("function y = envelope_t10b(x)")  # declaration bare
    assert f"y = {LIB}.smooth_t10b(abs(x));" in text
    assert f"h = @{LIB}.smooth_t10b;" in text
    assert "% uses smooth_t10b(x) in a comment" in text  # comment untouched
    assert report.qualified_calls


def test_the_document_uses_lib_labels_and_no_locations(project):
    from scidb.library import read_document

    _share(project)
    doc = read_document((_lib(project) / "src" / LIB / "pipelines" / "emg.json").read_bytes(), "emg")
    labels = {n["label"] for n in doc["canvas"]["nodes"]}
    assert {f"{LIB}.filter_emg", f"{LIB}.envelope_t10b", "EmgFiltered", "CUTOFF", "RawEmg"} <= labels
    assert "filter_emg" not in labels
    text = json.dumps(doc)
    assert "schemaSelection" not in text and "S02" not in text


def test_declarations_ship_as_defaults_without_roots(project):
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib

    _share(project)
    data = tomllib.loads((_lib(project) / "src" / LIB / "scistack_entities.toml").read_text())
    assert {"EmgFiltered", "EmgEnvelope"} <= set(data["variables"])
    assert data["parameters"] == {"CUTOFF": 20}
    assert data["path_inputs"] == {"RawEmg": "{subject}/emg.csv"}  # no root_folder
    assert data["library"] == {"schema_keys": ["subject"]}


def test_refusals(project):
    from scistack_gui.services.library_authoring import AuthoringRefused, share

    root, db, pid = project
    with pytest.raises(AuthoringRefused, match="already importable"):
        share(db, pid, "json")
    with pytest.raises(ValueError, match="Invalid project name"):
        share(db, pid, "Bad-Name")
    (root / "libs" / "taken_t10b").mkdir(parents=True)
    (root / "libs" / "taken_t10b" / "x").write_text("")
    with pytest.raises(AuthoringRefused, match="already exists"):
        share(db, pid, "taken_t10b")
    ps.write_manual_node(db, "fn__nowhere__t10bx", "functionNode", "nowhere_fn", pid)
    with pytest.raises(AuthoringRefused, match="nowhere_fn"):
        share(db, pid, LIB)
    assert not _lib(project).exists(), "a refused share wrote files"


def test_the_project_switches_to_its_library(project):
    """B1/B4/B5: listed, owned with the same id, relabelled, still editable."""
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib

    from scistack_gui import library_lock
    from scistack_gui.services.library_service import list_libraries

    root, db, pid = project
    _share(project)
    assert LIB in tomllib.loads((root / "scistack.toml").read_text())["packages"]
    assert ps.library_owner(db, pid)["library"] == LIB
    labels = {m["label"] for m in ps.get_manual_nodes(db, pid).values()}
    assert {f"{LIB}.filter_emg", f"{LIB}.envelope_t10b"} <= labels
    assert "filter_emg" not in labels
    rows = {r["library"]: r for r in list_libraries(db)}
    assert rows[LIB]["authored"] is True
    assert [p["pipeline_id"] for p in rows[LIB]["pipelines"]] == [pid]
    with library_lock.user_edit("rename_pipeline"):
        ps.rename_pipeline(db, pid, "my emg")  # not refused: authored here


def test_sync_never_overwrites_the_working_copy(project):
    """B6: an unpublished canvas edit survives a re-sync; publishing writes it."""
    from scidb.library import read_document

    from scistack_gui import library_lock
    from scistack_gui.services.library_authoring import share
    from scistack_gui.services.library_service import sync_libraries

    root, db, pid = project
    _share(project)
    doc_path = _lib(project) / "src" / LIB / "pipelines" / "emg.json"
    before = doc_path.read_bytes()
    with library_lock.user_edit("add_node"):
        ps.write_manual_node(db, "var__EmgRaw__t10bnew", "variableNode", "EmgRaw", pid)
    sync_libraries(db)
    assert "var__EmgRaw__t10bnew" in ps.get_manual_nodes(db, pid)
    assert doc_path.read_bytes() == before
    report = share(db, pid, LIB)
    assert report.action == "publish" and report.functions == {}
    labels = {n["label"] for n in read_document(doc_path.read_bytes(), "emg")["canvas"]["nodes"]}
    assert "EmgRaw" in labels


def test_the_cli_create_command(project, capsys):
    import argparse

    from scistack_gui.library_cli import add_library_subparsers

    root, db, pid = project
    parser = argparse.ArgumentParser()
    add_library_subparsers(parser.add_subparsers(dest="command"))
    args = parser.parse_args([
        "library", "--project", str(root), "--json", "create", "--from-submodule", "emg",
        "--name", LIB, "--db", str(root / f"{PKG}.duckdb"),
    ])
    capsys.readouterr()
    assert args._library_cmd(args) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["ok"] and out["report"]["library"] == LIB
    assert (root / "libs" / LIB / "src" / LIB / "pipelines" / "emg.json").is_file()


# ---------------------------------------------------------------------------
# Make my own copy, under a new name (B2)
# ---------------------------------------------------------------------------

COPY = "emgcopy_t10b"


def test_make_own_copy_renames_and_switches_every_placement(project):
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib

    from scidb.names import function_name

    from scistack_gui import matlab_registry, registry
    from scistack_gui.services.library_authoring import copy

    root, db, pid = project
    _share(project)
    report = copy(db, LIB, COPY)
    pkg = root / "libs" / COPY / "src" / COPY
    assert report.functions == {f"{LIB}.filter_emg": f"{COPY}.filter_emg",
                                f"{LIB}.envelope_t10b": f"{COPY}.envelope_t10b"}
    steps = (pkg / "steps.py").read_text()
    assert f"from {COPY}.util import scale" in steps and LIB not in steps
    text = (pkg / "matlab" / f"+{COPY}" / "envelope_t10b.m").read_text()
    assert f"y = {COPY}.smooth_t10b(abs(x));" in text and f"@{COPY}.smooth_t10b" in text
    assert LIB not in text
    data = tomllib.loads((pkg / "scistack_entities.toml").read_text())
    assert data["library"]["derived_from"] == [LIB]  # A8
    config = tomllib.loads((root / "scistack.toml").read_text())
    assert LIB not in config["packages"] and COPY in config["packages"]
    assert ps.library_owner(db, pid)["library"] == COPY  # same id, re-owned
    labels = {m["label"] for m in ps.get_manual_nodes(db, pid).values()}
    assert f"{COPY}.filter_emg" in labels and f"{LIB}.filter_emg" not in labels
    fn = registry.lookup_function(f"{COPY}.filter_emg")
    assert fn is not None and function_name(fn) == f"{COPY}.filter_emg"
    assert matlab_registry.is_matlab_function(f"{COPY}.envelope_t10b")


def test_copy_refusals(project):
    from scistack_gui.services.library_authoring import AuthoringRefused, copy

    root, db, _ = project
    _share(project)
    with pytest.raises(AuthoringRefused, match="not a library"):
        copy(db, "json", COPY)
    with pytest.raises(AuthoringRefused, match="already"):
        copy(db, LIB, LIB)


def test_dependencies_survive_a_broken_distribution_scan(monkeypatch):
    """One distribution with unreadable metadata made packages_distributions()
    raise, and every share then shipped NO dependencies (CI 2026-10-23)."""
    import importlib.metadata

    from scistack_gui.services.library_authoring import _distributions

    assert _distributions({"numpy", "json"}) == {"numpy"}  # stdlib left out

    def broken():
        raise KeyError("Name")

    monkeypatch.setattr(importlib.metadata, "packages_distributions", broken)
    assert _distributions({"numpy"}) == {"numpy"}
