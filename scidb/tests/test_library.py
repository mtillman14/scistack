"""scidb.library and the entities [library] table (portability Stage 10a).

A library ships pipeline documents and declares its schema vocabulary; this
layer reads both from an installed package without constructing its
Variables, and finds its MATLAB folder without running it.
"""

from __future__ import annotations

import importlib
import json
import sys

import pytest

from scidb import entities, library
from scidb.library import (
    DOCUMENT_FORMAT,
    LibraryError,
    definition_hash,
    document_bytes,
    make_document,
    read_document,
    read_library,
)

PKG = "scidb_libtest_pkg"


def _doc(name="pre", canvas=None):
    return make_document(name, "p1", [{"pipeline_id": "p1", "name": name}], canvas or {"nodes": []})


@pytest.fixture
def package(tmp_path, monkeypatch):
    root = tmp_path / "site"
    pkg = root / PKG
    (pkg / "pipelines").mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    monkeypatch.syspath_prepend(str(root))
    importlib.invalidate_caches()
    yield pkg
    sys.modules.pop(PKG, None)


# --- the [library] table -------------------------------------------------------


def test_library_schema_keys_parse():
    assert entities.parse_library_table({"library": {"schema_keys": ["subject", "trial"]}}) == (
        ["subject", "trial"], [],
    )
    assert entities.parse_library_table({}) == (None, [])


@pytest.mark.parametrize("table, message", [
    ({"schema_keys": "subject"}, "distinct"),
    ({"schema_keys": ["a", "a"]}, "distinct"),
    ({"schema_keys": ["a", ""]}, "distinct"),
    ({"schema_key": ["a"]}, "unknown key"),
])
def test_a_bad_library_table_is_an_error(table, message):
    keys, errors = entities.parse_library_table({"library": table})
    assert any(message in e for e in errors)


def test_the_entities_loader_reads_the_table_and_reports_errors(tmp_path):
    path = tmp_path / "scistack_entities.toml"
    path.write_text('variables = []\n\n[library]\nschema_keys = ["subject"]\n')
    assert entities.load(path).library_schema_keys == ["subject"]
    path.write_text('variables = []\n\n[library]\nschema_keys = [1]\n')
    loaded = entities.load(path)
    assert loaded.library_schema_keys is None
    assert any(e.name == "library" for e in loaded.errors)


# --- the pipeline document -----------------------------------------------------


def test_a_document_round_trips_and_hashes_stably():
    doc = _doc()
    assert doc["format"] == DOCUMENT_FORMAT
    again = read_document(document_bytes(doc), "x")
    assert again == doc
    assert definition_hash(again) == definition_hash(doc)
    changed = _doc(canvas={"nodes": [{"node_id": "n"}]})
    assert definition_hash(changed) != definition_hash(doc)


def test_a_document_is_validated():
    with pytest.raises(LibraryError, match="root"):
        make_document("pre", "missing", [{"pipeline_id": "p1", "name": "pre"}], {})
    with pytest.raises(LibraryError, match="not JSON"):
        read_document(b"{nope", "x.json")
    bad = dict(_doc(), format_version=99)
    with pytest.raises(LibraryError, match="format_version"):
        read_document(json.dumps(bad).encode(), "x.json")


# --- reading an installed library ---------------------------------------------


def test_read_library(package):
    (package / "scistack_entities.toml").write_text(
        'variables = ["NeverConstructed"]\n\n[library]\nschema_keys = ["subject", "session"]\n'
    )
    (package / "pipelines" / "pre.json").write_bytes(document_bytes(_doc("pre")))
    (package / "pipelines" / "notes.txt").write_text("ignored")
    (package / "matlab" / f"+{PKG}").mkdir(parents=True)

    from scidb import BaseVariable

    info = read_library(PKG)
    assert info.schema_keys == ["subject", "session"]
    assert [p.name for p in info.pipelines] == ["pre"]
    assert info.pipelines[0].definition_hash == definition_hash(_doc("pre"))
    assert info.matlab_dir == package / "matlab"
    assert info.errors == []
    # Reading the table never constructs the library's Variables.
    assert "NeverConstructed" not in BaseVariable._all_subclasses


def test_a_bad_document_or_duplicate_name_is_reported_not_fatal(package):
    (package / "pipelines" / "a.json").write_bytes(document_bytes(_doc("pre")))
    (package / "pipelines" / "b.json").write_bytes(document_bytes(_doc("pre")))
    (package / "pipelines" / "c.json").write_text("{bad")
    info = read_library(PKG)
    assert [p.name for p in info.pipelines] == ["pre"]
    assert any("c.json" in e for e in info.errors)
    assert any("shipped twice" in e for e in info.errors)


def test_matlab_dir_never_runs_the_package(package):
    (package / "__init__.py").write_text("raise RuntimeError('imported!')\n")
    (package / "matlab").mkdir()
    assert library.matlab_dir(PKG) == package / "matlab"
    assert library.matlab_dir("scidb_no_such_package_t10") is None


def test_an_unimportable_library_is_reported():
    info = read_library("scidb_no_such_package_t10")
    assert info.pipelines == [] and info.errors


def test_a_config_write_is_seen_by_the_next_read(tmp_path):
    """config_file.write invalidates the cached reads: a library listed and
    read back in the same request (the GUI's 'add library', then seeding)
    must not see the stale "no config here" schema_order cached a moment
    earlier."""
    from scifor.pathinput import clear_project_root, set_project_root

    from scidb import config_file, names, schema_order

    set_project_root(tmp_path)
    try:
        schema_order.clear_cache()
        names.clear_cache()
        assert schema_order.locate_config() is None  # cached: none here
        assert "gait_tools" not in names.library_packages()
        config_file.write(tmp_path / "scistack.toml", {"packages": ["gait_tools"]})
        assert "gait_tools" in names.library_packages()
    finally:
        clear_project_root()
        schema_order.clear_cache()
        names.clear_cache()
