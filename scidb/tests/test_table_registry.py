"""scidb owns the project database's DDL (decision D3; plan P3 stage 6).

Five packages used to create tables in one ``.duckdb`` with nobody owning
the list (design review 09, T2). Now every fixed table is a
``scidb.tables.TableDecl``: scidb's own are created on every writable open,
other packages' through ``tables.ensure`` on first use. Guards:

* no ``CREATE TABLE`` / ``SEQUENCE`` / ``INDEX`` outside the files listed in
  ``ALLOWED`` (each with its reason);
* opening a database creates every scidb table, and reopening it is a no-op;
* a second, different declaration of a taken name is refused.
"""

import re
from pathlib import Path

import pytest

from scidb import configure_database, tables
from scidb.exceptions import TableDeclarationError
from scidb.sciduck import SciDuck

REPO = Path(__file__).resolve().parents[2]
SCANNED = [
    REPO / "scidb" / "src",
    REPO / "sciduckdb" / "src",
    REPO / "scistackplotdb" / "src",
    REPO / "scistackstatsdb" / "src",
    REPO / "scistack-gui" / "scistack_gui",
]
ALLOWED = {
    "scidb/src/scidb/tables.py": "the registry: every scidb table",
    "scidb/src/scidb/versioned_store.py": "declares each store's table via TableDecl",
    "scidb/src/scidb/database.py": "the per-variable <Type>_data tables (dynamic)",
    "scidb/src/scidb/project/verify.py": "an in-memory copy of a bundle, not a project database",
    "scistack-gui/scistack_gui/stores/pipeline_store.py": "TableDecls, owner scistack_gui",
    "scistack-gui/scistack_gui/stores/layout.py": "TableDecls, owner scistack_gui",
    "scistack-gui/scistack_gui/stores/node_wiring.py": "TableDecls, owner scistack_gui",
    "scistack-gui/scistack_gui/stores/history.py": "TableDecl, owner scistack_gui",
}
_DDL = re.compile(r"^\s*(?:f?\"\"\"|f?')?\s*CREATE (?:TABLE|SEQUENCE|INDEX)\b", re.M)
_DYNAMIC = re.compile(r'CREATE TABLE "\{table_name\}"')


def _ddl_files() -> dict[str, str]:
    found = {}
    for root in SCANNED:
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8")
            if _DDL.search(text) or "CREATE TABLE \"" in text:
                found[path.relative_to(REPO).as_posix()] = text
    return found


def test_no_ddl_outside_the_registry() -> None:
    assert sorted(set(_ddl_files()) - set(ALLOWED)) == []


def test_gui_and_store_ddl_goes_through_table_decl() -> None:
    files = _ddl_files()
    for name in ALLOWED:
        if name.startswith("scistack-gui/") or name.endswith("versioned_store.py"):
            assert "TableDecl(" in files.get(name, ""), name


def test_database_creates_only_data_tables() -> None:
    text = (REPO / "scidb/src/scidb/database.py").read_text(encoding="utf-8")
    fixed = [m for m in re.finditer(r"CREATE TABLE IF NOT EXISTS", text)]
    assert fixed == [], "a fixed table belongs in scidb.tables"
    assert _DYNAMIC.search(text)


def test_open_creates_every_scidb_table_and_reopen_is_a_no_op(tmp_path, caplog):
    path = tmp_path / "registry.duckdb"
    db = configure_database(path, ["subject"])
    names = {r[0] for r in db._duck.fetchall("SELECT table_name FROM information_schema.tables")}
    db.close()
    expected = {d.name for d in tables.declared() if d.owner == "scidb"}
    assert expected <= names
    assert {"_schema", "_variables", "_intent", "_record"} <= expected

    db = configure_database(path, ["subject"])  # existing database: every statement IF NOT EXISTS
    db.close()


def test_read_only_open_runs_no_ddl(tmp_path):
    path = tmp_path / "ro.duckdb"
    SciDuck(path, dataset_schema=["subject"]).close()
    ro = SciDuck(path, dataset_schema=["subject"], read_only=True)
    try:
        assert tables.ensure_core(ro, ["subject"]) == 0
    finally:
        ro.close()


def test_conflicting_declaration_is_refused():
    tables.declare(tables.TableDecl("_registry_probe", ("SELECT 1",), owner="tests"))
    tables.declare(tables.TableDecl("_registry_probe", ("SELECT 1",), owner="tests"))
    with pytest.raises(TableDeclarationError):
        tables.declare(tables.TableDecl("_registry_probe", ("SELECT 2",), owner="other"))


def test_ensure_of_an_undeclared_table_is_refused(tmp_path):
    duck = SciDuck(tmp_path / "x.duckdb", dataset_schema=["subject"])
    try:
        with pytest.raises(TableDeclarationError):
            tables.ensure(duck, "_never_declared")
    finally:
        duck.close()
