"""scidb owns the project database's DDL (decision D3; plan P3 stage 6).

Five packages used to create tables in one ``.duckdb`` with nobody owning
the list (design review 09, T2). Now every fixed table is a
``scidb.store.tables.TableDecl``: scidb's own are created on every writable open,
other packages' through ``tables.ensure`` on first use. Guards:

* no ``CREATE TABLE`` / ``SEQUENCE`` / ``INDEX`` outside the files listed in
  ``ALLOWED`` (each with its reason);
* opening a database creates every scidb table, and reopening it is a no-op;
* a second, different declaration of a taken name is refused.
"""

import re
from pathlib import Path

import pytest

from scidb import configure_database
from scidb.store import tables
from scidb.model.exceptions import TableDeclarationError
from scidb.store.sciduck import SciDuck

REPO = Path(__file__).resolve().parents[2]
SCANNED = [
    REPO / "scidb" / "src",
    REPO / "sciduckdb" / "src",
    REPO / "scistackplotdb" / "src",
    REPO / "scistackstatsdb" / "src",
    REPO / "scistack-gui" / "scistack_gui",
]
ALLOWED = {
    "scidb/src/scidb/store/tables.py": "the registry: every scidb table",
    "scidb/src/scidb/store/versioned_store.py": "declares each store's table via TableDecl",
    "scidb/src/scidb/store/database.py": "the per-variable <Type>_data tables (dynamic)",
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
    text = (REPO / "scidb/src/scidb/store/database.py").read_text(encoding="utf-8")
    fixed = [m for m in re.finditer(r"CREATE TABLE IF NOT EXISTS", text)]
    assert fixed == [], "a fixed table belongs in scidb.store.tables"
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


# --- Classification: bundle sections and clear_data (P3 stage 6 step 7) ------

#: The classification as it stood when bundle.py and clear_data.py still kept
#: their own lists, plus schema key types (2026-11-01). Pinned so a change
#: is on purpose.
_HISTORY = {
    "_schema", "_record", "_constant", "_invocation", "_function_source",
    "_invocation_input", "_invocation_output", "_invocation_pooled", "_run", "_run_invocation",
    "_run_step", "_run_outputs", "_function_rename", "_variant_pin",
    "_variant_tombstone", "__scidb_schema_overrides",
    "_schema_key_type",  # dataset intent since 2026-11-01 (was in no section)
}
_INTENT = {"__scidb_schema_overrides", "_schema_key_type"}
_DATA = {"_registered_types", "_variables", "_variable_groups", "_record_save"}
_CLEARED = {
    "_record", "_constant", "_invocation", "_function_source",
    "_invocation_input", "_invocation_output", "_invocation_pooled", "_run_invocation",
    "_function_rename", "_record_save",
}


def test_classification_is_unchanged():
    assert set(tables.history_tables()) == _HISTORY
    assert set(tables.intent_tables()) == _INTENT
    assert set(tables.data_tables()) == _DATA
    assert set(tables.cleared_tables()) == _CLEARED
    core = {d.name for d in tables.declared() if d.owner == tables.CORE_OWNER}
    assert set(tables.kept_tables()) == core - _CLEARED


def test_section_constants_do_not_shadow_table_decls():
    """``tables.INTENT`` is the ``_intent`` declaration; a section constant
    under that name was overwritten by it and emptied ``intent_tables()``."""
    assert tables.INTENT.name == "_intent"
    assert tables.BUNDLE_INTENT == "intent"
    assert tables.intent_tables() == ("__scidb_schema_overrides", "_schema_key_type")


def test_every_scidb_table_is_cleared_xor_kept_with_a_reason():
    for d in tables.declared():
        if d.owner == tables.CORE_OWNER:
            assert d.cleared != bool(d.kept_reason), d.name


def test_bundle_and_clear_read_the_registry():
    from scidb.store import clear_data
    from scidb.project import bundle

    assert bundle.HISTORY_TABLES == tables.history_tables()
    assert bundle.INTENT_TABLES == tables.intent_tables()
    assert clear_data.CLEARED_TABLES == tables.cleared_tables()
    assert clear_data.KEPT_TABLES == tables.kept_tables()
    for name in ("HISTORY_TABLES", "INTENT_TABLES", "CLEARED_TABLES", "KEPT_TABLES"):
        for path in ("scidb/src/scidb/project/bundle.py", "scidb/src/scidb/store/clear_data.py"):
            text = (REPO / path).read_text(encoding="utf-8")
            assert not re.search(rf"^{name}\b[^=]*= *[\(\{{\[]", text, re.M), (
                f"{path} restates {name}; read it from scidb.store.tables"
            )


@pytest.mark.parametrize(
    "kwargs",
    [
        {},  # neither cleared nor kept
        {"cleared": True, "kept_reason": "both"},
        {"bundle": "archive", "kept_reason": "x"},  # unknown section
    ],
)
def test_contradictory_scidb_classification_is_refused(kwargs):
    with pytest.raises(TableDeclarationError):
        tables.declare(tables.TableDecl("_probe_cls", ("SELECT 1",), owner="scidb", **kwargs))
    assert "_probe_cls" not in {d.name for d in tables.declared()}


def test_other_owners_carry_no_classification():
    with pytest.raises(TableDeclarationError):
        tables.declare(
            tables.TableDecl("_probe_gui", ("SELECT 1",), owner="scistack_gui", cleared=True)
        )
    tables.declare(tables.TableDecl("_probe_gui_ok", ("SELECT 1",), owner="scistack_gui"))
    assert "_probe_gui_ok" not in tables.kept_tables()
