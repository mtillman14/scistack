"""The header's "Schema keys…" entry point, and a key change reaching every
place the project names a key (``schema_keys_service``,
``scidb.project.key_rename``, each bundle section's ``rename_keys``;
.claude/plan-schema-keys-entry-point.md; docs/claude/changing-schema-keys.md).

One project holds a reference to ``visit`` in every place there is:

* ``scistack.toml``: ``[schema_keys]``, ``[aliases]``, ``[colors]``;
* the entities file: a PathInput's template, aliases and key_regex;
* node settings (the ``schema_location`` statement): stated level, location
  selection, ``where=`` filter column -- on the root canvas, on a
  hypothesis canvas, in a legacy ``global`` row, and inside a LIBRARY
  pipeline (which speaks its own keys and must be left alone);
* submodule bindings: a project child (both sides renamed) and a library
  child (only the project side renamed), plus ``iterate`` overrides;
* saved plots and saved stats, every version;
* a PathInput declared in CODE (never edited; reported).

Then: a RENAME reaches all of them; a DELETE changes none and reports each;
ADDING keys (the user's [subject, session] -> + trial, cycle) changes none.
"""

from __future__ import annotations

import json

import pytest
import scifor as _scifor
from scifor import PathInput

from scidb import configure_database
from scidb.provenance.intent import ASPECT_SCHEMA_LOCATION, GLOBAL_SCOPE, Statement, SUBJECT_CALL_SITE
from scistack_gui.ids import ROOT_SCOPE
from scistack_gui.project import registry as _registry
from scistack_gui.runtime import db as _gui_db
from scistack_gui.services import schema_keys_service
from scistack_gui.stores import intent_store, library_lock
from scistack_gui.stores import node_wiring
from scistack_gui.stores import pipeline_store as ps

CONFIG = """\
entities_file = "entities.toml"

[schema_keys]
visit = ["BL", "FU"]

[aliases.visit]
name = "Visit"

[colors.visit]
"BL" = "#0072b2"
"""

ENTITIES = """\
# kept
[path_inputs]
RAW = { template = "{subject}/{visit}/raw.csv", aliases = { visit = { BL = ["Baseline"] } }, key_regex = { visit = '[A-Z]+' } }
"""

LOCATION = {
    "schemaLevel": ["subject", "visit"],
    # The stored shape (scifor.LocationFilter.to_dict).
    "schemaSelection": {
        "include": [[["subject", "S1"], ["visit", "BL"]]],
        "exclude_levels": {"visit": ["FU"]},
    },
    "whereFilters": [
        {"variable": "Score", "column": "visit", "op": "==", "value": "'BL'"},
        {"variable": "Score", "column": "value", "op": ">", "value": "3"},
    ],
}


def _statement(node, scope, value=LOCATION):
    return Statement(
        subject_kind=SUBJECT_CALL_SITE, subject_ref=node, aspect=ASPECT_SCHEMA_LOCATION,
        value=json.loads(json.dumps(value)), scope=scope,
    )


@pytest.fixture
def project(tmp_path, monkeypatch):
    """An empty dataset [subject, visit] whose project names ``visit`` everywhere.
    The project root is pinned to tmp_path by conftest's autouse fixture."""
    from scistackplotdb.saved import STORE as PLOTS
    from scistackstatsdb.saved import STORE as STATS

    (tmp_path / "scistack.toml").write_text(CONFIG, encoding="utf-8")
    (tmp_path / "entities.toml").write_text(ENTITIES, encoding="utf-8")
    _scifor.set_schema([])
    path = tmp_path / "keys.duckdb"
    db = configure_database(path, ["subject", "visit"])
    _gui_db._db = db
    _gui_db._db_path = path
    ps._ensure_tables(db)

    nodes = {
        "root": str(node_wiring.mint_node_id("on_root")),
        "hyp": str(node_wiring.mint_node_id("on_hyp")),
        "legacy": str(node_wiring.mint_node_id("legacy")),
        "lib": str(node_wiring.mint_node_id("in_lib")),
    }
    hyp = ps.create_pipeline(db, "Hypothesis A")
    child = ps.create_pipeline(db, "Child")
    lib = ps.create_pipeline(db, "Lib pipeline")
    ps.set_library_owner(db, lib, "mylib", "lib_pipe", "doc1", "hash1")
    intent_store.put_statements(db, [
        _statement(nodes["root"], ROOT_SCOPE),
        _statement(nodes["hyp"], hyp),
        _statement(nodes["legacy"], GLOBAL_SCOPE),
    ])
    with library_lock.library_writes():
        intent_store.put_statements(db, [_statement(nodes["lib"], lib)])

    uses = {
        "child": ps.add_pipeline_use(
            db, ROOT_SCOPE, child, {"key_map": {"visit": "visit"}, "iterate": {"visit": ["BL"]}}
        ),
        "lib": ps.add_pipeline_use(db, ROOT_SCOPE, lib, {"key_map": {"visit": "visit", "subj": "subject"}}),
    }

    def _clash(_row):
        return RuntimeError("clash")

    PLOTS.save(db, "Score", "Fig", {"spec": {"x": "visit", "hue": "subject"}}, overwrite=True,
               current_id=None, on_clash=_clash)
    PLOTS.save(db, "Score", "Fig", {"spec": {"x": "visit", "facet": ["visit"]}}, overwrite=True,
               current_id=None, on_clash=_clash)
    STATS.save(db, None, "T1", {"spec": {"factors": ["visit"]}}, overwrite=True,
               current_id=None, on_clash=_clash)

    _registry._path_inputs["CodePI"] = PathInput("{subject}/{visit}.csv", name="CodePI", root_folder=".")
    _registry._path_input_sources["CodePI"] = "module:proj.paths"
    _registry._path_inputs["TomlPI"] = PathInput("{visit}.csv", name="TomlPI", root_folder=".")
    _registry._path_input_sources["TomlPI"] = str(tmp_path / "entities.toml")
    reloads = []
    monkeypatch.setattr(_registry, "reload_entities_file", lambda: reloads.append(1))

    yield {"root": tmp_path, "nodes": nodes, "scopes": {"hyp": hyp, "lib": lib, "child": child},
           "uses": uses, "reloads": reloads}

    _gui_db.close_db()
    for name in ("CodePI", "TomlPI"):
        _registry._path_inputs.pop(name, None)
        _registry._path_input_sources.pop(name, None)
    _scifor.set_schema([])


def _location(db, node, scope):
    rows = intent_store.load_statements(db, aspect=ASPECT_SCHEMA_LOCATION, subject_refs=[node])
    return next(s.value for s in rows if s.scope == scope)


def _uses(db):
    return {u["use_id"]: u["binding"] for u in ps.get_pipeline_uses(db)}


def _envelopes(db):
    from scistackplotdb.saved import STORE as PLOTS
    from scistackstatsdb.saved import STORE as STATS

    return (
        [json.loads(r["envelope_json"]) for r in PLOTS.dump_rows(db)],
        [json.loads(r["envelope_json"]) for r in STATS.dump_rows(db)],
    )


def _toml(path):
    from scidb.project.bundle import _toml_loads

    return _toml_loads(path.read_text(encoding="utf-8"))


# --- the entry point -----------------------------------------------------------


def test_proposal_with_no_node_is_the_current_keys(project):
    db = _gui_db.get_db()
    p = schema_keys_service.schema_key_proposal(db, None, ["ignored", "columns"])
    assert p["existing"] == ["subject", "visit"]
    assert [x["key"] for x in p["proposed"]] == ["subject", "visit"]
    assert p["dataset_empty"] is True
    assert p["path_inputs"] == []


def test_the_api_request_takes_no_node(project):
    from scistack_gui.api.project import SchemaKeyProposalRequest, _schema_key_proposal

    req = SchemaKeyProposalRequest()
    assert req.node_id is None
    assert _schema_key_proposal(_gui_db.get_db(), req)["existing"] == ["subject", "visit"]


# --- a rename reaches every place ----------------------------------------------------


def test_a_rename_reaches_every_place(project):
    result = schema_keys_service.change_schema_keys(
        _gui_db.get_db(), ["subject", "session"], {"visit": "session"}
    )
    assert result["ok"] is True, result
    db = _gui_db.get_db()
    assert db.dataset_schema_keys == ["subject", "session"]
    root, nodes, scopes, uses = project["root"], project["nodes"], project["scopes"], project["uses"]

    # scistack.toml
    cfg = _toml(root / "scistack.toml")
    assert cfg["schema_keys"] == {"session": ["BL", "FU"]}
    assert cfg["aliases"] == {"session": {"name": "Visit"}}
    assert cfg["colors"] == {"session": {"BL": "#0072b2"}}
    assert cfg["entities_file"] == "entities.toml"

    # entities file, and the registry told to re-read it
    raw = _toml(root / "entities.toml")["path_inputs"]["RAW"]
    assert raw["template"] == "{subject}/{session}/raw.csv"
    assert raw["aliases"] == {"session": {"BL": ["Baseline"]}}
    assert raw["key_regex"] == {"session": "[A-Z]+"}
    assert "# kept" in (root / "entities.toml").read_text(encoding="utf-8")
    assert project["reloads"] == [1]

    # node settings on the root canvas, a hypothesis and a legacy global row
    renamed = {
        "schemaLevel": ["subject", "session"],
        "schemaSelection": {
            "include": [[["subject", "S1"], ["session", "BL"]]],
            "exclude_levels": {"session": ["FU"]},
        },
        "whereFilters": [
            {"variable": "Score", "column": "session", "op": "==", "value": "'BL'"},
            {"variable": "Score", "column": "value", "op": ">", "value": "3"},
        ],
    }
    assert _location(db, nodes["root"], ROOT_SCOPE) == renamed
    assert _location(db, nodes["hyp"], scopes["hyp"]) == renamed
    assert _location(db, nodes["legacy"], GLOBAL_SCOPE) == renamed
    # a library pipeline speaks its own keys: untouched
    assert _location(db, nodes["lib"], scopes["lib"]) == LOCATION

    # submodule bindings
    b = _uses(db)
    assert b[uses["child"]] == {"key_map": {"session": "session"}, "iterate": {"session": ["BL"]}}
    assert b[uses["lib"]] == {"key_map": {"visit": "session", "subj": "subject"}}

    # saved plots (both versions) and saved stats
    plots, stats = _envelopes(db)
    assert [p["spec"] for p in plots] == [
        {"x": "session", "hue": "subject"},
        {"x": "session", "facet": ["session"]},
    ]
    assert stats == [{"spec": {"factors": ["session"]}}]

    # what the dialog reports
    refs = result["references"]
    assert sorted(refs["config"]) == ["aliases", "colors", "schema_keys"]
    assert refs["entities"] == ["RAW"]
    assert refs["gui"] == {"statements": 3, "bindings": 2}
    assert sum(refs["plots"].values()) == 2 and sum(refs["stats"].values()) == 1
    assert result["renames"] == {"visit": "session"}

    # code is never edited: the code-declared PathInput is reported, the TOML one is not
    code_lines = [l for l in result["leftovers"] if l.startswith("PathInput ")]
    assert code_lines == ["PathInput CodePI in module:proj.paths still names {visit}; edit its template there."]


# --- a delete changes nothing and reports everything --------------------------------


def test_a_delete_keeps_every_reference_and_reports_each(project):
    root = project["root"]
    before_files = {p.name: p.read_bytes() for p in root.iterdir() if p.suffix == ".toml"}
    db = _gui_db.get_db()
    before_plots = _envelopes(db)

    result = schema_keys_service.change_schema_keys(db, ["subject"], {})
    assert result["ok"] is True, result
    db = _gui_db.get_db()
    assert db.dataset_schema_keys == ["subject"]
    assert {p.name: p.read_bytes() for p in root.iterdir() if p.suffix == ".toml"} == before_files
    assert _location(db, project["nodes"]["root"], ROOT_SCOPE) == LOCATION
    assert _envelopes(db) == before_plots
    assert project["reloads"] == []

    text = "\n".join(result["leftovers"])
    for where in (
        "scistack.toml [schema_keys]", "scistack.toml [aliases]", "scistack.toml [colors]",
        "PathInput RAW", f"node {project['nodes']['root']} ({ROOT_SCOPE})",
        f"submodule use {project['uses']['child']}", "saved plot 'Fig'", "saved stats table 'T1'",
        "PathInput CodePI",
    ):
        assert where in text, where
    assert f"node {project['nodes']['lib']}" not in text  # library pipelines are not ours


# --- adding keys: the user's case ---------------------------------------------------------


def test_adding_trial_and_cycle_changes_no_reference(project):
    root = project["root"]
    before_files = {p.name: p.read_bytes() for p in root.iterdir() if p.suffix == ".toml"}
    db = _gui_db.get_db()
    before_plots = _envelopes(db)

    result = schema_keys_service.change_schema_keys(db, ["subject", "visit", "trial", "cycle"], {})
    assert result["ok"] is True, result
    db = _gui_db.get_db()
    assert db.dataset_schema_keys == ["subject", "visit", "trial", "cycle"]
    assert {p.name: p.read_bytes() for p in root.iterdir() if p.suffix == ".toml"} == before_files
    assert _location(db, project["nodes"]["root"], ROOT_SCOPE) == LOCATION
    assert _envelopes(db) == before_plots
    assert result["leftovers"] == []
    assert result["references"] == {}


def test_one_unreadable_node_setting_is_named_and_the_rest_still_rename(project):
    db = _gui_db.get_db()
    bad = str(node_wiring.mint_node_id("bad_selection"))
    intent_store.put_statements(db, [_statement(
        bad, ROOT_SCOPE, {"schemaLevel": ["visit"], "schemaSelection": {"visit": ["BL"]}},
    )])
    result = schema_keys_service.change_schema_keys(db, ["subject", "session"], {"visit": "session"})
    assert result["ok"] is True, result
    db = _gui_db.get_db()
    assert _location(db, bad, ROOT_SCOPE)["schemaSelection"] == {"visit": ["BL"]}  # left as stored
    assert any(l.startswith(f"node {bad} ({ROOT_SCOPE}): settings could not be read") for l in result["leftovers"])
    assert _location(db, project["nodes"]["root"], ROOT_SCOPE)["schemaLevel"] == ["subject", "session"]
    assert project["reloads"] == [1]


def test_the_bundle_import_also_renames_where_filters(project):
    """The snapshot remap and the in-place rename share remap_location_value,
    so an import into another schema now renames where= columns too."""
    from scidb.project.schema_map import KeyMap, MapReport
    from scistack_gui.services.canvas_snapshot import remap_location_value

    km = KeyMap.auto(["subject", "visit"], ["subject", "session"], {"visit": "session"})
    out = remap_location_value(LOCATION, km, "node f", MapReport())
    assert out["whereFilters"][0]["column"] == "session"
    assert out["schemaLevel"] == ["subject", "session"]
