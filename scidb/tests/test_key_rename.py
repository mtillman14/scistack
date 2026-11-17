"""Renaming a schema key everywhere the project names it
(``scidb.project.key_rename``; ``.claude/plan-schema-keys-entry-point.md``;
docs/claude/changing-schema-keys.md).

Pinned, shape by shape (``scidb.project.schema_map.KeyMap``) and then through
the files and the database:

* a RENAMED key follows into every reference: a stated level, a location
  selection, ``where=`` filter columns, PathInput templates / aliases /
  key_regex, ``[library] schema_keys``, the config's ``[schema_keys]`` /
  ``[aliases]`` / ``[colors]``, and any plot/stats envelope string;
* a DELETED key is *retired*: every reference is kept as written and flagged
  (never dropped);
* adding keys changes no file at all (byte for byte);
* the import route (``KeyMap.auto``) still drops, and now also renames
  PathInput ``key_regex`` (it used to be skipped).
"""

from __future__ import annotations

import pytest
import scifor as _scifor

from scidb import configure_database
from scidb.project.key_rename import rename_key_references
from scidb.project.schema_map import KeyMap, MapReport, SCHEMA_KEYED_CONFIG_TABLES
from scidb.store.schema_keys_change import change_schema_keys


def _km(renames=None, retired=()):
    return KeyMap.renaming(renames or {}, retired)


# --- the shapes, in place --------------------------------------------------


def test_renaming_with_nothing_to_do_is_identity_and_returns_inputs():
    km = _km({"a": "a"})
    assert km.is_identity
    report = MapReport()
    assert km.level(["a"], "w", report) == ["a"]
    assert km.locations({"a": ["1"]}, "w", report) == {"a": ["1"]}
    assert report.flagged == [] and report.dropped == []


def test_retired_alone_is_not_identity():
    assert not _km({}, ["trial"]).is_identity


@pytest.mark.parametrize(
    "stated, expected",
    [
        (None, None),
        ([], []),
        (["subject", "visit"], ["subject", "session"]),
        ("visit", ["session"]),
        (["subject"], ["subject"]),
    ],
)
def test_level_renames(stated, expected):
    report = MapReport()
    assert _km({"visit": "session"}).level(stated, "node f", report) == expected
    assert report.dropped == []


def test_level_keeps_and_flags_a_retired_key():
    report = MapReport()
    assert _km({}, ["trial"]).level(["subject", "trial"], "node f", report) == ["subject", "trial"]
    assert report.dropped == []
    assert report.flagged == [("node f", "names 'trial', which is no longer a schema key")]


#: A location selection in its stored shape (``scifor.LocationFilter.to_dict``):
#: ragged ``include`` prefixes plus a per-key ``exclude_levels`` rule.
SELECTION = {
    "include": [[["subject", "S1"], ["visit", "BL"]], [["subject", "S2"]]],
    "exclude_levels": {"visit": ["FU"]},
}


def test_locations_rename_in_place_is_not_flagged():
    report = MapReport()
    out = _km({"visit": "session"}).locations(SELECTION, "node f", report)
    assert out == {
        "include": [[["subject", "S1"], ["session", "BL"]], [["subject", "S2"]]],
        "exclude_levels": {"session": ["FU"]},
    }
    assert report.flagged == []


def test_locations_on_import_are_flagged():
    km = KeyMap.auto(["subject", "visit"], ["subject", "session"], {"visit": "session"})
    report = MapReport()
    out = km.locations(SELECTION, "node f", report)
    assert out["exclude_levels"] == {"session": ["FU"]}
    assert len(report.flagged) == 1 and "exporter" in report.flagged[0][1]


def test_locations_keep_a_retired_key_and_flag_it():
    report = MapReport()
    assert _km({}, ["visit"]).locations(SELECTION, "node f", report) == SELECTION
    assert report.flagged == [("node f", "names 'visit', which is no longer a schema key")]


def test_locations_that_name_no_renamed_key_come_back_as_stored():
    raw = {"include": [[["subject", "S1"]]], "exclude_levels": {}}
    assert _km({"visit": "session"}).locations(raw, "node f", MapReport()) is raw


def test_template_renames_placeholders_and_flags_retired():
    report = MapReport()
    km = _km({"visit": "session"}, ["trial"])
    out = km.template("{subject}/{visit}/{trial}_{visit}.csv", "PathInput P", report)
    assert out == "{subject}/{session}/{trial}_{session}.csv"
    assert report.flagged == [("PathInput P", "names 'trial', which is no longer a schema key")]


def test_table_renames_keys_keeps_others_and_retired():
    report = MapReport()
    table = {"visit": ["BL", "FU"], "Demographics.Sex": {"name": "Sex"}, "trial": ["1"]}
    out = _km({"visit": "session"}, ["trial"]).table(table, "scistack.toml [x]", report)
    assert out == {"session": ["BL", "FU"], "Demographics.Sex": {"name": "Sex"}, "trial": ["1"]}
    assert [w for w, _ in report.flagged] == ["scistack.toml [x]"]


def test_config_tables_touch_only_the_key_named_tables():
    assert SCHEMA_KEYED_CONFIG_TABLES == ("schema_keys", "aliases", "colors")
    section = {
        "modules": ["visit.py"],  # a string equal to nothing; never a key reference
        "schema_keys": {"visit": ["BL", "FU"]},
        "aliases": {"visit": {"name": "Visit"}},
        "colors": {"default": "#333333", "visit": {"BL": "#0072b2"}},
        "outputs": {"folder": "visit"},
    }
    out = _km({"visit": "session"}).config_tables(section, MapReport())
    assert out == {
        "modules": ["visit.py"],
        "schema_keys": {"session": ["BL", "FU"]},
        "aliases": {"session": {"name": "Visit"}},
        "colors": {"default": "#333333", "session": {"BL": "#0072b2"}},
        "outputs": {"folder": "visit"},
    }


def test_path_input_arm_moves_aliases_and_key_regex_with_the_placeholder():
    arm = {
        "template": "{subject}_{visit}.csv",
        "root_folder": "/data",
        "aliases": {"visit": {"BL": ["Baseline"]}},
        "key_regex": {"subject": "S\\d+", "visit": "[A-Z]+"},
    }
    out = _km({"visit": "session"}).path_input_arm(arm, "PathInput P", MapReport())
    assert out == {
        "template": "{subject}_{session}.csv",
        "root_folder": "/data",
        "aliases": {"session": {"BL": ["Baseline"]}},
        "key_regex": {"subject": "S\\d+", "session": "[A-Z]+"},
    }


def test_path_input_arm_on_import_keeps_an_unmapped_key_under_its_old_name():
    km = KeyMap.auto(["subject", "visit"], ["subject"])  # visit -> None
    arm = {"template": "{visit}.csv", "aliases": {"visit": {"a": ["b"]}}, "key_regex": {"visit": "x"}}
    report = MapReport()
    out = km.path_input_arm(arm, "PathInput P", report)
    assert out["template"] == "{visit}.csv"
    assert out["aliases"] == {"visit": {"a": ["b"]}}
    assert out["key_regex"] == {"visit": "x"}
    assert report.flagged  # the placeholder has no counterpart


def test_key_list_renames_and_keeps_retired():
    report = MapReport()
    out = _km({"visit": "session"}, ["trial"]).key_list(["subject", "visit", "trial"], "lib", report)
    assert out == ["subject", "session", "trial"]
    assert len(report.flagged) == 1


def test_where_filters_rename_only_a_key_column():
    rows = [
        {"variable": "Score", "column": "visit", "op": "==", "value": "'BL'"},
        {"variable": "Score", "column": "value", "op": ">", "value": "3"},
        {"variable": "visit", "column": None, "op": "==", "value": "'1'"},  # a variable named like a key
        {"variable": "", "column": "", "op": "==", "value": ""},  # a blank row the user added
    ]
    out = _km({"visit": "session"}).where_filters(rows, "node f", MapReport())
    assert out[0]["column"] == "session"
    assert out[1:] == rows[1:]


def test_where_filters_keep_a_retired_column_and_flag_it():
    rows = [{"variable": "Score", "column": "trial", "op": "==", "value": "1"}]
    report = MapReport()
    assert _km({}, ["trial"]).where_filters(rows, "node f", report) == rows
    assert report.flagged == [("node f", "names 'trial', which is no longer a schema key")]


def test_exact_strings_rename_and_flag():
    spec = {"x": "visit", "hue": "subject", "facets": ["visit"], "order": {"visit": ["BL"]}, "n": 3}
    report = MapReport()
    out = _km({"visit": "session"}, ["trial"]).exact_strings(spec, "saved plot 'Fig'", report)
    assert out == {"x": "session", "hue": "subject", "facets": ["session"], "order": {"session": ["BL"]}, "n": 3}
    assert [why for _, why in report.flagged] == ["mentions schema keys ['visit']; review it"]


# --- the files ----------------------------------------------------------------

CONFIG = """\
modules = ["src/proj"]

[schema_keys]
visit = ["BL", "FU"]
subject = ["S2", "S1"]

[aliases.visit]
name = "Visit"

[aliases.visit.levels]
"BL" = "Baseline"

[colors]
default = "#333333"

[colors.visit]
"BL" = "#0072b2"
"""

ENTITIES = """\
# my entities -- this comment must survive
[parameters]
Cutoff = [5]

[path_inputs]
RAW = "{subject}/{visit}/raw.csv"
XS = { template = "{visit}/x_{subject}.xlsx", aliases = { visit = { BL = ["Baseline"] } }, key_regex = { visit = '[A-Z]+' } }
ALT = ["{subject}_{visit}.csv", { template = "{subject}/{visit}.txt" }]
OTHER = "{subject}/notes.txt"

[library]
schema_keys = ["subject", "visit"]
"""


@pytest.fixture
def project(tmp_path):
    (tmp_path / "scistack.toml").write_text(
        CONFIG.replace('modules = ["src/proj"]', 'modules = ["src/proj"]\nentities_file = "entities.toml"'),
        encoding="utf-8",
    )
    (tmp_path / "entities.toml").write_text(ENTITIES, encoding="utf-8")
    return tmp_path


def _toml(path):
    from scidb.project.bundle import _toml_loads  # tomllib, or tomli on 3.10

    return _toml_loads(path.read_text(encoding="utf-8"))


def test_a_rename_reaches_the_config_tables(project):
    out = rename_key_references(None, project, {"visit": "session"})
    cfg = _toml(project / "scistack.toml")
    assert cfg["schema_keys"] == {"session": ["BL", "FU"], "subject": ["S2", "S1"]}
    assert cfg["aliases"] == {"session": {"name": "Visit", "levels": {"BL": "Baseline"}}}
    assert cfg["colors"] == {"default": "#333333", "session": {"BL": "#0072b2"}}
    assert cfg["modules"] == ["src/proj"] and cfg["entities_file"] == "entities.toml"
    assert sorted(out["config"]) == ["aliases", "colors", "schema_keys"]


def test_a_rename_reaches_every_path_input_arm_and_the_library_table(project):
    out = rename_key_references(None, project, {"visit": "session"})
    text = (project / "entities.toml").read_text(encoding="utf-8")
    ent = _toml(project / "entities.toml")
    assert "# my entities -- this comment must survive" in text
    assert ent["parameters"] == {"Cutoff": [5]}
    pis = ent["path_inputs"]
    assert pis["RAW"] == "{subject}/{session}/raw.csv"
    assert pis["XS"]["template"] == "{session}/x_{subject}.xlsx"
    assert pis["XS"]["aliases"] == {"session": {"BL": ["Baseline"]}}
    assert pis["XS"]["key_regex"] == {"session": "[A-Z]+"}
    alt = [a if isinstance(a, str) else a["template"] for a in pis["ALT"]]
    assert alt == ["{subject}_{session}.csv", "{subject}/{session}.txt"]
    assert pis["OTHER"] == "{subject}/notes.txt"
    assert ent["library"]["schema_keys"] == ["subject", "session"]
    assert sorted(out["entities"]) == ["ALT", "RAW", "XS", "[library] schema_keys"]


def test_a_deleted_key_is_kept_everywhere_and_reported(project):
    before_cfg = (project / "scistack.toml").read_bytes()
    before_ent = (project / "entities.toml").read_bytes()
    out = rename_key_references(None, project, {}, ["visit"])
    assert (project / "scistack.toml").read_bytes() == before_cfg
    assert (project / "entities.toml").read_bytes() == before_ent
    wheres = {w for w, _ in out["flagged"]}
    assert {"scistack.toml [schema_keys]", "scistack.toml [aliases]", "scistack.toml [colors]"} <= wheres
    assert {"PathInput RAW", "PathInput XS", "PathInput ALT"} <= wheres
    assert "entities.toml [library] schema_keys" in wheres
    assert out["dropped"] == []


def test_adding_keys_changes_no_file(project):
    before = {p: p.read_bytes() for p in project.iterdir()}
    out = rename_key_references(None, project, {}, [])
    assert {p: p.read_bytes() for p in project.iterdir()} == before
    assert out == {"dropped": [], "flagged": []}


def test_a_project_with_no_config_and_no_entities_file_is_fine(tmp_path):
    out = rename_key_references(None, tmp_path, {"visit": "session"})
    assert out["config"] == [] and out["entities"] == []
    assert list(tmp_path.iterdir()) == []


def test_providers_are_called_with_the_context_and_reported(project):
    seen = {}

    class Fake:
        name = "fake"

        def rename_keys(self, ctx):
            seen["map"] = ctx.key_map.as_dict()
            seen["root"] = ctx.root
            ctx.map_report.flagged.append(("fake thing", "looked"))
            return {"rows": 2}

    class NoHook:
        name = "nohook"

    out = rename_key_references("DB", project, {"visit": "session"}, providers=[Fake(), NoHook()])
    assert seen == {"map": {"visit": "session"}, "root": project}
    assert out["fake"] == {"rows": 2}
    assert "nohook" not in out
    assert ["fake thing", "looked"] in out["flagged"]


# --- through change_schema_keys --------------------------------------------------


@pytest.fixture
def db_path(tmp_path):
    _scifor.set_schema([])
    yield tmp_path / "k.duckdb"
    _scifor.set_schema([])


def test_change_schema_keys_renames_the_files_too(project, db_path):
    db = configure_database(db_path, ["subject", "visit"])
    result = change_schema_keys(db, ["subject", "session"], renames={"visit": "session"}, root=project)
    assert result.new_keys == ["subject", "session"]
    assert _toml(project / "scistack.toml")["schema_keys"] == {"session": ["BL", "FU"], "subject": ["S2", "S1"]}
    assert _toml(project / "entities.toml")["path_inputs"]["RAW"] == "{subject}/{session}/raw.csv"
    assert result.references["config"] and result.references["entities"]
    assert result.leftovers == []


def test_change_schema_keys_deleting_a_key_leaves_files_and_lists_leftovers(project, db_path):
    db = configure_database(db_path, ["subject", "visit"])
    result = change_schema_keys(db, ["subject"], root=project)
    assert result.deleted_keys == ["visit"]
    assert _toml(project / "scistack.toml")["schema_keys"]["visit"] == ["BL", "FU"]
    assert any(line.startswith("scistack.toml [aliases]: names 'visit'") for line in result.leftovers)
    assert any(line.startswith("PathInput RAW: names 'visit'") for line in result.leftovers)


def test_adding_trial_and_cycle_to_an_empty_dataset_touches_no_file(project, db_path):
    """The user's case: [subject, session] -> [subject, session, trial, cycle]."""
    db = configure_database(db_path, ["subject", "session"])
    before = {p: p.read_bytes() for p in project.iterdir() if p.suffix == ".toml"}
    result = change_schema_keys(db, ["subject", "session", "trial", "cycle"], root=project)
    assert result.new_keys == ["subject", "session", "trial", "cycle"]
    assert result.deleted_keys == [] and result.leftovers == []
    assert {p: p.read_bytes() for p in project.iterdir() if p.suffix == ".toml"} == before
    _scifor.set_schema([])
    reopened = configure_database(db_path, ["subject", "session", "trial", "cycle"])
    assert reopened.dataset_schema_keys == ["subject", "session", "trial", "cycle"]
    reopened.close()


def test_one_failing_place_does_not_stop_the_others(project):
    class Broken:
        name = "broken"

        def rename_keys(self, ctx):
            raise RuntimeError("boom")

    class After:
        name = "after"

        def rename_keys(self, ctx):
            return {"rows": 1}

    out = rename_key_references(None, project, {"visit": "session"}, providers=[Broken(), After()])
    assert out["broken"] == {"error": "boom"}
    assert out["after"] == {"rows": 1}
    assert sorted(out["config"]) == ["aliases", "colors", "schema_keys"]
    assert ["broken", "could not be renamed (boom); it still names the old key"] in out["flagged"]


def test_a_failure_renaming_references_keeps_the_key_change_and_says_so(project, db_path):
    db = configure_database(db_path, ["subject", "visit"])

    class Broken:
        name = "broken"

        def rename_keys(self, ctx):
            raise RuntimeError("boom")

    result = change_schema_keys(
        db, ["subject", "session"], renames={"visit": "session"}, root=project, providers=[Broken()]
    )
    assert result.new_keys == ["subject", "session"]
    assert "broken: could not be renamed (boom); it still names the old key" in result.leftovers
    # the other places still renamed
    assert _toml(project / "scistack.toml")["schema_keys"] == {"session": ["BL", "FU"], "subject": ["S2", "S1"]}
    _scifor.set_schema([])
    reopened = configure_database(db_path, ["subject", "session"])
    assert reopened.dataset_schema_keys == ["subject", "session"]
    reopened.close()
