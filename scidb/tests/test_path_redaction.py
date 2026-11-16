"""Hiding PathInput locations at export (issue #16, plan-pathinput-anonymity).

The promises: with ``hide_path_locations`` the exporter's folders are in NO
bundle member (declarations, config, manifest, history text), PathInput names
and template shapes survive, a folder left in code refuses the export and
writes nothing, and the default export is unchanged.
"""

from __future__ import annotations

import json
import zipfile

import duckdb
import pytest

from scidb.project import bundle
from scidb.project.bundle import (
    BundleError, ExportOptions, ExportReport, export_project, import_project, read_bundle,
)
from scidb.project.init import init_project

import scifor as _scifor
from scifor.pathinput import PathInput, split_template_location

from test_bundle import CodeLikeSection, SCHEMA

ROOT_FOLDER = "/exporter/data"
ENTITIES = (
    'variables = []\n\n[parameters]\n\n[path_inputs]\n'
    f'RAW = {{ template = "{{subject}}/{{session}}.csv", root_folder = "{ROOT_FOLDER}" }}\n'
    'ABS = { template = "/Volumes/Lab/Study/{subject}/a.csv", '
    'aliases = { subject = { S1 = ["Subj1"] } }, key_regex = { subject = "S\\\\d+" } }\n'
    'NOTES = "{subject}.txt"\n'
)


class EntitiesSection(CodeLikeSection):
    def __init__(self, extra=None):
        super().__init__()
        self.extra = extra or {}

    def export(self, ctx):
        return {"src/gait/scistack_entities.toml": ENTITIES.encode(), **self.extra}


@pytest.fixture
def project(tmp_path):
    from scidb import configure_database
    from scidb.store.database import clear_current_database

    _scifor.set_schema([])
    root = tmp_path / "gait"
    init_project(root)
    db = configure_database(root / "gait.duckdb", SCHEMA)
    (root / "scistack.toml").write_text(
        f'modules = ["{root.resolve().as_posix()}/scripts"]\n'
        'entities_file = "src/gait/scistack_entities.toml"\n'
    )
    # What history holds: a PathInput spec with its root, and free text quoting a path.
    spec = PathInput("{subject}/{session}.csv", name="RAW", root_folder=ROOT_FOLDER).to_spec()
    db._duck.execute(
        "INSERT INTO _constant VALUES ('r1', ?, 'PathInput', 'h1'), ('r2', ?, 'str', 'h2')",
        [spec, f"could not open {ROOT_FOLDER}/S01/BL.csv under {root.resolve().as_posix()}"],
    )
    yield root, db
    db.close()
    clear_current_database()
    _scifor.set_schema([])


def _export(project, tmp_path, *, hide, section=None, report=None):
    root, db = project
    return export_project(
        root, db, tmp_path / "out" / "gait",
        options=ExportOptions(hide_path_locations=hide),
        providers=[section or EntitiesSection()],
        report=report,
    )


def _all_text(out) -> str:
    """Every member that is text, joined; the Parquet members are read as tables."""
    with zipfile.ZipFile(out) as zf:
        return "\n".join(
            zf.read(n).decode("utf-8") for n in zf.namelist() if not n.endswith(".parquet")
        )


def _constant_rows(out) -> dict:
    b = read_bundle(out)
    path = out.parent / "constant.parquet"
    path.write_bytes(b.sections["history"]["_constant.parquet"])
    return dict(duckdb.connect().execute(
        "SELECT record_id, value_repr FROM read_parquet(?)", [path.as_posix()]
    ).fetchall())


def test_split_template_location():
    assert split_template_location("/Volumes/Lab/Study/{subject}/a.csv") == (
        "/Volumes/Lab/Study", "{subject}/a.csv")
    assert split_template_location("Y:\\Lab\\{subject}\\a.csv") == ("Y:/Lab", "{subject}/a.csv")
    assert split_template_location("//srv/share/x/{subject}.csv") == ("//srv/share/x", "{subject}.csv")
    assert split_template_location("/data/all/a.csv") == ("/data/all", "a.csv")
    assert split_template_location("{subject}/a.csv") == ("", "{subject}/a.csv")


def test_without_location_keeps_the_matching_options_and_identity():
    pi = PathInput(
        "/Volumes/Lab/{subject}/a.csv", name="X", root_folder="/r",
        aliases={"subject": {"S1": ["one"]}}, key_regex={"subject": r"S\d+"},
    )
    hidden = pi.without_location()
    assert hidden.root_folder is None and hidden.path_template == "{subject}/a.csv"
    assert hidden.aliases == pi.aliases and hidden.key_regex == pi.key_regex
    assert hidden.to_key() == pi.to_key()


def test_hide_removes_every_folder_but_keeps_names_and_shapes(project, tmp_path):
    root, _ = project
    report = ExportReport()
    out = _export(project, tmp_path, hide=True, report=report)

    text = _all_text(out)
    for secret in (ROOT_FOLDER, "/Volumes/Lab", root.resolve().as_posix()):
        assert secret not in text
    constants = _constant_rows(out)
    for secret in (ROOT_FOLDER, root.resolve().as_posix()):
        assert all(secret not in v for v in constants.values())

    entities = bundle._toml_loads(read_bundle(out).sections["code"]["src/gait/scistack_entities.toml"].decode())
    pis = entities["path_inputs"]
    assert pis["RAW"] == "{subject}/{session}.csv"
    assert pis["ABS"]["template"] == "{subject}/a.csv"
    assert pis["ABS"]["aliases"] == {"subject": {"S1": ["Subj1"]}}
    assert pis["ABS"]["key_regex"] == {"subject": "S\\d+"}
    assert pis["NOTES"] == "{subject}.txt"

    spec = json.loads(constants["r1"])
    assert spec["name"] == "RAW" and spec["root_folder"] is None
    assert "<PathInput RAW>" in constants["r2"]

    manifest = read_bundle(out).manifest
    assert "root" not in manifest["project"]
    assert manifest["options"]["hide_path_locations"] is True
    assert {name for _, name in report.redaction["hidden"]} >= {"RAW", "ABS"}
    # The report never repeats the folders it hides.
    assert ROOT_FOLDER not in json.dumps(report.to_dict())


def test_preserve_is_unchanged(project, tmp_path):
    root, _ = project
    out = _export(project, tmp_path, hide=False)
    text = _all_text(out)
    assert ROOT_FOLDER in text
    assert read_bundle(out).manifest["project"]["root"] == root.resolve().as_posix()
    assert _constant_rows(out)["r1"].count(ROOT_FOLDER) == 1


def test_default_is_preserve():
    assert ExportOptions().hide_path_locations is False


def test_the_live_database_is_not_touched(project, tmp_path):
    _, db = project
    _export(project, tmp_path, hide=True)
    value = db._duck.fetchone("SELECT value_repr FROM _constant WHERE record_id = 'r1'")[0]
    assert ROOT_FOLDER in value


def test_a_folder_left_in_code_refuses_the_export_and_writes_nothing(project, tmp_path):
    section = EntitiesSection(extra={"src/gait/inputs.py": f'DATA = "{ROOT_FOLDER}/raw"\n'.encode()})
    with pytest.raises(BundleError) as err:
        _export(project, tmp_path, hide=True, section=section)
    assert "src/gait/inputs.py" in str(err.value) and "PathInput RAW" in str(err.value)
    # The refusal names the place, not the folder.
    assert ROOT_FOLDER not in str(err.value)
    assert not (tmp_path / "out").exists() or not list((tmp_path / "out").iterdir())


def test_import_of_a_hidden_bundle_flags_each_path_input_and_path_root_fills_it(project, tmp_path):
    out = _export(project, tmp_path, hide=True)
    project[1].close()
    report = import_project(
        out, tmp_path / "copy", providers=[EntitiesSection()], path_roots={"RAW": "/mine/raw"},
    )
    pi = report.sections["path_inputs"]
    assert "RAW" not in pi["roots_hidden"]
    assert set(pi["roots_hidden"]) == {"ABS", "NOTES"}
    entities = bundle._toml_loads((tmp_path / "copy/src/gait/scistack_entities.toml").read_text())
    assert entities["path_inputs"]["RAW"]["root_folder"] == "/mine/raw"
    assert entities["path_inputs"]["ABS"]["key_regex"] == {"subject": "S\\d+"}


def test_preview_says_paths_were_hidden(project, tmp_path):
    out = _export(project, tmp_path, hide=True)
    info = bundle.preview(out, providers=[EntitiesSection()])
    assert info["paths_hidden"] is True and info["exported_root"] is None

