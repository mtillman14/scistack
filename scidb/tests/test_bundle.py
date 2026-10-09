"""scidb.bundle: the .scistack format, with a fake section provider.

The format's own promises: one owner of the export defaults, a manifest that
lists exactly what is in the zip with hashes, a reader that refuses a
tampered, incomplete or foreign bundle, and an import that only ever makes a
NEW project and reports a section it cannot import.
"""

from __future__ import annotations

import json
import zipfile

import pytest

import scifor as _scifor
from scidb import bundle
from scidb.bundle import BundleError, ExportOptions, export_project, import_project, read_bundle
from scidb.project import init_project

SCHEMA = ["subject", "session"]


class FakeSection:
    name = "fake"

    def __init__(self, files=None):
        self.files = files if files is not None else {"a.txt": b"hello", "sub/b.json": b"{}"}
        self.exported_ctx = None
        self.imported = None

    def export(self, ctx):
        self.exported_ctx = ctx
        return dict(self.files)

    def import_(self, ctx, files):
        self.imported = (ctx, dict(files))
        return {"files": len(files)}


@pytest.fixture
def project(tmp_path):
    from scidb import configure_database
    from scidb.database import clear_current_database

    _scifor.set_schema([])
    root = tmp_path / "gait"
    init_project(root)
    db = configure_database(root / "gait.duckdb", SCHEMA)
    yield root, db
    db.close()
    clear_current_database()
    _scifor.set_schema([])


def _export(project, tmp_path, providers=None, **kw):
    root, db = project
    return export_project(root, db, tmp_path / "out" / "gait", providers=providers, **kw)


class TestOptions:
    def test_defaults_are_the_decided_ones(self):
        o = ExportOptions()
        assert (o.include_history, o.include_data, o.include_wheelhouse) == (True, False, False)

    def test_round_trip_and_unknown_keys_ignored(self):
        o = ExportOptions(include_data=True)
        assert ExportOptions.from_dict({**o.to_dict(), "future": 1}) == o


class TestExport:
    def test_writes_a_zip_with_manifest_config_and_sections(self, project, tmp_path):
        out = _export(project, tmp_path, providers=[FakeSection()])
        assert out.suffix == ".scistack"
        with zipfile.ZipFile(out) as zf:
            names = set(zf.namelist())
            manifest = json.loads(zf.read("manifest.json"))
        assert names == {
            "manifest.json", "config/scistack.toml", "env/environment.json",
            "fake/a.txt", "fake/sub/b.json",
        }
        assert manifest["format_version"] == bundle.FORMAT_VERSION
        assert manifest["project"] == {
            "package": "gait",
            "database": "gait.duckdb",
            "schema_keys": SCHEMA,
            # Stage 6: lets import make the exporter's absolute config paths relative.
            "root": project[0].resolve().as_posix(),
        }
        assert manifest["options"] == ExportOptions().to_dict()
        assert set(manifest["sections"]) == {"config", "env", "fake"}

    def test_the_provider_sees_the_project(self, project, tmp_path):
        section = FakeSection()
        _export(project, tmp_path, providers=[section], options=ExportOptions(include_data=True))
        ctx = section.exported_ctx
        assert ctx.root == project[0].resolve()
        assert ctx.package == "gait"
        assert ctx.options.include_data is True

    def test_two_sections_with_one_name_are_refused(self, project, tmp_path):
        with pytest.raises(BundleError, match="two sections"):
            _export(project, tmp_path, providers=[FakeSection(), FakeSection()])


def _rewrite(path, change):
    """Copy the zip at *path* with *change(name, data) -> (name, data)|None*."""
    src = zipfile.ZipFile(path)
    items = [(n, src.read(n)) for n in src.namelist()]
    src.close()
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in items:
            out = change(name, data)
            if out is not None:
                zf.writestr(*out)


class TestRead:
    def test_round_trip(self, project, tmp_path):
        out = _export(project, tmp_path, providers=[FakeSection()])
        b = read_bundle(out)
        assert b.sections["fake"] == {"a.txt": b"hello", "sub/b.json": b"{}"}
        assert b"entities_file" in b.sections["config"]["scistack.toml"]

    def test_a_changed_file_is_refused(self, project, tmp_path):
        out = _export(project, tmp_path, providers=[FakeSection()])
        _rewrite(out, lambda n, d: (n, b"tampered") if n == "fake/a.txt" else (n, d))
        with pytest.raises(BundleError, match="hash"):
            read_bundle(out)

    def test_a_missing_file_is_refused(self, project, tmp_path):
        out = _export(project, tmp_path, providers=[FakeSection()])
        _rewrite(out, lambda n, d: None if n == "fake/a.txt" else (n, d))
        with pytest.raises(BundleError, match="missing"):
            read_bundle(out)

    def test_an_unlisted_file_is_refused(self, project, tmp_path):
        out = _export(project, tmp_path)
        with zipfile.ZipFile(out, "a") as zf:
            zf.writestr("sneaky/x.py", b"print('hi')")
        with pytest.raises(BundleError, match="not in the manifest"):
            read_bundle(out)

    def test_another_format_version_is_refused(self, project, tmp_path):
        out = _export(project, tmp_path)

        def bump(name, data):
            if name == "manifest.json":
                m = json.loads(data)
                m["format_version"] = 999
                return name, json.dumps(m)
            return name, data

        _rewrite(out, bump)
        with pytest.raises(BundleError, match="format 999"):
            read_bundle(out)

    def test_not_a_zip(self, tmp_path):
        bad = tmp_path / "x.scistack"
        bad.write_text("nope")
        with pytest.raises(BundleError, match="not a SciStack bundle"):
            read_bundle(bad)


class TestImport:
    def _bundle(self, project, tmp_path):
        out = _export(project, tmp_path, providers=[FakeSection()])
        project[1].close()
        return out

    def test_makes_a_new_project_and_feeds_the_provider(self, project, tmp_path):
        out = self._bundle(project, tmp_path)
        section = FakeSection()
        target = tmp_path / "copy"

        report = import_project(out, target, providers=[section])

        assert report.package == "gait"
        assert report.schema_keys == SCHEMA
        assert report.db_path == target.resolve() / "gait.duckdb"
        assert report.db_path.exists()
        assert (target / "scistack.toml").read_bytes() == (project[0] / "scistack.toml").read_bytes()
        assert (target / "pyproject.toml").exists()
        assert (target / "src" / "gait" / "scistack_entities.toml").exists()
        ctx, files = section.imported
        assert files == {"a.txt": b"hello", "sub/b.json": b"{}"}
        assert ctx.schema_keys == SCHEMA
        assert report.sections["fake"] == {"files": 2}
        assert "env" in report.sections  # the dependency comparison
        report_db = ctx.db
        assert list(report_db.dataset_schema_keys) == SCHEMA
        report_db.close()

    def test_schema_keys_can_be_overridden(self, project, tmp_path):
        out = self._bundle(project, tmp_path)
        report = import_project(out, tmp_path / "copy", schema_keys=["participant"])
        assert report.schema_keys == ["participant"]

    def test_a_section_without_an_importer_is_reported(self, project, tmp_path):
        out = self._bundle(project, tmp_path)
        report = import_project(out, tmp_path / "copy", providers=[])
        assert any("'fake'" in w and "not imported" in w for w in report.warnings)

    def test_never_into_an_existing_project(self, project, tmp_path):
        out = self._bundle(project, tmp_path)
        with pytest.raises(BundleError, match="NEW project"):
            import_project(out, project[0])


# ---------------------------------------------------------------------------
# Stage 5b: environment, code-as-files phase, safe paths
# ---------------------------------------------------------------------------


class CodeLikeSection:
    """A "files"-phase provider: writes before init, with no database."""

    name = "code"
    phase = bundle.PHASE_FILES

    def __init__(self):
        self.ctx = None

    def export(self, ctx):
        return {"src/gait/__init__.py": b"EXPORTER = True\n"}

    def import_(self, ctx, files):
        self.ctx = ctx
        for rel, data in files.items():
            target = ctx.root / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        return {"files": len(files)}


def test_the_env_section_records_this_python(project, tmp_path):
    import platform

    out = _export(project, tmp_path)
    env = json.loads(read_bundle(out).sections["env"]["environment.json"])
    assert env["python"] == platform.python_version()
    assert "pytest" in env["distributions"]
    assert env["matlab"] is None


def test_files_phase_runs_before_init_without_a_database(project, tmp_path):
    out = _export(project, tmp_path, providers=[CodeLikeSection()])
    project[1].close()
    section = CodeLikeSection()

    report = import_project(out, tmp_path / "copy", providers=[section])

    assert section.ctx.db is None
    # init kept the exporter's file rather than writing its own.
    assert (tmp_path / "copy" / "src" / "gait" / "__init__.py").read_text() == "EXPORTER = True\n"
    assert report.sections["code"] == {"files": 1}


def test_check_environment_reports_missing_and_different(tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "x"\ndependencies = ["pytest", "definitely-not-installed-xyz>=1",'
        ' "scidb"]\n'
    )
    import importlib.metadata

    here = importlib.metadata.version("pytest")
    report = bundle.check_environment(
        tmp_path, {"python": "3.0.0", "distributions": {"pytest": "0.0.1"}}
    )
    assert report["missing"] == ["definitely-not-installed-xyz"]
    assert report["different"] == [{"name": "pytest", "exported": "0.0.1", "here": here}]
    assert '"pytest==0.0.1"' in report["install"]
    assert '"definitely-not-installed-xyz"' in report["install"]
    assert report["python"]["exported"] == "3.0.0"


@pytest.mark.parametrize("bad", ["../escape.txt", "/etc/passwd", "C:/x.txt", "a/../../b"])
def test_unsafe_paths_are_refused(project, tmp_path, bad):
    out = _export(project, tmp_path, providers=[FakeSection({bad: b"x"})])
    with pytest.raises(BundleError, match="unsafe path"):
        read_bundle(out)


# ---------------------------------------------------------------------------
# Stage 6: another schema, PathInput roots, relative config paths
# ---------------------------------------------------------------------------

ENTITIES = (
    'variables = []\n\n[parameters]\n\n[path_inputs]\n'
    'RAW = { template = "{subject}/{session}.csv", root_folder = "/exporter/data" }\n'
    'NOTES = "{subject}.txt"\n'
)


class EntitiesSection(CodeLikeSection):
    def export(self, ctx):
        return {"src/gait/scistack_entities.toml": ENTITIES.encode()}


def _export_with_config(project, tmp_path):
    root, db = project
    (root / "scistack.toml").write_text(
        f'modules = ["{root.resolve().as_posix()}", "{root.resolve().as_posix()}/scripts"]\n'
        'entities_file = "src/gait/scistack_entities.toml"\n\n'
        '[schema_keys]\nsession = ["BL", "FU"]\n\n'
        '[aliases.session]\nname = "Session"\n'
    )
    out = _export(project, tmp_path, providers=[EntitiesSection()])
    db.close()
    return out


def test_into_another_schema_remaps_config_and_path_inputs(project, tmp_path):
    out = _export_with_config(project, tmp_path)
    target = tmp_path / "copy"

    report = import_project(
        out, target,
        providers=[EntitiesSection()],
        schema_keys=["participant", "visit"],
        key_map={"subject": "participant", "session": "visit"},
        path_roots={"RAW": "/mine/raw", "NOPE": "/x"},
    )

    config = bundle._toml_loads((target / "scistack.toml").read_text())
    assert config["schema_keys"] == {"visit": ["BL", "FU"]}
    assert config["aliases"] == {"visit": {"name": "Session"}}
    # The exporter's absolute in-project paths are relative now.
    assert config["modules"] == [".", "scripts"]

    entities = bundle._toml_loads((target / "src/gait/scistack_entities.toml").read_text())
    assert entities["path_inputs"]["RAW"] == {
        "template": "{participant}/{visit}.csv", "root_folder": "/mine/raw",
    }
    assert entities["path_inputs"]["NOTES"] == "{participant}.txt"

    pi = report.sections["path_inputs"]
    assert set(pi["rewritten"]) == {"RAW", "NOTES"}
    assert pi["unknown_roots"] == ["NOPE"]
    schema = report.sections["schema"]
    assert schema["recipient"] == ["participant", "visit"]
    assert schema["key_map"] == {"subject": "participant", "session": "visit"}
    assert any("another schema" in w for w in report.warnings)


def test_a_kept_root_folder_is_reported(project, tmp_path):
    out = _export_with_config(project, tmp_path)
    report = import_project(out, tmp_path / "copy", providers=[EntitiesSection()])
    pi = report.sections["path_inputs"]
    assert pi["roots_unchanged"] == [{"name": "RAW", "root_folder": ["/exporter/data"]}]
    assert report.sections["schema"]["flagged"]


def test_the_same_schema_keeps_config_bytes(project, tmp_path):
    root, db = project
    original = (root / "scistack.toml").read_bytes()
    out = _export(project, tmp_path)
    db.close()
    import_project(out, tmp_path / "copy")
    assert (tmp_path / "copy" / "scistack.toml").read_bytes() == original


def test_a_key_map_off_the_recipient_schema_is_refused(project, tmp_path):
    out = _export(project, tmp_path)
    project[1].close()
    with pytest.raises(BundleError, match="recipient"):
        import_project(out, tmp_path / "copy", schema_keys=["participant"],
                       key_map={"subject": "patient"})
