"""
Tests for scistack_gui.bootstrap and the /api/bootstrap/* endpoints — the
browser-frontend project-creation wizard (mirrors the VS Code extension's
"SciStack: Open Pipeline" flow; see
docs/claude/scistack-gui-project-setup-guide.md §4/§5).
"""

from __future__ import annotations

import duckdb
import pytest
from fastapi.testclient import TestClient

# Same import dance as scistack_gui.config, so this test runs on 3.10 too.
try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - 3.10 fallback
    import tomli as tomllib  # type: ignore[no-redef]

from scistack_gui.app import create_app
from scistack_gui.bootstrap import open_or_create_project


@pytest.fixture
def api_client():
    """TestClient with no database loaded.

    conftest.py's autouse ``clear_db_state`` fixture resets
    ``scistack_gui.db._db``/``_db_path`` to None before and after every
    test, so a plain ``create_app()`` here (no ``populated_db``) starts
    with nothing open — the exact state the browser wizard is meant to
    bootstrap out of.
    """
    app = create_app()
    with TestClient(app) as c:
        yield c


def _make_existing_db(path, schema_key="subject"):
    con = duckdb.connect(str(path))
    con.execute(f"CREATE TABLE _schema (schema_id INTEGER, {schema_key} INTEGER)")
    con.close()


class TestOpenOrCreateProject:
    def test_create_new_database(self, tmp_path):
        db_path = tmp_path / "new.duckdb"
        result = open_or_create_project(db_path, schema_keys=["subject", "session"])
        assert db_path.exists()
        assert result.schema_keys == ["subject", "session"]
        assert result.db_name == "new.duckdb"

    def test_create_without_schema_keys_raises_file_not_found(self, tmp_path):
        db_path = tmp_path / "missing.duckdb"
        with pytest.raises(FileNotFoundError):
            open_or_create_project(db_path)

    def test_create_empty_schema_keys_raises_value_error(self, tmp_path):
        db_path = tmp_path / "new.duckdb"
        with pytest.raises(ValueError):
            open_or_create_project(db_path, schema_keys=[])

    def test_create_on_existing_path_raises_file_exists(self, tmp_path):
        db_path = tmp_path / "existing.duckdb"
        _make_existing_db(db_path)
        with pytest.raises(FileExistsError):
            open_or_create_project(db_path, schema_keys=["subject"])

    def test_open_existing_database(self, tmp_path):
        db_path = tmp_path / "existing.duckdb"
        _make_existing_db(db_path)
        result = open_or_create_project(db_path)
        assert result.schema_keys == ["subject"]

    def test_module_and_project_mutually_exclusive(self, tmp_path):
        db_path = tmp_path / "new.duckdb"
        with pytest.raises(ValueError):
            open_or_create_project(
                db_path,
                schema_keys=["subject"],
                module=tmp_path / "a.py",
                project=tmp_path,
            )


class TestCreateProjectEndpoint:
    def test_create_flips_db_loaded(self, api_client, tmp_path):
        resp = api_client.get("/api/info")
        assert resp.json() == {"db_loaded": False}

        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject", "session"],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["db_loaded"] is True
        assert data["db_name"] == "study.duckdb"
        assert (tmp_path / "study.duckdb").exists()

        resp = api_client.get("/api/info")
        assert resp.json()["db_loaded"] is True

    def test_create_appends_duckdb_extension(self, api_client, tmp_path):
        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "no_extension",
                "schema_keys": ["subject"],
            },
        )
        assert resp.status_code == 200
        assert (tmp_path / "no_extension.duckdb").exists()

    def test_create_on_existing_path_returns_409(self, api_client, tmp_path):
        _make_existing_db(tmp_path / "study.duckdb")

        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
            },
        )
        assert resp.status_code == 409

    def test_create_on_existing_path_leaves_no_stray_config_files(
        self, api_client, tmp_path
    ):
        """Regression: project initialization must not run before the
        existing-path check -- otherwise a rejected request would still
        write scistack.toml/entities files into a folder the caller never
        intended to modify."""
        _make_existing_db(tmp_path / "study.duckdb")

        api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
            },
        )

        assert not (tmp_path / "scistack.toml").exists()
        assert not (tmp_path / "src").exists()

    def test_create_empty_schema_keys_returns_400(self, api_client, tmp_path):
        resp = api_client.post(
            "/api/bootstrap/create",
            json={"folder": str(tmp_path), "filename": "study", "schema_keys": []},
        )
        assert resp.status_code == 400

    def test_create_missing_folder_returns_404(self, api_client, tmp_path):
        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path / "does_not_exist"),
                "filename": "study",
                "schema_keys": ["subject"],
            },
        )
        assert resp.status_code == 404

    def test_create_filename_with_path_separator_returns_400(self, api_client, tmp_path):
        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "sub/study",
                "schema_keys": ["subject"],
            },
        )
        assert resp.status_code == 400


class TestCreateProjectEagerEntitiesFile:
    """A GUI-created loose-script project now gets scistack.toml + its
    entities file written eagerly at creation time (default
    src/scistack_entities.toml) -- collapsing what used to be a
    pure-folder-scan state (no config at all) until the first
    PathInput/Parameter/Variable was created from the GUI. See
    docs/claude/code-discovery-categories.md."""

    def test_create_writes_scistack_toml_and_default_entities_file(
        self, api_client, tmp_path
    ):
        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
            },
        )
        assert resp.status_code == 200

        entities_file = tmp_path / "src" / "scistack_entities.toml"
        assert entities_file.exists()
        # Scaffolded as TOML with its three kinds, not as Python.
        content = entities_file.read_text()
        assert "variables = []" in content
        assert "[parameters]" in content
        assert "[path_inputs]" in content

        toml_path = tmp_path / "scistack.toml"
        assert toml_path.exists()
        # Recorded RELATIVE to the project root, not absolutely, so the
        # scistack.toml stays portable across machines (set_entities_file's
        # entities_file_for_toml, added in 89f4f35). This assertion used to
        # look for the absolute path and had been failing since that commit.
        with open(toml_path, "rb") as f:
            written = tomllib.load(f)
        assert written["entities_file"] == str(entities_file.relative_to(tmp_path))

    def test_create_respects_custom_entities_file(self, api_client, tmp_path):
        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
                "entities_file": "pipeline/my_entities.toml",
            },
        )
        assert resp.status_code == 200
        assert (tmp_path / "pipeline" / "my_entities.toml").exists()
        assert not (tmp_path / "src" / "scistack_entities.toml").exists()

    def test_create_with_null_entities_file_skips_eager_setup(
        self, api_client, tmp_path
    ):
        """Opting out (explicit null) leaves the project in the old
        pure-folder-scan state -- only entity creation still lazily
        auto-creates a target file later."""
        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
                "entities_file": None,
            },
        )
        assert resp.status_code == 200
        assert not (tmp_path / "scistack.toml").exists()
        assert not (tmp_path / "src" / "scistack_entities.toml").exists()

    def test_create_keeps_a_project_s_existing_entities_file(
        self, api_client, tmp_path
    ):
        """The reported bug's other half: creating a database in a project
        that ALREADY declares an entities file must not re-point it.

        The wizard's field always sends its default, and this endpoint used
        to apply it unconditionally -- so a project declaring
        ``entities_file = "pipeline/my_entities.toml"`` had the key swapped
        for a brand-new empty ``src/scistack_entities.toml``, and everything
        it declared disappeared from the GUI."""
        (tmp_path / "scistack.toml").write_text(
            'modules = []\nentities_file = "pipeline/my_entities.toml"\n'
        )
        (tmp_path / "pipeline").mkdir()
        declared = tmp_path / "pipeline" / "my_entities.toml"
        declared.write_text(
            'variables = ["RawEmg"]\n\n[parameters]\nSAMPLING_RATE_HZ = 1000\n'
        )

        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
                # exactly what the wizard sends when the user leaves the
                # field at its default
                "entities_file": "src/scistack_entities.toml",
            },
        )
        assert resp.status_code == 200

        with open(tmp_path / "scistack.toml", "rb") as f:
            written = tomllib.load(f)
        assert written["entities_file"] == "pipeline/my_entities.toml"
        assert not (tmp_path / "src" / "scistack_entities.toml").exists()
        assert "SAMPLING_RATE_HZ" in declared.read_text()

        from scistack_gui import registry as _registry

        assert "SAMPLING_RATE_HZ" in _registry.get_parameters_registry()

    def test_create_finds_an_entities_file_the_config_does_not_name(
        self, api_client, tmp_path
    ):
        """A scistack.toml with no entities_file key, next to a pre-existing
        src/scistack_entities.toml: its declarations must reach the GUI."""
        (tmp_path / "scistack.toml").write_text("modules = []\n")
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "scistack_entities.toml").write_text(
            "[parameters]\nSAMPLING_RATE_HZ = 1000\n"
        )

        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
            },
        )
        assert resp.status_code == 200

        from scistack_gui import registry as _registry

        assert "SAMPLING_RATE_HZ" in _registry.get_parameters_registry()

    def test_create_skips_eager_setup_for_packaged_project(self, api_client, tmp_path):
        """A pyproject.toml with [tool.scistack] already present makes this
        a packaged project -- entities_file must be hand-added there
        (config._reject_packaged_project), so eager creation is skipped
        rather than failing the whole database-creation request."""
        (tmp_path / "pyproject.toml").write_text("[tool.scistack]\n")

        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
            },
        )
        assert resp.status_code == 200
        assert not (tmp_path / "src" / "scistack_entities.toml").exists()
        assert not (tmp_path / "scistack.toml").exists()

    def test_pathinput_created_after_bootstrap_lands_in_eager_entities_file(
        self, api_client, tmp_path
    ):
        """End-to-end proof of the 'merge' this migration is about: a
        PathInput created from the GUI right after project creation lands
        in the SAME file scistack.toml already points at, with no further
        auto-create prompt needed."""
        resp = api_client.post(
            "/api/bootstrap/create",
            json={
                "folder": str(tmp_path),
                "filename": "study",
                "schema_keys": ["subject"],
            },
        )
        assert resp.status_code == 200

        resp = api_client.post(
            "/api/path-inputs",
            json={"name": "RAW_EMG", "template": "{subject}.mat"},
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        entities_file = tmp_path / "src" / "scistack_entities.toml"
        assert 'RAW_EMG = "{subject}.mat"' in entities_file.read_text()


class TestOpenProjectEndpoint:
    def test_open_existing_database(self, api_client, tmp_path):
        db_path = tmp_path / "existing.duckdb"
        _make_existing_db(db_path)

        resp = api_client.post("/api/bootstrap/open", json={"db_path": str(db_path)})
        assert resp.status_code == 200
        data = resp.json()
        assert data["db_loaded"] is True
        assert data["db_name"] == "existing.duckdb"

    def test_open_nonexistent_path_returns_404(self, api_client, tmp_path):
        resp = api_client.post(
            "/api/bootstrap/open",
            json={"db_path": str(tmp_path / "missing.duckdb")},
        )
        assert resp.status_code == 404
