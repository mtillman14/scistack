"""
Opening a project must leave it with somewhere to declare entities.

Regression cover for the 2026-09-01 report: ``config.add_path`` created a
``scistack.toml`` with no ``entities_file`` key, and
``scidb.entities.entities_path`` only falls back to the conventional
``src/scistack_entities.toml`` when it already exists. The project therefore
had no writable declaration surface at all -- the log's summary line read
``entities_file=None (writable)`` -- so a variable placed on the canvas
could not be declared, and no MATLAB classdef could be materialized for it
(``scimatlab.stubs.variable_stub_dir`` returns ``None`` with no entities
file to sit beside). The run died as ``Unrecognized function or variable
'Raw_EMG'``.

See ``.claude/plan-entity-surfaces-and-reload-cost.md`` Stage 1.
"""

from __future__ import annotations

import pytest

from scistack_gui.services.project_init_service import (
    ensure_language_stubs,
    ensure_project_files,
)


@pytest.fixture(autouse=True)
def _clear_entities_cache():
    from scidb import entities

    entities.clear_cache()
    yield
    entities.clear_cache()


class TestEnsureProjectFiles:
    def test_creates_config_and_entities_file_in_a_bare_project(self, tmp_path):
        result = ensure_project_files(tmp_path / "data.duckdb")

        assert (tmp_path / "scistack.toml").exists()
        assert (tmp_path / "src" / "scistack_entities.toml").exists()
        assert result.entities_file == tmp_path / "src" / "scistack_entities.toml"
        assert len(result.created) == 2

    def test_the_created_config_actually_names_the_entities_file(self, tmp_path):
        """The exact gap that caused the bug: a config can exist and still
        leave the project with no writable surface."""
        ensure_project_files(tmp_path / "data.duckdb")

        from scistack_gui.config import load_config

        config = load_config(tmp_path, tmp_path / "data.duckdb")
        assert config.entities_file is not None
        assert config.entities_file.exists()

    def test_is_idempotent_and_does_not_rewrite_on_reopen(self, tmp_path):
        ensure_project_files(tmp_path / "data.duckdb")
        toml = tmp_path / "scistack.toml"
        before_text = toml.read_text()
        before_mtime = toml.stat().st_mtime_ns

        second = ensure_project_files(tmp_path / "data.duckdb")

        assert second.created == []
        assert toml.read_text() == before_text
        assert toml.stat().st_mtime_ns == before_mtime, (
            "reopening a project rewrote scistack.toml; that churns git status "
            "and bumps the mtime other staleness guards read"
        )

    def test_never_overwrites_an_existing_entities_file(self, tmp_path):
        (tmp_path / "scistack.toml").write_text(
            'entities_file = "src/scistack_entities.toml"\n', encoding="utf-8"
        )
        (tmp_path / "src").mkdir()
        declared = tmp_path / "src" / "scistack_entities.toml"
        declared.write_text('variables = ["Existing"]\n', encoding="utf-8")

        ensure_project_files(tmp_path / "data.duckdb")

        assert declared.read_text() == 'variables = ["Existing"]\n'

    def test_a_requested_path_never_re_points_an_existing_entities_file(
        self, tmp_path
    ):
        """The wizard always sends its default path. Applying it to a project
        that already declares one would swap live declarations for an empty
        file -- so it is reported, not applied."""
        (tmp_path / "scistack.toml").write_text(
            'modules = []\nentities_file = "pipeline/e.toml"\n', encoding="utf-8"
        )
        (tmp_path / "pipeline").mkdir()
        (tmp_path / "pipeline" / "e.toml").write_text(
            'variables = ["Existing"]\n', encoding="utf-8"
        )

        result = ensure_project_files(
            tmp_path / "data.duckdb", entities_file="src/scistack_entities.toml"
        )

        assert result.created == []
        assert result.entities_file == tmp_path / "pipeline" / "e.toml"
        assert not (tmp_path / "src" / "scistack_entities.toml").exists()
        assert any("pipeline" in w for w in result.warnings)

    def test_requesting_the_file_already_configured_is_not_a_conflict(self, tmp_path):
        (tmp_path / "scistack.toml").write_text(
            'entities_file = "src/scistack_entities.toml"\n', encoding="utf-8"
        )
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "scistack_entities.toml").write_text(
            "variables = []\n", encoding="utf-8"
        )

        result = ensure_project_files(
            tmp_path / "data.duckdb", entities_file="src/scistack_entities.toml"
        )

        assert result.warnings == []
        assert result.entities_file == tmp_path / "src" / "scistack_entities.toml"

    def test_a_declared_but_missing_entities_file_is_created_where_declared(
        self, tmp_path
    ):
        """The key is a decision already made; a requested path must not
        override it just because the file hasn't been created yet."""
        (tmp_path / "scistack.toml").write_text(
            'modules = []\nentities_file = "pipeline/e.toml"\n', encoding="utf-8"
        )

        result = ensure_project_files(
            tmp_path / "data.duckdb", entities_file="src/scistack_entities.toml"
        )

        assert (tmp_path / "pipeline" / "e.toml").exists()
        assert result.entities_file == tmp_path / "pipeline" / "e.toml"
        assert not (tmp_path / "src" / "scistack_entities.toml").exists()

    def test_a_requested_path_is_used_when_the_project_has_none(self, tmp_path):
        result = ensure_project_files(
            tmp_path / "data.duckdb", entities_file="pipeline/mine.toml"
        )

        assert (tmp_path / "pipeline" / "mine.toml").exists()
        assert result.entities_file == tmp_path / "pipeline" / "mine.toml"

    def test_an_explicit_opt_out_is_not_undone_on_reopen(self, tmp_path):
        """``entities_file = ""`` is what "clear entities file" writes.
        Re-creating one here would silently put it back every open."""
        (tmp_path / "scistack.toml").write_text(
            'modules = []\nentities_file = ""\n', encoding="utf-8"
        )

        result = ensure_project_files(tmp_path / "data.duckdb")

        assert result.created == []
        assert result.entities_file is None
        assert not (tmp_path / "src" / "scistack_entities.toml").exists()

    def test_a_packaged_project_is_reported_not_written(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "demo"\n', encoding="utf-8"
        )
        before = (tmp_path / "pyproject.toml").read_text()

        result = ensure_project_files(tmp_path / "data.duckdb")

        assert (tmp_path / "pyproject.toml").read_text() == before
        assert not (tmp_path / "scistack.toml").exists()
        assert result.created == []
        assert any("pyproject.toml" in w for w in result.warnings)
        assert any("entities_file" in w for w in result.warnings)


class TestLanguageStubs:
    def _config(self, tmp_path):
        from scistack_gui.config import load_config

        ensure_project_files(tmp_path / "data.duckdb")
        from scidb import entities

        entities.clear_cache()
        return load_config(tmp_path, tmp_path / "data.duckdb")

    def test_python_project_gets_only_the_py_stub(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir(parents=True, exist_ok=True)
        (src / "steps.py").write_text("def a(x):\n    return x\n", encoding="utf-8")

        ensure_language_stubs(self._config(tmp_path))

        assert (src / "scistack_entities.py").exists()
        assert not (src / "scistack_entities.m").exists()

    def test_matlab_project_gets_the_m_stub(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir(parents=True, exist_ok=True)
        (src / "doThing.m").write_text(
            "function y = doThing(x)\ny = x;\nend\n", encoding="utf-8"
        )

        config = self._config(tmp_path)
        ensure_language_stubs(config)

        if config.has_matlab:
            assert (src / "scistack_entities.m").exists()

    def test_never_overwrites_a_hand_written_stub(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir(parents=True, exist_ok=True)
        (src / "steps.py").write_text("def a(x):\n    return x\n", encoding="utf-8")
        config = self._config(tmp_path)

        mine = src / "scistack_entities.py"
        mine.write_text("# mine\nimport scidb\n", encoding="utf-8")

        result = ensure_language_stubs(config)

        assert mine.read_text() == "# mine\nimport scidb\n"
        assert str(mine) not in result.created

    def test_the_python_stub_is_importable_and_declares_nothing(self, tmp_path):
        """It must be inert: a stub that broke on import would take out
        discovery for the whole file, silently."""
        src = tmp_path / "src"
        src.mkdir(parents=True, exist_ok=True)
        (src / "steps.py").write_text("def a(x):\n    return x\n", encoding="utf-8")
        ensure_language_stubs(self._config(tmp_path))

        import ast

        tree = ast.parse((src / "scistack_entities.py").read_text())
        assert not [n for n in tree.body if isinstance(n, ast.ClassDef)]

    def test_no_stubs_without_an_entities_file(self, tmp_path):
        """A packaged project init refused: nothing to sit beside."""

        class _Config:
            entities_file = None
            modules = []
            packages = []
            has_matlab = False
            project_root = tmp_path

        assert ensure_language_stubs(_Config()).created == []


class TestPreExistingEntitiesFileIsPopulated:
    """The reported bug, end to end: create a database in a project that
    ALREADY has a scistack_entities.toml, and everything it declares must
    reach the GUI.

    It did not, because two layers disagreed about where the entities file
    is. ``scidb.entities`` falls back to the conventional
    ``src/scistack_entities.toml`` when no ``entities_file`` key is set;
    ``scistack_gui.config.load_config`` did not, and the registry follows the
    GUI's answer. So ``ensure_project_files`` asked scidb, was told the
    project already had an entities file, and skipped writing the key --
    after which ``load_config`` returned ``entities_file=None`` and the
    registry never opened a file that was sitting right there. Its entities
    stayed live for scidb and MATLAB and invisible in the GUI. See
    ``.claude/plan-preexisting-entities-on-db-create-26-09-10.md``.
    """

    def _project(self, tmp_path, config_text: str):
        (tmp_path / "scistack.toml").write_text(config_text, encoding="utf-8")
        src = tmp_path / "src"
        src.mkdir(exist_ok=True)
        (src / "scistack_entities.toml").write_text(
            'variables = ["RawEmg"]\n'
            "\n"
            "[parameters]\n"
            "SAMPLING_RATE_HZ = 1000\n"
            "\n"
            "[path_inputs]\n"
            'EMG_FILE = "{subject}/emg.csv"\n',
            encoding="utf-8",
        )
        from scidb import entities

        entities.clear_cache()

        # Exactly what opening/creating a database does, in order.
        ensure_project_files(tmp_path / "data.duckdb")
        from scistack_gui import registry as _registry
        from scistack_gui.config import load_config

        config = load_config(None, tmp_path / "data.duckdb")
        _registry.load_from_config(config)
        return config

    def test_entities_load_when_the_config_has_no_entities_file_key(self, tmp_path):
        from scidb import BaseVariable
        from scistack_gui import registry as _registry

        config = self._project(tmp_path, "modules = []\n")

        assert config.entities_file == tmp_path / "src" / "scistack_entities.toml"
        assert "SAMPLING_RATE_HZ" in _registry.get_parameters_registry()
        assert "EMG_FILE" in _registry.get_path_inputs_registry()
        assert "RawEmg" in BaseVariable._all_subclasses

    def test_they_reach_the_sidebar_and_the_canvas(self, tmp_path):
        """Registry membership is the mechanism; these two are what the user
        actually looks at."""
        from scistack_gui.services.layout_service import get_parameters, get_path_inputs

        self._project(tmp_path, "modules = []\n")

        assert any(p["name"] == "SAMPLING_RATE_HZ" for p in get_parameters())
        assert any(p["name"] == "EMG_FILE" for p in get_path_inputs())

    def test_still_works_when_there_is_no_config_file_at_all(self, tmp_path):
        """The bare-folder case: ensure_project_files writes the scistack.toml
        that makes the pre-existing entities file discoverable, instead of
        leaving the project in folder-scan mode where entities_file is None
        by construction."""
        from scidb import entities
        from scistack_gui import registry as _registry

        src = tmp_path / "src"
        src.mkdir()
        (src / "scistack_entities.toml").write_text(
            "[parameters]\nSAMPLING_RATE_HZ = 1000\n", encoding="utf-8"
        )
        entities.clear_cache()

        ensure_project_files(tmp_path / "data.duckdb")
        from scistack_gui.config import load_config

        config = load_config(None, tmp_path / "data.duckdb")
        _registry.load_from_config(config)

        assert config.entities_file == tmp_path / "src" / "scistack_entities.toml"
        assert "SAMPLING_RATE_HZ" in _registry.get_parameters_registry()


class TestEveryEntryPointInitializesTheProject:
    """``server.py``'s JSON-RPC ``main()`` (the VS Code extension's entry
    point) duplicates ``bootstrap.open_or_create_project``'s startup sequence
    inline rather than calling it, and had simply never been given the
    ``ensure_project_files`` step -- so creating a database from VS Code in a
    bare folder holding a scistack_entities.toml left it undiscovered.

    Checked at the source level because ``main()`` parses argv, opens a
    database and blocks on stdin; there is no way to exercise it here. The
    point is only that the call has not gone missing again."""

    def _calls_in(self, module) -> set[str]:
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(module))
        return {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }

    @pytest.mark.parametrize("module_name", ["server", "bootstrap"])
    def test_calls_ensure_project_files(self, module_name):
        import importlib

        module = importlib.import_module(f"scistack_gui.{module_name}")

        assert "ensure_project_files" in self._calls_in(module), (
            f"scistack_gui.{module_name} no longer calls ensure_project_files; "
            "projects opened through it will have no entities file configured"
        )
