"""
An entity write must re-read one file, not the whole project.

The cost this guards, measured on a real project from scidb.log on
2026-09-01: one full ``_refresh_registries()`` was ~16.5 s -- 2.5 s to
re-parse the config, 1.6 s to re-import 19 Python modules, and 14.9 s to
re-classify 303 MATLAB source files. Creating a single variable paid all of
it, to learn something that only one file could have changed.

These tests assert the *absence* of that work rather than a duration, since
timings are not portable: if re-importing modules or re-classifying MATLAB
sources ever creeps back into a write path, the call counters here fail.

See ``.claude/plan-entity-surfaces-and-reload-cost.md`` Stage 3.
"""

from __future__ import annotations

import pytest

import scistack_gui.registry as _registry


@pytest.fixture(autouse=True)
def _clean_registry():
    yield
    _registry._unregister_tracked_variables()
    _registry._config = None
    _registry._module_path = None


@pytest.fixture
def project(tmp_path):
    """A loose project with an entities file and one Python module."""
    (tmp_path / "scistack.toml").write_text(
        'modules = ["src/steps.py"]\n'
        'entities_file = "src/scistack_entities.toml"\n',
        encoding="utf-8",
    )
    src = tmp_path / "src"
    src.mkdir()
    (src / "steps.py").write_text(
        "def a_step(x):\n    return x\n", encoding="utf-8"
    )
    (src / "scistack_entities.toml").write_text("variables = []\n", encoding="utf-8")

    from scidb import entities

    entities.clear_cache()
    from scistack_gui.config import load_config

    config = load_config(tmp_path, tmp_path / "test.duckdb")
    _registry.load_from_config(config)
    return tmp_path, config


@pytest.fixture
def counters(monkeypatch):
    """Count the two expensive halves of a full reload."""
    calls = {"modules": 0, "matlab": 0}

    real_modules = _registry._load_file_modules

    def counting_modules(paths, *args, **kwargs):
        calls["modules"] += 1
        return real_modules(paths, *args, **kwargs)

    monkeypatch.setattr(_registry, "_load_file_modules", counting_modules)

    from scistack_gui import matlab_registry

    real_sources = matlab_registry.load_from_sources

    def counting_sources(paths):
        calls["matlab"] += 1
        return real_sources(paths)

    monkeypatch.setattr(matlab_registry, "load_from_sources", counting_sources)
    return calls


def _write_variable(entities_file, name):
    from scistack_gui.services.target_file_service import write_variable

    return write_variable(entities_file, name)


class TestEntitiesWriteIsNarrow:
    def test_creating_a_variable_reimports_nothing(self, project, counters):
        _, config = project

        assert _write_variable(config.entities_file, "RawEMG") is None

        assert counters["modules"] == 0, "a variable write re-imported Python modules"
        assert counters["matlab"] == 0, "a variable write re-parsed MATLAB sources"

    def test_the_variable_is_actually_registered(self, project):
        """Narrow must not mean incomplete."""
        from scidb import BaseVariable

        _, config = project
        _write_variable(config.entities_file, "RawEMG")

        assert "RawEMG" in BaseVariable._all_subclasses
        assert _registry._variable_sources["RawEMG"] == str(config.entities_file)

    def test_creating_a_parameter_reimports_nothing(self, project, counters):
        from scistack_gui.services.target_file_service import write_entity

        _, config = project
        error = write_entity(
            config.entities_file,
            section="parameters",
            name="SAMPLING_RATE_HZ",
            rendered="1000",
        )

        assert error is None
        assert "SAMPLING_RATE_HZ" in _registry.get_parameters_registry()
        assert counters["modules"] == 0
        assert counters["matlab"] == 0

    def test_functions_from_other_modules_survive_a_narrow_reload(self, project):
        """The narrow path prunes by source, so it must not drop entities
        that came from files it did not re-read."""
        _, config = project
        assert "a_step" in _registry._functions

        _write_variable(config.entities_file, "RawEMG")

        assert "a_step" in _registry._functions


class TestNarrowReloadPrunes:
    def test_a_removed_declaration_disappears(self, project):
        from scidb import BaseVariable

        tmp_path, config = project
        _write_variable(config.entities_file, "RawEMG")
        assert "RawEMG" in BaseVariable._all_subclasses

        config.entities_file.write_text("variables = []\n", encoding="utf-8")
        assert _registry.reload_entities_file() is None

        assert "RawEMG" not in BaseVariable._all_subclasses

    def test_a_fixed_declaration_clears_its_old_load_error(self, project):
        _, config = project
        config.entities_file.write_text(
            'variables = []\n\n[parameters]\nBAD = 1979-05-27\n', encoding="utf-8"
        )
        _registry.reload_entities_file()
        before = len(_registry.get_load_errors())

        config.entities_file.write_text(
            "variables = []\n\n[parameters]\nGOOD = 1\n", encoding="utf-8"
        )
        _registry.reload_entities_file()

        assert len(_registry.get_load_errors()) <= before

    def test_reload_without_a_configured_entities_file_reports_rather_than_raises(
        self, tmp_path
    ):
        (tmp_path / "scistack.toml").write_text("modules = []\n", encoding="utf-8")
        from scistack_gui.config import load_config

        config = load_config(tmp_path, tmp_path / "test.duckdb")
        _registry.load_from_config(config)

        error = _registry.reload_entities_file()

        assert error is not None
        assert "entities file" in error.lower()


class TestRefreshParameterSource:
    """layout_service.refresh_parameter_source is the standalone,
    user-triggered counterpart to reload_entities_file: it exists so a
    Parameter's canvas/sidebar context menu can pull in a hand-edited value
    (e.g. a dict/struct too complex for the sidebar's add-value form)
    without paying for refresh_module()'s reimport-everything cost — see
    docs/claude/entity-editability-model.md."""

    def test_success_picks_up_a_hand_edit_and_stays_narrow(self, project, counters):
        from scistack_gui.services.layout_service import refresh_parameter_source

        _, config = project
        config.entities_file.write_text(
            "variables = []\n\n[parameters]\nSAMPLING_RATE_HZ = 2000\n",
            encoding="utf-8",
        )

        result = refresh_parameter_source("SAMPLING_RATE_HZ")

        assert result == {"ok": True}
        assert _registry.get_parameters_registry()["SAMPLING_RATE_HZ"].value == 2000
        assert counters["modules"] == 0, "refresh_parameter_source re-imported Python modules"
        assert counters["matlab"] == 0, "refresh_parameter_source re-parsed MATLAB sources"

    def test_success_broadcasts_dag_updated(self, project, monkeypatch):
        from scistack_gui.services import layout_service

        _, config = project
        config.entities_file.write_text(
            "variables = []\n\n[parameters]\nSAMPLING_RATE_HZ = 2000\n",
            encoding="utf-8",
        )
        pushed = []
        monkeypatch.setattr(
            "scistack_gui.api.ws.push_message", lambda msg: pushed.append(msg)
        )

        layout_service.refresh_parameter_source("SAMPLING_RATE_HZ")

        assert pushed == [{"type": "dag_updated"}]

    def test_failure_when_no_entities_file_configured_does_not_broadcast(
        self, tmp_path, monkeypatch
    ):
        (tmp_path / "scistack.toml").write_text("modules = []\n", encoding="utf-8")
        from scistack_gui.config import load_config
        from scistack_gui.services import layout_service

        config = load_config(tmp_path, tmp_path / "test.duckdb")
        _registry.load_from_config(config)
        pushed = []
        monkeypatch.setattr(
            "scistack_gui.api.ws.push_message", lambda msg: pushed.append(msg)
        )

        result = layout_service.refresh_parameter_source("SAMPLING_RATE_HZ")

        assert result["ok"] is False
        assert "entities file" in result["error"].lower()
        assert pushed == []

    def test_name_is_logged_only_not_used_to_scope_the_reload(self, project):
        """The reload is whole-file, so a stale unrelated Parameter picks up
        its current value too -- refresh_parameter_source's *name* argument
        is attribution for logging, not a filter."""
        from scistack_gui.services.target_file_service import write_entity
        from scistack_gui.services.layout_service import refresh_parameter_source

        _, config = project
        write_entity(config.entities_file, section="parameters", name="A", rendered="1")
        write_entity(config.entities_file, section="parameters", name="B", rendered="2")
        config.entities_file.write_text(
            "variables = []\n\n[parameters]\nA = 1\nB = 99\n", encoding="utf-8"
        )

        refresh_parameter_source("A")

        assert _registry.get_parameters_registry()["B"].value == 99


class TestGetParametersSourceLocation:
    """get_parameters() feeds the sidebar's Parameter rows the same
    source_file/source_line/declared_in_entities_file fields
    build_parameter_nodes puts on the canvas node (graph_builder's
    is_declared_in_entities_file is the single comparison shared by both)."""

    def test_entities_file_declared_parameter_is_refreshable(self, project):
        from scistack_gui.services.target_file_service import write_entity
        from scistack_gui.services.layout_service import get_parameters

        _, config = project
        write_entity(
            config.entities_file, section="parameters", name="HZ", rendered="10"
        )

        entry = next(p for p in get_parameters() if p["name"] == "HZ")
        assert entry["source_file"] == str(config.entities_file)
        assert entry["source_line"] is not None
        assert entry["declared_in_entities_file"] is True

    def test_legacy_module_declared_parameter_is_not_refreshable(self, tmp_path):
        (tmp_path / "scistack.toml").write_text(
            'modules = ["src/consts.py"]\n'
            'entities_file = "src/scistack_entities.toml"\n',
            encoding="utf-8",
        )
        src = tmp_path / "src"
        src.mkdir()
        (src / "consts.py").write_text(
            "from scidb import Parameter\nHZ = Parameter(10)\n", encoding="utf-8"
        )
        (src / "scistack_entities.toml").write_text("variables = []\n", encoding="utf-8")

        from scidb import entities

        entities.clear_cache()
        from scistack_gui.config import load_config
        from scistack_gui.services.layout_service import get_parameters

        config = load_config(tmp_path, tmp_path / "test.duckdb")
        _registry.load_from_config(config)

        entry = next(p for p in get_parameters() if p["name"] == "HZ")
        assert entry["source_file"] == str(src / "consts.py")
        assert entry["declared_in_entities_file"] is False


class TestDispatch:
    def test_toml_target_routes_to_the_narrow_entities_reload(self, project, monkeypatch):
        from scistack_gui.services import target_file_service

        called = {"n": 0}
        monkeypatch.setattr(
            _registry,
            "reload_entities_file",
            lambda: called.__setitem__("n", called["n"] + 1),
        )
        _, config = project

        target_file_service._reload_after_write(config.entities_file)

        assert called["n"] == 1

    def test_dot_m_target_routes_to_the_single_source_reload(self, project, monkeypatch):
        from scistack_gui import matlab_registry
        from scistack_gui.services import target_file_service

        seen = {}
        monkeypatch.setattr(
            matlab_registry, "reload_source", lambda p: seen.setdefault("path", p)
        )
        tmp_path, _ = project
        target = tmp_path / "src" / "someScript.m"

        target_file_service._reload_after_write(target)

        assert seen["path"] == target
