"""Tests for the /api/project/* endpoints (Phase 6)."""

from __future__ import annotations

import sys
import textwrap

import pytest
from scistack_gui.api import project as _project_mod


@pytest.fixture
def project_client(populated_db, tmp_path):
    """
    FastAPI TestClient with a scaffold project directory around the DB.

    The populated_db fixture creates test.duckdb in tmp_path and wires up
    scistack_gui.db._db_path. Here we add the rest of the project
    structure so the discovery scanner has something to find.

    Mirrors loose_project_client: load the config and feed the registry
    BEFORE the app starts serving, exactly like the real startup path
    (__main__.py/server.py always call open_or_create_project, which does
    this, before the "Discovered Code" panel is ever queried) -- otherwise
    registry.get_function/etc. stay empty and every test here would only
    ever be exercising scan_project's now-GUI-unused standalone behavior,
    not what actually happens in the app.
    """
    from fastapi.testclient import TestClient
    from scistack_gui import registry
    from scistack_gui.app import create_app
    from scistack_gui.config import load_config

    project_name = "test_project"

    # A project that is itself a package: pyproject.toml (packaging only)
    # names it, scistack.toml (the config; empty = defaults) makes this
    # project-mode rather than folder-scan. The autouse _pin_project_root
    # fixture pins the root to tmp_path, so the files placed HERE are found.
    (tmp_path / "pyproject.toml").write_text(
        f'[project]\nname = "{project_name}"\nversion = "0.1.0"\n'
    )
    (tmp_path / "scistack.toml").write_text("")

    # Source package with variables, functions, constants
    src = tmp_path / "src" / project_name
    src.mkdir(parents=True)
    (src / "__init__.py").write_text("")
    (src / "variables.py").write_text(
        textwrap.dedent("""
            from scidb import BaseVariable

            class ProjectVar(BaseVariable):
                schema_version = 1
        """)
    )
    (src / "functions.py").write_text(
        textwrap.dedent("""
            from scidb import scistack

            @scistack
            def project_fcn(x):
                return x + 1
        """)
    )
    (src / "constants.py").write_text(
        textwrap.dedent("""
            from scidb import Parameter

            PROJECT_RATE = Parameter(1000, description="Sample rate")
        """)
    )

    config = load_config(None, tmp_path / "test.duckdb")
    registry.load_from_config(config)

    # Clear the cached scan result from previous tests.
    _project_mod._last_result = None

    app = create_app()
    with TestClient(app) as c:
        yield c

    # Clean up the dynamically imported modules.
    for mod_name in list(sys.modules):
        if mod_name == project_name or mod_name.startswith(project_name + "."):
            sys.modules.pop(mod_name, None)
    _project_mod._last_result = None


class TestGetProjectCode:
    def test_returns_exports(self, project_client):
        resp = project_client.get("/api/project/code")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test_project"
        assert data["variable_count"] >= 1
        # Check that ProjectVar was found
        all_vars = []
        for mod in data["modules"]:
            all_vars.extend(mod["variables"])
        assert "ProjectVar" in all_vars

    def test_finds_functions(self, project_client):
        resp = project_client.get("/api/project/code")
        data = resp.json()
        all_fns = []
        for mod in data["modules"]:
            all_fns.extend(mod["functions"])
        assert "project_fcn" in all_fns

    def test_finds_constants(self, project_client):
        resp = project_client.get("/api/project/code")
        data = resp.json()
        all_consts = []
        for mod in data["modules"]:
            all_consts.extend(c["name"] for c in mod["parameters"])
        assert "PROJECT_RATE" in all_consts

    def test_displayed_function_also_resolves_for_execution(self, project_client):
        """Closing proof for the packaged-mode display/execution gap: a
        function shown in the Discovered Code panel must also be resolvable
        via registry.get_function -- previously this branch called
        scidb.discover.scan_project directly, which never populated the
        registry execution_service.py actually reads at run time, so a
        displayed function could raise KeyError here."""
        from scistack_gui import registry

        resp = project_client.get("/api/project/code")
        all_fns = []
        for mod in resp.json()["modules"]:
            all_fns.extend(mod["functions"])
        assert "project_fcn" in all_fns

        fn = registry.get_function("project_fcn")
        assert fn(1) == 2


class TestRefreshProject:
    def test_refresh_returns_ok(self, project_client):
        resp = project_client.post("/api/project/refresh")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert "project_code" in data
        assert data["project_code"]["name"] == "test_project"

    def test_refresh_updates_cached_scan(self, project_client):
        # First call populates cache.
        project_client.post("/api/project/refresh")
        # Second call should still work (from cache or re-scan).
        resp = project_client.get("/api/project/code")
        assert resp.status_code == 200
        assert resp.json()["variable_count"] >= 1


# ---------------------------------------------------------------------------
# Loose-script / folder-scan projects (no pyproject.toml) — registry-backed
# discovery, added alongside the packaged-project scan_project path above.
# ---------------------------------------------------------------------------


@pytest.fixture
def loose_project_client(populated_db, tmp_path):
    """FastAPI TestClient for a loose-script project: no pyproject.toml,
    no src/{name}/ layout — just .py/.m files sitting next to the .duckdb.
    Mirrors what the folder-scan fallback (config.load_config) + the
    zero-config startup path (__main__.py/server.py) actually do: load the
    config, then feed it to both registries before the app starts serving.
    """
    from fastapi.testclient import TestClient
    from scistack_gui import matlab_registry, registry
    from scistack_gui.api import project as _project_mod
    from scistack_gui.app import create_app
    from scistack_gui.config import load_config

    (tmp_path / "good_module.py").write_text(
        textwrap.dedent("""
            def loose_fn(x):
                return x + 1
        """)
    )
    (tmp_path / "broken_module.py").write_text("def broken(:\n    pass\n")
    (tmp_path / "bandpass_filter.m").write_text(
        "function y = bandpass_filter(x)\ny = x;\nend\n"
    )
    (tmp_path / "LooseMatlabVar.m").write_text(
        "classdef LooseMatlabVar < scidb.BaseVariable\nend\n"
    )
    (tmp_path / "constants_module.py").write_text(
        textwrap.dedent("""
            from scidb import Parameter

            LOOSE_RATE = Parameter(500, description="Loose sample rate")
        """)
    )

    config = load_config(None, tmp_path / "test.duckdb")
    registry.load_from_config(config)
    matlab_registry.load_from_config(config)

    _project_mod._last_result = None

    app = create_app()
    with TestClient(app) as c:
        yield c

    for mod_name in list(sys.modules):
        if mod_name.startswith("scistack_user_"):
            sys.modules.pop(mod_name, None)
    registry._functions.clear()
    registry._function_sources.clear()
    registry._parameters.clear()
    registry._parameter_sources.clear()
    registry._module_paths.clear()
    registry._load_errors.clear()
    registry._config = None
    matlab_registry._matlab_functions.clear()
    matlab_registry._matlab_variables.clear()
    matlab_registry._load_errors.clear()
    matlab_registry._config = None
    _project_mod._last_result = None


class TestLooseProjectCode:
    def test_code_matches_registry_functions_and_variables(self, loose_project_client):
        registry_resp = loose_project_client.get("/api/registry").json()
        code_resp = loose_project_client.get("/api/project/code").json()

        all_fns = {f for mod in code_resp["modules"] for f in mod["functions"]}
        all_vars = {v for mod in code_resp["modules"] for v in mod["variables"]}

        assert set(registry_resp["functions"]) <= all_fns
        assert set(registry_resp["matlab_functions"]) <= all_fns
        assert set(registry_resp["variables"]) <= all_vars

    def test_broken_module_surfaces_as_error_not_silence(self, loose_project_client):
        code_resp = loose_project_client.get("/api/project/code").json()
        error_sources = [e["module_name"] for e in code_resp["errors"]]
        assert any("broken_module.py" in s for s in error_sources)

    def test_broken_module_error_also_in_registry_response(self, loose_project_client):
        # The primary palette (EditTab -> get_registry) must see the same
        # failure, not just the Discovered Code popup.
        registry_resp = loose_project_client.get("/api/registry").json()
        assert any(
            "broken_module.py" in e["source"] for e in registry_resp["load_errors"]
        )

    def test_matlab_function_and_variable_discovered(self, loose_project_client):
        code_resp = loose_project_client.get("/api/project/code").json()
        all_fns = {f for mod in code_resp["modules"] for f in mod["functions"]}
        all_vars = {v for mod in code_resp["modules"] for v in mod["variables"]}
        assert "bandpass_filter" in all_fns
        assert "LooseMatlabVar" in all_vars

    def test_constant_discovered(self, loose_project_client):
        # Full parity with scan_project's packaged-project behavior:
        # scidb.Parameter() instances in loose scripts must show up here too.
        code_resp = loose_project_client.get("/api/project/code").json()
        all_consts = [c["name"] for mod in code_resp["modules"] for c in mod["parameters"]]
        assert "LOOSE_RATE" in all_consts

    def test_constant_value_and_description(self, loose_project_client):
        code_resp = loose_project_client.get("/api/project/code").json()
        entries = {
            c["name"]: c for mod in code_resp["modules"] for c in mod["parameters"]
        }
        entry = entries["LOOSE_RATE"]
        assert entry["value"] == "500"
        assert entry["description"] == "Loose sample rate"

    def test_refresh_re_imports_from_disk(self, loose_project_client, tmp_path):
        # Fix the broken module, hit refresh, confirm it's picked up live —
        # this is the "Refresh" button's whole point for loose-script mode.
        (tmp_path / "broken_module.py").write_text("def now_fixed(x):\n    return x\n")

        resp = loose_project_client.post("/api/project/refresh")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        registry_resp = loose_project_client.get("/api/registry").json()
        assert "now_fixed" in registry_resp["functions"]
        assert registry_resp["load_errors"] == []


# ---------------------------------------------------------------------------
# add_project_path / remove_project_path — Paths popup's editable list
# (loose-script projects only). Uses external, non-nested directories to
# mirror the real use case: pointing at a shared, reusable code repository
# elsewhere on disk, not something inside the project's own folder.
# ---------------------------------------------------------------------------


class TestAddProjectPath:
    def test_add_discovers_new_code_without_restart(
        self, loose_project_client, tmp_path_factory
    ):
        """The critical regression case: registry.refresh_all()/
        matlab_registry.refresh_all() replay against a *stale* in-memory
        config and would NOT pick up a newly added path. The handler must
        re-read scistack.toml from disk (see api/project.py's
        _reload_config_and_rescan) so this works without a server restart.
        """
        external = tmp_path_factory.mktemp("external_repo")
        (external / "shared_fn.py").write_text("def shared_fn(x):\n    return x * 2\n")

        resp = loose_project_client.post(
            "/api/project/paths", json={"path": str(external)}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert str(external) in data["managed_paths"]

        code_resp = loose_project_client.get("/api/project/code").json()
        all_fns = {f for mod in code_resp["modules"] for f in mod["functions"]}
        assert "shared_fn" in all_fns

    def test_add_rejects_nonexistent_path(self, loose_project_client, tmp_path):
        resp = loose_project_client.post(
            "/api/project/paths", json={"path": str(tmp_path / "does_not_exist")}
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is False

    def test_add_rejects_file_not_directory(self, loose_project_client, tmp_path):
        a_file = tmp_path / "just_a_file.py"
        a_file.write_text("")
        resp = loose_project_client.post(
            "/api/project/paths", json={"path": str(a_file)}
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is False


class TestRemoveProjectPath:
    def test_remove_stops_discovery_of_that_path(
        self, loose_project_client, tmp_path_factory
    ):
        from urllib.parse import quote

        external = tmp_path_factory.mktemp("external_repo")
        (external / "temp_fn.py").write_text("def temp_fn(x):\n    return x\n")

        add_resp = loose_project_client.post(
            "/api/project/paths", json={"path": str(external)}
        )
        assert add_resp.json()["ok"] is True
        code_resp = loose_project_client.get("/api/project/code").json()
        all_fns = {f for mod in code_resp["modules"] for f in mod["functions"]}
        assert "temp_fn" in all_fns

        remove_resp = loose_project_client.delete(
            f"/api/project/paths?path={quote(str(external))}"
        )
        assert remove_resp.status_code == 200
        assert remove_resp.json()["ok"] is True

        code_resp2 = loose_project_client.get("/api/project/code").json()
        all_fns2 = {f for mod in code_resp2["modules"] for f in mod["functions"]}
        assert "temp_fn" not in all_fns2

    def test_remove_fails_when_no_scistack_toml_yet(self, loose_project_client, tmp_path):
        from urllib.parse import quote

        resp = loose_project_client.delete(
            f"/api/project/paths?path={quote(str(tmp_path))}"
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is False
