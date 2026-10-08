"""Tests for :mod:`scistack_gui.startup` (Phase 8 project-open diagnostics).

Two layers:

1. **Dedup / state management** for the module-level error list.
2. **API surface**: the ``/api/info`` endpoint must include the recorded
   errors so the frontend can render them as a blocking dialog.

The uv lockfile check that used to live in ``startup`` was removed with uv
itself (2026-10-08); ``test_no_lockfile_check_remains`` pins that.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from scistack_gui import startup
from scistack_gui.startup import (
    StartupError,
    clear_startup_errors,
    get_startup_errors,
)


# ---------------------------------------------------------------------------
# _send_progress — startup progress notifications
# ---------------------------------------------------------------------------
def test_send_progress_writes_json_rpc_notification(capsys):
    """_send_progress should emit a well-formed JSON-RPC notification on stdout."""
    from scistack_gui.server import _send_progress

    _send_progress("hello")

    captured = capsys.readouterr()
    line = captured.out.strip().splitlines()[-1]
    msg = json.loads(line)
    assert msg["jsonrpc"] == "2.0"
    assert msg["method"] == "progress"
    assert msg["params"]["message"] == "hello"
    # It's a notification, not a request — no id.
    assert "id" not in msg


def test_startup_discovery_reports_progress_per_module():
    """Every ``registry.load_from_config`` in ``server.main`` must forward
    ``_send_progress``.

    The extension's ready timer is an INACTIVITY timer reset only by
    ``progress`` notifications. With one notification before discovery,
    all module imports shared a single 60 s window, and two stats scripts
    doing their analysis on import exhausted it (Stroke-R01-Aim1,
    2026-09-25). Source guard because ``main`` blocks on stdin.
    """
    import ast

    from scistack_gui import server

    tree = ast.parse(Path(server.__file__).read_text(encoding="utf-8"))
    main_fn = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    calls = [
        n
        for n in ast.walk(main_fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "load_from_config"
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "registry"
    ]
    assert calls, "server.main no longer calls registry.load_from_config"
    for call in calls:
        kw = {k.arg: k.value for k in call.keywords}
        assert (
            isinstance(kw.get("on_progress"), ast.Name)
            and kw["on_progress"].id == "_send_progress"
        ), f"registry.load_from_config at line {call.lineno} lacks on_progress=_send_progress"


def test_load_from_config_forwards_progress_to_module_imports(tmp_path, monkeypatch):
    """The callback reaches the per-file import loop (not just the signature)."""
    from scistack_gui import registry

    seen: list = []

    def fake_load_file_modules(paths, on_progress=None):
        seen.append(on_progress)

    monkeypatch.setattr(registry, "_load_file_modules", fake_load_file_modules)
    monkeypatch.setattr(registry, "_load_packages", lambda names: None)
    monkeypatch.setattr(registry, "_load_entry_points", lambda: None)
    # load_from_config pins scifor's project root globally; keep it out of
    # other tests.
    import scifor.pathinput

    monkeypatch.setattr(scifor.pathinput, "set_project_root", lambda root: None)

    class _Cfg:
        project_root = tmp_path
        modules: list = []
        packages: list = []
        auto_discover = False
        entities_file = None

    def cb(msg: str) -> None:
        pass

    registry.load_from_config(_Cfg(), on_progress=cb)
    assert seen == [cb]


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_startup_state():
    """Clear the module-level error list before and after every test."""
    clear_startup_errors()
    yield
    clear_startup_errors()



# ---------------------------------------------------------------------------
# Error state management
# ---------------------------------------------------------------------------
class TestErrorState:
    def test_clear_startup_errors(self):
        startup._record(StartupError(kind="test", message="x"))
        assert len(get_startup_errors()) == 1
        clear_startup_errors()
        assert get_startup_errors() == []

    def test_dedup_by_kind(self):
        """Re-recording an error with the same kind should replace, not append."""
        startup._record(StartupError(kind="some_check", message="old"))
        startup._record(StartupError(kind="some_check", message="new"))
        errors = get_startup_errors()
        assert len(errors) == 1
        assert errors[0].message == "new"

    def test_distinct_kinds_are_kept_separately(self):
        startup._record(StartupError(kind="check_a", message="a"))
        startup._record(StartupError(kind="check_b", message="b"))
        errors = get_startup_errors()
        assert len(errors) == 2
        kinds = {e.kind for e in errors}
        assert kinds == {"check_a", "check_b"}

    def test_get_startup_errors_returns_copy(self):
        """Callers mutating the returned list must not poison module state."""
        startup._record(StartupError(kind="a", message="a"))
        errors = get_startup_errors()
        errors.clear()
        assert len(get_startup_errors()) == 1

    def test_to_dict(self):
        err = StartupError(
            kind="windows_config_paths",
            message="boom",
            details="paths",
            blocking=True,
        )
        d = err.to_dict()
        assert d == {
            "kind": "windows_config_paths",
            "message": "boom",
            "details": "paths",
            "blocking": True,
        }


# ---------------------------------------------------------------------------
# /api/info integration: errors are surfaced to the frontend
# ---------------------------------------------------------------------------
class TestInfoEndpointSurfaceErrors:
    def test_info_includes_empty_errors_when_none(self, client):
        resp = client.get("/api/info")
        assert resp.status_code == 200
        data = resp.json()
        assert "startup_errors" in data
        assert data["startup_errors"] == []

    def test_info_includes_recorded_errors(self, client):
        """Errors recorded before the request should appear in /api/info."""
        startup._record(
            StartupError(
                kind="windows_config_paths",
                message="scistack.toml was written on Windows",
                details="src\\scistack_entities.toml",
                blocking=True,
            )
        )

        resp = client.get("/api/info")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["startup_errors"]) == 1

        err = data["startup_errors"][0]
        assert err["kind"] == "windows_config_paths"
        assert err["blocking"] is True
        assert "scistack_entities" in err["details"]

    def test_info_still_returns_db_name(self, client):
        """Adding startup_errors must not break the existing db_name field."""
        resp = client.get("/api/info")
        data = resp.json()
        assert "db_name" in data
        assert data["db_name"].endswith(".duckdb")


# ---------------------------------------------------------------------------
# StartupError defaults and to_dict
# ---------------------------------------------------------------------------
class TestStartupErrorDefaults:
    def test_default_details_empty(self):
        err = StartupError(kind="test", message="msg")
        assert err.details == ""

    def test_default_blocking_true(self):
        err = StartupError(kind="test", message="msg")
        assert err.blocking is True

    def test_non_blocking_error(self):
        err = StartupError(kind="warn", message="msg", blocking=False)
        assert err.blocking is False
        d = err.to_dict()
        assert d["blocking"] is False

    def test_to_dict_all_fields(self):
        err = StartupError(
            kind="k",
            message="m",
            details="d",
            blocking=False,
        )
        d = err.to_dict()
        assert set(d.keys()) == {"kind", "message", "details", "blocking"}
        assert d["kind"] == "k"
        assert d["message"] == "m"
        assert d["details"] == "d"
        assert d["blocking"] is False




def test_no_lockfile_check_remains():
    """SciStack does not manage environments (2026-10-08): no uv lockfile
    check runs on project open, in either server mode."""
    assert not hasattr(startup, "check_lockfile_staleness")
