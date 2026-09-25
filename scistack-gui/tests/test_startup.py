"""Tests for :mod:`scistack_gui.startup` (Phase 8 stale lockfile handling).

These tests exercise the check at three layers:

1. **Pure unit tests** for ``check_lockfile_staleness``, patching the
   scistack.uv_wrapper functions to simulate fresh / stale / uv-missing /
   sync-failure states.
2. **Dedup / state management** for the module-level error list.
3. **API surface**: the ``/api/info`` endpoint must include the recorded
   errors so the frontend can render them as a blocking dialog.

The tests deliberately don't invoke the real ``uv`` CLI — that's covered by
``scistack/tests/test_uv_wrapper.py``. Here we only care that the GUI
orchestration layer wires everything together correctly.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
from scistack_gui import startup
from scistack_gui.startup import (
    StartupError,
    check_lockfile_staleness,
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


@pytest.fixture
def project_with_pyproject(tmp_path: Path) -> Path:
    """Create a minimal project directory with a pyproject.toml."""
    (tmp_path / "pyproject.toml").write_text(
        textwrap.dedent(
            """
            [project]
            name = "tmp_study"
            version = "0.1.0"
            dependencies = []
            """
        )
    )
    return tmp_path


class _FakeSyncResult:
    """Stand-in for :class:`scistack.uv_wrapper.SyncResult`."""

    def __init__(
        self, ok: bool, returncode: int = 0, stderr: str = "", stdout: str = ""
    ):
        self.ok = ok
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout

    @property
    def combined_output(self) -> str:
        parts = [p for p in (self.stdout, self.stderr) if p]
        return "\n".join(parts)


# ---------------------------------------------------------------------------
# Skip-path tests (no pyproject, no scistack, etc.)
# ---------------------------------------------------------------------------
class TestSkipPaths:
    def test_no_pyproject_is_silent_noop(self, tmp_path: Path):
        """If there's no pyproject.toml, we don't even look for uv.lock."""
        result = check_lockfile_staleness(tmp_path)
        assert result is None
        assert get_startup_errors() == []

    def test_scistack_missing_is_silent_noop(
        self,
        project_with_pyproject: Path,
        monkeypatch,
    ):
        """If the scistack package isn't importable we shouldn't crash."""
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "scistack.uv_wrapper":
                raise ImportError("scistack not installed")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        result = check_lockfile_staleness(project_with_pyproject)
        assert result is None
        assert get_startup_errors() == []


# ---------------------------------------------------------------------------
# Happy path: lockfile is fresh
# ---------------------------------------------------------------------------
class TestFreshLockfile:
    def test_fresh_lockfile_is_silent_noop(
        self, project_with_pyproject: Path, monkeypatch
    ):
        """When ``is_lockfile_stale`` returns False we should do nothing."""
        calls = {"sync": 0}

        def fake_is_stale(root):
            return False

        def fake_sync(root, **kwargs):
            calls["sync"] += 1
            return _FakeSyncResult(ok=True)

        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", fake_is_stale)
        monkeypatch.setattr("scistack.uv_wrapper.sync", fake_sync)

        result = check_lockfile_staleness(project_with_pyproject)
        assert result is None
        assert calls["sync"] == 0  # sync must not be invoked
        assert get_startup_errors() == []


# ---------------------------------------------------------------------------
# Stale lockfile path: sync is attempted
# ---------------------------------------------------------------------------
class TestStaleLockfileSync:
    def test_successful_sync_is_silent(self, project_with_pyproject: Path, monkeypatch):
        """Stale + sync succeeds → silent success, no errors recorded."""
        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", lambda root: True)
        monkeypatch.setattr(
            "scistack.uv_wrapper.sync",
            lambda root, **kw: _FakeSyncResult(ok=True, stdout="Resolved 3 packages"),
        )

        result = check_lockfile_staleness(project_with_pyproject)
        assert result is None
        assert get_startup_errors() == []

    def test_failed_sync_records_blocking_error(
        self, project_with_pyproject: Path, monkeypatch
    ):
        """Stale + sync fails → error recorded as blocking."""
        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", lambda root: True)
        monkeypatch.setattr(
            "scistack.uv_wrapper.sync",
            lambda root, **kw: _FakeSyncResult(
                ok=False,
                returncode=1,
                stderr="error: version conflict: numpy<2 vs >=2",
            ),
        )

        result = check_lockfile_staleness(project_with_pyproject)

        assert result is not None
        assert isinstance(result, StartupError)
        assert result.kind == "lockfile_sync_failed"
        assert result.blocking is True
        assert "version conflict" in result.details

        errors = get_startup_errors()
        assert len(errors) == 1
        assert errors[0].kind == "lockfile_sync_failed"

    def test_uv_not_installed_records_error(
        self, project_with_pyproject: Path, monkeypatch
    ):
        """Stale + uv missing → distinct error kind + install hint."""
        from scistack.uv_wrapper import UvNotFoundError

        def fake_sync(root, **kw):
            raise UvNotFoundError("uv not on PATH")

        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", lambda root: True)
        monkeypatch.setattr("scistack.uv_wrapper.sync", fake_sync)

        result = check_lockfile_staleness(project_with_pyproject)

        assert result is not None
        assert result.kind == "uv_not_installed"
        assert "astral-sh/uv" in result.message  # installation hint
        assert result.blocking is True

    def test_unexpected_sync_exception_records_error(
        self, project_with_pyproject: Path, monkeypatch
    ):
        """Any other exception from sync should also surface as a blocking error."""

        def fake_sync(root, **kw):
            raise RuntimeError("something exploded")

        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", lambda root: True)
        monkeypatch.setattr("scistack.uv_wrapper.sync", fake_sync)

        result = check_lockfile_staleness(project_with_pyproject)

        assert result is not None
        assert result.kind == "lockfile_sync_failed"
        assert "something exploded" in result.details


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
        startup._record(StartupError(kind="lockfile_sync_failed", message="old"))
        startup._record(StartupError(kind="lockfile_sync_failed", message="new"))
        errors = get_startup_errors()
        assert len(errors) == 1
        assert errors[0].message == "new"

    def test_distinct_kinds_are_kept_separately(self):
        startup._record(StartupError(kind="lockfile_sync_failed", message="a"))
        startup._record(StartupError(kind="uv_not_installed", message="b"))
        errors = get_startup_errors()
        assert len(errors) == 2
        kinds = {e.kind for e in errors}
        assert kinds == {"lockfile_sync_failed", "uv_not_installed"}

    def test_get_startup_errors_returns_copy(self):
        """Callers mutating the returned list must not poison module state."""
        startup._record(StartupError(kind="a", message="a"))
        errors = get_startup_errors()
        errors.clear()
        assert len(get_startup_errors()) == 1

    def test_to_dict(self):
        err = StartupError(
            kind="lockfile_sync_failed",
            message="boom",
            details="uv log",
            blocking=True,
        )
        d = err.to_dict()
        assert d == {
            "kind": "lockfile_sync_failed",
            "message": "boom",
            "details": "uv log",
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
                kind="lockfile_sync_failed",
                message="uv sync failed on project open",
                details="error: version conflict on numpy",
                blocking=True,
            )
        )

        resp = client.get("/api/info")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["startup_errors"]) == 1

        err = data["startup_errors"][0]
        assert err["kind"] == "lockfile_sync_failed"
        assert err["blocking"] is True
        assert "version conflict" in err["details"]

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


# ---------------------------------------------------------------------------
# is_lockfile_stale exception handling
# ---------------------------------------------------------------------------
class TestIsLockfileStaleException:
    def test_is_lockfile_stale_exception_returns_none(
        self, project_with_pyproject: Path, monkeypatch
    ):
        """If is_lockfile_stale itself raises, check returns None gracefully."""

        def boom(root):
            raise OSError("permission denied")

        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", boom)

        result = check_lockfile_staleness(project_with_pyproject)
        assert result is None
        assert get_startup_errors() == []


# ---------------------------------------------------------------------------
# Multiple check calls (accumulation / replacement)
# ---------------------------------------------------------------------------
class TestMultipleChecks:
    def test_repeated_failures_dedup(self, project_with_pyproject: Path, monkeypatch):
        """Calling check twice with failures should replace, not append."""
        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", lambda root: True)

        call_count = [0]

        def fake_sync(root, **kw):
            call_count[0] += 1
            return _FakeSyncResult(
                ok=False,
                returncode=1,
                stderr=f"error attempt {call_count[0]}",
            )

        monkeypatch.setattr("scistack.uv_wrapper.sync", fake_sync)

        check_lockfile_staleness(project_with_pyproject)
        check_lockfile_staleness(project_with_pyproject)

        errors = get_startup_errors()
        assert len(errors) == 1  # deduped by kind
        assert "attempt 2" in errors[0].details

    def test_different_error_kinds_accumulate(
        self, project_with_pyproject: Path, monkeypatch
    ):
        """Different error kinds should accumulate, not replace each other."""
        from scistack.uv_wrapper import UvNotFoundError

        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", lambda root: True)

        # First call: uv not found
        def uv_missing_sync(root, **kw):
            raise UvNotFoundError("uv not on PATH")

        monkeypatch.setattr("scistack.uv_wrapper.sync", uv_missing_sync)
        check_lockfile_staleness(project_with_pyproject)

        # Record a different kind manually
        startup._record(StartupError(kind="other_check", message="something else"))

        errors = get_startup_errors()
        assert len(errors) == 2
        kinds = {e.kind for e in errors}
        assert kinds == {"uv_not_installed", "other_check"}


# ---------------------------------------------------------------------------
# check_lockfile_staleness message content
# ---------------------------------------------------------------------------
class TestCheckMessages:
    def test_sync_failure_message_includes_exit_code(
        self, project_with_pyproject: Path, monkeypatch
    ):
        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", lambda root: True)
        monkeypatch.setattr(
            "scistack.uv_wrapper.sync",
            lambda root, **kw: _FakeSyncResult(ok=False, returncode=42, stderr="boom"),
        )

        result = check_lockfile_staleness(project_with_pyproject)
        assert result is not None
        assert "42" in result.message

    def test_uv_not_installed_message_has_install_url(
        self, project_with_pyproject: Path, monkeypatch
    ):
        from scistack.uv_wrapper import UvNotFoundError

        monkeypatch.setattr("scistack.uv_wrapper.is_lockfile_stale", lambda root: True)
        monkeypatch.setattr(
            "scistack.uv_wrapper.sync",
            lambda root, **kw: (_ for _ in ()).throw(UvNotFoundError("not found")),
        )

        result = check_lockfile_staleness(project_with_pyproject)
        assert result is not None
        assert "astral-sh/uv" in result.message
