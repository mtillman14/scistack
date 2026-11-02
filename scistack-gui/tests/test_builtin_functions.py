"""Tests for scistack_gui.services.builtin_function_service — manual
built-in/library function references (numpy.mean, MATLAB builtins, ...)."""

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scistack_gui.project import registry
from scistack_gui.services import builtin_function_service as svc


@pytest.fixture(autouse=True)
def _clean_matlab_registry():
    from scistack_gui.matlab import registry as matlab_registry

    matlab_registry._matlab_functions.clear()
    matlab_registry._matlab_variables.clear()
    yield
    matlab_registry._matlab_functions.clear()
    matlab_registry._matlab_variables.clear()


# ---------------------------------------------------------------------------
# Python — validation-only paths (no DB needed, never reach _persist_builtin)
# ---------------------------------------------------------------------------


class TestPythonBuiltinValidation:
    def test_disallowed_installed_module_rejected(self):
        # duckdb is a real, always-installed dependency here (scidb wraps
        # it) but is neither stdlib nor numpy/pandas — the policy must
        # reject it even though it's importable, not just reject packages
        # that don't exist at all (see test_uninstalled_package_rejected).
        result = svc.create_builtin_function("python", "duckdb.connect")
        assert result["ok"] is False
        assert "not allowed" in result["error"]
        assert "duckdb.connect" not in registry._functions

    def test_nonexistent_attribute_rejected(self):
        result = svc.create_builtin_function("python", "math.not_a_real_function")
        assert result["ok"] is False

    def test_not_callable_rejected(self):
        result = svc.create_builtin_function("python", "math.pi")
        assert result["ok"] is False
        assert "not callable" in result["error"]

    def test_uninstalled_package_rejected(self):
        result = svc.create_builtin_function("python", "totally_not_a_real_package_xyz.foo")
        assert result["ok"] is False
        assert "not installed" in result["error"]

    def test_invalid_syntax_rejected(self):
        result = svc.create_builtin_function("python", "not a valid ref!")
        assert result["ok"] is False

    def test_empty_reference_rejected(self):
        result = svc.create_builtin_function("python", "   ")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# Python — successful creation (needs a real DB for persistence)
# ---------------------------------------------------------------------------


class TestPythonLibraryFunctionCreation:
    def test_stdlib_bare_name(self, populated_db):
        result = svc.create_builtin_function("python", "len")
        assert result["ok"] is True
        assert result["name"] == "len"
        assert registry.get_function("len") is len

    def test_stdlib_dotted_module(self, populated_db):
        import math

        result = svc.create_builtin_function("python", "math.sqrt")
        assert result["ok"] is True
        assert result["name"] == "math.sqrt"
        fn = registry.get_function("math.sqrt")
        # Name-qualified wrapper (library_functions.with_qualified_name):
        # `sqrt.__name__` is "sqrt", so it gets wrapped to record as
        # "math.sqrt". `len` above needs no wrapper — its name already matches.
        assert fn.__wrapped__ is math.sqrt
        assert fn.__name__ == "math.sqrt"
        assert fn(9) == 3.0

    def test_numpy_allowed(self, populated_db):
        result = svc.create_builtin_function("python", "numpy.mean")
        assert result["ok"] is True
        assert result["name"] == "numpy.mean"
        assert callable(registry.get_function("numpy.mean"))

    def test_pandas_allowed(self, populated_db):
        result = svc.create_builtin_function("python", "pandas.read_csv")
        assert result["ok"] is True
        assert result["name"] == "pandas.read_csv"

    def test_never_stored_in_the_function_registry(self, populated_db):
        """The whole point of the import-on-demand design: the callable is
        NOT cached in registry._functions, so no refresh can evict it."""
        svc.create_builtin_function("python", "numpy.mean")
        assert "numpy.mean" not in registry._functions
        assert callable(registry.get_function("numpy.mean"))

    def test_survives_a_registry_clear_without_replay(self, populated_db):
        """Regression for the reported bug: several refresh paths clear
        registry._functions without calling replay_persisted_builtins, and
        the run then failed with 'not found in registry'. Resolution by
        import has no such dependency."""
        svc.create_builtin_function("python", "pandas.read_csv")

        registry._functions.clear()
        registry._function_sources.clear()

        assert callable(registry.get_function("pandas.read_csv"))

    def test_persisted_name_is_listed(self, populated_db):
        """The DB table is now the only record of which library functions
        exist, so the listing has to come from there."""
        svc.create_builtin_function("python", "numpy.mean")
        assert "numpy.mean" in svc.get_python_library_function_names(populated_db)

    def test_replay_reports_python_without_registering(self, populated_db):
        svc.create_builtin_function("python", "numpy.mean")
        registry._functions.clear()

        replay_result = svc.replay_persisted_builtins(populated_db)
        assert replay_result["counts"]["python"] == 1
        assert "numpy.mean" not in registry._functions


# ---------------------------------------------------------------------------
# Python — conventional import aliases (pd./np.)
# ---------------------------------------------------------------------------


class TestPythonAliases:
    def test_pd_alias_canonicalized(self, populated_db):
        result = svc.create_builtin_function("python", "pd.read_csv")
        assert result["ok"] is True
        assert result["name"] == "pandas.read_csv"

    def test_np_alias_canonicalized(self, populated_db):
        result = svc.create_builtin_function("python", "np.mean")
        assert result["ok"] is True
        assert result["name"] == "numpy.mean"

    def test_only_the_canonical_name_is_persisted(self, populated_db):
        """The alias must not reach persistence — two names for one
        function would fork run history."""
        svc.create_builtin_function("python", "pd.read_csv")
        names = svc.get_python_library_function_names(populated_db)
        assert "pandas.read_csv" in names
        assert "pd.read_csv" not in names

    def test_alias_resolves_at_lookup_too(self, populated_db):
        """A node label saved before canonicalization (or typed straight
        into a run) still resolves rather than dying at run time."""
        import pandas

        fn = registry.get_function("pd.read_csv")
        # Name-qualified wrapper (see library_functions.with_qualified_name),
        # so identity is checked through __wrapped__.
        assert fn.__wrapped__ is pandas.read_csv
        assert fn.__name__ == "pandas.read_csv"


# ---------------------------------------------------------------------------
# MATLAB
# ---------------------------------------------------------------------------


class TestMatlabBuiltin:
    def test_invalid_identifier_rejected(self):
        result = svc.create_builtin_function("matlab", "not valid!")
        assert result["ok"] is False
        assert "identifier" in result["error"]

    def test_matlab_not_on_path_rejected(self, monkeypatch):
        monkeypatch.setattr(svc.shutil, "which", lambda name: None)
        result = svc.create_builtin_function("matlab", "mean")
        assert result["ok"] is False
        assert "PATH" in result["error"]

    def test_valid_builtin_registers(self, monkeypatch, populated_db):
        monkeypatch.setattr(svc.shutil, "which", lambda name: "/usr/local/bin/matlab")

        class FakeResult:
            stdout = "5\n"

        monkeypatch.setattr(svc.subprocess, "run", lambda *a, **k: FakeResult())

        result = svc.create_builtin_function("matlab", "mean")
        assert result["ok"] is True
        assert result["name"] == "mean"

        from scistack_gui.matlab import registry as matlab_registry

        assert matlab_registry.is_matlab_function("mean")
        info = matlab_registry.get_matlab_function("mean")
        assert info.file_path is None

    def test_unknown_function_rejected(self, monkeypatch):
        monkeypatch.setattr(svc.shutil, "which", lambda name: "/usr/local/bin/matlab")

        class FakeResult:
            stdout = "0\n"

        monkeypatch.setattr(svc.subprocess, "run", lambda *a, **k: FakeResult())

        result = svc.create_builtin_function("matlab", "not_a_real_fn")
        assert result["ok"] is False

        from scistack_gui.matlab import registry as matlab_registry

        assert not matlab_registry.is_matlab_function("not_a_real_fn")

    def test_timeout_rejected(self, monkeypatch):
        monkeypatch.setattr(svc.shutil, "which", lambda name: "/usr/local/bin/matlab")

        def raise_timeout(*a, **k):
            raise subprocess.TimeoutExpired(cmd="matlab", timeout=30)

        monkeypatch.setattr(svc.subprocess, "run", raise_timeout)

        result = svc.create_builtin_function("matlab", "mean")
        assert result["ok"] is False
        assert "Timed out" in result["error"]

    def test_reference_never_reaches_subprocess_before_identifier_check(self, monkeypatch):
        """Security regression: a reference that fails the identifier
        check must never reach subprocess.run — no shell-injection
        surface via a crafted 'name'."""
        monkeypatch.setattr(svc.shutil, "which", lambda name: "/usr/local/bin/matlab")

        def fail_if_called(*a, **k):
            raise AssertionError("subprocess.run must not be called for an invalid identifier")

        monkeypatch.setattr(svc.subprocess, "run", fail_if_called)

        result = svc.create_builtin_function("matlab", "mean'); !rm -rf /; disp('")
        assert result["ok"] is False

    def test_persisted_and_replayed_without_reinvoking_matlab(self, monkeypatch, populated_db):
        """Replay must NOT shell out to MATLAB again — it trusts the
        already-validated persisted reference (slow/fragile to re-check
        on every refresh)."""
        monkeypatch.setattr(svc.shutil, "which", lambda name: "/usr/local/bin/matlab")

        class FakeResult:
            stdout = "5\n"

        monkeypatch.setattr(svc.subprocess, "run", lambda *a, **k: FakeResult())
        svc.create_builtin_function("matlab", "mean")

        from scistack_gui.matlab import registry as matlab_registry

        matlab_registry._matlab_functions.clear()
        assert not matlab_registry.is_matlab_function("mean")

        def fail_if_called(*a, **k):
            raise AssertionError("replay must not shell out to MATLAB again")

        monkeypatch.setattr(svc.subprocess, "run", fail_if_called)

        replay_result = svc.replay_persisted_builtins(populated_db)
        assert replay_result["counts"]["matlab"] == 1
        assert matlab_registry.is_matlab_function("mean")
