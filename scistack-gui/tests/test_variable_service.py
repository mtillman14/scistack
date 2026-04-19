"""
Unit tests for scistack_gui.services.variable_service.

Validation tests need no DB. File-writing tests use tmp_path and
a real Python module file, exercising the full create_variable path.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

import scistack_gui.registry as _registry
from scistack_gui.services.variable_service import create_variable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reset_registry_state():
    """Clear registry state between tests (complement to conftest autouse)."""
    _registry._module_path = None
    _registry._config = None


# ---------------------------------------------------------------------------
# Validation — no file I/O needed
# ---------------------------------------------------------------------------

class TestCreateVariableValidation:
    def test_empty_name_rejected(self):
        result = create_variable("")
        assert result["ok"] is False
        assert "valid class name" in result["error"]

    def test_whitespace_only_rejected(self):
        result = create_variable("   ")
        assert result["ok"] is False

    def test_non_identifier_rejected(self):
        result = create_variable("My-Var")
        assert result["ok"] is False
        assert "valid class name" in result["error"]

    def test_keyword_rejected(self):
        result = create_variable("class")
        assert result["ok"] is False
        assert "valid class name" in result["error"]

    def test_leading_underscore_rejected(self):
        result = create_variable("_MyVar")
        assert result["ok"] is False
        assert "underscore" in result["error"]

    def test_lowercase_start_rejected(self):
        result = create_variable("myVar")
        assert result["ok"] is False
        assert "uppercase" in result["error"]

    def test_leading_whitespace_stripped_then_validated(self):
        # " myVar" strips to "myVar" → lowercase start → rejected.
        result = create_variable(" myVar")
        assert result["ok"] is False
        assert "uppercase" in result["error"]

    def test_already_exists_rejected(self, populated_db):
        # RawSignal is defined in conftest and registered in BaseVariable.
        result = create_variable("RawSignal")
        assert result["ok"] is False
        assert "already exists" in result["error"]

    def test_no_module_file_and_no_matlab_returns_error(self):
        _reset_registry_state()
        result = create_variable("NewVar")
        assert result["ok"] is False
        assert "No module file" in result["error"]


# ---------------------------------------------------------------------------
# Python file writing
# ---------------------------------------------------------------------------

class TestCreateVariablePythonWrite:
    """Tests that require a real writable Python module file."""

    @pytest.fixture(autouse=True)
    def setup_module_file(self, tmp_path):
        """Point registry at a temp .py file with a minimal BaseVariable import."""
        module_file = tmp_path / "variables.py"
        module_file.write_text(
            textwrap.dedent("""\
                from scidb import BaseVariable
            """)
        )
        _registry._module_path = module_file
        _registry._config = None
        yield module_file
        _reset_registry_state()
        # Remove the module from sys.modules so next reload is clean.
        for key in list(sys.modules.keys()):
            if "variables" in key and str(tmp_path) in getattr(
                getattr(sys.modules[key], "__file__", ""), "__class__", type(None)
            ).__name__ or (
                hasattr(sys.modules.get(key), "__file__")
                and sys.modules[key].__file__ is not None
                and str(tmp_path) in sys.modules[key].__file__
            ):
                sys.modules.pop(key, None)

    def test_creates_class_in_file(self, setup_module_file):
        result = create_variable("BrandNewVar")
        assert result["ok"] is True, result.get("error")
        content = setup_module_file.read_text()
        assert "class BrandNewVar(BaseVariable)" in content

    def test_docstring_included_when_provided(self, setup_module_file):
        result = create_variable("DocVar", docstring="My docstring")
        assert result["ok"] is True, result.get("error")
        content = setup_module_file.read_text()
        assert '"""My docstring"""' in content

    def test_no_docstring_produces_pass(self, setup_module_file):
        result = create_variable("NoDocVar")
        assert result["ok"] is True, result.get("error")
        content = setup_module_file.read_text()
        assert "pass" in content

    def test_docstring_triple_quotes_escaped(self, setup_module_file):
        result = create_variable("QuoteVar", docstring='Contains """quotes"""')
        assert result["ok"] is True, result.get("error")
        content = setup_module_file.read_text()
        # The triple quotes inside the docstring should be escaped.
        assert '"""' not in content.split('class QuoteVar')[1].split('pass')[0].replace(
            '"""Contains', '').replace('"""', '')

    def test_returns_name_on_success(self, setup_module_file):
        result = create_variable("ReturnedVar")
        assert result.get("name") == "ReturnedVar"

    def test_unwritable_file_returns_error(self, tmp_path):
        """If the target file is unwritable, return a friendly error."""
        module_file = tmp_path / "ro_variables.py"
        module_file.write_text("from scidb import BaseVariable\n")
        module_file.chmod(0o444)  # read-only
        _registry._module_path = module_file
        try:
            result = create_variable("ShouldFail")
            assert result["ok"] is False
            assert "Failed to write" in result["error"]
        finally:
            module_file.chmod(0o644)
            _reset_registry_state()
