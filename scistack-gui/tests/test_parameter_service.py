"""
Unit tests for scistack_gui.services.parameter_service.

Mirrors test_path_input_service.py's shape: GUI-created Parameters are
source-declared (``NAME = scidb.Parameter(v1, ..., description=...)``),
appended to the configured entities file rather than written to
layout.json. One service covers both value counts — there is no separate
"constant" and "sweep" path (D6).
"""

from __future__ import annotations

import scistack_gui.project.registry as _registry
from scistack_gui.services.parameter_service import create_parameter


def _reset_registry_state():
    _registry._module_path = None
    _registry._config = None


class TestCreateParameterValidation:
    def test_empty_name_rejected(self):
        result = create_parameter("")
        assert result["ok"] is False

    def test_non_identifier_rejected(self):
        result = create_parameter("My-Const")
        assert result["ok"] is False

    def test_leading_underscore_rejected(self):
        result = create_parameter("_Hidden")
        assert result["ok"] is False
        assert "underscore" in result["error"]

    def test_no_module_file_returns_error(self):
        _reset_registry_state()
        result = create_parameter("CUTOFF_HZ")
        assert result["ok"] is False


class TestCreateParameterWrite:
    def setup_method(self):
        _reset_registry_state()

    def teardown_method(self):
        _reset_registry_state()

    def test_creates_declaration_in_empty_file(self, tmp_path):
        """No pre-existing imports -- ensure_scidb_import must add one, and
        the appended scidb.Parameter(...) call must resolve on re-scan."""
        module_file = tmp_path / "empty_entities.py"
        module_file.write_text("")
        _registry._module_path = module_file

        result = create_parameter("CUTOFF_HZ")
        assert result["ok"] is True, result.get("error")
        content = module_file.read_text()
        assert "import scidb" in content
        assert "CUTOFF_HZ = scidb.Parameter(description='')" in content

        registered = _registry.get_parameters_registry()
        assert "CUTOFF_HZ" in registered
        assert registered["CUTOFF_HZ"].values == []

    def test_no_values_writes_no_value_not_a_placeholder(self, tmp_path):
        """The GUI's 'New parameter' form only collects a name, so the
        Parameter is created with NO values. It used to scaffold a
        placeholder 0, which is indistinguishable from a declared value once
        written: it showed as a checked value on the node, fed for_each, and
        any run started before the user noticed stamped 0 into records."""
        module_file = tmp_path / "entities.py"
        module_file.write_text("import scidb\n")
        _registry._module_path = module_file

        result = create_parameter("PLACEHOLDER")
        assert result["ok"] is True, result.get("error")
        assert "0" not in module_file.read_text()
        assert _registry.get_parameters_registry()["PLACEHOLDER"].values == []

    def test_explicit_value_and_description(self, tmp_path):
        module_file = tmp_path / "entities.py"
        module_file.write_text("import scidb\n")
        _registry._module_path = module_file

        result = create_parameter("SAMPLE_RATE", [1000], description="Hz")
        assert result["ok"] is True, result.get("error")
        obj = _registry.get_parameters_registry()["SAMPLE_RATE"]
        assert obj.value == 1000
        assert obj.description == "Hz"

    def test_multi_value_parameter_written_as_one_call(self, tmp_path):
        """Several values is the SAME constructor with more arguments — no
        second form, no conversion (D6)."""
        module_file = tmp_path / "entities.py"
        module_file.write_text("import scidb\n")
        _registry._module_path = module_file

        result = create_parameter("WINDOW", [10, 20, 30], description="secs")
        assert result["ok"] is True, result.get("error")
        content = module_file.read_text()
        assert "WINDOW = scidb.Parameter(10, 20, 30, description='secs')" in content
        assert _registry.get_parameters_registry()["WINDOW"].values == [10, 20, 30]

    def test_second_entity_does_not_duplicate_import(self, tmp_path):
        module_file = tmp_path / "empty_entities.py"
        module_file.write_text("")
        _registry._module_path = module_file

        create_parameter("FIRST")
        create_parameter("SECOND")

        content = module_file.read_text()
        assert content.count("import scidb") == 1

    def test_duplicate_name_rejected(self, tmp_path):
        module_file = tmp_path / "entities.py"
        module_file.write_text("import scidb\n")
        _registry._module_path = module_file

        create_parameter("CUTOFF_HZ")
        result = create_parameter("CUTOFF_HZ")
        assert result["ok"] is False
        assert "already exists" in result["error"]


class TestUpdateParameterAcceptsEmpty:
    """A Parameter's value set may be empty at ANY time, not only at
    creation — so removing the last value is allowed rather than blocked.
    Anything wired to the now-empty Parameter fails loudly at run
    (execution_service.build_run_inputs) instead of running with a value
    nobody chose."""

    def setup_method(self):
        _reset_registry_state()

    def teardown_method(self):
        _reset_registry_state()

    def test_last_value_can_be_removed(self, tmp_path):
        from scistack_gui.services.parameter_service import update_parameter

        module_file = tmp_path / "entities.py"
        module_file.write_text("import scidb\n")
        _registry._module_path = module_file
        assert create_parameter("CUTOFF_HZ", [30])["ok"]

        result = update_parameter("CUTOFF_HZ", [])
        assert result["ok"] is True, result.get("error")
        assert "CUTOFF_HZ = scidb.Parameter(description='')" in module_file.read_text()
        assert _registry.get_parameters_registry()["CUTOFF_HZ"].values == []
