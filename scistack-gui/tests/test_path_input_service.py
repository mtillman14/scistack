"""
Unit tests for scistack_gui.services.path_input_service.

Validation tests need no DB. File-writing tests use tmp_path and a real
Python module file, exercising the full create_path_input
path — including the qualified ``scidb.X`` append form and the shared
``ensure_scidb_import`` idempotency it depends on.
"""

from __future__ import annotations

import scistack_gui.registry as _registry
from scistack_gui.services.path_input_service import create_path_input


def _reset_registry_state():
    _registry._module_path = None
    _registry._config = None


class TestCreatePathInputValidation:
    def test_empty_name_rejected(self):
        result = create_path_input("", "{subject}.mat")
        assert result["ok"] is False

    def test_non_identifier_rejected(self):
        result = create_path_input("My-Input", "{subject}.mat")
        assert result["ok"] is False

    def test_leading_underscore_rejected(self):
        result = create_path_input("_Hidden", "{subject}.mat")
        assert result["ok"] is False
        assert "underscore" in result["error"]

    def test_no_module_file_returns_error(self):
        _reset_registry_state()
        result = create_path_input("RAW", "{subject}.mat")
        assert result["ok"] is False


class TestCreatePathInputWrite:
    def setup_method(self):
        _reset_registry_state()

    def teardown_method(self):
        _reset_registry_state()

    def test_creates_declaration_in_empty_file(self, tmp_path):
        """No pre-existing imports at all -- ensure_scidb_import must add
        one, and the qualified scidb.PathInput(...) form must resolve."""
        module_file = tmp_path / "empty_entities.py"
        module_file.write_text("")
        _registry._module_path = module_file

        result = create_path_input("RAW_EMG", "{subject}/{trial}.mat")
        assert result["ok"] is True, result.get("error")
        content = module_file.read_text()
        assert "import scidb" in content
        assert "RAW_EMG = scidb.PathInput('{subject}/{trial}.mat', name='RAW_EMG')" in content
        assert "RAW_EMG" in _registry.get_path_inputs_registry()

    def test_second_entity_does_not_duplicate_import(self, tmp_path):
        module_file = tmp_path / "empty_entities.py"
        module_file.write_text("")
        _registry._module_path = module_file

        create_path_input("FIRST", "{subject}.mat")
        create_path_input("SECOND", "{session}.mat")

        content = module_file.read_text()
        assert content.count("import scidb") == 1

    def test_root_folder_included(self, tmp_path):
        module_file = tmp_path / "entities.py"
        module_file.write_text("import scidb\n")
        _registry._module_path = module_file

        result = create_path_input("RAW", "{subject}.mat", root_folder="/data")
        assert result["ok"] is True, result.get("error")
        content = module_file.read_text()
        assert "root_folder='/data'" in content

    def test_alternates_produce_eachof(self, tmp_path):
        module_file = tmp_path / "entities.py"
        module_file.write_text("import scidb\n")
        _registry._module_path = module_file

        result = create_path_input(
            "RAW",
            "{subject}.mat",
            alternate_templates=[{"template": "{subject}.csv"}],
        )
        assert result["ok"] is True, result.get("error")
        content = module_file.read_text()
        assert "scidb.EachOf(scidb.PathInput(" in content

    def test_duplicate_name_rejected(self, tmp_path):
        module_file = tmp_path / "entities.py"
        module_file.write_text("import scidb\n")
        _registry._module_path = module_file

        create_path_input("RAW", "{subject}.mat")
        result = create_path_input("RAW", "{other}.mat")
        assert result["ok"] is False
