"""
The palette's Delete (.claude/plan-palette-order-and-delete.md).

Delete removes a Variable/Parameter/PathInput's DECLARATION from the TOML
entities file and nothing else:

* only the entities file is written (``entity_editability``);
* refused while the entity is on any canvas (the resolved graph);
* the database is untouched -- records of a deleted variable stay;
* Undo is the generic history: the file comes back byte for byte, and a
  hand edit made after the delete makes Undo refuse rather than clobber.
"""

from __future__ import annotations

import json

import scistack_gui.project.registry as _registry
from scistack_gui.project import config as config_mod
from scistack_gui.stores import history


def _toml_project(tmp_path, body):
    from scistack_gui.runtime.db import get_db_path

    entities = tmp_path / "entities.toml"
    entities.write_text(body, encoding="utf-8")
    config_mod.set_entities_file(get_db_path(), entities)
    _registry._module_path = None
    _registry.load_from_config(config_mod.load_config(None, get_db_path()))
    return entities


BODY = (
    'variables = ["Unused", "Kept"]\n'
    "\n"
    "[parameters]\n"
    "HZ = [10, 20]  # sampling\n"
    "OTHER = 1\n"
    "\n"
    "[path_inputs]\n"
    'RAW = "{subject}/a.csv"\n'
)


class TestRemoveDeclaration:
    def test_parameter_entry_goes_and_nothing_else(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import remove_declaration

        entities = _toml_project(tmp_path, BODY)

        result = remove_declaration("parameter", "HZ")

        assert result["ok"], result
        assert result["removed"] == "HZ = [10, 20]  # sampling\n"
        assert entities.read_text() == BODY.replace("HZ = [10, 20]  # sampling\n", "")
        assert "HZ" not in _registry.get_parameters_registry()
        assert "OTHER" in _registry.get_parameters_registry()

    def test_variable_element_goes(self, populated_db, tmp_path):
        from scidb import BaseVariable

        from scistack_gui.services.target_file_service import remove_declaration

        entities = _toml_project(tmp_path, BODY)
        assert "Unused" in BaseVariable._all_subclasses

        result = remove_declaration("variable", "Unused")

        assert result["ok"], result
        assert entities.read_text().startswith('variables = ["Kept"]\n')
        assert "Unused" not in _registry._variable_sources
        assert result["still_declared_in"] is None

    def test_path_input_entry_goes(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import remove_declaration

        entities = _toml_project(tmp_path, BODY)

        assert remove_declaration("path_input", "RAW")["ok"]
        assert "RAW" not in entities.read_text()
        assert "RAW" not in _registry.get_path_inputs_registry()

    def test_preview_writes_nothing(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import remove_declaration

        entities = _toml_project(tmp_path, BODY)

        result = remove_declaration("parameter", "HZ", preview=True)

        assert result["ok"] and result["preview"]
        assert result["removed"] == "HZ = [10, 20]  # sampling\n"
        assert entities.read_text() == BODY

    def test_declared_outside_the_entities_file_is_read_only(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import remove_declaration

        _toml_project(tmp_path, BODY)
        elsewhere = tmp_path / "params.py"
        elsewhere.write_text("import scidb\nPY_P = scidb.Parameter(1)\n")
        _registry._register_parameter("PY_P", _registry.get_parameters_registry()["OTHER"],
                                      source=str(elsewhere))

        result = remove_declaration("parameter", "PY_P")

        assert not result["ok"]
        assert result["reason"] == "read_only"
        assert "PY_P" in elsewhere.read_text()

    def test_stale_file_is_refused_not_clobbered(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import remove_declaration

        entities = _toml_project(tmp_path, BODY)
        entities.write_text(BODY + "# hand edit\n", encoding="utf-8")

        result = remove_declaration("parameter", "HZ")

        assert result["reason"] == "stale"
        assert entities.read_text() == BODY + "# hand edit\n"

    def test_unknown_name(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import remove_declaration

        _toml_project(tmp_path, BODY)
        assert remove_declaration("parameter", "NOPE")["reason"] == "unknown"


class TestDeleteEntity:
    def test_refused_while_on_a_canvas(self, populated_db, tmp_path):
        from scistack_gui.runtime.db import get_db
        from scistack_gui.services.entity_delete_service import delete_entity
        from scistack_gui.stores import pipeline_store as ps

        entities = _toml_project(tmp_path, BODY)
        ps.write_manual_node(get_db(), "param__HZ::main", "parameterNode", "HZ", "main")

        result = delete_entity("parameter", "HZ")

        assert not result["ok"]
        assert result["reason"] == "placed"
        assert [p["pipeline_id"] for p in result["placements"]] == ["main"]
        assert entities.read_text() == BODY

    def test_unplaced_entity_is_deleted(self, populated_db, tmp_path):
        from scistack_gui.services.entity_delete_service import delete_entity, entity_placements

        entities = _toml_project(tmp_path, BODY)
        assert entity_placements("parameter", "OTHER") == []

        result = delete_entity("parameter", "OTHER")

        assert result["ok"], result
        assert "OTHER" not in entities.read_text()

    def test_deleting_a_variable_keeps_its_records(self, populated_db, tmp_path):
        """A declared variable with saved data: delete the declaration, the
        rows stay (declaring it again brings them back)."""
        import numpy as np
        from scidb import BaseVariable

        from scistack_gui.services.entity_delete_service import delete_entity

        _toml_project(tmp_path, BODY)
        BaseVariable._all_subclasses["Kept"].save(np.array([1.0]), subject=1, session="pre")
        duck = populated_db._duck
        before = duck.fetchall("SELECT COUNT(*) FROM _record WHERE type = 'Kept'")[0][0]
        assert before == 1

        result = delete_entity("variable", "Kept")

        # A saved variable shows on the canvas from run history only if it
        # was produced by a run; a bare save is not placed.
        assert result["ok"], result
        after = duck.fetchall("SELECT COUNT(*) FROM _record WHERE type = 'Kept'")[0][0]
        assert after == before

    def test_unsupported_kind(self, populated_db, tmp_path):
        from scistack_gui.services.entity_delete_service import delete_entity

        assert delete_entity("function", "f")["reason"] == "unsupported_kind"


class TestUndo:
    """Undo is the generic history -- no second mechanism."""

    @staticmethod
    def _change(cid):
        from scistack_gui.api.handlers import CHANGE_HEADER

        return {CHANGE_HEADER: json.dumps({"id": cid, "label": None})}

    def test_undo_restores_the_file_byte_for_byte(self, client, tmp_path):
        history.clear()
        entities = _toml_project(tmp_path, BODY)
        original = entities.read_bytes()

        reply = client.delete("/api/entities/parameter/OTHER", headers=self._change("d1"))
        assert reply.json()["ok"], reply.json()
        assert entities.read_bytes() != original

        result = client.post("/api/history/undo", json={"change_id": "d1"}).json()
        assert result["status"] == "ok", result
        assert entities.read_bytes() == original
        history.clear()

    def test_undo_after_a_hand_edit_is_a_conflict(self, client, tmp_path):
        history.clear()
        entities = _toml_project(tmp_path, BODY)
        client.delete("/api/entities/parameter/OTHER", headers=self._change("d2"))
        edited = entities.read_text() + "# a hand edit\n"
        entities.write_text(edited, encoding="utf-8")

        result = client.post("/api/history/undo", json={"change_id": "d2"}).json()

        assert result["status"] == "conflict"
        assert entities.read_text() == edited
        history.clear()

    def test_preview_is_not_an_undo_step(self, client, tmp_path):
        history.clear()
        entities = _toml_project(tmp_path, BODY)

        reply = client.get("/api/entities/parameter/HZ/delete-preview").json()

        assert reply["ok"] and reply["removed"] == "HZ = [10, 20]  # sampling\n"
        assert entities.read_text() == BODY
        history.clear()


class TestRegistryEntries:
    def test_variable_entries_flag_and_last_saved(self, client, tmp_path):
        import numpy as np
        from scidb import BaseVariable

        _toml_project(tmp_path, BODY)
        BaseVariable._all_subclasses["Kept"].save(np.array([1.0]), subject=1, session="pre")

        reg = client.get("/api/registry").json()
        by_name = {e["name"]: e for e in reg["variable_entries"]}

        assert by_name["Kept"]["declared_in_entities_file"] is True
        assert by_name["Kept"]["last_saved"] is not None
        assert by_name["Unused"]["last_saved"] is None
        # Declared in the conftest module, not the entities file: no Delete.
        assert by_name["RawSignal"]["declared_in_entities_file"] is False
        assert [e["name"] for e in reg["variable_entries"]] == reg["variables"]

    def test_function_sources(self, client):
        reg = client.get("/api/registry").json()
        src = reg["function_sources"]["bandpass_filter"]

        assert src["library"] is False
        assert src["file"].endswith(".py")
        assert set(reg["function_sources"]) == set(reg["sidebar_functions"])


class TestRunOptionDefaults:
    def test_defaults_are_for_each_signature(self):
        import inspect

        from scidb.run.foreach import for_each

        from scistack_gui.domain.run_options import run_option_defaults

        params = inspect.signature(for_each).parameters
        d = run_option_defaults()
        assert d["dry_run"] == params["dry_run"].default
        assert d["save"] == params["save"].default
        assert d["distribute"] == params["distribute"].default
        assert d["as_table"] == bool(params["as_table"].default)
        assert d["on_error"] == params["on_error"].default

    def test_empty_run_options_read_as_the_defaults(self):
        from scistack_gui.domain.run_options import run_option_defaults
        from scistack_gui.services.run_failures import (
            on_error_from_config,
        )

        d = run_option_defaults()
        assert on_error_from_config({}) == d["on_error"]

    def test_served_over_the_api(self, client):
        from scistack_gui.domain.run_options import run_option_defaults

        assert client.get("/api/run-option-defaults").json() == run_option_defaults()
