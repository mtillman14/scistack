"""
The sidebar's Delete (GitHub #26, .claude/plan-delete-entries-with-data.md;
replaces the 2026-11-03 declaration-only Delete).

* ``remove_declaration`` still owns the entities-file cut (TestRemoveDeclaration);
* Delete removes the data and everything downstream, the canvas nodes and the
  declaration, and a placement never blocks it;
* refusals (stale file, changed data) are found BEFORE anything is written;
* Undo is the generic history: one Ctrl Z restores the file byte for byte,
  the canvas rows, and the data (scidb trash via ``history.note_trash``).
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
    """The sidebar's Delete (GitHub #26): data + downstream, canvas nodes and
    declaration, never blocked by a placement."""

    def test_placed_entity_is_deleted_with_its_node(self, populated_db, tmp_path):
        from scistack_gui.services.entity_delete_service import (
            delete_entity,
            delete_entity_plan,
        )
        from scistack_gui.stores import pipeline_store as ps

        entities = _toml_project(tmp_path, BODY)
        ps.write_manual_node(populated_db, "param__HZ::main", "parameterNode", "HZ", "main")

        plan = delete_entity_plan(populated_db, "parameter", "HZ")
        assert plan["ok"] and plan["blocked"] == []
        assert [p["pipeline_id"] for p in plan["placements"]] == ["main"]
        assert plan["declaration"]["action"] == "remove"

        result = delete_entity(populated_db, "parameter", "HZ")

        assert result["ok"], result
        assert result["nodes_removed"] == 1
        assert "HZ" not in entities.read_text()
        assert "param__HZ::main" not in ps.get_manual_nodes(populated_db)

    def test_a_parameter_with_data_deletes_its_records(self, populated_db, tmp_path):
        """``low_hz`` ran (FilteredSignal) and is not declared anywhere: the
        plan says so, and the delete removes the records it made."""
        from scistack_gui.services.entity_delete_service import (
            delete_entity,
            delete_entity_plan,
        )

        _toml_project(tmp_path, BODY)
        plan = delete_entity_plan(populated_db, "parameter", "low_hz")
        assert plan["data"]["by_variable"] == {"FilteredSignal": 4}
        assert plan["data"]["undoable"] is True
        assert plan["declaration"]["action"] == "none"

        result = delete_entity(
            populated_db, "parameter", "low_hz", fingerprint=plan["data"]["fingerprint"]
        )

        assert result["ok"], result
        assert result["deleted"] == {"FilteredSignal": 4}
        assert _count(populated_db, "FilteredSignal") == 0
        assert _count(populated_db, "RawSignal") == 4

    def test_chosen_values_only(self, populated_db, tmp_path):
        """A subset of a Parameter's values: those values' data goes, the
        declaration keeps the rest, and the node stays."""
        from scistack_gui.services.entity_delete_service import delete_entity_plan

        _toml_project(tmp_path, BODY)
        plan = delete_entity_plan(populated_db, "parameter", "HZ", values=[10])
        assert plan["declaration"]["action"] == "edit_values"
        assert plan["declaration"]["remaining"] == [20]
        assert plan["placements"] == []

    def test_a_variable_and_its_records(self, populated_db, tmp_path):
        from scistack_gui.services.entity_delete_service import delete_entity

        _toml_project(tmp_path, BODY)
        result = delete_entity(populated_db, "variable", "FilteredSignal")
        assert result["ok"], result
        assert result["deleted"] == {"FilteredSignal": 4}
        assert _count(populated_db, "FilteredSignal") == 0

    def test_a_function_in_user_code_keeps_its_code(self, client, populated_db):
        """``client`` registers bandpass_filter (it lives in the test module, not
        in functions_dir): its data and nodes go, its code stays."""
        from scistack_gui.services.entity_delete_service import delete_entity_plan

        plan = delete_entity_plan(populated_db, "function", "bandpass_filter")
        assert plan["data"]["by_variable"] == {"FilteredSignal": 4}
        assert plan["declaration"]["action"] == "none"

    def test_a_changed_plan_is_refused(self, populated_db, tmp_path):
        from scistack_gui.services.entity_delete_service import delete_entity

        _toml_project(tmp_path, BODY)
        result = delete_entity(populated_db, "variable", "FilteredSignal", fingerprint="0" * 16)
        assert not result["ok"] and result["reason"] == "changed"
        assert _count(populated_db, "FilteredSignal") == 4

    def test_a_stale_entities_file_blocks_before_anything_is_written(self, populated_db, tmp_path):
        from scistack_gui.services.entity_delete_service import delete_entity

        entities = _toml_project(tmp_path, BODY)
        entities.write_text(BODY + "# edited behind the GUI's back\n", encoding="utf-8")
        result = delete_entity(populated_db, "parameter", "HZ")
        assert not result["ok"] and result["reason"] == "blocked", result
        assert entities.read_text().endswith("# edited behind the GUI's back\n")

    def test_unsupported_kind(self, populated_db):
        from scistack_gui.services.entity_delete_service import delete_entity_plan

        assert delete_entity_plan(populated_db, "merge", "m")["reason"] == "unsupported_kind"

    def test_a_submodule_hides_and_removes_its_placements(self, populated_db):
        from scistack_gui.services.entity_delete_service import (
            delete_entity,
            delete_entity_plan,
        )
        from scistack_gui.stores import pipeline_store as ps

        child = ps.create_pipeline(populated_db, "child")
        plan = delete_entity_plan(populated_db, "submodule", child)
        assert plan["ok"] and plan["data"]["total_records"] == 0, plan
        result = delete_entity(populated_db, "submodule", child)
        assert result["ok"], result
        hidden = {p["pipeline_id"] for p in ps.list_hidden_pipelines(populated_db)}
        assert child in hidden


def _count(db, variable):
    return db._duck.fetchone("SELECT COUNT(*) FROM _record WHERE type = ?", [variable])[0]


class TestUndo:
    """One Ctrl Z restores everything: file, canvas rows AND data."""

    @staticmethod
    def _change(cid):
        from scistack_gui.api.handlers import CHANGE_HEADER

        return {CHANGE_HEADER: json.dumps({"id": cid, "label": None})}

    def test_undo_restores_the_file_byte_for_byte(self, client, tmp_path):
        history.clear()
        entities = _toml_project(tmp_path, BODY)
        original = entities.read_bytes()

        reply = client.post(
            "/api/entities/delete", json={"kind": "parameter", "name": "OTHER"},
            headers=self._change("d1"),
        )
        assert reply.json()["ok"], reply.json()
        assert entities.read_bytes() != original

        result = client.post("/api/history/undo", json={"change_id": "d1"}).json()
        assert result["status"] == "ok", result
        assert entities.read_bytes() == original
        history.clear()

    def test_undo_restores_the_data_and_redo_deletes_it_again(self, client, populated_db, tmp_path):
        history.clear()
        _toml_project(tmp_path, BODY)

        reply = client.post(
            "/api/entities/delete", json={"kind": "parameter", "name": "low_hz"},
            headers=self._change("d3"),
        ).json()
        assert reply["ok"] and reply["undoable"], reply
        assert _count(populated_db, "FilteredSignal") == 0

        undo = client.post("/api/history/undo", json={"change_id": "d3"}).json()
        assert undo["status"] == "ok", undo
        assert _count(populated_db, "FilteredSignal") == 4

        redo = client.post("/api/history/redo", json={"change_id": "d3"}).json()
        assert redo["status"] == "ok", redo
        assert _count(populated_db, "FilteredSignal") == 0
        history.clear()

    def test_undo_after_a_hand_edit_is_a_conflict(self, client, tmp_path):
        history.clear()
        entities = _toml_project(tmp_path, BODY)
        client.post(
            "/api/entities/delete", json={"kind": "parameter", "name": "OTHER"},
            headers=self._change("d2"),
        )
        edited = entities.read_text() + "# a hand edit\n"
        entities.write_text(edited, encoding="utf-8")

        result = client.post("/api/history/undo", json={"change_id": "d2"}).json()

        assert result["status"] == "conflict"
        assert entities.read_text() == edited
        history.clear()

    def test_the_plan_is_not_an_undo_step(self, client, tmp_path):
        history.clear()
        entities = _toml_project(tmp_path, BODY)

        reply = client.post(
            "/api/entities/delete-plan", json={"kind": "parameter", "name": "HZ"}
        ).json()

        assert reply["ok"] and reply["declaration"]["removed"] == "HZ = [10, 20]  # sampling\n"
        assert entities.read_text() == BODY
        assert history.status()["records"] == 0
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
