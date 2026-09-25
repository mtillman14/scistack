"""
Renaming a PathInput from the GUI (.claude/plan-pathinput-rename.md).

A PathInput's name is used in four places, and a rename that moves only
the first leaves the rest stale:

1. the key under ``[path_inputs]`` in the entities file;
2. canvas node ids (``pathInput__NAME[::scope]``), which carry positions,
   manual rows, node config, intent statements, manual edges and hidden
   DB-derived edges (whose ids embed the name too);
3. run history, whose RECORDED declared name wins over a content match
   (F38), so without a rename record an old run draws as a ghost node;
4. the sidebar note, keyed ``pathInput:NAME``.
"""

from __future__ import annotations

import inspect

import scistack_gui.registry as _registry
from scistack_gui import config as config_mod


def _toml_project(tmp_path, body):
    from scistack_gui.db import get_db_path

    entities = tmp_path / "entities.toml"
    entities.write_text(body, encoding="utf-8")
    config_mod.set_entities_file(get_db_path(), entities)
    _registry._module_path = None
    _registry.load_from_config(config_mod.load_config(None, get_db_path()))
    return entities


# ---------------------------------------------------------------------------
# 1. The declaration
# ---------------------------------------------------------------------------


class TestRenameDeclaration:
    def test_renames_the_toml_key_and_the_registry_follows(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import rename_declaration

        entities = _toml_project(
            tmp_path, '[path_inputs]\nRAW = "{subject}/a.csv"  # keep me\n'
        )

        result = rename_declaration("path_input", "RAW", "RAW_EMG")

        assert result["ok"], result
        assert entities.read_text() == '[path_inputs]\nRAW_EMG = "{subject}/a.csv"  # keep me\n'
        registry = _registry.get_path_inputs_registry()
        assert "RAW" not in registry
        assert registry["RAW_EMG"].path_template == "{subject}/a.csv"

    def test_a_taken_name_is_refused_and_nothing_is_written(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import rename_declaration

        body = '[parameters]\nHZ = 10\n\n[path_inputs]\nRAW = "a.csv"\n'
        entities = _toml_project(tmp_path, body)

        result = rename_declaration("path_input", "RAW", "HZ")

        assert not result["ok"]
        assert result["reason"] == "name_taken"
        assert entities.read_text() == body

    def test_an_invalid_name_is_refused(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import rename_declaration

        _toml_project(tmp_path, '[path_inputs]\nRAW = "a.csv"\n')

        result = rename_declaration("path_input", "RAW", "not valid")

        assert not result["ok"]
        assert result["reason"] == "invalid_name"

    def test_same_name_is_a_no_op(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import rename_declaration

        _toml_project(tmp_path, '[path_inputs]\nRAW = "a.csv"\n')

        assert rename_declaration("path_input", "RAW", "RAW")["unchanged"]

    def test_stale_file_is_refused_not_clobbered(self, populated_db, tmp_path):
        from scistack_gui.services.target_file_service import rename_declaration

        entities = _toml_project(tmp_path, '[path_inputs]\nRAW = "a.csv"\n')
        entities.write_text('[path_inputs]\nRAW = "hand-edited.csv"\n', encoding="utf-8")

        result = rename_declaration("path_input", "RAW", "NEW")

        assert result["reason"] == "stale"
        assert "hand-edited.csv" in entities.read_text()
        assert "NEW" not in entities.read_text()


# ---------------------------------------------------------------------------
# 2. Node-keyed GUI state
# ---------------------------------------------------------------------------


class TestRebaseNodeState:
    def test_every_placement_and_its_state_moves(self, populated_db, tmp_path):
        from scistack_gui import intent_store, layout as layout_store
        from scistack_gui import pipeline_store as ps
        from scistack_gui.db import get_db
        from scistack_gui.services.layout_service import rename_path_input

        _toml_project(tmp_path, '[path_inputs]\nRAW = "{subject}/a.csv"\n')
        db = get_db()
        ps.write_manual_node(db, "pathInput__RAW::main", "pathInputNode", "RAW", "main")
        ps.write_manual_node(db, "pathInput__RAW::hyp2", "pathInputNode", "RAW", "hyp2")
        ps.write_manual_node(db, "pathInput__RAWISH::main", "pathInputNode", "RAWISH", "main")
        layout_store.write_node_position("pathInput__RAW::main", 1.0, 2.0, "main")
        layout_store.write_node_position("pathInput__RAW::hyp2", 3.0, 4.0, "hyp2")
        layout_store.write_note("pathInput:RAW", "the raw EMG")
        ps.write_manual_edge(db, {
            "id": "edge_1", "source": "pathInput__RAW::hyp2",
            "target": "fn__f__0123456789abcdef", "targetHandle": "in__path",
        })
        ps.hide_edge(db, "e__RAW__path__f__0123456789abcdef", "pathInput__RAW", "fn__f__0123456789abcdef",
                     None, "in__path", "main")

        result = rename_path_input("RAW", "RAW_EMG")

        assert result["ok"], result
        nodes = ps.get_manual_nodes(db)
        assert "pathInput__RAW_EMG::main" in nodes
        assert "pathInput__RAW_EMG::hyp2" in nodes
        assert nodes["pathInput__RAW_EMG::hyp2"]["label"] == "RAW_EMG"
        assert not any(n.startswith("pathInput__RAW::") for n in nodes)
        # A different node whose name merely starts with the old one is untouched.
        assert nodes["pathInput__RAWISH::main"]["label"] == "RAWISH"

        positions = layout_store.read_positions_by_scope()
        assert positions["main"]["pathInput__RAW_EMG::main"] == {"x": 1.0, "y": 2.0}
        assert positions["hyp2"]["pathInput__RAW_EMG::hyp2"] == {"x": 3.0, "y": 4.0}
        assert "pathInput__RAW::main" not in positions["main"]

        notes = layout_store.read_notes()
        assert notes.get("pathInput:RAW_EMG") == "the raw EMG"
        assert "pathInput:RAW" not in notes

        edge = next(e for e in ps.get_manual_edges(db) if e["id"] == "edge_1")
        assert edge["source"] == "pathInput__RAW_EMG::hyp2"

        hidden = intent_store.hidden_edges(db, "main")
        assert [h["edge_id"] for h in hidden] == ["e__RAW_EMG__path__f__0123456789abcdef"]
        assert hidden[0]["source"] == "pathInput__RAW_EMG"

    def test_ungraduated_manual_rows_are_relabelled(self, populated_db, tmp_path):
        """A freshly dropped node (``pathInput__RAW__x1y2z3``) graduates by
        LABEL, so its label must follow the rename or it never finds the
        renamed node."""
        from scistack_gui import pipeline_store as ps
        from scistack_gui.db import get_db
        from scistack_gui.services.layout_service import rename_path_input

        _toml_project(tmp_path, '[path_inputs]\nRAW = "a.csv"\n')
        db = get_db()
        ps.write_manual_node(db, "pathInput__RAW__x1y2z3", "pathInputNode", "RAW", "main")

        assert rename_path_input("RAW", "NEW")["ok"]

        assert ps.get_manual_nodes(db)["pathInput__RAW__x1y2z3"]["label"] == "NEW"

    def test_a_refused_rename_moves_nothing(self, populated_db, tmp_path):
        from scistack_gui import pipeline_store as ps
        from scistack_gui.db import get_db
        from scistack_gui.services.layout_service import rename_path_input

        _toml_project(tmp_path, '[parameters]\nHZ = 1\n\n[path_inputs]\nRAW = "a.csv"\n')
        db = get_db()
        ps.write_manual_node(db, "pathInput__RAW::main", "pathInputNode", "RAW", "main")

        assert not rename_path_input("RAW", "HZ")["ok"]

        assert "pathInput__RAW::main" in ps.get_manual_nodes(db)
        assert ps.path_input_rename_index(db) == {}


# ---------------------------------------------------------------------------
# 3. Run history recorded under the old name
# ---------------------------------------------------------------------------


class TestResolveRenamed:
    def test_declared_name_is_kept(self):
        from scistack_gui.domain.graph_builder import resolve_renamed_path_input

        assert resolve_renamed_path_input("A", {"A": "B"}, {"A": 1, "B": 1}) == "A"

    def test_follows_a_chain_to_the_declared_name(self):
        from scistack_gui.domain.graph_builder import resolve_renamed_path_input

        assert resolve_renamed_path_input("A", {"A": "B", "B": "C"}, {"C": 1}) == "C"

    def test_a_cycle_returns_the_recorded_name(self):
        from scistack_gui.domain.graph_builder import resolve_renamed_path_input

        assert resolve_renamed_path_input("A", {"A": "B", "B": "A"}, {}) == "A"

    def test_no_renames_is_identity(self):
        from scistack_gui.domain.graph_builder import resolve_renamed_path_input

        assert resolve_renamed_path_input("A", None, {}) == "A"


class TestRecordedNameAfterRename:
    def _scidb_path_inputs(self):
        """One spec, one run that RECORDED the declared name OLD."""
        return {
            "spec": {
                "template": "{subject}/a.csv",
                "root_folder": None,
                "functions": {(("load", "c1"), "path")},
                "declared_names": {(("load", "c1"), "path"): "OLD"},
            }
        }

    def test_old_runs_attach_to_the_renamed_node_not_a_ghost(self):
        from scifor import PathInput

        from scistack_gui.domain.graph_builder import convert_scidb_path_inputs

        registry = {"NEW": PathInput("{subject}/a.csv", name="{subject}/a.csv")}

        result = convert_scidb_path_inputs(
            self._scidb_path_inputs(), registry, None, None,
            path_input_renames={"OLD": "NEW"},
        )

        assert set(result) == {"NEW"}
        assert result["NEW"]["functions"] == {(("load", "c1"), "path")}

    def test_without_the_record_the_old_name_is_a_ghost(self):
        """The failure the record exists to prevent — pinned so a caller
        that forgets to pass the index shows up here."""
        from scifor import PathInput

        from scistack_gui.domain.graph_builder import convert_scidb_path_inputs

        registry = {"NEW": PathInput("{subject}/a.csv", name="{subject}/a.csv")}

        result = convert_scidb_path_inputs(self._scidb_path_inputs(), registry, None, None)

        assert "OLD" in result

    def test_rename_is_recorded(self, populated_db, tmp_path):
        from scistack_gui import pipeline_store as ps
        from scistack_gui.db import get_db
        from scistack_gui.services.layout_service import rename_path_input

        _toml_project(tmp_path, '[path_inputs]\nOLD = "a.csv"\n')

        assert rename_path_input("OLD", "MID")["ok"]
        assert rename_path_input("MID", "NEW")["ok"]

        assert ps.path_input_rename_index(get_db()) == {"OLD": "MID", "MID": "NEW"}

    def test_both_callers_pass_the_rename_index(self):
        """convert_scidb_path_inputs has two production callers, and they
        must resolve identically (its docstring) — a caller without the
        index draws the ghost the other does not."""
        from scistack_gui.api import pipeline as api_pipeline
        from scistack_gui.services import execution_service

        for fn in (api_pipeline.build_aggregate, execution_service._db_path_input_params):
            assert "path_input_rename_index" in inspect.getsource(fn), fn.__name__


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


class TestTransport:
    def test_rest_and_rpc_both_rename(self, client, populated_db, tmp_path):
        from scistack_gui.server import METHODS

        entities = _toml_project(tmp_path, '[path_inputs]\nA = "a.csv"\nB = "b.csv"\n')

        rest = client.post("/api/path-inputs/A/rename", json={"new_name": "A2"}).json()
        rpc = METHODS["rename_path_input"]({"name": "B", "new_name": "B2"})

        assert rest["ok"], rest
        assert rpc["ok"], rpc
        text = entities.read_text()
        assert 'A2 = "a.csv"' in text and 'B2 = "b.csv"' in text
