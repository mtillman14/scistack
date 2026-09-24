"""
Tests for scistack_gui.layout — the node-position / manual-node persistence layer.

All tests use the `layout_path` fixture which ensures scistack_gui.db._db_path
points at a temp directory, so layout.py reads/writes a temp JSON file.
"""

import json

from scistack_gui import layout as layout_store
from scistack_gui import pipeline_store
from scistack_gui.db import get_db

# ---------------------------------------------------------------------------
# read_layout — default state
# ---------------------------------------------------------------------------


class TestReadLayout:
    def test_missing_file_returns_defaults(self, layout_path):
        assert not layout_path.exists()
        result = layout_store.read_layout()
        assert result == {
            "pipeline_id": "main",
            "positions": {},
            "manual_nodes": {},
            "constants": [],
            "manual_edges": [],
        }

    def test_existing_empty_file_returns_defaults(self, layout_path):
        layout_path.write_text(
            json.dumps(
                {
                    "positions": {},
                    "manual_nodes": {},
                    "constants": [],
                    "manual_edges": [],
                }
            )
        )
        result = layout_store.read_layout()
        assert result["positions"] == {}
        assert result["manual_nodes"] == {}


# ---------------------------------------------------------------------------
# write_node_position / read_layout round-trip
# ---------------------------------------------------------------------------


class TestNodePosition:
    def test_write_and_read_position(self, layout_path):
        layout_store.write_node_position("fn__bandpass", 100.0, 200.0)
        result = layout_store.read_layout()
        assert result["positions"]["fn__bandpass"] == {"x": 100.0, "y": 200.0}

    def test_overwrite_position(self, layout_path):
        layout_store.write_node_position("fn__bandpass", 100.0, 200.0)
        layout_store.write_node_position("fn__bandpass", 300.0, 400.0)
        pos = layout_store.read_layout()["positions"]["fn__bandpass"]
        assert pos == {"x": 300.0, "y": 400.0}

    def test_multiple_positions(self, layout_path):
        layout_store.write_node_position("a", 1.0, 2.0)
        layout_store.write_node_position("b", 3.0, 4.0)
        positions = layout_store.read_layout()["positions"]
        assert positions["a"] == {"x": 1.0, "y": 2.0}
        assert positions["b"] == {"x": 3.0, "y": 4.0}


# ---------------------------------------------------------------------------
# write_manual_node / get_manual_nodes
# ---------------------------------------------------------------------------


class TestManualNodes:
    def test_write_manual_node_stores_position_and_metadata(self, layout_path):
        layout_store.write_manual_node("manual__1", 50.0, 75.0, "functionNode", "my_fn")
        data = layout_store.read_layout()
        assert data["positions"]["manual__1"] == {"x": 50.0, "y": 75.0}
        assert data["manual_nodes"]["manual__1"] == {
            "type": "functionNode",
            "label": "my_fn",
            "pipeline_id": "main",
        }

    def test_get_manual_nodes_returns_only_manual(self, layout_path):
        layout_store.write_node_position("fn__real", 0.0, 0.0)
        layout_store.write_manual_node("manual__x", 1.0, 2.0, "variableNode", "Foo")
        manual = pipeline_store.get_manual_nodes(get_db())
        assert "manual__x" in manual
        assert "fn__real" not in manual

    def test_multiple_manual_nodes(self, layout_path):
        layout_store.write_manual_node("m1", 0.0, 0.0, "functionNode", "fn_a")
        layout_store.write_manual_node("m2", 1.0, 1.0, "variableNode", "VarB")
        manual = pipeline_store.get_manual_nodes(get_db())
        assert len(manual) == 2
        assert manual["m1"]["type"] == "functionNode"
        assert manual["m2"]["label"] == "VarB"


# ---------------------------------------------------------------------------
# delete_node
# ---------------------------------------------------------------------------


class TestDeleteNode:
    def test_delete_regular_node(self, layout_path):
        layout_store.write_node_position("fn__foo", 1.0, 2.0)
        layout_store.delete_node("fn__foo")
        assert "fn__foo" not in layout_store.read_layout()["positions"]

    def test_delete_manual_node_removes_both_entries(self, layout_path):
        layout_store.write_manual_node("m1", 0.0, 0.0, "functionNode", "fn_x")
        layout_store.delete_node("m1")
        data = layout_store.read_layout()
        assert "m1" not in data["positions"]
        assert "m1" not in data["manual_nodes"]

    def test_delete_nonexistent_node_is_a_noop(self, layout_path):
        layout_store.delete_node("nonexistent__node")  # must not raise
        assert layout_store.read_layout()["positions"] == {}


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_read_empty(self, layout_path):
        assert layout_store.read_constants() == []

    def test_write_and_read(self, layout_path):
        layout_store.write_constant("low_hz")
        assert "low_hz" in layout_store.read_constants()

    def test_no_duplicates(self, layout_path):
        layout_store.write_constant("low_hz")
        layout_store.write_constant("low_hz")
        assert layout_store.read_constants().count("low_hz") == 1

    def test_multiple_constants(self, layout_path):
        layout_store.write_constant("low_hz")
        layout_store.write_constant("high_hz")
        constants = layout_store.read_constants()
        assert set(constants) == {"low_hz", "high_hz"}

    def test_delete_constant(self, layout_path):
        layout_store.write_constant("low_hz")
        layout_store.write_constant("high_hz")
        layout_store.delete_parameter_from_palette("low_hz")
        constants = layout_store.read_constants()
        assert "low_hz" not in constants
        assert "high_hz" in constants

    def test_delete_nonexistent_constant_is_a_noop(self, layout_path):
        layout_store.delete_parameter_from_palette("does_not_exist")  # must not raise
        assert layout_store.read_constants() == []


# ---------------------------------------------------------------------------
# notes (sidebar palette item-info panel)
# ---------------------------------------------------------------------------


class TestNotes:
    def test_read_empty(self, layout_path):
        assert layout_store.read_notes() == {}

    def test_write_and_read(self, layout_path):
        layout_store.write_note("variable:Position", "meters, post-filter")
        assert layout_store.read_notes() == {"variable:Position": "meters, post-filter"}

    def test_overwrite(self, layout_path):
        layout_store.write_note("constant:low_hz", "first draft")
        layout_store.write_note("constant:low_hz", "revised")
        assert layout_store.read_notes() == {"constant:low_hz": "revised"}

    def test_multiple_keys_independent(self, layout_path):
        layout_store.write_note("constant:low_hz", "note A")
        layout_store.write_note("sweep:gain", "note B")
        notes = layout_store.read_notes()
        assert notes == {"constant:low_hz": "note A", "sweep:gain": "note B"}

    def test_empty_text_deletes_the_note(self, layout_path):
        layout_store.write_note("pathInput:raw_dir", "some note")
        layout_store.write_note("pathInput:raw_dir", "")
        assert layout_store.read_notes() == {}

    def test_whitespace_only_text_deletes_the_note(self, layout_path):
        layout_store.write_note("pathInput:raw_dir", "some note")
        layout_store.write_note("pathInput:raw_dir", "   ")
        assert layout_store.read_notes() == {}

    def test_delete_nonexistent_note_is_a_noop(self, layout_path):
        layout_store.write_note("submodule:pipe_abc", "")  # must not raise
        assert layout_store.read_notes() == {}


# ---------------------------------------------------------------------------
# manual edges
# ---------------------------------------------------------------------------


class TestManualEdges:
    def test_read_empty(self, layout_path):
        assert pipeline_store.get_manual_edges(get_db()) == []

    def test_write_and_read(self, layout_path):
        edge = {
            "id": "e__foo__bar",
            "source": "var__Foo",
            "target": "fn__bar",
            "sourceHandle": None,
            "targetHandle": "in__signal",
        }
        pipeline_store.write_manual_edge(get_db(), edge)
        edges = pipeline_store.get_manual_edges(get_db())
        assert len(edges) == 1
        assert edges[0]["id"] == "e__foo__bar"
        assert edges[0]["target"] == "fn__bar"

    def test_upsert_replaces_existing_edge(self, layout_path):
        edge_v1 = {"id": "e1", "source": "A", "target": "B"}
        edge_v2 = {"id": "e1", "source": "A", "target": "C"}
        pipeline_store.write_manual_edge(get_db(), edge_v1)
        pipeline_store.write_manual_edge(get_db(), edge_v2)
        edges = pipeline_store.get_manual_edges(get_db())
        assert len(edges) == 1
        assert edges[0]["target"] == "C"

    def test_delete_edge(self, layout_path):
        pipeline_store.write_manual_edge(get_db(), {"id": "e1", "source": "A", "target": "B"})
        pipeline_store.write_manual_edge(get_db(), {"id": "e2", "source": "C", "target": "D"})
        pipeline_store.delete_manual_edge(get_db(), "e1")
        edges = pipeline_store.get_manual_edges(get_db())
        assert len(edges) == 1
        assert edges[0]["id"] == "e2"

    def test_delete_nonexistent_edge_is_a_noop(self, layout_path):
        pipeline_store.delete_manual_edge(get_db(), "does_not_exist")  # must not raise
        assert pipeline_store.get_manual_edges(get_db()) == []


# ---------------------------------------------------------------------------
# graduate_manual_node
# ---------------------------------------------------------------------------


class TestGraduateManualNode:
    def test_transfers_position_to_new_id(self, layout_path):
        layout_store.write_manual_node(
            "manual__fn_a", 10.0, 20.0, "functionNode", "fn_a"
        )
        layout_store.graduate_manual_node("manual__fn_a", "fn__fn_a")
        data = layout_store.read_layout()
        assert "fn__fn_a" in data["positions"]
        assert data["positions"]["fn__fn_a"] == {"x": 10.0, "y": 20.0}

    def test_removes_old_position_and_manual_entry(self, layout_path):
        layout_store.write_manual_node(
            "manual__fn_a", 10.0, 20.0, "functionNode", "fn_a"
        )
        layout_store.graduate_manual_node("manual__fn_a", "fn__fn_a")
        data = layout_store.read_layout()
        assert "manual__fn_a" not in data["positions"]
        assert "manual__fn_a" not in data["manual_nodes"]

    def test_does_not_overwrite_existing_canonical_position(self, layout_path):
        """If canonical node already has a position, graduation must not clobber it."""
        layout_store.write_node_position("fn__fn_a", 999.0, 888.0)
        layout_store.write_manual_node(
            "manual__fn_a", 10.0, 20.0, "functionNode", "fn_a"
        )
        layout_store.graduate_manual_node("manual__fn_a", "fn__fn_a")
        pos = layout_store.read_layout()["positions"]["fn__fn_a"]
        assert pos == {"x": 999.0, "y": 888.0}
