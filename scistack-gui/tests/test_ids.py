"""A node id is a type, not a substring test — ``scistack_gui.ids``."""

import json

import pytest

from scistack_gui.ids import (
    DB_DERIVED_PREFIXES,
    PLACEMENT_SEP,
    ROOT_SCOPE,
    BareNodeId,
    PlacedNodeId,
    fn_node_id,
    node_id,
    parse_fn_node_id,
    parse_placement_id,
    placement_id,
    strip_placement,
)


class TestTheTwoTypes:
    def test_a_bare_id_refuses_a_placement_suffix(self):
        with pytest.raises(ValueError, match="placement suffix"):
            BareNodeId("var__Wide::main")

    def test_a_placed_id_requires_one(self):
        with pytest.raises(ValueError, match="not a placement-qualified"):
            PlacedNodeId("var__Wide")
        with pytest.raises(ValueError):
            PlacedNodeId("::main")

    def test_place_and_strip_round_trip(self):
        bare = BareNodeId("fn__bandpass__0123456789abcdef")
        placed = bare.place("pipe_ab12")
        assert isinstance(placed, PlacedNodeId)
        assert placed == f"fn__bandpass__0123456789abcdef{PLACEMENT_SEP}pipe_ab12"
        assert placed.bare == bare and placed.scope == "pipe_ab12"
        assert strip_placement(placed) == bare
        assert isinstance(strip_placement(placed), BareNodeId)

    def test_placing_a_placed_id_is_an_error_not_a_double_suffix(self):
        """``a::x::y`` was a silent bug (a lookup that never matched); it is
        now a ValueError at the seam that would have written it."""
        with pytest.raises(ValueError):
            placement_id("var__Wide::main", "pipe_1")

    def test_they_are_still_strings(self):
        """JSON, DuckDB parameters, ``startswith`` — nothing downstream has
        to unwrap anything."""
        bare = BareNodeId("param__HZ")
        assert json.dumps({"id": bare}) == '{"id": "param__HZ"}'
        assert bare.startswith("param__") and bare.is_db_derived
        assert not BareNodeId("fn__x__abc123").startswith(DB_DERIVED_PREFIXES[0])
        assert {bare: 1}["param__HZ"] == 1  # hashes as its text

    def test_node_id_classifies(self):
        assert isinstance(node_id("var__Wide"), BareNodeId)
        assert isinstance(node_id("var__Wide::main"), PlacedNodeId)
        existing = PlacedNodeId("var__Wide::main")
        assert node_id(existing) is existing


class TestTheHelpers:
    def test_parse_placement_id(self):
        assert parse_placement_id("var__Wide") is None
        bare, scope = parse_placement_id("var__Wide::pipe_1")
        assert isinstance(bare, BareNodeId) and (bare, scope) == ("var__Wide", "pipe_1")
        assert parse_placement_id("::main") is None

    def test_fn_node_id_round_trip(self):
        nid = fn_node_id("bandpass", "0123456789abcdef")
        assert isinstance(nid, BareNodeId)
        assert parse_fn_node_id(nid) == ("bandpass", "0123456789abcdef")
        assert parse_fn_node_id(nid.place("main")) == ("bandpass", "0123456789abcdef")
        assert parse_fn_node_id("fn__bandpass__abc123") is None  # manual suffix
        assert parse_fn_node_id("fn__bandpass") is None
        assert parse_fn_node_id("var__Wide") is None

    def test_one_root_scope(self):
        from scistack_gui import pipeline_store
        from scistack_gui.domain import scope_filter

        assert ROOT_SCOPE == "main"
        assert pipeline_store.ROOT_SCOPE is ROOT_SCOPE
        assert scope_filter.ROOT_SCOPE is ROOT_SCOPE
