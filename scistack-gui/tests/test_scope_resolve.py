"""A hand-placed node that has run is visible wherever it is placed.

Regression for scidb.log 2026-10-01 22:51 / 23:03: duplicating the Aim 2
hypothesis drew 1 of 4 function nodes. The missing three had been dragged onto
the canvas and then run, so each is BOTH a hand-placed row (scope main) and a DB
node with the same allocated id. Duplicating placed them in the copy as
``{id}::{copy}``, but _resolve_in_scope answered from the manual row's own
scope and returned "not visible" before looking at the placement.
"""

from __future__ import annotations

from scistack_gui.domain.scope_filter import _resolve_in_scope, resolve_scope_view

NODE = "fn__grSides__cd2bd329841b413c"
COPY = "pipe_bf2a723782c5"
MANUAL = {NODE: {"type": "functionNode", "label": "grSides", "pipeline_id": "main"}}
POSITIONS = {
    "main": {NODE: {"x": 0, "y": 0}},
    COPY: {f"{NODE}::{COPY}": {"x": 0, "y": 0}},
}


def test_hand_placed_node_resolves_in_its_own_scope():
    assert _resolve_in_scope(NODE, "main", MANUAL, POSITIONS) == NODE


def test_hand_placed_node_resolves_through_its_placement_in_a_copy():
    assert _resolve_in_scope(NODE, COPY, MANUAL, POSITIONS) == f"{NODE}::{COPY}"


def test_hand_placed_node_is_not_visible_where_it_is_not_placed():
    assert _resolve_in_scope(NODE, "pipe_elsewhere", MANUAL, POSITIONS) is None


def test_the_copy_view_keeps_the_node_and_its_edge():
    nodes = [
        {"id": NODE, "type": "functionNode", "data": {"label": "grSides"}},
        {"id": "var__Raw", "type": "variableNode", "data": {"label": "Raw"}},
    ]
    edges = [{"id": "e1", "source": "var__Raw", "target": NODE, "targetHandle": "in__x"}]
    positions = {**POSITIONS, COPY: {**POSITIONS[COPY], f"var__Raw::{COPY}": {"x": 1, "y": 1}}}
    kept_nodes, kept_edges = resolve_scope_view(nodes, edges, COPY, MANUAL, positions)
    assert f"{NODE}::{COPY}" in {n["id"] for n in kept_nodes}
    assert [(e["source"], e["target"]) for e in kept_edges] == [
        (f"var__Raw::{COPY}", f"{NODE}::{COPY}")
    ]
