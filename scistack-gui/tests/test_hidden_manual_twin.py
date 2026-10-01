"""A stored manual edge whose history twin is hidden binds nothing.

Regression for scidb.log 2026-10-01 11:29-11:32: the user disconnected
``GAITRiteLoaded -> calculateSymmetryOneVector.v`` (hid the history edge) and
drew ``GaitRiteLoaded_UA -> v``. An older hand-drawn ``GAITRiteLoaded -> v``
row was still in the layout. build_edges never drew it (it is superseded by
its history twin), but manual_edge_handle_index indexed it, so ``v`` bound
``['GAITRiteLoaded', 'GaitRiteLoaded_UA']``. MATLAB command generation then
refused that as an EachOf, and Run did nothing.

The rule has one owner, graph_builder.manual_edge_is_hidden. These tests pin
that the binding index, the overrides and the disconnected report follow it.
"""

from __future__ import annotations

import logging

import pytest

from scistack_gui.domain import graph_builder as gb
from scistack_gui.domain.graph_builder import (
    hidden_wirings,
    history_twin_edge_id,
    identity_token,
    manual_edge_handle_index,
    manual_edge_is_hidden,
    manual_input_overrides,
    wiring_id,
)
from scistack_gui.ids import fn_node_id

FN = "calculateSymmetryOneVector"
HISTORY = {"v": "GAITRiteLoaded"}
F_KEY = (FN, "cs1")
WID = wiring_id(FN, HISTORY, set(), {})
NODE = fn_node_id(FN, WID)
HIDDEN_HISTORY_EDGE = f"e__GAITRiteLoaded__{FN}__{WID}"

STALE_TWIN = {
    "id": "manual__stale1",
    "source": "var__GAITRiteLoaded",
    "target": NODE,
    "targetHandle": "in__v",
}
RECONNECT = {
    "id": "manual__new001",
    "source": "var__GaitRiteLoaded_UA",
    "target": NODE,
    "targetHandle": "in__v",
}


@pytest.fixture(autouse=True)
def _fresh_report_set():
    gb._REPORTED_HIDDEN_MANUAL_EDGES.clear()
    yield
    gb._REPORTED_HIDDEN_MANUAL_EDGES.clear()


class TestHistoryTwinEdgeId:
    """The twin id must be spelled exactly as build_edges spells it."""

    def test_variable_source(self):
        assert history_twin_edge_id(STALE_TWIN) == HIDDEN_HISTORY_EDGE

    def test_placement_suffixes_are_ignored(self):
        edge = {
            **STALE_TWIN,
            "source": "var__GAITRiteLoaded::main",
            "target": NODE + "::main",
        }
        assert history_twin_edge_id(edge) == HIDDEN_HISTORY_EDGE

    def test_manual_variable_node_resolves_through_its_label(self):
        edge = {**STALE_TWIN, "source": "mv_abc"}
        manual_nodes = {"mv_abc": {"type": "variableNode", "label": "GAITRiteLoaded"}}
        assert history_twin_edge_id(edge, manual_nodes) == HIDDEN_HISTORY_EDGE

    def test_path_input_source(self):
        edge = {
            "id": "m",
            "source": "pathInput__GaitRite",
            "target": NODE,
            "targetHandle": "in__gaitRitePath",
        }
        assert history_twin_edge_id(edge) == f"e__GaitRite__gaitRitePath__{FN}__{WID}"

    def test_parameter_source_is_keyed_by_the_argument(self):
        edge = {
            "id": "m",
            "source": "param__formulaNum",
            "target": NODE,
            "targetHandle": "param__formula",
        }
        assert history_twin_edge_id(edge) == f"e__formula__{FN}__{WID}"

    def test_edge_into_a_variable_has_no_twin(self):
        edge = {"id": "m", "source": NODE, "target": "var__X", "targetHandle": ""}
        assert history_twin_edge_id(edge) is None


class TestManualEdgeIsHidden:
    def test_hidden_twin_hides_the_manual_edge(self):
        assert manual_edge_is_hidden(STALE_TWIN, {HIDDEN_HISTORY_EDGE})

    def test_own_id_hidden(self):
        assert manual_edge_is_hidden(STALE_TWIN, {"manual__stale1"})

    def test_visible_twin_leaves_it_visible(self):
        assert not manual_edge_is_hidden(STALE_TWIN, {"e__Other__x__y"})
        assert not manual_edge_is_hidden(STALE_TWIN, frozenset())

    def test_a_different_variable_is_not_hidden_by_the_old_edge(self):
        assert not manual_edge_is_hidden(RECONNECT, {HIDDEN_HISTORY_EDGE})


class TestIndexLeavesHiddenEdgesOut:
    def test_stale_twin_is_not_indexed(self):
        index = manual_edge_handle_index(
            [STALE_TWIN, RECONNECT], hidden_edge_ids={HIDDEN_HISTORY_EDGE}
        )
        assert index[(FN, WID, "in__v")] == [RECONNECT]

    def test_twin_is_indexed_while_its_history_edge_is_visible(self):
        index = manual_edge_handle_index([STALE_TWIN], hidden_edge_ids=frozenset())
        assert index[(FN, WID, "in__v")] == [STALE_TWIN]

    def test_dropped_edge_is_logged_once_at_info(self, caplog):
        caplog.set_level(logging.INFO, logger="scistack_gui.domain.graph_builder")
        for _ in range(3):
            manual_edge_handle_index(
                [STALE_TWIN, RECONNECT], hidden_edge_ids={HIDDEN_HISTORY_EDGE}
            )
        lines = [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.INFO and "manual__stale1" in r.getMessage()
        ]
        assert len(lines) == 1
        assert f"twin {HIDDEN_HISTORY_EDGE} hidden" in lines[0]


class TestBindingFollowsTheCanvas:
    """The 2026-10-01 scenario end to end through the pure owners."""

    def test_reconnect_binds_only_the_new_variable(self):
        hidden = {HIDDEN_HISTORY_EDGE}
        index = manual_edge_handle_index(
            [STALE_TWIN, RECONNECT], hidden_edge_ids=hidden
        )
        overrides = manual_input_overrides(FN, WID, HISTORY, set(), index, None, hidden)
        # A bare string: one source, not the EachOf MATLAB generation refuses.
        assert overrides == {"v": "GaitRiteLoaded_UA"}

    def test_stale_twin_alone_leaves_the_input_disconnected(self):
        hidden = {HIDDEN_HISTORY_EDGE}
        index = manual_edge_handle_index([STALE_TWIN], hidden_edge_ids=hidden)
        assert manual_input_overrides(FN, WID, HISTORY, set(), index, None, hidden) == {}
        assert hidden_wirings(
            fn_input_params={F_KEY: HISTORY},
            fn_outputs={},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids=hidden,
            token_for=identity_token,
            manual_edges=[STALE_TWIN],
        ) == {(FN, WID)}

    def test_reconnect_clears_the_disconnected_state(self):
        assert hidden_wirings(
            fn_input_params={F_KEY: HISTORY},
            fn_outputs={},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids={HIDDEN_HISTORY_EDGE},
            token_for=identity_token,
            manual_edges=[STALE_TWIN, RECONNECT],
        ) == set()
