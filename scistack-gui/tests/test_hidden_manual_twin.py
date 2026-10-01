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

import numpy as np
import pytest
from scidb import BaseVariable

from scistack_gui import pipeline_store
from scistack_gui.db import get_db
from scistack_gui.domain import graph_builder as gb
from scistack_gui.domain.edge_resolver import (
    infer_manual_fn_output_types,
    resolve_function_edges,
)
from scistack_gui.domain.graph_builder import (
    hidden_wirings,
    history_twin_edge_id,
    identity_token,
    manual_edge_handle_index,
    manual_edge_is_hidden,
    manual_input_overrides,
    visible_manual_edges,
    wiring_id,
)
from scistack_gui.ids import fn_node_id
from scistack_gui.services.execution_service import derive_target_for_node

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

    def test_parameter_on_an_input_port_has_the_same_twin(self):
        """A Parameter drawn onto in__X before the node ran with it; history
        later draws the same connection as param__X."""
        edge = {
            "id": "m",
            "source": "param__formulaNum::main",
            "target": NODE,
            "targetHandle": "in__formulaNum",
        }
        assert history_twin_edge_id(edge) == f"e__formulaNum__{FN}__{WID}"

    def test_output_edge(self):
        edge = {"id": "m", "source": NODE, "target": "var__GaitRiteSymmetry"}
        assert history_twin_edge_id(edge) == f"e__{FN}__{WID}__GaitRiteSymmetry"

    def test_edge_between_variables_has_no_twin(self):
        edge = {"id": "m", "source": "var__A", "target": "var__B"}
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
            is_current=lambda _fn, _wiring: True,
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
            is_current=lambda _fn, _wiring: True,
        ) == set()


class TestEdgeResolverReadsOnlyVisibleEdges:
    """The second reader (2026-10-01, after the index fix): a node still in
    _pipeline_nodes resolves from resolve_function_edges, which read raw rows,
    so the MATLAB run still failed with the same EachOf."""

    def test_resolve_function_edges_binds_only_the_reconnect(self):
        resolved = resolve_function_edges(
            fn_node_ids={NODE},
            manual_edges=[STALE_TWIN, RECONNECT],
            manual_nodes={},
            existing_node_labels={},
            hidden_edge_ids={HIDDEN_HISTORY_EDGE},
        )
        assert resolved.input_types == {"v": "GaitRiteLoaded_UA"}

    def test_without_the_hide_both_bind(self):
        """Control: the same rows with nothing hidden are a real EachOf."""
        resolved = resolve_function_edges(
            fn_node_ids={NODE},
            manual_edges=[STALE_TWIN, RECONNECT],
            manual_nodes={},
            existing_node_labels={},
            hidden_edge_ids=frozenset(),
        )
        assert sorted(resolved.input_type_candidates["v"]) == [
            "GAITRiteLoaded",
            "GaitRiteLoaded_UA",
        ]

    def test_hidden_output_twin_claims_no_output(self):
        stale_out = {"id": "manual__out1", "source": NODE, "target": "var__OldOut"}
        new_out = {"id": "manual__out2", "source": NODE, "target": "var__NewOut"}
        assert infer_manual_fn_output_types(
            {NODE},
            [stale_out, new_out],
            {},
            existing_node_labels={},
            hidden_edge_ids={f"e__{FN}__{WID}__OldOut"},
        ) == ["NewOut"]

    def test_visible_manual_edges_keeps_order_and_drops_hidden(self):
        assert visible_manual_edges(
            [STALE_TWIN, RECONNECT], {HIDDEN_HISTORY_EDGE}
        ) == [RECONNECT]


class TwinOldSignal(BaseVariable):
    pass


class TwinNewSignal(BaseVariable):
    pass


class TwinFiltered(BaseVariable):
    pass


class TestDeriveTargetForNodeIgnoresHiddenTwin:
    """The path the failing MATLAB run took: a function node that is still a
    manual row, whose id is in the fn__{fn}__{token} form, resolving its
    target from its own edges."""

    TOKEN = "aaaabbbbccccdddd"

    def test_reconnected_input_is_the_only_binding(self, client):
        TwinOldSignal.save(np.zeros(5), subject=1, session="pre")
        TwinNewSignal.save(np.zeros(5), subject=1, session="pre")
        node = fn_node_id("bandpass_filter", self.TOKEN)
        for nid, ntype, label in [
            ("mv_twin_old", "variableNode", "TwinOldSignal"),
            ("mv_twin_new", "variableNode", "TwinNewSignal"),
            (node, "functionNode", "bandpass_filter"),
            ("mv_twin_out", "variableNode", "TwinFiltered"),
            ("mc_twin_low_hz", "parameterNode", "low_hz"),
        ]:
            client.put(
                f"/api/layout/{nid}",
                json={"x": 0, "y": 0, "node_type": ntype, "label": label},
            )
        client.put("/api/edges/e_twin_old", json={
            "source": "mv_twin_old", "target": node, "target_handle": "in__signal",
        })
        client.put("/api/edges/e_twin_new", json={
            "source": "mv_twin_new", "target": node, "target_handle": "in__signal",
        })
        client.put("/api/edges/e_twin_out", json={"source": node, "target": "mv_twin_out"})
        client.put("/api/edges/e_twin_low_hz", json={
            "source": "mc_twin_low_hz", "target": node, "target_handle": "in__low_hz",
        })

        db = get_db()
        # Control: both edges visible is a genuine EachOf.
        before = derive_target_for_node(db, node)
        assert before and isinstance(before[0]["input_types"]["signal"], list)

        # The user disconnects the OLD wire. The canvas drew its history twin,
        # so that is the id that gets hidden.
        pipeline_store.hide_edge(
            db, f"e__TwinOldSignal__bandpass_filter__{self.TOKEN}"
        )
        after = derive_target_for_node(db, node)
        assert after
        assert {t["input_types"]["signal"] for t in after} == {"TwinNewSignal"}


class TestOldParameterEdgeOnAnInputPort:
    """Regression for scidb.log 2026-10-01 14:52: three formulaNum edges were
    stored on in__formulaNum (from before the node ran with formulaNum). History
    then drew the same connection on param__formulaNum. Deleting that visible
    edge hid it, but the in__ copies kept binding formulaNum for edge
    resolution (the MATLAB route) while the disconnected check saw no edge."""

    OLD_COPY = {
        "id": "manual__vdw6ip",
        "source": "param__formulaNum::main",
        "target": NODE,
        "targetHandle": "in__formulaNum",
    }
    HISTORY_EDGE = f"e__formulaNum__{FN}__{WID}"

    def test_hiding_the_history_edge_hides_the_old_copy(self):
        assert manual_edge_is_hidden(self.OLD_COPY, {self.HISTORY_EDGE})
        index = manual_edge_handle_index([self.OLD_COPY], hidden_edge_ids={self.HISTORY_EDGE})
        assert (FN, WID, "in__formulaNum") not in index

    def test_edge_resolution_no_longer_binds_the_deleted_parameter(self):
        resolved = resolve_function_edges(
            fn_node_ids={NODE},
            manual_edges=[self.OLD_COPY],
            manual_nodes={},
            existing_node_labels={},
            hidden_edge_ids={self.HISTORY_EDGE},
        )
        assert "formulaNum" not in resolved.parameter_params

    def test_control_while_visible_it_still_binds(self):
        resolved = resolve_function_edges(
            fn_node_ids={NODE},
            manual_edges=[self.OLD_COPY],
            manual_nodes={},
            existing_node_labels={},
            hidden_edge_ids=frozenset(),
        )
        assert resolved.parameter_params == {"formulaNum": "formulaNum"}
