"""A node's PathInput edges and its disconnected state follow its CURRENT
wiring, as its handles and Parameter edges already do.

Two gaps left by the Parameter-edge fix (test_history_parameter_edges.py):

1. A node rewired from PathInput A to PathInput B and run kept drawing
   ``A -> node`` forever: group_call_sites_by_wiring mapped every call site's
   PathInputs, history included, into the group that build_edges draws from.
2. Hiding that stale edge could turn the whole node red. hidden_wirings checked
   every recorded wiring, found the historical one with a hidden input, and
   reported the node's TOKEN as disconnected, which wiring_disconnected_fkeys
   maps back to every call site, the current one included. The same held for a
   hidden history VARIABLE edge with nothing drawn over it.
"""

from __future__ import annotations

from scistack_gui.domain.graph_builder import (
    AggregatedData,
    build_edges,
    connection_id,
    group_call_sites_by_wiring,
    hidden_wirings,
    wiring_id,
)
from scistack_gui.ids import fn_node_id

FN = "loadGaitRiteOneFile"
TOKEN = "e19566adeee442b8"
NODE = fn_node_id(FN, TOKEN)
HIST_KEY = (FN, "a" * 16)
CUR_KEY = (FN, "b" * 16)

HIST_WIRING = wiring_id(FN, {}, {"GAITRiteLoaded"}, {"gaitRitePath": "GaitRiteOld"})
CUR_WIRING = wiring_id(FN, {}, {"GAITRiteLoaded"}, {"gaitRitePath": "GaitRiteNew"})


def _token_for(_fn, _wiring):
    return TOKEN


def _is_current(fn, wiring):
    return fn == FN and wiring == CUR_WIRING


def _all_current(_fn, _wiring):
    return True


def _rewired_path_input_agg() -> AggregatedData:
    """Ran with GaitRiteOld, rewired to GaitRiteNew, ran again."""
    agg = AggregatedData()
    for key in (HIST_KEY, CUR_KEY):
        agg.fn_input_params[key] = {}
        agg.fn_outputs[key] = {"GAITRiteLoaded"}
    agg.path_inputs["GaitRiteOld"] = {"functions": {(HIST_KEY, "gaitRitePath")}}
    agg.path_inputs["GaitRiteNew"] = {"functions": {(CUR_KEY, "gaitRitePath")}}
    return agg


class TestPathInputEdgesFollowTheCurrentWiring:
    def test_the_old_path_input_draws_no_edge(self):
        grouped, _, _ = group_call_sites_by_wiring(
            _rewired_path_input_agg(), {}, token_for=_token_for, is_current=_is_current
        )
        # The key stays, so the PathInput node itself is still built.
        assert grouped.path_inputs["GaitRiteOld"]["functions"] == set()
        # ...and it has run, so it is not mistaken for declared-only.
        assert grouped.path_inputs["GaitRiteOld"]["recorded"] is True
        assert grouped.path_inputs["GaitRiteNew"]["functions"] == {
            ((FN, TOKEN), "gaitRitePath")
        }
        edges = build_edges(
            fn_input_params=dict(grouped.fn_input_params),
            fn_outputs=dict(grouped.fn_outputs),
            const_fns=dict(grouped.const_fns),
            path_inputs=dict(grouped.path_inputs),
            manual_edges=[],
            hidden_ids=set(),
        )
        sources = {e["source"] for e in edges if e["target"] == NODE}
        assert "pathInput__GaitRiteNew" in sources
        assert "pathInput__GaitRiteOld" not in sources

    def test_with_every_wiring_current_both_still_draw(self):
        """Control: the pre-allocation behaviour is unchanged."""
        grouped, _, _ = group_call_sites_by_wiring(
            _rewired_path_input_agg(), {}, token_for=_token_for, is_current=_all_current
        )
        assert grouped.path_inputs["GaitRiteOld"]["functions"]
        assert grouped.path_inputs["GaitRiteNew"]["functions"]


class TestHidingAHistoryEdgeDoesNotDisconnectTheNode:
    def _hidden_wirings(self, agg, hidden, is_current):
        return hidden_wirings(
            fn_input_params=dict(agg.fn_input_params),
            fn_outputs=dict(agg.fn_outputs),
            fn_constants={},
            path_inputs=dict(agg.path_inputs),
            hidden_edge_ids=hidden,
            token_for=_token_for,
            is_current=is_current,
        )

    def test_hiding_the_stale_path_input_edge_leaves_the_node_connected(self):
        hidden = {connection_id("pathInput__GaitRiteOld", NODE, "in__gaitRitePath")}
        assert self._hidden_wirings(_rewired_path_input_agg(), hidden, _is_current) == set()

    def test_control_every_wiring_current_reports_it(self):
        hidden = {connection_id("pathInput__GaitRiteOld", NODE, "in__gaitRitePath")}
        assert self._hidden_wirings(
            _rewired_path_input_agg(), hidden, _all_current
        ) == {(FN, TOKEN)}

    def test_hiding_the_current_path_input_edge_still_disconnects(self):
        hidden = {connection_id("pathInput__GaitRiteNew", NODE, "in__gaitRitePath")}
        assert self._hidden_wirings(
            _rewired_path_input_agg(), hidden, _is_current
        ) == {(FN, TOKEN)}

    def test_a_hidden_history_variable_edge_leaves_the_node_connected(self):
        fn = "calculateSymmetryOneVector"
        hist, cur = (fn, "c" * 16), (fn, "d" * 16)
        agg = AggregatedData()
        agg.fn_input_params[hist] = {"v": "GAITRiteLoaded"}
        agg.fn_input_params[cur] = {"v": "GaitRiteLoaded_UA"}
        for k in (hist, cur):
            agg.fn_outputs[k] = {"GaitRiteSymmetry"}
        cur_wiring = wiring_id(fn, {"v": "GaitRiteLoaded_UA"}, {"GaitRiteSymmetry"}, {})
        hidden = {connection_id("var__GAITRiteLoaded", fn_node_id(fn, TOKEN), "in__v")}
        assert self._hidden_wirings(
            agg, hidden, lambda f, w: f == fn and w == cur_wiring
        ) == set()
