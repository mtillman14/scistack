"""A Parameter edge follows the node's CURRENT wiring, like its handle.

Regression for scidb.log 2026-10-01 12:38: the user drew formulaNum ->
calculateSymmetryOneVector and the edge disappeared. The node had been rewired
(v: GAITRiteLoaded -> GaitRiteLoaded_UA), and its current wiring had never run,
so it had no constant handle and formulaNum showed as an ``in__formulaNum``
port. group_call_sites_by_wiring still mapped the HISTORY call site's
const_fns into the group, so build_edges drew ``param__formulaNum -> node`` onto
a ``param__formulaNum`` handle the node did not render. React Flow dropped that
edge, and build_edges dropped the user's drawn edge as "superseded" by it.
"""

from __future__ import annotations

from scistack_gui.domain.graph_builder import (
    AggregatedData,
    build_edges,
    group_call_sites_by_wiring,
)
from scistack_gui.ids import fn_node_id

FN = "calculateSymmetryOneVector"
HISTORY_CID = "c" * 16
HISTORY_KEY = (FN, HISTORY_CID)
TOKEN = "f5e43677b2b7437d"
NODE = fn_node_id(FN, TOKEN)


def _history_only_agg() -> AggregatedData:
    """One recorded call site: the shape the node was rewired AWAY from."""
    agg = AggregatedData()
    agg.fn_input_params[HISTORY_KEY] = {"v": "GAITRiteLoaded"}
    agg.fn_outputs[HISTORY_KEY] = {"GaitRiteSymmetry"}
    agg.fn_constants[HISTORY_KEY] = {"formulaNum"}
    agg.const_fns["formulaNum"] = {HISTORY_KEY}
    agg.const_counts["formulaNum"] = 1
    agg.fn_variants_map[HISTORY_KEY] = [{"constants": {"formulaNum": 6}}]
    return agg


def _group(is_current):
    return group_call_sites_by_wiring(
        _history_only_agg(),
        {},
        token_for=lambda _fn, _wiring: TOKEN,
        is_current=is_current,
    )


class TestParameterEdgesFollowTheCurrentWiring:
    def test_a_history_only_constant_draws_no_edge(self):
        grouped, _, _ = _group(lambda _fn, _wiring: False)
        gkey = (FN, TOKEN)
        # The handle side, which was already right...
        assert grouped.fn_constants[gkey] == set()
        # ...and now the edge side agrees with it. The key stays, so the
        # Parameter node itself is still on the canvas.
        assert grouped.const_fns["formulaNum"] == set()

    def test_a_current_constant_still_draws_its_edge(self):
        grouped, _, _ = _group(lambda _fn, _wiring: True)
        assert grouped.const_fns["formulaNum"] == {(FN, TOKEN)}

    def test_the_drawn_edge_is_no_longer_superseded(self):
        grouped, _, _ = _group(lambda _fn, _wiring: False)
        drawn = {
            "id": "manual__zg1466",
            "source": "param__formulaNum::main",
            "target": NODE,
            "targetHandle": "in__formulaNum",
        }
        edges = build_edges(
            fn_input_params=dict(grouped.fn_input_params),
            fn_outputs=dict(grouped.fn_outputs),
            const_fns=dict(grouped.const_fns),
            path_inputs={},
            manual_edges=[drawn],
            hidden_ids=set(),
        )
        formula_edges = [e for e in edges if "formulaNum" in e["source"]]
        assert [e["id"] for e in formula_edges] == ["manual__zg1466"]
        assert formula_edges[0]["targetHandle"] == "in__formulaNum"
