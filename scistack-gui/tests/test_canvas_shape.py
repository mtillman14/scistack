"""The id-free canvas description and its diagnostics (plan
``.claude/plan-canvas-as-document.md`` Stage 0).

``canvas_shape`` is the regression oracle for the stages that move the canvas
from "derived from history on every build" to "stored document": node ids
change across those stages, what a person sees must not.
"""

from scistack_gui.canvas_shape_cli import capture_shapes, compare_shapes
from scistack_gui.domain.canvas_shape import canvas_shape, canvas_sources, diff_shapes


def _graph(prefix: str, low_values=("20",), with_param_edge=True) -> dict:
    """A tiny built graph whose node ids carry *prefix*, so two calls differ
    only in ids."""
    nodes = [
        {"id": f"{prefix}raw", "type": "variableNode", "data": {"label": "Raw", "run_state": "green"}},
        {"id": f"{prefix}fn", "type": "functionNode", "data": {"label": "bandpass", "run_state": "green"}},
        {
            "id": f"{prefix}p",
            "type": "parameterNode",
            "data": {"label": "low_hz", "values": [{"value": v} for v in low_values]},
        },
        {"id": f"{prefix}out", "type": "variableNode", "data": {"label": "Filtered", "run_state": "green"}},
    ]
    edges = [
        {"source": f"{prefix}raw", "target": f"{prefix}fn", "targetHandle": "in__signal"},
        {"source": f"{prefix}fn", "target": f"{prefix}out"},
    ]
    if with_param_edge:
        edges.append({"source": f"{prefix}p", "target": f"{prefix}fn", "targetHandle": "in__low_hz"})
    return {"pipeline_id": "main", "nodes": nodes, "edges": edges}


class TestCanvasShape:
    def test_ids_do_not_matter(self):
        a = canvas_shape(_graph("fn__history__"))
        b = canvas_shape(_graph("manual__"))
        assert a == b
        assert diff_shapes(a, b) == []

    def test_a_missing_edge_and_a_changed_value_are_reported(self):
        before = canvas_shape(_graph("a"))
        after = canvas_shape(_graph("b", low_values=("20", "30"), with_param_edge=False))
        diffs = diff_shapes(before, after)
        assert any("edge parameterNode:low_hz -> functionNode:bandpass [in__low_hz]: 1 -> 0" in d for d in diffs)
        assert any("parameterNode:low_hz" in d and "values=['20', '30']" in d for d in diffs)

    def test_positions_are_read_through_the_placement_lookup(self):
        positions = {"araw": {"x": 10.04, "y": 20.0}, "afn::main": {"x": 1.0, "y": 2.0}}
        shape = canvas_shape(_graph("a"), positions)
        by_ref = {n["ref"]: n["position"] for n in shape["nodes"]}
        assert by_ref["variableNode:Raw"] == [10.0, 20.0]
        assert by_ref["functionNode:bandpass"] == [1.0, 2.0]
        assert by_ref["variableNode:Filtered"] is None

    def test_canvas_sources_counts_stored_and_derived(self):
        graph = _graph("a")
        graph["edges"][0]["data"] = {"manual": True}
        sources = canvas_sources(graph, manual_node_ids={"araw"})
        assert sources == {
            "nodes_stored": 1,
            "nodes_derived": 3,
            "nodes_derived_by_type": {"functionNode": 1, "parameterNode": 1, "variableNode": 1},
            "edges_drawn": 1,
            "edges_derived": 2,
        }


class TestScriptRunProject:
    """``populated_db`` is a SCRIPT-run project: one for_each, no GUI rows.
    Its canvas is the golden case adoption must reproduce from stored rows."""

    def test_after_the_first_build_the_canvas_is_entirely_stored(self, client, populated_db):
        """Until Stage 2 this canvas was 100% derived from history on every
        build. The first build now adopts it, so it is 100% stored rows."""
        shapes = capture_shapes(populated_db)
        main = shapes["pipelines"]["main"]
        assert main["sources"]["nodes_derived"] == 0
        assert main["sources"]["edges_derived"] == 0
        assert main["sources"]["nodes_stored"] >= 3
        assert main["sources"]["edges_drawn"] >= 3

    def test_golden_shape(self, client, populated_db):
        shape = capture_shapes(populated_db)["pipelines"]["main"]["shape"]
        refs = {n["ref"] for n in shape["nodes"]}
        assert {
            "variableNode:RawSignal",
            "functionNode:bandpass_filter",
            "variableNode:FilteredSignal",
        } <= refs
        params = [n for n in shape["nodes"] if n["ref"] == "parameterNode:low_hz"]
        assert len(params) == 1 and params[0]["values"] == ["20"]
        pairs = {(e[0], e[1]) for e in shape["edges"]}
        assert ("variableNode:RawSignal", "functionNode:bandpass_filter") in pairs
        assert ("parameterNode:low_hz", "functionNode:bandpass_filter") in pairs
        assert ("functionNode:bandpass_filter", "variableNode:FilteredSignal") in pairs

    def test_capture_then_compare_is_clean(self, client, populated_db):
        first = capture_shapes(populated_db)
        assert compare_shapes(first, capture_shapes(populated_db)) == []
