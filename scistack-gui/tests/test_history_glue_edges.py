"""A glued history input is drawn through its glue node, not past it.

Regression for scidb.log 2026-10-21 (run uw6ncxs0): Parameter ``config`` ->
``glue_xsens_config`` -> ``load_xsens_one_file.col_names``. After the run,
history drew ``param__config -> fn.col_names`` straight into the function,
beside the user's drawn ``glue -> fn`` edge — two paths into one handle, and
the user had to hide the history one. scidb now records the glue chain per
argument (``pipeline_variants``' ``glue_chains``, carried by
``aggregate_pipeline_variants``), and :func:`build_edges` leaves out the
direct edge when the drawn chain is the recorded one
(:func:`history_glue_on_canvas`).
"""

from __future__ import annotations

from scistack_gui.domain.graph_builder import build_edges, history_glue_on_canvas
from scistack_gui.ids import fn_node_id

FN = "load_xsens_one_file"
TOKEN = "c09a678b398c48d2"
FKEY = (FN, TOKEN)
NODE = fn_node_id(FN, TOKEN)
GLUE = "var__glue_xsens_config__nkjld9"
MANUAL_NODES = {GLUE: {"type": "glueNode", "label": "glue_xsens_config"}}


def _drawn(glue_label_matches: bool = True) -> tuple[list[dict], dict]:
    nodes = dict(MANUAL_NODES)
    if not glue_label_matches:
        nodes = {GLUE: {"type": "glueNode", "label": "glue_other"}}
    edges = [
        {
            "id": "manual__param_to_glue",
            "source": "param__config::main",
            "target": GLUE,
            "targetHandle": "in__filepath",
        },
        {
            "id": "manual__glue_to_fn",
            "source": GLUE,
            "target": NODE,
            "targetHandle": "in__col_names",
        },
    ]
    return edges, nodes


def _build(manual_edges, manual_nodes, *, input_params=None, const=True):
    return build_edges(
        fn_input_params={FKEY: dict(input_params or {})},
        fn_outputs={FKEY: {"XSENS"}},
        const_fns={"config": {FKEY}} if const else {},
        path_inputs={},
        manual_edges=manual_edges,
        hidden_ids=set(),
        fn_parameter_names={FKEY: {"col_names": "config"}},
        fn_glue_chains={FKEY: {"col_names": ["glue_xsens_config"]}},
        manual_nodes=manual_nodes,
    )


def _into_col_names(edges):
    return sorted(
        (e["source"], e["id"])
        for e in edges
        if e["target"] == NODE and e.get("targetHandle") == "in__col_names"
    )


def test_a_parameter_fed_glue_is_drawn_through_its_glue_node():
    edges, nodes = _drawn()
    got = _into_col_names(_build(edges, nodes))
    assert got == [(GLUE, "manual__glue_to_fn")], got


def test_a_variable_fed_glue_is_drawn_through_its_glue_node():
    edges, nodes = _drawn()
    edges[0] = {**edges[0], "source": "var__RawConfig"}
    got = _into_col_names(
        _build(edges, nodes, input_params={"col_names": "RawConfig"}, const=False)
    )
    assert got == [(GLUE, "manual__glue_to_fn")], got


def test_without_the_glue_on_the_canvas_history_keeps_its_direct_edge():
    """Dropping the edge with no glue drawn would lose the connection."""
    got = _into_col_names(_build([], {}))
    assert [src for src, _id in got] == ["param__config"], got


def test_a_different_chain_on_the_canvas_does_not_hide_history():
    edges, nodes = _drawn(glue_label_matches=False)
    sources = [src for src, _id in _into_col_names(_build(edges, nodes))]
    assert "param__config" in sources and GLUE in sources


def test_the_helper_reports_represented_and_undrawn():
    edges, nodes = _drawn()
    represented, undrawn = history_glue_on_canvas(
        {FKEY: {"col_names": ["glue_xsens_config"]}}, edges, nodes
    )
    assert represented == {(NODE, "col_names")}
    assert undrawn == []

    represented, undrawn = history_glue_on_canvas(
        {FKEY: {"col_names": ["glue_xsens_config"]}}, [], {}
    )
    assert represented == set()
    assert len(undrawn) == 1 and "col_names" in undrawn[0]


def test_build_edges_logs_the_glue_routing(caplog):
    import logging

    edges, nodes = _drawn()
    with caplog.at_level(logging.INFO, logger="scistack_gui"):
        _build(edges, nodes)
    assert any(
        "drawn through their glue node" in r.getMessage() for r in caplog.records
    )
