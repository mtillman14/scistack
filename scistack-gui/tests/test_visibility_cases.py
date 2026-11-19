"""THE visibility rule against docs/claude/visibility-cases.json.

Owner: ``scistack_gui.domain.visibility.Visibility``; concept
``docs/claude/visibility-model.md``. Each case asks one question of one small
world (main places a submodule; a hypothesis beside it). The answer must not
depend on which canvas asks, which is what the 2026-11-19 XSENS run broke: a
copy hidden inside the submodule cut main's lines into its ports.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scistack_gui.domain.visibility import Visibility

CASES_FILE = (
    Path(__file__).resolve().parents[2] / "docs" / "claude" / "visibility-cases.json"
)


def _data() -> dict:
    if not CASES_FILE.exists():
        raise AssertionError(f"Visibility cases not found at {CASES_FILE}.")
    return json.loads(CASES_FILE.read_text(encoding="utf-8"))


def _world(extra_node_hides: "dict | None" = None) -> Visibility:
    w = _data()["world"]
    node_hides = {k: set(v) for k, v in w["node_hides"].items()}
    for canvas, ids_ in (extra_node_hides or {}).items():
        node_hides.setdefault(canvas, set()).update(ids_)
    return Visibility(
        node_hides={k: frozenset(v) for k, v in node_hides.items()},
        edge_hides={k: frozenset(v) for k, v in w["edge_hides"].items()},
        value_hides={
            canvas: {name: set(values) for name, values in by_name.items()}
            for canvas, by_name in w["value_hides"].items()
        },
        combos=frozenset(w["combos"]),
        manual_nodes=w["manual_nodes"],
        positions_by_scope={},
        use_parent=w["use_parent"],
        edge_hide_ends={
            canvas: {conn: [tuple(pair) for pair in ends] for conn, ends in by_conn.items()}
            for canvas, by_conn in w.get("edge_hide_ends", {}).items()
        },
    )


CASES = _data()["cases"]


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_visibility_case(case):
    vis = _world(case.get("extra_node_hides"))
    ask = case["ask"]
    if ask == "edge_live":
        got = vis.edge_live(case["edge"])
    elif ask == "node_hidden_on":
        got = vis.node_hidden_on(case["canvas"], case["node"])
    elif ask == "hidden_port_ids":
        got = sorted(vis.hidden_port_ids())
    elif ask == "hidden_values_for_node":
        got = {
            k: sorted(v)
            for k, v in vis.hidden_values_for_node(case["node"], case["wiring"]).items()
        }
    elif ask == "connection_hidden":
        got = vis.connection_hidden(case["connection"], case["source"], case["target"])
    else:
        raise AssertionError(f"unknown ask {ask!r} in {CASES_FILE.name}")
    assert got == case["expect"], case["name"]


def test_an_edge_s_answer_does_not_depend_on_who_asks():
    """The class of bug, stated directly: Visibility has no asking canvas, so
    every edge in the world gets one answer. Pinned for the XSENS shape."""
    vis = _world()
    outer = {"id": "o1", "source": "pathInput__P::main", "target": "use_1",
             "targetHandle": "in__xsens_path"}
    assert vis.canvases_of_edge(outer) == ("main",)
    assert vis.edge_live(outer)
