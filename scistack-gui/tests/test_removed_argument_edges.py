"""An edge on a port whose argument the function no longer takes.

Regression (2026-10-27): ``parse_loaded_data_per_subject(df_in, ...)`` was
renamed to take ``data_path``. The canvas drew only ``in__data_path``, so the
stored edge on ``in__df_in`` was invisible and could not be deleted, yet the
run still bound it: "unexpected keyword argument 'df_in'". One rule,
``edge_view.missing_port``, now decides it for function and glue nodes; the
end-to-end scenarios are in tests/test_edge_parity.py.
"""

from __future__ import annotations

import inspect

import pytest
from scifor.signature import Argument

from scistack_gui.domain.edge_view import missing_port

PK = inspect.Parameter.POSITIONAL_OR_KEYWORD
FN_NODE = "fn__parse__abc123"


def _edge(eid, target_handle, target=FN_NODE, source="pathInput__data_path"):
    return {
        "id": eid,
        "source": source,
        "target": target,
        "targetHandle": target_handle,
        "sourceHandle": None,
    }


@pytest.fixture
def signature(monkeypatch):
    """Set the function's signature as the one reader returns it."""
    from scistack_gui.api import pipeline as pipeline_api

    def set_(arguments):
        monkeypatch.setattr(pipeline_api, "_fn_arguments", lambda n: arguments)

    return set_


MANUAL_NODES = {FN_NODE: {"type": "functionNode", "label": "parse"}}


def test_renamed_function_argument_is_stale(signature):
    signature([Argument("data_path", PK), Argument("col", PK, "Subject")])
    assert missing_port(_edge("e_old", "in__df_in"), MANUAL_NODES, {}) == (
        "parse",
        ["data_path", "col"],
    )


def test_current_argument_is_not_stale(signature):
    signature([Argument("data_path", PK)])
    assert missing_port(_edge("e_new", "in__data_path"), MANUAL_NODES, {}) is None


def test_placement_qualified_target_is_matched(signature):
    signature([Argument("data_path", PK)])
    edge = _edge("e_old", "in__df_in", target=f"{FN_NODE}::main")
    assert missing_port(edge, MANUAL_NODES, {}) is not None


def test_unknown_signature_keeps_the_edge(signature):
    """MATLAB not scanned yet, or an import error: nothing is known stale."""
    signature([])
    assert missing_port(_edge("e_old", "in__df_in"), MANUAL_NODES, {}) is None


def test_kwargs_accepts_any_port(signature):
    signature(
        [Argument("data_path", PK), Argument("kwargs", inspect.Parameter.VAR_KEYWORD)]
    )
    assert missing_port(_edge("e_old", "in__df_in"), MANUAL_NODES, {}) is None


def test_output_edges_and_other_nodes_are_not_judged(signature):
    signature([Argument("data_path", PK)])
    out_edge = _edge("e_out", None, target="var__Loaded", source=FN_NODE)
    assert missing_port(out_edge, MANUAL_NODES, {}) is None
    unknown_node = _edge("e_x", "in__df_in", target="fn__not_stored")
    assert missing_port(unknown_node, MANUAL_NODES, {}) is None


def test_signature_is_read_once_per_pass(monkeypatch):
    from scistack_gui.api import pipeline as pipeline_api

    calls = []

    def reader(name):
        calls.append(name)
        return [Argument("data_path", PK)]

    monkeypatch.setattr(pipeline_api, "_fn_arguments", reader)
    memo: dict = {}
    for i in range(3):
        missing_port(_edge(f"e{i}", "in__df_in"), MANUAL_NODES, memo)
    assert calls == ["parse"]
