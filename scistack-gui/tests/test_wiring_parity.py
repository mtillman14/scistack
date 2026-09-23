"""The fifth identity: a node's STATED wiring equals the wiring its run records.

``scidb/tests/test_identity_parity.py`` pins four identities — invocation_id,
call_id, selector, current records — each by the same method: run something
for real, reconstruct the identity from what provenance recorded, and assert
the two are the same bytes. Every bug of the 2026-09-19 session had one shape,
two derivations of one fact agreeing by convention, and that tier is what
catches it at authoring time.

This is the fifth, and it lives here rather than there because one of its two
sides is the CANVAS: the node's stated wiring is history plus the edges the
user drew, and scidb must never import ``pipeline_store``
(``docs/claude/intent-and-fact.md`` §8). Separately named from the scidb file
so a full-monorepo collection can never see two modules with one name.

**The question it answers.** Forward: what wiring does this node say it has?
Backward: what wiring did its run write? Those two disagreeing is precisely
what used to make a node duplicate — the id was the backward answer, and the
user was editing the forward one (``docs/claude/node-identity.md``).
"""

import numpy as np
import pytest
from conftest import FilteredSignal, RawSignal

import scistack_gui.db as _gui_db
from scidb import BaseVariable, for_each
from scistack_gui import node_wiring, registry as _registry
from scistack_gui.domain.graph_builder import (
    manual_edge_handle_index,
    manual_input_overrides,
    wiring_id,
)


@pytest.fixture
def wide(client):
    def bandpass_filter(signal, low_hz, side=None):  # noqa: ARG001
        return np.asarray(signal, dtype=float) * float(low_hz)

    _registry._functions["bandpass_filter"] = bandpass_filter
    return client


@pytest.fixture
def side_var(wide):
    class SideTable(BaseVariable):
        pass

    SideTable.save(np.zeros(3), subject=1, session="pre")
    return SideTable


def _stated_wiring(db, node_id: str, recorded_params: dict) -> str:
    """FORWARD: what the node says it is — its recorded bindings with the
    edges the user drew folded in, which is what a run of it would record."""
    from scistack_gui import pipeline_store

    token = node_id.split("__", 2)[2]
    overrides = manual_input_overrides(
        "bandpass_filter",
        token,
        recorded_params,
        {"low_hz"},
        manual_edge_handle_index(pipeline_store.get_manual_edges(db)),
    )
    effective = {
        **recorded_params,
        **{
            param: (sources[0] if isinstance(sources, list) else sources)
            for param, sources in overrides.items()
        },
    }
    return wiring_id("bandpass_filter", effective, {"FilteredSignal"}, {})


def _recorded_wirings(db) -> set[str]:
    """BACKWARD: every wiring provenance holds for this function."""
    out = set()
    for v in db.list_pipeline_variants():
        if v["function_name"] != "bandpass_filter":
            continue
        out.add(
            wiring_id(
                "bandpass_filter",
                {k: t for k, t in v["input_types"].items()},
                {v["output_type"]},
                {},
            )
        )
    return out


def test_before_any_edge_the_two_sides_already_agree(client, bp_node_id):
    db = _gui_db.get_db()
    client.get("/api/pipeline")

    stated = _stated_wiring(db, bp_node_id, {"signal": "RawSignal"})
    assert stated in _recorded_wirings(db)


def test_a_drawn_edge_moves_the_stated_wiring_ahead_of_the_recorded_one(
    wide, side_var, bp_node_id
):
    """The gap this whole change is about: between drawing and running, the
    node states a wiring nothing has recorded. That is legitimate — it is an
    intent about a future run — and it must NOT move the node's id."""
    db = _gui_db.get_db()
    wide.put(
        "/api/edges/manual__side",
        json={
            "source": "var__SideTable",
            "target": bp_node_id,
            "source_handle": None,
            "target_handle": "in__side",
        },
    )
    ids_before = {
        n["id"] for n in wide.get("/api/pipeline").json()["nodes"]
    }

    stated = _stated_wiring(db, bp_node_id, {"signal": "RawSignal"})
    assert stated not in _recorded_wirings(db), "the edge has somehow already run"
    assert bp_node_id in ids_before, "drawing an edge moved the node's id"


def test_after_the_run_the_stated_wiring_is_what_the_node_recorded(
    wide, side_var, bp_node_id
):
    """Parity, both directions, on one node.

    Forward: the node's stated wiring. Backward: the wirings its runs are
    recorded under (``_node_wiring``). The first must be among the second, or
    the canvas is describing a node the run did not produce.
    """
    db = _gui_db.get_db()
    wide.put(
        "/api/edges/manual__side",
        json={
            "source": "var__SideTable",
            "target": bp_node_id,
            "source_handle": None,
            "target_handle": "in__side",
        },
    )
    stated_before_run = _stated_wiring(db, bp_node_id, {"signal": "RawSignal"})

    for_each(
        _registry._functions["bandpass_filter"],
        inputs={"signal": RawSignal, "low_hz": 20, "side": side_var},
        outputs=[FilteredSignal],
        subject=[1],
        session=["pre"],
    )
    wide.get("/api/pipeline")

    assert stated_before_run in _recorded_wirings(db), (
        "the run recorded a different shape from the one the node stated"
    )
    assert stated_before_run in node_wiring.wirings_for_node(db, bp_node_id), (
        "the node does not claim the wiring its own run recorded — the next "
        "build will attribute that wiring to a node of its own"
    )


def test_the_id_never_encodes_a_wiring_the_node_has_not_run(
    wide, side_var, bp_node_id
):
    """Every id-keyed row survives, which is the whole point: the association
    table is allowed to grow, the id is not allowed to move."""
    db = _gui_db.get_db()
    wide.put(
        "/api/edges/manual__side",
        json={
            "source": "var__SideTable",
            "target": bp_node_id,
            "source_handle": None,
            "target_handle": "in__side",
        },
    )
    for_each(
        _registry._functions["bandpass_filter"],
        inputs={"signal": RawSignal, "low_hz": 20, "side": side_var},
        outputs=[FilteredSignal],
        subject=[1],
        session=["pre"],
    )

    ids = {n["id"] for n in wide.get("/api/pipeline").json()["nodes"]}
    assert bp_node_id in ids
    recorded = node_wiring.wirings_for_node(db, bp_node_id)
    assert len(recorded) >= 2, (
        f"the node should hold both shapes it has run as, holds {recorded}"
    )
