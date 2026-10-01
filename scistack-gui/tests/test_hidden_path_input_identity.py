"""Deleting a PathInput node off the canvas must not change who a function
node IS (docs/claude/hidden-path-input-identity.md).

The 2026-09-28 session: ``pandas.read_csv`` had run as
``DemographicsPath -> read_csv -> Demographics`` on node ``rlw90w``. The user
deleted the ``DemographicsPath`` node (a hide), and the next build recomputed
the call site's wiring WITHOUT the PathInput term, found no node for it, and
minted ``cdb48…``. Every edge drawn afterwards landed on that phantom, whose
wiring matches no history, and every Run said "No pipeline history or output
connections found".

The rule these tests pin: hiding is a VIEW decision, and a node's identity is
computed from what history RECORDED. So ``filter_hidden`` runs once, after
identity and grouping, and never before (``TestFilterRunsAfterIdentity``).
"""

from __future__ import annotations

import numpy as np
import pytest

import scistack_gui.db as _gui_db
from scidb import BaseVariable, PathInput, for_each
from scistack_gui import node_wiring, registry as _registry


class LoadedTable(BaseVariable):
    pass


class LoadedTable2(BaseVariable):
    pass


def load_table(filepath):
    return np.asarray([len(str(filepath))], dtype=float)


def _pi(tmp_path, folder, name):
    return PathInput(
        "{subject}/{session}.csv", root_folder=str(tmp_path / folder), name=name
    )


@pytest.fixture
def pi_client(client, tmp_path):
    """A PathInput-fed function with real history: ``RAW -> load_table ->
    LoadedTable``, run over every (subject, session) the seeded db holds."""
    for folder in ("a", "b"):
        for subj in (1, 2):
            (tmp_path / folder / str(subj)).mkdir(parents=True, exist_ok=True)
            for sess in ("pre", "post"):
                (tmp_path / folder / str(subj) / f"{sess}.csv").write_text("x\n")

    raw = _pi(tmp_path, "a", "RAW")
    _registry._path_inputs["RAW"] = raw
    _registry._functions["load_table"] = load_table
    for_each(
        load_table,
        inputs={"filepath": raw},
        outputs=[LoadedTable],
        subject=[1, 2],
        session=["pre", "post"],
    )
    return client


def _graph(client):
    return client.get("/api/pipeline").json()


def _nodes(graph, label="load_table"):
    return [
        n
        for n in graph["nodes"]
        if n.get("type") == "functionNode" and n["data"]["label"] == label
    ]


def _path_input_node_id(graph, name):
    ids = [
        n["id"]
        for n in graph["nodes"]
        if n.get("type") == "pathInputNode" and n["data"].get("label") == name
    ]
    assert len(ids) == 1, f"expected one {name} PathInput node, got {ids}"
    return ids[0]


def _load_table_node_ids(db):
    return {
        n for n in node_wiring.known_node_ids(db) if n.startswith("fn__load_table__")
    }


def _hide_raw(client):
    graph = _graph(client)
    r = client.delete(f"/api/layout/{_path_input_node_id(graph, 'RAW')}")
    assert r.status_code == 200, r.text


class TestHidingAPathInputKeepsTheNode:
    def test_no_second_node_is_minted(self, pi_client):
        before = _nodes(_graph(pi_client))
        assert len(before) == 1
        node_id = before[0]["id"]
        db = _gui_db.get_db()
        wiring_before = node_wiring.current_wiring(db, node_id)

        _hide_raw(pi_client)
        _graph(pi_client)

        assert _load_table_node_ids(db) == {node_id}, (
            "hiding the PathInput minted a new node for the same call site: "
            f"{sorted(_load_table_node_ids(db))}"
        )
        assert node_wiring.current_wiring(db, node_id) == wiring_before, (
            "hiding the PathInput rewrote the node's recorded wiring"
        )

    def test_the_node_stays_on_the_canvas(self, pi_client):
        node_id = _nodes(_graph(pi_client))[0]["id"]

        _hide_raw(pi_client)

        assert [n["id"] for n in _nodes(_graph(pi_client))] == [node_id]


class TestRewiringAfterTheHideRuns:
    """The user's next steps: a new PathInput and a new output, drawn onto
    the (same) history node. The Run must use what is drawn."""

    @pytest.fixture
    def rewired(self, pi_client, tmp_path):
        node_id = _nodes(_graph(pi_client))[0]["id"]
        _hide_raw(pi_client)
        _registry._path_inputs["RAW2"] = _pi(tmp_path, "b", "RAW2")
        r = pi_client.put(
            "/api/edges/manual__raw2",
            json={
                "source": "pathInput__RAW2",
                "target": node_id,
                "source_handle": None,
                "target_handle": "in__filepath",
            },
        )
        assert r.status_code == 200, r.text
        return pi_client, node_id

    def test_the_drawn_path_input_is_the_binding(self, rewired):
        from scistack_gui.domain.edge_resolver import (
            BINDING_PATHINPUT,
            bindings_of_kind,
        )
        from scistack_gui.services.execution_service import derive_target_for_node

        client, node_id = rewired
        _graph(client)

        targets = derive_target_for_node(_gui_db.get_db(), node_id)

        assert targets, "the rewired history node has nothing to run"
        bound = [bindings_of_kind(t.get("bindings"), BINDING_PATHINPUT) for t in targets]
        assert all(b == {"filepath": "RAW2"} for b in bound), (
            f"the Run ignored the drawn PathInput edge: {bound}"
        )

    def test_the_drawn_output_is_the_output(self, rewired):
        from scistack_gui.services.execution_service import derive_target_for_node

        client, node_id = rewired
        graph = _graph(client)
        old_out = next(
            n["id"]
            for n in graph["nodes"]
            if n.get("type") == "variableNode" and n["data"].get("label") == "LoadedTable"
        )
        assert client.delete(f"/api/layout/{old_out}").status_code == 200
        r = client.put(
            "/api/edges/manual__out2",
            json={
                "source": node_id,
                "target": "var__LoadedTable2",
                "source_handle": None,
                "target_handle": None,
            },
        )
        assert r.status_code == 200, r.text
        _graph(client)

        targets = derive_target_for_node(_gui_db.get_db(), node_id)

        assert targets, "the rewired history node has nothing to run"
        assert {t["output_type"] for t in targets} == {"LoadedTable2"}, (
            f"the Run ignored the drawn output edge: {[t['output_type'] for t in targets]}"
        )


# ---------------------------------------------------------------------------
# The general rule: hiding ANY node leaves every function node's identity alone
# ---------------------------------------------------------------------------

# (node type on the canvas, label). The graph holds two histories:
#   RAW -> load_table -> LoadedTable
#   RawSignal + low_hz -> bandpass_filter -> FilteredSignal   (conftest)
HIDE_CASES = [
    ("pathInputNode", "RAW"),
    ("parameterNode", "low_hz"),
    ("variableNode", "RawSignal"),  # an input
    ("variableNode", "FilteredSignal"),  # an output
    ("variableNode", "LoadedTable"),  # an output fed by a PathInput call site
    ("functionNode", "load_table"),
    ("functionNode", "bandpass_filter"),
]


def _node_id(graph, node_type, label):
    ids = [
        n["id"]
        for n in graph["nodes"]
        if n.get("type") == node_type and n["data"].get("label") == label
    ]
    assert len(ids) == 1, f"expected one {node_type} {label!r}, got {ids}"
    return ids[0]


def _fn_identity_snapshot(db):
    """``{node_id: current wiring}`` for every function node ever recorded."""
    return {n: node_wiring.current_wiring(db, n) for n in node_wiring.known_node_ids(db)}


class TestHidingAnyNodeKeepsEveryOtherNodesIdentity:
    """Hiding is a VIEW decision. It may take the hidden node off the canvas;
    it must never mint, re-key or rewire any OTHER node of any type, nor any
    function node's recorded wiring — including the
    hidden one, whose id must still be there when it is unhidden."""

    @pytest.mark.parametrize(("node_type", "label"), HIDE_CASES)
    def test_hide_mints_and_rewires_nothing(self, pi_client, node_type, label):
        graph = _graph(pi_client)
        db = _gui_db.get_db()
        before = _fn_identity_snapshot(db)
        assert {"fn__load_table__", "fn__bandpass_filter__"} <= {
            n.rsplit("__", 1)[0] + "__" for n in before
        }, f"fixture did not record both function nodes: {sorted(before)}"

        target = _node_id(graph, node_type, label)
        r = pi_client.delete(f"/api/layout/{target}")
        assert r.status_code == 200, r.text
        _graph(pi_client)

        after = _fn_identity_snapshot(db)
        assert set(after) == set(before), (
            f"hiding {node_type} {label!r} minted or dropped function node ids: "
            f"new={sorted(set(after) - set(before))} "
            f"gone={sorted(set(before) - set(after))}"
        )
        changed = {n: (before[n], after[n]) for n in before if before[n] != after[n]}
        assert not changed, (
            f"hiding {node_type} {label!r} rewrote current wiring(s): {changed}"
        )

    @pytest.mark.parametrize(("node_type", "label"), HIDE_CASES)
    def test_every_other_node_keeps_its_canvas_id(self, pi_client, node_type, label):
        """Identity of EVERY node type — Function, Variable, Parameter,
        PathInput — not just functions. No node may come back under a new id,
        and no id may appear that was not there before."""
        graph = _graph(pi_client)
        target = _node_id(graph, node_type, label)
        before = _canvas_ids(graph, exclude=target)
        assert pi_client.delete(f"/api/layout/{target}").status_code == 200

        after = _canvas_ids(_graph(pi_client))

        rekeyed = {
            k: (before[k], after[k]) for k in before.keys() & after.keys()
            if before[k] != after[k]
        }
        minted = sorted(
            f"{k[0]} {k[1]!r} -> {after[k]}" for k in after.keys() - before.keys()
        )
        assert not rekeyed and not minted, (
            f"hiding {node_type} {label!r} changed other nodes' identity: "
            f"re-keyed={rekeyed} new={minted}"
        )

    @pytest.mark.parametrize(("node_type", "label"), HIDE_CASES)
    def test_every_other_node_stays_on_the_canvas(self, pi_client, node_type, label):
        """Hiding one node takes THAT node off the canvas and nothing else.
        (Separate from the identity test above: a failure here with that one
        passing is a display cascade, not an identity leak.)"""
        graph = _graph(pi_client)
        target = _node_id(graph, node_type, label)
        before = _canvas_ids(graph, exclude=target)
        assert pi_client.delete(f"/api/layout/{target}").status_code == 200

        after = _canvas_ids(_graph(pi_client))

        gone = sorted(f"{k[0]} {k[1]!r}" for k in before.keys() - after.keys())
        assert not gone, (
            f"hiding {node_type} {label!r} also removed other nodes: {gone}"
        )


def _canvas_ids(graph, exclude=None):
    """``{(node type, label): id}`` for every node on the canvas. Labels are
    unique per type in this fixture; a collision would hide a duplicate, so it
    fails loudly instead."""
    out: dict[tuple[str, str], str] = {}
    for n in graph["nodes"]:
        if n["id"] == exclude:
            continue
        key = (n.get("type"), n["data"].get("label"))
        assert key not in out, f"two canvas nodes share {key}: {out[key]}, {n['id']}"
        out[key] = n["id"]
    return out


# ---------------------------------------------------------------------------
# Structural guard: the one hidden-node filter runs after identity
# ---------------------------------------------------------------------------


class TestFilterRunsAfterIdentity:
    """Every step before grouping hashes ``wiring_id``; a filter placed before
    them leaks a hide into identity, one node kind at a time (hidden outputs,
    then hidden PathInputs). Pinned by source order so a re-introduced
    pre-identity pass fails here, whatever kind of node it would leak."""

    def _calls_in_build_graph(self):
        import ast
        import inspect
        import textwrap

        from scistack_gui.api import pipeline

        tree = ast.parse(textwrap.dedent(inspect.getsource(pipeline._build_graph)))
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                f = node.func
                name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
                calls.append((node.lineno, name))
        return sorted(calls)

    def test_filter_hidden_is_called_once(self):
        calls = [n for _, n in self._calls_in_build_graph() if n == "filter_hidden"]
        assert len(calls) == 1, f"_build_graph calls filter_hidden {len(calls)} times"

    def test_filter_hidden_follows_identity_and_grouping(self):
        lines = {}
        for lineno, name in self._calls_in_build_graph():
            lines.setdefault(name, []).append(lineno)
        filt = min(lines["filter_hidden"])
        for step in ("_resolve_node_identity", "group_call_sites_by_wiring"):
            assert step in lines, f"_build_graph no longer calls {step}"
            assert filt > max(lines[step]), (
                f"filter_hidden (line {filt}) runs before {step} "
                f"(line {max(lines[step])}): a hide can reach wiring_id"
            )


# ---------------------------------------------------------------------------
# Stage 2: a drawn PathInput edge on a history node — the owner, pure
# ---------------------------------------------------------------------------

_NODE = "fn__f__0123456789abcdef"
_TOKEN = "0123456789abcdef"


def _edge(source, target=_NODE, handle="in__filepath"):
    return {"id": f"m_{source}_{handle}", "source": source, "target": target,
            "targetHandle": handle}


def _pi_overrides(edges, manual_nodes=None):
    from scistack_gui.domain.graph_builder import (
        manual_edge_handle_index,
        manual_path_input_overrides,
    )

    return manual_path_input_overrides(
        "f", _TOKEN, manual_edge_handle_index(edges, hidden_edge_ids=frozenset()), manual_nodes or {}
    )


class TestManualPathInputOverrides:
    def test_a_drawn_path_input_is_the_override(self):
        assert _pi_overrides([_edge("pathInput__RAW2::main")]) == {"filepath": "RAW2"}

    def test_variable_and_parameter_sources_are_not_path_inputs(self):
        edges = [_edge("var__Raw"), _edge("param__hz", handle="in__hz")]
        assert _pi_overrides(edges) == {}

    def test_another_nodes_edges_do_not_apply(self):
        edges = [_edge("pathInput__RAW2", target="fn__f__fedcba9876543210")]
        assert _pi_overrides(edges) == {}

    def test_two_drawn_on_one_handle_the_last_wins_and_warns(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger="scistack_gui"):
            out = _pi_overrides([_edge("pathInput__A"), _edge("pathInput__B")])
        assert out == {"filepath": "B"}
        assert "2 PathInputs drawn onto 'filepath'" in caplog.text


# ---------------------------------------------------------------------------
# Stage 2: a script run through the drawn PathInput keeps ONE node
# ---------------------------------------------------------------------------


class TestAScriptRunThroughTheDrawnPathInput:
    """No node id in a script run, so attribution is inferred from what the
    node STATES — and a drawn PathInput is part of what it states. Without
    that, the run records a wiring nothing claims and it forks a node."""

    def test_keeps_one_node_and_advances_its_shape(self, pi_client, tmp_path):
        node_id = _nodes(_graph(pi_client))[0]["id"]
        db = _gui_db.get_db()
        before = node_wiring.current_wiring(db, node_id)
        _hide_raw(pi_client)
        raw2 = _pi(tmp_path, "b", "RAW2")
        _registry._path_inputs["RAW2"] = raw2
        r = pi_client.put(
            "/api/edges/manual__raw2",
            json={
                "source": "pathInput__RAW2",
                "target": node_id,
                "source_handle": None,
                "target_handle": "in__filepath",
            },
        )
        assert r.status_code == 200, r.text
        _graph(pi_client)

        for_each(
            load_table,
            inputs={"filepath": raw2},
            outputs=[LoadedTable],
            subject=[1, 2],
            session=["pre", "post"],
        )

        nodes = _nodes(_graph(pi_client))
        assert [n["id"] for n in nodes] == [node_id], (
            f"the run through the drawn PathInput forked a node: {[n['id'] for n in nodes]}"
        )
        assert node_wiring.current_wiring(db, node_id) != before, (
            "the node did not take the shape it just ran as"
        )


# ---------------------------------------------------------------------------
# Logging: a stuck node names the edges it did not consult
# ---------------------------------------------------------------------------


class TestStuckNodeLogNamesItsDrawnEdges:
    """The 2026-09-28 log said "no manual edges to infer from" about a node
    with three drawn edges, which pointed the diagnosis the wrong way."""

    def test_the_log_lists_the_drawn_edges(self, pi_client, caplog):
        import logging

        from scistack_gui.services.execution_service import derive_target_for_node

        db = _gui_db.get_db()
        stuck = str(node_wiring.mint_node_id("load_table"))
        node_wiring.record(db, stuck, "f" * 16)  # a wiring no history holds
        r = pi_client.put(
            "/api/edges/manual__stuck",
            json={
                "source": stuck,
                "target": "var__LoadedTable2",
                "source_handle": None,
                "target_handle": None,
            },
        )
        assert r.status_code == 200, r.text

        with caplog.at_level(logging.INFO, logger="scistack_gui"):
            assert derive_target_for_node(db, stuck) == []

        assert "no manual edges" not in caplog.text
        assert "1 drawn" in caplog.text
        assert "var__LoadedTable2" in caplog.text


class TestMintedBesideOrphansWarns:
    """The signature of an identity leak, whatever kind of node was hidden:
    a function mints a node in the same pass one of its existing nodes stops
    matching history."""

    OLD = "fn__f__" + "a" * 16

    def _resolve(self, history, caplog):
        import logging

        from scistack_gui.domain.node_identity import resolve_identities

        with caplog.at_level(logging.WARNING, logger="scistack_gui"):
            return resolve_identities(
                history,
                associations=[{"node_id": self.OLD, "wiring_id": "w1", "first_seen": "1"}],
                current_by_node={self.OLD: "w1"},
                mint=lambda fn, taken: "fn__f__" + "b" * 16,
            )

    def test_warns_when_the_old_node_drops_out(self, caplog):
        plan = self._resolve({("f", "w2")}, caplog)
        assert plan.minted
        assert "match nothing in history" in caplog.text
        assert self.OLD in caplog.text

    def test_silent_for_a_genuinely_new_call_site(self, caplog):
        plan = self._resolve({("f", "w1"), ("f", "w2")}, caplog)
        assert plan.minted
        assert "match nothing in history" not in caplog.text


# ---------------------------------------------------------------------------
# A hand-dragged function node that RAN keeps working (scidb.log 2026-09-29)
# ---------------------------------------------------------------------------

_SHORT = "fn__f__rlw90w"  # a hand-dragged node's id: 6 chars, outside the grammar


class TestRunManualNodeIsReKeyedPure:
    """Its dispatch record made the short id the claimant of history, and
    ``ids.parse_fn_node_id`` returns None for it — so its drawn edges were
    never indexed and Run derived nothing, silently."""

    def _resolve(self, history, current=None):
        from scistack_gui.domain.node_identity import resolve_identities

        minted = iter(["fn__f__" + c * 16 for c in "abcdef"])
        return resolve_identities(
            history,
            associations=[
                {"node_id": _SHORT, "wiring_id": w, "first_seen": "1"} for _f, w in sorted(history)
            ],
            current_by_node=current or {},
            mint=lambda fn, taken: next(minted),
        )

    def test_claimed_history_gets_an_id_in_the_grammar(self):
        from scistack_gui.ids import parse_fn_node_id

        plan = self._resolve({("f", "w1")}, {_SHORT: "w1"})

        new = plan.rekeys[_SHORT]
        assert parse_fn_node_id(new) is not None
        assert plan.node_by_wiring[("f", "w1")] == new
        assert (new, "w1", "main") in plan.to_record
        assert plan.is_current("f", "w1")
        assert not plan.minted, "a re-key is not a new node"

    def test_every_wiring_of_the_node_moves_to_ONE_new_id(self):
        plan = self._resolve({("f", "w1"), ("f", "w2")}, {_SHORT: "w2"})

        assert len(plan.rekeys) == 1
        new = plan.rekeys[_SHORT]
        assert plan.node_by_wiring[("f", "w1")] == plan.node_by_wiring[("f", "w2")] == new
        assert plan.is_current("f", "w2") and not plan.is_current("f", "w1"), (
            "the re-keyed node must keep the current shape it had"
        )

    def test_an_id_already_in_the_grammar_is_left_alone(self):
        from scistack_gui.domain.node_identity import resolve_identities

        good = "fn__f__" + "0" * 16
        plan = resolve_identities(
            {("f", "w1")},
            associations=[{"node_id": good, "wiring_id": "w1", "first_seen": "1"}],
        )
        assert plan.rekeys == {}
        assert plan.node_by_wiring[("f", "w1")] == good


class TestRunManualNodeEndToEnd:
    """The 2026-09-28/29 sequence: drag `load_table` in by hand, run it (the
    dispatch record claims history under its short id), then draw a new output
    edge and press Run."""

    MANUAL = "fn__load_table__rlw90w"

    @pytest.fixture
    def ran_manual(self, pi_client):
        from scistack_gui import layout as layout_store
        from scistack_gui import pipeline_store as ps
        from scistack_gui.api.pipeline import build_aggregate
        from scistack_gui.domain import graph_builder as gb

        db = _gui_db.get_db()
        # The wiring history recorded for the run — computed by the one
        # aggregate the build uses, not re-derived here.
        agg = build_aggregate(db, db.get_aggregated_variants())
        fkey = next(k for k in agg.fn_input_params if k[0] == "load_table")
        recorded = gb.wiring_id(
            "load_table",
            agg.fn_input_params[fkey],
            agg.fn_outputs[fkey],
            gb.path_input_bindings_by_fkey(agg.path_inputs).get(fkey, {}),
        )
        node_wiring.ensure_tables(db)
        ps.write_manual_node(db, self.MANUAL, "functionNode", "load_table", "main")
        node_wiring.record(db, self.MANUAL, recorded, run_id="r1")  # the dispatch record
        layout_store.write_node_position(self.MANUAL, 12.0, 34.0, "main")
        return pi_client, db

    def test_the_node_is_re_keyed_with_its_position(self, ran_manual):
        from scistack_gui import layout as layout_store
        from scistack_gui.ids import parse_fn_node_id

        client, db = ran_manual
        nodes = _nodes(_graph(client))

        assert len(nodes) == 1, [n["id"] for n in nodes]
        new = nodes[0]["id"]
        assert parse_fn_node_id(new) is not None, f"still outside the grammar: {new}"
        assert self.MANUAL not in node_wiring.known_node_ids(db)
        positions = layout_store.read_positions_by_scope()["main"]
        assert positions.get(new) == {"x": 12.0, "y": 34.0}
        assert self.MANUAL not in positions

    def test_a_drawn_output_edge_follows_and_runs(self, ran_manual):
        from scistack_gui.services.execution_service import derive_target_for_node

        client, db = ran_manual
        r = client.put(
            "/api/edges/manual__out_short",
            json={
                "source": self.MANUAL,
                "target": "var__LoadedTable2",
                "source_handle": None,
                "target_handle": None,
            },
        )
        assert r.status_code == 200, r.text

        new = _nodes(_graph(client))[0]["id"]
        edge = next(
            e for e in pipeline_store_edges(db) if e["id"] == "manual__out_short"
        )
        assert edge["source"] == new, "the drawn edge was left on the old id"

        targets = derive_target_for_node(db, new)
        assert targets, "the re-keyed node has nothing to run"
        assert {t["output_type"] for t in targets} == {"LoadedTable2"}


def pipeline_store_edges(db):
    from scistack_gui import pipeline_store as ps

    return ps.get_manual_edges(db)
