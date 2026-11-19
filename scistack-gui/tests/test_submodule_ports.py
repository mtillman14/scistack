"""Submodule ports: one connection, stored in two segments, read as one.

Plan ``.claude/plan-submodule-ports.md``; concept ``docs/claude/submodule-ports.md``.
The worked example is ``pathinputA -> fcnB -> varC -> fcnD -> varE``: every split
gets the ports the framework says, and every reader of wiring sees the original
connections whatever the split.
"""

from __future__ import annotations

import pytest

from scistack_gui import ids
from scistack_gui.domain import ports
from scistack_gui.domain.adoption import plan_adoption
from scistack_gui.domain.edge_view import effective_edges
from scistack_gui.domain.graph_builder import edge_connection_id
from scistack_gui.runtime.db import get_db
from scistack_gui.stores import intent_store
from scistack_gui.stores import layout as layout_store
from scistack_gui.stores import pipeline_store as ps

A, B, C, D, E = "pathInput__A", "mf_B", "mv_C", "mf_D", "mv_E"
BY_LETTER = {"A": A, "B": B, "C": C, "D": D, "E": E}

#: selection, input ports, output ports: the framework's table. A (nothing
#: feeds it) and E (it feeds nothing) stay on the parent wherever they are
#: selected (GitHub #22), so "A" or "E" alone is refused (TestSourcesAndSinks).
SPLITS = [
    ("AB", ["A"], ["VarC"]),
    ("ABC", ["A"], ["VarC"]),
    ("ABCD", ["A"], ["VarE"]),
    ("ABCDE", ["A"], ["VarE"]),
    ("B", ["A"], ["VarC"]),
    ("BC", ["A"], ["VarC"]),
    ("BCD", ["A"], ["VarE"]),
    ("BCDE", ["A"], ["VarE"]),
    ("C", ["VarC"], ["VarC"]),
    ("CD", ["VarC"], ["VarE"]),
    ("CDE", ["VarC"], ["VarE"]),
    ("D", ["VarC"], ["VarE"]),
    ("DE", ["VarC"], ["VarE"]),
]


def _place(client, node_id, node_type, label, pipeline_id="main"):
    return client.put(
        f"/api/layout/{node_id}",
        json={
            "x": 0,
            "y": 0,
            "node_type": node_type,
            "label": label,
            "pipeline_id": pipeline_id,
        },
    )


@pytest.fixture
def chain(client):
    """pathinputA -> fcnB -> varC -> fcnD -> varE on main."""
    _place(client, A, "pathInputNode", "A")
    _place(client, B, "functionNode", "fcn_b")
    _place(client, C, "variableNode", "VarC")
    _place(client, D, "functionNode", "fcn_d")
    _place(client, E, "variableNode", "VarE")
    client.put("/api/edges/e_ab", json={"source": A, "target": B, "target_handle": "in__path"})
    client.put("/api/edges/e_bc", json={"source": B, "target": C})
    client.put("/api/edges/e_cd", json={"source": C, "target": D, "target_handle": "in__signal"})
    client.put("/api/edges/e_de", json={"source": D, "target": E})
    return client


def _connections(scope=None) -> set[str]:
    view = effective_edges(get_db(), scope, caller="test")
    return {edge_connection_id(e, view.manual_nodes) for e in view.wiring_list}


def _extract(client, letters: str, name: str = "sub"):
    return client.post(
        "/api/pipelines/main/extract",
        json={"node_ids": [BY_LETTER[x] for x in letters], "name": name},
    )


class TestEverySplit:
    @pytest.mark.parametrize("letters,inputs,outputs", SPLITS)
    def test_ports_match_the_framework(self, chain, letters, inputs, outputs):
        r = _extract(chain, letters)
        assert r.status_code == 200, r.text
        pid = r.json()["pipeline_id"]
        assert chain.get(f"/api/pipelines/{pid}/interface").json() == {
            "inputs": inputs,
            "outputs": outputs,
        }

    @pytest.mark.parametrize("letters,_inputs,_outputs", SPLITS)
    def test_resolved_connections_equal_the_original_dag(
        self, chain, letters, _inputs, _outputs
    ):
        """The core promise: a port never changes a connection's identity."""
        before = _connections()
        assert len(before) >= 4
        assert _extract(chain, letters).status_code == 200
        assert _connections() == before

    @pytest.mark.parametrize("letters,_inputs,_outputs", SPLITS)
    def test_every_outer_line_lands_on_a_real_port(
        self, chain, letters, _inputs, _outputs
    ):
        """React Flow silently drops an edge whose port doesn't exist."""
        use_id = _extract(chain, letters).json()["use_id"]
        graph = chain.get("/api/pipeline").json()
        node = next(n for n in graph["nodes"] if n["id"] == use_id)
        ids_here = {n["id"] for n in graph["nodes"]}
        for edge in graph["edges"]:
            assert edge["source"] in ids_here and edge["target"] in ids_here
            if edge["target"] == use_id:
                assert edge["targetHandle"] in [f"in__{p}" for p in node["data"]["inputs"]]
            if edge["source"] == use_id:
                assert edge["sourceHandle"] in [f"out__{p}" for p in node["data"]["outputs"]]

    @pytest.mark.parametrize("letters,_inputs,_outputs", SPLITS)
    def test_stored_edges_never_span_two_canvases(
        self, chain, letters, _inputs, _outputs
    ):
        """pipeline_store's rule: both ends of an edge are on one canvas."""
        _extract(chain, letters)
        db = get_db()
        nodes = ps.get_manual_nodes(db)
        for edge in ps.get_manual_edges(db):
            source, target = nodes.get(edge["source"]), nodes.get(edge["target"])
            if source and target:
                assert source["pipeline_id"] == target["pipeline_id"], edge

    def test_the_whole_chain_keeps_its_source_and_sink_outside(self, chain):
        """GitHub #22: selecting everything moves the steps, not A and E."""
        before = _connections()
        r = _extract(chain, "ABCDE")
        assert r.status_code == 200, r.text
        db = get_db()
        assert ps.manual_node_row(db, A)[1] == "main"
        assert ps.manual_node_row(db, E)[1] == "main"
        for inside in (B, C, D):
            assert ps.manual_node_row(db, inside)[1] == r.json()["pipeline_id"]
        assert _connections() == before


class TestSourcesAndSinks:
    """GitHub #22 (decision 2026-11-18: data nodes only; steps always move)."""

    @pytest.mark.parametrize("letters", ["A", "E", "AE"])
    def test_a_selection_of_only_sources_and_sinks_is_refused(self, chain, letters):
        before = sorted(e["id"] for e in ps.get_manual_edges(get_db()))
        r = _extract(chain, letters)
        assert r.status_code == 400
        assert "Select the steps between them too" in r.text
        assert sorted(e["id"] for e in ps.get_manual_edges(get_db())) == before

    def test_split_selection_rule(self):
        manual = {
            "pi": {"type": "pathInputNode", "label": "p"},
            "f": {"type": "functionNode", "label": "f"},
            "v": {"type": "variableNode", "label": "V"},
            "w": {"type": "variableNode", "label": "W"},
            "end": {"type": "variableNode", "label": "End"},
            "var__glue_g__1": {"type": "glueNode", "label": "g"},
        }
        edges = [
            _edge("1", "pi", "var__glue_g__1", target_handle="in__value"),
            _edge("2", "var__glue_g__1", "f", target_handle="in__x"),
            _edge("3", "f", "v"),
            _edge("4", "v", "f2", target_handle="in__v"),
            _edge("5", "f", "end"),
        ]
        selected = {"pi", "var__glue_g__1", "f", "v", "w", "end"}
        split = ports.split_selection(
            selected, edges, manual, lambda end: end if end in selected else None
        )
        # w is unconnected: a source (nothing feeds it). A glue node is a step.
        assert split.sources == ["pi", "w"]
        assert split.sinks == ["end"]
        assert split.moved == {"var__glue_g__1", "f", "v"}

    def test_the_load_filter_emg_shape(self, client, glue_project):
        """The user's 2026-11-18 extract: two Parameters, a PathInput through a
        glue node, three steps, a final variable. About 25 actions followed to
        turn the data nodes back into ports by hand."""
        glue = client.post("/api/glue", json={"name": "cfg"}).json()["name"]
        glue_id = "var__glue_cfg__ab12cd34"  # the GUI's glue spelling (var__ prefix)
        for node_id, kind, label in [
            ("param__low", "parameterNode", "low"),
            ("param__high", "parameterNode", "high"),
            ("pathInput__runConfig", "pathInputNode", "runConfig"),
            (glue_id, "glueNode", glue),
            ("mf_load", "functionNode", "load"),
            ("mv_raw", "variableNode", "RawEMG"),
            ("mf_filter", "functionNode", "filterDelsys"),
            ("mv_filtered", "variableNode", "FilteredEMG"),
            ("mf_rename", "functionNode", "renameFields"),
            ("mv_renamed", "variableNode", "RenamedEMG"),
        ]:
            _place(client, node_id, kind, label)
        for edge_id, source, target, handle in [
            ("g1", "pathInput__runConfig", glue_id, "in__value"),
            ("g2", glue_id, "mf_load", "in__config"),
            ("l1", "mf_load", "mv_raw", None),
            ("f1", "mv_raw", "mf_filter", "in__signal"),
            ("f2", "param__low", "mf_filter", "in__low"),
            ("f3", "param__high", "mf_filter", "in__high"),
            ("f4", "mf_filter", "mv_filtered", None),
            ("r1", "mv_filtered", "mf_rename", "in__data"),
            ("r2", "mf_rename", "mv_renamed", None),
        ]:
            body = {"source": source, "target": target}
            if handle:
                body["target_handle"] = handle
            assert client.put(f"/api/edges/{edge_id}", json=body).status_code == 200
        before = _connections()
        selected = [
            "param__low", "param__high", "pathInput__runConfig", glue_id, "mf_load",
            "mv_raw", "mf_filter", "mv_filtered", "mf_rename", "mv_renamed",
        ]

        r = client.post(
            "/api/pipelines/main/extract", json={"node_ids": selected, "name": "load_filter_emg"}
        )

        assert r.status_code == 200, r.text
        pid = r.json()["pipeline_id"]
        interface = client.get(f"/api/pipelines/{pid}/interface").json()
        # The PathInput's port is named after it, not after the glue node it feeds.
        assert sorted(interface["inputs"]) == ["high", "low", "runConfig"]
        assert interface["outputs"] == ["RenamedEMG"]
        db = get_db()
        for kept in ("param__low", "param__high", "pathInput__runConfig", "mv_renamed"):
            assert ps.manual_node_row(db, kept)[1] == "main", kept
        for moved in (glue_id, "mf_load", "mv_raw", "mf_filter", "mv_filtered", "mf_rename"):
            assert ps.manual_node_row(db, moved)[1] == pid, moved
        assert _connections() == before


class TestRefusedSelection:
    def test_a_path_leaving_and_reentering_is_refused(self, chain):
        before = sorted(e["id"] for e in ps.get_manual_edges(get_db()))
        r = chain.post(
            "/api/pipelines/main/extract",
            json={"node_ids": [B, D], "name": "sub"},
        )
        assert r.status_code == 400
        assert "comes back into it" in r.text
        assert sorted(e["id"] for e in ps.get_manual_edges(get_db())) == before


class TestRemovingALine:
    def test_removing_an_outer_edge_disconnects_the_connection(self, chain):
        """A hidden segment hides what it carried: the line on the canvas and
        what runs can't disagree."""
        use_id = _extract(chain, "AB").json()["use_id"]
        outer = next(e for e in ps.get_manual_edges(get_db()) if e["source"] == use_id)
        before = _connections()

        assert chain.delete(f"/api/edges/{outer['id']}").status_code == 200

        view = effective_edges(get_db(), "main", caller="test")
        gone = before - {edge_connection_id(e, view.manual_nodes) for e in view.wiring_list}
        assert len(gone) == 1
        assert gone <= view.hidden_edge_ids

    def test_redrawing_it_reconnects(self, chain):
        use_id = _extract(chain, "AB").json()["use_id"]
        outer = next(e for e in ps.get_manual_edges(get_db()) if e["source"] == use_id)
        before = _connections()
        chain.delete(f"/api/edges/{outer['id']}")
        chain.put(
            f"/api/edges/{outer['id']}",
            json={
                "source": outer["source"],
                "target": outer["target"],
                "source_handle": outer.get("sourceHandle"),
            },
        )
        assert _connections() == before


class TestRemovedLinesMoveWithExtract:
    """scistack-debug.log 2026-11-18 11:34: main had 7 hidden edges, the new
    submodule 0, so recorded edges the user had removed came back inside,
    beside the drawn ones, and filterDelsys/renameFields stalled as a "cycle"."""

    @staticmethod
    def _hidden(scope) -> set[str]:
        return ps.get_hidden_edge_ids(get_db(), scope)

    @staticmethod
    def _cd() -> str:
        view = effective_edges(get_db(), None, caller="test")
        return edge_connection_id(
            {"source": C, "target": D, "targetHandle": "in__signal"}, view.manual_nodes
        )

    @staticmethod
    def _remove_cd_and_redraw_beside(chain):
        """The log's shape: the old line removed, a new one drawn next to it.
        The new line keeps C fed into D, so C is not a sink left outside
        (GitHub #22)."""
        assert chain.delete("/api/edges/e_cd").status_code == 200
        r = chain.put(
            "/api/edges/e_cd2", json={"source": C, "target": D, "target_handle": "in__other"}
        )
        assert r.status_code == 200, r.text

    def test_a_line_removed_between_two_moved_nodes_stays_removed(self, chain):
        self._remove_cd_and_redraw_beside(chain)
        assert self._cd() in self._hidden("main")

        sub = _extract(chain, "CD").json()["pipeline_id"]

        assert self._cd() in self._hidden(sub)
        assert self._cd() not in self._hidden("main")
        inside = effective_edges(get_db(), sub, caller="test")
        assert "e_cd" not in {e["id"] for e in inside.drawn}

    def test_a_removed_crossing_line_stays_removed_on_both_canvases(self, chain):
        """One end stays behind. The stored row then spans both canvases, so
        its hide must apply on both (2026-11-19)."""
        assert chain.delete("/api/edges/e_bc").status_code == 200
        # Something else still feeds C, so C is not a source left outside.
        _place(chain, "mf_X", "functionNode", "fcn_x")
        assert chain.put("/api/edges/e_xc", json={"source": "mf_X", "target": C}).status_code == 200
        before = self._hidden("main")
        bc = next(iter(before))

        sub = _extract(chain, "CD").json()["pipeline_id"]

        assert self._hidden("main") == before
        assert bc in self._hidden(sub)

    def test_nothing_runs_through_the_removed_line(self, chain):
        """What the extracted steps are wired to: D no longer reads C."""
        self._remove_cd_and_redraw_beside(chain)
        sub = _extract(chain, "CD").json()["pipeline_id"]

        assert self._cd() not in _connections(sub)

    def test_a_removed_line_to_a_sink_left_outside_runs_nothing(self, chain):
        """C's only line removed: C is a sink and stays on main, so the removed
        C -> D crosses. Inside, D must not read C."""
        chain.delete("/api/edges/e_cd")
        r = _extract(chain, "CD")
        assert r.status_code == 200, r.text
        assert ps.manual_node_row(get_db(), C)[1] == "main"
        assert self._cd() not in _connections(r.json()["pipeline_id"])
        assert self._cd() not in _connections("main")


class TestManualPorts:
    def _submodule(self, client):
        return client.post("/api/pipelines", json={"name": "manual"}).json()["pipeline_id"]

    def test_refused_on_a_hypothesis(self, client):
        r = _place(client, "p", "inputPortNode", "x", "main")
        assert r.json()["ok"] is False
        assert r.json()["reason"] == "invalid_port"

    def test_duplicate_name_refused_per_direction(self, client):
        pid = self._submodule(client)
        assert _place(client, "p1", "inputPortNode", "x", pid).json()["ok"] is True
        assert _place(client, "p2", "inputPortNode", "x", pid).json()["ok"] is False
        assert _place(client, "p3", "outputPortNode", "x", pid).json()["ok"] is True

    def test_order_is_top_to_bottom_on_the_canvas(self, client):
        pid = self._submodule(client)
        for node_id, name, y in (("p_b", "b", 100), ("p_a", "a", 200), ("p_c", "c", 0)):
            client.put(f"/api/layout/{node_id}", json={
                "x": 0, "y": y, "node_type": "inputPortNode", "label": name, "pipeline_id": pid,
            })
        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface["inputs"] == ["c", "b", "a"]

        client.put("/api/layout/p_a", json={"x": 0, "y": -50, "pipeline_id": pid})
        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface["inputs"] == ["a", "c", "b"]

    def test_rename_rewrites_the_outer_edges_of_every_placement(self, client):
        pid = self._submodule(client)
        _place(client, "p_out", "outputPortNode", "old", pid)
        _place(client, "mf", "functionNode", "fcn", pid)
        _place(client, "mv", "variableNode", "Var")
        client.put("/api/edges/inner", json={"source": "mf", "target": "p_out"})
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": pid}
        ).json()["use_id"]
        client.put(
            "/api/edges/outer",
            json={"source": use_id, "target": "mv", "source_handle": "out__old"},
        )
        before = _connections()

        assert client.post(
            "/api/ports/rename", json={"node_id": "p_out", "name": "new"}
        ).status_code == 200

        outer = next(e for e in ps.get_manual_edges(get_db()) if e["id"] == "outer")
        assert outer["sourceHandle"] == "out__new"
        assert _connections() == before

    def test_a_manual_build_resolves_like_an_extraction(self, client):
        """Same rows, same reading: one representation of a submodule."""
        pid = self._submodule(client)
        _place(client, "mv_c", "variableNode", "VarC")
        _place(client, "mv_e", "variableNode", "VarE")
        _place(client, "mf_d", "functionNode", "fcn_d", pid)
        _place(client, "p_in", "inputPortNode", "VarC", pid)
        _place(client, "p_out", "outputPortNode", "VarE", pid)
        client.put(
            "/api/edges/i1",
            json={"source": "p_in", "target": "mf_d", "target_handle": "in__signal"},
        )
        client.put("/api/edges/i2", json={"source": "mf_d", "target": "p_out"})
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": pid}
        ).json()["use_id"]
        client.put(
            "/api/edges/o1",
            json={"source": "mv_c", "target": use_id, "target_handle": "in__VarC"},
        )
        client.put(
            "/api/edges/o2",
            json={"source": use_id, "target": "mv_e", "source_handle": "out__VarE"},
        )
        view = effective_edges(get_db(), None, caller="test")
        ends = {(e["source"], e["target"], e.get("targetHandle")) for e in view.wiring_list}
        assert ends == {("mv_c", "mf_d", "in__signal"), ("mf_d", "mv_e", None)}


def _node(node_type, label, pipeline_id):
    return {"type": node_type, "label": label, "pipeline_id": pipeline_id}


def _edge(edge_id, source, target, source_handle=None, target_handle=None):
    edge = {"id": edge_id, "source": source, "target": target}
    if source_handle:
        edge["sourceHandle"] = source_handle
    if target_handle:
        edge["targetHandle"] = target_handle
    return edge


class TestNesting:
    def test_ports_trace_through_two_levels(self):
        nodes = {
            "fn": _node("functionNode", "fcn", "inner"),
            "in_inner": _node(ports.INPUT_PORT_NODE, "x", "inner"),
            "in_outer": _node(ports.INPUT_PORT_NODE, "x", "outer"),
            "var__A": _node("variableNode", "A", "main"),
        }
        uses = [
            {"use_id": "u_inner", "parent_pipeline_id": "outer", "child_pipeline_id": "inner"},
            {"use_id": "u_outer", "parent_pipeline_id": "main", "child_pipeline_id": "outer"},
        ]
        wiring = ports.resolve_wiring(
            [
                _edge("o", "var__A", "u_outer", target_handle="in__x"),
                _edge("m", "in_outer", "u_inner", target_handle="in__x"),
                _edge("i", "in_inner", "fn", target_handle="in__a"),
            ],
            nodes,
            uses,
        )
        assert [(e["source"], e["target"], e["targetHandle"]) for e in wiring.edges] == [
            ("var__A", "fn", "in__a")
        ]


class TestOneWiring:
    """A submodule has one wiring; Duplicate makes an independent copy."""

    def _submodule_with_two_placements(self, client):
        pid = client.post("/api/pipelines", json={"name": "shared"}).json()["pipeline_id"]
        _place(client, "p_in", "inputPortNode", "x", pid)
        _place(client, "p_out", "outputPortNode", "y", pid)
        _place(client, "mf", "functionNode", "fcn", pid)
        client.put("/api/edges/i1", json={"source": "p_in", "target": "mf", "target_handle": "in__a"})
        client.put("/api/edges/i2", json={"source": "mf", "target": "p_out"})
        for node_id, label in (("mv_a", "VarA"), ("mv_b", "VarB"), ("mv_y", "VarY"), ("mv_z", "VarZ")):
            _place(client, node_id, "variableNode", label)
        uses = [
            client.post("/api/pipelines/main/uses", json={"child_pipeline_id": pid}).json()["use_id"]
            for _ in range(2)
        ]
        return pid, uses

    def _input(self, client, edge_id, source, use_id):
        return client.put(
            f"/api/edges/{edge_id}",
            json={"source": source, "target": use_id, "target_handle": "in__x"},
        )

    def _output(self, client, edge_id, use_id, target):
        return client.put(
            f"/api/edges/{edge_id}",
            json={"source": use_id, "target": target, "source_handle": "out__y"},
        )

    def test_a_different_input_on_another_placement_is_refused(self, client):
        _pid, (u1, u2) = self._submodule_with_two_placements(client)
        assert self._input(client, "o1", "mv_a", u1).status_code == 200
        r = self._input(client, "o2", "mv_b", u2)
        assert r.status_code == 400
        assert "Duplicate" in r.text
        assert "o2" not in {e["id"] for e in ps.get_manual_edges(get_db())}

    def test_a_different_output_on_another_placement_is_refused(self, client):
        _pid, (u1, u2) = self._submodule_with_two_placements(client)
        assert self._output(client, "o1", u1, "mv_y").status_code == 200
        r = self._output(client, "o2", u2, "mv_z")
        assert r.status_code == 400
        assert "Duplicate" in r.text

    def test_the_same_connection_on_another_placement_is_one_wire(self, client):
        _pid, (u1, u2) = self._submodule_with_two_placements(client)
        assert self._input(client, "o1", "mv_a", u1).status_code == 200
        assert self._input(client, "o2", "mv_a", u2).status_code == 200
        view = effective_edges(get_db(), None, caller="test")
        into_fn = [e for e in view.wiring_list if e["target"] == "mf"]
        assert [(e["source"], e["targetHandle"]) for e in into_fn] == [("mv_a", "in__a")]

    def test_an_unconnected_placement_conflicts_with_nothing(self, client):
        _pid, (u1, _u2) = self._submodule_with_two_placements(client)
        assert self._input(client, "o1", "mv_a", u1).status_code == 200
        assert effective_edges(get_db(), None, caller="test").open_arguments("mf") == []

    def test_a_line_removed_on_the_other_placement_no_longer_conflicts(self, client):
        _pid, (u1, u2) = self._submodule_with_two_placements(client)
        self._input(client, "o1", "mv_a", u1)
        client.delete("/api/edges/o1")
        assert self._input(client, "o2", "mv_b", u2).status_code == 200

    def test_duplicate_swaps_this_placement_to_its_own_copy(self, client):
        pid, (u1, u2) = self._submodule_with_two_placements(client)
        self._input(client, "o1", "mv_a", u1)
        self._input(client, "o2", "mv_a", u2)

        r = client.post(f"/api/pipeline-uses/{u2}/duplicate", json={"use_id": u2, "name": "copy"})
        assert r.status_code == 200, r.text
        copy = r.json()["pipeline_id"]

        uses = {u["use_id"]: u["child_pipeline_id"] for u in ps.get_pipeline_uses(get_db())}
        assert uses[u1] == pid and uses[u2] == copy
        assert client.get(f"/api/pipelines/{copy}/interface").json() == {
            "inputs": ["x"],
            "outputs": ["y"],
        }
        # Its connection stayed, and it now reaches the copy's own function.
        graph = client.get("/api/pipeline").json()
        assert any(e["id"] == "o2" for e in graph["edges"])
        view = effective_edges(get_db(), None, caller="test")
        fed = [e for e in view.wiring_list if e.get("targetHandle") == "in__a"]
        assert len(fed) == 2 and {e["source"] for e in fed} == {"mv_a"}

    def test_a_duplicated_placement_can_be_connected_differently(self, client):
        _pid, (u1, u2) = self._submodule_with_two_placements(client)
        self._input(client, "o1", "mv_a", u1)
        client.post(f"/api/pipeline-uses/{u2}/duplicate", json={"use_id": u2, "name": "copy"})
        assert self._input(client, "o2", "mv_b", u2).status_code == 200

        view = effective_edges(get_db(), None, caller="test")
        fed = {e["source"] for e in view.wiring_list if e.get("targetHandle") == "in__a"}
        assert fed == {"mv_a", "mv_b"}

    def test_duplicating_a_placement_that_does_not_exist_is_refused(self, client):
        r = client.post("/api/pipeline-uses/use_nope/duplicate", json={"use_id": "use_nope", "name": "c"})
        assert r.status_code == 400

    def test_a_duplicated_hypothesis_keeps_its_submodule_connected(self, chain):
        """It shares the submodule and holds its own node for the same variable."""
        _extract(chain, "BCD")
        before = _connections()
        r = chain.post("/api/hypotheses/main/duplicate", json={"name": "second"})
        assert r.status_code == 200, r.text
        chain.get("/api/pipeline", params={"pipeline_id": r.json()["pipeline_id"]})
        assert before <= _connections()
        assert effective_edges(get_db(), None, caller="test").open_arguments(B) == []


class TestOpenPorts:
    NODES = {
        "in": _node(ports.INPUT_PORT_NODE, "x", "sub"),
        "fn": _node("functionNode", "fcn", "sub"),
    }
    USES = [{"use_id": "u1", "parent_pipeline_id": "main", "child_pipeline_id": "sub"}]

    def test_an_open_input_is_reported(self):
        wiring = ports.resolve_wiring(
            [_edge("i", "in", "fn", target_handle="in__a")], self.NODES, self.USES
        )
        assert wiring.open_arguments("fn") == ["a"]
        assert wiring.edges == []

    def test_a_node_fed_by_an_open_port_has_no_targets(self, client):
        from scistack_gui.services.execution_service import derive_target_for_node

        pid = client.post("/api/pipelines", json={"name": "open"}).json()["pipeline_id"]
        _place(client, "p_in", "inputPortNode", "x", pid)
        _place(client, "mf", "functionNode", "bandpass_filter", pid)
        _place(client, "mv", "variableNode", "FilteredSignal", pid)
        client.put(
            "/api/edges/i",
            json={"source": "p_in", "target": "mf", "target_handle": "in__signal"},
        )
        client.put("/api/edges/o", json={"source": "mf", "target": "mv"})
        assert derive_target_for_node(get_db(), "mf") == []


class TestNothingReruns:
    """The promise that matters most: a submodule changes nothing about what
    ran or what counts as computed. Uses the seeded bandpass_filter run."""

    @pytest.mark.parametrize(
        "selection", [("fn",), ("fn", "out"), ("in", "fn"), ("in", "fn", "out")]
    )
    def test_extract_keeps_targets_and_skips_everything(self, client, selection):
        from scistack_gui.services.execution_service import (
            _target_key,
            derive_target_for_node,
            run_pipeline,
        )

        nodes = client.get("/api/pipeline").json()["nodes"]
        fn = next(n["id"] for n in nodes if n["type"] == "functionNode")
        by_label = {n["data"]["label"]: n["id"] for n in nodes if n["type"] == "variableNode"}
        chosen = {"fn": fn, "in": by_label["RawSignal"], "out": by_label["FilteredSignal"]}

        db = get_db()
        keys_before = sorted(_target_key(t) for t in derive_target_for_node(db, fn))
        assert keys_before

        r = client.post(
            "/api/pipelines/main/extract",
            json={"node_ids": [chosen[s] for s in selection], "name": "sub"},
        )
        assert r.status_code == 200, r.text
        pid = r.json()["pipeline_id"]

        inside = client.get("/api/pipeline", params={"pipeline_id": pid}).json()["nodes"]
        fn_inside = next(n["id"] for n in inside if n["type"] == "functionNode")
        keys_after = sorted(_target_key(t) for t in derive_target_for_node(db, fn_inside))
        assert keys_after == keys_before

        entries = {e["step"]: e for e in client.get("/api/pipelines/main/plan").json()}
        assert entries["bandpass_filter"]["state"] == "green"
        assert entries["bandpass_filter"]["pipeline"] == "sub"

        invocations = db._duck.fetchall("SELECT COUNT(*) FROM _invocation")[0][0]
        result = run_pipeline(db, "main", mode="until", target="bandpass_filter")
        assert result["ok"] is True
        assert db._duck.fetchall("SELECT COUNT(*) FROM _invocation")[0][0] == invocations


class TestParameterCrossing:
    def test_a_parameter_outside_becomes_an_input_port(self, client):
        _place(client, "param__gain", "parameterNode", "gain")
        _place(client, "mf_b", "functionNode", "fcn_b")
        _place(client, "mv_c", "variableNode", "VarC")
        client.put(
            "/api/edges/p",
            json={"source": "param__gain", "target": "mf_b", "target_handle": "in__gain"},
        )
        client.put("/api/edges/o", json={"source": "mf_b", "target": "mv_c"})
        before = _connections()

        r = client.post(
            "/api/pipelines/main/extract", json={"node_ids": ["mf_b"], "name": "sub"}
        )

        assert client.get(f"/api/pipelines/{r.json()['pipeline_id']}/interface").json() == {
            "inputs": ["gain"],
            "outputs": ["VarC"],
        }
        assert _connections() == before


class TestParamsOnPorts:
    def test_exact_and_bare_names(self):
        fed = {("fcn", "low_hz")}
        assert ports.params_on_ports({"low_hz": 1, "other": 2}, fed) == ["low_hz"]
        assert ports.params_on_ports({"fcn.low_hz": 1}, fed) == ["fcn.low_hz"]
        assert ports.params_on_ports({"other_fn.low_hz": 1}, fed) == []
        assert ports.params_on_ports(None, fed) == []

    def test_fed_arguments_reach_nested_submodules(self):
        nodes = {
            "fn": _node("functionNode", "fcn", "inner"),
            "in_inner": _node(ports.INPUT_PORT_NODE, "x", "inner"),
        }
        uses = [
            {"use_id": "u_inner", "parent_pipeline_id": "outer", "child_pipeline_id": "inner"},
        ]
        edges = [_edge("i", "in_inner", "fn", target_handle="in__a")]
        assert ports.port_fed_arguments("outer", edges, nodes, uses) == {("fcn", "a")}
        assert ports.port_fed_arguments("elsewhere", edges, nodes, uses) == set()

    def test_compiling_refuses_a_value_with_two_owners(self, client):
        from scistack_gui.services.execution_service import (
            PortBindingError,
            _discard_compiled,
            build_backend_pipeline,
        )

        pid = client.post("/api/pipelines", json={"name": "sub"}).json()["pipeline_id"]
        _place(client, "p_in", "inputPortNode", "low", pid)
        _place(client, "mf", "functionNode", "bandpass_filter", pid)
        client.put(
            "/api/edges/i",
            json={"source": "p_in", "target": "mf", "target_handle": "in__low_hz"},
        )
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": pid}
        ).json()["use_id"]
        db = get_db()
        ps.update_use_binding(db, use_id, {"params": {"low_hz": 5}})

        built: dict = {}
        with pytest.raises(PortBindingError, match="low_hz"):
            build_backend_pipeline(db, "main", built)
        _discard_compiled(built)


class TestRewiredNode:
    def test_a_named_script_run_never_hides_a_segment(self, chain):
        """retire_rewired_edges compares resolved connections, so what it hides
        is the connection, and the two edges drawn for it stay as rows."""
        from scistack_gui.services.adoption import retire_rewired_edges

        _extract(chain, "AB")
        db = get_db()
        view = effective_edges(db, None, caller="test")
        segments = {
            e["id"]
            for e in view.drawn_list
            if view.manual_nodes.get(e["source"], {}).get("type") in ports.PORT_NODE_TYPES
            or view.manual_nodes.get(e["target"], {}).get("type") in ports.PORT_NODE_TYPES
            or str(e["source"]).startswith("use_")
            or str(e["target"]).startswith("use_")
        }
        assert segments
        retired = retire_rewired_edges(db, {B: "new_wiring"}, [], view.wiring_list, view.manual_nodes)
        assert retired

        after = effective_edges(db, None, caller="test")
        assert segments <= {e["id"] for e in after.drawn_list}
        assert not {
            edge_connection_id(e, after.manual_nodes) for e in after.wiring_list
        } & set(retired)


class TestExport:
    def test_ports_travel_with_an_exported_submodule(self, chain):
        from scistack_gui.services.portability_service import export_pipeline

        pid = _extract(chain, "BCD").json()["pipeline_id"]
        document = export_pipeline(get_db(), pid)
        node_types = {n["node_type"] for n in document["canvas"]["nodes"]}
        assert {ports.INPUT_PORT_NODE, ports.OUTPUT_PORT_NODE} <= node_types
        assert len(document["canvas"]["edges"]) == 4  # in->B, B->C, C->D, D->out


class TestLegality:
    def test_function_to_function_through_ports_binds_nothing(self):
        nodes = {
            "fn1": _node("functionNode", "one", "s1"),
            "fn2": _node("functionNode", "two", "s2"),
            "o": _node(ports.OUTPUT_PORT_NODE, "y", "s1"),
            "i": _node(ports.INPUT_PORT_NODE, "x", "s2"),
        }
        uses = [
            {"use_id": "u1", "parent_pipeline_id": "main", "child_pipeline_id": "s1"},
            {"use_id": "u2", "parent_pipeline_id": "main", "child_pipeline_id": "s2"},
        ]
        wiring = ports.resolve_wiring(
            [
                _edge("a", "fn1", "o"),
                _edge("b", "u1", "u2", source_handle="out__y", target_handle="in__x"),
                _edge("c", "i", "fn2", target_handle="in__a"),
            ],
            nodes,
            uses,
        )
        assert wiring.edges == []
        assert wiring.illegal[0][:2] == ("fn1", "fn2")

    def test_drawing_it_is_refused(self, client):
        s1 = client.post("/api/pipelines", json={"name": "s1"}).json()["pipeline_id"]
        s2 = client.post("/api/pipelines", json={"name": "s2"}).json()["pipeline_id"]
        _place(client, "fn1", "functionNode", "one", s1)
        _place(client, "o", "outputPortNode", "y", s1)
        _place(client, "fn2", "functionNode", "two", s2)
        _place(client, "i", "inputPortNode", "x", s2)
        client.put("/api/edges/a", json={"source": "fn1", "target": "o"})
        client.put(
            "/api/edges/c",
            json={"source": "i", "target": "fn2", "target_handle": "in__a"},
        )
        u1 = client.post("/api/pipelines/main/uses", json={"child_pipeline_id": s1}).json()["use_id"]
        u2 = client.post("/api/pipelines/main/uses", json={"child_pipeline_id": s2}).json()["use_id"]
        r = client.put(
            "/api/edges/b",
            json={
                "source": u1,
                "target": u2,
                "source_handle": "out__y",
                "target_handle": "in__x",
            },
        )
        assert r.status_code == 400
        assert "need a variable between them" in r.text


class TestIdenticalEdge:
    def test_redrawing_a_stored_line_writes_nothing(self, chain):
        """GitHub #24: a PathInput -> glue edge vanished from the screen and
        the user drew it twice more; all three rows were stored."""
        r = chain.put(
            "/api/edges/e_ab_again",
            json={"source": A, "target": B, "target_handle": "in__path"},
        )
        assert r.status_code == 200, r.text
        assert r.json().get("existing_edge_id") == "e_ab"
        stored = [e["id"] for e in ps.get_manual_edges(get_db())]
        assert "e_ab_again" not in stored
        assert stored.count("e_ab") == 1

    def test_a_different_port_is_a_different_line(self, chain):
        r = chain.put(
            "/api/edges/e_ab_other",
            json={"source": A, "target": B, "target_handle": "in__other"},
        )
        assert "existing_edge_id" not in r.json()


class TestGlueOnTheParent:
    """GitHub #24 / scistack-debug.log 2026-11-18 11:57: a PathInput -> glue
    edge drawn on `main` beside a submodule whose own glue node (same glue
    function) was removed never reached the canvas (client verdict
    `server_omitted`). The build now says where a drawn edge is lost
    (`graph_service._DrawnEdgeTracer`); if this fails, its WARN in the
    captured log names the stage."""

    def _canvas_edges(self, client, scope="main") -> set[str]:
        graph = client.get("/api/pipeline", params={"pipeline_id": scope}).json()
        return {e["id"] for e in graph["edges"]}

    def _glue(self, client) -> str:
        r = client.post("/api/glue", json={"name": "cfg"})
        assert r.json()["ok"], r.json()
        return r.json()["name"]  # glue_cfg(value)

    def test_a_pathinput_to_glue_edge_is_drawn(self, client, glue_project):
        glue = self._glue(client)
        _place(client, A, "pathInputNode", "A")
        _place(client, "glue_out", "glueNode", glue)
        r = client.put(
            "/api/edges/e_pi_glue",
            json={"source": A, "target": "glue_out", "target_handle": "in__value"},
        )
        assert r.status_code == 200, r.text

        assert "e_pi_glue" in self._canvas_edges(client)

    def test_it_is_drawn_beside_a_submodule_with_a_removed_glue_of_the_same_function(
        self, client, glue_project, caplog
    ):
        glue = self._glue(client)
        sub = client.post("/api/pipelines", json={"name": "sub"}).json()["pipeline_id"]
        _place(client, "port_cfg", "inputPortNode", glue, sub)
        _place(client, "glue_in", "glueNode", glue, sub)
        client.put(
            "/api/edges/e_port_glue",
            json={"source": "port_cfg", "target": "glue_in", "target_handle": "in__value"},
        )
        _place(client, A, "pathInputNode", "A")
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": sub}
        ).json()["use_id"]
        client.put(
            "/api/edges/e_pi_use",
            json={"source": A, "target": use_id, "target_handle": f"in__{glue}"},
        )
        # The user removes the inner glue and puts one on the parent instead.
        assert client.delete("/api/layout/glue_in").status_code == 200
        _place(client, "glue_out", "glueNode", glue)
        r = client.put(
            "/api/edges/e_pi_glue",
            json={"source": A, "target": "glue_out", "target_handle": "in__value"},
        )
        assert r.status_code == 200, r.text

        with caplog.at_level("WARNING"):
            drawn = self._canvas_edges(client)

        lost = [m for m in caplog.messages if "lost at" in m]
        assert "e_pi_glue" in drawn, lost


class TestOutputToAVariable:
    """GitHub #24 / scistack-debug.log 2026-11-18 11:41: a submodule output
    connected to a variable on the parent was refused because put_edge traced
    a segment already removed inside the submodule."""

    def _submodule(self, client):
        """Inside: ``fn -> VarX -> [output VarX]``. Parent: a placement and a
        VarX node of its own. Returns the use id."""
        sub = client.post("/api/pipelines", json={"name": "sub"}).json()["pipeline_id"]
        _place(client, "fn_in", "functionNode", "make_x", sub)
        _place(client, "var_in", "variableNode", "VarX", sub)
        _place(client, "out_x", "outputPortNode", "VarX", sub)
        client.put("/api/edges/e_fn_var", json={"source": "fn_in", "target": "var_in"})
        client.put("/api/edges/e_var_port", json={"source": "var_in", "target": "out_x"})
        _place(client, "var_out", "variableNode", "VarX", "main")
        return client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": sub}
        ).json()["use_id"]

    def _connect(self, client, use_id):
        return client.put(
            "/api/edges/e_outer",
            json={"source": use_id, "target": "var_out", "source_handle": "out__VarX"},
        )

    def test_a_variable_through_the_port_to_a_variable_is_refused_with_the_fix(self, client):
        """Decision 3 (2026-11-18): still refused, but the message says how to
        wire it instead."""
        r = self._connect(client, self._submodule(client))
        assert r.status_code == 400
        assert "two variables need a function between them" in r.text
        assert "Connect the step that makes the variable to the output port" in r.text

    def test_a_removed_inner_segment_does_not_block_the_connection(self, client):
        use_id = self._submodule(client)
        assert client.delete("/api/edges/e_var_port").status_code == 200
        r = client.put("/api/edges/e_fn_port", json={"source": "fn_in", "target": "out_x"})
        assert r.status_code == 200, r.text

        r = self._connect(client, use_id)

        assert r.status_code == 200, r.text
        view = effective_edges(get_db(), "main", caller="test")
        assert view.port_wiring is not None and view.port_wiring.illegal == ()
        assert any(
            e["source"] == "fn_in" and e["target"] == "var_out" for e in view.wiring_list
        )

    def test_the_trace_owner_drops_a_segment_hidden_on_another_canvas(self, client):
        """put_edge and the canvas read one trace (edge_view.trace_through_ports):
        a segment hidden inside the submodule is out of it even when the edge
        list comes from the parent's view."""
        from scistack_gui.domain.edge_view import trace_through_ports

        use_id = self._submodule(client)
        client.delete("/api/edges/e_var_port")
        view = effective_edges(get_db(), "main", caller="test")
        assert "e_var_port" in {e["id"] for e in view.drawn}  # hidden on `sub` only
        outer = {"id": "e_outer", "source": use_id, "target": "var_out",
                 "sourceHandle": "out__VarX", "targetHandle": None}

        wiring = trace_through_ports(
            get_db(), list(view.drawn) + [outer], view.manual_nodes, ps.get_pipeline_uses(get_db())
        )

        assert wiring.illegal == ()


class TestNoPorts:
    def test_wiring_is_exactly_what_is_drawn(self, chain):
        """With no input/output node anywhere, tracing doesn't touch anything."""
        view = effective_edges(get_db(), None, caller="test")
        assert view.port_wiring is None
        assert view.wiring_list == view.drawn_list
        assert view.open_arguments(B) == []


class TestBoundaryPlan:
    NODES = {"f": _node("functionNode", "f", "main"), "g": _node("functionNode", "g", "main")}

    def _plan(self, crossing, moved):
        return ports.plan_boundary(
            crossing,
            lambda end: end in moved,
            lambda end: end[5:] if end.startswith("var__") else None,
            lambda end: end,
        )

    def test_one_source_feeding_many_is_one_port(self):
        plan = self._plan(
            [
                _edge("1", "var__X", "f", target_handle="in__a"),
                _edge("2", "var__X", "g", target_handle="in__b"),
            ],
            {"f", "g"},
        )
        assert [(p.direction, p.name, len(p.edges)) for p in plan] == [
            (ports.INPUT_PORT_NODE, "X", 2)
        ]

    def test_name_collisions_get_a_suffix(self):
        taken = {"X", "X_2"}
        assert ports.unique_port_name("X", taken) == "X_3"
        assert ports.unique_port_name("Y", taken) == "Y"


class TestAdoption:
    def test_a_recorded_connection_through_ports_is_not_adopted_back(self):
        """History rebuilds B -> C, whose ends are on two canvases, so no canvas
        draws it and adoption stores nothing: the ports stay the one form."""
        nodes = [
            {"id": "var__C", "type": "variableNode", "data": {"label": "C"}},
            {"id": "fn__b__t", "type": "functionNode", "data": {"label": "b"}},
        ]
        manual = {
            "var__C": _node("variableNode", "C", "main"),
            "fn__b__t": _node("functionNode", "b", "sub"),
        }
        history_edge = {"id": "e__fn:b__var:C__", "source": "fn__b__t", "target": "var__C"}
        plan = plan_adoption(
            nodes,
            [history_edge],
            manual_nodes=manual,
            drawn_edges=[],
            adopted=set(),
            visible={"var__C": ["main"], "fn__b__t": ["sub"]},
            visible_edge_ids=set(),
            recorded_values={},
            stated_values={},
        )
        assert plan.edges == []


# ---------------------------------------------------------------------------
# Moving a folded node: its row is under the bare id, the canvas says ::main
# ---------------------------------------------------------------------------

PATH_INPUT, VARIABLE = "pathInput__data_path", "var__VarC"


def _fold(node_id: str, scope: str = "main") -> str:
    """The shape a hand-placed entity box folds into once its entity has history:
    the row under the bare id, the position under ``{bare}::{scope}``
    (``graduate_manual_node``). Returns the id the canvas draws it under."""
    placed = ids.placement_id(node_id, scope)
    layout_store.move_node_position(node_id, scope, new_node_id=placed)
    return placed


def _labels_on(client, scope: str) -> set[str]:
    nodes = client.get("/api/pipeline", params={"pipeline_id": scope}).json()["nodes"]
    return {n["data"]["label"] for n in nodes if n["type"] not in ports.PORT_NODE_TYPES}


@pytest.fixture
def folded(client):
    """data_path -> fcn_b -> VarC -> fcn_d, the two entities folded. Matches the
    canvas in scistack-debug.log 2026-11-04 09:19."""
    _place(client, PATH_INPUT, "pathInputNode", "data_path")
    _place(client, "mf_b", "functionNode", "fcn_b")
    _place(client, VARIABLE, "variableNode", "VarC")
    _place(client, "mf_d", "functionNode", "fcn_d")
    client.put("/api/edges/e_ab", json={"source": PATH_INPUT, "target": "mf_b", "target_handle": "in__path"})
    client.put("/api/edges/e_bc", json={"source": "mf_b", "target": VARIABLE})
    client.put("/api/edges/e_cd", json={"source": VARIABLE, "target": "mf_d", "target_handle": "in__signal"})
    _fold(PATH_INPUT)
    _fold(VARIABLE)
    return client


def _extract_folded(client, source="main"):
    r = client.post(
        f"/api/pipelines/{source}/extract",
        json={
            "node_ids": [
                ids.placement_id(PATH_INPUT, source),
                "mf_b",
                ids.placement_id(VARIABLE, source),
            ],
            "name": "load",
        },
    )
    assert r.status_code == 200, r.text
    return r.json()["pipeline_id"]


class TestMovingAFoldedNode:
    def test_it_leaves_the_parent_canvas(self, folded):
        """Both were drawn on main and in the submodule, main's copy disconnected.
        The folded PathInput is a source, so it stays on main (GitHub #22)."""
        moved = {"fcn_b", "VarC"}
        before = _labels_on(folded, "main")  # includes the fixture's seeded run
        assert moved | {"data_path"} <= before

        pid = _extract_folded(folded)

        assert _labels_on(folded, "main") == (before - moved) | {"load"}
        assert _labels_on(folded, pid) == moved

    def test_its_row_moves_with_it(self, folded):
        pid = _extract_folded(folded)
        db = get_db()
        assert ps.manual_node_row(db, PATH_INPUT) == ("pathInputNode", "main")
        assert ps.manual_node_row(db, VARIABLE) == ("variableNode", pid)

    def test_the_connection_out_of_the_submodule_survives(self, folded):
        before = _connections()
        _extract_folded(folded)
        assert _connections() == before

    def test_a_bare_selected_id_still_moves(self, chain):
        """The spelling that always worked."""
        r = _extract(chain, "ABC")
        assert r.status_code == 200, r.text
        assert ps.manual_node_row(get_db(), C)[1] == r.json()["pipeline_id"]

    def test_a_copy_on_another_canvas_keeps_the_original(self, folded):
        """A duplicated hypothesis draws ``var__VarC::hyp`` from main's row, so
        extracting there must not take the node off main."""
        hyp = folded.post("/api/pipelines", json={"name": "hyp"}).json()["pipeline_id"]
        placed = ids.placement_id(VARIABLE, hyp)
        layout_store.write_node_position(placed, 5, 6, pipeline_id=hyp)
        # Wired on hyp, so it is neither a source nor a sink there (GitHub #22).
        _place(folded, "mf_x", "functionNode", "fcn_x", hyp)
        _place(folded, "mf_y", "functionNode", "fcn_y", hyp)
        folded.put("/api/edges/e_hx", json={"source": "mf_x", "target": placed})
        folded.put(
            "/api/edges/e_hy",
            json={"source": placed, "target": "mf_y", "target_handle": "in__signal"},
        )
        intent_store.set_run_options(get_db(), VARIABLE, {"dry_run": True}, scope="main")

        r = folded.post(
            f"/api/pipelines/{hyp}/extract", json={"node_ids": [placed], "name": "sub"}
        )
        assert r.status_code == 200, r.text
        sub = r.json()["pipeline_id"]

        db = get_db()
        assert ps.manual_node_row(db, VARIABLE)[1] == "main"
        assert "VarC" in _labels_on(folded, "main")
        assert "VarC" in _labels_on(folded, sub)
        assert "VarC" not in _labels_on(folded, hyp)
        assert ps.get_node_config(db, ids.placement_id(VARIABLE, "main"))["runOptions"] == {"dry_run": True}


class TestSettingsFollowTheNode:
    """Statements apply on the canvas they were stated on. ``read_csv`` lost its
    run options and schema location when extract left them on main (scistack-debug.log
    2026-11-04: orphan WARN 0 times before the extract, 8 after)."""

    RUN_OPTIONS = {"dry_run": True}
    LOCATION = {"schemaLevel": ["subject"]}

    @pytest.fixture
    def stated(self, folded):
        db = get_db()
        intent_store.set_run_options(db, "mf_b", self.RUN_OPTIONS)
        intent_store.set_schema_location(db, "mf_b", self.LOCATION)
        return folded

    def test_a_function_reads_the_same_settings_inside(self, stated):
        db = get_db()
        before = ps.get_node_config(db, "mf_b")
        assert before["runOptions"] == self.RUN_OPTIONS
        assert before["schemaLevel"] == self.LOCATION["schemaLevel"]

        pid = _extract_folded(stated)

        assert ps.get_node_config(db, "mf_b") == before
        assert ps.get_node_configs(db, pid)["mf_b"]["runOptions"] == self.RUN_OPTIONS

    def test_the_parent_no_longer_holds_them(self, stated):
        db = get_db()
        _extract_folded(stated)
        assert "mf_b" not in ps.get_node_configs(db, "main")

    def test_a_folded_node_keeps_its_settings_too(self, folded):
        db = get_db()
        placed = ids.placement_id(VARIABLE, "main")
        intent_store.set_run_options(db, placed, self.RUN_OPTIONS)

        pid = _extract_folded(folded)

        moved = ids.placement_id(VARIABLE, pid)
        assert ps.get_node_config(db, moved)["runOptions"] == self.RUN_OPTIONS
        assert ps.get_node_config(db, VARIABLE)["runOptions"] == self.RUN_OPTIONS

    def test_another_canvas_keeps_its_own(self, stated):
        """Only what was stated on the source canvas moves."""
        db = get_db()
        other = stated.post("/api/pipelines", json={"name": "other"}).json()["pipeline_id"]
        intent_store.set_run_options(db, "mf_b", {"dry_run": False}, scope=other)
        _extract_folded(stated)
        assert ps.get_node_configs(db, other)["mf_b"]["runOptions"] == {"dry_run": False}

    def test_a_legacy_global_statement_stays_global(self, folded):
        db = get_db()
        intent_store.set_run_options(db, "mf_b", self.RUN_OPTIONS, scope="global")
        _extract_folded(folded)
        rows = intent_store.load_statements(db, subject_refs=["mf_b"], aspect="run_options")
        assert [st.scope for st in rows] == ["global"]

    def test_a_stated_setting_gives_the_same_targets(self, client):
        """The seeded run: a moved function's settings still reach its targets."""
        from scistack_gui.services.execution_service import _target_key, derive_target_for_node

        nodes = client.get("/api/pipeline").json()["nodes"]
        fn = next(n["id"] for n in nodes if n["type"] == "functionNode")
        db = get_db()
        intent_store.set_schema_location(db, fn, self.LOCATION)
        before = sorted(_target_key(t) for t in derive_target_for_node(db, fn))
        assert before

        r = client.post("/api/pipelines/main/extract", json={"node_ids": [fn], "name": "sub"})
        assert r.status_code == 200, r.text
        inside = client.get("/api/pipeline", params={"pipeline_id": r.json()["pipeline_id"]}).json()["nodes"]
        fn_inside = next(n["id"] for n in inside if n["type"] == "functionNode")

        assert sorted(_target_key(t) for t in derive_target_for_node(db, fn_inside)) == before
        assert ps.get_node_config(db, fn_inside)["schemaLevel"] == self.LOCATION["schemaLevel"]


class TestAlreadyRunDownstream:
    def test_a_recorded_edge_is_not_drawn_back_onto_the_parent(self, client):
        """RawSignal -> bandpass_filter has history. Extracting the step (and
        the variable, a source that stays outside, GitHub #22) must leave the
        recorded edge drawn on neither canvas, through refreshes (adoption runs
        in each build), with the line going into the placement."""
        _place(client, "mv_box", "variableNode", "RawSignal")
        nodes = client.get("/api/pipeline").json()["nodes"]
        raw = next(n["id"] for n in nodes if n["data"]["label"] == "RawSignal")
        fn = next(n["id"] for n in nodes if n["type"] == "functionNode")

        r = client.post(
            "/api/pipelines/main/extract", json={"node_ids": [raw, fn], "name": "sub"}
        )
        assert r.status_code == 200, r.text
        use_id = r.json()["use_id"]

        for _ in range(2):
            graph = client.get("/api/pipeline").json()
            labels = {n["data"]["label"] for n in graph["nodes"]}
            assert "RawSignal" in labels and "bandpass_filter" not in labels
            on_canvas = {n["id"] for n in graph["nodes"]}
            for e in graph["edges"]:
                assert e["source"] in on_canvas and e["target"] in on_canvas
            assert any(e["target"] == use_id for e in graph["edges"])


class TestDanglingDiagnostic:
    """scistack-debug.log 2026-11-18: 126 `point at a node not on this canvas`
    WARNs, every one an edge into a placement. The check ran before the
    placement nodes were added (stage C)."""

    def test_a_wired_placement_does_not_warn(self, chain, caplog):
        assert _extract(chain, "BCD").status_code == 200
        with caplog.at_level("WARNING", logger="scistack_gui.domain.scope_filter"):
            graph = chain.get("/api/pipeline").json()
            chain.get("/api/pipeline")  # a second build runs adoption too
        assert any(e["target"].startswith("use_") for e in graph["edges"])
        assert not [m for m in caplog.messages if "point at a node not on this canvas" in m]

    def test_a_real_dangling_edge_still_warns(self, caplog):
        from scistack_gui.domain.scope_filter import warn_dangling_edges

        nodes = [{"id": "a"}, {"id": "use_1"}]
        edges = [_edge("ok", "a", "use_1"), _edge("bad", "a", "gone")]
        with caplog.at_level("WARNING", logger="scistack_gui.domain.scope_filter"):
            assert warn_dangling_edges("main", nodes, edges) == [("a", "gone")]
        assert any("1 edge(s) point at a node not on this canvas" in m for m in caplog.messages)


class TestLocalSubmoduleTunables:
    """Stage E (user decision 2026-11-18: parity). ⚙ on a submodule extracted
    in this project failed with `is not a library pipeline` (scistack-debug.log
    2026-11-18); a library is only the shareable kind."""

    @pytest.fixture
    def placed(self, client, monkeypatch):
        from scistack_gui.project import registry

        def scale_signal(signal, gain=1.0):
            return signal

        monkeypatch.setitem(registry._functions, "scale_signal", scale_signal)
        pid = client.post("/api/pipelines", json={"name": "local_sub"}).json()["pipeline_id"]
        _place(client, "p_sig", "inputPortNode", "signal", pid)
        _place(client, "param__cutoff", "parameterNode", "cutoff", pid)
        _place(client, "mf_scale", "functionNode", "scale_signal", pid)
        client.put(
            "/api/edges/i",
            json={"source": "p_sig", "target": "mf_scale", "target_handle": "in__signal"},
        )
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": pid}
        ).json()["use_id"]
        return client, use_id

    def test_a_local_placement_lists_its_tunables(self, placed):
        client, use_id = placed
        r = client.get(f"/api/pipeline-uses/{use_id}/tunables")
        assert r.status_code == 200, r.text
        tun = r.json()
        assert tun["library"] is None
        assert tun["parameters"] == [{"name": "cutoff", "value": None, "source": "missing"}]
        assert tun["step_values"] == [
            {"key": "scale_signal.gain", "value": 1.0, "default": 1.0, "parameter": None}
        ]

    def test_setting_a_step_value_writes_this_placement_s_binding(self, placed):
        client, use_id = placed
        r = client.put(
            f"/api/pipeline-uses/{use_id}/tunables",
            json={"key": "scale_signal.gain", "value": 2.0},
        )
        assert r.status_code == 200, r.text
        assert r.json()["step_values"][0]["value"] == 2.0
        use = next(u for u in ps.get_pipeline_uses(get_db()) if u["use_id"] == use_id)
        assert use["binding"]["params"] == {"scale_signal.gain": 2.0}

    def test_the_project_view_lists_local_placements(self, placed):
        client, use_id = placed
        rows = client.get("/api/tunables").json()
        row = next(r for r in rows if r["use_id"] == use_id)
        assert row["library"] is None and row["path"][-1] == "local_sub"


class TestRemovingAPortNode:
    """Stage F. scistack-debug.log 2026-11-18: the user removed an input node;
    its outer edge `runConfig -> use_…` kept naming the gone port forever."""

    @pytest.fixture
    def wired(self, client):
        pid = client.post("/api/pipelines", json={"name": "sub"}).json()["pipeline_id"]
        _place(client, "p_in", "inputPortNode", "x", pid)
        _place(client, "mf_in", "functionNode", "fcn_in", pid)
        _place(client, "p_out", "outputPortNode", "Y", pid)
        client.put("/api/edges/i1", json={"source": "p_in", "target": "mf_in", "target_handle": "in__x"})
        client.put("/api/edges/i2", json={"source": "mf_in", "target": "p_out"})
        _place(client, A, "pathInputNode", "A")
        _place(client, E, "variableNode", "VarE")
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": pid}
        ).json()["use_id"]
        assert client.put(
            "/api/edges/o_in", json={"source": A, "target": use_id, "target_handle": "in__x"}
        ).status_code == 200
        assert client.put(
            "/api/edges/o_out", json={"source": use_id, "target": E, "source_handle": "out__Y"}
        ).status_code == 200
        return client

    @staticmethod
    def _drawn_on_main() -> set[str]:
        return {e["id"] for e in effective_edges(get_db(), "main", caller="test").drawn}

    @pytest.mark.parametrize("port,outer,other", [("p_in", "o_in", "o_out"), ("p_out", "o_out", "o_in")])
    def test_its_outer_edges_go_with_it(self, wired, port, outer, other):
        assert {outer, other} <= self._drawn_on_main()
        assert wired.delete(f"/api/layout/{port}").status_code == 200
        drawn = self._drawn_on_main()
        assert outer not in drawn
        assert other in drawn
        view = effective_edges(get_db(), None, caller="test")
        dangling = view.port_wiring.dangling if view.port_wiring else []
        assert not [e for e in dangling if e.get("id") == outer]

    def test_undo_brings_back_both(self, wired):
        from tests.test_history import _change, _undo

        assert wired.delete("/api/layout/p_in", headers=_change("rm_port")).status_code == 200
        assert "o_in" not in self._drawn_on_main()
        assert _undo(wired, "rm_port")["status"] == "ok"
        assert "o_in" in self._drawn_on_main()
