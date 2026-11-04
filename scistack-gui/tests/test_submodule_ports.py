"""Submodule ports: one connection, stored in two segments, read as one.

Plan ``.claude/plan-submodule-ports.md``; concept ``docs/claude/submodule-ports.md``.
The worked example is ``pathinputA -> fcnB -> varC -> fcnD -> varE``: every split
gets the ports the framework says, and every reader of wiring sees the original
connections whatever the split.
"""

from __future__ import annotations

import pytest

from scistack_gui.domain import ports
from scistack_gui.domain.adoption import plan_adoption
from scistack_gui.domain.edge_view import effective_edges
from scistack_gui.domain.graph_builder import edge_connection_id
from scistack_gui.runtime.db import get_db
from scistack_gui.stores import pipeline_store as ps

A, B, C, D, E = "pathInput__A", "mf_B", "mv_C", "mf_D", "mv_E"
BY_LETTER = {"A": A, "B": B, "C": C, "D": D, "E": E}

#: selection, input ports, output ports: the framework's table.
SPLITS = [
    ("A", [], ["A"]),
    ("AB", [], ["VarC"]),
    ("ABC", [], ["VarC"]),
    ("ABCD", [], ["VarE"]),
    ("ABCDE", [], []),
    ("B", ["A"], ["VarC"]),
    ("BC", ["A"], ["VarC"]),
    ("BCD", ["A"], ["VarE"]),
    ("BCDE", ["A"], []),
    ("C", ["VarC"], ["VarC"]),
    ("CD", ["VarC"], ["VarE"]),
    ("CDE", ["VarC"], []),
    ("D", ["VarC"], ["VarE"]),
    ("DE", ["VarC"], []),
    ("E", ["VarE"], []),
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

    def test_a_selection_with_no_crossing_needs_no_ports(self, chain):
        before = _connections()
        pid = _extract(chain, "ABCDE").json()["pipeline_id"]
        nodes = ps.get_manual_nodes(get_db(), pid)
        assert not [n for n in nodes.values() if n["type"] in ports.PORT_NODE_TYPES]
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
