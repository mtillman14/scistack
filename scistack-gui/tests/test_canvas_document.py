"""The canvas is a document (plan ``.claude/plan-canvas-as-document.md``
Stages 2-4; walkthrough ``docs/claude/canvas-as-document-explained.md``).

* Adoption: the first build stores what history would draw, under the same
  ids, once; a removed node or edge is never adopted back.
* The document view: only stored rows are drawn.
* Running from the document: a node's stored edges decide what runs, and the
  recorded rows of that wiring supply the exact values (D1: no superfluous
  re-runs).
* Clear data keeps the canvas.
"""

from __future__ import annotations

import pytest

from scistack_gui import node_wiring
from scistack_gui import pipeline_store as ps
from scistack_gui.canvas_shape_cli import capture_shapes
from scistack_gui.domain.adoption import (
    AdoptionPlan,
    document_view,
    edge_subject,
    node_subject,
    plan_adoption,
    value_subject,
)


def _graph(client, scope: str = "main") -> dict:
    return client.get("/api/pipeline", params={"pipeline_id": scope}).json()


def _shape(db) -> dict:
    return capture_shapes(db)["pipelines"]["main"]["shape"]


def _refs_and_edges(shape: dict) -> tuple:
    return (
        sorted((n["ref"], tuple(n.get("values") or ())) for n in shape["nodes"]),
        shape["edges"],
    )


# ---------------------------------------------------------------------------
# The pure rule
# ---------------------------------------------------------------------------


def _n(nid, ntype="variableNode", label=None):
    return {"id": nid, "type": ntype, "data": {"label": label or nid.split("__")[1]}}


def _e(src, tgt, port=""):
    edge = {"id": f"h_{src}_{tgt}", "source": src, "target": tgt}
    if port:
        edge["targetHandle"] = f"in__{port}"
    return edge


FN = "fn__f__0123456789abcdef"


class TestPlanAdoption:
    def _plan(self, *, manual=None, drawn=(), adopted=(), visible=None, values=None, stated=None):
        nodes = [_n("var__Raw"), _n(FN, "functionNode", "f"), _n("var__Out")]
        edges = [_e("var__Raw", FN, "x"), _e(FN, "var__Out")]
        from scistack_gui.domain.graph_builder import connection_id

        for e in edges:
            e["id"] = connection_id(e["source"], e["target"], e.get("targetHandle"))
        vis = visible if visible is not None else {n["id"]: ["main"] for n in nodes}
        return plan_adoption(
            nodes,
            edges,
            manual_nodes=manual or {},
            drawn_edges=list(drawn),
            adopted=set(adopted),
            visible=vis,
            visible_edge_ids={e["id"] for e in edges},
            recorded_values=values or {},
            stated_values=stated or {},
        ), edges

    def test_everything_drawn_is_adopted_under_its_own_id(self):
        plan, edges = self._plan()
        assert sorted(n[0] for n in plan.nodes) == sorted(["var__Raw", FN, "var__Out"])
        assert {e["id"] for e in plan.edges} == {e["id"] for e in edges}
        assert plan.functions() == ["f"]

    def test_a_stored_row_is_not_adopted_again(self):
        manual = {nid: {"type": "variableNode", "label": "x"} for nid in ("var__Raw", FN, "var__Out")}
        plan, edges = self._plan(manual=manual, drawn=edges_of(self._plan()[1]))
        assert plan.empty

    def test_the_marker_stops_a_removed_node_coming_back(self):
        plan, _ = self._plan(adopted={node_subject("var__Out")})
        assert "var__Out" not in plan.node_ids
        # ...and with no row, its edge has no stored end, so it is not adopted.
        assert not any(e["target"] == "var__Out" for e in plan.edges)

    def test_the_marker_stops_a_removed_edge_coming_back(self):
        _, edges = self._plan()
        plan, _ = self._plan(adopted={edge_subject(edges[0]["id"])})
        assert edges[0]["id"] not in plan.edge_ids

    def test_a_node_no_canvas_draws_is_not_adopted(self):
        plan, _ = self._plan(visible={"var__Raw": ["main"], FN: ["main"]})
        assert "var__Out" not in plan.node_ids

    def test_scope_is_the_first_canvas_root_first(self):
        plan, _ = self._plan(visible={"var__Raw": ["main", "pipe_x"], FN: ["pipe_x"], "var__Out": ["main"]})
        scope_of = {n[0]: n[3] for n in plan.nodes}
        assert scope_of["var__Raw"] == "main" and scope_of[FN] == "pipe_x"

    def test_recorded_values_become_stated_once(self):
        plan, _ = self._plan(values={"low_hz": {"20": 4, "30": 2}}, stated={"low_hz": {"30"}})
        assert plan.values == [("low_hz", "20")]
        plan2, _ = self._plan(values={"low_hz": {"20": 4}}, adopted={value_subject("low_hz", "20")})
        assert plan2.values == []


def edges_of(edges):
    return [dict(e) for e in edges]


class TestDocumentView:
    def test_history_only_nodes_and_edges_are_not_drawn(self):
        nodes = [_n("var__Raw"), _n("var__Gone")]
        edges = [_e("var__Raw", "var__Gone")]
        kept_nodes, kept_edges, counts = document_view(
            nodes, edges, manual_nodes={"var__Raw": {"type": "variableNode", "label": "Raw"}}, drawn_edges=[]
        )
        assert [n["id"] for n in kept_nodes] == ["var__Raw"]
        assert kept_edges == []
        assert counts["nodes_dropped"] == 1 and counts["edges_dropped"] == 1

    def test_a_history_copy_of_a_drawn_connection_is_kept_once_and_tagged_drawn(self):
        rows = {nid: {"type": "variableNode", "label": nid} for nid in ("var__A", FN)}
        history = {"id": "e1", "source": "var__A", "target": FN, "targetHandle": "in__x"}
        drawn = {"id": "manual__1", "source": "var__A", "target": f"{FN}::main", "targetHandle": "in__x"}
        _, kept, _ = document_view(
            [_n("var__A"), _n(FN, "functionNode", "f")],
            [history, {**drawn, "data": {"manual": True}}],
            manual_nodes=rows,
            drawn_edges=[drawn],
        )
        assert len(kept) == 1 and kept[0]["id"] == "manual__1"

    def test_a_deferred_plan_is_drawn_without_being_stored(self):
        plan = AdoptionPlan(nodes=[("var__Raw", "variableNode", "Raw", "main")])
        kept, _, _ = document_view([_n("var__Raw")], [], manual_nodes={}, drawn_edges=[], pending=plan)
        assert [n["id"] for n in kept] == ["var__Raw"]


class TestMissingComboHides:
    def test_the_two_combinations_a_diagonal_never_ran_are_hidden(self):
        from scidb.wirings import RecordedWiring

        from scistack_gui.services.adoption import missing_combo_hides

        w = RecordedWiring(
            fn_name="f",
            wiring_id="w",
            call_ids=("a", "b"),
            input_params={"x": "Raw"},
            outputs=("Out",),
            path_inputs={},
            constant_rows=({"k": 6, "m": 2}, {"k": 10, "m": 4}),
        )
        hides = missing_combo_hides(None, [w], set())
        assert sorted(tuple(sorted(h["variant_key"].items())) for h in hides) == [
            (("k", 6), ("m", 4)),
            (("k", 10), ("m", 2)),
        ]
        assert all(h["node_id"] == f"fn__f__{h['call_id']}" for h in hides)
        assert len({h["call_id"] for h in hides}) == 2

    def test_already_adopted_combos_are_skipped(self):
        from scidb.wirings import RecordedWiring

        from scistack_gui.domain.adoption import combo_subject
        from scistack_gui.services.adoption import missing_combo_hides

        w = RecordedWiring("f", "w", (), {"x": "Raw"}, ("Out",), {}, constant_rows=({"k": 1, "m": 1}, {"k": 2, "m": 2}))
        first = missing_combo_hides(None, [w], set())
        again = missing_combo_hides(None, [w], {combo_subject("f", h["call_id"]) for h in first})
        assert again == []


# ---------------------------------------------------------------------------
# Adoption, end to end (populated_db is a SCRIPT-run project)
# ---------------------------------------------------------------------------


class TestAdoption:
    def test_the_first_build_stores_every_node_and_edge(self, client, populated_db, bp_node_id):
        graph = _graph(client)
        rows = ps.get_manual_nodes(populated_db)
        for n in graph["nodes"]:
            if n["type"] in ("variableNode", "functionNode", "parameterNode", "pathInputNode"):
                assert n["id"].split("::")[0] in rows, f"{n['id']} is not stored"
        assert bp_node_id in rows, "adopted under the id the build already gave it"
        assert all((e.get("data") or {}).get("manual") for e in graph["edges"])

    def test_the_build_after_adoption_draws_the_same_picture(self, client, populated_db):
        first = _shape(populated_db)
        second = _shape(populated_db)
        assert _refs_and_edges(first) == _refs_and_edges(second)

    def test_the_second_build_adopts_nothing(self, client, populated_db):
        _graph(client)
        before = node_wiring.adopted_subjects(populated_db)
        graph = _graph(client)
        assert node_wiring.adopted_subjects(populated_db) == before
        assert not any(w.get("kind") == "adopted" for w in graph.get("warnings", []))

    def test_the_adopting_build_notifies(self, client, populated_db):
        graph = _graph(client)
        notices = [w for w in graph.get("warnings", []) if w.get("kind") == "adopted"]
        assert len(notices) == 1 and "bandpass_filter" in notices[0]["message"]

    def test_recorded_values_are_stated_not_pending(self, client, populated_db):
        _graph(client)
        assert "20" in ps.get_stated_constants(populated_db).get("low_hz", set())
        assert not ps.get_pending_constants(populated_db).get("low_hz")

    def test_adoption_writes_an_audit_event(self, client, populated_db):
        _graph(client)
        rows = populated_db._duck._fetchall(
            "SELECT method, state FROM _canvas_change WHERE method = 'adopt'"
        )
        assert rows and rows[0][1] == "event"

    def test_inside_an_undo_recording_nothing_is_written_but_it_is_drawn(self, client, populated_db):
        from scistack_gui import history
        from scistack_gui.api.pipeline import _build_graph

        with history.recording("t-adopt", label="t", method="t"):
            graph = _build_graph(populated_db, "main", run_states=False)
        assert ps.get_manual_nodes(populated_db) == {}
        labels = {n["data"]["label"] for n in graph["nodes"]}
        assert "bandpass_filter" in labels


class TestRemovingStaysRemoved:
    def test_a_removed_edge_is_hidden_and_never_adopted_back(self, client, populated_db, bp_node_id):
        graph = _graph(client)
        edge = next(
            e for e in graph["edges"]
            if e["source"].startswith("var__RawSignal") and e["target"].split("::")[0] == bp_node_id
        )
        r = client.request("DELETE", f"/api/edges/{edge['id']}", json={
            "source": edge["source"], "target": edge["target"],
            "target_handle": edge.get("targetHandle"),
            })
        assert r.status_code == 200
        for _ in range(2):
            ids = {e["id"] for e in _graph(client)["edges"]}
            assert edge["id"] not in ids

    def test_a_removed_node_keeps_its_row_and_stays_off_the_canvas(self, client, populated_db):
        graph = _graph(client)
        var = next(n for n in graph["nodes"] if n["data"]["label"] == "FilteredSignal")
        client.request("DELETE", f"/api/layout/{var['id']}")
        assert var["id"].split("::")[0] in ps.get_manual_nodes(populated_db)
        ids = {n["id"] for n in _graph(client)["nodes"]}
        assert var["id"] not in ids


# ---------------------------------------------------------------------------
# Running from the document (Stage 4)
# ---------------------------------------------------------------------------


class TestRunFromTheDocument:
    def test_an_unchanged_node_runs_its_recorded_rows(self, client, populated_db, bp_node_id):
        from scistack_gui.services.execution_service import derive_target_for_node

        _graph(client)
        targets = derive_target_for_node(populated_db, bp_node_id)
        assert targets and all(t.get("call_id") for t in targets), "recorded rows, exact values"
        assert {t["constants"]["low_hz"] for t in targets} == {20}

    def test_a_run_before_any_build_adopts_first(self, client, populated_db, bp_node_id):
        from scistack_gui.services.execution_service import derive_target_for_node

        assert derive_target_for_node(populated_db, bp_node_id)
        assert bp_node_id in ps.get_manual_nodes(populated_db)

    def test_name_scoped_equals_the_union_of_its_nodes(self, client, populated_db, bp_node_id):
        from scistack_gui.services.execution_service import (
            _target_key,
            derive_fn_targets,
            derive_target_for_node,
        )

        _graph(client)
        by_name = {_target_key(t) for t in derive_fn_targets(populated_db, "bandpass_filter")}
        by_node = {_target_key(t) for t in derive_target_for_node(populated_db, bp_node_id)}
        assert by_name == by_node

    def test_a_hidden_input_disconnects_the_node(self, client, populated_db, bp_node_id):
        from scistack_gui.services.execution_service import (
            derive_target_for_node,
            disconnected_reason,
        )

        graph = _graph(client)
        edge = next(
            e for e in graph["edges"]
            if e["source"].startswith("var__RawSignal") and e["target"].split("::")[0] == bp_node_id
        )
        client.request("DELETE", f"/api/edges/{edge['id']}", json={
            "source": edge["source"], "target": edge["target"],
            "target_handle": edge.get("targetHandle"),
            })
        assert derive_target_for_node(populated_db, bp_node_id) == []
        assert "disconnected" in (disconnected_reason(populated_db, "bandpass_filter", bp_node_id) or "")


# ---------------------------------------------------------------------------
# Clear data keeps the canvas (plan-clear-data.md GUI half)
# ---------------------------------------------------------------------------


class TestClearDataKeepsTheCanvas:
    def test_the_canvas_survives_and_the_records_go(self, client, populated_db, bp_node_id):
        from scistack_gui.services.clear_service import clear_data, clear_data_plan

        before = _shape(populated_db)
        plan = clear_data_plan(populated_db)
        assert plan["total_records"] > 0
        out = clear_data(populated_db, "test", plan["fingerprint"])
        assert out["deleted_records"] == plan["total_records"]
        after = _shape(populated_db)
        assert _refs_and_edges(after) == _refs_and_edges(before)

    def test_a_run_after_the_clear_states_the_same_wiring(self, client, populated_db, bp_node_id):
        from scistack_gui.domain.edge_resolver import variable_types_view
        from scistack_gui.domain.graph_builder import wiring_id
        from scistack_gui.services.clear_service import clear_data, clear_data_plan
        from scistack_gui.services.execution_service import derive_target_for_node

        recorded = wiring_id("bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {})
        plan = clear_data_plan(populated_db)
        clear_data(populated_db, "test", plan["fingerprint"])
        targets = derive_target_for_node(populated_db, bp_node_id)
        assert targets, "the stored edges and stated values are enough to run"
        for t in targets:
            assert wiring_id(
                "bandpass_filter", variable_types_view(t["bindings"]), {t["output_type"]}, {}
            ) == recorded
        assert {t["constants"]["low_hz"] for t in targets} == {20}, "stated '20' runs as 20"

    def test_a_stale_fingerprint_is_refused(self, client, populated_db):
        from scistack_gui.services.clear_service import clear_data

        with pytest.raises(ValueError, match="changed since the plan"):
            clear_data(populated_db, "test", "0000000000000000")


# ---------------------------------------------------------------------------
# Rules added after the first full-suite run (2026-10-22)
# ---------------------------------------------------------------------------


class TestNeverRunNodesStateTheirWiring:
    """node-identity §7a rule 1 for stored nodes that never ran
    (graph_builder.document_stated_claims): graduation used to fold them in."""

    def _rows(self):
        return {
            "mf": {"type": "functionNode", "label": "f"},
            "mv_in": {"type": "variableNode", "label": "Raw"},
            "mv_out": {"type": "variableNode", "label": "Out"},
        }

    def _edges(self):
        return [
            {"id": "e1", "source": "mv_in", "target": "mf", "targetHandle": "in__x"},
            {"id": "e2", "source": "mf", "target": "mv_out"},
        ]

    def test_a_recorded_wiring_it_states_is_claimed(self):
        from scistack_gui.domain.graph_builder import document_stated_claims, wiring_id

        w = wiring_id("f", {"x": "Raw"}, {"Out"}, {})
        assert document_stated_claims(self._rows(), self._edges(), {}, {("f", w)}) == {
            ("f", w): ["mf"]
        }

    def test_a_node_that_already_ran_is_left_to_its_record(self):
        from scistack_gui.domain.graph_builder import document_stated_claims, wiring_id

        w = wiring_id("f", {"x": "Raw"}, {"Out"}, {})
        assert document_stated_claims(self._rows(), self._edges(), {"mf": "w0"}, {("f", w)}) == {}

    def test_a_wiring_history_does_not_hold_is_not_claimed(self):
        from scistack_gui.domain.graph_builder import document_stated_claims

        assert document_stated_claims(self._rows(), self._edges(), {}, set()) == {}


class TestAHiddenNodesEdgesBindNothing:
    def test_hiding_an_input_variable_takes_its_edge_out_of_the_view(
        self, client, populated_db, bp_node_id
    ):
        from scistack_gui.domain.edge_view import effective_edges

        graph = _graph(client)
        var = next(n for n in graph["nodes"] if n["data"]["label"] == "RawSignal")
        before = effective_edges(populated_db, "main", caller="test").drawn_list
        assert any(e["source"].split("::")[0] == var["id"].split("::")[0] for e in before)
        client.request("DELETE", f"/api/layout/{var['id']}")
        after = effective_edges(populated_db, "main", caller="test").drawn_list
        assert not any(e["source"].split("::")[0] == var["id"].split("::")[0] for e in after)
