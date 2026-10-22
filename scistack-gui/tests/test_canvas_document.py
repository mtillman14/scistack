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


# ---------------------------------------------------------------------------
# D1 follow-up: recorded selectors and run options survive a clear-data
# ---------------------------------------------------------------------------


class TestRecordedSettingsSurviveAClear:
    def _with_recorded(self, db, monkeypatch, **extra):
        rows = [dict(v, **extra) for v in db.list_pipeline_variants()]
        monkeypatch.setattr(db, "list_pipeline_variants", lambda *a, **k: [dict(r) for r in rows])
        return rows

    def test_what_every_row_agrees_on_becomes_the_nodes_settings(
        self, client, populated_db, bp_node_id, monkeypatch
    ):
        from scistack_gui.services.adoption import recorded_node_settings

        _graph(client)
        self._with_recorded(
            populated_db,
            monkeypatch,
            selectors={"signal": {"columns": ["a"], "iterate": False}},
            distribute=True,
            across_variants=["signal"],
        )
        settings = recorded_node_settings(populated_db, {bp_node_id: "bandpass_filter"})
        assert settings[bp_node_id]["columnSelections"] == {
            "signal": {"columns": ["a"], "iterate": False}
        }
        assert settings[bp_node_id]["runOptions"]["distribute"] is True
        assert settings[bp_node_id]["runOptions"]["across_variants"] == ["signal"]

    def test_rows_that_disagree_leave_it_unset(self, client, populated_db, bp_node_id, monkeypatch):
        from scistack_gui.services.adoption import recorded_node_settings

        _graph(client)
        rows = self._with_recorded(populated_db, monkeypatch)
        rows.append({**rows[0], "call_id": "other", "distribute": True})
        settings = recorded_node_settings(populated_db, {bp_node_id: "bandpass_filter"})
        assert "runOptions" not in settings.get(bp_node_id, {})

    def test_adoption_stores_them_on_the_node(self, client, populated_db, bp_node_id, monkeypatch):
        from scistack_gui import pipeline_store

        self._with_recorded(populated_db, monkeypatch, distribute=True)
        _graph(client)  # the adopting build
        cfg = pipeline_store.get_node_config(populated_db, bp_node_id)
        assert cfg.get("runOptions", {}).get("distribute") is True

    def test_pooling_reaches_a_run_built_from_edges(self):
        from scistack_gui.services.execution_service import _apply_node_pooling

        t = {"bindings": {"signal": {"kind": "variable", "ref": ["Raw"]}}, "constants": {}}
        [out] = _apply_node_pooling([t], ["signal"], "test")
        assert out["bindings"]["signal"]["pool_variants"] is True
        assert "pool_variants" not in t["bindings"]["signal"], "input not mutated"


# ---------------------------------------------------------------------------
# Step names (Q6): a named script run belongs to the node of that name
# ---------------------------------------------------------------------------

from scidb import BaseVariable as _BaseVariable  # noqa: E402


class StepRewireSignal(_BaseVariable):
    pass


class TestStepNames:
    def test_names_are_unique_per_function(self, client, populated_db):
        from scistack_gui.step_names import ensure_step_names, step_names

        out = ensure_step_names(
            populated_db, [("fn__f__a", "pkg.f"), ("fn__f__b", "pkg.f"), ("fn__g__c", "g")]
        )
        assert out == {"fn__f__a": "f", "fn__f__b": "f_2", "fn__g__c": "g"}
        assert step_names(populated_db)["fn__f__b"] == "f_2"
        # Existing names are kept.
        assert ensure_step_names(populated_db, [("fn__f__b", "pkg.f")]) == {"fn__f__b": "f_2"}

    def test_a_named_script_rewire_moves_the_node_not_a_new_one(
        self, client, populated_db, bp_node_id
    ):
        """The Q6 case: a script edited to feed the step a different input,
        run under the step's name, rewires THAT node."""
        import numpy as np
        from conftest import FilteredSignal, bandpass_filter

        from scidb import for_each
        from scistack_gui.step_names import set_step_name

        _graph(client)  # adopt
        set_step_name(populated_db, bp_node_id, "filter")
        for subj in (1, 2):
            for sess in ("pre", "post"):
                StepRewireSignal.save(np.zeros(10), subject=subj, session=sess)
        for_each(
            bandpass_filter,
            {"signal": StepRewireSignal, "low_hz": 20},
            [FilteredSignal],
            subject=[1, 2],
            session=["pre", "post"],
            step="filter",
        )

        graph = _graph(client)
        bp = [n for n in graph["nodes"] if n["data"].get("label") == "bandpass_filter"]
        assert [n["id"].split("::")[0] for n in bp] == [bp_node_id], "no second node"
        sources = {
            e["source"].split("::")[0]
            for e in graph["edges"]
            if e["target"].split("::")[0] == bp_node_id and e.get("targetHandle") == "in__signal"
        }
        assert sources == {"var__StepRewireSignal"}, f"the old input still drawn: {sources}"
        assert not [w for w in graph.get("warnings", []) if w.get("kind") == "adopted"
                    and "bandpass_filter" in w.get("functions", [])]

    def test_export_writes_the_step_name(self, client, populated_db, bp_node_id):
        from scistack_gui.services.code_export_service import export_pipeline_to_code
        from scistack_gui.step_names import step_name_of

        _graph(client)
        script = export_pipeline_to_code(populated_db, "main")["script"]
        assert step_name_of(populated_db, bp_node_id) == "bandpass_filter"
        assert "step='bandpass_filter'" in script

    def test_claims_are_by_function_and_newest_step(self):
        from types import SimpleNamespace

        from scistack_gui.step_names import step_claims

        agg = SimpleNamespace(
            fn_input_params={("f", "c1"): {}, ("g", "c2"): {}},
            fn_steps={("f", "c1"): ["old", "filter"], ("g", "c2"): ["filter"]},
        )
        rows = {"nf": {"type": "functionNode", "label": "f"}}
        claims = step_claims(agg, lambda fkey: f"w_{fkey[1]}", rows, {"nf": "filter"})
        assert claims == {("f", "w_c1"): ["nf"]}, "g's run named 'filter' is not f's node"
