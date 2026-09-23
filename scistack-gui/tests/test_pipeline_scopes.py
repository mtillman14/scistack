"""
Tests for nested-pipeline persistence (plan-gui-nested-pipelines.md Part A1).

Scoping model: every node belongs to one pipeline scope (root = 'main',
always exists; pre-scoping documents migrate into it). A pipeline placed on
a parent canvas is one _pipeline_uses row whose use_id IS the canvas
node_id (G1: same child twice = two nodes; bindings live on the use edge).
Layout positions are per-scope in the JSON file.
"""

import json

import pytest
from scistack_gui import layout as layout_store
from scistack_gui import pipeline_store as ps
from scistack_gui.db import get_db

# ---------------------------------------------------------------------------
# Pipeline scopes (CRUD + root guarantees)
# ---------------------------------------------------------------------------


class TestPipelineScopes:
    def test_root_always_exists(self, layout_path):
        pipes = ps.list_pipelines(get_db())
        assert pipes[0] == {"pipeline_id": "main", "name": "main"}

    def test_create_rename_hide(self, layout_path):
        db = get_db()
        pid = ps.create_pipeline(db, "loading")
        assert pid.startswith("pipe_")
        assert {"pipeline_id": pid, "name": "loading"} in ps.list_pipelines(db)

        ps.rename_pipeline(db, pid, "loading_v2")
        names = {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(db)}
        assert names[pid] == "loading_v2"

        ps.hide_pipeline(db, pid)
        assert pid not in {p["pipeline_id"] for p in ps.list_pipelines(db)}
        assert pid in {p["pipeline_id"] for p in ps.list_hidden_pipelines(db)}

        ps.unhide_pipeline(db, pid)
        names = {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(db)}
        assert names[pid] == "loading_v2"  # rename survived hide/unhide

    def test_duplicate_name_rejected(self, layout_path):
        db = get_db()
        ps.create_pipeline(db, "loading")
        with pytest.raises(ValueError, match="already exists"):
            ps.create_pipeline(db, "loading")

    def test_duplicate_name_rejected_even_when_hidden(self, layout_path):
        # Name uniqueness must hold globally, not just among visible
        # pipelines — otherwise two pipelines would collide by name the
        # moment the hidden one is restored.
        db = get_db()
        pid = ps.create_pipeline(db, "loading")
        ps.create_pipeline(db, "sibling")  # keep >1 visible so hiding is legal
        ps.hide_pipeline(db, pid)
        with pytest.raises(ValueError, match="already exists"):
            ps.create_pipeline(db, "loading")

    def test_root_can_be_renamed(self, layout_path):
        # 'main' is just the default hypothesis, not a special scratch scope.
        db = get_db()
        ps.rename_pipeline(db, "main", "other")
        names = {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(db)}
        assert names["main"] == "other"

    def test_cannot_hide_last_remaining_pipeline(self, layout_path):
        db = get_db()
        with pytest.raises(ValueError, match="last remaining"):
            ps.hide_pipeline(db, "main")

    def test_root_can_be_hidden_when_sibling_exists(self, layout_path):
        db = get_db()
        sibling = ps.create_pipeline(db, "sibling")

        ps.hide_pipeline(db, "main")

        assert "main" not in {p["pipeline_id"] for p in ps.list_pipelines(db)}
        hidden = {p["pipeline_id"]: p for p in ps.list_hidden_pipelines(db)}
        assert "main" in hidden

        # Hiding the last visible one now (only `sibling` left) is rejected.
        with pytest.raises(ValueError, match="last remaining"):
            ps.hide_pipeline(db, sibling)

        ps.unhide_pipeline(db, "main")
        assert "main" in {p["pipeline_id"] for p in ps.list_pipelines(db)}

    def test_hide_used_pipeline_rejected(self, layout_path):
        db = get_db()
        child = ps.create_pipeline(db, "loading")
        ps.add_pipeline_use(db, "main", child)
        with pytest.raises(ValueError, match="still used by"):
            ps.hide_pipeline(db, child)

    def test_hide_preserves_own_contents(self, layout_path):
        db = get_db()
        pid = ps.create_pipeline(db, "loading")
        ps.write_manual_node(db, "n1", "functionNode", "fn_a", pid)
        ps.write_manual_node(db, "n2", "variableNode", "VarB", pid)
        ps.write_manual_edge(db, {"id": "e1", "source": "n1", "target": "n2"})

        ps.hide_pipeline(db, pid)

        assert pid not in {p["pipeline_id"] for p in ps.list_pipelines(db)}
        # Never delete data: nodes/edges survive the hide untouched.
        assert set(ps.get_manual_nodes(db, pid)) == {"n1", "n2"}
        assert any(e["id"] == "e1" for e in ps.get_manual_edges(db))


# ---------------------------------------------------------------------------
# Hypotheses (tagged top-level pipelines, rendered as tabs)
# ---------------------------------------------------------------------------


class TestHypotheses:
    def test_root_is_default_hypothesis(self, layout_path):
        db = get_db()
        (hyp,) = ps.list_hypotheses(db)
        assert hyp["pipeline_id"] == "main"
        assert hyp["name"] == "main"
        assert hyp["research_question"] == ""
        assert hyp["evidence_for"] == []
        assert hyp["evidence_against"] == []

    def test_create_hypothesis_tags_a_new_pipeline(self, layout_path):
        db = get_db()
        pid = ps.create_hypothesis(db, "gait symmetry")
        assert pid.startswith("pipe_")
        # It's a real pipeline too (submodule placement machinery just works).
        assert {"pipeline_id": pid, "name": "gait symmetry"} in ps.list_pipelines(db)

        ids = {h["pipeline_id"] for h in ps.list_hypotheses(db)}
        assert ids == {"main", pid}

    def test_pipeline_without_tag_is_not_a_hypothesis(self, layout_path):
        db = get_db()
        pid = ps.create_pipeline(db, "loading")  # plain submodule, not tagged
        ids = {h["pipeline_id"] for h in ps.list_hypotheses(db)}
        assert pid not in ids

    def test_update_hypothesis_partial_fields(self, layout_path):
        db = get_db()
        pid = ps.create_hypothesis(db, "gait symmetry")

        ps.update_hypothesis(db, pid, research_question="Does symmetry change?")
        (hyp,) = [h for h in ps.list_hypotheses(db) if h["pipeline_id"] == pid]
        assert hyp["research_question"] == "Does symmetry change?"
        assert hyp["hypothesis_statement"] == ""

        ps.update_hypothesis(
            db, pid, evidence_for=["symmetry improved at week 4"]
        )
        (hyp,) = [h for h in ps.list_hypotheses(db) if h["pipeline_id"] == pid]
        # Untouched fields survive a partial update.
        assert hyp["research_question"] == "Does symmetry change?"
        assert hyp["evidence_for"] == ["symmetry improved at week 4"]

    def test_update_non_hypothesis_pipeline_rejected(self, layout_path):
        db = get_db()
        pid = ps.create_pipeline(db, "loading")
        with pytest.raises(ValueError, match="not a hypothesis"):
            ps.update_hypothesis(db, pid, research_question="x")

    def test_hide_hypothesis_preserves_pipeline_and_tag(self, layout_path):
        db = get_db()
        pid = ps.create_hypothesis(db, "gait symmetry")
        ps.update_hypothesis(db, pid, research_question="Does symmetry change?")

        ps.hide_hypothesis(db, pid)

        assert pid not in {p["pipeline_id"] for p in ps.list_pipelines(db)}
        assert pid not in {h["pipeline_id"] for h in ps.list_hypotheses(db)}
        hidden = {p["pipeline_id"]: p for p in ps.list_hidden_pipelines(db)}
        assert hidden[pid]["is_hypothesis"] is True

        ps.unhide_pipeline(db, pid)
        (hyp,) = [h for h in ps.list_hypotheses(db) if h["pipeline_id"] == pid]
        # Never delete data: research question survived the hide untouched.
        assert hyp["research_question"] == "Does symmetry change?"

    def test_hide_root_hypothesis_rejected_when_alone(self, layout_path):
        db = get_db()
        with pytest.raises(ValueError, match="last remaining"):
            ps.hide_hypothesis(db, "main")

    def test_hide_root_hypothesis_allowed_with_sibling(self, layout_path):
        db = get_db()
        ps.create_hypothesis(db, "gait symmetry")
        ps.hide_hypothesis(db, "main")
        assert "main" not in {h["pipeline_id"] for h in ps.list_hypotheses(db)}


class TestHypothesisApi:
    def test_list_and_create(self, client):
        r = client.get("/api/hypotheses")
        assert r.status_code == 200
        assert r.json()["hypotheses"][0]["pipeline_id"] == "main"

        r = client.post("/api/hypotheses", json={"name": "gait symmetry"})
        assert r.status_code == 200
        pid = r.json()["pipeline_id"]

        ids = {h["pipeline_id"] for h in client.get("/api/hypotheses").json()["hypotheses"]}
        assert ids == {"main", pid}

    def test_update_and_delete(self, client):
        pid = client.post("/api/hypotheses", json={"name": "gait symmetry"}).json()[
            "pipeline_id"
        ]

        r = client.put(
            f"/api/hypotheses/{pid}",
            json={
                "research_question": "Does symmetry change over time?",
                "evidence_for": ["week 4 trend"],
            },
        )
        assert r.status_code == 200
        hyps = {h["pipeline_id"]: h for h in client.get("/api/hypotheses").json()["hypotheses"]}
        assert hyps[pid]["research_question"] == "Does symmetry change over time?"
        assert hyps[pid]["evidence_for"] == ["week 4 trend"]

        assert client.delete(f"/api/hypotheses/{pid}").status_code == 200
        ids = {h["pipeline_id"] for h in client.get("/api/hypotheses").json()["hypotheses"]}
        assert pid not in ids

    def test_last_remaining_hypothesis_delete_is_400(self, client):
        assert client.delete("/api/hypotheses/main").status_code == 400

    def test_root_hypothesis_delete_succeeds_with_sibling(self, client):
        client.post("/api/hypotheses", json={"name": "gait symmetry"})
        assert client.delete("/api/hypotheses/main").status_code == 200
        ids = {h["pipeline_id"] for h in client.get("/api/hypotheses").json()["hypotheses"]}
        assert "main" not in ids


# ---------------------------------------------------------------------------
# Scoped nodes
# ---------------------------------------------------------------------------


class TestScopedNodes:
    def test_default_scope_is_root(self, layout_path):
        db = get_db()
        ps.write_manual_node(db, "n1", "functionNode", "fn_a")
        assert ps.get_manual_nodes(db)["n1"]["pipeline_id"] == "main"

    def test_scope_filtering(self, layout_path):
        db = get_db()
        pid = ps.create_pipeline(db, "loading")
        ps.write_manual_node(db, "root_node", "functionNode", "fn_a", "main")
        ps.write_manual_node(db, "sub_node", "functionNode", "fn_b", pid)

        assert set(ps.get_manual_nodes(db, "main")) == {"root_node"}
        assert set(ps.get_manual_nodes(db, pid)) == {"sub_node"}
        assert set(ps.get_manual_nodes(db)) == {"root_node", "sub_node"}


# ---------------------------------------------------------------------------
# Pipeline uses (pipeline-as-node)
# ---------------------------------------------------------------------------


class TestPipelineUses:
    def test_use_creates_row_and_canvas_node(self, layout_path):
        db = get_db()
        child = ps.create_pipeline(db, "loading")
        use_id = ps.add_pipeline_use(db, "main", child)

        (use,) = ps.get_pipeline_uses(db, "main")
        assert use == {
            "use_id": use_id,
            "parent_pipeline_id": "main",
            "child_pipeline_id": child,
            "binding": {},
        }
        node = ps.get_manual_nodes(db, "main")[use_id]
        assert node["type"] == "pipelineNode"
        assert node["label"] == "loading"  # child's name

    def test_same_child_twice_is_two_nodes(self, layout_path):
        db = get_db()
        child = ps.create_pipeline(db, "loading")
        u1 = ps.add_pipeline_use(db, "main", child, binding={"params": {"low_hz": 20}})
        u2 = ps.add_pipeline_use(db, "main", child, binding={"params": {"low_hz": 30}})

        assert u1 != u2
        uses = {u["use_id"]: u for u in ps.get_pipeline_uses(db, "main")}
        assert uses[u1]["binding"] == {"params": {"low_hz": 20}}
        assert uses[u2]["binding"] == {"params": {"low_hz": 30}}
        nodes = ps.get_manual_nodes(db, "main")
        assert nodes[u1]["type"] == "pipelineNode"
        assert nodes[u2]["type"] == "pipelineNode"

    def test_cycle_rejected_direct_and_transitive(self, layout_path):
        db = get_db()
        a = ps.create_pipeline(db, "a")
        b = ps.create_pipeline(db, "b")
        c = ps.create_pipeline(db, "c")
        ps.add_pipeline_use(db, a, b)
        ps.add_pipeline_use(db, b, c)

        with pytest.raises(ValueError, match="cycle"):
            ps.add_pipeline_use(db, c, a)  # transitive: a -> b -> c -> a
        with pytest.raises(ValueError, match="cycle"):
            ps.add_pipeline_use(db, a, a)  # self

    def test_unknown_pipeline_rejected(self, layout_path):
        db = get_db()
        with pytest.raises(ValueError, match="unknown pipeline_id"):
            ps.add_pipeline_use(db, "main", "pipe_nonexistent")

    def test_remove_use_cleans_node_and_edges(self, layout_path):
        db = get_db()
        child = ps.create_pipeline(db, "loading")
        use_id = ps.add_pipeline_use(db, "main", child)
        ps.write_manual_node(db, "v1", "variableNode", "VarA", "main")
        ps.write_manual_edge(db, {"id": "e1", "source": use_id, "target": "v1"})

        ps.remove_pipeline_use(db, use_id)

        assert ps.get_pipeline_uses(db, "main") == []
        assert use_id not in ps.get_manual_nodes(db)
        assert all(e["id"] != "e1" for e in ps.get_manual_edges(db))

    def test_update_binding_validates_keys(self, layout_path):
        db = get_db()
        child = ps.create_pipeline(db, "loading")
        use_id = ps.add_pipeline_use(db, "main", child)

        ps.update_use_binding(db, use_id, {"key_map": {"session": "subject"}})
        (use,) = ps.get_pipeline_uses(db, "main")
        assert use["binding"] == {"key_map": {"session": "subject"}}

        with pytest.raises(ValueError, match="unknown binding key"):
            ps.update_use_binding(db, use_id, {"bogus": 1})

    def test_rename_child_updates_canvas_labels(self, layout_path):
        db = get_db()
        child = ps.create_pipeline(db, "loading")
        use_id = ps.add_pipeline_use(db, "main", child)

        ps.rename_pipeline(db, child, "loading_v2")

        assert ps.get_manual_nodes(db, "main")[use_id]["label"] == "loading_v2"


# ---------------------------------------------------------------------------
# Scoped layout positions
# ---------------------------------------------------------------------------


class TestScopedPositions:
    def test_positions_are_per_scope(self, layout_path):
        db = get_db()
        pid = ps.create_pipeline(db, "loading")
        layout_store.write_node_position("n1", 1.0, 2.0)  # root
        layout_store.write_node_position("n2", 3.0, 4.0, pipeline_id=pid)

        root = layout_store.read_layout()
        sub = layout_store.read_layout(pipeline_id=pid)
        assert root["positions"] == {"n1": {"x": 1.0, "y": 2.0}}
        assert sub["positions"] == {"n2": {"x": 3.0, "y": 4.0}}
        assert sub["pipeline_id"] == pid

    def test_flat_positions_migrate_to_root_scope(self, layout_path):
        # A pre-scoping layout file: flat node_id -> {x, y} positions.
        layout_path.write_text(
            json.dumps(
                {
                    "positions": {"var__RawSignal": {"x": 10.0, "y": 20.0}},
                    "pipeline_db_migrated": True,
                }
            )
        )

        result = layout_store.read_layout()

        # DB-derived ids also pick up the one-time placement-qualification
        # migration — root (their one existing scope) becomes their one
        # existing placement (ids.placement_id).
        assert result["positions"]["var__RawSignal::main"] == {"x": 10.0, "y": 20.0}
        # And the migration is scope-shaped on disk after the next write.
        layout_store.write_node_position("n_new", 1.0, 1.0)
        on_disk = json.loads(layout_path.read_text())
        assert on_disk["positions_scoped"] is True
        assert on_disk["positions"]["main"]["var__RawSignal::main"] == {"x": 10.0, "y": 20.0}
        assert on_disk["positions"]["main"]["n_new"] == {"x": 1.0, "y": 1.0}

    def test_scoped_manual_node_write_and_read(self, layout_path):
        db = get_db()
        pid = ps.create_pipeline(db, "loading")
        layout_store.write_manual_node(
            "m1", 5.0, 6.0, "functionNode", "fn_a", pipeline_id=pid
        )

        sub = layout_store.read_layout(pipeline_id=pid)
        assert sub["positions"]["m1"] == {"x": 5.0, "y": 6.0}
        assert sub["manual_nodes"]["m1"]["pipeline_id"] == pid
        # Root scope does not see it.
        assert "m1" not in layout_store.read_layout()["manual_nodes"]

    def test_delete_node_clears_position_in_any_scope(self, layout_path):
        db = get_db()
        pid = ps.create_pipeline(db, "loading")
        layout_store.write_manual_node(
            "m1", 5.0, 6.0, "functionNode", "fn_a", pipeline_id=pid
        )

        layout_store.delete_node("m1")

        assert "m1" not in layout_store.read_layout(pipeline_id=pid)["positions"]
        assert "m1" not in ps.get_manual_nodes(db)


# ---------------------------------------------------------------------------
# Checkpoint 2: scope services + API (scoped graph, scope CRUD endpoints)
# ---------------------------------------------------------------------------


class TestScopeApi:
    def test_list_and_create_pipelines(self, client):
        r = client.get("/api/pipelines")
        assert r.status_code == 200
        assert r.json()["pipelines"][0]["pipeline_id"] == "main"

        r = client.post("/api/pipelines", json={"name": "loading"})
        assert r.status_code == 200
        pid = r.json()["pipeline_id"]
        assert pid.startswith("pipe_")

        # Duplicate name -> 400 with the store's message.
        r = client.post("/api/pipelines", json={"name": "loading"})
        assert r.status_code == 400
        assert "already exists" in r.json()["detail"]

    def test_last_remaining_pipeline_guard_is_400_but_rename_succeeds(self, client):
        # 'main' has no special protection — only the "don't hide the last
        # visible pipeline" guard applies, same as any other pipeline.
        assert client.delete("/api/pipelines/main").status_code == 400
        r = client.put("/api/pipelines/main", json={"name": "x"})
        assert r.status_code == 200

        client.post("/api/pipelines", json={"name": "sibling"})
        assert client.delete("/api/pipelines/main").status_code == 200

    def test_use_flow_and_pipeline_node_on_parent_canvas(self, client):
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]

        r = client.post(
            "/api/pipelines/main/uses",
            json={
                "child_pipeline_id": pid,
                "binding": {"params": {"low_hz": 30}},
                "x": 10.0,
                "y": 20.0,
            },
        )
        assert r.status_code == 200
        use_id = r.json()["use_id"]

        graph = client.get("/api/pipeline").json()
        pipeline_nodes = [n for n in graph["nodes"] if n["type"] == "pipelineNode"]
        assert [n["id"] for n in pipeline_nodes] == [use_id]
        data = pipeline_nodes[0]["data"]
        assert data["label"] == "loading"
        assert data["binding"] == {"params": {"low_hz": 30}}
        assert data["child_pipeline_id"] == pid
        # Its position landed in the parent scope.
        layout = client.get("/api/layout").json()
        assert layout["positions"][use_id] == {"x": 10.0, "y": 20.0}

    def test_cycle_and_binding_validation_are_400(self, client):
        a = client.post("/api/pipelines", json={"name": "a"}).json()["pipeline_id"]
        b = client.post("/api/pipelines", json={"name": "b"}).json()["pipeline_id"]
        client.post(f"/api/pipelines/{a}/uses", json={"child_pipeline_id": b})

        r = client.post(f"/api/pipelines/{b}/uses", json={"child_pipeline_id": a})
        assert r.status_code == 400
        assert "cycle" in r.json()["detail"]

        use_id = client.get("/api/pipelines").json()["uses"][0]["use_id"]
        r = client.put(
            f"/api/pipeline-uses/{use_id}/binding", json={"binding": {"bogus": 1}}
        )
        assert r.status_code == 400

    def test_remove_use_clears_node_and_position(self, client):
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        use_id = client.post(
            "/api/pipelines/main/uses",
            json={"child_pipeline_id": pid, "x": 5.0, "y": 5.0},
        ).json()["use_id"]

        assert client.delete(f"/api/pipeline-uses/{use_id}").status_code == 200

        graph = client.get("/api/pipeline").json()
        assert all(n["id"] != use_id for n in graph["nodes"])
        assert use_id not in client.get("/api/layout").json()["positions"]


class TestScopedGraph:
    def test_root_graph_excludes_sub_scope_manual_nodes(self, client):
        # Label with NO DB-derived counterpart, so the node stays manual
        # (a label matching one DB node would GRADUATE — separate test).
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        client.put(
            "/api/layout/sub_fn_node",
            json={
                "x": 0,
                "y": 0,
                "node_type": "functionNode",
                "label": "my_custom_fn",
                "pipeline_id": pid,
            },
        )

        root_ids = {n["id"] for n in client.get("/api/pipeline").json()["nodes"]}
        sub_ids = {
            n["id"]
            for n in client.get("/api/pipeline", params={"pipeline_id": pid}).json()[
                "nodes"
            ]
        }

        assert "sub_fn_node" not in root_ids
        assert "sub_fn_node" in sub_ids
        # DB-derived nodes (no saved position anywhere) default to root.
        assert "var__RawSignal" in root_ids
        assert "var__RawSignal" not in sub_ids

    def test_graduation_preserves_sub_scope_membership(self, client):
        """A manual node whose label matches exactly one DB-derived node
        GRADUATES into it — and because the position transfer is
        scope-aware, the canonical node inherits the sub-scope membership
        (it moves off the root canvas onto the sub canvas)."""
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        client.put(
            "/api/layout/sub_fn_node",
            json={
                "x": 0,
                "y": 0,
                "node_type": "functionNode",
                "label": "bandpass_filter",
                "pipeline_id": pid,
            },
        )

        root_ids = {n["id"] for n in client.get("/api/pipeline").json()["nodes"]}
        sub_ids = {
            n["id"]
            for n in client.get("/api/pipeline", params={"pipeline_id": pid}).json()[
                "nodes"
            ]
        }

        canonical = {
            i for i in (root_ids | sub_ids) if i.startswith("fn__bandpass_filter__")
        }
        assert len(canonical) == 1
        assert canonical <= sub_ids, (
            "graduated node must live on the sub canvas (position scope)"
        )
        assert not (canonical & root_ids)
        assert "sub_fn_node" not in (root_ids | sub_ids)  # replaced

    def test_position_moves_db_derived_node_between_scopes(self, client):
        """Dragging a DB-derived node onto a sub-canvas (position write in
        that scope) IS the membership record."""
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        client.put(
            "/api/layout/var__RawSignal", json={"x": 1.0, "y": 2.0, "pipeline_id": pid}
        )

        root_ids = {n["id"] for n in client.get("/api/pipeline").json()["nodes"]}
        sub_ids = {
            n["id"]
            for n in client.get("/api/pipeline", params={"pipeline_id": pid}).json()[
                "nodes"
            ]
        }

        assert "var__RawSignal" not in root_ids
        assert "var__RawSignal" in sub_ids

    def test_edges_filtered_by_scope_membership(self, client):
        """The seeded bandpass edges stay on root; a sub-scope canvas sees
        none of them."""
        pid = client.post("/api/pipelines", json={"name": "empty"}).json()[
            "pipeline_id"
        ]

        root = client.get("/api/pipeline").json()
        sub = client.get("/api/pipeline", params={"pipeline_id": pid}).json()

        assert len(root["edges"]) > 0
        assert sub["edges"] == []
        assert sub["pipeline_id"] == pid


class TestPathInputDeepCopy:
    def test_copy_shares_template_by_default(self, client_with_variable_file):
        """Placing the SAME named PathInput twice — via two separate manual
        nodes — is the default 'shared' behavior: no deep copy involved."""
        client = client_with_variable_file
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
        client.put("/api/layout/pi_a", json={
            "x": 0, "y": 0, "node_type": "pathInputNode", "label": "gait_data",
        })
        client.put("/api/layout/pi_b", json={
            "x": 10, "y": 0, "node_type": "pathInputNode", "label": "gait_data",
        })

        db = get_db()
        assert ps.get_manual_nodes(db)["pi_a"]["label"] == "gait_data"
        assert ps.get_manual_nodes(db)["pi_b"]["label"] == "gait_data"
        names = {p["name"] for p in client.get("/api/path-inputs").json()}
        assert names == {"gait_data"}  # one shared definition, not two

    def test_deep_copy_forks_only_the_targeted_node(self, client_with_variable_file):
        client = client_with_variable_file
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
        client.put("/api/layout/pi_a", json={
            "x": 0, "y": 0, "node_type": "pathInputNode", "label": "gait_data",
        })
        client.put("/api/layout/pi_b", json={
            "x": 10, "y": 0, "node_type": "pathInputNode", "label": "gait_data",
        })

        r = client.post("/api/path-inputs/pi_a/deep-copy")
        assert r.status_code == 200
        new_name = r.json()["name"]
        assert new_name != "gait_data"

        db = get_db()
        # Only pi_a was repointed; pi_b still references the original name.
        assert ps.get_manual_nodes(db)["pi_a"]["label"] == new_name
        assert ps.get_manual_nodes(db)["pi_b"]["label"] == "gait_data"
        # pi_a kept its position (deep-copy is not a move).
        layout = client.get("/api/layout").json()
        assert layout["positions"]["pi_a"] == {"x": 0.0, "y": 0.0}

        # The new definition cloned the original's template independently
        # (a fresh top-level source declaration, not a layout.json row —
        # see docs/claude/code-discovery-categories.md). There is no
        # "editing the original afterward" to test anymore: PathInput is
        # source-scanned and create-only from the GUI (no update endpoint).
        by_name = {p["name"]: p for p in client.get("/api/path-inputs").json()}
        assert by_name[new_name]["template"] == "{subject}.csv"
        assert by_name["gait_data"]["template"] == "{subject}.csv"

    def test_deep_copy_disambiguates_repeated_names(self, client_with_variable_file):
        client = client_with_variable_file
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "t"})
        client.put("/api/layout/pi_a", json={
            "x": 0, "y": 0, "node_type": "pathInputNode", "label": "gait_data",
        })
        client.put("/api/layout/pi_c", json={
            "x": 0, "y": 0, "node_type": "pathInputNode", "label": "gait_data",
        })

        name1 = client.post("/api/path-inputs/pi_a/deep-copy").json()["name"]
        name2 = client.post("/api/path-inputs/pi_c/deep-copy").json()["name"]
        assert name1 != name2

    def test_deep_copy_rejects_non_path_input_node(self, client):
        client.put("/api/layout/fn_node", json={
            "x": 0, "y": 0, "node_type": "functionNode", "label": "fn_a",
        })
        r = client.post("/api/path-inputs/fn_node/deep-copy")
        assert r.status_code == 400


class TestSweeps:
    """Parameter sweep nodes (#8) — builds on Constant node identity
    (shared-by-name) but is source-scanned: created with its full value
    list up front (no update endpoint — see
    docs/claude/code-discovery-categories.md), >1 value runs as
    EachOf(...) at execution time (see execution_service.build_run_inputs)."""

    def test_create_and_list(self, client_with_variable_file):
        client = client_with_variable_file
        r = client.post(
            "/api/parameters", json={"name": "window_seconds", "values": [10, 20, 30]}
        )
        assert r.status_code == 200

        by_name = {s["name"]: s for s in client.get("/api/parameters").json()}
        assert by_name["window_seconds"]["values"] == [10, 20, 30]

    def test_create_without_values_leaves_it_unvalued(self, client_with_variable_file):
        """The sidebar's "New parameter" form only collects a name — it
        never sends `values` (see EditTab.tsx's commitSweepDraft). This used
        to 422/error with "A Sweep needs at least one value" and, worse, the
        frontend swallowed that failure silently. Creating with no values
        must succeed and leave the Parameter with NO values — a legal,
        declared state at rest (see parameter_service.create_parameter). It
        used to scaffold a placeholder `0`, which became indistinguishable
        from a real declared value once written: it showed as a checked
        value on the node, fed for_each, and stamped `0` into any records
        produced before the user noticed."""
        client = client_with_variable_file
        r = client.post("/api/parameters", json={"name": "window_seconds"})
        assert r.status_code == 200
        assert r.json()["ok"] is True

        by_name = {s["name"]: s for s in client.get("/api/parameters").json()}
        assert by_name["window_seconds"]["values"] == []

    def test_delete_sweep_hides_node_but_keeps_source_declaration(
        self, client_with_variable_file
    ):
        """"Delete" hides the param__ node only — the source declaration
        (and hence the /api/parameters listing, which reads the registry
        directly) is untouched. Never delete, mark hidden."""
        client = client_with_variable_file
        client.post(
            "/api/parameters", json={"name": "window_seconds", "values": [10, 20, 30]}
        )
        r = client.delete("/api/parameters/window_seconds")
        assert r.status_code == 200

        names = {s["name"] for s in client.get("/api/parameters").json()}
        assert "window_seconds" in names  # still a valid source declaration

        graph = client.get("/api/pipeline").json()
        assert not any(
            n["type"] == "parameterNode" and n["data"]["label"] == "window_seconds"
            for n in graph["nodes"]
        )

    def test_sweep_node_placement_and_edge(self, client_with_variable_file):
        """A Sweep node behaves like a Constant node on the canvas: place
        it, wire it into a function's in__{param} handle. The manual
        node_id (sweep_a) GRADUATES to the canonical placement-qualified
        id (param__window_seconds::main) once the graph rebuilds — same
        mechanism PathInput/Constant nodes already use (see
        ids.DB_DERIVED_PREFIXES) — so look it up by label, not by the raw id
        assigned at creation."""
        client = client_with_variable_file
        client.post(
            "/api/parameters", json={"name": "window_seconds", "values": [10]}
        )
        client.put("/api/layout/sweep_a", json={
            "x": 0, "y": 0, "node_type": "parameterNode", "label": "window_seconds",
        })
        client.put("/api/layout/fn_a", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "some_fn",
        })
        r = client.put("/api/edges/e_sweep", json={
            "source": "sweep_a", "target": "fn_a", "target_handle": "in__window_seconds",
        })
        assert r.status_code == 200

        graph = client.get("/api/pipeline").json()
        node = next(
            n for n in graph["nodes"]
            if n["type"] == "parameterNode" and n["data"]["label"] == "window_seconds"
        )
        assert node["id"] == "param__window_seconds::main"


class TestSweepExecutionResolution:
    """Parameter values resolve to EachOf(...) at execution time — same
    resolution point build_run_inputs already uses for PathInput (see
    TestPathInputExecutionResolution), extended rather than duplicated.

    The binding always comes from the target's Parameter ``bindings``
    (``{param_name: {"kind": "parameter", "ref": declared_name}}``), which
    the wiring produces. These tests pass it explicitly to keep them at unit
    level; see ``TestEdgeDrivenBinding`` for the end-to-end path that builds
    it from a real canvas edge.
    """

    @staticmethod
    def _binding(**pairs):
        """A target binding *pairs* of ``param_name=declared_name``."""
        return {
            "bindings": {
                param: {"kind": "parameter", "ref": decl}
                for param, decl in pairs.items()
            },
            "input_types": {},
            "output_type": "X",
            "constants": {},
        }

    @staticmethod
    def _register_fn(name: str, params: str = "window_seconds"):
        """Append a real function definition to the configured
        variable_file, rather than injecting into registry._functions
        directly — create_path_input/create_parameter now call
        registry.refresh_module(), which re-scans that ONE file from
        scratch and would otherwise wipe a dict-injected function."""
        from scistack_gui import registry

        target = registry._module_path
        with open(target, "a") as f:
            f.write(f"\n\ndef {name}({params}):\n    return {params}\n")
        registry.refresh_module()

    def test_multi_value_sweep_resolves_to_eachof(self, client_with_variable_file):
        from scidb import EachOf

        client = client_with_variable_file
        client.post(
            "/api/parameters", json={"name": "window_seconds", "values": [10, 20, 30]}
        )
        self._register_fn("compute_rolling_sweep")

        from scistack_gui.services.execution_service import build_run_inputs

        target = self._binding(window_seconds="window_seconds")
        inputs = build_run_inputs(target, "compute_rolling_sweep")

        assert isinstance(inputs["window_seconds"], EachOf)
        assert inputs["window_seconds"].alternatives == [10, 20, 30]

    def test_single_value_sweep_stays_an_each_of(self, client_with_variable_file):
        """A registry-backed Sweep is returned AS-IS regardless of how many
        alternatives it has — build_run_inputs no longer reconstructs/
        collapses it (that was an artifact of the old layout.json-values
        rebuild; the registry now already holds the live object). A
        single-alternative EachOf/Sweep is functionally identical to the
        bare scalar at execution time (see EachOf's own docstring: "With a
        single value, behaves identically to passing that value directly")
        — the collapse happens at for_each's EachOf-expansion step, not
        here."""
        from scidb import EachOf

        client = client_with_variable_file
        client.post("/api/parameters", json={"name": "window_seconds", "values": [30]})
        self._register_fn("compute_rolling_sweep2")

        from scistack_gui.services.execution_service import build_run_inputs

        target = self._binding(window_seconds="window_seconds")
        inputs = build_run_inputs(target, "compute_rolling_sweep2")

        assert isinstance(inputs["window_seconds"], EachOf)
        assert inputs["window_seconds"].alternatives == [30]

    def test_unwired_param_is_left_unresolved(self, client_with_variable_file):
        """Nothing wired to the param -> fail safe (param absent from
        inputs), so the function's own default applies."""
        self._register_fn("compute_rolling_sweep3")

        from scistack_gui.services.execution_service import build_run_inputs

        inputs = build_run_inputs(self._binding(), "compute_rolling_sweep3")

        assert "window_seconds" not in inputs

    def test_name_coincidence_alone_does_not_resolve(
        self, client_with_variable_file
    ):
        """A Parameter declared with exactly the param's name, but NOT
        wired to it, must not be picked up. Resolution is the edge, and an
        unwired declaration is not a binding — this is the behaviour that
        replaced name matching outright."""
        client = client_with_variable_file
        client.post(
            "/api/parameters", json={"name": "window_seconds", "values": [10, 20]}
        )
        self._register_fn("compute_rolling_sweep4")

        from scistack_gui.services.execution_service import build_run_inputs

        inputs = build_run_inputs(self._binding(), "compute_rolling_sweep4")

        assert "window_seconds" not in inputs

    def test_declared_name_may_differ_from_the_param_it_fills(
        self, client_with_variable_file
    ):
        """The whole point of binding by edge: a Parameter declared 'test'
        can fill a param named 'window_seconds'. Name matching could not
        express this at all."""
        from scidb import EachOf

        client = client_with_variable_file
        client.post("/api/parameters", json={"name": "test", "values": [1, 2]})
        self._register_fn("compute_rolling_sweep5")

        from scistack_gui.services.execution_service import build_run_inputs

        target = self._binding(window_seconds="test")
        inputs = build_run_inputs(target, "compute_rolling_sweep5")

        assert isinstance(inputs["window_seconds"], EachOf)
        assert inputs["window_seconds"].alternatives == [1, 2]

    def test_binding_to_a_deleted_declaration_is_skipped_not_fatal(
        self, client_with_variable_file
    ):
        """The declaration can be removed from source after the edge was
        drawn. That leaves the param unbound (with a WARNING), never an
        exception mid-run."""
        self._register_fn("compute_rolling_sweep6")

        from scistack_gui.services.execution_service import build_run_inputs

        target = self._binding(window_seconds="no_such_parameter")
        inputs = build_run_inputs(target, "compute_rolling_sweep6")

        assert "window_seconds" not in inputs

    def test_path_input_and_parameter_bindings_resolve_independently(
        self, client_with_variable_file
    ):
        """A function with both a PathInput-backed param and a Parameter-
        backed param gets both resolved in one call — the two registries
        don't interfere with each other."""
        from scidb import EachOf, PathInput

        client = client_with_variable_file
        self._register_fn("mixed_fn", params="data_dir, window_seconds")

        client.post("/api/path-inputs", json={"name": "data_dir", "template": "{subject}"})
        client.post(
            "/api/parameters", json={"name": "window_seconds", "values": [30, 60]}
        )

        from scistack_gui.services.execution_service import build_run_inputs

        target = self._binding(window_seconds="window_seconds")
        target["bindings"]["data_dir"] = {"kind": "pathinput", "ref": "data_dir"}
        inputs = build_run_inputs(target, "mixed_fn")

        assert isinstance(inputs["data_dir"], PathInput)
        assert isinstance(inputs["window_seconds"], EachOf)
        assert inputs["window_seconds"].alternatives == [30, 60]

    def test_unchecked_values_are_excluded_from_the_run(
        self, client_with_variable_file
    ):
        """The per-value checkbox must reach EXECUTION, not just display.

        A scalar constant is filtered upstream (the whole target is
        dropped), but a multi-valued Parameter is handed to for_each whole
        and fanned out inside scidb, where the GUI's hidden-value state is
        invisible -- so unchecking one value used to look right in the UI
        and still run.
        """
        from scistack_gui import pipeline_store
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import build_run_inputs

        client = client_with_variable_file
        client.post(
            "/api/parameters", json={"name": "window_seconds", "values": [10, 20, 30]}
        )
        self._register_fn("fn_with_unchecked")

        db = get_db()
        pipeline_store.hide_parameter_value(db, "window_seconds", "20")

        target = self._binding(window_seconds="window_seconds")
        inputs = build_run_inputs(target, "fn_with_unchecked", db)

        # 10/30 arrive as floats: POST /api/parameters coerces to list[float].
        assert inputs["window_seconds"].alternatives == [10.0, 30.0]

    def test_hidden_value_matches_across_int_float_spelling(self):
        """The store holds rendered strings; a Parameter holds numbers. The
        /api/parameters model accepts ``float | int``, so the SAME declared
        value can arrive as 20 or 20.0 depending on how it was written -- a
        plain str() comparison would never match a hidden '20' and the
        checkbox would silently do nothing.

        Lives in the domain layer because both hidden-value routes share it
        (see is_hidden_value's docstring)."""
        from scistack_gui.domain.variant_resolver import is_hidden_value

        assert is_hidden_value(20.0, {"20"})
        assert is_hidden_value(20, {"20.0"})
        assert is_hidden_value(20.0, {"20.0"})
        assert is_hidden_value("x", {"x"})
        assert not is_hidden_value(20.5, {"20"})
        assert not is_hidden_value(21.0, {"20"})

    def test_unchecking_every_value_is_an_explicit_error(
        self, client_with_variable_file
    ):
        """Running the full set (or an arbitrary one) would produce exactly
        the records the user unchecked, so this raises instead."""
        import pytest

        from scistack_gui import pipeline_store
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import build_run_inputs

        client = client_with_variable_file
        client.post("/api/parameters", json={"name": "window_seconds", "values": [10, 20]})
        self._register_fn("fn_all_unchecked")

        db = get_db()
        for v in ("10", "20"):
            pipeline_store.hide_parameter_value(db, "window_seconds", v)

        target = self._binding(window_seconds="window_seconds")
        with pytest.raises(ValueError, match="every value of parameter"):
            build_run_inputs(target, "fn_all_unchecked", db)

    def test_unchecking_uses_the_declared_name_not_the_param_name(
        self, client_with_variable_file
    ):
        """The checkbox writes the PARAMETER NODE's declared name, so the
        hidden-value lookup has to use that too. Looking it up by the
        signature param it happens to feed made unchecking a value on a
        Parameter whose names differ silently do nothing.
        """
        from scistack_gui import pipeline_store
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import build_run_inputs

        client = client_with_variable_file
        client.post("/api/parameters", json={"name": "test", "values": [10, 20, 30]})
        self._register_fn("fn_declared_name_hide")

        db = get_db()
        pipeline_store.hide_parameter_value(db, "test", "20")

        target = self._binding(window_seconds="test")
        inputs = build_run_inputs(target, "fn_declared_name_hide", db)

        assert inputs["window_seconds"].alternatives == [10.0, 30.0]

    def test_unhidden_values_run_unchanged(self, client_with_variable_file):
        """No hidden values means the declared Parameter passes through
        untouched -- not rebuilt, so identity-sensitive callers see the
        registry object itself."""
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import build_run_inputs

        client = client_with_variable_file
        client.post("/api/parameters", json={"name": "window_seconds", "values": [10, 20]})
        self._register_fn("fn_no_hides")

        from scistack_gui import registry

        declared = registry.get_parameters_registry()["window_seconds"]
        target = self._binding(window_seconds="window_seconds")
        inputs = build_run_inputs(target, "fn_no_hides", get_db())

        assert inputs["window_seconds"] is declared


class TestEdgeDrivenBinding:
    """End-to-end: a function's inputs are built from the CANVAS EDGES,
    never from a name coincidence between a declaration and a signature
    parameter.

    Motivated by a real GUI session (examples/vo2max/scidb.log, runs
    6ila96uv / 9lt11d5m): a PathInput declared `test_pi` was wired into
    `pd.read_csv`, and because `read_csv` has no parameter called
    `test_pi`, the name match resolved nothing. for_each was called with
    `inputs={}`, fell back to schema-key iteration on an empty database,
    ran 0 iterations, wrote no records — and reported success, leaving the
    function and its output variable red with nothing in the log saying
    why.
    """

    @staticmethod
    def _register_fn(name: str, params: str):
        from scistack_gui import registry

        with open(registry._module_path, "a") as f:
            f.write(f"\n\ndef {name}({params}):\n    return {params.split(',')[0]}\n")
        registry.refresh_module()

    def _wire(self, client, *, fn: str, fn_node: str, out_var: str, source: str,
              handle: str):
        client.put(f"/api/layout/{fn_node}", json={
            "x": 0, "y": 0, "node_type": "functionNode", "label": fn,
        })
        client.put(f"/api/layout/{fn_node}_out", json={
            "x": 10, "y": 0, "node_type": "variableNode", "label": out_var,
        })
        client.put(f"/api/edges/{fn_node}_e_out", json={
            "source": fn_node, "target": f"{fn_node}_out",
        })
        client.put(f"/api/edges/{fn_node}_e_in", json={
            "source": source, "target": fn_node, "target_handle": handle,
        })

    def test_path_input_whose_name_differs_from_the_param_resolves(
        self, client_with_variable_file
    ):
        """The exact failure from the log, in miniature."""
        from scidb import PathInput

        client = client_with_variable_file
        self._register_fn("read_csv_like", "filepath_or_buffer")
        client.post("/api/path-inputs", json={
            "name": "test_pi", "template": "{subject}/{subject}_CPET.csv",
        })
        self._wire(
            client,
            fn="read_csv_like",
            fn_node="mf_readcsv",
            out_var="CpetRaw",
            source="pathInput__test_pi",
            handle="in__filepath_or_buffer",
        )

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            build_run_inputs,
            derive_fn_targets,
        )

        targets = derive_fn_targets(get_db(), "read_csv_like")
        assert targets, "output wiring should make a target derivable"
        inputs = build_run_inputs(targets[0], "read_csv_like")

        pi = inputs["filepath_or_buffer"]
        assert isinstance(pi, PathInput)
        assert pi.path_template == "{subject}/{subject}_CPET.csv"

    def test_parameter_whose_name_differs_from_the_param_resolves(
        self, client_with_variable_file
    ):
        """A wired Parameter is FANNED OUT AT DERIVATION: one target per
        declared value, each carrying a scalar under the parameter name.
        (The single-EachOf shape belongs to the other route — a Parameter
        resolved in build_run_inputs and handed to for_each whole, which
        then fans out inside scidb. Both produce the same runs.)

        What matters here is that the values reach the target at all, under
        the PARAMETER's name, from a declaration named something else.
        """
        client = client_with_variable_file
        self._register_fn("summarize_like", "window")
        client.post("/api/parameters", json={"name": "test", "values": [5, 10]})
        self._wire(
            client,
            fn="summarize_like",
            fn_node="mf_summarize",
            out_var="Summary",
            source="param__test",
            handle="in__window",
        )

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            build_run_inputs,
            derive_fn_targets,
        )

        targets = derive_fn_targets(get_db(), "summarize_like")

        assert {t["constants"]["window"] for t in targets} == {5.0, 10.0}
        # The binding survives onto the target, so the hidden-value filter
        # can translate the parameter back to its declaration.
        assert targets[0]["bindings"]["window"] == {
            "kind": "parameter",
            "ref": "test",
        }

        inputs = build_run_inputs(targets[0], "summarize_like")
        assert inputs["window"] == targets[0]["constants"]["window"]

    def test_unchecking_a_value_filters_a_differently_named_parameter(
        self, client_with_variable_file
    ):
        """The fan-out above is keyed by PARAMETER name while the checkbox
        store is keyed by DECLARED name. Comparing them directly matches
        nothing when they differ, so every unchecked value would run anyway
        — the same declared-vs-parameter confusion as the registry lookup,
        one layer further on."""
        from scistack_gui import pipeline_store
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import derive_fn_targets

        client = client_with_variable_file
        self._register_fn("summarize_like2", "window")
        client.post("/api/parameters", json={"name": "test", "values": [5, 10]})
        self._wire(
            client,
            fn="summarize_like2",
            fn_node="mf_summarize2",
            out_var="Summary2",
            source="param__test",
            handle="in__window",
        )

        db = get_db()
        # Hidden as '5.0' while the declaration holds int 5 — the store's
        # spelling is whatever the checkbox rendered, so the match has to be
        # int/float-tolerant (is_hidden_value), exactly as it already was on
        # the other route.
        pipeline_store.hide_parameter_value(db, "test", "5.0")

        targets = derive_fn_targets(db, "summarize_like2")

        assert {t["constants"]["window"] for t in targets} == {10}

    def test_declaration_named_after_the_param_but_unwired_does_not_resolve(
        self, client_with_variable_file
    ):
        """The converse, and the reason this is a clean break rather than a
        fallback: a PathInput named exactly like the parameter, with NO edge
        to it, must not be picked up. Under name matching this silently
        bound; now the param is left to its default."""
        client = client_with_variable_file
        self._register_fn("loader_like", "filepath")
        client.post("/api/path-inputs", json={
            "name": "filepath", "template": "data.csv",
        })
        # Output wiring only — nothing feeding the input.
        client.put("/api/layout/mf_unwired", json={
            "x": 0, "y": 0, "node_type": "functionNode", "label": "loader_like",
        })
        client.put("/api/layout/mv_unwired", json={
            "x": 10, "y": 0, "node_type": "variableNode", "label": "Unwired",
        })
        client.put("/api/edges/e_unwired_out", json={
            "source": "mf_unwired", "target": "mv_unwired",
        })

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            build_run_inputs,
            derive_fn_targets,
        )

        targets = derive_fn_targets(get_db(), "loader_like")
        assert targets
        inputs = build_run_inputs(targets[0], "loader_like")

        assert "filepath" not in inputs

    def test_wrong_handle_binds_the_param_it_actually_names(
        self, client_with_variable_file
    ):
        """Dropping a connection on the wrong handle is a real, easy mistake
        (the log's edge landed on `in__dtype_backend` instead of
        `in__filepath_or_buffer`). It must bind what the edge SAYS — being
        wrong here is recoverable and visible; silently rebinding to the
        param we guess was meant is neither."""
        from scidb import PathInput

        client = client_with_variable_file
        self._register_fn("two_param_fn", "filepath_or_buffer, dtype_backend")
        client.post("/api/path-inputs", json={
            "name": "test_pi", "template": "x.csv",
        })
        self._wire(
            client,
            fn="two_param_fn",
            fn_node="mf_two",
            out_var="TwoOut",
            source="pathInput__test_pi",
            handle="in__dtype_backend",
        )

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            build_run_inputs,
            derive_fn_targets,
        )

        targets = derive_fn_targets(get_db(), "two_param_fn")
        inputs = build_run_inputs(targets[0], "two_param_fn")

        assert isinstance(inputs["dtype_backend"], PathInput)
        assert "filepath_or_buffer" not in inputs


class TestDuplicatePipeline:
    def test_duplicate_copies_manual_nodes_config_and_edges(self, client):
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()["pipeline_id"]
        client.put("/api/layout/mv_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": pid,
        })
        client.put("/api/layout/mf_proc", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": pid,
        })
        client.put("/api/layout/mv_out", json={
            "x": 20, "y": 0, "node_type": "variableNode", "label": "FilteredSignal", "pipeline_id": pid,
        })
        client.put("/api/edges/e_in", json={"source": "mv_in", "target": "mf_proc"})
        client.put("/api/edges/e_out", json={"source": "mf_proc", "target": "mv_out"})
        client.put("/api/layout/mf_proc/config", json={"config": {"schemaSelection": {"exclude_levels": {"subject": ["S02"]}}}})

        r = client.post(f"/api/pipelines/{pid}/duplicate", json={"name": "loading_v2"})
        assert r.status_code == 200
        new_pid = r.json()["pipeline_id"]

        db = get_db()
        original_ids = set(ps.get_manual_nodes(db, pid))
        copy_nodes = ps.get_manual_nodes(db, new_pid)
        copy_ids = set(copy_nodes)
        assert original_ids.isdisjoint(copy_ids)  # fresh node ids
        assert {(n["type"], n["label"]) for n in copy_nodes.values()} == {
            ("variableNode", "RawSignal"),
            ("functionNode", "custom_proc"),
            ("variableNode", "FilteredSignal"),
        }
        # Config forked to the new node id.
        fn_copy = next(n for n in copy_nodes.values() if n["type"] == "functionNode")
        assert fn_copy.get("config") == {"schemaSelection": {"exclude_levels": {"subject": ["S02"]}}}

        # Edges duplicated onto the new node ids.
        id_by_label = {n["label"]: nid for nid, n in copy_nodes.items()}
        copy_edges = [
            e for e in ps.get_manual_edges(db)
            if e["source"] in copy_ids or e["target"] in copy_ids
        ]
        in_edge = next(e for e in copy_edges if e["target"] == id_by_label["custom_proc"])
        assert in_edge["source"] == id_by_label["RawSignal"]
        out_edge = next(e for e in copy_edges if e["source"] == id_by_label["custom_proc"])
        assert out_edge["target"] == id_by_label["FilteredSignal"]

        # The original is untouched.
        assert set(ps.get_manual_nodes(db, pid)) == original_ids

    def test_duplicate_includes_graduated_nodes_without_corrupting_original(self, client):
        """The seeded bandpass_filter graph (root scope) is DB-derived
        ("graduated"), not manual. An earlier version of duplicate_pipeline
        skipped graduated content entirely, because a second manual node
        with the same label used to collide on the next graph build and
        STEAL the position away from the original (graduation matched by
        label only, scope-blind). Graduation is now scope-aware
        (placement-qualified ids — see ids.placement_id):
        duplicate copies graduated content as an independent placement
        (own node_id, own run-state), and — the actual regression this
        whole rework exists to fix — the original is completely
        untouched.

        Note: duplication also *solidifies* the source's own bare
        canonical ids into explicit `{bare}::main` placements (see
        scope_service.duplicate_pipeline) — this is what prevents the
        ambiguity that used to make root's implicit content vanish once
        the duplicate's own copy independently graduated elsewhere. So
        `main`'s exact node ids legitimately change (bare -> placement-
        qualified); what must stay identical is its content (labels/
        types/count), not the literal id strings."""
        before = client.get("/api/pipeline").json()["nodes"]
        before_labels = {(n["type"], n["data"]["label"]) for n in before}

        r = client.post("/api/pipelines/main/duplicate", json={"name": "main_copy"})
        assert r.status_code == 200
        pid = r.json()["pipeline_id"]

        copy_labels = {
            (n["type"], n["data"]["label"])
            for n in client.get("/api/pipeline", params={"pipeline_id": pid}).json()["nodes"]
        }
        assert ("functionNode", "bandpass_filter") in copy_labels
        assert ("variableNode", "RawSignal") in copy_labels
        assert ("variableNode", "FilteredSignal") in copy_labels

        after = client.get("/api/pipeline").json()["nodes"]
        after_labels = {(n["type"], n["data"]["label"]) for n in after}
        assert after_labels == before_labels  # main's content is untouched
        assert len(after) == len(before)

    def test_duplicate_solidifies_never_positioned_nodes_without_collapsing_them(
        self, client
    ):
        """Regression test: the seeded bandpass_filter/RawSignal/
        FilteredSignal nodes have NEVER been explicitly positioned (no
        `/api/layout/...` PUT was ever issued for them) — the frontend
        auto-arranges such nodes via dagre on every load
        (frontend/src/layout.ts), and only stops doing so once a real
        saved position exists.

        `duplicate_pipeline` must still solidify main's own claim on each
        of these (see the big comment in scope_service.duplicate_pipeline)
        so the original doesn't lose them once the copy independently
        graduates elsewhere — but an earlier version of that fix wrote
        the SAME shared (0, 0) fallback for every one of them. The
        frontend treats any saved position as authoritative, so identical
        coordinates collapsed every such node onto the same point:
        they visually overlapped into what looked like disappeared nodes,
        with zero-length edges between them looking like dangling stubs.
        Solidified positions must be distinct from each other."""
        r = client.post("/api/pipelines/main/duplicate", json={"name": "main_copy"})
        assert r.status_code == 200

        positions = client.get("/api/layout", params={"pipeline_id": "main"}).json()[
            "positions"
        ]
        solidified = [
            (node_id, (pos["x"], pos["y"]))
            for node_id, pos in positions.items()
            if node_id.endswith("::main")
            and any(
                node_id.startswith(f"{p}__")
                for p in ("var", "fn", "const", "pathInput")
            )
        ]
        # The seeded graph has at least RawSignal, bandpass_filter, and
        # FilteredSignal — all previously unpositioned.
        assert len(solidified) >= 3
        coords = [c for _, c in solidified]
        assert len(set(coords)) == len(coords), (
            f"solidified nodes must not share coordinates, got {solidified}"
        )

    def test_duplicate_keeps_submodule_use_pointing_at_same_child(self, client):
        child = client.post("/api/pipelines", json={"name": "shared_prep"}).json()[
            "pipeline_id"
        ]
        parent = client.post("/api/pipelines", json={"name": "symmetry"}).json()[
            "pipeline_id"
        ]
        # No binding: `child` has zero steps, so any params binding would
        # fail bind-time validation regardless of duplication — this test
        # is only about the child_pipeline_id reference, not bindings.
        client.post(
            f"/api/pipelines/{parent}/uses", json={"child_pipeline_id": child}
        )

        r = client.post(f"/api/pipelines/{parent}/duplicate", json={"name": "speed"})
        assert r.status_code == 200
        new_pid = r.json()["pipeline_id"]

        db = get_db()
        (use,) = ps.get_pipeline_uses(db, new_pid)
        assert use["child_pipeline_id"] == child  # SAME submodule, not a copy
        # The original parent's use is untouched (different use_id).
        (orig_use,) = ps.get_pipeline_uses(db, parent)
        assert orig_use["use_id"] != use["use_id"]

    def test_duplicate_name_collision_is_400(self, client):
        client.post("/api/pipelines", json={"name": "loading"})
        r = client.post("/api/pipelines/main/duplicate", json={"name": "loading"})
        assert r.status_code == 400


class TestPasteNodes:
    """to-do #5: copy/paste a SELECTION of nodes (not a whole scope, see
    TestDuplicatePipeline) via scope_service.paste_nodes / _clone_nodes,
    within one pipeline or between two different ones.

    Uses PasteRawSignal/PasteFilteredSignal, NOT the fixture's RawSignal/
    FilteredSignal — those already have real saved records from
    populated_db's seeded bandpass_filter run, so the first graph-fetch
    paste_nodes does internally (get_pipeline_graph, needed to enumerate
    what to copy) GRADUATES mv_in/mv_out to their canonical DB-derived ids
    (var__RawSignal::main, ...) as an ordinary side effect of that fetch —
    same mechanism TestSweeps hit earlier. The literal 'mv_in'/'mv_out'
    ids this class's assertions hardcode would silently stop matching
    anything. Labels with no backing DB history never graduate, so the
    bare manual ids stay stable for the whole test."""

    def _wire(self, client, pid):
        client.put("/api/layout/mv_in", json={
            "x": 100, "y": 100, "node_type": "variableNode", "label": "PasteRawSignal", "pipeline_id": pid,
        })
        client.put("/api/layout/mf_proc", json={
            "x": 200, "y": 150, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": pid,
        })
        client.put("/api/layout/mv_out", json={
            "x": 300, "y": 100, "node_type": "variableNode", "label": "PasteFilteredSignal", "pipeline_id": pid,
        })
        client.put("/api/edges/e_in", json={"source": "mv_in", "target": "mf_proc"})
        client.put("/api/edges/e_out", json={"source": "mf_proc", "target": "mv_out"})

    def test_paste_within_same_scope_copies_config_and_internal_edges(self, client):
        self._wire(client, "main")
        client.put("/api/layout/mf_proc/config", json={"config": {"schemaSelection": {"exclude_levels": {"subject": ["S02"]}}}})

        r = client.post(
            "/api/pipelines/main/paste-nodes",
            json={
                "source_pipeline_id": "main",
                "node_ids": ["mv_in", "mf_proc", "mv_out"],
                "x": 500,
                "y": 500,
            },
        )
        assert r.status_code == 200
        node_id_map = r.json()["node_id_map"]
        assert set(node_id_map) == {"mv_in", "mf_proc", "mv_out"}

        db = get_db()
        copy_ids = set(node_id_map.values())
        assert copy_ids.isdisjoint({"mv_in", "mf_proc", "mv_out"})  # fresh ids
        copy_nodes = ps.get_manual_nodes(db, "main")
        assert copy_ids <= set(copy_nodes)
        assert {(n["type"], n["label"]) for nid, n in copy_nodes.items() if nid in copy_ids} == {
            ("variableNode", "PasteRawSignal"),
            ("functionNode", "custom_proc"),
            ("variableNode", "PasteFilteredSignal"),
        }
        fn_copy_id = node_id_map["mf_proc"]
        assert copy_nodes[fn_copy_id].get("config") == {"schemaSelection": {"exclude_levels": {"subject": ["S02"]}}}

        copy_edges = [
            e for e in ps.get_manual_edges(db)
            if e["source"] in copy_ids or e["target"] in copy_ids
        ]
        assert any(
            e["source"] == node_id_map["mv_in"] and e["target"] == node_id_map["mf_proc"]
            for e in copy_edges
        )
        assert any(
            e["source"] == node_id_map["mf_proc"] and e["target"] == node_id_map["mv_out"]
            for e in copy_edges
        )

        # The originals are untouched.
        original_labels = {(n["type"], n["label"]) for nid, n in ps.get_manual_nodes(db, "main").items() if nid in {"mv_in", "mf_proc", "mv_out"}}
        assert original_labels == {
            ("variableNode", "PasteRawSignal"),
            ("functionNode", "custom_proc"),
            ("variableNode", "PasteFilteredSignal"),
        }

    def test_paste_translates_selection_to_anchor_preserving_relative_layout(self, client):
        self._wire(client, "main")
        # mv_in at (100,100), mf_proc at (200,150) — a (100, 50) offset.
        r = client.post(
            "/api/pipelines/main/paste-nodes",
            json={
                "source_pipeline_id": "main",
                "node_ids": ["mv_in", "mf_proc"],
                "x": 0,
                "y": 0,
            },
        )
        node_id_map = r.json()["node_id_map"]

        positions = client.get("/api/layout", params={"pipeline_id": "main"}).json()["positions"]
        in_pos = positions[node_id_map["mv_in"]]
        proc_pos = positions[node_id_map["mf_proc"]]
        # Bounding-box top-left (mv_in, the min corner) lands exactly at the anchor.
        assert in_pos == {"x": 0.0, "y": 0.0}
        # Relative offset between the two copied nodes is preserved.
        assert proc_pos["x"] - in_pos["x"] == 100
        assert proc_pos["y"] - in_pos["y"] == 50

    def test_paste_drops_edges_to_nodes_outside_the_selection(self, client):
        self._wire(client, "main")

        r = client.post(
            "/api/pipelines/main/paste-nodes",
            json={
                "source_pipeline_id": "main",
                "node_ids": ["mv_in", "mf_proc"],  # mv_out excluded
                "x": 0,
                "y": 0,
            },
        )
        node_id_map = r.json()["node_id_map"]

        db = get_db()
        copy_ids = set(node_id_map.values())
        copy_edges = [
            e for e in ps.get_manual_edges(db)
            if e["source"] in copy_ids or e["target"] in copy_ids
        ]
        # Only the mv_in -> mf_proc edge survives; nothing points at mv_out
        # (never copied) or the original mv_out node id.
        assert len(copy_edges) == 1
        assert copy_edges[0]["source"] == node_id_map["mv_in"]
        assert copy_edges[0]["target"] == node_id_map["mf_proc"]

    def test_paste_between_different_scopes(self, client):
        self._wire(client, "main")
        target = client.post("/api/pipelines", json={"name": "other"}).json()["pipeline_id"]

        r = client.post(
            f"/api/pipelines/{target}/paste-nodes",
            json={
                "source_pipeline_id": "main",
                "node_ids": ["mv_in", "mf_proc", "mv_out"],
                "x": 0,
                "y": 0,
            },
        )
        assert r.status_code == 200
        node_id_map = r.json()["node_id_map"]

        db = get_db()
        assert set(node_id_map.values()) <= set(ps.get_manual_nodes(db, target))
        # main's originals are untouched, still on main.
        assert {"mv_in", "mf_proc", "mv_out"} <= set(ps.get_manual_nodes(db, "main"))

    def test_paste_keeps_submodule_use_pointing_at_same_child(self, client):
        child = client.post("/api/pipelines", json={"name": "shared_prep"}).json()["pipeline_id"]
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": child}
        ).json()["use_id"]

        r = client.post(
            "/api/pipelines/main/paste-nodes",
            json={"source_pipeline_id": "main", "node_ids": [use_id], "x": 0, "y": 0},
        )
        node_id_map = r.json()["node_id_map"]
        new_use_id = node_id_map[use_id]
        assert new_use_id != use_id

        db = get_db()
        uses_by_id = {u["use_id"]: u for u in ps.get_pipeline_uses(db, "main")}
        assert uses_by_id[new_use_id]["child_pipeline_id"] == child  # SAME submodule

    def test_paste_empty_selection_is_a_noop(self, client):
        r = client.post(
            "/api/pipelines/main/paste-nodes",
            json={"source_pipeline_id": "main", "node_ids": [], "x": 0, "y": 0},
        )
        assert r.status_code == 200
        assert r.json() == {"ok": True, "node_id_map": {}}


class TestScopedNodeEdgeHiding:
    """Regression tests for plan-scope-hidden-nodes-edges.md: a hidden
    canonical node/edge (var__/fn__/param__/pathInput__, and its
    e__{fn}__{wiring_id}__{...} edges) used to be recorded and read back
    GLOBALLY, not per pipeline scope. Since graph_builder.wiring_id is
    scope-independent by design (two pipelines with identical, unedited
    wiring compute the exact same canonical id — that sharing is
    intentional, see duplicate_pipeline's docstring), deleting/re-adding a
    node in one hypothesis pipeline used to bleed into every OTHER
    hypothesis pipeline sharing that same wiring."""

    @staticmethod
    def _nodes(client, pipeline_id):
        return client.get("/api/pipeline", params={"pipeline_id": pipeline_id}).json()[
            "nodes"
        ]

    @classmethod
    def _labels(cls, client, pipeline_id):
        return {
            (n["type"], n["data"]["label"]) for n in cls._nodes(client, pipeline_id)
        }

    @classmethod
    def _node_id(cls, client, pipeline_id, node_type, label):
        return next(
            n["id"]
            for n in cls._nodes(client, pipeline_id)
            if n["type"] == node_type and n["data"]["label"] == label
        )

    def test_delete_in_duplicate_does_not_hide_in_original(self, client):
        r = client.post("/api/pipelines/main/duplicate", json={"name": "main_copy"})
        assert r.status_code == 200
        copy_pid = r.json()["pipeline_id"]

        copy_node_id = self._node_id(client, copy_pid, "variableNode", "FilteredSignal")
        r = client.delete(f"/api/layout/{copy_node_id}")
        assert r.status_code == 200

        # The copy no longer shows it...
        assert ("variableNode", "FilteredSignal") not in self._labels(client, copy_pid)
        # ...but the ORIGINAL still does — used to fail: the delete hid the
        # node (and its edges) globally, removing it from every scope
        # sharing the wiring.
        assert ("variableNode", "FilteredSignal") in self._labels(client, "main")
        assert ("functionNode", "bandpass_filter") in self._labels(client, "main")

    def test_readd_in_one_scope_does_not_unhide_in_another(self, client):
        r = client.post("/api/pipelines/main/duplicate", json={"name": "main_copy"})
        assert r.status_code == 200
        copy_pid = r.json()["pipeline_id"]

        # Delete FilteredSignal from BOTH scopes independently — a clean
        # baseline where it's hidden everywhere.
        copy_node_id = self._node_id(client, copy_pid, "variableNode", "FilteredSignal")
        assert client.delete(f"/api/layout/{copy_node_id}").status_code == 200
        main_node_id = self._node_id(client, "main", "variableNode", "FilteredSignal")
        assert client.delete(f"/api/layout/{main_node_id}").status_code == 200

        assert ("variableNode", "FilteredSignal") not in self._labels(client, "main")
        assert ("variableNode", "FilteredSignal") not in self._labels(client, copy_pid)

        # Re-add it to `main` only, unconnected — mirrors the bug report.
        r = client.put(
            "/api/layout/var__FilteredSignal__readded",
            json={
                "x": 0,
                "y": 0,
                "node_type": "variableNode",
                "label": "FilteredSignal",
                "pipeline_id": "main",
            },
        )
        assert r.status_code == 200

        assert ("variableNode", "FilteredSignal") in self._labels(client, "main")
        # The copy must stay untouched — re-adding in main used to unhide
        # (and re-wire) the shared canonical id everywhere too.
        assert ("variableNode", "FilteredSignal") not in self._labels(client, copy_pid)

    def test_delete_edge_in_duplicate_does_not_hide_in_original(self, client):
        r = client.post("/api/pipelines/main/duplicate", json={"name": "main_copy"})
        assert r.status_code == 200
        copy_pid = r.json()["pipeline_id"]

        # duplicate_pipeline copies the wiring as a fresh MANUAL edge, which
        # then graduates in place (rewired, not removed — see
        # pipeline_store.graduate_manual_node) alongside the real DB-derived
        # edge for the same connection, so two edges into FilteredSignal
        # coexist in the copy. Target the DB-derived one specifically (its
        # id is deterministic — e__{fn}__{wiring_id}__{out_type} — see
        # graph_builder.build_edges) since that's the one this fix scopes;
        # the leftover manual duplicate is a separate, unrelated mechanism.
        copy_edges = client.get(
            "/api/pipeline", params={"pipeline_id": copy_pid}
        ).json()["edges"]
        fs_id = self._node_id(client, copy_pid, "variableNode", "FilteredSignal")
        db_edge = next(
            e for e in copy_edges if e["target"] == fs_id and e["id"].startswith("e__")
        )

        r = client.request(
            "DELETE",
            f"/api/edges/{db_edge['id']}",
            json={"source": db_edge["source"], "target": db_edge["target"]},
        )
        assert r.status_code == 200

        copy_edges_after = client.get(
            "/api/pipeline", params={"pipeline_id": copy_pid}
        ).json()["edges"]
        assert not any(e["id"] == db_edge["id"] for e in copy_edges_after)

        # The original's own bandpass_filter -> FilteredSignal DB-derived
        # edge must still be there — used to fail: hiding it in the copy
        # hid the same bare edge id everywhere.
        main_fs_id = self._node_id(client, "main", "variableNode", "FilteredSignal")
        main_edges = client.get("/api/pipeline").json()["edges"]
        assert any(
            e["target"] == main_fs_id and e["id"] == db_edge["id"]
            for e in main_edges
        )

    def test_delete_output_var_does_not_delete_its_producing_fn(self, client):
        """Regression: deleting a leaf OUTPUT variable node used to also
        wipe out the function node that produces it, in the SAME scope.

        Root cause — graph_builder.filter_hidden stripped the hidden output
        type out of fn_outputs BEFORE graph_builder.wiring_id was computed
        for grouping, so the function's canonical node id (which encodes
        wiring_id) changed the instant its output was hidden. The node's
        saved scope placement was recorded under the OLD id, so the
        renamed node resolved to no placement in a non-root scope and
        vanished from the view entirely — see graph_builder.filter_hidden's
        strip_var_type_values docstring for the fix."""
        r = client.post("/api/pipelines/main/duplicate", json={"name": "main_copy"})
        assert r.status_code == 200
        copy_pid = r.json()["pipeline_id"]

        assert ("functionNode", "bandpass_filter") in self._labels(client, copy_pid)

        copy_node_id = self._node_id(client, copy_pid, "variableNode", "FilteredSignal")
        r = client.delete(f"/api/layout/{copy_node_id}")
        assert r.status_code == 200

        copy_labels = self._labels(client, copy_pid)
        assert ("variableNode", "FilteredSignal") not in copy_labels
        # The function that PRODUCES FilteredSignal must survive the
        # deletion of its output leaf — only the leaf itself is hidden.
        assert ("functionNode", "bandpass_filter") in copy_labels


class TestDuplicateHypothesis:
    def test_duplicate_tags_copy_as_hypothesis(self, client):
        pid = client.post("/api/hypotheses", json={"name": "gait symmetry"}).json()[
            "pipeline_id"
        ]

        r = client.post(f"/api/hypotheses/{pid}/duplicate", json={"name": "gait speed"})
        assert r.status_code == 200
        new_pid = r.json()["pipeline_id"]

        ids = {h["pipeline_id"] for h in client.get("/api/hypotheses").json()["hypotheses"]}
        assert new_pid in ids

        # But a plain (non-hypothesis) pipeline duplicate is NOT auto-tagged.
        plain = client.post("/api/pipelines", json={"name": "prep"}).json()["pipeline_id"]
        new_plain = client.post(f"/api/pipelines/{plain}/duplicate", json={"name": "prep_v2"}).json()[
            "pipeline_id"
        ]
        ids = {h["pipeline_id"] for h in client.get("/api/hypotheses").json()["hypotheses"]}
        assert new_plain not in ids


class TestExtractToSubmodule:
    def test_extract_moves_node_and_adds_boundary_edges(self, client):
        client.put("/api/layout/mv_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal",
        })
        client.put("/api/layout/mf_proc", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "custom_proc",
        })
        client.put("/api/layout/mv_out", json={
            "x": 20, "y": 0, "node_type": "variableNode", "label": "FilteredSignal",
        })
        client.put("/api/edges/e_in", json={"source": "mv_in", "target": "mf_proc"})
        client.put("/api/edges/e_out", json={"source": "mf_proc", "target": "mv_out"})

        r = client.post(
            "/api/pipelines/main/extract",
            json={"node_ids": ["mf_proc"], "name": "processing"},
        )
        assert r.status_code == 200
        pid, use_id = r.json()["pipeline_id"], r.json()["use_id"]

        db = get_db()
        # The function node moved scopes; the boundary variables stayed.
        assert set(ps.get_manual_nodes(db, pid)) == {"mf_proc"}
        assert set(ps.get_manual_nodes(db, "main")) == {"mv_in", "mv_out", use_id}

        # Original edges are UNCHANGED (they feed the submodule's own
        # interface via document_interface — see the function's docstring)
        # — plus new boundary edges landed on the pipeline node's ports.
        edges = ps.get_manual_edges(db)
        assert {"id": "e_in", "source": "mv_in", "target": "mf_proc"} in edges
        assert {"id": "e_out", "source": "mf_proc", "target": "mv_out"} in edges
        new_in = [e for e in edges if e["source"] == "mv_in" and e["target"] == use_id]
        new_out = [e for e in edges if e["source"] == use_id and e["target"] == "mv_out"]
        assert len(new_in) == 1 and new_in[0]["targetHandle"] == "in__RawSignal"
        assert len(new_out) == 1 and new_out[0]["sourceHandle"] == "out__FilteredSignal"

        # The submodule's interface computes correctly from the untouched
        # original edges.
        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface == {"inputs": ["RawSignal"], "outputs": ["FilteredSignal"]}

        # The pipeline node on main carries the same ports.
        graph = client.get("/api/pipeline").json()
        node = next(n for n in graph["nodes"] if n["id"] == use_id)
        assert node["data"]["inputs"] == ["RawSignal"]
        assert node["data"]["outputs"] == ["FilteredSignal"]

    def test_extract_boundary_edge_where_moved_side_is_the_variable(self, client):
        """Regression: a selection can carry a variable node into the
        submodule while its downstream consuming function stays behind
        (e.g. RollingVO2 + its producer moved into "rolling_vo2", while
        stat_vo2_summary stayed on main). The kept side of that boundary
        edge is a FUNCTION, not a variable — the label must be looked up
        on the MOVED side instead, or the connection is silently dropped.
        """
        client.put("/api/layout/mv_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal",
        })
        client.put("/api/layout/mf_proc", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "custom_proc",
        })
        client.put("/api/layout/mv_out", json={
            "x": 20, "y": 0, "node_type": "variableNode", "label": "RollingVO2",
        })
        client.put("/api/layout/mf_summary", json={
            "x": 30, "y": 0, "node_type": "functionNode", "label": "stat_vo2_summary",
        })
        client.put("/api/edges/e_in", json={"source": "mv_in", "target": "mf_proc"})
        client.put("/api/edges/e_out", json={"source": "mf_proc", "target": "mv_out"})
        client.put("/api/edges/e_downstream", json={
            "source": "mv_out", "target": "mf_summary", "target_handle": "in__RollingVO2",
        })

        r = client.post(
            "/api/pipelines/main/extract",
            json={"node_ids": ["mv_in", "mf_proc", "mv_out"], "name": "rolling_vo2"},
        )
        assert r.status_code == 200
        pid, use_id = r.json()["pipeline_id"], r.json()["use_id"]

        db = get_db()
        # stat_vo2_summary stayed on main; the other three moved.
        assert set(ps.get_manual_nodes(db, "main")) == {"mf_summary", use_id}

        # The original boundary edge (mv_out -> mf_summary) is unchanged,
        # plus a NEW edge from the placed pipeline node's output port
        # replaces it for visual continuity on main's canvas.
        edges = ps.get_manual_edges(db)
        assert {
            "id": "e_downstream", "source": "mv_out", "target": "mf_summary",
            "targetHandle": "in__RollingVO2",
        } in edges
        replacement = [e for e in edges if e["source"] == use_id and e["target"] == "mf_summary"]
        assert len(replacement) == 1
        assert replacement[0]["sourceHandle"] == "out__RollingVO2"
        assert replacement[0]["targetHandle"] == "in__RollingVO2"

        graph = client.get("/api/pipeline").json()
        node = next(n for n in graph["nodes"] if n["id"] == use_id)
        assert "RollingVO2" in node["data"]["outputs"]

    def test_extract_rejects_node_outside_scope(self, client):
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        client.put("/api/layout/sub_node", json={
            "x": 0, "y": 0, "node_type": "functionNode", "label": "fn_a",
            "pipeline_id": pid,
        })

        r = client.post(
            "/api/pipelines/main/extract",
            json={"node_ids": ["sub_node"], "name": "regrouped"},
        )
        assert r.status_code == 400
        assert "not on pipeline" in r.json()["detail"]

    def test_extract_rejects_empty_selection(self, client):
        r = client.post(
            "/api/pipelines/main/extract", json={"node_ids": [], "name": "empty"}
        )
        assert r.status_code == 400


class TestDocumentInterface:
    def test_interface_from_manual_document(self, client):
        """var -> fn -> var wired inside a sub-scope: consumed-not-produced
        in, produced out."""
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        client.put(
            "/api/layout/mv_in",
            json={
                "x": 0,
                "y": 0,
                "node_type": "variableNode",
                "label": "RawSignal",
                "pipeline_id": pid,
            },
        )
        client.put(
            "/api/layout/mf_proc",
            json={
                "x": 0,
                "y": 0,
                "node_type": "functionNode",
                "label": "bandpass_filter",
                "pipeline_id": pid,
            },
        )
        client.put(
            "/api/layout/mv_out",
            json={
                "x": 0,
                "y": 0,
                "node_type": "variableNode",
                "label": "FilteredSignal",
                "pipeline_id": pid,
            },
        )
        client.put("/api/edges/e_in", json={"source": "mv_in", "target": "mf_proc"})
        client.put("/api/edges/e_out", json={"source": "mf_proc", "target": "mv_out"})

        iface = client.get(f"/api/pipelines/{pid}/interface").json()

        assert iface == {"inputs": ["RawSignal"], "outputs": ["FilteredSignal"]}

        # The pipeline node on the root canvas carries the same ports.
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": pid}
        ).json()["use_id"]
        graph = client.get("/api/pipeline").json()
        node = next(n for n in graph["nodes"] if n["id"] == use_id)
        assert node["data"]["inputs"] == ["RawSignal"]
        assert node["data"]["outputs"] == ["FilteredSignal"]


class TestHiddenPorts:
    """pipeline_store-level coverage for to-do #9's manual port-hiding
    override (see pipeline_store.hide_port's module docstring)."""

    def test_hide_unhide_roundtrip(self, layout_path):
        db = get_db()
        assert ps.get_hidden_ports(db, "main") == {"input": set(), "output": set()}

        ps.hide_port(db, "main", "input", "RawSignal")
        assert ps.get_hidden_ports(db, "main") == {
            "input": {"RawSignal"},
            "output": set(),
        }

        ps.unhide_port(db, "main", "input", "RawSignal")
        assert ps.get_hidden_ports(db, "main") == {"input": set(), "output": set()}

    def test_hide_is_idempotent(self, layout_path):
        db = get_db()
        ps.hide_port(db, "main", "output", "FilteredSignal")
        ps.hide_port(db, "main", "output", "FilteredSignal")  # ON CONFLICT DO NOTHING
        assert ps.get_hidden_ports(db, "main") == {
            "input": set(),
            "output": {"FilteredSignal"},
        }

    def test_get_hidden_ports_by_scope_keyed_per_pipeline(self, layout_path):
        db = get_db()
        other = ps.create_pipeline(db, "loading")
        ps.hide_port(db, "main", "input", "RawSignal")
        ps.hide_port(db, other, "output", "FilteredSignal")

        by_scope = ps.get_hidden_ports_by_scope(db)
        assert by_scope["main"] == {"input": {"RawSignal"}, "output": set()}
        assert by_scope[other] == {"input": set(), "output": {"FilteredSignal"}}


class TestHiddenPortsFiltering:
    """API-level coverage: hide-port/unhide-port toggling and its effect
    on document_interface's computed ports (to-do #9)."""

    def _wire_loading_scope(self, client, pid):
        client.put(
            "/api/layout/mv_in",
            json={
                "x": 0,
                "y": 0,
                "node_type": "variableNode",
                "label": "RawSignal",
                "pipeline_id": pid,
            },
        )
        client.put(
            "/api/layout/mf_proc",
            json={
                "x": 0,
                "y": 0,
                "node_type": "functionNode",
                "label": "bandpass_filter",
                "pipeline_id": pid,
            },
        )
        client.put(
            "/api/layout/mv_out",
            json={
                "x": 0,
                "y": 0,
                "node_type": "variableNode",
                "label": "FilteredSignal",
                "pipeline_id": pid,
            },
        )
        client.put("/api/edges/e_in", json={
            "source": "mv_in", "target": "mf_proc", "target_handle": "in__signal",
        })
        client.put("/api/edges/e_out", json={"source": "mf_proc", "target": "mv_out"})

    def test_hidden_ports_endpoint_reflects_current_state(self, client):
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        assert client.get(f"/api/pipelines/{pid}/hidden-ports").json() == {
            "input": [],
            "output": [],
        }

        r = client.post(
            f"/api/pipelines/{pid}/hide-port",
            json={"direction": "output", "var_type": "FilteredSignal"},
        )
        assert r.status_code == 200
        assert client.get(f"/api/pipelines/{pid}/hidden-ports").json() == {
            "input": [],
            "output": ["FilteredSignal"],
        }

        r = client.post(
            f"/api/pipelines/{pid}/unhide-port",
            json={"direction": "output", "var_type": "FilteredSignal"},
        )
        assert r.status_code == 200
        assert client.get(f"/api/pipelines/{pid}/hidden-ports").json() == {
            "input": [],
            "output": [],
        }

    def test_hidden_input_port_suppressed_from_interface_and_use_node(self, client):
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        self._wire_loading_scope(client, pid)

        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface == {"inputs": ["RawSignal"], "outputs": ["FilteredSignal"]}

        client.post(
            f"/api/pipelines/{pid}/hide-port",
            json={"direction": "input", "var_type": "RawSignal"},
        )
        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface == {"inputs": [], "outputs": ["FilteredSignal"]}

        # The placed pipeline node's port list bubbles the filtered
        # interface too.
        use_id = client.post(
            "/api/pipelines/main/uses", json={"child_pipeline_id": pid}
        ).json()["use_id"]
        graph = client.get("/api/pipeline").json()
        node = next(n for n in graph["nodes"] if n["id"] == use_id)
        assert node["data"]["inputs"] == []
        assert node["data"]["outputs"] == ["FilteredSignal"]

        # Un-hiding restores it everywhere.
        client.post(
            f"/api/pipelines/{pid}/unhide-port",
            json={"direction": "input", "var_type": "RawSignal"},
        )
        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface == {"inputs": ["RawSignal"], "outputs": ["FilteredSignal"]}

    def test_hidden_output_port_suppressed_from_interface(self, client):
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        self._wire_loading_scope(client, pid)

        client.post(
            f"/api/pipelines/{pid}/hide-port",
            json={"direction": "output", "var_type": "FilteredSignal"},
        )
        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface == {"inputs": ["RawSignal"], "outputs": []}

    def test_hidden_port_does_not_touch_the_internal_node(self, client):
        """Hiding a port suppresses the exposed dot on the scope's
        BOUNDARY only — the internal node that produces/consumes that
        type must stay fully visible on its own canvas (see
        pipeline_store.hide_port's module docstring)."""
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        self._wire_loading_scope(client, pid)

        client.post(
            f"/api/pipelines/{pid}/hide-port",
            json={"direction": "input", "var_type": "RawSignal"},
        )

        graph = client.get(f"/api/pipeline?pipeline_id={pid}").json()
        labels = {
            (n["type"], n["data"]["label"])
            for n in graph["nodes"]
            if n["type"] in ("variableNode", "functionNode")
        }
        assert ("variableNode", "RawSignal") in labels
        assert ("functionNode", "bandpass_filter") in labels

    def test_hide_is_scoped_to_its_own_pipeline(self, client):
        """A hide toggled on one scope doesn't retroactively change what
        another scope reports for the same type — hide_port is stored
        per (pipeline_id, direction, var_type), not globally."""
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        self._wire_loading_scope(client, pid)

        # Hide RawSignal on 'main' (unrelated wiring) first — must not
        # affect the child scope's own report.
        client.post(
            "/api/pipelines/main/hide-port",
            json={"direction": "input", "var_type": "RawSignal"},
        )
        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface == {"inputs": ["RawSignal"], "outputs": ["FilteredSignal"]}

        # Hiding it on the child itself filters the child's own report,
        # while main's earlier (independent) hide stays recorded too.
        client.post(
            f"/api/pipelines/{pid}/hide-port",
            json={"direction": "input", "var_type": "RawSignal"},
        )
        iface = client.get(f"/api/pipelines/{pid}/interface").json()
        assert iface == {"inputs": [], "outputs": ["FilteredSignal"]}

        db = get_db()
        by_scope = ps.get_hidden_ports_by_scope(db)
        assert by_scope["main"] == {"input": {"RawSignal"}, "output": set()}
        assert by_scope[pid] == {"input": {"RawSignal"}, "output": set()}


# ---------------------------------------------------------------------------
# Checkpoint 3: execution rearchitecture (G2 — document -> backend pipelines)
# ---------------------------------------------------------------------------


class TestExecutionCompiler:
    def test_derive_fn_targets_from_db_history(self, client):
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import derive_fn_targets

        targets = derive_fn_targets(get_db(), "bandpass_filter")

        assert len(targets) == 1
        assert targets[0]["constants"] == {"low_hz": 20}
        assert targets[0]["output_type"] == "FilteredSignal"

    def test_derive_fn_targets_unknown_fn_is_empty(self, client):
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import derive_fn_targets

        assert derive_fn_targets(get_db(), "no_such_fn") == []


class TestDeriveTargetForNode:
    """Regression coverage: clicking Run on a SPECIFIC function node must
    execute THAT node's own wiring, never a different node's real DB
    history just because they share a function name. Found via a real GUI
    session: a manual bandpass_filter node wired to a different input
    variable silently re-ran the already-executed bandpass_filter(RawSignal)
    call site instead of its own wiring — derive_fn_targets resolves by
    function NAME across every node sharing that name, with no way to
    distinguish which node the user actually clicked. derive_target_for_node
    is the node-scoped alternative that fixes this."""

    def test_manual_node_derives_its_own_wiring_not_a_different_call_sites(self, client):
        import numpy as np
        from scidb import BaseVariable

        class OtherSignal2(BaseVariable):
            pass

        class OtherFiltered2(BaseVariable):
            pass

        OtherSignal2.save(np.zeros(5), subject=1, session="pre")

        client.put("/api/layout/mv_o_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "OtherSignal2",
        })
        client.put("/api/layout/mf_bp_other", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter",
        })
        client.put("/api/layout/mv_o_out", json={
            "x": 20, "y": 0, "node_type": "variableNode", "label": "OtherFiltered2",
        })
        client.put("/api/edges/e_o_in", json={
            "source": "mv_o_in", "target": "mf_bp_other", "target_handle": "in__signal",
        })
        client.put("/api/edges/e_o_out", json={"source": "mf_bp_other", "target": "mv_o_out"})
        client.put("/api/parameters/low_hz/pending/99")
        client.put("/api/edges/e_o_const", json={
            "source": "param__low_hz", "target": "mf_bp_other", "target_handle": "in__low_hz",
        })

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import derive_target_for_node

        targets = derive_target_for_node(get_db(), "mf_bp_other")

        assert len(targets) == 1
        # A never-run target's input_types values are lists (matching
        # derive_fn_targets' own never-run fallback shape — see
        # edge_resolver.ResolvedEdges.input_types, which supports multi-type/
        # EachOf wiring); only DB-history-derived targets (list_pipeline_
        # variants' rows) use bare strings. run.py already handles both
        # ("type_names may be a list (new) or a string (from DB history)").
        assert targets[0]["input_types"].get("signal") == ["OtherSignal2"]
        assert targets[0]["output_type"] == "OtherFiltered2"
        assert targets[0]["constants"].get("low_hz") == 99, (
            "must use the staged pending value (99), not the real "
            "bandpass_filter(RawSignal) call site's low_hz=20 — this is a "
            "different, never-run wiring"
        )

    def test_never_run_wiring_reuses_known_constant_value_without_staging(self, client):
        """Regression test: a never-run wiring's constant doesn't need to
        be re-staged as a pending value if it's ALREADY a real, known value
        from a different call site of the SAME function — found via a real
        GUI session where wiring the SAME shared window_seconds/
        sample_interval constant nodes (already 30/5 from the real
        compute_rolling_vo2(RawVO2) run) into a new compute_rolling_vo2
        (RawHeartRate) node produced 'wired but has no pending values' and
        for_each failed with missing required arguments — the real,
        already-visible-on-canvas constant value was silently dropped."""
        import numpy as np
        from scidb import BaseVariable

        class OtherSignal3(BaseVariable):
            pass

        class OtherFiltered3(BaseVariable):
            pass

        OtherSignal3.save(np.zeros(5), subject=1, session="pre")

        client.put("/api/layout/mv_o3_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "OtherSignal3",
        })
        client.put("/api/layout/mf_bp_other3", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter",
        })
        client.put("/api/layout/mv_o3_out", json={
            "x": 20, "y": 0, "node_type": "variableNode", "label": "OtherFiltered3",
        })
        client.put("/api/edges/e_o3_in", json={
            "source": "mv_o3_in", "target": "mf_bp_other3", "target_handle": "in__signal",
        })
        client.put("/api/edges/e_o3_out", json={"source": "mf_bp_other3", "target": "mv_o3_out"})
        # Wire the REAL, already-known low_hz constant (value 20, from the
        # populated_db fixture's real bandpass_filter(RawSignal) run) —
        # deliberately WITHOUT staging any pending value for it.
        client.put("/api/edges/e_o3_const", json={
            "source": "param__low_hz", "target": "mf_bp_other3", "target_handle": "in__low_hz",
        })

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import derive_target_for_node

        targets = derive_target_for_node(get_db(), "mf_bp_other3")

        assert len(targets) == 1
        assert targets[0]["input_types"].get("signal") == ["OtherSignal3"]
        assert targets[0]["output_type"] == "OtherFiltered3"
        assert targets[0]["constants"].get("low_hz") == 20, (
            "must reuse the real, already-known low_hz=20 value from the "
            "original bandpass_filter(RawSignal) call site, not drop the "
            "constant just because it was never staged as pending"
        )

    def test_never_run_wiring_falls_back_to_source_declared_constant_default(
        self, client
    ):
        """A constant that's genuinely brand new — never staged as pending
        AND never appearing in any real DB history for this function — must
        still produce a runnable target if it has a source-declared
        ``scidb.Parameter(...)`` default, instead of silently dropping the
        arg (the same 'wired but has no pending values' gap the previous
        two tests cover for the staged/known-DB-value cases, now covered
        for the source-default case)."""
        import numpy as np
        from scidb import BaseVariable, Parameter

        from scistack_gui import registry as _registry

        class OtherSignal4(BaseVariable):
            pass

        class OtherFiltered4(BaseVariable):
            pass

        OtherSignal4.save(np.zeros(5), subject=1, session="pre")

        # A source-declared constant that has NEVER been run anywhere —
        # no DB history, no staged pending value.
        _registry._register_parameter(
            "brand_new_gain", Parameter(7, description="test default"), source="test"
        )
        try:
            client.put("/api/layout/mv_o4_in", json={
                "x": 0, "y": 0, "node_type": "variableNode", "label": "OtherSignal4",
            })
            client.put("/api/layout/mf_bp_other4", json={
                "x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter",
            })
            client.put("/api/layout/mv_o4_out", json={
                "x": 20, "y": 0, "node_type": "variableNode", "label": "OtherFiltered4",
            })
            client.put("/api/edges/e_o4_in", json={"source": "mv_o4_in", "target": "mf_bp_other4"})
            client.put("/api/edges/e_o4_out", json={"source": "mf_bp_other4", "target": "mv_o4_out"})
            client.put("/api/edges/e_o4_const", json={
                "source": "param__brand_new_gain", "target": "mf_bp_other4",
                "target_handle": "in__brand_new_gain",
            })

            from scistack_gui.db import get_db
            from scistack_gui.services.execution_service import derive_target_for_node

            targets = derive_target_for_node(get_db(), "mf_bp_other4")

            assert len(targets) == 1
            assert targets[0]["constants"].get("brand_new_gain") == 7, (
                "must fall back to the source-declared default (7) instead "
                "of dropping the constant entirely"
            )
        finally:
            _registry._parameters.pop("brand_new_gain", None)
            _registry._parameter_sources.pop("brand_new_gain", None)

    def test_graduated_node_derives_only_its_own_call_site(self, client):
        """The flip side: an already-graduated node's own wiring must
        resolve to ITS real DB history, not get confused by an unrelated
        manual node sharing the same label."""
        from scistack_gui import node_wiring
        from scistack_gui.db import get_db
        from scistack_gui.domain.graph_builder import wiring_id
        from scistack_gui.services.execution_service import derive_target_for_node

        # The node's id is allocated, not derived, so it is LOOKED UP by the
        # wiring rather than spelled (docs/claude/node-identity.md).
        real_wid = wiring_id("bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {})
        node_wiring.ensure_tables(get_db())
        real_node_id = node_wiring.node_for_wiring(get_db(), real_wid)
        if real_node_id is None:
            real_node_id = str(node_wiring.mint_node_id("bandpass_filter"))
            node_wiring.record(get_db(), real_node_id, real_wid)

        targets = derive_target_for_node(get_db(), real_node_id)

        assert len(targets) == 1
        assert targets[0]["input_types"].get("signal") == "RawSignal"
        assert targets[0]["output_type"] == "FilteredSignal"
        assert targets[0]["constants"] == {"low_hz": 20}

    def test_reconnect_different_variable_on_graduated_node_derives_substituted_target(
        self, client
    ):
        """Regression: hiding a graduated node's input edge then manually
        reconnecting a DIFFERENT variable to the same handle must produce a
        runnable target with the new variable substituted in, not [] — the
        'stuck disconnected forever' bug found via a real GUI session (the
        node kept showing 🔌 disconnected and Run kept failing with
        "input 'signal' is disconnected" even after the reconnect)."""
        import numpy as np
        from scidb import BaseVariable

        from scistack_gui import layout as layout_store
        from scistack_gui import pipeline_store
        from scistack_gui.db import get_db
        from scistack_gui import node_wiring
        from scistack_gui.domain.graph_builder import wiring_id
        from scistack_gui.services.execution_service import derive_target_for_node

        class OtherSignal7(BaseVariable):
            pass

        OtherSignal7.save(np.zeros(5), subject=1, session="pre")

        db = get_db()
        # The node id is allocated, and the hidden-edge id is built FROM it —
        # both sides of that lookup are keyed by the node, not by the wiring
        # (docs/claude/node-identity.md).
        real_wid = wiring_id("bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {})
        node_wiring.ensure_tables(db)
        real_node_id = node_wiring.node_for_wiring(db, real_wid)
        if real_node_id is None:
            real_node_id = str(node_wiring.mint_node_id("bandpass_filter"))
            node_wiring.record(db, real_node_id, real_wid)
        real_token = real_node_id.split("__", 2)[2]

        pipeline_store.hide_edge(
            db,
            f"e__RawSignal__bandpass_filter__{real_token}",
            "var__RawSignal",
            real_node_id,
            None,
            "in__signal",
            "main",
        )
        layout_store.write_manual_edge(
            {
                "id": "manual__reconnect7",
                "source": "var__OtherSignal7",
                "target": real_node_id,
                "targetHandle": "in__signal",
            }
        )

        targets = derive_target_for_node(db, real_node_id)

        assert len(targets) == 1
        assert targets[0]["input_types"].get("signal") == "OtherSignal7"
        assert targets[0]["output_type"] == "FilteredSignal"
        assert targets[0]["constants"] == {"low_hz": 20}

    def test_compile_root_scope(self, client):
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            _discard_compiled,
            build_backend_pipeline,
        )

        built: dict = {}
        pipe = build_backend_pipeline(get_db(), "main", built)
        try:
            assert [s.name for s in pipe.steps] == ["bandpass_filter"]
            iface = pipe.interface()
            assert [c.__name__ for c in iface["inputs"]] == ["RawSignal"]
            assert [c.__name__ for c in iface["outputs"]] == ["FilteredSignal"]
        finally:
            _discard_compiled(built)

    def test_plan_endpoint_green_after_seed_run(self, client):
        r = client.get("/api/pipelines/main/plan")
        assert r.status_code == 200
        entries = r.json()
        by_name = {e["step"]: e for e in entries}
        assert by_name["bandpass_filter"]["state"] == "green"
        assert by_name["bandpass_filter"]["endpoint"] is False
        assert by_name["bandpass_filter"]["pipeline"] == "main"
        assert by_name["bandpass_filter"]["n_combos"] == 4

    def test_composed_plan_crosses_scopes(self, client):
        """Move the bandpass call site into a sub scope, use it from main:
        main's plan resolves the step inside the used pipeline."""
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()[
            "pipeline_id"
        ]
        # Membership = where the position is saved (drag onto sub canvas).
        graph = client.get("/api/pipeline").json()
        fn_id = next(n["id"] for n in graph["nodes"] if n["type"] == "functionNode")
        client.put(f"/api/layout/{fn_id}", json={"x": 0, "y": 0, "pipeline_id": pid})
        client.post("/api/pipelines/main/uses", json={"child_pipeline_id": pid})

        entries = client.get("/api/pipelines/main/plan").json()

        by_name = {e["step"]: e for e in entries}
        assert by_name["bandpass_filter"]["pipeline"] == "loading"
        assert by_name["bandpass_filter"]["state"] == "green"

    def test_run_pipeline_skips_current_steps(self, client):
        """Synchronous run through the compiler: the seed already ran
        bandpass fully, so skip_computed leaves zero new invocations."""
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import run_pipeline

        db = get_db()
        n_inv_before = db._duck._fetchall("SELECT COUNT(*) FROM _invocation")[0][0]

        result = run_pipeline(db, "main", mode="until", target="bandpass_filter")

        assert result["ok"] is True
        n_inv_after = db._duck._fetchall("SELECT COUNT(*) FROM _invocation")[0][0]
        assert n_inv_after == n_inv_before  # all combos skipped

    def test_run_endpoint_validation(self, client):
        r = client.post("/api/pipelines/main/run", json={"mode": "until", "target": ""})
        assert r.status_code == 400
        r = client.post("/api/pipelines/main/run", json={"mode": "bogus"})
        assert r.status_code == 400

    def test_compiled_pipelines_are_discarded(self, client):
        from scidb.pipeline import _all_pipelines
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import plan_pipeline

        before = len(_all_pipelines)
        plan_pipeline(get_db(), "main")
        assert len(_all_pipelines) == before  # transient compiles cleaned

    def test_compiled_steps_iterate_schema_grid(self, client):
        """Compiled steps must carry the FULL schema grid as explicit
        iterables — without them for_each pools every schema row into ONE
        call and per-combo functions crash on multi-row tables (regression
        found via gui_test_data, 2026-07-18). Explicit iterables (not
        schema_level) so binding `iterate` overrides compose per key."""
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            _discard_compiled,
            build_backend_pipeline,
        )

        built: dict = {}
        pipe = build_backend_pipeline(get_db(), "main", built)
        try:
            spec = pipe.steps[0]
            assert set(spec.metadata_iterables) == {"subject", "session"}
            assert len(spec.metadata_iterables["subject"]) == 2
            assert sorted(spec.metadata_iterables["session"]) == ["post", "pre"]
        finally:
            _discard_compiled(built)

    def test_compile_applies_pending_constant_override(self, client):
        """Staged pending values override DB history at COMPILE time (Stage
        2 of the wiring-grouped plan) — the same Strategy-2 semantics as the
        eager run thread, via the shared apply_pending_overrides helper."""
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            _discard_compiled,
            build_backend_pipeline,
        )

        client.put("/api/parameters/low_hz/pending/42")

        built: dict = {}
        pipe = build_backend_pipeline(get_db(), "main", built)
        try:
            spec = pipe.steps[0]
            assert spec.inputs["low_hz"] == 42  # literal_eval'd, not "42"
        finally:
            _discard_compiled(built)
            client.delete("/api/parameters/low_hz/pending/42")

    def test_compile_excludes_hidden_combo(self, client):
        """A hidden combo must never compile into a pipeline step -- Run
        Pipeline and plan-preview both go through build_backend_pipeline,
        which previously never consulted the hidden set at all (see
        plan-combo-hiding.md)."""
        from scistack_gui import pipeline_store
        from scistack_gui.db import get_db
        from scistack_gui.ids import fn_node_id
        from scistack_gui.services.execution_service import (
            _discard_compiled,
            build_backend_pipeline,
            resolve_combo_call_ids,
        )

        db = get_db()
        call_ids = resolve_combo_call_ids(db, "bandpass_filter", None, {"low_hz": "20"})
        assert call_ids, "expected the seeded low_hz=20 combo to resolve to a real call_id"
        for cid in call_ids:
            pipeline_store.hide_combo(
                db, fn_node_id("bandpass_filter", cid), "bandpass_filter", {"low_hz": "20"}
            )

        built: dict = {}
        pipe = build_backend_pipeline(db, "main", built)
        try:
            assert "bandpass_filter" not in [s.name for s in pipe.steps]
        finally:
            _discard_compiled(built)

    def test_plan_previews_staged_variant_as_red(self, client):
        """The plan dialog shows what materializing the staged value will
        run: the overridden variant has no records yet -> red, full grid."""
        client.put("/api/parameters/low_hz/pending/42")
        try:
            entries = client.get("/api/pipelines/main/plan").json()
            by_name = {e["step"]: e for e in entries}
            assert by_name["bandpass_filter"]["state"] == "red"
            assert by_name["bandpass_filter"]["n_combos"] == 4
        finally:
            client.delete("/api/parameters/low_hz/pending/42")

    def test_pull_run_materializes_staged_value_and_pending_clears(self, client):
        """The full Stage-2 loop: stage a value -> pull run writes the
        staged variant's records -> next graph build auto-cleans the
        pending value and the node returns to green."""
        from scistack_gui import layout as layout_store
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import run_pipeline

        client.put("/api/parameters/low_hz/pending/42")

        result = run_pipeline(get_db(), "main", mode="until", target="bandpass_filter")

        entry = result["report"][0]
        assert entry["failed"] == 0
        assert entry["completed"] == 4  # staged variant: full grid computed

        # The staged variant now exists in DB history.
        constants_seen = {
            v["constants"].get("low_hz")
            for v in get_db().list_pipeline_variants()
            if v["function_name"] == "bandpass_filter"
        }
        assert 42 in constants_seen

        # Next graph build: pending auto-cleans, node green with both
        # variant chips.
        nodes = client.get("/api/pipeline").json()["nodes"]
        assert "42" not in layout_store.get_pending_constants().get("low_hz", set())
        node = next(
            n
            for n in nodes
            if n.get("type") == "functionNode"
            and n["data"]["label"] == "bandpass_filter"
        )
        assert node["data"]["run_state"] == "green"
        chip_values = {v["constants"].get("low_hz") for v in node["data"]["variants"]}
        assert chip_values == {20, 42}

    def test_run_pipeline_returns_step_report(self, client):
        """run_pipeline surfaces the backend's last_run_report so the run
        thread can report honest success (iteration failures never raise
        out of for_each)."""
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import run_pipeline

        result = run_pipeline(get_db(), "main", mode="until", target="bandpass_filter")

        assert [e["step"] for e in result["report"]] == ["bandpass_filter"]
        entry = result["report"][0]
        assert entry["failed"] == 0
        # Seed already ran everything → memoized skip, nothing re-computed.
        assert entry["completed"] == 0

    def test_compile_gives_each_sibling_wiring_its_own_step(self, client):
        """Regression: build_backend_pipeline (Run Pipeline / Run Until
        Here) must compile ONE step per WIRING, not per function name.
        Found via a real GUI session: two bandpass_filter nodes wired to
        different signals shared the compiled pipeline's single
        'bandpass_filter' step, which derived targets by function name
        (derive_fn_targets — every wiring sharing that name) instead of by
        the exact node (derive_target_for_node). Running the pipeline
        re-ran the SIBLING wiring's own real DB history under the hood and
        resurrected a stale node for it on the next graph build — the same
        no-blur bug TestDeriveTargetForNode fixed for single-node Run,
        just unfixed one layer up in the pipeline compiler."""
        import numpy as np
        from scidb import BaseVariable

        class OtherSignal5(BaseVariable):
            pass

        class OtherFiltered5(BaseVariable):
            pass

        OtherSignal5.save(np.zeros(5), subject=1, session="pre")

        client.put("/api/layout/mv_o5_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "OtherSignal5",
        })
        client.put("/api/layout/mf_bp_other5", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter",
        })
        client.put("/api/layout/mv_o5_out", json={
            "x": 20, "y": 0, "node_type": "variableNode", "label": "OtherFiltered5",
        })
        client.put(
            "/api/edges/e_o5_in",
            json={
                "source": "mv_o5_in",
                "target": "mf_bp_other5",
                "target_handle": "in__signal",
            },
        )
        client.put(
            "/api/edges/e_o5_out", json={"source": "mf_bp_other5", "target": "mv_o5_out"}
        )
        client.put("/api/edges/e_o5_const", json={
            "source": "param__low_hz",
            "target": "mf_bp_other5",
            "target_handle": "in__low_hz",
        })

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            _discard_compiled,
            build_backend_pipeline,
        )

        built: dict = {}
        pipe = build_backend_pipeline(get_db(), "main", built)
        try:
            bp_steps = [s for s in pipe.steps if s.name == "bandpass_filter"]
            assert len(bp_steps) == 2, (
                "expected one compiled step per wiring (RawSignal and "
                f"OtherSignal5), got {len(bp_steps)}"
            )
            signals = {s.inputs["signal"].__name__ for s in bp_steps}
            assert signals == {"RawSignal", "OtherSignal5"}, (
                "each step must carry its OWN wiring's input — one must "
                f"never borrow the other's, got {signals}"
            )
        finally:
            _discard_compiled(built)


class TestPathInputExecutionResolution:
    """Regression coverage: a PathInput-backed function param is resolved in
    build_run_inputs, not in derive_fn_targets/derive_target_for_node — a
    PathInput is never a citizen of input_types or DB variant history (it
    resolves files, not a versioned variable), so neither branch of target
    derivation ever constructs one. Before that fix, NEITHER the never-run
    fallback NOR an already-run DB-history target ever built a live
    scifor.PathInput anywhere in scistack-gui — a PathInput-driven function
    couldn't be run through the GUI at all, first run or re-run. api/run.py
    and execution_service.build_backend_pipeline used to carry two
    independently-drifting copies of the input-building logic; both now call
    the one shared execution_service.build_run_inputs.

    The binding itself comes from the target's PathInput ``bindings``, built
    by the WIRING — originally it was a name match between the PathInput's
    declared name and the signature param, which silently resolved nothing
    whenever the two differed (see TestEdgeDrivenBinding)."""

    @staticmethod
    def _target(output_type: str, path_input_params: "dict | None" = None):
        """A target binding ``filepath`` to *path_input_params*
        (``{param_name: declared_name}``, defaulting to nothing wired)."""
        return {
            "bindings": {
                param: {"kind": "pathinput", "ref": decl}
                for param, decl in (path_input_params or {}).items()
            },
            "input_types": {},
            "output_type": output_type,
            "constants": {},
        }

    @staticmethod
    def _register_loader(name: str):
        """Append a real function definition to the configured
        variable_file, rather than injecting into registry._functions
        directly — create_path_input/create_parameter now call
        registry.refresh_module(), which re-scans that ONE file from
        scratch and would otherwise wipe a dict-injected function."""
        from scistack_gui import registry

        target = registry._module_path
        with open(target, "a") as f:
            f.write(f"\n\ndef {name}(filepath):\n    return str(filepath)\n")
        registry.refresh_module()

    def test_never_run_target_omits_pathinput_from_input_types(
        self, client_with_variable_file
    ):
        """derive_fn_targets deliberately does NOT resolve PathInput params
        — see its docstring — build_run_inputs is the single place that
        does, right before execution."""
        from scidb import BaseVariable

        class LoadedThing(BaseVariable):
            pass

        client = client_with_variable_file
        self._register_loader("load_raw")
        client.post(
            "/api/path-inputs",
            json={"name": "filepath", "template": "{subject}/{session}/data.csv"},
        )
        client.put("/api/layout/mf_load", json={
            "x": 0, "y": 0, "node_type": "functionNode", "label": "load_raw",
        })
        client.put("/api/layout/mv_loaded", json={
            "x": 10, "y": 0, "node_type": "variableNode", "label": "LoadedThing",
        })
        client.put(
            "/api/edges/e_load_out", json={"source": "mf_load", "target": "mv_loaded"}
        )

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import derive_fn_targets

        targets = derive_fn_targets(get_db(), "load_raw")

        assert len(targets) == 1
        assert "filepath" not in targets[0]["input_types"]
        assert targets[0]["output_type"] == "LoadedThing"

    def test_build_run_inputs_resolves_never_run_target(
        self, client_with_variable_file
    ):
        from scidb import BaseVariable, PathInput

        class LoadedThing2(BaseVariable):
            pass

        client = client_with_variable_file
        self._register_loader("load_raw2")
        client.post(
            "/api/path-inputs",
            json={"name": "filepath", "template": "{subject}/{session}/data.csv"},
        )
        client.put("/api/layout/mf_load2", json={
            "x": 0, "y": 0, "node_type": "functionNode", "label": "load_raw2",
        })
        client.put("/api/layout/mv_loaded2", json={
            "x": 10, "y": 0, "node_type": "variableNode", "label": "LoadedThing2",
        })
        client.put(
            "/api/edges/e_load2_out", json={"source": "mf_load2", "target": "mv_loaded2"}
        )
        client.put("/api/edges/e_load2_in", json={
            "source": "pathInput__filepath",
            "target": "mf_load2",
            "target_handle": "in__filepath",
        })

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            build_run_inputs,
            derive_fn_targets,
        )

        targets = derive_fn_targets(get_db(), "load_raw2")
        inputs = build_run_inputs(targets[0], "load_raw2")

        assert isinstance(inputs["filepath"], PathInput)
        assert inputs["filepath"].path_template == "{subject}/{session}/data.csv"

    def test_compiled_pipeline_step_carries_live_pathinput(
        self, client_with_variable_file
    ):
        """Same resolution, exercised through the real compiler path
        (Run Pipeline / Run Until Here) rather than calling
        build_run_inputs directly."""
        from scidb import BaseVariable, PathInput

        class LoadedThing3(BaseVariable):
            pass

        client = client_with_variable_file
        self._register_loader("load_raw3")
        client.post("/api/path-inputs", json={"name": "filepath", "template": "{subject}.csv"})
        client.put("/api/layout/mf_load3", json={
            "x": 0, "y": 0, "node_type": "functionNode", "label": "load_raw3",
        })
        client.put("/api/layout/mv_loaded3", json={
            "x": 10, "y": 0, "node_type": "variableNode", "label": "LoadedThing3",
        })
        client.put(
            "/api/edges/e_load3_out", json={"source": "mf_load3", "target": "mv_loaded3"}
        )
        client.put("/api/edges/e_load3_in", json={
            "source": "pathInput__filepath",
            "target": "mf_load3",
            "target_handle": "in__filepath",
        })

        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import (
            _discard_compiled,
            build_backend_pipeline,
        )

        built: dict = {}
        pipe = build_backend_pipeline(get_db(), "main", built)
        try:
            step = next(s for s in pipe.steps if s.name == "load_raw3")
            assert isinstance(step.inputs["filepath"], PathInput)
        finally:
            _discard_compiled(built)

    def test_missing_stored_pathinput_leaves_param_unresolved(
        self, client_with_variable_file
    ):
        """Nothing wired to the param -> fail safe (the param is simply
        absent from inputs), matching the KeyError-tolerant contract callers
        already rely on for other unresolvable params — not a crash inside
        build_run_inputs."""
        self._register_loader("load_raw4")

        from scistack_gui.services.execution_service import build_run_inputs

        target = self._target("LoadedThing4")
        inputs = build_run_inputs(target, "load_raw4")

        assert "filepath" not in inputs

    def test_alternate_templates_resolve_to_eachof(self, client_with_variable_file):
        """Multiple templates under one PathInput name become
        EachOf(PathInput(...), ...) — the PathInput analog of the
        multi-type variable branch's EachOf, same as a Constant node's
        multiple staged values fanning out. "Alternate templates" is now
        purely a source-code concept (EachOf(PathInput(...), ...) bound to
        one name) — no GUI/API endpoint authors it (see
        docs/claude/code-discovery-categories.md), so this writes the
        declaration directly rather than going through the API."""
        from scidb import EachOf, PathInput
        from scistack_gui import registry

        self._register_loader("load_raw5")
        target = registry._module_path
        with open(target, "a") as f:
            f.write(
                '\nfilepath = EachOf(PathInput("primary.csv"), '
                'PathInput("alt.csv", root_folder="/alt"))\n'
            )
        registry.refresh_module()

        from scistack_gui.services.execution_service import build_run_inputs

        target = self._target("LoadedThing5", {"filepath": "filepath"})
        inputs = build_run_inputs(target, "load_raw5")

        assert isinstance(inputs["filepath"], EachOf)
        alts = inputs["filepath"].alternatives
        assert len(alts) == 2
        assert all(isinstance(a, PathInput) for a in alts)
        assert alts[0].path_template == "primary.csv"
        assert alts[1].path_template == "alt.csv"
        assert alts[1].root_folder is not None and str(alts[1].root_folder) == "/alt"

    def test_single_template_stays_plain_pathinput(self, client_with_variable_file):
        """No alternates -> still a bare PathInput, not EachOf-wrapped —
        confirms the single-template case is unaffected by this feature."""
        from scidb import EachOf, PathInput

        client = client_with_variable_file
        self._register_loader("load_raw6")
        client.post(
            "/api/path-inputs", json={"name": "filepath", "template": "solo.csv"}
        )

        from scistack_gui.services.execution_service import build_run_inputs

        target = self._target("LoadedThing6", {"filepath": "filepath"})
        inputs = build_run_inputs(target, "load_raw6")

        assert isinstance(inputs["filepath"], PathInput)
        assert not isinstance(inputs["filepath"], EachOf)
