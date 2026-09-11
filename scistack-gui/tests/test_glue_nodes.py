"""
GUI glue nodes — Stage 5 of the glue-node feature.

A glue node is a **function-node variant**, not a new kind of thing: same
``in__{param}`` / ``out__`` handles, so edge resolution gains one source case
and nothing else. What it is not is a *step* — no run button, no run state,
and never a ``StepSpec`` in the compiled pipeline (D5).

Covers:
- the handle-id contract (the ids the frontend renders are the ids the
  backend resolves), in the style of TestHandleIdsMatchTheFrontend
- edge resolution: a glued param binds to whatever is at the HEAD of the
  chain — variable, Parameter or PathInput — with the chain riding alongside;
  glue→glue chains; cycles; unwired glue
- ``build_backend_pipeline`` produces the same step count with and without
  glue on the canvas
- the per-node Run route refuses a glue node instead of reporting a
  do-nothing success
- the code panel round-trip: create → edit → file written → registry
  refreshed → the consuming function's glue hash changes
"""

from pathlib import Path

from scistack_gui.domain.edge_resolver import (
    GLUE_NODE_TYPE,
    resolve_function_edges,
    resolve_glue_chain,
)

# ---------------------------------------------------------------------------
# Fixtures for the pure edge-resolution tests (no I/O)
# ---------------------------------------------------------------------------
FN_IDS = {"fn__analyze"}


def _edge(eid, source, target, target_handle=None, source_handle=None):
    return {
        "id": eid,
        "source": source,
        "target": target,
        "targetHandle": target_handle,
        "sourceHandle": source_handle,
    }


def _glue_node(label):
    return {"type": GLUE_NODE_TYPE, "label": label}


class TestHandleIdContract:
    """The ids GlueNode.tsx renders must be the ids the resolver parses.

    React Flow silently drops an edge whose handle does not exist, so a
    mismatch here is invisible on the canvas and produces a node that is
    wired in the DB and disconnected on screen.
    """

    def test_input_handles_use_the_function_node_prefix(self):
        tsx = (
            Path(__file__).parent.parent
            / "frontend/src/components/DAG/GlueNode.tsx"
        ).read_text()
        assert "`in__${param}`" in tsx
        assert 'id="out__"' in tsx

    def test_glue_node_type_string_matches_the_frontend(self):
        tsx = (
            Path(__file__).parent.parent
            / "frontend/src/components/DAG/PipelineDAG.tsx"
        ).read_text()
        assert f"{GLUE_NODE_TYPE}: GlueNode" in tsx


class TestEdgeResolution:
    def test_a_glued_param_binds_to_the_upstream_variable(self):
        manual_nodes = {"g1": _glue_node("glue_drop_baseline")}
        edges = [
            _edge("e1", "var__RawEMG", "g1", "in__value"),
            _edge("e2", "g1", "fn__analyze", "in__emg"),
        ]
        resolved = resolve_function_edges(FN_IDS, edges, manual_nodes, {})

        # The binding is the VARIABLE; the glue rides alongside. Glue
        # interposes on a binding — it is never a binding of its own,
        # because it has no saved output for anything to bind to.
        assert resolved.input_types == {"emg": ["RawEMG"]}
        assert resolved.glue_chains == {"emg": ["glue_drop_baseline"]}

    def test_glue_feeding_glue_is_one_chain_in_application_order(self):
        manual_nodes = {"g1": _glue_node("glue_a"), "g2": _glue_node("glue_b")}
        edges = [
            _edge("e1", "var__RawEMG", "g1", "in__value"),
            _edge("e2", "g1", "g2", "in__value"),
            _edge("e3", "g2", "fn__analyze", "in__emg"),
        ]
        resolved = resolve_function_edges(FN_IDS, edges, manual_nodes, {})

        assert resolved.glue_chains == {"emg": ["glue_a", "glue_b"]}
        assert resolved.input_types == {"emg": ["RawEMG"]}

    def test_an_unwired_glue_node_binds_nothing(self):
        manual_nodes = {"g1": _glue_node("glue_x")}
        edges = [_edge("e1", "g1", "fn__analyze", "in__emg")]
        resolved = resolve_function_edges(FN_IDS, edges, manual_nodes, {})

        # The chain is known but nothing is wired behind it, so the parameter
        # stays unbound rather than binding to nothing. This is now the ONLY
        # reason a glued parameter goes unbound — see TestGlueChainHeadKinds.
        assert resolved.glue_chains == {"emg": ["glue_x"]}
        assert "emg" not in resolved.bindings

    def test_a_glue_cycle_is_truncated_not_hung(self):
        manual_nodes = {"g1": _glue_node("glue_a"), "g2": _glue_node("glue_b")}
        edges = [
            _edge("e1", "g2", "g1", "in__value"),
            _edge("e2", "g1", "g2", "in__value"),
        ]
        chain, head = resolve_glue_chain("g1", edges, manual_nodes, {})
        assert head is None
        assert len(chain) <= 2

    def test_no_glue_leaves_the_chain_map_empty(self):
        edges = [_edge("e1", "var__RawEMG", "fn__analyze", "in__emg")]
        resolved = resolve_function_edges(FN_IDS, edges, {}, {})
        assert resolved.glue_chains == {}

    def test_a_glue_edge_without_a_handle_is_dropped(self):
        manual_nodes = {"g1": _glue_node("glue_x")}
        edges = [
            _edge("e1", "var__RawEMG", "g1", "in__value"),
            _edge("e2", "g1", "fn__analyze", None),
        ]
        resolved = resolve_function_edges(FN_IDS, edges, manual_nodes, {})
        assert resolved.bindings == {}
        assert resolved.glue_chains == {}


class TestGlueChainHeadKinds:
    """A glue chain may start from ANY bindable source, not only a variable.

    The regression this class exists for (2026-09-10): ``Parameter -> glue ->
    fn`` resolved its head to ``None``, so ``resolve_function_edges`` skipped
    the binding entirely and the parameter simply vanished from the function's
    inputs. In a MATLAB run the remaining arguments then bound by POSITION —
    ``filterDelsys(loaded_data, config, Fs)`` ran as
    ``filterDelsys(loaded_data, Fs)``.
    """

    def test_a_parameter_fed_glue_binds_the_parameter(self):
        manual_nodes = {"g1": _glue_node("glue_config_filter")}
        edges = [
            _edge("e1", "param__delsys_config", "g1", "in__config"),
            _edge("e2", "g1", "fn__analyze", "in__config"),
        ]
        resolved = resolve_function_edges(FN_IDS, edges, manual_nodes, {})

        assert resolved.parameter_params == {"config": "delsys_config"}
        assert resolved.glue_chains == {"config": ["glue_config_filter"]}
        # And it must NOT masquerade as a variable input.
        assert resolved.input_types == {}

    def test_a_manual_parameter_node_head_binds_by_its_label(self):
        manual_nodes = {
            "g1": _glue_node("glue_config_filter"),
            "p1": {"type": "parameterNode", "label": "delsys_config"},
        }
        edges = [
            _edge("e1", "p1", "g1", "in__config"),
            _edge("e2", "g1", "fn__analyze", "in__config"),
        ]
        resolved = resolve_function_edges(FN_IDS, edges, manual_nodes, {})
        assert resolved.parameter_params == {"config": "delsys_config"}

    def test_a_pathinput_fed_glue_binds_and_leaves_scidb_to_refuse_it(self):
        """§7 scopes PathInput glue out, but the refusal belongs to scidb.

        Dropping the binding here made an unsupported wiring look like a
        missing argument. Binding it means ``refuse_pathinput_glue`` raises
        ``GlueUnsupportedInputError`` with a message that names the problem.
        """
        manual_nodes = {"g1": _glue_node("glue_x")}
        edges = [
            _edge("e1", "pathInput__raw_files", "g1", "in__value"),
            _edge("e2", "g1", "fn__analyze", "in__path"),
        ]
        resolved = resolve_function_edges(FN_IDS, edges, manual_nodes, {})

        assert resolved.path_input_params == {"path": "raw_files"}
        assert resolved.glue_chains == {"path": ["glue_x"]}

    def test_a_glue_chain_from_a_parameter_survives_a_second_glue_hop(self):
        manual_nodes = {"g1": _glue_node("glue_a"), "g2": _glue_node("glue_b")}
        edges = [
            _edge("e1", "param__cfg", "g1", "in__value"),
            _edge("e2", "g1", "g2", "in__value"),
            _edge("e3", "g2", "fn__analyze", "in__config"),
        ]
        resolved = resolve_function_edges(FN_IDS, edges, manual_nodes, {})

        assert resolved.glue_chains == {"config": ["glue_a", "glue_b"]}
        assert resolved.parameter_params == {"config": "cfg"}

    def test_a_stale_input_edge_is_named_with_its_handle(self, caplog):
        """C4: a renamed glue parameter leaves the STALE edge first in the
        list, and therefore the one followed. The warning has to carry the
        handles or the cause is invisible in the log."""
        import logging

        manual_nodes = {"g1": _glue_node("glue_config_filter")}
        edges = [
            _edge("e1", "param__cfg", "g1", "in__value"),  # stale after rename
            _edge("e2", "param__cfg", "g1", "in__config"),
            _edge("e3", "g1", "fn__analyze", "in__config"),
        ]
        with caplog.at_level(logging.WARNING):
            resolve_function_edges(FN_IDS, edges, manual_nodes, {})

        text = caplog.text
        assert "in__value" in text and "in__config" in text


# ---------------------------------------------------------------------------
# Glue is not a step
# ---------------------------------------------------------------------------
class TestGlueIsNotAStep:
    def test_glue_nodes_are_not_scope_function_nodes(self, client, populated_db):
        from scistack_gui import pipeline_store
        from scistack_gui.services.execution_service import _scope_function_node_ids

        before = _scope_function_node_ids(populated_db, "main")
        pipeline_store._upsert_node(
            populated_db, "glue-node-1", GLUE_NODE_TYPE, "glue_x", "main"
        )
        after = _scope_function_node_ids(populated_db, "main")

        assert [n for n in after if n not in before] == [], (
            "a glue node was compiled as a pipeline step"
        )

    def test_build_backend_pipeline_step_count_is_unchanged_by_glue(
        self, client, populated_db
    ):
        from scistack_gui import pipeline_store
        from scistack_gui.services.execution_service import (
            _discard_compiled,
            build_backend_pipeline,
        )

        def _step_count():
            built: dict = {}
            try:
                return len(build_backend_pipeline(populated_db, "main", built).steps)
            finally:
                _discard_compiled(built)

        before = _step_count()
        pipeline_store._upsert_node(
            populated_db, "glue-node-2", GLUE_NODE_TYPE, "glue_y", "main"
        )
        assert _step_count() == before

    def test_the_run_route_refuses_a_glue_node(self, client, populated_db):
        from scistack_gui import pipeline_store

        pipeline_store._upsert_node(
            populated_db, "glue-node-3", GLUE_NODE_TYPE, "glue_z", "main"
        )
        resp = client.post(
            "/api/run",
            json={"function_name": "glue_z", "node_id": "glue-node-3", "variants": []},
        )
        # Refused, not "successfully ran nothing".
        assert resp.status_code == 400
        assert "never run on its own" in resp.json()["detail"]

    def test_the_run_route_refuses_by_name_without_a_node_id(self, client):
        resp = client.post(
            "/api/run", json={"function_name": "glue_orphan", "variants": []}
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# The code panel round-trip
# ---------------------------------------------------------------------------
class TestGluePanelRoundTrip:
    def test_create_writes_the_minimum_that_parses(self, client, glue_project):
        resp = client.post("/api/glue", json={"name": "drop_baseline"})
        body = resp.json()
        assert body["ok"], body
        # The glue_ prefix is applied for the user — it is what makes the
        # node a glue node.
        assert body["name"] == "glue_drop_baseline"
        source = Path(body["path"]).read_text()
        # Two lines. No docstring, no TODO, no comment block.
        assert source == "def glue_drop_baseline(value):\n    return value\n"

    def test_created_glue_is_discovered_with_the_glue_role(self, client, glue_project):
        client.post("/api/glue", json={"name": "drop_baseline"})
        roles = client.get("/api/registry").json()["function_roles"]
        assert roles.get("glue_drop_baseline") == "glue"

    def test_edit_writes_the_file_and_refreshes_the_registry(
        self, client, glue_project
    ):
        client.post("/api/glue", json={"name": "scale"})
        new_body = "def glue_scale(value):\n    return value * 2\n"

        resp = client.put("/api/glue", json={"name": "glue_scale", "source": new_body})
        assert resp.json()["ok"], resp.json()

        # 1. the file changed...
        path = Path(client.get("/api/glue/glue_scale").json()["path"])
        assert path.read_text() == new_body
        # 2. ...and the live registry now holds the NEW body, which is what
        #    makes the consuming function's glue hash move.
        from scistack_gui import registry

        assert registry.get_function("glue_scale")(3) == 6

    def test_editing_the_body_changes_the_glue_hash(self, client, glue_project):
        from scidb.glue import GlueSpec
        from scistack_gui import registry

        client.post("/api/glue", json={"name": "shift"})
        before = GlueSpec(name="glue_shift", fn=registry.get_function("glue_shift")).hash

        client.put(
            "/api/glue",
            json={
                "name": "glue_shift",
                "source": "def glue_shift(value):\n    return value + 1\n",
            },
        )
        after = GlueSpec(name="glue_shift", fn=registry.get_function("glue_shift")).hash

        # This is the end-to-end version of the Stage 2 identity test, and
        # the one a user actually exercises: panel edit → new hash → new
        # virtual record → the consumer recomputes instead of skipping.
        assert before != after

    def test_a_bad_body_is_rejected_and_rolled_back(self, client, glue_project):
        from scistack_gui import registry

        client.post("/api/glue", json={"name": "ok_one"})
        path = Path(client.get("/api/glue/glue_ok_one").json()["path"])
        good = path.read_text()

        resp = client.put(
            "/api/glue", json={"name": "glue_ok_one", "source": "def glue_ok_one(\n"}
        )
        body = resp.json()

        # A syntax error does not surface as a refresh error — discovery
        # captures per-module import failures and carries on — so the save
        # path has to verify the function still resolves, or a broken body
        # would report "Saved" while the node vanished from the registry.
        assert not body["ok"], body
        assert path.read_text() == good, "a rejected save left the file broken"
        assert registry.lookup_function("glue_ok_one") is not None

    def test_delete_removes_the_node_but_never_the_file(
        self, client, glue_project, populated_db
    ):
        from scistack_gui import pipeline_store

        created = client.post("/api/glue", json={"name": "keepme"}).json()
        path = Path(created["path"])
        pipeline_store._upsert_node(
            populated_db, "glue-node-4", GLUE_NODE_TYPE, "glue_keepme", "main"
        )

        resp = client.delete("/api/glue/glue_keepme")
        assert resp.json()["ok"]
        assert resp.json()["removed"] == 1
        # Project ethos: "remove X" means stop showing it, never destroy the
        # user's code.
        assert path.exists()

    def test_creating_a_duplicate_is_refused(self, client, glue_project):
        client.post("/api/glue", json={"name": "once"})
        second = client.post("/api/glue", json={"name": "once"}).json()
        assert not second["ok"]
        assert "already exists" in second["error"]


# ---------------------------------------------------------------------------
# The column list beside the editor
# ---------------------------------------------------------------------------
class TestColumnList:
    def test_a_scalar_variable_reports_one_column_named_after_the_class(
        self, client, populated_db
    ):
        body = client.get(
            "/api/glue/glue_x/columns", params={"variable_type": "RawSignal"}
        ).json()
        assert body["ok"], body
        assert "RawSignal" in body["data_columns"]
        assert set(body["schema_keys"]) == set(populated_db.dataset_schema_keys)

    def test_an_unwired_node_says_so_rather_than_erroring(self, client, populated_db):
        body = client.get("/api/glue/glue_x/columns").json()
        assert not body["ok"]
        assert "not wired" in body["error"]


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
class TestExport:
    def test_python_export_emits_glue_inline_not_as_a_step(self):
        from scistack_gui.services.code_export_service import (
            _glue_comments,
            _py_glue_literal,
        )
        from scidb.glue import GlueSpec

        def glue_drop_baseline(emg):
            return emg

        glue = {"emg": [GlueSpec(name="glue_drop_baseline", fn=glue_drop_baseline)]}

        assert _py_glue_literal(glue) == "{'emg': [glue_drop_baseline]}"
        (comment,) = _glue_comments(glue, "#")
        assert comment.startswith("# 'emg' is reshaped in memory by glue_drop_baseline")
        # Not a for_each of its own.
        assert "for_each" not in comment

    def test_matlab_export_wraps_the_chain_in_a_cell(self):
        from scidb.glue import GlueSpec
        from scistack_gui.services.code_export_service import _matlab_glue_struct

        chain = [
            GlueSpec(name="glue_a", language="matlab", source_text="a"),
            GlueSpec(name="glue_b", language="matlab", source_text="b"),
        ]
        src = _matlab_glue_struct({"emg": chain})
        # Doubled braces: struct('f', {c}) would build a struct ARRAY, one
        # element per chain entry.
        assert src == "struct('emg', {{@glue_a, @glue_b}})"

    def test_no_glue_emits_nothing(self):
        from scistack_gui.services.code_export_service import (
            _glue_comments,
            _matlab_glue_struct,
            _py_glue_literal,
        )

        assert _py_glue_literal(None) == ""
        assert _matlab_glue_struct(None) == ""
        assert _glue_comments(None, "#") == []


class TestMatlabRunCommandCarriesGlue:
    """The generated MATLAB *run* command must fill in ``for_each``'s ``glue``
    option. It never did: the MATLAB half of the feature was built and
    ``+scidb/for_each.m`` accepted ``glue``, but nothing in the command
    generator ever passed one, so every MATLAB run executed with the glue
    silently dropped (2026-09-10)."""

    def test_matlab_glue_becomes_a_handle(self):
        from scistack_gui.api.matlab_command import _format_glue_struct

        src = _format_glue_struct(
            {"emg": [{"name": "glue_a", "language": "matlab", "source_file": "/a.m"}]}
        )
        assert src == "struct('emg', {{@glue_a}})"

    def test_python_glue_becomes_a_located_struct(self):
        """Python glue reaches a MATLAB run only on a constant-fed param,
        where prepare applies it. MATLAB cannot make a handle for it, and the
        bridge has no registry — so it crosses as name + file."""
        from scistack_gui.api.matlab_command import _format_glue_struct

        src = _format_glue_struct(
            {
                "config": [
                    {
                        "name": "glue_config_filter",
                        "language": "python",
                        "source_file": "/p/glue_config_filter.py",
                    }
                ]
            }
        )
        assert "'language', 'python'" in src
        assert "glue_config_filter" in src
        assert "/p/glue_config_filter.py" in src
        # Still a cell-valued field, not a struct array.
        assert src.startswith("struct('config', {{") and src.endswith("}})")

    def test_no_glue_emits_no_option(self):
        from scistack_gui.api.matlab_command import _format_glue_struct

        assert _format_glue_struct(None) == ""
        assert _format_glue_struct({}) == ""

    def test_the_generated_command_contains_the_glue_option(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filterDelsys",
            db_path="/tmp/x.duckdb",
            schema_keys=["subject"],
            variable_inputs={"loaded_data": "RawEMG"},
            output_types=["FilteredEMG"],
            glue={
                "config": [
                    {
                        "name": "glue_config_filter",
                        "language": "python",
                        "source_file": "/p/glue_config_filter.py",
                    }
                ]
            },
        )
        assert "'glue'" in cmd
        assert "glue_config_filter" in cmd


# ---------------------------------------------------------------------------
# scistack.toml plumbing
# ---------------------------------------------------------------------------
def test_glue_dir_is_registered_on_first_create(client, glue_project):
    client.post("/api/glue", json={"name": "first"})
    toml = (glue_project / "scistack.toml").read_text()
    assert "glue_dir" in toml
