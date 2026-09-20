"""
Integration tests for all scistack-gui HTTP API endpoints.

Uses FastAPI's TestClient (backed by a real populated DuckDB) to exercise
every route defined in the application.
"""

import threading
import time

import numpy as np
import pytest
import scistack_gui.db as _gui_db

# Import test variable classes and pipeline function from conftest so we share
# the same class objects (avoids duplicate BaseVariable subclass registrations).
from conftest import (
    FilteredSignal,
    RawSignal,
    bandpass_filter,
    find_fn_node_id_by_label,
    fn_min_state_across_call_sites,
)
from scidb.database import _local
from scistack_gui import layout as layout_store
from scistack_gui import registry as _registry

from scidb import BaseVariable, configure_database, for_each

# ---------------------------------------------------------------------------
# /api/info
# ---------------------------------------------------------------------------


class TestInfo:
    def test_returns_db_name(self, client):
        r = client.get("/api/info")
        assert r.status_code == 200
        assert r.json()["db_name"] == "test.duckdb"


# ---------------------------------------------------------------------------
# /api/schema
# ---------------------------------------------------------------------------


class TestSchema:
    def test_returns_schema_keys(self, client):
        r = client.get("/api/schema")
        assert r.status_code == 200
        data = r.json()
        assert data["keys"] == ["subject", "session"]

    def test_returns_distinct_values(self, client):
        r = client.get("/api/schema")
        values = r.json()["values"]
        assert {str(v) for v in values["subject"]} == {"1", "2"}
        assert {str(v) for v in values["session"]} == {"pre", "post"}


# ---------------------------------------------------------------------------
# /api/registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_returns_functions(self, client):
        r = client.get("/api/registry")
        assert r.status_code == 200
        data = r.json()
        assert "bandpass_filter" in data["functions"]

    def test_returns_variable_classes(self, client):
        r = client.get("/api/registry")
        data = r.json()
        assert "RawSignal" in data["variables"]
        assert "FilteredSignal" in data["variables"]

    def test_lists_are_sorted(self, client):
        r = client.get("/api/registry")
        data = r.json()
        assert data["functions"] == sorted(data["functions"])
        assert data["variables"] == sorted(data["variables"])


# ---------------------------------------------------------------------------
# /api/pipeline
# ---------------------------------------------------------------------------


class TestPipeline:
    def test_returns_200(self, client):
        r = client.get("/api/pipeline")
        assert r.status_code == 200

    def test_has_nodes_and_edges_keys(self, client):
        data = client.get("/api/pipeline").json()
        assert "nodes" in data
        assert "edges" in data

    def test_variable_nodes_present(self, client):
        nodes = client.get("/api/pipeline").json()["nodes"]
        node_ids = {n["id"] for n in nodes}
        assert "var__RawSignal" in node_ids
        assert "var__FilteredSignal" in node_ids

    def test_function_node_present(self, client, bp_node_id):
        nodes = client.get("/api/pipeline").json()["nodes"]
        node_ids = {n["id"] for n in nodes}
        assert bp_node_id in node_ids

    def test_constant_node_present(self, client):
        nodes = client.get("/api/pipeline").json()["nodes"]
        node_ids = {n["id"] for n in nodes}
        assert "param__low_hz" in node_ids

    def test_variable_node_has_total_records(self, client):
        nodes = client.get("/api/pipeline").json()["nodes"]
        raw_node = next(n for n in nodes if n["id"] == "var__RawSignal")
        assert raw_node["data"]["total_records"] == 4  # 2 subjects × 2 sessions

    def test_function_node_has_variants(self, client, bp_node_id):
        nodes = client.get("/api/pipeline").json()["nodes"]
        fn_node = next(n for n in nodes if n["id"] == bp_node_id)
        assert len(fn_node["data"]["variants"]) >= 1
        variant = fn_node["data"]["variants"][0]
        assert "constants" in variant
        assert "input_types" in variant
        assert "output_type" in variant
        # Sanity: the call_id in the node data matches the suffix of the id.
        assert fn_node["data"]["call_id"] == bp_node_id.rsplit("__", 1)[1]

    def test_edges_connect_raw_to_function(self, client, bp_node_id):
        edges = client.get("/api/pipeline").json()["edges"]
        matches = [
            e
            for e in edges
            if e["source"] == "var__RawSignal" and e["target"] == bp_node_id
        ]
        assert len(matches) >= 1

    def test_edges_connect_function_to_filtered(self, client, bp_node_id):
        edges = client.get("/api/pipeline").json()["edges"]
        matches = [
            e
            for e in edges
            if e["source"] == bp_node_id and e["target"] == "var__FilteredSignal"
        ]
        assert len(matches) >= 1

    def test_constant_edge_connects_to_function(self, client, bp_node_id):
        edges = client.get("/api/pipeline").json()["edges"]
        matches = [
            e
            for e in edges
            if e["source"] == "param__low_hz" and e["target"] == bp_node_id
        ]
        assert len(matches) >= 1

    def test_manual_node_appears_in_pipeline(self, client):
        # Add a manual node via the layout API
        client.put(
            "/api/layout/manual__extra_var",
            json={
                "x": 50.0,
                "y": 100.0,
                "node_type": "variableNode",
                "label": "ExtraVar",
            },
        )
        nodes = client.get("/api/pipeline").json()["nodes"]
        node_ids = {n["id"] for n in nodes}
        assert "manual__extra_var" in node_ids

    def test_manual_edge_appears_in_pipeline(self, client):
        client.put(
            "/api/edges/manual_e1",
            json={
                "source": "var__RawSignal",
                "target": "fn__bandpass_filter",
                "source_handle": None,
                "target_handle": None,
            },
        )
        edges = client.get("/api/pipeline").json()["edges"]
        edge_ids = {e["id"] for e in edges}
        assert "manual_e1" in edge_ids

    def test_manual_matlab_function_node_tagged_language(self, client, tmp_path):
        """Regression: manually-placed MATLAB function nodes must have
        ``data.language == "matlab"`` so the VS Code extension intercepts
        start_run and routes to handleMatlabRun instead of the Python
        registry (which doesn't know about MATLAB functions).

        Without this tag, clicking Run on a freshly-placed MATLAB node
        (no DB history yet) fails with:
            "Function '...' not found in registry."
        """
        from scistack_gui import matlab_registry
        from scistack_gui.matlab_parser import MatlabFunctionInfo

        # Stub a MATLAB function into the registry.
        fn_name = "my_matlab_fn"
        m_file = tmp_path / f"{fn_name}.m"
        m_file.write_text("function y = my_matlab_fn(x)\ny=x;\nend\n")
        matlab_registry._matlab_functions[fn_name] = MatlabFunctionInfo(
            name=fn_name,
            file_path=m_file,
            params=["x"],
            source_hash="0" * 64,
        )
        try:
            # Drop a manual function node on the canvas.
            client.put(
                f"/api/layout/manual__{fn_name}",
                json={
                    "x": 0.0,
                    "y": 0.0,
                    "node_type": "functionNode",
                    "label": fn_name,
                },
            )
            nodes = client.get("/api/pipeline").json()["nodes"]
            matches = [n for n in nodes if n["data"]["label"] == fn_name]
            assert len(matches) == 1
            assert matches[0]["data"].get("language") == "matlab"
        finally:
            matlab_registry._matlab_functions.pop(fn_name, None)


# ---------------------------------------------------------------------------
# /api/layout
# ---------------------------------------------------------------------------


class TestLayoutEndpoints:
    def test_get_layout_returns_dict(self, client):
        r = client.get("/api/layout")
        assert r.status_code == 200
        data = r.json()
        assert "positions" in data
        assert "manual_nodes" in data

    def test_put_layout_saves_position(self, client):
        r = client.put("/api/layout/fn__bandpass_filter", json={"x": 100.0, "y": 200.0})
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        layout = client.get("/api/layout").json()
        assert layout["positions"]["fn__bandpass_filter"] == {"x": 100.0, "y": 200.0}

    def test_put_layout_with_node_type_creates_manual_node(self, client):
        client.put(
            "/api/layout/manual__foo",
            json={
                "x": 10.0,
                "y": 20.0,
                "node_type": "functionNode",
                "label": "my_fn",
            },
        )
        layout = client.get("/api/layout").json()
        assert "manual__foo" in layout["manual_nodes"]
        assert layout["manual_nodes"]["manual__foo"]["label"] == "my_fn"

    def test_delete_layout_removes_node(self, client):
        client.put("/api/layout/fn__bandpass_filter", json={"x": 1.0, "y": 2.0})
        r = client.delete("/api/layout/fn__bandpass_filter")
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        layout = client.get("/api/layout").json()
        assert "fn__bandpass_filter" not in layout["positions"]

    def test_put_layout_overwrites_position(self, client):
        client.put("/api/layout/fn__bandpass_filter", json={"x": 1.0, "y": 2.0})
        client.put("/api/layout/fn__bandpass_filter", json={"x": 99.0, "y": 88.0})
        layout = client.get("/api/layout").json()
        assert layout["positions"]["fn__bandpass_filter"] == {"x": 99.0, "y": 88.0}


# ---------------------------------------------------------------------------
# /api/parameters
# ---------------------------------------------------------------------------


class TestConstantsEndpoints:
    """Constants are now source-declared (``NAME = scidb.Parameter(...)``),
    same append-only pattern as PathInput/Sweep — see
    docs/claude/code-discovery-categories.md. Creation needs a writable
    target file, hence ``client_with_variable_file`` (not plain ``client``)
    everywhere here."""

    def test_get_constants_initially_empty(self, client):
        r = client.get("/api/parameters")
        assert r.status_code == 200
        assert r.json() == []

    def test_post_constant_adds_it(self, client_with_variable_file):
        client = client_with_variable_file
        r = client.post("/api/parameters", json={"name": "window_size"})
        assert r.status_code == 200
        assert r.json() == {"ok": True, "name": "window_size"}
        constants = client.get("/api/parameters").json()
        names = [c["name"] for c in constants]
        assert "window_size" in names

    def test_post_constant_no_duplicate(self, client_with_variable_file):
        client = client_with_variable_file
        client.post("/api/parameters", json={"name": "alpha"})
        client.post("/api/parameters", json={"name": "alpha"})
        constants = client.get("/api/parameters").json()
        names = [c["name"] for c in constants]
        assert names.count("alpha") == 1

    def test_delete_constant_removes_it(self, client_with_variable_file):
        client = client_with_variable_file
        client.post("/api/parameters", json={"name": "alpha"})
        client.post("/api/parameters", json={"name": "beta"})
        r = client.delete("/api/parameters/alpha")
        assert r.status_code == 200
        # "Delete" hides the node only (never delete, mark hidden) — the
        # source declaration (and therefore the registry entry) is untouched.
        constants = client.get("/api/parameters").json()
        names = [c["name"] for c in constants]
        assert "alpha" in names
        assert "beta" in names

    def test_delete_nonexistent_constant_is_ok(self, client):
        r = client.delete("/api/parameters/ghost")
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# /api/edges
# ---------------------------------------------------------------------------


class TestEdgesEndpoints:
    def test_put_edge_saves_it(self, client):
        r = client.put(
            "/api/edges/e_test_1",
            json={
                "source": "var__RawSignal",
                "target": "fn__bandpass_filter",
                "source_handle": None,
                "target_handle": "in__signal",
            },
        )
        assert r.status_code == 200
        assert r.json() == {"ok": True}

        # Verify it shows up in the pipeline
        edges = client.get("/api/pipeline").json()["edges"]
        assert any(e["id"] == "e_test_1" for e in edges)

    def test_put_edge_upserts(self, client):
        client.put(
            "/api/edges/e_test_2",
            json={
                "source": "A",
                "target": "B",
                "source_handle": None,
                "target_handle": None,
            },
        )
        client.put(
            "/api/edges/e_test_2",
            json={
                "source": "A",
                "target": "C",
                "source_handle": None,
                "target_handle": None,
            },
        )
        layout = client.get("/api/layout").json()
        matching = [e for e in layout["manual_edges"] if e["id"] == "e_test_2"]
        assert len(matching) == 1
        assert matching[0]["target"] == "C"

    def test_delete_edge_removes_it(self, client):
        client.put(
            "/api/edges/e_del",
            json={
                "source": "X",
                "target": "Y",
                "source_handle": None,
                "target_handle": None,
            },
        )
        r = client.delete("/api/edges/e_del")
        assert r.status_code == 200
        layout = client.get("/api/layout").json()
        assert not any(e["id"] == "e_del" for e in layout["manual_edges"])

    def test_delete_nonexistent_edge_is_ok(self, client):
        r = client.delete("/api/edges/ghost_edge")
        assert r.status_code == 200

    def test_put_edge_rejects_self_loop(self, client):
        r = client.put(
            "/api/edges/e_self",
            json={
                "source": "var__RawSignal",
                "target": "var__RawSignal",
                "source_handle": None,
                "target_handle": None,
            },
        )
        assert r.status_code == 400
        assert "cycle" in r.json()["detail"]
        edges = client.get("/api/pipeline").json()["edges"]
        assert not any(e["id"] == "e_self" for e in edges)

    def test_put_edge_rejects_transitive_manual_cycle(self, client):
        # n1 -> n2 -> n3 already wired manually; closing n3 -> n1 would
        # create a cycle purely through manual edges.
        client.put(
            "/api/edges/e_n1_n2",
            json={"source": "n1", "target": "n2", "source_handle": None, "target_handle": None},
        )
        client.put(
            "/api/edges/e_n2_n3",
            json={"source": "n2", "target": "n3", "source_handle": None, "target_handle": None},
        )
        r = client.put(
            "/api/edges/e_n3_n1",
            json={"source": "n3", "target": "n1", "source_handle": None, "target_handle": None},
        )
        assert r.status_code == 400
        assert "cycle" in r.json()["detail"]
        edges = client.get("/api/pipeline").json()["edges"]
        assert not any(e["id"] == "e_n3_n1" for e in edges)

    def test_put_edge_upsert_of_same_id_is_not_a_self_cycle(self, client):
        # Re-PUTting the SAME edge_id with a different source/target is an
        # update, not a new edge — the edge's own stale row must not count
        # against it when checking reachability.
        client.put(
            "/api/edges/e_update",
            json={"source": "p1", "target": "p2", "source_handle": None, "target_handle": None},
        )
        r = client.put(
            "/api/edges/e_update",
            json={"source": "p2", "target": "p1", "source_handle": None, "target_handle": None},
        )
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Deleting a DB-derived edge disconnects the wiring: red, un-runnable,
# reconnecting the exact same nodes restores it fresh from real DB state.
# ---------------------------------------------------------------------------


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestDisconnectedEdges:
    def _find_edge(self, client, source, target):
        edges = client.get("/api/pipeline").json()["edges"]
        return next(e for e in edges if e["source"] == source and e["target"] == target)

    def _delete(self, client, edge):
        return client.request(
            "DELETE",
            f"/api/edges/{edge['id']}",
            json={"source": edge["source"], "target": edge["target"]},
        )

    def test_delete_var_input_edge_reddens_and_marks_disconnected(self, client, bp_node_id):
        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        r = self._delete(client, edge)
        assert r.status_code == 200

        data = client.get("/api/pipeline").json()
        assert not any(e["id"] == edge["id"] for e in data["edges"])
        node = next(n for n in data["nodes"] if n["id"] == bp_node_id)
        assert node["data"]["run_state"] == "red"
        assert node["data"].get("disconnected") is True

    def test_delete_var_input_edge_reddens_downstream_variable(self, client, bp_node_id):
        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        self._delete(client, edge)

        nodes = client.get("/api/pipeline").json()["nodes"]
        out_node = next(n for n in nodes if n["id"] == "var__FilteredSignal")
        assert out_node["data"]["run_state"] == "red"

    def test_delete_const_input_edge_also_disconnects(self, client, bp_node_id):
        edge = self._find_edge(client, "param__low_hz", bp_node_id)
        self._delete(client, edge)

        nodes = client.get("/api/pipeline").json()["nodes"]
        node = next(n for n in nodes if n["id"] == bp_node_id)
        assert node["data"]["run_state"] == "red"
        assert node["data"].get("disconnected") is True

    def test_delete_output_edge_is_cosmetic_only(self, client, bp_node_id):
        # fn -> var (output) hides never disconnect — the data still exists.
        edge = self._find_edge(client, bp_node_id, "var__FilteredSignal")
        self._delete(client, edge)

        data = client.get("/api/pipeline").json()
        assert not any(e["id"] == edge["id"] for e in data["edges"])
        node = next(n for n in data["nodes"] if n["id"] == bp_node_id)
        assert node["data"]["run_state"] == "green"
        assert not node["data"].get("disconnected")

    def test_reconnect_same_edge_unhides_original_and_restores_green(self, client, bp_node_id):
        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        original_id = edge["id"]
        self._delete(client, edge)
        assert not any(
            e["id"] == original_id for e in client.get("/api/pipeline").json()["edges"]
        )

        # Reconnect the SAME two nodes via a brand-new client-chosen id —
        # backend must recognize the match and unhide the ORIGINAL edge
        # instead of creating a redundant manual one.
        r = client.put(
            "/api/edges/manual__reconnect1",
            json={
                "source": "var__RawSignal",
                "target": bp_node_id,
                "source_handle": None,
                "target_handle": None,
            },
        )
        assert r.status_code == 200
        assert r.json().get("unhidden") == original_id

        data = client.get("/api/pipeline").json()
        assert any(e["id"] == original_id for e in data["edges"])
        assert not any(e["id"] == "manual__reconnect1" for e in data["edges"])
        node = next(n for n in data["nodes"] if n["id"] == bp_node_id)
        assert node["data"]["run_state"] == "green"
        assert not node["data"].get("disconnected")

    def test_reconnect_different_source_creates_new_manual_edge_instead(
        self, client, bp_node_id
    ):
        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        self._delete(client, edge)

        # A genuinely different source (never wired to this fn) must NOT
        # match the hidden edge — falls through to a normal manual edge.
        r = client.put(
            "/api/edges/manual__newconst",
            json={
                "source": "param__brand_new_constant",
                "target": bp_node_id,
                "source_handle": None,
                "target_handle": "param__brand_new_constant",
            },
        )
        assert r.status_code == 200
        assert "unhidden" not in r.json()
        data = client.get("/api/pipeline").json()
        assert any(e["id"] == "manual__newconst" for e in data["edges"])

    def test_manual_edge_delete_still_hard_deletes_not_hides(self, client):
        client.put(
            "/api/edges/manual__x",
            json={"source": "A", "target": "B", "source_handle": None, "target_handle": None},
        )
        r = client.request("DELETE", "/api/edges/manual__x")
        assert r.status_code == 200
        layout = client.get("/api/layout").json()
        assert not any(e["id"] == "manual__x" for e in layout["manual_edges"])
        hidden = client.get("/api/edges/hidden").json()["edges"]
        assert not any(h["edge_id"] == "manual__x" for h in hidden)

    def test_hidden_edges_list_and_unhide_endpoint(self, client, bp_node_id):
        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        self._delete(client, edge)

        hidden = client.get("/api/edges/hidden").json()["edges"]
        assert any(h["edge_id"] == edge["id"] for h in hidden)

        r = client.post(f"/api/edges/{edge['id']}/unhide")
        assert r.status_code == 200
        hidden_after = client.get("/api/edges/hidden").json()["edges"]
        assert not any(h["edge_id"] == edge["id"] for h in hidden_after)

        # Unhiding restores the ORIGINAL edge and green state, same as the
        # reconnect-via-drag path.
        data = client.get("/api/pipeline").json()
        assert any(e["id"] == edge["id"] for e in data["edges"])
        node = next(n for n in data["nodes"] if n["id"] == bp_node_id)
        assert node["data"]["run_state"] == "green"

    def test_run_blocked_when_disconnected(self, client, bp_node_id, monkeypatch):
        from scistack_gui.api import run as run_api

        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        self._delete(client, edge)

        captured = []
        monkeypatch.setattr(
            run_api, "for_each", lambda *a, **k: captured.append(k) or None
        )

        r = client.post(
            "/api/run", json={"function_name": "bandpass_filter", "variants": []}
        )
        assert r.status_code == 200
        _wait_for_threads("Thread-")
        assert captured == [], "disconnected function must never reach for_each"

    def test_run_blocked_emits_explicit_disconnected_error(
        self, client, bp_node_id, monkeypatch
    ):
        from scistack_gui.api import run as run_api

        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        self._delete(client, edge)

        emitted: list[dict] = []
        monkeypatch.setattr(run_api, "push_message", lambda msg: emitted.append(msg))

        r = client.post(
            "/api/run", json={"function_name": "bandpass_filter", "variants": []}
        )
        assert r.status_code == 200
        _wait_for_threads("Thread-")

        done = [m for m in emitted if m.get("type") == "run_done"]
        assert len(done) == 1
        assert done[0]["success"] is False
        assert "disconnected" in done[0]["error"]
        assert "signal" in done[0]["error"]

    def test_reconnect_different_variable_clears_disconnected_and_run_state(
        self, client, bp_node_id
    ):
        # Regression: deleting the signal edge then manually wiring a
        # DIFFERENT variable onto the same in__signal handle must clear
        # the disconnected/red state — it used to stay stuck forever
        # because the disconnected check never looked at manual edges.
        import numpy as np
        from scidb import BaseVariable

        class OtherSignal4(BaseVariable):
            pass

        OtherSignal4.save(np.zeros(5), subject=1, session="pre")

        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        self._delete(client, edge)

        r = client.put(
            "/api/edges/manual__reconnect4",
            json={
                "source": "var__OtherSignal4",
                "target": bp_node_id,
                "source_handle": None,
                "target_handle": "in__signal",
            },
        )
        assert r.status_code == 200

        data = client.get("/api/pipeline").json()
        node = next(n for n in data["nodes"] if n["id"] == bp_node_id)
        assert not node["data"].get("disconnected")
        assert node["data"]["run_state"] != "red"

    def test_reconnect_different_variable_unblocks_run_and_uses_new_input(
        self, client, bp_node_id, monkeypatch
    ):
        import numpy as np
        from scidb import BaseVariable

        from scistack_gui.api import run as run_api

        class OtherSignal5(BaseVariable):
            pass

        OtherSignal5.save(np.zeros(5), subject=1, session="pre")

        edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        self._delete(client, edge)
        client.put(
            "/api/edges/manual__reconnect5",
            json={
                "source": "var__OtherSignal5",
                "target": bp_node_id,
                "source_handle": None,
                "target_handle": "in__signal",
            },
        )

        captured = []
        monkeypatch.setattr(
            run_api, "for_each", lambda *a, **k: captured.append(k) or None
        )

        r = client.post(
            "/api/run", json={"function_name": "bandpass_filter", "variants": []}
        )
        assert r.status_code == 200
        _wait_for_threads("Thread-")

        assert len(captured) == 1, "reconnected function must run, not stay blocked"
        assert captured[0]["inputs"]["signal"] is OtherSignal5

    def test_reconnect_only_one_of_two_disconnected_inputs_stays_blocked(
        self, client, bp_node_id, monkeypatch
    ):
        # Partial reconnection: signal gets a new variable, but low_hz's
        # hidden const edge is left untouched — the node must stay
        # disconnected/blocked until BOTH are reconnected.
        import numpy as np
        from scidb import BaseVariable

        from scistack_gui.api import run as run_api

        class OtherSignal6(BaseVariable):
            pass

        OtherSignal6.save(np.zeros(5), subject=1, session="pre")

        var_edge = self._find_edge(client, "var__RawSignal", bp_node_id)
        self._delete(client, var_edge)
        const_edge = self._find_edge(client, "param__low_hz", bp_node_id)
        self._delete(client, const_edge)

        client.put(
            "/api/edges/manual__reconnect6",
            json={
                "source": "var__OtherSignal6",
                "target": bp_node_id,
                "source_handle": None,
                "target_handle": "in__signal",
            },
        )

        data = client.get("/api/pipeline").json()
        node = next(n for n in data["nodes"] if n["id"] == bp_node_id)
        assert node["data"].get("disconnected") is True
        assert node["data"]["run_state"] == "red"

        captured = []
        monkeypatch.setattr(
            run_api, "for_each", lambda *a, **k: captured.append(k) or None
        )
        r = client.post(
            "/api/run", json={"function_name": "bandpass_filter", "variants": []}
        )
        assert r.status_code == 200
        _wait_for_threads("Thread-")
        assert captured == [], "partially-reconnected function must stay blocked"


# ---------------------------------------------------------------------------
# /api/variables
# ---------------------------------------------------------------------------


class TestVariableRecordsEndpoint:
    def test_returns_schema_keys(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        assert r.status_code == 200
        data = r.json()
        assert data["schema_keys"] == ["subject", "session"]

    def test_returns_records(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        data = r.json()
        assert len(data["records"]) == 4  # 2 subjects × 2 sessions

    def test_records_have_schema_key_values(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        records = r.json()["records"]
        subjects = {rec["subject"] for rec in records}
        sessions = {rec["session"] for rec in records}
        assert subjects == {"1", "2"}
        assert sessions == {"pre", "post"}

    def test_records_have_variant_label(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        records = r.json()["records"]
        assert all("variant_label" in rec for rec in records)
        # bandpass_filter with low_hz=20 should appear in every label
        assert all("low_hz=20" in rec["variant_label"] for rec in records)

    def test_variants_summary(self, client):
        r = client.get("/api/variables/FilteredSignal/records")
        variants = r.json()["variants"]
        assert len(variants) == 1  # single variant (low_hz=20)
        assert variants[0]["record_count"] == 4
        assert "low_hz=20" in variants[0]["label"]

    def test_raw_variable_returns_records(self, client):
        # RawSignal was saved directly (no for_each), branch_params should be empty
        r = client.get("/api/variables/RawSignal/records")
        assert r.status_code == 200
        data = r.json()
        assert len(data["records"]) == 4
        assert all(rec["variant_label"] == "(raw)" for rec in data["records"])

    def test_unknown_variable_returns_empty(self, client):
        r = client.get("/api/variables/NonExistentVariable/records")
        # No records in metadata → empty result (not 404)
        assert r.status_code == 200
        data = r.json()
        assert data["records"] == []
        assert data["variants"] == []

    def test_single_version_carries_no_code_discriminator(self, client):
        """The ordinary case must look exactly as it did before this feature."""
        r = client.get("/api/variables/FilteredSignal/records")
        data = r.json()
        assert all(rec["fn_version"] is None for rec in data["records"])
        assert all(v["fn_version"] is None for v in data["variants"])
        assert all(rec["fn_name"] == "bandpass_filter" for rec in data["records"])


class TestVariableRecordsFunctionVersions:
    """An edited function body must be visible in the records/variants payload.

    Before this, two records produced by different versions of one function had
    identical branch params, so they collapsed into a single "(raw)"-style
    variant whose count silently summed both — the reported bug.
    """

    @staticmethod
    def _run_edited_body(populated_db):
        """Re-run the seeded pipeline with a DIFFERENT body under the same name.

        ``__name__`` is what lands in ``_invocation.function_name``, while
        ``function_hash`` is an AST hash of the source — so this models "the
        user edited the function and hit Run".
        """

        def bandpass_filter_v2(signal, low_hz):
            return np.asarray(signal, dtype=float) * float(low_hz) + 1.0

        bandpass_filter_v2.__name__ = "bandpass_filter"
        for_each(
            bandpass_filter_v2,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["pre", "post"],
        )

    def test_two_versions_do_not_collapse_into_one_variant(self, client, populated_db):
        before = client.get("/api/variables/FilteredSignal/records").json()
        assert len(before["variants"]) == 1

        self._run_edited_body(populated_db)

        after = client.get("/api/variables/FilteredSignal/records").json()
        assert len(after["records"]) == 8, "4 combos x 2 code versions"
        assert len(after["variants"]) == 2, (
            "the two code versions must be separate variant rows, not one row "
            "of count 8"
        )
        assert {v["record_count"] for v in after["variants"]} == {4}

    def test_labels_name_the_function_and_version(self, client, populated_db):
        self._run_edited_body(populated_db)
        data = client.get("/api/variables/FilteredSignal/records").json()

        labels = [v["label"] for v in data["variants"]]
        assert all("bandpass_filter" in label for label in labels)
        assert any("v1" in label for label in labels)
        assert any("v2" in label for label in labels)
        # The constant the user configured still leads the label.
        assert all(label.startswith("low_hz=20") for label in labels)
        assert sum("(latest)" in label for label in labels) == 1

    def test_latest_variant_is_listed_first(self, client, populated_db):
        self._run_edited_body(populated_db)
        data = client.get("/api/variables/FilteredSignal/records").json()

        assert data["variants"][0]["is_latest"] is True
        assert data["variants"][0]["fn_version"] == "v2"

    def test_variants_are_separated_by_function_hash(self, client, populated_db):
        self._run_edited_body(populated_db)
        data = client.get("/api/variables/FilteredSignal/records").json()

        hashes = {v["fn_hash"] for v in data["variants"]}
        assert len(hashes) == 2 and None not in hashes
        # Same branch params on both — the hash is the ONLY thing separating
        # them, which is exactly why grouping on branch params alone failed.
        assert len({str(v["branch_params"]) for v in data["variants"]}) == 1

    def test_records_carry_their_own_version(self, client, populated_db):
        self._run_edited_body(populated_db)
        data = client.get("/api/variables/FilteredSignal/records").json()

        versions = {rec["fn_version"] for rec in data["records"]}
        assert versions == {"v1", "v2"}
        assert sum(1 for rec in data["records"] if rec["is_latest"]) == 4
        assert all(rec["saved_at"] for rec in data["records"])


# ---------------------------------------------------------------------------
# Wiring mutations broadcast dag_updated
# ---------------------------------------------------------------------------


class TestWiringMutationBroadcasts:
    """Node/edge create+delete must broadcast dag_updated so the canvas
    refetches and a freshly placed node gets its real DB-checked state —
    a re-dropped, re-wired, already-computed function must come back green,
    not the frontend-local red (user-found 2026-07-19). Position-only
    writes must NOT broadcast (a rebuild per drag would be pathological).
    """

    @pytest.fixture
    def captured(self, monkeypatch):
        from scistack_gui.api import ws as ws_mod

        messages: list[dict] = []
        monkeypatch.setattr(ws_mod, "push_message", lambda msg: messages.append(msg))

        async def _fake_broadcast(msg):
            messages.append(msg)

        monkeypatch.setattr(ws_mod, "broadcast", _fake_broadcast)
        return messages

    def _dag_updates(self, messages):
        return [m for m in messages if m.get("type") == "dag_updated"]

    def test_node_creation_broadcasts(self, client, captured):
        client.put(
            "/api/layout/manual__bcast_test",
            json={
                "x": 1.0,
                "y": 2.0,
                "node_type": "variableNode",
                "label": "BcastVar",
            },
        )
        assert len(self._dag_updates(captured)) == 1

    def test_position_only_write_does_not_broadcast(self, client, captured):
        client.put(
            "/api/layout/manual__bcast_test2",
            json={
                "x": 1.0,
                "y": 2.0,
                "node_type": "variableNode",
                "label": "BcastVar2",
            },
        )
        captured.clear()
        client.put("/api/layout/manual__bcast_test2", json={"x": 50.0, "y": 60.0})
        assert self._dag_updates(captured) == []

    def test_node_delete_broadcasts(self, client, captured):
        client.put(
            "/api/layout/manual__bcast_test3",
            json={
                "x": 0.0,
                "y": 0.0,
                "node_type": "variableNode",
                "label": "BcastVar3",
            },
        )
        captured.clear()
        client.delete("/api/layout/manual__bcast_test3")
        assert len(self._dag_updates(captured)) == 1

    def test_edge_create_and_delete_broadcast(self, client, captured):
        client.put(
            "/api/edges/manual__bcast_e1",
            json={
                "source": "var__RawSignal",
                "target": "var__FilteredSignal",
            },
        )
        assert len(self._dag_updates(captured)) == 1
        captured.clear()
        client.delete("/api/edges/manual__bcast_e1")
        assert len(self._dag_updates(captured)) == 1

    def test_variable_creation_does_not_broadcast(
        self, client_with_variable_file, captured
    ):
        """A freshly declared type has no DB records yet, so it cannot
        appear as a canvas node (variable nodes come from list_variables,
        not the type registry — see graph_builder.build_variable_nodes).
        Broadcasting here used to force every connected client through a
        full pipeline refetch+relayout for a change the canvas can't show
        (user-found 2026-09-03: "creating a variable forced a whole
        codebase refresh"). The sidebar updates its own registry state
        directly instead, same as create_parameter/create_path_input."""
        resp = client_with_variable_file.post(
            "/api/variables/create", json={"name": "BcastNewVar"}
        )
        assert resp.json()["ok"] is True
        assert self._dag_updates(captured) == []


# ---------------------------------------------------------------------------
# /api/run
# ---------------------------------------------------------------------------


def _wait_for_threads(prefix: str, timeout: float = 2.0) -> None:
    """Wait for any background run threads to finish before DB teardown."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        live = [t for t in threading.enumerate() if t.name.startswith(prefix)]
        if not live:
            break
        time.sleep(0.05)


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestRunEndpoint:
    def test_returns_run_id(self, client):
        r = client.post(
            "/api/run",
            json={
                "function_name": "bandpass_filter",
                "variants": [],
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert "run_id" in data
        assert data["run_id"]  # non-empty
        _wait_for_threads("Thread-")

    def test_accepts_caller_supplied_run_id(self, client):
        r = client.post(
            "/api/run",
            json={
                "function_name": "bandpass_filter",
                "variants": [],
                "run_id": "my_run_42",
            },
        )
        assert r.status_code == 200
        assert r.json()["run_id"] == "my_run_42"
        _wait_for_threads("Thread-")

    def test_forwards_schema_keys_not_schema_level_to_for_each(self, client, monkeypatch):
        """Regression test: scidb.for_each's real parameter is `schema_keys`
        (see scidb/src/scidb/foreach.py), not `schema_level` — `schema_level`
        is only this module's own GUI-facing name (RunRequest.schema_level)
        for "which schema keys to iterate".

        A prior version of `_run_in_thread` forwarded it verbatim as
        `schema_level=...` when calling the real for_each. Since for_each
        accepts arbitrary **metadata_iterables, that unrecognized keyword
        didn't error — it silently became a bogus metadata-iteration axis
        literally named "schema_level", leaving the REAL schema key (e.g.
        "subject") un-iterated. for_each then entered aggregation mode and
        pooled every row for that key into one call, crashing any function
        written per-combo the moment its input had more than one variant
        (found via a real GUI run of gui_test_data.compute_max_vo2).

        This test doesn't need to reproduce that full multi-variant crash —
        it directly pins the fix: with no explicit schema_level/schema_filter/
        as_table, `_run_in_thread` must call for_each with `schema_keys` set
        to all DB schema keys, and must never pass a `schema_level` kwarg at
        all (since for_each has no such parameter)."""
        from scistack_gui.api import run as run_api

        captured = {}

        def fake_for_each(*args, **kwargs):
            captured.update(kwargs)
            return None

        monkeypatch.setattr(run_api, "for_each", fake_for_each)

        r = client.post(
            "/api/run",
            json={"function_name": "bandpass_filter", "variants": []},
        )
        assert r.status_code == 200
        _wait_for_threads("Thread-")

        assert captured, "for_each was never called"
        assert "schema_level" not in captured
        assert captured.get("schema_keys") == list(
            _gui_db.get_db().dataset_schema_keys
        )

    def test_hidden_combo_excluded_from_run(
        self, client, monkeypatch, populated_db
    ):
        """A hidden combo must never reach for_each -- the execution-time
        enforcement this feature closes (hiding was previously
        display-only; see plan-combo-hiding.md).

        Hides by the combo's REAL call_id (via resolve_combo_call_ids),
        not the canvas node's wiring_id (bp_node_id) -- wiring_id excludes
        constants so it's a different hash from the real per-combo call_id
        and would never match."""
        from scistack_gui import pipeline_store
        from scistack_gui.api import run as run_api
        from scistack_gui.ids import fn_node_id
        from scistack_gui.services.execution_service import resolve_combo_call_ids

        call_ids = resolve_combo_call_ids(
            populated_db, "bandpass_filter", None, {"low_hz": "20"}
        )
        assert call_ids, "expected the seeded low_hz=20 combo to resolve to a real call_id"
        for cid in call_ids:
            pipeline_store.hide_combo(
                populated_db,
                fn_node_id("bandpass_filter", cid),
                "bandpass_filter",
                {"low_hz": "20"},
            )

        captured = []

        def fake_for_each(*args, **kwargs):
            captured.append(kwargs)
            return None

        monkeypatch.setattr(run_api, "for_each", fake_for_each)

        r = client.post(
            "/api/run",
            json={"function_name": "bandpass_filter", "variants": []},
        )
        assert r.status_code == 200
        _wait_for_threads("Thread-")

        assert captured == [], "hidden combo must never reach for_each"

    def test_unknown_function_still_returns_200_with_run_id(self, client):
        """
        The HTTP layer returns immediately; the error is surfaced via WebSocket.
        So even for an unknown function, the POST itself should succeed.
        """
        r = client.post(
            "/api/run",
            json={
                "function_name": "no_such_function",
                "variants": [],
            },
        )
        assert r.status_code == 200
        assert "run_id" in r.json()
        _wait_for_threads("Thread-")

    def test_cancel_run_unknown_id_returns_error(self, client):
        """cancel_run on a run_id that isn't active should return ok=False."""
        from scistack_gui.api.run import cancel_run

        result = cancel_run("does_not_exist_xyz")
        assert result["ok"] is False
        assert "unknown run_id" in result["error"]

    def test_force_cancel_run_unknown_id_returns_error(self, client):
        """force_cancel_run on an unknown run_id should also error gracefully."""
        from scistack_gui.api.run import force_cancel_run

        result = force_cancel_run("not_a_real_run_id")
        assert result["ok"] is False
        assert "unknown run_id" in result["error"]

    def test_cancel_run_sets_event_and_terminates(self, client, monkeypatch):
        """Cooperative cancel: setting the event should make the worker stop
        between variants, emit run_done with cancelled=True, and pop the entry
        from _active_runs.

        We synthesise a worker that loops forever while the event is unset,
        bypassing _run_in_thread's heavyweight setup. This exercises the
        registry, event plumbing, and the run_done emission path.
        """
        from scistack_gui.api import run as run_api

        run_id = "cancel_test_1"
        emitted: list[dict] = []

        # Capture push_message calls into a list rather than going to WS.
        monkeypatch.setattr(run_api, "push_message", lambda msg: emitted.append(msg))

        cancel_event = threading.Event()
        worker_done = threading.Event()

        def synthetic_worker():
            with run_api._active_runs_lock:
                run_api._active_runs[run_id] = {
                    "event": cancel_event,
                    "thread": threading.current_thread(),
                    "cancelled": False,
                    "force_cancelled": False,
                }
            try:
                # Poll the event up to ~2s; cancel should fire well before that.
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    if cancel_event.is_set():
                        run_api.push_message(
                            {
                                "type": "run_done",
                                "run_id": run_id,
                                "success": True,
                                "duration_ms": 0,
                                "cancelled": True,
                                "force_cancelled": False,
                            }
                        )
                        return
                    time.sleep(0.02)
            finally:
                with run_api._active_runs_lock:
                    run_api._active_runs.pop(run_id, None)
                worker_done.set()

        t = threading.Thread(target=synthetic_worker, name="SynthRun-1", daemon=True)
        t.start()

        # Give the worker a moment to register itself.
        time.sleep(0.05)

        result = run_api.cancel_run(run_id)
        assert result["ok"] is True
        assert result["cancelled"] is True
        assert result["force"] is False

        assert worker_done.wait(timeout=2.0), "worker did not stop after cancel"
        t.join(timeout=1.0)

        # Verify the emitted run_done has cancelled=True.
        done_msgs = [m for m in emitted if m.get("type") == "run_done"]
        assert len(done_msgs) == 1
        assert done_msgs[0]["cancelled"] is True

        # Registry must be cleaned up.
        with run_api._active_runs_lock:
            assert run_id not in run_api._active_runs

    def test_force_cancel_run_smoke(self, client):
        """force_cancel_run should set the event, attempt ctypes injection,
        and return the documented best-effort shape.

        We don't actually exercise the worker thread (KeyboardInterrupt
        injection inside pytest is destabilising) — we register a dummy
        thread, call force_cancel_run, then clean up.
        """
        from scistack_gui.api import run as run_api

        run_id = "force_cancel_smoke"
        cancel_event = threading.Event()

        # Register against the *current* thread but immediately remove
        # before it can be hit by ctypes. We pop just before the call so
        # the lookup succeeds, but the recorded thread is harmless.
        # To be really safe, use a finished thread (ident may be None or
        # invalid) — force_cancel_run handles both gracefully.
        finished = threading.Thread(target=lambda: None, name="ForceCancelSmoke")
        finished.start()
        finished.join()

        with run_api._active_runs_lock:
            run_api._active_runs[run_id] = {
                "event": cancel_event,
                "thread": finished,
                "cancelled": False,
                "force_cancelled": False,
            }

        try:
            result = run_api.force_cancel_run(run_id)
            assert result["ok"] is True
            assert result["cancelled"] is True
            assert result["force"] is True
            assert result["best_effort"] is True
            # The finished thread either has no ident or the injection
            # returns 0 — either way "injected" is a bool.
            assert "injected" in result
            assert isinstance(result["injected"], bool)
            # Cooperative cancel event must have been set as well.
            assert cancel_event.is_set()
        finally:
            with run_api._active_runs_lock:
                run_api._active_runs.pop(run_id, None)


# ---------------------------------------------------------------------------
# Run state: pipeline node run_state integration tests
# ---------------------------------------------------------------------------


# Extra variable class and function for the two-step chain tests
class ProcessedSignal(BaseVariable):
    pass


def process_signal(filtered):
    return np.asarray(filtered, dtype=float) * 2.0


class TestRunStateGreen:
    """Fully-run pipeline → green for both function and output variable."""

    def test_function_node_is_green(self, client, bp_node_id):
        nodes = client.get("/api/pipeline").json()["nodes"]
        fn_node = next(n for n in nodes if n["id"] == bp_node_id)
        assert fn_node["data"].get("run_state") == "green"

    def test_output_variable_is_green(self, client):
        nodes = client.get("/api/pipeline").json()["nodes"]
        var_node = next(n for n in nodes if n["id"] == "var__FilteredSignal")
        assert var_node["data"].get("run_state") == "green"


class TestRunStateRed:
    """Function registered but never run → red."""

    @pytest.fixture
    def client_never_run(self, tmp_path):
        # Fresh DB with raw data only — for_each never called
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "red.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "red.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app

        with TestClient(create_app()) as c:
            yield c

        db.close()

    def test_function_node_is_red(self, client_never_run):
        nodes = client_never_run.get("/api/pipeline").json()["nodes"]
        # Function never run — no variants in DB, so no fn node in pipeline.
        # State is red only if the node appears (via registry).
        state = fn_min_state_across_call_sites(nodes, "bandpass_filter")
        if state is not None:
            assert state == "red"


class TestRunStatePartialIsRed:
    """Partially-run pipeline → red. scidb node state is BINARY green/red
    ('needs attention' is one state whether never-run, partial, or stale —
    the former tri-state staleness verdict was removed with the bipartite
    graph). The GUI's only third state is 'pending' (yellow), which ONLY
    means an unrun pending constant value staged in the GUI."""

    @pytest.fixture
    def client_partial(self, tmp_path):
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "partial.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        # Run for only 1 of 2 subjects → 2 of 4 schema_ids processed
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre", "post"],
        )

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "partial.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app

        with TestClient(create_app()) as c:
            yield c

        db.close()

    def test_function_node_is_red_when_partial(self, client_partial):
        nodes = client_partial.get("/api/pipeline").json()["nodes"]
        # One call site (low_hz=20), partially run → red (binary state).
        node_id = find_fn_node_id_by_label(nodes, "bandpass_filter")
        fn_node = next(n for n in nodes if n["id"] == node_id)
        assert fn_node["data"].get("run_state") == "red"

    def test_output_variable_is_red_when_partial(self, client_partial):
        nodes = client_partial.get("/api/pipeline").json()["nodes"]
        var_node = next(n for n in nodes if n["id"] == "var__FilteredSignal")
        assert var_node["data"].get("run_state") == "red"


class TestRunStatePropagation:
    """
    Two-step chain: RawSignal → bandpass_filter → FilteredSignal → process_signal → ProcessedSignal.
    When the first step is red (partial run — binary node state), the second
    step must also be red even if it ran completely for all available
    FilteredSignal records (pessimistic DAG propagation).
    """

    @pytest.fixture
    def client_propagation(self, tmp_path):
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "prop.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        # Step 1: partial run of bandpass_filter (only subject=1)
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre", "post"],
        )

        # Step 2: fully process ALL available FilteredSignal records
        for_each(
            process_signal,
            inputs={"filtered": FilteredSignal},
            outputs=[ProcessedSignal],
            subject=[1],
            session=["pre", "post"],
        )

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "prop.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter
        _registry._functions["process_signal"] = process_signal

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app

        with TestClient(create_app()) as c:
            yield c

        db.close()

    def test_upstream_is_red(self, client_propagation):
        nodes = client_propagation.get("/api/pipeline").json()["nodes"]
        node_id = find_fn_node_id_by_label(nodes, "bandpass_filter")
        fn_node = next(n for n in nodes if n["id"] == node_id)
        # Partial run → red (binary node state).
        assert fn_node["data"].get("run_state") == "red"

    def test_downstream_function_is_red_due_to_staleness(self, client_propagation):
        nodes = client_propagation.get("/api/pipeline").json()["nodes"]
        node_id = find_fn_node_id_by_label(nodes, "process_signal")
        fn_node = next(n for n in nodes if n["id"] == node_id)
        # process_signal ran for all available inputs, but upstream is red → red
        assert fn_node["data"].get("run_state") == "red"

    def test_downstream_variable_is_red_due_to_staleness(self, client_propagation):
        nodes = client_propagation.get("/api/pipeline").json()["nodes"]
        var_node = next(n for n in nodes if n["id"] == "var__ProcessedSignal")
        assert var_node["data"].get("run_state") == "red"


# ---------------------------------------------------------------------------
# Pending constant lifecycle: green → staged value → pending → run → green
# ---------------------------------------------------------------------------


class TestPendingConstantLifecycle:
    """End-to-end: user drags a new constant value in the GUI, the consumer
    function node turns pending, the user runs for_each with that new value, and
    on the next /api/pipeline request the pending value is auto-cleaned and
    the function returns to green.

    The `populated_db` fixture leaves bandpass_filter fully run with low_hz=20.
    """

    def _fn_state(self, client):
        # The pending-constant lifecycle creates additional bandpass_filter
        # call sites mid-test (one per low_hz value).  "Is bandpass_filter
        # green?" therefore means "are all its call sites green?", so we
        # return the most pessimistic state across them.
        nodes = client.get("/api/pipeline").json()["nodes"]
        return fn_min_state_across_call_sites(nodes, "bandpass_filter")

    def _const_values(self, client):
        """Return the set of value strings under param__low_hz."""
        nodes = client.get("/api/pipeline").json()["nodes"]
        cnode = next(n for n in nodes if n["id"] == "param__low_hz")
        return {v["value"] for v in cnode["data"]["values"]}

    def test_starts_green(self, client):
        assert self._fn_state(client) == "green"

    def test_adding_pending_value_marks_consumer_pending(self, client):
        assert self._fn_state(client) == "green"

        r = client.put("/api/parameters/low_hz/pending/42")
        assert r.status_code == 200

        # Consumer downgraded green → pending.
        assert self._fn_state(client) == "pending"

        # Downstream variable inherits the pending state via DAG propagation.
        nodes = client.get("/api/pipeline").json()["nodes"]
        var_node = next(n for n in nodes if n["id"] == "var__FilteredSignal")
        assert var_node["data"].get("run_state") == "pending"

    def test_pending_value_appears_under_constant_node(self, client):
        client.put("/api/parameters/low_hz/pending/42")
        values = self._const_values(client)
        assert "42" in values

    def test_removing_pending_restores_green(self, client):
        client.put("/api/parameters/low_hz/pending/42")
        assert self._fn_state(client) == "pending"

        r = client.delete("/api/parameters/low_hz/pending/42")
        assert r.status_code == 200

        assert self._fn_state(client) == "green"

    def test_running_pipeline_with_new_value_auto_cleans_pending(
        self, client, populated_db
    ):
        """Running for_each with the pending scale produces matching records;
        on the next /api/pipeline call, auto_clean_pending_constants removes
        the pending row and the node returns to green."""
        client.put("/api/parameters/low_hz/pending/42")
        assert self._fn_state(client) == "pending"

        # Simulate the user running with the new constant value.
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 42},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["pre", "post"],
        )

        # Next pipeline request: auto-clean removes "42" from pending, and
        # both variants (20 and 42) are fully populated → green.
        assert self._fn_state(client) == "green"

        # The layout_store no longer reports "42" as pending.
        pending = layout_store.get_pending_constants()
        assert "42" not in pending.get("low_hz", set())

    def test_already_red_not_affected_by_pending(self, client, populated_db):
        """If a function is already red (stale input), adding a pending value
        does NOT reset it to pending — the more severe state wins."""
        # Re-save an input record to put the function into red.
        RawSignal.save(np.ones(10), subject=1, session="pre")
        assert self._fn_state(client) == "red"

        client.put("/api/parameters/low_hz/pending/99")
        assert self._fn_state(client) == "red"


class TestPendingConstantRecovery:
    """Recovery paths — a function that is red (partial) or never-run, has a
    pending constant added, and then is fully run, should end up green.
    """

    def _fn_state(self, c):
        nodes = c.get("/api/pipeline").json()["nodes"]
        return fn_min_state_across_call_sites(nodes, "bandpass_filter")

    @pytest.fixture
    def client_partial(self, tmp_path):
        """bandpass_filter ran for subject=1 only with low_hz=20 → red (partial)."""
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(
            tmp_path / "partial_recover.duckdb", ["subject", "session"]
        )
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre", "post"],
        )

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "partial_recover.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app

        with TestClient(create_app()) as c:
            yield c

        db.close()

    @pytest.fixture
    def client_never_run(self, tmp_path):
        """No for_each run yet → function would be red (if visible)."""
        if hasattr(_local, "database"):
            delattr(_local, "database")
        _gui_db._db = None

        db = configure_database(tmp_path / "red_recover.duckdb", ["subject", "session"])
        for subj in [1, 2]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.random.randn(10), subject=subj, session=sess)

        _gui_db._db = db
        _gui_db._db_path = tmp_path / "red_recover.duckdb"
        _registry._functions["bandpass_filter"] = bandpass_filter

        from fastapi.testclient import TestClient
        from scistack_gui.app import create_app

        with TestClient(create_app()) as c:
            yield c

        db.close()

    def test_red_plus_pending_resolves_to_green_after_full_run(self, client_partial):
        """red (partial run) + pending low_hz=42 → run both variants fully → green."""
        assert self._fn_state(client_partial) == "red"

        # Add pending on top of red — still red (the pending downgrade only
        # applies to green nodes; red already needs attention).
        client_partial.put("/api/parameters/low_hz/pending/42")
        assert self._fn_state(client_partial) == "red"

        # Complete both variants.
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal],
            subject=[2],
            session=["pre", "post"],  # remaining subjects
        )
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 42},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["pre", "post"],
        )

        # Auto-clean removes pending; all variants fully populated → green.
        assert self._fn_state(client_partial) == "green"
        pending = layout_store.get_pending_constants()
        assert "42" not in pending.get("low_hz", set())

    def test_red_never_run_plus_pending_resolves_to_green(self, client_never_run):
        """red (never run, via DB variants path) + pending low_hz=42 → run → green.

        Because no variants exist yet, the fn node may not appear in the
        pipeline at all. Run the pipeline with the pending value first,
        then assert the node exists and is green.
        """
        # Prior state: no fn node visible (no variants), so state == None.
        assert self._fn_state(client_never_run) is None

        client_never_run.put("/api/parameters/low_hz/pending/42")

        # Still no variants → still no fn node.
        assert self._fn_state(client_never_run) is None

        # Run with the pending value for all combos.
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 42},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["pre", "post"],
        )

        assert self._fn_state(client_never_run) == "green"
        pending = layout_store.get_pending_constants()
        assert "42" not in pending.get("low_hz", set())


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestManualInputEdgesOnHistoryNodes:
    """A manual variable edge drawn onto a HISTORY node's parameter that
    history never bound (docs/claude/manual-edges-on-history-nodes.md).

    Real session, 2026-09-15: ``grSides`` had run; ``side`` was added to its
    signature afterwards; the user wired ``Demographics → in__side`` and the
    Inputs section said ``n/a``, the run would have ignored the edge, and a
    fresh ``grSides`` node graduated straight into the history node. The
    rule now: the edges visible on the DAG are the ground truth.

    The vehicle is the populated ``bandpass_filter(RawSignal, low_hz=20)``
    history with the registry re-pointed at a WIDER signature, exactly the
    real shape (grSides' log said "source has changed since it last ran").
    """

    @pytest.fixture
    def wide(self, client):
        def bandpass_filter(signal, low_hz, side=None):  # noqa: ARG001
            return np.asarray(signal, dtype=float) * float(low_hz)

        _registry._functions["bandpass_filter"] = bandpass_filter
        return client

    @pytest.fixture
    def side_var(self, wide):
        class SideTable(BaseVariable):
            pass

        SideTable.save(np.zeros(3), subject=1, session="pre")
        return SideTable

    def _node(self, client, node_id):
        data = client.get("/api/pipeline").json()
        return next(n for n in data["nodes"] if n["id"] == node_id)

    def _wire_side(self, client, bp_node_id, source="var__SideTable", edge_id="manual__side"):
        r = client.put(
            f"/api/edges/{edge_id}",
            json={
                "source": source,
                "target": bp_node_id,
                "source_handle": None,
                "target_handle": "in__side",
            },
        )
        assert r.status_code == 200

    def test_unbound_param_shows_n_a_shape_before_the_edge(self, wide, bp_node_id):
        node = self._node(wide, bp_node_id)
        assert node["data"]["input_params"]["side"] == ""
        assert "manual_inputs" not in node["data"]

    def test_manual_edge_fills_the_history_nodes_input_params(
        self, wide, side_var, bp_node_id
    ):
        self._wire_side(wide, bp_node_id)
        node = self._node(wide, bp_node_id)
        assert node["data"]["input_params"]["side"] == "SideTable", (
            "the Inputs picker keys off input_params — an empty type is the "
            "one and only reason it renders 'n/a'"
        )
        assert node["data"]["input_params"]["signal"] == "RawSignal"
        assert node["data"]["manual_inputs"] == {"side": "SideTable"}
        # Node identity is untouched: saved position/config still key off it.
        assert node["id"] == bp_node_id

    def test_run_binds_the_manually_wired_variable(
        self, wide, side_var, bp_node_id, monkeypatch
    ):
        from scistack_gui.api import run as run_api

        self._wire_side(wide, bp_node_id)
        captured: list[dict] = []
        monkeypatch.setattr(run_api, "for_each", lambda *a, **k: captured.append(k) or None)

        r = wide.post("/api/run", json={"function_name": "bandpass_filter", "variants": []})
        assert r.status_code == 200
        _wait_for_threads("Thread-")

        assert len(captured) == 1
        assert captured[0]["inputs"]["signal"] is RawSignal
        assert captured[0]["inputs"]["side"] is side_var
        assert captured[0]["inputs"]["low_hz"] == 20, "history constants are kept"

    def test_node_scoped_run_binds_it_too(self, wide, side_var, bp_node_id, monkeypatch):
        from scistack_gui.api import run as run_api

        self._wire_side(wide, bp_node_id)
        captured: list[dict] = []
        monkeypatch.setattr(run_api, "for_each", lambda *a, **k: captured.append(k) or None)

        r = wide.post(
            "/api/run",
            json={"function_name": "bandpass_filter", "variants": [], "node_id": bp_node_id},
        )
        assert r.status_code == 200
        _wait_for_threads("Thread-")
        assert len(captured) == 1
        assert captured[0]["inputs"]["side"] is side_var

    def test_column_selection_on_the_overlaid_param_reaches_the_run(
        self, wide, side_var, bp_node_id, monkeypatch
    ):
        from scifor import ColumnSelection

        from scistack_gui import pipeline_store
        from scistack_gui.api import run as run_api

        self._wire_side(wide, bp_node_id)
        pipeline_store.update_node_config(
            _gui_db.get_db(),
            bp_node_id,
            {"columnSelections": {"side": {"columns": ["age"], "iterate": False}}},
        )
        captured: list[dict] = []
        monkeypatch.setattr(run_api, "for_each", lambda *a, **k: captured.append(k) or None)

        wide.post("/api/run", json={"function_name": "bandpass_filter", "variants": []})
        _wait_for_threads("Thread-")

        sel = captured[0]["inputs"]["side"]
        assert isinstance(sel, ColumnSelection)
        assert sel.columns == ["age"]

    def test_fresh_node_wired_to_the_unbound_param_does_not_graduate(
        self, wide, side_var, bp_node_id
    ):
        # The escape hatch the user tried: a fresh bandpass_filter node wired
        # like the history node PLUS SideTable on `side`. That is a different
        # visible wiring, so it must stay its own node instead of being
        # folded into the history node (which is what made the fresh node
        # inherit the history node's saved settings and its 'n/a').
        wide.put(
            "/api/layout/mf_fresh",
            json={"x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter"},
        )
        wide.put(
            "/api/edges/mf_e_sig",
            json={"source": "var__RawSignal", "target": "mf_fresh", "target_handle": "in__signal"},
        )
        wide.put(
            "/api/edges/mf_e_side",
            json={"source": "var__SideTable", "target": "mf_fresh", "target_handle": "in__side"},
        )
        wide.put(
            "/api/edges/mf_e_out",
            json={"source": "mf_fresh", "target": "var__FilteredSignal"},
        )
        data = wide.get("/api/pipeline").json()
        ids = {n["id"] for n in data["nodes"]}
        assert "mf_fresh" in ids, "must not graduate into the history node"
        assert bp_node_id in ids
        fresh = next(n for n in data["nodes"] if n["id"] == "mf_fresh")
        assert fresh["data"]["input_params"]["side"] == "SideTable"

    def test_bare_fresh_node_still_graduates(self, wide, bp_node_id):
        # The existing one-candidate-wins UX for an UNWIRED fresh node.
        wide.put(
            "/api/layout/mf_bare",
            json={"x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter"},
        )
        ids = {n["id"] for n in wide.get("/api/pipeline").json()["nodes"]}
        assert "mf_bare" not in ids

    def test_after_the_wiring_has_run_the_edge_moves_and_the_selection_follows(
        self, wide, side_var, bp_node_id
    ):
        from scistack_gui import pipeline_store
        from scistack_gui.domain.graph_builder import wiring_id
        from scistack_gui.ids import fn_node_id, strip_placement

        db = _gui_db.get_db()
        self._wire_side(wide, bp_node_id)
        pipeline_store.update_node_config(
            db,
            bp_node_id,
            {"columnSelections": {"side": {"columns": ["age"], "iterate": False}}},
        )

        # The run the overlay describes, done for real: history now carries
        # the effective wiring under a second node id.
        for_each(
            _registry._functions["bandpass_filter"],
            inputs={"signal": RawSignal, "low_hz": 20, "side": side_var},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre"],
        )
        new_wid = wiring_id(
            "bandpass_filter",
            {"signal": "RawSignal", "side": "SideTable"},
            {"FilteredSignal"},
            {},
        )
        new_id = fn_node_id("bandpass_filter", new_wid)

        data = wide.get("/api/pipeline").json()
        by_id = {n["id"]: n for n in data["nodes"]}
        assert new_id in by_id
        # The old node reverts to its true history.
        assert by_id[bp_node_id]["data"]["input_params"]["side"] == ""
        assert "manual_inputs" not in by_id[bp_node_id]["data"]
        # The manual edge row now names the new node and is folded into the
        # DB-derived edge there — drawn once, not twice.
        stored = next(e for e in pipeline_store.get_manual_edges(db) if e["id"] == "manual__side")
        assert strip_placement(stored["target"]) == new_id
        side_edges = [
            e
            for e in data["edges"]
            if e["target"] == new_id and e.get("targetHandle") == "in__side"
        ]
        assert len(side_edges) == 1
        # The column selection used by that run follows it.
        assert by_id[new_id]["data"]["columnSelections"] == {
            "side": {"columns": ["age"], "iterate": False}
        }
        assert pipeline_store.get_node_config(db, new_id)["columnSelections"] == {
            "side": {"columns": ["age"], "iterate": False}
        }


class TestGraduationCarriesNodeConfig:
    """Settings made on a fresh node must survive its graduation into the
    history node — in the SAME response that graduates it, and without
    leaving an orphaned config row behind. Real symptom: three
    "saved node config(s) match no node" warnings for fresh-node ids
    (`fn__grSides__5c9r0r`, ...) whose settings had silently vanished.
    See .claude/plan-graduation-config-migration.md.
    """

    FRESH = "fn__bandpass_filter__zz9fr3"

    def _place_fresh(self, client, config, pipeline_id=None):
        body = {"x": 0, "y": 0, "node_type": "functionNode", "label": "bandpass_filter"}
        if pipeline_id:
            body["pipeline_id"] = pipeline_id
        assert client.put(f"/api/layout/{self.FRESH}", json=body).status_code == 200
        assert (
            client.put(f"/api/layout/{self.FRESH}/config", json={"config": config}).status_code
            == 200
        )

    def _graduated(self, data, bp_node_id):
        ids = {n["id"] for n in data["nodes"]}
        assert self.FRESH not in ids, "the bare fresh node must graduate"
        return next(
            n for n in data["nodes"] if n["id"] in (bp_node_id, f"{bp_node_id}::main")
        )

    def test_setting_shows_on_the_graduated_node_in_the_graduating_response(
        self, client, bp_node_id, caplog
    ):
        import logging

        from scistack_gui import pipeline_store

        self._place_fresh(
            client,
            {"schemaLevel": ["subject"], "columnSelections": {"signal": {"columns": ["a"]}}},
        )

        with caplog.at_level(logging.WARNING, logger="scistack_gui.domain.graph_builder"):
            data = client.get("/api/pipeline").json()

        node = self._graduated(data, bp_node_id)
        assert node["data"]["schemaLevel"] == ["subject"]
        # Normalised on the way through the intent store: the one shape, not the
        # raw blob the panel happened to send.
        assert node["data"]["columnSelections"] == {
            "signal": {"columns": ["a"], "iterate": False}
        }

        configs = pipeline_store.get_node_configs(_gui_db.get_db())
        assert self.FRESH not in configs, "moved, not copied"
        # caplog.records spans the whole test call at every level — the INFO
        # lines from put_layout / update_node_config legitimately name the
        # fresh id. Only a WARN naming it (the orphan check) is a failure.
        naming_fresh = [
            f"{r.name}:{r.levelname}: {r.getMessage()}"
            for r in caplog.records
            if r.levelno >= logging.WARNING and self.FRESH in r.getMessage()
        ]
        assert naming_fresh == [], (
            "the orphan WARN must not name an id whose config was migrated"
        )
        # And it sticks on the next, unrelated rebuild.
        again = self._graduated(client.get("/api/pipeline").json(), bp_node_id)
        assert again["data"]["schemaLevel"] == ["subject"]

    def test_migrated_setting_reaches_the_run(self, client, bp_node_id, monkeypatch):
        from scifor import ColumnSelection

        from scistack_gui.api import run as run_api

        self._place_fresh(client, {"columnSelections": {"signal": {"columns": ["a"]}}})
        client.get("/api/pipeline")  # graduates

        captured: list[dict] = []
        monkeypatch.setattr(run_api, "for_each", lambda *a, **k: captured.append(k) or None)
        client.post("/api/run", json={"function_name": "bandpass_filter", "variants": []})
        _wait_for_threads("Thread-")

        sel = captured[0]["inputs"]["signal"]
        assert isinstance(sel, ColumnSelection) and sel.columns == ["a"]

    def test_fresh_node_setting_replaces_the_history_nodes_and_is_logged(
        self, client, bp_node_id, caplog
    ):
        import logging

        from scistack_gui import pipeline_store

        pipeline_store.update_node_config(
            _gui_db.get_db(), f"{bp_node_id}::main", {"schemaLevel": ["session"]}
        )
        self._place_fresh(client, {"schemaLevel": ["subject"]})

        # The move is the intent store's now (rekey_subject), so capture the
        # whole GUI namespace rather than one module's logger.
        with caplog.at_level(logging.INFO, logger="scistack_gui"):
            data = client.get("/api/pipeline").json()

        node = self._graduated(data, bp_node_id)
        assert node["data"]["schemaLevel"] == ["subject"], "the fresh node's setting wins"
        assert "['session']" in caplog.text, "the replaced value is logged verbatim"

    def test_sub_scope_graduation_keeps_the_setting_on_the_sub_canvas(self, client):
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()["pipeline_id"]
        self._place_fresh(client, {"schemaLevel": ["subject"]}, pipeline_id=pid)

        sub = client.get("/api/pipeline", params={"pipeline_id": pid}).json()
        canonical = [n for n in sub["nodes"] if n["id"].startswith("fn__bandpass_filter__")]
        assert len(canonical) == 1
        assert canonical[0]["data"]["schemaLevel"] == ["subject"]
