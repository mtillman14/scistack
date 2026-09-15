"""
Tests for scistack_gui.services.portability_service (to-do #7: pipeline
import/export between SciStack users — plan-pipeline-import-export.md).

Two flavors:
- SAME-database roundtrip (export then import back into the very db it
  came from) — exercises fresh-id remapping and, since every referenced
  Constant/PathInput/Sweep name already exists there by construction,
  naturally covers the "reuse local definition by name" path.
- CROSS-database roundtrip (import into a genuinely separate
  DatabaseManager built via a second configure_database() call) — the
  real "share with another user" scenario, and the only way to exercise
  "create it locally, it didn't exist yet" for globals.
"""

from __future__ import annotations

from scistack_gui import pipeline_store as ps
from scistack_gui.db import get_db
from scistack_gui.services.portability_service import (
    export_pipeline,
    import_pipeline_document,
)


def _labels(graph_nodes):
    return {(n["type"], n["data"]["label"]) for n in graph_nodes}


class TestExportImportSameDatabase:
    def test_unchanged_reimport_is_a_noop_reuse(self, client):
        """Re-importing a document into the very db it came from, with the
        pipeline still unchanged, must reuse it in place (same identity,
        same content) rather than create a duplicate copy of itself."""
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()["pipeline_id"]
        client.put("/api/layout/mv_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": pid,
        })
        client.put("/api/layout/mf_proc", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": pid,
        })
        client.put("/api/edges/e_in", json={"source": "mv_in", "target": "mf_proc"})

        db = get_db()
        document = export_pipeline(db, pid)
        assert document["format_version"] == 1
        assert {p["pipeline_id"] for p in document["pipelines"]} == {pid}

        result = import_pipeline_document(db, document)
        assert result["ok"] is True
        assert result["pipeline_id"] == pid
        assert result["reused"]["pipelines"] == ["loading"]
        assert [p["name"] for p in ps.list_pipelines(db)].count("loading") == 1

    def test_reimport_after_local_edit_forks_and_preserves_original(self, client):
        """A pipeline edited locally AFTER export no longer matches its own
        exported document by content — re-importing that document must
        fork into a new, renamed pipeline rather than overwrite or
        duplicate-collide with the (now-diverged) original."""
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

        db = get_db()
        document = export_pipeline(db, pid)
        assert document["format_version"] == 1
        assert {p["pipeline_id"] for p in document["pipelines"]} == {pid}
        assert {(n["node_type"], n["label"]) for n in document["nodes"]} == {
            ("variableNode", "RawSignal"),
            ("functionNode", "custom_proc"),
            ("variableNode", "FilteredSignal"),
        }

        # Diverge the local pipeline AFTER export, before re-importing the
        # (now stale) document — forces the fork path.
        client.put("/api/layout/mv_extra", json={
            "x": 30, "y": 0, "node_type": "variableNode", "label": "ExtraSignal", "pipeline_id": pid,
        })

        result = import_pipeline_document(db, document)
        assert result["ok"] is True
        new_pid = result["pipeline_id"]
        assert new_pid != pid
        assert result["reused"]["pipelines"] == []

        graph = client.get("/api/pipeline", params={"pipeline_id": new_pid}).json()
        assert _labels(graph["nodes"]) == {
            ("variableNode", "RawSignal"),
            ("functionNode", "custom_proc"),
            ("variableNode", "FilteredSignal"),
        }
        fn_node = next(n for n in graph["nodes"] if n["data"]["label"] == "custom_proc")
        manual = ps.get_manual_nodes(db, new_pid)
        assert manual[fn_node["id"]]["config"] == {"schemaSelection": {"exclude_levels": {"subject": ["S02"]}}}

        edges = ps.get_manual_edges(db)
        id_by_label = {n["data"]["label"]: n["id"] for n in graph["nodes"]}
        assert any(
            e["source"] == id_by_label["RawSignal"] and e["target"] == id_by_label["custom_proc"]
            for e in edges
        )
        assert any(
            e["source"] == id_by_label["custom_proc"] and e["target"] == id_by_label["FilteredSignal"]
            for e in edges
        )

        names = [p["name"] for p in ps.list_pipelines(db)]
        assert "loading (imported)" in names

        # Original (now-diverged) pipeline is untouched by the import —
        # still has the post-export edit, not reverted to the exported state.
        original_graph = client.get("/api/pipeline", params={"pipeline_id": pid}).json()
        assert _labels(original_graph["nodes"]) == {
            ("variableNode", "RawSignal"),
            ("functionNode", "custom_proc"),
            ("variableNode", "FilteredSignal"),
            ("variableNode", "ExtraSignal"),
        }

    def test_globals_already_present_are_reused_not_reimported(
        self, client_with_variable_file
    ):
        """Importing back into the SAME db means every referenced Constant/
        PathInput/Sweep name already exists there by construction — the
        confirmed 'reuse by name' decision means the import must not touch
        their existing values."""
        client = client_with_variable_file
        pid = client.post("/api/pipelines", json={"name": "loading"}).json()["pipeline_id"]
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
        client.put("/api/layout/pi_a", json={
            "x": 0, "y": 0, "node_type": "pathInputNode", "label": "gait_data", "pipeline_id": pid,
        })

        db = get_db()
        document = export_pipeline(db, pid)
        assert [p["name"] for p in document["path_inputs"]] == ["gait_data"]

        # Mutate the local definition AFTER export but before import (there
        # is no update endpoint anymore — PathInput is source-scanned and
        # create-only from the GUI, see
        # docs/claude/code-discovery-categories.md — so this edits the
        # source file directly, same as the user would in their own
        # editor, then hits Refresh Code). If import were to "reimport" it,
        # this edit would be clobbered back to the exported template;
        # reuse-by-name means it must survive.
        from scistack_gui import registry

        registry._module_path.write_text(
            'from scidb import PathInput\ngait_data = PathInput("{subject}_edited.csv")\n'
        )
        registry.refresh_module()

        result = import_pipeline_document(db, document)
        assert result["reused"]["path_inputs"] == ["gait_data"]

        by_name = {p["name"]: p for p in client.get("/api/path-inputs").json()}
        assert by_name["gait_data"]["template"] == "{subject}_edited.csv"


class TestExportImportCrossDatabase:
    def _second_db(self, tmp_path):
        """A genuinely separate DatabaseManager simulating another user's
        database, made the ACTIVE one for scistack_gui's own db module
        (which `layout_store` — used internally by import/export for
        positions/constants/path_inputs/sweeps — always reads through,
        independent of whatever `db` object a caller passes explicitly;
        `configure_database` alone only sets scidb's OWN internal global,
        not this separate scistack_gui-level pointer, same two-globals
        distinction conftest.py's own `populated_db` fixture already
        handles the same way)."""
        from scidb import configure_database

        from scistack_gui import db as gui_db

        db_path = tmp_path / "other_user.duckdb"
        db = configure_database(db_path, ["subject", "session"])
        gui_db._db = db
        gui_db._db_path = db_path
        return db

    def test_recursive_submodule_export_and_import(self, client, tmp_path):
        child = client.post("/api/pipelines", json={"name": "shared_prep"}).json()["pipeline_id"]
        client.put("/api/layout/cv_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": child,
        })
        client.put("/api/layout/cf_a", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": child,
        })
        client.put("/api/edges/ce_a", json={"source": "cv_in", "target": "cf_a"})

        parent = client.post("/api/pipelines", json={"name": "analysis"}).json()["pipeline_id"]
        use_id = client.post(
            f"/api/pipelines/{parent}/uses", json={"child_pipeline_id": child}
        ).json()["use_id"]

        source_db = get_db()
        document = export_pipeline(source_db, parent)
        assert {p["pipeline_id"] for p in document["pipelines"]} == {parent, child}
        assert any(u["use_id"] == use_id for u in document["uses"])

        target_db = self._second_db(tmp_path)
        result = import_pipeline_document(target_db, document)
        assert result["ok"] is True
        new_parent = result["pipeline_id"]

        (new_use,) = ps.get_pipeline_uses(target_db, new_parent)
        new_child = new_use["child_pipeline_id"]
        # Identity (pipeline_id) is preserved into a database that has
        # never seen it before — the id IS the portable identity.
        assert new_child == child

        child_manual = ps.get_manual_nodes(target_db, new_child)
        assert {(n["type"], n["label"]) for n in child_manual.values()} == {
            ("variableNode", "RawSignal"),
            ("functionNode", "custom_proc"),
        }

    def test_hypothesis_tag_carries_over_to_new_root(self, client, tmp_path):
        pid = client.post("/api/hypotheses", json={"name": "gait symmetry"}).json()["pipeline_id"]
        client.put(f"/api/hypotheses/{pid}", json={"research_question": "Does gait symmetry improve?"})

        source_db = get_db()
        document = export_pipeline(source_db, pid)
        assert document["hypothesis"]["research_question"] == "Does gait symmetry improve?"

        target_db = self._second_db(tmp_path)
        result = import_pipeline_document(target_db, document)

        hyps = {h["pipeline_id"]: h for h in ps.list_hypotheses(target_db)}
        assert result["pipeline_id"] in hyps
        assert hyps[result["pipeline_id"]]["research_question"] == "Does gait symmetry improve?"

    def test_bare_submodule_export_is_not_tagged_as_hypothesis(self, client, tmp_path):
        pid = client.post("/api/pipelines", json={"name": "shared_prep"}).json()["pipeline_id"]

        source_db = get_db()
        document = export_pipeline(source_db, pid)
        assert document["hypothesis"] is None

        target_db = self._second_db(tmp_path)
        result = import_pipeline_document(target_db, document)

        hyp_ids = {h["pipeline_id"] for h in ps.list_hypotheses(target_db)}
        assert result["pipeline_id"] not in hyp_ids

    def test_globals_created_fresh_when_absent_locally(
        self, client_with_variable_file, tmp_path
    ):
        # "export_gain", NOT "low_hz" — "low_hz" is the exact constant
        # name+value (20) conftest's seeded bandpass_filter already has in
        # REAL DB history, so pending "low_hz"=20 would get silently
        # stripped by auto_clean_pending_constants (an existing, correct
        # feature: pending values that duplicate real history are
        # redundant) the next time this scope's graph rebuilds — nothing
        # to do with export/import, a name collision with the fixture.
        client = client_with_variable_file
        from scistack_gui import registry

        pid = client.post("/api/pipelines", json={"name": "loading"}).json()["pipeline_id"]
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
        # "Alternate templates" is a source-code-only concept now (EachOf
        # bound to one name, see docs/claude/code-discovery-categories.md)
        # — no API endpoint authors it, so write it directly.
        with open(registry._module_path, "a") as f:
            f.write(
                '\ngait_data = EachOf(gait_data, PathInput("alt.csv"))\n'
            )
        registry.refresh_module()
        client.put("/api/layout/pi_a", json={
            "x": 0, "y": 0, "node_type": "pathInputNode", "label": "gait_data", "pipeline_id": pid,
        })
        client.post("/api/parameters", json={"name": "window_seconds", "values": [10, 20, 30]})
        client.put("/api/layout/sw_a", json={
            "x": 0, "y": 0, "node_type": "parameterNode", "label": "window_seconds", "pipeline_id": pid,
        })
        client.put("/api/parameters/export_gain/pending/20")
        client.put("/api/parameters/export_gain/pending/40")
        client.put("/api/layout/const_a", json={
            "x": 0, "y": 0, "node_type": "parameterNode", "label": "export_gain", "pipeline_id": pid,
        })

        source_db = get_db()
        document = export_pipeline(source_db, pid)
        assert document["constants"] == {"export_gain": ["20", "40"]}
        assert [p["name"] for p in document["path_inputs"]] == ["gait_data"]
        assert [s["name"] for s in document["sweeps"]] == ["window_seconds"]

        target_db = self._second_db(tmp_path)
        # Simulate a genuinely separate project: the registry is a single
        # process-wide global (not scoped per-database), so importing "into
        # another user's project" means that user's registry doesn't
        # already have gait_data/window_seconds — clear it and point
        # _module_path at a fresh file before importing, exactly like a
        # different server process with a different --module would.
        registry._path_inputs.clear()
        registry._path_input_sources.clear()
        registry._parameters.clear()
        registry._parameter_sources.clear()
        target_file = tmp_path / "other_user_vars.py"
        target_file.write_text("from scidb import EachOf, Parameter, PathInput\n")
        registry._module_path = target_file

        result = import_pipeline_document(target_db, document)
        assert result["reused"] == {"pipelines": [], "constants": [], "path_inputs": [], "sweeps": []}
        assert result["materialization_errors"] == []

        path_inputs = registry.get_path_inputs_registry()
        sweeps = registry.get_parameters_registry()

        from scistack_gui.domain.graph_builder import path_input_display

        display = path_input_display(path_inputs["gait_data"])
        assert display["template"] == "{subject}.csv"
        assert [a["template"] for a in display["alternate_templates"]] == ["alt.csv"]
        assert list(sweeps["window_seconds"].alternatives) == [10, 20, 30]
        assert set(ps.get_pending_constants(target_db).get("export_gain", set())) == {"20", "40"}


class TestSubmoduleContentDedup:
    """Identity-based reuse (user-reported, 2026-08-14): every pipeline in
    the closure — root AND submodules, at any nesting depth — carries a
    stable portable identity (its pipeline_id, preserved verbatim through
    export/import). A pipeline whose full content — recursively, through
    its own sub-submodules — is IDENTICAL to the LOCAL pipeline already
    holding that same id gets REUSED rather than duplicated as
    "{name} (imported)"; a pipeline whose id was never seen locally at
    all (e.g. a DIFFERENT pipeline that merely happens to share a NAME) is
    never a reuse candidate at all — only an id match is ever compared for
    content, so it always lands as a fresh, distinctly-named copy."""

    def _second_db(self, tmp_path):
        from scidb import configure_database

        from scistack_gui import db as gui_db

        db_path = tmp_path / "other_user.duckdb"
        db = configure_database(db_path, ["subject", "session"])
        gui_db._db = db
        gui_db._db_path = db_path
        return db

    def test_reimporting_reuses_identical_content_root_and_submodule(self, client, tmp_path):
        child = client.post("/api/pipelines", json={"name": "shared_prep"}).json()["pipeline_id"]
        client.put("/api/layout/cv_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": child,
        })
        client.put("/api/layout/cf_a", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": child,
        })
        client.put("/api/edges/ce_a", json={"source": "cv_in", "target": "cf_a"})

        parent = client.post("/api/pipelines", json={"name": "analysis"}).json()["pipeline_id"]
        client.post(f"/api/pipelines/{parent}/uses", json={"child_pipeline_id": child})

        source_db = get_db()
        document = export_pipeline(source_db, parent)

        target_db = self._second_db(tmp_path)
        first = import_pipeline_document(target_db, document)
        assert first["reused"]["pipelines"] == []  # target was empty; nothing to reuse yet

        second = import_pipeline_document(target_db, document)
        # Unchanged re-import: root AND submodule both match their own
        # local id+content and are REUSED, not duplicated.
        assert second["pipeline_id"] == first["pipeline_id"]
        names = {p["name"] for p in ps.list_pipelines(target_db)}
        assert "analysis (imported)" not in names
        assert second["reused"]["pipelines"] == ["analysis", "shared_prep"]

        (first_use,) = ps.get_pipeline_uses(target_db, first["pipeline_id"])
        (second_use,) = ps.get_pipeline_uses(target_db, second["pipeline_id"])
        assert first_use["child_pipeline_id"] == second_use["child_pipeline_id"]  # SAME shared submodule

    def test_submodule_with_same_name_but_different_identity_is_not_reused(self, client, tmp_path):
        child = client.post("/api/pipelines", json={"name": "shared_prep"}).json()["pipeline_id"]
        client.put("/api/layout/cv_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": child,
        })
        client.put("/api/layout/cf_a", json={
            "x": 10, "y": 0, "node_type": "functionNode", "label": "custom_proc", "pipeline_id": child,
        })
        client.put("/api/edges/ce_a", json={"source": "cv_in", "target": "cf_a"})

        parent = client.post("/api/pipelines", json={"name": "analysis"}).json()["pipeline_id"]
        client.post(f"/api/pipelines/{parent}/uses", json={"child_pipeline_id": child})

        source_db = get_db()
        document = export_pipeline(source_db, parent)

        target_db = self._second_db(tmp_path)
        # Pre-seed target with an UNRELATED "shared_prep" — same name, but
        # its own independently-minted id, so it's never even a reuse
        # candidate for the imported "shared_prep" (different identity).
        other_pid = ps.create_pipeline(target_db, "shared_prep")
        ps.write_manual_node(target_db, "other_node", "variableNode", "SomethingElse", other_pid)

        result = import_pipeline_document(target_db, document)
        assert result["reused"]["pipelines"] == []
        names = [p["name"] for p in ps.list_pipelines(target_db)]
        # The pre-existing one is untouched (still just "shared_prep", not
        # duplicated), and the imported one landed as a renamed copy.
        assert names.count("shared_prep") == 1
        assert "shared_prep (imported)" in names

    def test_recursive_reuse_through_nested_submodules(self, client, tmp_path):
        grandchild = client.post("/api/pipelines", json={"name": "raw_load"}).json()["pipeline_id"]
        client.put("/api/layout/gv", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": grandchild,
        })

        child = client.post("/api/pipelines", json={"name": "prep"}).json()["pipeline_id"]
        client.post(f"/api/pipelines/{child}/uses", json={"child_pipeline_id": grandchild})

        parent = client.post("/api/pipelines", json={"name": "analysis"}).json()["pipeline_id"]
        client.post(f"/api/pipelines/{parent}/uses", json={"child_pipeline_id": child})

        source_db = get_db()
        document = export_pipeline(source_db, parent)

        target_db = self._second_db(tmp_path)
        import_pipeline_document(target_db, document)  # first import: everything created fresh
        second = import_pipeline_document(target_db, document)  # second: all three levels reused

        assert set(second["reused"]["pipelines"]) == {"analysis", "prep", "raw_load"}

    def test_parent_not_reused_when_a_nested_child_differs_by_identity(self, client, tmp_path):
        """A submodule's own direct content matching isn't enough — an id
        mismatch (or content mismatch under a matching id) several levels
        down must still block reuse at every ancestor level, since an
        ancestor's own content signature recursively includes its
        children's resolved identities."""
        grandchild = client.post("/api/pipelines", json={"name": "raw_load"}).json()["pipeline_id"]
        client.put("/api/layout/gv", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": grandchild,
        })

        child = client.post("/api/pipelines", json={"name": "prep"}).json()["pipeline_id"]
        client.post(f"/api/pipelines/{child}/uses", json={"child_pipeline_id": grandchild})

        parent = client.post("/api/pipelines", json={"name": "analysis"}).json()["pipeline_id"]
        client.post(f"/api/pipelines/{parent}/uses", json={"child_pipeline_id": child})

        source_db = get_db()
        document = export_pipeline(source_db, parent)

        target_db = self._second_db(tmp_path)
        # Pre-seed target with UNRELATED "prep"/"raw_load" (different ids,
        # different content) — never reuse candidates by identity, so the
        # mismatch bubbles up to "prep" too, even though "prep" itself has
        # no other content to differ on.
        local_grandchild = ps.create_pipeline(target_db, "raw_load")
        ps.write_manual_node(target_db, "diff_node", "variableNode", "SomethingDifferent", local_grandchild)
        local_child = ps.create_pipeline(target_db, "prep")
        ps.add_pipeline_use(target_db, local_child, local_grandchild)

        result = import_pipeline_document(target_db, document)
        assert result["reused"]["pipelines"] == []
        names = [p["name"] for p in ps.list_pipelines(target_db)]
        assert "prep (imported)" in names
        assert "raw_load (imported)" in names


class TestHiddenPipelineReimport:
    """The reported bug: hiding a pipeline (user-facing "delete") must
    never block re-importing it, and must never surface
    create_pipeline's raw "already exists" ValueError — see module
    docstring's "Identity-based reuse"."""

    def _second_db(self, tmp_path):
        from scidb import configure_database

        from scistack_gui import db as gui_db

        db_path = tmp_path / "other_user.duckdb"
        db = configure_database(db_path, ["subject", "session"])
        gui_db._db = db
        gui_db._db_path = db_path
        return db

    def test_reimport_of_hidden_pipeline_unhides_instead_of_erroring(self, client):
        pid = client.post("/api/pipelines", json={"name": "test"}).json()["pipeline_id"]
        client.put("/api/layout/v_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": pid,
        })

        db = get_db()
        document = export_pipeline(db, pid)

        ps.hide_pipeline(db, pid)
        assert pid in {p["pipeline_id"] for p in ps.list_hidden_pipelines(db)}

        result = import_pipeline_document(db, document)  # must NOT raise

        assert result["ok"] is True
        assert result["pipeline_id"] == pid
        assert pid in {p["pipeline_id"] for p in ps.list_pipelines(db)}  # visible again
        assert [p["name"] for p in ps.list_pipelines(db)].count("test") == 1  # no duplicate

    def test_reimport_name_collision_with_unrelated_hidden_pipeline_is_suffixed(self, client, tmp_path):
        """The latent gap the bug report actually hit: the colliding name
        belongs to a DIFFERENT (unrelated identity) pipeline that happens
        to be hidden. Must suffix, never raise "already exists"."""
        pid = client.post("/api/pipelines", json={"name": "test"}).json()["pipeline_id"]
        client.put("/api/layout/v_in", json={
            "x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal", "pipeline_id": pid,
        })

        source_db = get_db()
        document = export_pipeline(source_db, pid)

        target_db = self._second_db(tmp_path)
        # An UNRELATED local pipeline, hidden, that just happens to share
        # the desired name — different identity, so never a reuse
        # candidate, but its name must still be avoided.
        other_pid = ps.create_pipeline(target_db, "test")
        ps.write_manual_node(target_db, "other_node", "variableNode", "SomethingElse", other_pid)
        ps.create_pipeline(target_db, "sibling")  # keep >1 visible so hiding is legal
        ps.hide_pipeline(target_db, other_pid)

        result = import_pipeline_document(target_db, document)  # must NOT raise

        assert result["ok"] is True
        names = [p["name"] for p in ps.list_pipelines(target_db)]
        assert names.count("test") == 0  # the hidden original stays hidden, not counted here
        assert "test (imported)" in names
