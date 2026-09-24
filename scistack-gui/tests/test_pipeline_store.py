"""
Unit tests for scistack_gui.pipeline_store's hidden-combo and hidden-edge
primitives.

Hiding must never delete data -- these tests confirm hide_combo/unhide_combo
compose with (not duplicate) the existing hide_node/unhide_node hidden-set,
and that the structural variant_key survives a hide/unhide round trip. The
hidden-edges table (hide_edge/unhide_edge) is its own independent set --
hiding an edge keeps the node itself on screen (red), unlike hiding a node.
"""

from scistack_gui import pipeline_store


class TestHiddenCombos:
    def test_hide_combo_lands_in_hidden_node_ids(self, populated_db):
        db = populated_db
        node_id = "fn__bandpass_filter__abc123"
        pipeline_store.hide_combo(db, node_id, "bandpass_filter", {"low_hz": "20"})
        assert node_id in pipeline_store.get_hidden_node_ids(db)

    def test_list_hidden_combos_round_trip(self, populated_db):
        db = populated_db
        node_id = "fn__bandpass_filter__abc123"
        pipeline_store.hide_combo(db, node_id, "bandpass_filter", {"low_hz": "20"})
        combos = pipeline_store.list_hidden_combos(db, "bandpass_filter")
        assert combos == [{"node_id": node_id, "variant_key": {"low_hz": "20"}}]

    def test_list_hidden_combos_scoped_to_function(self, populated_db):
        db = populated_db
        pipeline_store.hide_combo(
            db, "fn__bandpass_filter__abc123", "bandpass_filter", {"low_hz": "20"}
        )
        pipeline_store.hide_combo(db, "fn__other_fn__def456", "other_fn", {"k": "1"})
        combos = pipeline_store.list_hidden_combos(db, "bandpass_filter")
        assert len(combos) == 1
        assert combos[0]["node_id"] == "fn__bandpass_filter__abc123"

    def test_unhide_combo_removes_from_both_tables(self, populated_db):
        db = populated_db
        node_id = "fn__bandpass_filter__abc123"
        pipeline_store.hide_combo(db, node_id, "bandpass_filter", {"low_hz": "20"})
        pipeline_store.unhide_combo(db, node_id)
        assert node_id not in pipeline_store.get_hidden_node_ids(db)
        assert pipeline_store.list_hidden_combos(db, "bandpass_filter") == []

    def test_hide_combo_idempotent(self, populated_db):
        db = populated_db
        node_id = "fn__bandpass_filter__abc123"
        pipeline_store.hide_combo(db, node_id, "bandpass_filter", {"low_hz": "20"})
        pipeline_store.hide_combo(db, node_id, "bandpass_filter", {"low_hz": "20"})
        assert len(pipeline_store.list_hidden_combos(db, "bandpass_filter")) == 1

    def test_unhide_node_alone_leaves_stale_structural_row(self, populated_db):
        """Calling the OLD whole-node unhide_node directly (bypassing
        unhide_combo) doesn't clean up _pipeline_hidden_combos -- documents
        that callers must go through unhide_combo, not unhide_node, to
        restore a hidden combo. get_hidden_node_ids always unions in
        _pipeline_hidden_combos (plan-scope-hidden-nodes-edges.md) precisely
        so this half-restore can't make the node silently reappear on
        canvas while the restore panel still lists it as hidden -- it must
        still report the node hidden until unhide_combo cleans up both
        tables."""
        db = populated_db
        node_id = "fn__bandpass_filter__abc123"
        pipeline_store.hide_combo(db, node_id, "bandpass_filter", {"low_hz": "20"})
        pipeline_store.unhide_node(db, node_id)
        assert node_id in pipeline_store.get_hidden_node_ids(db)
        assert pipeline_store.list_hidden_combos(db, "bandpass_filter") == [
            {"node_id": node_id, "variant_key": {"low_hz": "20"}}
        ]


class TestHiddenParameterValues:
    """hide_parameter_value/unhide_parameter_value never delete data -- they
    persist a (pipeline_id, const_name, value) exclusion, a coarser
    granularity than hide_combo (every call site using the pair, across
    every function, not just one function's one Cartesian-product row)."""

    def test_round_trip(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_value(db, "low_hz", "20")
        assert pipeline_store.list_hidden_parameter_values(db) == [
            {"const_name": "low_hz", "value": "20"}
        ]

    def test_unhide_removes_it(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_value(db, "low_hz", "20")
        pipeline_store.unhide_parameter_value(db, "low_hz", "20")
        assert pipeline_store.list_hidden_parameter_values(db) == []

    def test_hide_idempotent(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_value(db, "low_hz", "20")
        pipeline_store.hide_parameter_value(db, "low_hz", "20")
        assert len(pipeline_store.list_hidden_parameter_values(db)) == 1

    def test_unhide_idempotent(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_value(db, "low_hz", "20")
        pipeline_store.unhide_parameter_value(db, "low_hz", "20")
        pipeline_store.unhide_parameter_value(db, "low_hz", "20")
        assert pipeline_store.list_hidden_parameter_values(db) == []

    def test_scoped_to_pipeline_id(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_value(db, "low_hz", "20", pipeline_id="main")
        pipeline_store.hide_parameter_value(db, "low_hz", "30", pipeline_id="other")
        assert pipeline_store.list_hidden_parameter_values(db, "main") == [
            {"const_name": "low_hz", "value": "20"}
        ]
        assert pipeline_store.list_hidden_parameter_values(db, "other") == [
            {"const_name": "low_hz", "value": "30"}
        ]

    def test_none_pipeline_id_unions_all_scopes(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_value(db, "low_hz", "20", pipeline_id="main")
        pipeline_store.hide_parameter_value(db, "low_hz", "30", pipeline_id="other")
        all_hidden = pipeline_store.list_hidden_parameter_values(db, None)
        assert len(all_hidden) == 2

    def test_different_values_are_independent(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_value(db, "low_hz", "20")
        pipeline_store.hide_parameter_value(db, "low_hz", "30")
        pipeline_store.unhide_parameter_value(db, "low_hz", "20")
        assert pipeline_store.list_hidden_parameter_values(db) == [
            {"const_name": "low_hz", "value": "30"}
        ]


class TestBulkHiddenParameterValues:
    """The generated-set checkbox toggles every member at once, so it must
    do it in one statement rather than one call per value."""

    def test_hide_many_in_one_call(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_values(db, "low_hz", ["10", "20", "30"])
        assert {
            r["value"] for r in pipeline_store.list_hidden_parameter_values(db)
        } == {"10", "20", "30"}

    def test_unhide_many_leaves_others_hidden(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_values(db, "low_hz", ["10", "20", "30"])
        pipeline_store.hide_parameter_value(db, "other", "99")
        pipeline_store.unhide_parameter_values(db, "low_hz", ["10", "30"])
        remaining = {
            (r["const_name"], r["value"])
            for r in pipeline_store.list_hidden_parameter_values(db)
        }
        assert remaining == {("low_hz", "20"), ("other", "99")}

    def test_empty_list_is_a_noop(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_values(db, "low_hz", [])
        pipeline_store.unhide_parameter_values(db, "low_hz", [])
        assert pipeline_store.list_hidden_parameter_values(db) == []

    def test_bulk_hide_is_idempotent(self, populated_db):
        db = populated_db
        pipeline_store.hide_parameter_values(db, "low_hz", ["10", "20"])
        pipeline_store.hide_parameter_values(db, "low_hz", ["10", "20"])
        assert len(pipeline_store.list_hidden_parameter_values(db)) == 2


class TestParameterValueGroups:
    """Which values came from the Generate section. GUI display state, kept
    out of source so the declaration stays a flat list in every language."""

    def test_round_trip(self, populated_db):
        db = populated_db
        pipeline_store.set_parameter_value_group(
            db,
            "low_hz",
            kind="range",
            spec={"start": 0, "end": 6, "step": 2},
            values=["0", "2", "4", "6"],
        )
        groups = pipeline_store.get_parameter_value_groups(db)
        assert groups["low_hz"]["kind"] == "range"
        assert groups["low_hz"]["spec"] == {"start": 0, "end": 6, "step": 2}
        assert groups["low_hz"]["values"] == ["0", "2", "4", "6"]

    def test_one_group_per_parameter(self, populated_db):
        db = populated_db
        pipeline_store.set_parameter_value_group(
            db, "low_hz", kind="range", spec={}, values=["1"]
        )
        pipeline_store.set_parameter_value_group(
            db, "low_hz", kind="list", spec={"members": [7, 8]}, values=["7", "8"]
        )
        groups = pipeline_store.get_parameter_value_groups(db)
        assert len(groups) == 1
        assert groups["low_hz"]["kind"] == "list"
        assert groups["low_hz"]["values"] == ["7", "8"]

    def test_clear_removes_only_that_parameter(self, populated_db):
        db = populated_db
        pipeline_store.set_parameter_value_group(
            db, "a", kind="list", spec={}, values=["1"]
        )
        pipeline_store.set_parameter_value_group(
            db, "b", kind="list", spec={}, values=["2"]
        )
        pipeline_store.clear_parameter_value_group(db, "a")
        assert list(pipeline_store.get_parameter_value_groups(db)) == ["b"]

    def test_empty_by_default(self, populated_db):
        assert pipeline_store.get_parameter_value_groups(populated_db) == {}


class TestHiddenEdges:
    """hide_edge/unhide_edge never delete data -- they're a completely
    separate table from hidden NODES (hiding an edge keeps the node on
    screen, red -- see graph_builder.hidden_wirings), so these tests also
    confirm the two hidden-sets don't leak into each other."""

    EDGE_ID = "e__RawSignal__bandpass_filter__abc123"

    def test_hide_edge_round_trip(self, populated_db):
        db = populated_db
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123"
        )
        assert self.EDGE_ID in pipeline_store.get_hidden_edge_ids(db)

    def test_hide_edge_does_not_touch_hidden_nodes(self, populated_db):
        db = populated_db
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123"
        )
        assert pipeline_store.get_hidden_node_ids(db) == set()

    def test_list_hidden_edges_round_trip(self, populated_db):
        db = populated_db
        pipeline_store.hide_edge(
            db,
            self.EDGE_ID,
            "var__RawSignal",
            "fn__bandpass_filter__abc123",
            source_handle=None,
            target_handle="in__signal",
        )
        edges = pipeline_store.list_hidden_edges(db)
        assert edges == [
            {
                "edge_id": self.EDGE_ID,
                "source": "var__RawSignal",
                "target": "fn__bandpass_filter__abc123",
                "source_handle": None,
                "target_handle": "in__signal",
            }
        ]

    def test_unhide_edge_removes_it(self, populated_db):
        db = populated_db
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123"
        )
        pipeline_store.unhide_edge(db, self.EDGE_ID)
        assert self.EDGE_ID not in pipeline_store.get_hidden_edge_ids(db)
        assert pipeline_store.list_hidden_edges(db) == []

    def test_hide_edge_idempotent(self, populated_db):
        db = populated_db
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123"
        )
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123"
        )
        assert len(pipeline_store.list_hidden_edges(db)) == 1

    def test_unhide_nonexistent_edge_is_a_noop(self, populated_db):
        db = populated_db
        pipeline_store.unhide_edge(db, "does_not_exist")  # must not raise
        assert pipeline_store.get_hidden_edge_ids(db) == set()

    def test_get_hidden_edge_ids_empty_by_default(self, populated_db):
        db = populated_db
        assert pipeline_store.get_hidden_edge_ids(db) == set()


class TestEnsureTablesIdempotent:
    """_ensure_tables is called on every request path (get_db() dependency),
    not just at startup, so it must stay safe to call repeatedly against an
    already-initialized database without erroring or losing data.

    This used to be a real bug: _ensure_tables ran ALTER TABLE ADD COLUMN
    migration steps guarded only by a bare try/except, and on a DB that
    already had those columns (i.e. every call after the first) they failed
    with CatalogException every time. Catching the Python exception did not
    undo DuckDB's connection-level aborted-transaction state, so the next
    *unguarded* statement inherited that state and raised
    TransactionException -- which then never cleared, breaking every
    subsequent query against the shared connection for the rest of the
    process. Since this is a beta project with no installed base to
    migrate, the fix was to drop the ALTER-based migration steps entirely
    and give every table its final schema at CREATE time (no more
    already-exists case to hit). The underlying connection-recovery
    mechanism (so a future migration's failure can't cascade the same way)
    is regression-tested at the sciduckdb layer instead --
    see sciduckdb/tests/test_sciduck.py::TestAutocommitFailureRecovery.
    """

    def test_ensure_tables_survives_repeated_calls(self, populated_db):
        db = populated_db
        # populated_db's fixture already called _ensure_tables once.
        pipeline_store._ensure_tables(db)
        pipeline_store._ensure_tables(db)

        assert pipeline_store.get_hidden_node_ids(db) == set()
        assert pipeline_store.list_hidden_edges(db) == []

    def test_repeated_calls_preserve_existing_hidden_nodes(self, populated_db):
        db = populated_db
        node_id = "fn__bandpass_filter__abc123"
        pipeline_store.hide_combo(db, node_id, "bandpass_filter", {"low_hz": "20"})

        pipeline_store._ensure_tables(db)  # called again, as on every request

        assert node_id in pipeline_store.get_hidden_node_ids(db)


class TestScopedHiding:
    """hide_node/hide_edge are scoped per pipeline_id (plan-scope-hidden-
    nodes-edges.md) -- a canonical id is scope-INDEPENDENT (graph_builder.
    wiring_id hashes only fn name + input/output shape), so two pipeline
    scopes can independently place the SAME id; hiding it in one must not
    hide it in the other. ``pipeline_id=None`` on the getters still returns
    every scope's hidden ids unioned, for the (not yet scope-aware)
    execution-readiness callers."""

    NODE_ID = "var__FilteredSignal"
    EDGE_ID = "e__RawSignal__bandpass_filter__abc123"

    def test_hide_node_scoped_to_one_pipeline(self, populated_db):
        db = populated_db
        pipeline_store.hide_node(db, self.NODE_ID, "pipe_a")
        assert self.NODE_ID in pipeline_store.get_hidden_node_ids(db, "pipe_a")
        assert self.NODE_ID not in pipeline_store.get_hidden_node_ids(db, "main")
        assert self.NODE_ID not in pipeline_store.get_hidden_node_ids(db, "pipe_b")
        # Unscoped getter still sees it (execution-path compatibility).
        assert self.NODE_ID in pipeline_store.get_hidden_node_ids(db)

    def test_hide_node_in_two_scopes_independently(self, populated_db):
        db = populated_db
        pipeline_store.hide_node(db, self.NODE_ID, "pipe_a")
        pipeline_store.hide_node(db, self.NODE_ID, "pipe_b")
        pipeline_store.unhide_node(db, self.NODE_ID, "pipe_a")
        # Unhiding in pipe_a must not affect pipe_b's independent hide.
        assert self.NODE_ID not in pipeline_store.get_hidden_node_ids(db, "pipe_a")
        assert self.NODE_ID in pipeline_store.get_hidden_node_ids(db, "pipe_b")

    def test_unhide_nodes_by_prefix_scoped(self, populated_db):
        db = populated_db
        pipeline_store.hide_node(db, "fn__bandpass_filter__abc123", "pipe_a")
        pipeline_store.hide_node(db, "fn__bandpass_filter__def456", "pipe_b")
        pipeline_store.unhide_nodes_by_prefix(db, "fn__bandpass_filter__", "pipe_a")
        assert pipeline_store.get_hidden_node_ids(db, "pipe_a") == set()
        assert "fn__bandpass_filter__def456" in pipeline_store.get_hidden_node_ids(
            db, "pipe_b"
        )

    def test_hide_edge_scoped_to_one_pipeline(self, populated_db):
        db = populated_db
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123",
            pipeline_id="pipe_a",
        )
        assert self.EDGE_ID in pipeline_store.get_hidden_edge_ids(db, "pipe_a")
        assert self.EDGE_ID not in pipeline_store.get_hidden_edge_ids(db, "main")
        assert self.EDGE_ID in pipeline_store.get_hidden_edge_ids(db)  # unscoped union

    def test_unhide_edge_scoped_leaves_other_scope_hidden(self, populated_db):
        db = populated_db
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123",
            pipeline_id="pipe_a",
        )
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123",
            pipeline_id="pipe_b",
        )
        pipeline_store.unhide_edge(db, self.EDGE_ID, "pipe_a")
        assert self.EDGE_ID not in pipeline_store.get_hidden_edge_ids(db, "pipe_a")
        assert self.EDGE_ID in pipeline_store.get_hidden_edge_ids(db, "pipe_b")

    def test_list_hidden_edges_scoped(self, populated_db):
        db = populated_db
        pipeline_store.hide_edge(
            db, self.EDGE_ID, "var__RawSignal", "fn__bandpass_filter__abc123",
            pipeline_id="pipe_a",
        )
        assert len(pipeline_store.list_hidden_edges(db, "pipe_a")) == 1
        assert pipeline_store.list_hidden_edges(db, "pipe_b") == []
        assert len(pipeline_store.list_hidden_edges(db)) == 1  # unscoped union

    def test_combo_hide_still_visible_in_every_scope(self, populated_db):
        """Pending-constant combo hides remain intentionally global (not
        yet scoped -- see plan-scope-hidden-nodes-edges.md follow-up), so
        a combo hidden via hide_combo must still show up for ANY scope's
        get_hidden_node_ids, not just 'main'."""
        db = populated_db
        node_id = "fn__bandpass_filter__abc123"
        pipeline_store.hide_combo(db, node_id, "bandpass_filter", {"low_hz": "20"})
        assert node_id in pipeline_store.get_hidden_node_ids(db, "pipe_a")
        assert node_id in pipeline_store.get_hidden_node_ids(db, "main")


class TestNodeConfig:
    """Node config must persist for EVERY node id, not just placed ones.

    update_node_config used to be a bare
    ``UPDATE _pipeline_nodes ... WHERE node_id = ?``. A node that had already
    run has no row in that table -- its id is the composite
    ``fn__{fn}__{call_id}`` -- so the UPDATE matched nothing, affected zero
    rows, and reported success. The frontend cleared its dirty flag and the
    setting was gone on the next rebuild, which is what "the distribute
    checkbox doesn't stay checked" actually was (2026-09-14).

    See docs/claude/gui-run-options-flow.md.
    """

    DERIVED_ID = "fn__loadGaitRiteOneFile__a1b2c3d4"

    def test_config_round_trips_for_a_node_with_no_pipeline_nodes_row(
        self, populated_db
    ):
        db = populated_db
        assert self.DERIVED_ID not in pipeline_store.get_manual_nodes(db)

        pipeline_store.update_node_config(
            db, self.DERIVED_ID, {"runOptions": {"distribute": True}}
        )

        cfg = pipeline_store.get_node_config(db, self.DERIVED_ID)
        assert cfg["runOptions"]["distribute"] is True

    def test_config_survives_rewrite(self, populated_db):
        db = populated_db
        pipeline_store.update_node_config(
            db, self.DERIVED_ID, {"runOptions": {"distribute": True}}
        )
        pipeline_store.update_node_config(
            db, self.DERIVED_ID, {"runOptions": {"distribute": False, "save": False}}
        )
        cfg = pipeline_store.get_node_config(db, self.DERIVED_ID)
        assert cfg["runOptions"] == {"distribute": False, "save": False}

    def test_missing_config_is_empty_not_an_error(self, populated_db):
        assert pipeline_store.get_node_config(populated_db, "fn__nope__0000") == {}

    def test_get_node_configs_collects_every_node(self, populated_db):
        db = populated_db
        pipeline_store.update_node_config(db, self.DERIVED_ID, {"schemaLevel": ["subject"]})
        pipeline_store.update_node_config(db, "fn__other__ffff", {"whereFilters": []})
        configs = pipeline_store.get_node_configs(db)
        assert configs[self.DERIVED_ID] == {"schemaLevel": ["subject"]}
        assert "fn__other__ffff" in configs

    def test_manual_node_config_is_overlaid_onto_get_manual_nodes(self, populated_db):
        """Export, copy/paste and the graph build all read node config through
        get_manual_nodes. The overlay is what keeps them from each needing to
        learn about the _node_config split (and one of them being missed)."""
        db = populated_db
        node_id = "fn__placed_fn__manual1"
        pipeline_store.write_manual_node(db, node_id, "functionNode", "placed_fn", "main")
        pipeline_store.update_node_config(
            db, node_id, {"runOptions": {"distribute": True}}
        )
        nodes = pipeline_store.get_manual_nodes(db)
        assert nodes[node_id]["config"]["runOptions"]["distribute"] is True

    def test_write_is_logged_with_its_node_id(self, populated_db, caplog):
        import logging

        with caplog.at_level(logging.INFO):
            pipeline_store.update_node_config(
                populated_db, self.DERIVED_ID, {"runOptions": {"distribute": True}}
            )
        assert self.DERIVED_ID in caplog.text


class TestGraduationMovesNodeConfig:
    """Graduation moves position and edges; it must move the saved config
    too, or the settings the user made on a fresh node are orphaned under
    an id that no longer exists on the canvas — the "saved node config(s)
    match no node" WARN. See .claude/plan-graduation-config-migration.md.

    The SEAM is ``graduate_manual_node``, and that matters since every
    execution-intent aspect graduated to the intent store: a config written
    through ``update_node_config`` is split, with ``columnSelections`` /
    ``runOptions`` / the three schema-location keys becoming statements and
    only the rest staying in the ``_node_config`` blob. So
    ``migrate_node_config`` alone moves only the blob remainder — the
    statements move via ``intent_store.rekey_subject``, and the tests below
    ask the entry point that does both.
    """

    OLD = "fn__grSides__5c9r0r"
    NEW = "fn__grSides__a50088e2da549e1b::main"

    def _fresh(self, db, node_id=None, config=None, key=None):
        node_id = node_id or self.OLD
        pipeline_store.write_manual_node(db, node_id, "functionNode", "grSides", "main")
        if config is not None:
            pipeline_store.update_node_config(db, key or node_id, config)

    def test_bare_config_moves_to_the_graduated_id(self, populated_db):
        db = populated_db
        self._fresh(db, config={"schemaLevel": ["subject"], "runOptions": {"distribute": True}})

        pipeline_store.graduate_manual_node(db, self.OLD, self.NEW)

        assert pipeline_store.get_node_config(db, self.NEW) == {
            "schemaLevel": ["subject"],
            "runOptions": {"distribute": True},
        }
        assert pipeline_store.get_node_config(db, self.OLD) == {}, (
            "a move, not a copy — the orphan WARN must stop naming this id"
        )

    def test_scope_qualified_source_key_moves_too(self, populated_db):
        """A config written through a ``::scope``-qualified id is found and
        moved — and it lands on the canvas it was MADE on.

        Both halves matter, and the second one changed under us. While a
        config was a blob, ``fn__x::sub1`` was just another spelling of the
        key, so migration carried it wholesale onto the graduated id
        whatever canvas that id named. Since scope became a column of its
        own (``intent_store.scope_of_node``, commit a5de1c0d — "a statement
        is made on a canvas and applies there"), ``rekey_subject`` moves the
        SUBJECT and leaves the scope alone: the statement is still about
        canvas ``sub1`` afterwards, and reading the graduated node on
        ``main`` must not see it.

        This test still asserted the blob-era outcome and had been failing
        since that commit.
        """
        from scistack_gui.ids import strip_placement

        db = populated_db
        bare_new = strip_placement(self.NEW)
        self._fresh(db, config={"whereFilters": [{"a": 1}]}, key=f"{self.OLD}::sub1")

        pipeline_store.graduate_manual_node(db, self.OLD, self.NEW)

        assert pipeline_store.get_node_config(db, f"{bare_new}::sub1")[
            "whereFilters"
        ] == [{"a": 1}], "the qualified source key was not found, or not moved"
        assert "whereFilters" not in pipeline_store.get_node_config(db, self.NEW), (
            "a statement made on sub1 must not apply on main"
        )
        assert pipeline_store.get_node_config(db, f"{self.OLD}::sub1") == {}, (
            "a move, not a copy — the orphan WARN must stop naming this id"
        )

    def test_a_setting_is_keyed_by_the_node_not_its_placement(self, populated_db):
        """A statement's subject is the BARE node id (``rekey_subject``
        strips the placement), so configuring the same node through two
        ``::scope``-qualified ids is two writes to ONE setting and the later
        wins. Before the graduation to statements these were two rows and
        the qualified one was preferred; the outcome here is the same, the
        reason is simpler."""
        db = populated_db
        self._fresh(db, config={"schemaLevel": ["subject"]})
        pipeline_store.update_node_config(db, f"{self.OLD}::main", {"schemaLevel": ["session"]})
        pipeline_store.graduate_manual_node(db, self.OLD, self.NEW)
        assert pipeline_store.get_node_config(db, self.NEW)["schemaLevel"] == ["session"]

    def test_fresh_node_wins_and_previous_value_is_logged(self, populated_db, caplog):
        """The fresh node's location statement REPLACES the graduated id's,
        whole — not merged key by key as the old blob was.

        Where a node runs is one decision with three parts
        (``schemaSelection`` / ``schemaLevel`` / ``whereFilters``), and the
        panel always writes all three together from the node's own state, so
        the fresh node's statement is the complete intent and merging half of
        an older one into it would produce a location the user never asked
        for. (Contrast ``columnSelections``, which IS per-key: those are
        authored one parameter at a time.) Nothing is lost silently — the
        replaced value is logged verbatim.
        """
        import logging

        db = populated_db
        pipeline_store.update_node_config(
            db, self.NEW, {"schemaLevel": ["session"], "whereFilters": [{"old": 1}]}
        )
        self._fresh(db, config={"schemaLevel": ["subject"]})

        with caplog.at_level(logging.INFO, logger="scistack_gui"):
            pipeline_store.graduate_manual_node(db, self.OLD, self.NEW)

        cfg = pipeline_store.get_node_config(db, self.NEW)
        assert cfg["schemaLevel"] == ["subject"], "the fresh node's setting wins"
        assert "whereFilters" not in cfg, (
            "the whole location statement is replaced, not merged per key"
        )
        assert "['session']" in caplog.text, "the replaced value is in the log verbatim"
        assert "'whereFilters': [{'old': 1}]" in caplog.text, (
            "what the replacement dropped must be visible, not silent"
        )

    def test_nothing_to_move_is_a_no_op(self, populated_db):
        db = populated_db
        self._fresh(db)
        pipeline_store.update_node_config(db, self.NEW, {"schemaLevel": ["session"]})
        assert pipeline_store.migrate_node_config(db, self.OLD, self.NEW) == {
            "moved": [],
            "replaced": {},
        }
        assert pipeline_store.get_node_config(db, self.NEW) == {"schemaLevel": ["session"]}

    def test_graduate_manual_node_moves_config_before_deleting_the_row(self, populated_db):
        db = populated_db
        self._fresh(db)
        pipeline_store.update_node_config(db, self.OLD, {"schemaLevel": ["subject"]})
        pipeline_store.graduate_manual_node(db, self.OLD, self.NEW)
        assert self.OLD not in pipeline_store.get_manual_nodes(db)
        assert pipeline_store.get_node_config(db, self.NEW) == {"schemaLevel": ["subject"]}
