"""The intent store: one table, one shape, for statements about runs.

Stage 5 of `.claude/plan-intent-and-fact.md`; model in
`docs/claude/intent-and-fact.md`. The `columns` aspect has graduated out of
`_node_config` into `_intent`, and these pin what that move has to preserve:

* a selection saved on a node is still what the panel shows;
* it is still what the run reads;
* graduation MOVES it, duplication COPIES it, and neither leaks into the
  other's node.
"""

from __future__ import annotations

from scidb.intent import (
    ASPECT_COLUMNS,
    GLOBAL_SCOPE,
    ORIGIN_GUI,
    SUBJECT_CALL_SITE,
    Statement,
    resolve,
)
from scistack_gui import intent_store, pipeline_store

NODE = "fn__trial_mean_symmetry__0123456789abcdef"
OTHER = "fn__trial_mean_symmetry__fedcba9876543210"


def _sel(*columns, iterate=False):
    return {"columns": list(columns), "iterate": iterate}


class TestRoundTrip:
    def test_a_selection_is_stored_per_parameter(self, populated_db):
        intent_store.set_column_selections(
            populated_db, NODE, {"cycles": _sel("ankle", "knee")}
        )
        statements = intent_store.load_statements(
            populated_db, aspect=ASPECT_COLUMNS, subject_refs=[NODE]
        )
        assert [s.key for s in statements] == ["cycles"]
        assert statements[0].value == _sel("ankle", "knee")
        assert statements[0].subject_kind == SUBJECT_CALL_SITE
        assert statements[0].scope == GLOBAL_SCOPE

    def test_two_parameters_are_two_statements(self, populated_db):
        intent_store.set_column_selections(
            populated_db, NODE, {"a": _sel("x"), "b": _sel(iterate=True)}
        )
        statements = intent_store.load_statements(
            populated_db, aspect=ASPECT_COLUMNS, subject_refs=[NODE]
        )
        assert sorted(s.key for s in statements) == ["a", "b"]

    def test_writing_again_replaces_rather_than_accumulates(self, populated_db):
        intent_store.set_column_selections(populated_db, NODE, {"a": _sel("x")})
        intent_store.set_column_selections(populated_db, NODE, {"a": _sel("y")})
        statements = intent_store.load_statements(
            populated_db, aspect=ASPECT_COLUMNS, subject_refs=[NODE]
        )
        assert len(statements) == 1
        assert statements[0].value == _sel("y")

    def test_an_empty_selection_stores_nothing(self, populated_db):
        """`{"columns": [], "iterate": False}` means the whole variable, which
        is what binding the bare class already does."""
        intent_store.set_column_selections(populated_db, NODE, {"a": _sel()})
        assert (
            intent_store.load_statements(
                populated_db, aspect=ASPECT_COLUMNS, subject_refs=[NODE]
            )
            == []
        )

    def test_the_placement_suffix_is_not_part_of_the_subject(self, populated_db):
        """Placement lives in `scope`, its own column — which is what retires
        the placement-id trap rather than working around it."""
        intent_store.set_column_selections(
            populated_db, f"{NODE}::main", {"cycles": _sel("ankle")}
        )
        statements = intent_store.load_statements(
            populated_db, aspect=ASPECT_COLUMNS, subject_refs=[NODE]
        )
        assert [s.subject_ref for s in statements] == [NODE]


class TestThroughNodeConfig:
    """The panel still reads and writes one blob; only the storage moved."""

    def test_a_saved_selection_comes_back_for_display(self, populated_db):
        pipeline_store.update_node_config(
            populated_db, NODE, {"columnSelections": {"cycles": _sel("ankle")}}
        )
        assert pipeline_store.get_node_config(populated_db, NODE) == {
            "columnSelections": {"cycles": _sel("ankle")}
        }

    def test_the_blob_no_longer_owns_it(self, populated_db):
        """One owner per aspect: every graduated aspect is stripped on the way
        in, so `_node_config` cannot drift from `_intent` — and with every
        aspect graduated, the blob is empty."""
        pipeline_store.update_node_config(
            populated_db,
            NODE,
            {"columnSelections": {"cycles": _sel("ankle")}, "schemaLevel": ["subject"]},
        )
        raw = pipeline_store._duck(populated_db)._fetchone(
            "SELECT config FROM _node_config WHERE node_id = ?", [NODE]
        )
        assert raw[0] == "{}"
        config = pipeline_store.get_node_config(populated_db, NODE)
        assert config["schemaLevel"] == ["subject"]
        assert config["columnSelections"] == {"cycles": _sel("ankle")}

    def test_other_settings_are_untouched(self, populated_db):
        pipeline_store.update_node_config(
            populated_db,
            NODE,
            {"columnSelections": {"cycles": _sel("ankle")}, "schemaLevel": ["subject"]},
        )
        config = pipeline_store.get_node_config(populated_db, NODE)
        assert config["schemaLevel"] == ["subject"]
        assert config["columnSelections"] == {"cycles": _sel("ankle")}

    def test_a_node_with_no_config_row_still_reports_its_selection(self, populated_db):
        intent_store.set_column_selections(populated_db, NODE, {"cycles": _sel("ankle")})
        configs = pipeline_store.get_node_configs(populated_db)
        assert configs[NODE]["columnSelections"] == {"cycles": _sel("ankle")}


class TestMoveAndCopy:
    def test_graduation_moves_the_statement(self, populated_db):
        manual = "fn__trial_mean_symmetry__a1b2c3"
        intent_store.set_column_selections(populated_db, manual, {"cycles": _sel("ankle")})
        pipeline_store.graduate_manual_node(populated_db, manual, NODE)

        assert intent_store.load_statements(populated_db, subject_refs=[manual]) == []
        moved = intent_store.load_statements(populated_db, subject_refs=[NODE])
        assert [s.value for s in moved] == [_sel("ankle")]

    def test_graduation_lets_the_fresh_node_win(self, populated_db):
        """The realistic conflict: a wired fresh node the user configured and
        ran, whose settings produced the very history it graduates into.
        After graduation it must run the way it just ran."""
        manual = "fn__trial_mean_symmetry__a1b2c3"
        intent_store.set_column_selections(populated_db, NODE, {"cycles": _sel("knee")})
        intent_store.set_column_selections(populated_db, manual, {"cycles": _sel("ankle")})
        intent_store.rekey_subject(populated_db, manual, NODE)

        kept = intent_store.load_statements(populated_db, subject_refs=[NODE])
        assert [s.value for s in kept] == [_sel("ankle")]

    def test_rekey_can_keep_the_existing_statement_instead(self, populated_db):
        manual = "fn__trial_mean_symmetry__a1b2c3"
        intent_store.set_column_selections(populated_db, NODE, {"cycles": _sel("knee")})
        intent_store.set_column_selections(populated_db, manual, {"cycles": _sel("ankle")})
        intent_store.rekey_subject(populated_db, manual, NODE, old_wins=False)

        kept = intent_store.load_statements(populated_db, subject_refs=[NODE])
        assert [s.value for s in kept] == [_sel("knee")]

    def test_duplication_copies_and_the_copy_is_independent(self, populated_db):
        intent_store.set_column_selections(populated_db, NODE, {"cycles": _sel("ankle")})
        intent_store.copy_subject(populated_db, NODE, OTHER)
        intent_store.set_column_selections(populated_db, OTHER, {"cycles": _sel("hip")})

        original = intent_store.load_statements(populated_db, subject_refs=[NODE])
        copy = intent_store.load_statements(populated_db, subject_refs=[OTHER])
        assert [s.value for s in original] == [_sel("ankle")]
        assert [s.value for s in copy] == [_sel("hip")]

    def test_copying_a_scope_leaves_the_source_alone(self, populated_db):
        intent_store.put_statements(
            populated_db,
            [
                Statement(
                    subject_kind=SUBJECT_CALL_SITE,
                    subject_ref=NODE,
                    aspect=ASPECT_COLUMNS,
                    value=_sel("ankle"),
                    key="cycles",
                    scope="pipe_aaa",
                )
            ],
        )
        assert intent_store.copy_scope(populated_db, "pipe_aaa", "pipe_bbb") == 1
        scopes = {s.scope for s in intent_store.load_statements(populated_db)}
        assert {"pipe_aaa", "pipe_bbb"} <= scopes


class TestResolution:
    """The store feeds `scidb.intent.resolve`; these pin the two rules that
    only matter once statements carry a scope."""

    def test_another_scopes_statement_does_not_apply(self, populated_db):
        intent_store.put_statements(
            populated_db,
            [
                Statement(
                    subject_kind=SUBJECT_CALL_SITE,
                    subject_ref=NODE,
                    aspect=ASPECT_COLUMNS,
                    value=_sel("theirs"),
                    key="cycles",
                    scope="pipe_other",
                )
            ],
        )
        statements = intent_store.load_statements(populated_db, subject_refs=[NODE])
        plan = resolve(statements, None, origin=ORIGIN_GUI, scope="pipe_mine")
        assert plan.columns == {}

    def test_a_global_statement_applies_in_every_scope(self, populated_db):
        intent_store.set_column_selections(populated_db, NODE, {"cycles": _sel("ankle")})
        statements = intent_store.load_statements(populated_db, subject_refs=[NODE])
        plan = resolve(statements, None, origin=ORIGIN_GUI, scope="pipe_mine")
        assert plan.columns == {"cycles": _sel("ankle")}


class TestImport:
    def test_the_one_time_import_does_not_resurrect_a_deletion(self, populated_db):
        """The marker makes the import idempotent: a selection deleted after
        it ran must stay deleted on the next start."""
        intent_store.set_column_selections(populated_db, NODE, {"cycles": _sel("ankle")})
        intent_store.clear_aspect(populated_db, NODE, ASPECT_COLUMNS)
        intent_store.ensure_tables(populated_db)
        assert intent_store.load_statements(populated_db, subject_refs=[NODE]) == []


# ---------------------------------------------------------------------------
# Every other execution-intent aspect, through pipeline_store's unchanged API
# ---------------------------------------------------------------------------


class TestRunOptionsAndSchemaLocation:
    def test_run_options_round_trip_through_the_blob(self, populated_db):
        pipeline_store.update_node_config(
            populated_db, NODE, {"runOptions": {"distribute": True, "as_table": False}}
        )
        assert pipeline_store.get_node_config(populated_db, NODE)["runOptions"] == {
            "distribute": True,
            "as_table": False,
        }
        raw = pipeline_store._duck(populated_db)._fetchone(
            "SELECT config FROM _node_config WHERE node_id = ?", [NODE]
        )
        assert "runOptions" not in (raw[0] if raw else "")

    def test_schema_location_round_trips_as_one_statement(self, populated_db):
        pipeline_store.update_node_config(
            populated_db,
            NODE,
            {
                "schemaLevel": ["subject", "session"],
                "schemaSelection": {"include": [["01"]], "exclude_levels": []},
                "whereFilters": [{"variable": "X", "op": "==", "value": "1"}],
            },
        )
        config = pipeline_store.get_node_config(populated_db, NODE)
        assert config["schemaLevel"] == ["subject", "session"]
        assert config["schemaSelection"]["include"] == [["01"]]
        assert config["whereFilters"][0]["variable"] == "X"
        statements = intent_store.load_statements(
            populated_db, aspect="schema_location", subject_refs=[NODE]
        )
        assert len(statements) == 1

    def test_the_blob_is_empty_once_every_aspect_has_moved(self, populated_db):
        pipeline_store.update_node_config(
            populated_db,
            NODE,
            {"columnSelections": {"a": _sel("x")}, "runOptions": {"save": True}, "schemaLevel": ["subject"]},
        )
        raw = pipeline_store._duck(populated_db)._fetchone(
            "SELECT config FROM _node_config WHERE node_id = ?", [NODE]
        )
        assert raw[0] == "{}"


class TestHidden:
    def test_hidden_nodes_are_per_pipeline(self, populated_db):
        pipeline_store.hide_node(populated_db, "fn__x__1", "pipe_a")
        assert "fn__x__1" in pipeline_store.get_hidden_node_ids(populated_db, "pipe_a")
        assert "fn__x__1" not in pipeline_store.get_hidden_node_ids(populated_db, "pipe_b")
        assert "fn__x__1" in pipeline_store.get_hidden_node_ids(populated_db, None)
        pipeline_store.unhide_node(populated_db, "fn__x__1", "pipe_a")
        assert "fn__x__1" not in pipeline_store.get_hidden_node_ids(populated_db, "pipe_a")

    def test_unhide_by_prefix(self, populated_db):
        pipeline_store.hide_node(populated_db, "fn__x__1", "main")
        pipeline_store.hide_node(populated_db, "fn__x__2", "main")
        pipeline_store.hide_node(populated_db, "fn__y__1", "main")
        pipeline_store.unhide_nodes_by_prefix(populated_db, "fn__x__", "main")
        assert pipeline_store.get_hidden_node_ids(populated_db, "main") == {"fn__y__1"}

    def test_hidden_combos_are_global_and_listed_by_function(self, populated_db):
        pipeline_store.hide_combo(populated_db, "fn__f__abc", "f", {"k": "1"})
        assert pipeline_store.list_hidden_combos(populated_db, "f") == [
            {"node_id": "fn__f__abc", "variant_key": {"k": "1"}}
        ]
        assert pipeline_store.list_hidden_combos(populated_db, "g") == []
        assert "fn__f__abc" in pipeline_store.get_hidden_node_ids(populated_db, "any_pipe")
        pipeline_store.unhide_combo(populated_db, "fn__f__abc")
        assert pipeline_store.list_hidden_combos(populated_db, "f") == []

    def test_hidden_parameter_values_bulk_and_single(self, populated_db):
        pipeline_store.hide_parameter_values(populated_db, "low_hz", ["10", "20"])
        pipeline_store.hide_parameter_value(populated_db, "low_hz", "30")
        got = {
            (r["const_name"], r["value"])
            for r in pipeline_store.list_hidden_parameter_values(populated_db)
        }
        assert got == {("low_hz", "10"), ("low_hz", "20"), ("low_hz", "30")}
        pipeline_store.unhide_parameter_values(populated_db, "low_hz", ["10", "30"])
        pipeline_store.unhide_parameter_value(populated_db, "low_hz", "20")
        assert pipeline_store.list_hidden_parameter_values(populated_db) == []

    def test_hidden_edges_keep_their_context(self, populated_db):
        pipeline_store.hide_edge(
            populated_db, "e1", "var__A", "fn__f__abc", None, "in__x", pipeline_id="main"
        )
        assert pipeline_store.get_hidden_edge_ids(populated_db, "main") == {"e1"}
        assert pipeline_store.get_hidden_edge_ids(populated_db, "other") == set()
        listed = pipeline_store.list_hidden_edges(populated_db, "main")
        assert listed[0]["target_handle"] == "in__x" and listed[0]["source"] == "var__A"
        pipeline_store.unhide_edge(populated_db, "e1", "main")
        assert pipeline_store.get_hidden_edge_ids(populated_db, None) == set()


class TestWiringAndConstants:
    def test_manual_edges_round_trip(self, populated_db):
        pipeline_store.write_manual_edge(
            populated_db,
            {"id": "e9", "source": "var__A", "target": "fn__f__abc", "targetHandle": "in__x"},
        )
        edges = pipeline_store.get_manual_edges(populated_db)
        assert edges == [
            {"id": "e9", "source": "var__A", "target": "fn__f__abc", "targetHandle": "in__x"}
        ]
        pipeline_store.rename_edge_endpoints(populated_db, "fn__f__abc", "fn__f__0123456789abcdef")
        assert pipeline_store.get_manual_edges(populated_db)[0]["target"] == "fn__f__0123456789abcdef"
        pipeline_store.delete_manual_edge(populated_db, "e9")
        assert pipeline_store.get_manual_edges(populated_db) == []

    def test_pending_constants_round_trip(self, populated_db):
        pipeline_store.add_pending_constant(populated_db, "low_hz", "40")
        pipeline_store.add_pending_constant(populated_db, "low_hz", "50")
        assert pipeline_store.get_pending_constants(populated_db) == {"low_hz": {"40", "50"}}
        pipeline_store.remove_pending_constant(populated_db, "low_hz", "40")
        assert pipeline_store.get_pending_constants(populated_db) == {"low_hz": {"50"}}


class TestImports:
    def test_legacy_rows_are_carried_over_once(self, populated_db):
        """Rows written straight into the OLD tables (a database from before
        the store) are imported on the next start, and left in place."""
        pipeline_store._ensure_tables(populated_db)
        duck = pipeline_store._duck(populated_db)
        duck._execute(
            "INSERT INTO _pipeline_hidden_constant_values (pipeline_id, const_name, value) "
            "VALUES ('main', 'legacy', '7')"
        )
        duck._execute(
            "INSERT INTO _pipeline_edges (edge_id, source, target) VALUES ('old_e', 'a', 'b')"
        )
        duck._execute("DELETE FROM _intent WHERE subject_kind = 'migration'")
        intent_store.run_imports(populated_db)

        assert {"const_name": "legacy", "value": "7"} in pipeline_store.list_hidden_parameter_values(
            populated_db, "main"
        )
        assert any(e["id"] == "old_e" for e in pipeline_store.get_manual_edges(populated_db))
        # the source rows are untouched
        assert duck._fetchone("SELECT count(*) FROM _pipeline_edges")[0] == 1
        # and a second start does not re-import what the user then removes
        pipeline_store.delete_manual_edge(populated_db, "old_e")
        intent_store.run_imports(populated_db)
        assert not any(e["id"] == "old_e" for e in pipeline_store.get_manual_edges(populated_db))
