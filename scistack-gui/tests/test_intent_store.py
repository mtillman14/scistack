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
        """One owner per aspect: the selection is stripped on the way in, so
        `_node_config` cannot drift from `_intent`."""
        pipeline_store.update_node_config(
            populated_db,
            NODE,
            {"columnSelections": {"cycles": _sel("ankle")}, "schemaLevel": ["subject"]},
        )
        raw = pipeline_store._duck(populated_db)._fetchone(
            "SELECT config FROM _node_config WHERE node_id = ?", [NODE]
        )
        assert "columnSelections" not in (raw[0] if raw else "")
        assert "schemaLevel" in raw[0]

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

    def test_the_real_call_site_wins_over_its_placeholder(self, populated_db):
        manual = "fn__trial_mean_symmetry__a1b2c3"
        intent_store.set_column_selections(populated_db, NODE, {"cycles": _sel("knee")})
        intent_store.set_column_selections(populated_db, manual, {"cycles": _sel("ankle")})
        intent_store.rekey_subject(populated_db, manual, NODE)

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
