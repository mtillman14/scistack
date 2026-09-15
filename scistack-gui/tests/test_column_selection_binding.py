"""
Tests for the GUI's column selection at the BINDING level.

Stage 1 of .claude/plan-column-selection-ui.md: a variable binding can carry
``columns``/``iterate``, and ``domain/column_selection.py`` is the one place
that decides what a stored selection means.

The regression pins at the bottom are the load-bearing half: a column pick
must NOT move ``wiring_id`` or ``compute_call_id``. scidb's forward
``to_call_id`` does fold ``ColumnSelection.to_key()`` into ``__inputs``, but
the GUI never sees the forward id -- provenance stores an input edge as
``(param -> record -> variable_type)``, so ``pipeline_variants`` reconstructs
``__inputs = {"param": "Type"}`` and the canvas node id comes from THAT.
Feeding columns into the GUI's prediction would compute an id no record ever
carries, and silently break combo hiding on every column-selected node.
"""

from __future__ import annotations

import logging

from scistack_gui.domain import column_selection as cs
from scistack_gui.domain.edge_resolver import (
    BINDING_PARAMETER,
    BINDING_VARIABLE,
    bindings_of_kind,
    parameter_binding,
    variable_binding,
    variable_types_view,
)


# ---------------------------------------------------------------------------
# normalize
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_none_is_no_selection(self):
        assert cs.normalize(None) is None

    def test_bare_string_becomes_one_column(self):
        assert cs.normalize("filename") == {"columns": ["filename"], "iterate": False}

    def test_bare_list(self):
        assert cs.normalize(["a", "b"]) == {"columns": ["a", "b"], "iterate": False}

    def test_canonical_dict_round_trips(self):
        raw = {"columns": ["a"], "iterate": True}
        assert cs.normalize(raw) == {"columns": ["a"], "iterate": True}

    def test_partial_dict_columns_only(self):
        assert cs.normalize({"columns": ["a"]}) == {"columns": ["a"], "iterate": False}

    def test_partial_dict_iterate_only_is_all_columns(self):
        """``MyVar.for_columns()`` -- every data column, one call each,
        resolved at for_each time. Legal, and must survive normalisation."""
        assert cs.normalize({"iterate": True}) == {"columns": [], "iterate": True}

    def test_dict_with_string_columns(self):
        assert cs.normalize({"columns": "a"}) == {"columns": ["a"], "iterate": False}

    def test_empty_non_iterate_selection_is_dropped(self):
        """No columns and no iteration means "the whole variable", which is
        what binding the bare class already does. Surviving as a
        ColumnSelection would fork the version key for no change in what the
        function receives."""
        assert cs.normalize({"columns": [], "iterate": False}) is None
        assert cs.normalize([]) is None
        assert cs.normalize("") is None

    def test_blank_column_names_are_dropped(self):
        assert cs.normalize(["a", "", None]) == {"columns": ["a"], "iterate": False}

    def test_unexpected_type_is_ignored_not_raised(self, caplog):
        with caplog.at_level(logging.WARNING):
            assert cs.normalize(17) is None
        assert "unexpected type" in caplog.text


class TestFromBinding:
    def test_plain_binding_has_no_selection(self):
        assert cs.from_binding(variable_binding(["Trials"])) is None

    def test_binding_with_columns(self):
        b = variable_binding(["Trials"], ["filename"])
        assert cs.from_binding(b) == {"columns": ["filename"], "iterate": False}

    def test_binding_with_iterate_only(self):
        b = variable_binding(["Trials"], iterate=True)
        assert cs.from_binding(b) == {"columns": [], "iterate": True}


class TestDescribe:
    def test_no_selection(self):
        assert cs.describe(None) == "whole variable"

    def test_one_column(self):
        assert cs.describe({"columns": ["filename"], "iterate": False}) == '"filename"'

    def test_several_columns(self):
        sel = {"columns": ["a", "b", "c"], "iterate": False}
        assert cs.describe(sel) == "3 columns"

    def test_iterate_all(self):
        assert cs.describe({"columns": [], "iterate": True}) == "per column"

    def test_iterate_subset(self):
        assert cs.describe({"columns": ["a", "b"], "iterate": True}) == "per column (2)"


# ---------------------------------------------------------------------------
# variable_binding / apply_to_bindings
# ---------------------------------------------------------------------------


class TestVariableBinding:
    def test_no_selection_keeps_exactly_two_keys(self):
        """``resolve_function_edges._bind`` compares bindings with ``==`` to
        detect two edges on one handle. An always-present columns/iterate pair
        would be harmless here but is extra state everywhere else."""
        assert variable_binding(["Trials"]) == {
            "kind": BINDING_VARIABLE,
            "ref": ["Trials"],
        }

    def test_columns_are_copied_not_aliased(self):
        cols = ["a"]
        b = variable_binding(["Trials"], cols)
        cols.append("b")
        assert b["columns"] == ["a"]

    def test_iterate_without_columns_is_recorded(self):
        b = variable_binding(["Trials"], iterate=True)
        assert b["columns"] == [] and b["iterate"] is True


class TestApplyToBindings:
    def test_stamps_onto_the_variable_binding(self):
        bindings = {"table_in": variable_binding(["Trials"])}
        out = cs.apply_to_bindings(bindings, {"table_in": ["filename"]})
        assert out["table_in"]["columns"] == ["filename"]
        assert out["table_in"]["iterate"] is False

    def test_does_not_mutate_the_input(self):
        """``_inferred_targets`` builds every target from ONE shared ``base``
        dict, so an in-place stamp would be applied through an alias."""
        bindings = {"table_in": variable_binding(["Trials"])}
        cs.apply_to_bindings(bindings, {"table_in": ["filename"]})
        assert "columns" not in bindings["table_in"]

    def test_selection_for_a_non_variable_param_warns_and_is_ignored(self, caplog):
        bindings = {"sep": parameter_binding("sep")}
        with caplog.at_level(logging.WARNING):
            out = cs.apply_to_bindings(bindings, {"sep": ["a"]})
        assert out["sep"] == {"kind": BINDING_PARAMETER, "ref": "sep"}
        assert "not a variable" in caplog.text

    def test_selection_for_an_unknown_param_warns_and_is_ignored(self, caplog):
        with caplog.at_level(logging.WARNING):
            out = cs.apply_to_bindings({}, {"gone": ["a"]})
        assert out == {}
        assert "not bound at all" in caplog.text

    def test_empty_selection_leaves_the_binding_bare(self):
        bindings = {"table_in": variable_binding(["Trials"])}
        out = cs.apply_to_bindings(bindings, {"table_in": {"columns": []}})
        assert "columns" not in out["table_in"]


# ---------------------------------------------------------------------------
# Regression pins: identity must not move
# ---------------------------------------------------------------------------


class TestSelectionIsNotPartOfIdentity:
    def test_variable_types_view_still_returns_bare_names(self):
        bindings = {"table_in": variable_binding(["Trials"], ["filename"])}
        assert variable_types_view(bindings) == {"table_in": "Trials"}

    def test_bindings_of_kind_still_returns_refs_only(self):
        bindings = {"table_in": variable_binding(["Trials"], ["filename"])}
        assert bindings_of_kind(bindings, BINDING_VARIABLE) == {"table_in": ["Trials"]}

    def test_wiring_id_unchanged_by_a_column_selection(self):
        from scistack_gui.domain.graph_builder import wiring_id

        plain = variable_types_view({"table_in": variable_binding(["Trials"])})
        picked = variable_types_view(
            {"table_in": variable_binding(["Trials"], ["filename"])}
        )
        assert wiring_id("load", plain, {"Out"}, {}) == wiring_id(
            "load", picked, {"Out"}, {}
        )

    def test_compute_call_id_unchanged_by_a_column_selection(self):
        """The GUI predicts the call_id a run will land on so a combo can be
        hidden before it has ever run. Provenance carries no trace of the
        columns, so the id the canvas will see is the bare-type one -- see
        this module's docstring."""
        from scistack_gui.domain.variant_resolver import compute_call_id

        plain = {
            "bindings": {"table_in": variable_binding(["Trials"])},
            "constants": {"sep": ","},
            "output_type": "Out",
        }
        picked = {
            "bindings": {
                "table_in": variable_binding(["Trials"], ["filename"], iterate=True)
            },
            "constants": {"sep": ","},
            "output_type": "Out",
        }
        assert compute_call_id("load", plain) == compute_call_id("load", picked)

    def test_saved_config_keys_include_column_selections(self):
        """Without this the key is written and never read back -- the node's
        Inputs section visibly snaps back on the next dag_updated refetch.
        Same defect class as the 2026-09-14 runOptions bug."""
        from scistack_gui.domain.graph_builder import _SAVED_CONFIG_KEYS

        assert "columnSelections" in _SAVED_CONFIG_KEYS
