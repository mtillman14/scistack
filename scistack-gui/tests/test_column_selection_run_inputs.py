"""
Tests for the GUI's column selection at the RUN level.

Stages 2 and 3 of .claude/plan-column-selection-ui.md:

* ``_attach_column_selections`` stamps a node's saved ``columnSelections``
  onto every derived target -- from BOTH derivation paths, and for a function
  that has already run as well as one that never has. The has-history case is
  the one that regresses: a source-declared pipeline has no manual edge rows
  once it has run, so the selection has no wiring to ride on and the node
  config is the only record there is.
* ``build_run_inputs`` turns that into a real ``scidb.ColumnSelection``.
"""

from __future__ import annotations

import numpy as np
from scidb import BaseVariable
from scifor import ColumnSelection as SciforColumnSelection

from scistack_gui import pipeline_store
from scistack_gui.domain.edge_resolver import variable_binding
from scistack_gui.services.execution_service import (
    build_run_inputs,
    column_selections_for_nodes,
    derive_fn_targets,
    derive_target_for_node,
)


def _selection_of(target: dict, param: str):
    from scistack_gui.domain import column_selection as cs

    return cs.from_binding((target.get("bindings") or {}).get(param) or {})


# ---------------------------------------------------------------------------
# build_run_inputs
# ---------------------------------------------------------------------------


class TestBuildRunInputsColumnSelection:
    def _target(self, columns=None, iterate=False, types=("RawSignal",)):
        return {
            "constants": {},
            "bindings": {
                "signal": variable_binding(list(types), columns, iterate)
            },
        }

    def test_no_selection_binds_the_bare_class(self, populated_db):
        inputs = build_run_inputs(self._target(), "bandpass_filter", populated_db)
        assert isinstance(inputs["signal"], type)

    def test_single_column(self, populated_db):
        inputs = build_run_inputs(
            self._target(["a"]), "bandpass_filter", populated_db
        )
        sel = inputs["signal"]
        assert isinstance(sel, SciforColumnSelection)
        assert sel.columns == ["a"]
        assert sel.iterate is False

    def test_multiple_columns(self, populated_db):
        inputs = build_run_inputs(
            self._target(["a", "b"]), "bandpass_filter", populated_db
        )
        assert inputs["signal"].columns == ["a", "b"]

    def test_iterate_with_columns(self, populated_db):
        inputs = build_run_inputs(
            self._target(["a", "b"], iterate=True), "bandpass_filter", populated_db
        )
        sel = inputs["signal"]
        assert sel.columns == ["a", "b"] and sel.iterate is True

    def test_iterate_over_all_columns(self, populated_db):
        """``MyVar.for_columns()`` -- resolved at for_each time, not here."""
        inputs = build_run_inputs(
            self._target(iterate=True), "bandpass_filter", populated_db
        )
        sel = inputs["signal"]
        assert sel.columns == [] and sel.iterate is True

    def test_it_is_scidbs_subclass_not_the_bare_scifor_container(self, populated_db):
        """The DB-only surface (.load(), comparison operators) has to be
        there -- scidb's foreach dispatches on what is INSIDE the wrapper."""
        from scidb.column_selection import ColumnSelection as ScidbColumnSelection

        inputs = build_run_inputs(
            self._target(["a"]), "bandpass_filter", populated_db
        )
        assert isinstance(inputs["signal"], ScidbColumnSelection)

    def test_version_key_differs_from_the_bare_class(self, populated_db):
        """This is what makes a column change RE-RUN rather than silently
        reuse the cached result: ``ForEachConfig._serialize_inputs`` puts
        ``to_key()`` into ``__inputs``, forking the version-key group."""
        from scistack_gui import registry

        picked = build_run_inputs(
            self._target(["a"]), "bandpass_filter", populated_db
        )["signal"]
        bare = registry.get_variable_class("RawSignal")
        assert picked.to_key() != getattr(bare, "__name__", str(bare))

    def test_each_alternative_of_a_multi_type_binding_is_wrapped(self, populated_db):
        inputs = build_run_inputs(
            self._target(["a"], types=("RawSignal", "FilteredSignal")),
            "bandpass_filter",
            populated_db,
        )
        alternatives = inputs["signal"].alternatives
        assert len(alternatives) == 2
        assert all(isinstance(a, SciforColumnSelection) for a in alternatives)
        assert all(a.columns == ["a"] for a in alternatives)


# ---------------------------------------------------------------------------
# Derivation: has-history and never-run
# ---------------------------------------------------------------------------


class TestAttachToTargetsWithHistory:
    """The regressing case. ``bandpass_filter`` in the ``populated_db``
    fixture has real DB history and no manual edges at all."""

    def _save(self, db, node_id, selection):
        pipeline_store.update_node_config(
            db, node_id, {"columnSelections": {"signal": selection}}
        )

    def test_name_scoped_path_picks_it_up(self, populated_db, bp_node_id):
        self._save(populated_db, bp_node_id, {"columns": ["a"], "iterate": False})
        targets = derive_fn_targets(populated_db, "bandpass_filter")
        assert targets
        assert _selection_of(targets[0], "signal") == {
            "columns": ["a"],
            "iterate": False,
        }

    def test_node_scoped_path_picks_it_up(self, populated_db, bp_node_id):
        self._save(populated_db, bp_node_id, {"columns": ["a"], "iterate": True})
        targets = derive_target_for_node(populated_db, bp_node_id)
        assert targets
        assert _selection_of(targets[0], "signal") == {"columns": ["a"], "iterate": True}

    def test_placement_qualified_config_id_is_matched(self, populated_db, bp_node_id):
        """The id the panel saves under is the placement-qualified one
        whenever the node has a qualified placement, while the derivation
        path's id set is bare. Comparing them with ``==`` silently runs the
        whole variable (the trap ``bare_fn_node_ids`` documents)."""
        self._save(populated_db, f"{bp_node_id}::main", {"columns": ["a"]})
        targets = derive_fn_targets(populated_db, "bandpass_filter")
        assert _selection_of(targets[0], "signal") == {
            "columns": ["a"],
            "iterate": False,
        }

    def test_no_config_leaves_bindings_bare(self, populated_db):
        targets = derive_fn_targets(populated_db, "bandpass_filter")
        assert targets and _selection_of(targets[0], "signal") is None

    def test_stale_selection_for_a_gone_param_warns_and_does_not_raise(
        self, populated_db, bp_node_id, caplog
    ):
        """A parameter renamed in source leaves its key behind in a config
        nobody rewrote. Raising mid-run would be far worse than running with
        the whole variable."""
        import logging

        pipeline_store.update_node_config(
            populated_db, bp_node_id, {"columnSelections": {"no_such_param": ["a"]}}
        )
        with caplog.at_level(logging.WARNING):
            targets = derive_fn_targets(populated_db, "bandpass_filter")
        assert targets
        assert "no_such_param" in caplog.text

    def test_conflicting_selections_keep_the_first_and_warn(
        self, populated_db, bp_node_id, caplog
    ):
        import logging

        self._save(populated_db, bp_node_id, {"columns": ["a"]})
        self._save(populated_db, f"{bp_node_id}::main", {"columns": ["b"]})
        with caplog.at_level(logging.WARNING):
            merged = column_selections_for_nodes(
                populated_db, {bp_node_id}, "bandpass_filter"
            )
        assert merged["signal"]["columns"] in (["a"], ["b"])
        assert "conflicting column selections" in caplog.text

    def test_node_scoped_lookup_ignores_another_nodes_config(
        self, populated_db, bp_node_id
    ):
        """Two call sites of one function name must stay independent when the
        run is node-scoped."""
        self._save(populated_db, "fn__bandpass_filter__deadbeef", {"columns": ["a"]})
        assert column_selections_for_nodes(populated_db, {bp_node_id}) == {}


class OtherRaw(BaseVariable):
    pass


class OtherOut(BaseVariable):
    pass


class TestAttachToNeverRunTargets:
    def test_inferred_target_carries_the_selection(self, client):
        from scistack_gui.db import get_db

        OtherRaw.save(np.zeros(5), subject=1, session="pre")
        client.put(
            "/api/layout/cs_in",
            json={"x": 0, "y": 0, "node_type": "variableNode", "label": "OtherRaw"},
        )
        client.put(
            "/api/layout/cs_fn",
            json={
                "x": 10,
                "y": 0,
                "node_type": "functionNode",
                "label": "bandpass_filter",
            },
        )
        client.put(
            "/api/layout/cs_out",
            json={"x": 20, "y": 0, "node_type": "variableNode", "label": "OtherOut"},
        )
        client.put(
            "/api/edges/cs_e_in",
            json={
                "source": "cs_in",
                "target": "cs_fn",
                "target_handle": "in__signal",
            },
        )
        client.put("/api/edges/cs_e_out", json={"source": "cs_fn", "target": "cs_out"})

        db = get_db()
        pipeline_store.update_node_config(
            db, "cs_fn", {"columnSelections": {"signal": {"columns": ["a", "b"]}}}
        )

        targets = derive_target_for_node(db, "cs_fn")
        assert targets
        assert _selection_of(targets[0], "signal") == {
            "columns": ["a", "b"],
            "iterate": False,
        }

    def test_inferred_targets_do_not_share_one_bindings_dict(self, client):
        """``_inferred_targets`` builds every target from one shared ``base``
        dict; stamping in place would write through the alias."""
        from scistack_gui.services.execution_service import _inferred_targets
        from scistack_gui.domain.edge_resolver import ResolvedEdges

        resolved = ResolvedEdges(
            bindings={"signal": variable_binding(["OtherRaw"])},
            output_types=["OtherOut", "OtherRaw"],
        )
        targets = _inferred_targets(resolved, {})
        assert targets[0]["bindings"] is targets[1]["bindings"]  # the alias

        from scistack_gui.domain import column_selection as cs

        targets[0]["bindings"] = cs.apply_to_bindings(
            targets[0]["bindings"], {"signal": ["a"]}
        )
        assert "columns" not in targets[1]["bindings"]["signal"]
