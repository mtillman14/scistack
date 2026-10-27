"""
A variable type nothing declares must be surfaced early and clearly -- but
not blocked, because Python is not the authority on MATLAB's path.

From scistack.log, 2026-09-01: ``Raw_EMG`` was placed on the canvas at
15:31:58 (``put_layout`` -> ``write_manual_node``, layout only -- nothing
declared it), the command generator warned about it at 15:33:03, and MATLAB
failed at 15:35:02 with ``Unrecognized function or variable 'Raw_EMG'``.

The fix is *visibility*, not refusal, and this module pins both halves:

- placement stays permissive (an undeclared manual node is a designed
  placeholder that graduates later, and paste/duplicate/extract copy them);
- the unresolvable-type answer reaches the run console before MATLAB
  starts, instead of living only in a script comment and a log line.

See ``.claude/plan-entity-surfaces-and-reload-cost.md`` Stage 4.
"""

from __future__ import annotations

import pytest
from scidb import BaseVariable

import scistack_gui.registry as _registry


@pytest.fixture(autouse=True)
def _clean():
    yield
    _registry._unregister_tracked_variables()


class TestPutLayoutAcceptsUndeclaredVariables:
    """Placing one is NOT refused, deliberately.

    A manual variable node whose label nothing declares yet is a designed
    state: it graduates to a canonical ``var__`` id once a run gives it DB
    history, and paste/duplicate/extract copy such nodes wholesale (see
    ``test_pipeline_scopes.TestPasteNodes``, whose labels are synthetic on
    purpose). An earlier version of this guard refused them and broke all
    four flows. The hard gate belongs at the run boundary, where the
    decision is unambiguous.
    """

    def test_an_undeclared_variable_node_is_still_written(self, monkeypatch):
        from scistack_gui import layout as layout_store
        from scistack_gui.services import layout_service

        written = {}
        monkeypatch.setattr(
            layout_service, "_notify_dag_updated", lambda *a, **k: None, raising=False
        )
        monkeypatch.setattr(
            layout_store,
            "write_manual_node",
            lambda *a, **k: written.setdefault("called", True),
        )

        assert "NoSuchVariable" not in BaseVariable._all_subclasses

        result = layout_service.put_layout(
            "var__NoSuchVariable__abc123",
            10.0,
            20.0,
            node_type="variableNode",
            label="NoSuchVariable",
        )

        assert result["ok"] is True
        assert written.get("called") is True


class TestUnresolvableTypesAreSurfacedNotBlocked:
    """Advisory, deliberately.

    Blocking the run on this looks right and is wrong: the check sees only
    classdefs the GUI's registry parsed plus entities-file declarations,
    while a user's own ``startup.m`` can ``addpath`` a perfectly good
    ``RawEMG.m`` that nothing here will ever know about. ``scimatlab.stubs``
    states the same rule -- MATLAB's path is the only authority on whether a
    class resolves. So this is surfaced loudly and MATLAB decides.
    """

    def test_the_warning_names_the_type_and_the_fix(self):
        from scistack_gui.api.matlab_command import unresolvable_var_type_warning

        warning = unresolvable_var_type_warning(["Raw_EMG"])

        assert warning is not None
        assert "Raw_EMG" in warning
        assert "Unrecognized function or variable" in warning
        assert "declare" in warning.lower()

    def test_no_warning_when_every_type_resolves(self, monkeypatch):
        from scistack_gui import matlab_registry
        from scistack_gui.api.matlab_command import unresolvable_var_type_warning

        monkeypatch.setitem(matlab_registry._matlab_variables, "RawEMG", None)

        assert unresolvable_var_type_warning(["RawEMG"]) is None

    def test_empty_type_list_is_fine(self):
        from scistack_gui.api.matlab_command import unresolvable_var_type_warning

        assert unresolvable_var_type_warning([]) is None
        assert unresolvable_var_type_warning([None, ""]) is None

    def test_generation_annotates_the_script_and_never_raises(self):
        """The copy-command path must keep producing an inspectable script."""
        from scistack_gui.api.matlab_command import _unresolvable_var_type_lines

        lines = _unresolvable_var_type_lines(["Raw_EMG"])

        assert any("Raw_EMG" in line for line in lines)
        assert all(line == "" or line.startswith("%") for line in lines)

    def test_the_service_reports_it_as_a_run_console_warning(self, monkeypatch):
        """``run.py`` emits ``result["warnings"]`` into the run console, so
        this is what puts the diagnosis in front of the user *before* they
        start waiting on MATLAB."""
        from scistack_gui.services import matlab_command_service

        monkeypatch.setattr(
            matlab_command_service, "_fmt", lambda **kwargs: "% script", raising=False
        )

        from scistack_gui.api.matlab_command import unresolvable_var_type_warning

        warning = unresolvable_var_type_warning({"Raw_EMG"})
        assert warning is not None and "Raw_EMG" in warning
