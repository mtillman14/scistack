"""GUI state about a Parameter reaches the argument it feeds
(cleanup-audit F20, .claude/plan-f20-name-keyed-state.md).

Pending and hidden values are keyed by the Parameter's DECLARED name — the
node the user clicks. A target's ``constants`` are keyed by the function
ARGUMENT. For a Parameter declared ``gaitrite_config`` feeding
``gaitRiteConfig`` (the real B1 shape), every reader below used to compare
the two directly and silently match nothing: a staged value never ran, an
unchecked value ran anyway.
"""

from scistack_gui.domain.edge_resolver import (
    by_argument,
    declared_by_argument,
    parameter_binding,
    variable_binding,
)
from scistack_gui.domain.variant_resolver import (
    filter_hidden_constant_value_targets,
    merge_pending_constants,
    resolve_target_call_id,
)
from scistack_gui.services import execution_service
from scistack_gui.services.execution_service import apply_pending_overrides

FN = "loadGaitRiteOneFile"
ARG = "gaitRiteConfig"
DECLARED = "gaitrite_config"


def _target(value=5, *, bound=True):
    bindings = {"signal": variable_binding(["Raw"])}
    if bound:
        bindings[ARG] = parameter_binding(DECLARED)
    return {
        "function_name": FN,
        "call_id": "stale-call-id",
        "output_type": "GAITRiteLoaded",
        "input_types": {"signal": "Raw"},
        "constants": {ARG: value},
        "bindings": bindings,
    }


class TestTheTranslation:
    def test_declared_by_argument(self):
        assert declared_by_argument(_target()) == {ARG: DECLARED}
        assert declared_by_argument(_target(bound=False)) == {ARG: ARG}

    def test_by_argument_rekeys_only_this_targets_constants(self):
        named = {DECLARED: {"7"}, "unrelated": {"1"}}
        assert by_argument(named, _target()) == {ARG: {"7"}}


def test_a_staged_value_overrides_the_argument_it_feeds():
    [t] = apply_pending_overrides([_target(5)], {DECLARED: {"7"}})
    assert t["constants"] == {ARG: 7}


def test_a_staged_value_is_merged_into_history_targets():
    out = merge_pending_constants([_target(5)], {DECLARED: {"7"}})
    assert sorted(t["constants"][ARG] for t in out) == [5, 7]


def test_an_unchecked_value_drops_the_target():
    assert filter_hidden_constant_value_targets([_target(5)], {DECLARED: {"5"}}) == []
    assert len(filter_hidden_constant_value_targets([_target(6)], {DECLARED: {"5"}})) == 1


def test_a_staged_parameter_forces_a_fresh_call_id():
    cid = resolve_target_call_id(FN, _target(), {DECLARED})
    assert cid != "stale-call-id"
    assert resolve_target_call_id(FN, _target(), {"unrelated"}) == "stale-call-id"


def test_history_targets_bind_the_recorded_parameter(monkeypatch):
    """What makes the four readers work for targets built from history."""
    monkeypatch.setattr(execution_service, "_db_path_input_params", lambda db, fn: {})
    history = {
        "function_name": FN,
        "call_id": "c1",
        "output_type": "GAITRiteLoaded",
        "input_types": {"signal": "Raw"},
        "constants": {ARG: 5, "window": 3},
        "parameter_names": {ARG: DECLARED},
    }
    [t] = execution_service._attach_db_path_inputs(None, FN, [history])
    assert declared_by_argument(t) == {ARG: DECLARED, "window": "window"}
    [overridden] = apply_pending_overrides([t], {DECLARED: {"9"}})
    assert overridden["constants"][ARG] == 9
