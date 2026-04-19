"""
Unit tests for scistack_gui.domain.run_state.

All inputs are plain dicts — no DB or fixtures required.
"""

import pytest

from scistack_gui.domain.run_state import propagate_run_states


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fn(name): return f"fn__{name}"
def var(name): return f"var__{name}"


# ---------------------------------------------------------------------------
# Single function, no upstream dependencies
# ---------------------------------------------------------------------------

class TestSingleFunction:
    def test_green_own_state_no_inputs(self):
        result = propagate_run_states(
            fn_own_states={"f": "green"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"Out"}},
        )
        assert result[fn("f")] == "green"
        assert result[var("Out")] == "green"

    def test_grey_own_state_propagates_to_output(self):
        result = propagate_run_states(
            fn_own_states={"f": "grey"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"Out"}},
        )
        assert result[fn("f")] == "grey"
        assert result[var("Out")] == "grey"

    def test_red_own_state_propagates_to_output(self):
        result = propagate_run_states(
            fn_own_states={"f": "red"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"Out"}},
        )
        assert result[fn("f")] == "red"
        assert result[var("Out")] == "red"

    def test_root_variable_treated_as_green(self):
        # "Raw" has no producer — treated as green input.
        result = propagate_run_states(
            fn_own_states={"f": "green"},
            fn_input_params={"f": {"signal": "Raw"}},
            fn_outputs={"f": {"Out"}},
        )
        assert result[fn("f")] == "green"

    def test_function_with_no_outputs_still_gets_state(self):
        result = propagate_run_states(
            fn_own_states={"f": "green"},
            fn_input_params={"f": {}},
            fn_outputs={"f": set()},
        )
        assert result[fn("f")] == "green"


# ---------------------------------------------------------------------------
# Two-function chain: A → Out → B → FinalOut
# ---------------------------------------------------------------------------

class TestChainedFunctions:
    def _chain(self, state_a, state_b):
        return propagate_run_states(
            fn_own_states={"A": state_a, "B": state_b},
            fn_input_params={"A": {}, "B": {"x": "Out"}},
            fn_outputs={"A": {"Out"}, "B": {"FinalOut"}},
        )

    def test_both_green(self):
        result = self._chain("green", "green")
        assert result[fn("A")] == "green"
        assert result[var("Out")] == "green"
        assert result[fn("B")] == "green"
        assert result[var("FinalOut")] == "green"

    def test_upstream_red_propagates_down(self):
        result = self._chain("red", "green")
        assert result[fn("A")] == "red"
        assert result[var("Out")] == "red"
        assert result[fn("B")] == "red"
        assert result[var("FinalOut")] == "red"

    def test_upstream_grey_propagates_down(self):
        result = self._chain("grey", "green")
        assert result[fn("A")] == "grey"
        assert result[var("Out")] == "grey"
        assert result[fn("B")] == "grey"
        assert result[var("FinalOut")] == "grey"

    def test_downstream_red_doesnt_affect_upstream(self):
        result = self._chain("green", "red")
        assert result[fn("A")] == "green"
        assert result[var("Out")] == "green"
        assert result[fn("B")] == "red"
        assert result[var("FinalOut")] == "red"

    def test_downstream_grey_doesnt_affect_upstream(self):
        result = self._chain("green", "grey")
        assert result[fn("A")] == "green"
        assert result[fn("B")] == "grey"

    def test_minimum_state_wins(self):
        # A is grey, B is green → B becomes grey because its input is grey.
        result = self._chain("grey", "green")
        assert result[fn("B")] == "grey"


# ---------------------------------------------------------------------------
# Pending constants downgrade green → grey
# ---------------------------------------------------------------------------

class TestPendingConstants:
    def test_green_downgraded_when_pending_constant_exists(self):
        result = propagate_run_states(
            fn_own_states={"f": "green"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"Out"}},
            fn_constants={"f": {"low_hz"}},
            pending_constants={"low_hz": {42}},
        )
        assert result[fn("f")] == "grey"
        assert result[var("Out")] == "grey"

    def test_no_downgrade_when_pending_constant_empty(self):
        result = propagate_run_states(
            fn_own_states={"f": "green"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"Out"}},
            fn_constants={"f": {"low_hz"}},
            pending_constants={"low_hz": set()},
        )
        assert result[fn("f")] == "green"

    def test_no_downgrade_when_constant_not_pending(self):
        result = propagate_run_states(
            fn_own_states={"f": "green"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"Out"}},
            fn_constants={"f": {"low_hz"}},
            pending_constants={},
        )
        assert result[fn("f")] == "green"

    def test_already_red_not_downgraded_further(self):
        result = propagate_run_states(
            fn_own_states={"f": "red"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"Out"}},
            fn_constants={"f": {"low_hz"}},
            pending_constants={"low_hz": {99}},
        )
        # Still red, not some other value.
        assert result[fn("f")] == "red"

    def test_pending_constant_propagates_to_downstream_function(self):
        # A has a pending constant → downgraded to grey → B also becomes grey.
        result = propagate_run_states(
            fn_own_states={"A": "green", "B": "green"},
            fn_input_params={"A": {}, "B": {"x": "Out"}},
            fn_outputs={"A": {"Out"}, "B": {"Final"}},
            fn_constants={"A": {"low_hz"}},
            pending_constants={"low_hz": {20}},
        )
        assert result[fn("A")] == "grey"
        assert result[fn("B")] == "grey"

    def test_none_fn_constants_is_safe(self):
        result = propagate_run_states(
            fn_own_states={"f": "green"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"Out"}},
            fn_constants=None,
            pending_constants={"x": {1}},
        )
        assert result[fn("f")] == "green"


# ---------------------------------------------------------------------------
# Multiple outputs per function
# ---------------------------------------------------------------------------

class TestMultipleOutputs:
    def test_all_outputs_get_same_state(self):
        result = propagate_run_states(
            fn_own_states={"f": "grey"},
            fn_input_params={"f": {}},
            fn_outputs={"f": {"A", "B", "C"}},
        )
        assert result[var("A")] == "grey"
        assert result[var("B")] == "grey"
        assert result[var("C")] == "grey"


# ---------------------------------------------------------------------------
# Multiple inputs — minimum state wins
# ---------------------------------------------------------------------------

class TestMultipleInputs:
    def test_worst_input_determines_function_state(self):
        # Producer of "Raw" is grey, producer of "Ref" is green.
        result = propagate_run_states(
            fn_own_states={"ProducerA": "grey", "ProducerB": "green", "Consumer": "green"},
            fn_input_params={"ProducerA": {}, "ProducerB": {}, "Consumer": {"a": "Raw", "b": "Ref"}},
            fn_outputs={"ProducerA": {"Raw"}, "ProducerB": {"Ref"}, "Consumer": {"Out"}},
        )
        assert result[fn("Consumer")] == "grey"


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------

class TestCycleDetection:
    def test_cycle_results_in_red(self):
        # A depends on B's output, B depends on A's output — cycle.
        result = propagate_run_states(
            fn_own_states={"A": "green", "B": "green"},
            fn_input_params={"A": {"x": "BOut"}, "B": {"y": "AOut"}},
            fn_outputs={"A": {"AOut"}, "B": {"BOut"}},
        )
        assert result[fn("A")] == "red"
        assert result[fn("B")] == "red"


# ---------------------------------------------------------------------------
# Empty inputs
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_inputs(self):
        result = propagate_run_states(
            fn_own_states={},
            fn_input_params={},
            fn_outputs={},
        )
        assert result == {}

    def test_result_keys_use_fn_and_var_prefixes(self):
        result = propagate_run_states(
            fn_own_states={"my_func": "green"},
            fn_input_params={"my_func": {}},
            fn_outputs={"my_func": {"MyVar"}},
        )
        assert "fn__my_func" in result
        assert "var__MyVar" in result
        assert "my_func" not in result
        assert "MyVar" not in result

    def test_caller_dict_not_mutated(self):
        original = {"f": "green"}
        propagate_run_states(
            fn_own_states=original,
            fn_input_params={"f": {}},
            fn_outputs={"f": set()},
            fn_constants={"f": {"k"}},
            pending_constants={"k": {1}},
        )
        assert original["f"] == "green"
