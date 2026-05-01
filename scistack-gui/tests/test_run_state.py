"""
Unit tests for scistack_gui.domain.run_state.

All inputs are plain dicts — no DB or fixtures required.
"""

import pytest

from scistack_gui.domain.run_state import propagate_run_states


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
#
# propagate_run_states is now keyed by FnKey = (fn_name, call_id).  Tests
# below use a stable per-name dummy call_id so the test bodies stay
# readable: K("f") yields ("f", "<16-hex>") and fn("f") yields the
# matching "fn__f__<16-hex>" node ID.

import hashlib


def _cid(name: str) -> str:
    return hashlib.sha256(name.encode()).hexdigest()[:16]


def K(name: str) -> tuple[str, str]:
    """FnKey for a function with a synthetic stable call_id."""
    return (name, _cid(name))


def fn(name: str) -> str:
    return f"fn__{name}__{_cid(name)}"


def var(name: str) -> str:
    return f"var__{name}"


# ---------------------------------------------------------------------------
# Single function, no upstream dependencies
# ---------------------------------------------------------------------------

class TestSingleFunction:
    def test_green_own_state_no_inputs(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
        )
        assert result[fn("f")] == "green"
        assert result[var("Out")] == "green"

    def test_grey_own_state_propagates_to_output(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "grey"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
        )
        assert result[fn("f")] == "grey"
        assert result[var("Out")] == "grey"

    def test_red_own_state_propagates_to_output(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "red"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
        )
        assert result[fn("f")] == "red"
        assert result[var("Out")] == "red"

    def test_root_variable_treated_as_green(self):
        # "Raw" has no producer — treated as green input.
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {"signal": "Raw"}},
            fn_outputs={K("f"): {"Out"}},
        )
        assert result[fn("f")] == "green"

    def test_function_with_no_outputs_still_gets_state(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): set()},
        )
        assert result[fn("f")] == "green"


# ---------------------------------------------------------------------------
# Two-function chain: A → Out → B → FinalOut
# ---------------------------------------------------------------------------

class TestChainedFunctions:
    def _chain(self, state_a, state_b):
        return propagate_run_states(
            fn_own_states={K("A"): state_a, K("B"): state_b},
            fn_input_params={K("A"): {}, K("B"): {"x": "Out"}},
            fn_outputs={K("A"): {"Out"}, K("B"): {"FinalOut"}},
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
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
            fn_constants={K("f"): {"low_hz"}},
            pending_constants={"low_hz": {42}},
        )
        assert result[fn("f")] == "grey"
        assert result[var("Out")] == "grey"

    def test_no_downgrade_when_pending_constant_empty(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
            fn_constants={K("f"): {"low_hz"}},
            pending_constants={"low_hz": set()},
        )
        assert result[fn("f")] == "green"

    def test_no_downgrade_when_constant_not_pending(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
            fn_constants={K("f"): {"low_hz"}},
            pending_constants={},
        )
        assert result[fn("f")] == "green"

    def test_already_red_not_downgraded_further(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "red"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
            fn_constants={K("f"): {"low_hz"}},
            pending_constants={"low_hz": {99}},
        )
        # Still red, not some other value.
        assert result[fn("f")] == "red"

    def test_pending_constant_propagates_to_downstream_function(self):
        result = propagate_run_states(
            fn_own_states={K("A"): "green", K("B"): "green"},
            fn_input_params={K("A"): {}, K("B"): {"x": "Out"}},
            fn_outputs={K("A"): {"Out"}, K("B"): {"Final"}},
            fn_constants={K("A"): {"low_hz"}},
            pending_constants={"low_hz": {20}},
        )
        assert result[fn("A")] == "grey"
        assert result[fn("B")] == "grey"

    def test_none_fn_constants_is_safe(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
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
            fn_own_states={K("f"): "grey"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"A", "B", "C"}},
        )
        assert result[var("A")] == "grey"
        assert result[var("B")] == "grey"
        assert result[var("C")] == "grey"


# ---------------------------------------------------------------------------
# Multiple inputs — minimum state wins
# ---------------------------------------------------------------------------

class TestMultipleInputs:
    def test_worst_input_determines_function_state(self):
        result = propagate_run_states(
            fn_own_states={
                K("ProducerA"): "grey",
                K("ProducerB"): "green",
                K("Consumer"): "green",
            },
            fn_input_params={
                K("ProducerA"): {},
                K("ProducerB"): {},
                K("Consumer"): {"a": "Raw", "b": "Ref"},
            },
            fn_outputs={
                K("ProducerA"): {"Raw"},
                K("ProducerB"): {"Ref"},
                K("Consumer"): {"Out"},
            },
        )
        assert result[fn("Consumer")] == "grey"


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------

class TestCycleDetection:
    def test_cycle_results_in_red(self):
        result = propagate_run_states(
            fn_own_states={K("A"): "green", K("B"): "green"},
            fn_input_params={K("A"): {"x": "BOut"}, K("B"): {"y": "AOut"}},
            fn_outputs={K("A"): {"AOut"}, K("B"): {"BOut"}},
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

    def test_result_keys_use_composite_fn_id_and_var_prefix(self):
        result = propagate_run_states(
            fn_own_states={K("my_func"): "green"},
            fn_input_params={K("my_func"): {}},
            fn_outputs={K("my_func"): {"MyVar"}},
        )
        assert fn("my_func") in result  # fn__my_func__<call_id>
        assert "var__MyVar" in result
        assert "my_func" not in result
        assert "fn__my_func" not in result, (
            "the bare ``fn__{name}`` form must NOT appear — IDs are now "
            "composite ``fn__{name}__{call_id}``"
        )

    def test_caller_dict_not_mutated(self):
        original = {K("f"): "green"}
        propagate_run_states(
            fn_own_states=original,
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): set()},
            fn_constants={K("f"): {"k"}},
            pending_constants={"k": {1}},
        )
        assert original[K("f")] == "green"


# ---------------------------------------------------------------------------
# Per-call-site behavior (new with call_id)
# ---------------------------------------------------------------------------

class TestPerCallSite:
    def test_two_call_sites_same_fn_get_independent_states(self):
        """Same fn name reused at two call sites → distinct nodes, distinct states."""
        ka = ("bp", "a" * 16)
        kb = ("bp", "b" * 16)
        result = propagate_run_states(
            fn_own_states={ka: "green", kb: "red"},
            fn_input_params={ka: {}, kb: {}},
            fn_outputs={ka: {"OutA"}, kb: {"OutB"}},
        )
        assert result["fn__bp__" + "a" * 16] == "green"
        assert result["fn__bp__" + "b" * 16] == "red"
        assert result[var("OutA")] == "green"
        assert result[var("OutB")] == "red"

    def test_shared_output_takes_worst_producer_state(self):
        """Two call sites of the same fn writing to the same Variable type
        propagate the most pessimistic producer state to that variable."""
        ka = ("bp", "a" * 16)
        kb = ("bp", "b" * 16)
        result = propagate_run_states(
            fn_own_states={ka: "green", kb: "red"},
            fn_input_params={ka: {}, kb: {}},
            fn_outputs={ka: {"Filtered"}, kb: {"Filtered"}},
        )
        # Filtered has two producers; the worst (red) wins.
        assert result[var("Filtered")] == "red"
