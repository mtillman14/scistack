"""
Unit tests for scistack_gui.domain.run_state.

All inputs are plain dicts — no DB or fixtures required.
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
#
# propagate_run_states is now keyed by FnKey = (fn_name, call_id).  Tests
# below use a stable per-name dummy call_id so the test bodies stay
# readable: K("f") yields ("f", "<16-hex>") and fn("f") yields the
# matching "fn__f__<16-hex>" node ID.
import hashlib

from scistack_gui.domain.run_state import propagate_run_states


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

    def test_pending_own_state_propagates_to_output(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "pending"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
        )
        assert result[fn("f")] == "pending"
        assert result[var("Out")] == "pending"

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

    def test_upstream_pending_propagates_down(self):
        result = self._chain("pending", "green")
        assert result[fn("A")] == "pending"
        assert result[var("Out")] == "pending"
        assert result[fn("B")] == "pending"
        assert result[var("FinalOut")] == "pending"

    def test_downstream_red_doesnt_affect_upstream(self):
        result = self._chain("green", "red")
        assert result[fn("A")] == "green"
        assert result[var("Out")] == "green"
        assert result[fn("B")] == "red"
        assert result[var("FinalOut")] == "red"

    def test_downstream_pending_doesnt_affect_upstream(self):
        result = self._chain("green", "pending")
        assert result[fn("A")] == "green"
        assert result[fn("B")] == "pending"

    def test_minimum_state_wins(self):
        # A is pending, B is green → B becomes pending because its input is pending.
        result = self._chain("pending", "green")
        assert result[fn("B")] == "pending"


# ---------------------------------------------------------------------------
# Pending constants are NOT a DAG-propagation concern
# ---------------------------------------------------------------------------
#
# A real, recorded call site's own state is green or red on its merits,
# full stop — propagate_run_states doesn't know about staged/pending
# constant values at all anymore (that used to blur a call site to
# "pending" just because SOME sibling wiring sharing the same constant
# node had an unrun value, including — worse — the exact call site that
# had just been run to satisfy it). "pending" for an unrun combo is
# represented purely by the synthesized staged row that
# graph_builder.group_call_sites_by_wiring adds for that not-yet-existing
# combo — see test_graph_builder.py's TestGroupCallSitesByWiring and
# TestFkeyPendingCoverage for that half of the behavior.


class TestPendingConstantsNotPropagated:
    def test_green_stays_green_regardless_of_extra_kwargs_removed(self):
        """propagate_run_states no longer accepts fn_constants /
        pending_constants at all — a real green call site is simply
        green."""
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
        )
        assert result[fn("f")] == "green"
        assert result[var("Out")] == "green"

    def test_two_sibling_wirings_each_keep_their_own_true_state(self):
        """Regression for the bug where two function nodes sharing a
        constant (e.g. compute_rolling_vo2 fed by two different signals)
        both got stuck on "pending" until BOTH had (re)run a newly staged
        constant value — even the one that had just been run itself. Real
        call sites never blur into each other here; the "pending" nudge
        for the unrun sibling combo is a graph_builder display concern,
        not a DAG-propagation one.
        """
        result = propagate_run_states(
            fn_own_states={K("A"): "green", K("B"): "green"},
            fn_input_params={K("A"): {}, K("B"): {}},
            fn_outputs={K("A"): {"OutA"}, K("B"): {"OutB"}},
        )
        assert result[fn("A")] == "green"
        assert result[fn("B")] == "green"


# ---------------------------------------------------------------------------
# Multiple outputs per function
# ---------------------------------------------------------------------------


class TestMultipleOutputs:
    def test_all_outputs_get_same_state(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "pending"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"A", "B", "C"}},
        )
        assert result[var("A")] == "pending"
        assert result[var("B")] == "pending"
        assert result[var("C")] == "pending"


# ---------------------------------------------------------------------------
# Multiple inputs — minimum state wins
# ---------------------------------------------------------------------------


class TestMultipleInputs:
    def test_worst_input_determines_function_state(self):
        result = propagate_run_states(
            fn_own_states={
                K("ProducerA"): "pending",
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
        assert result[fn("Consumer")] == "pending"


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


# ---------------------------------------------------------------------------
# Disconnected wirings (deleted required inbound edge) force red and cascade
# ---------------------------------------------------------------------------


class TestDisconnected:
    def test_disconnected_green_forced_red(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {"x": "Raw"}},
            fn_outputs={K("f"): {"Out"}},
            disconnected_fkeys={K("f")},
        )
        assert result[fn("f")] == "red"
        assert result[var("Out")] == "red"

    def test_disconnected_cascades_downstream(self):
        result = propagate_run_states(
            fn_own_states={K("A"): "green", K("B"): "green"},
            fn_input_params={K("A"): {}, K("B"): {"x": "Out"}},
            fn_outputs={K("A"): {"Out"}, K("B"): {"FinalOut"}},
            disconnected_fkeys={K("A")},
        )
        assert result[fn("A")] == "red"
        assert result[var("Out")] == "red"
        assert result[fn("B")] == "red"
        assert result[var("FinalOut")] == "red"

    def test_disconnected_downstream_node_itself_unaffected_by_others(self):
        # Disconnecting B (downstream) must not affect A (upstream).
        result = propagate_run_states(
            fn_own_states={K("A"): "green", K("B"): "green"},
            fn_input_params={K("A"): {}, K("B"): {"x": "Out"}},
            fn_outputs={K("A"): {"Out"}, K("B"): {"FinalOut"}},
            disconnected_fkeys={K("B")},
        )
        assert result[fn("A")] == "green"
        assert result[var("Out")] == "green"
        assert result[fn("B")] == "red"

    def test_disconnected_already_red_stays_red(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "red"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
            disconnected_fkeys={K("f")},
        )
        assert result[fn("f")] == "red"

    def test_disconnected_fkey_not_in_own_states_is_ignored(self):
        # A disconnected fkey with no own-state entry (e.g. invalid output
        # classes upstream) must not spuriously enter the propagation graph.
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
            disconnected_fkeys={K("ghost")},
        )
        assert result[fn("f")] == "green"
        assert K("ghost") not in result

    def test_empty_disconnected_fkeys_is_noop(self):
        result = propagate_run_states(
            fn_own_states={K("f"): "green"},
            fn_input_params={K("f"): {}},
            fn_outputs={K("f"): {"Out"}},
            disconnected_fkeys=set(),
        )
        assert result[fn("f")] == "green"


# ---------------------------------------------------------------------------
# Hidden nodes take no part (2026-10-01)
# ---------------------------------------------------------------------------


class TestHiddenNodes:
    """A hidden node is not a producer of anything on screen. The regression:
    a hidden, disconnected phantom ``pandas.read_csv`` node still produced
    ``DemographicsTable`` in the cascade, and the worst-producer rule held the
    variable red after the visible node had re-run it green."""

    def _phantom_and_real(self, **extra):
        return propagate_run_states(
            fn_own_states={K("real"): "green", K("phantom"): "green"},
            fn_input_params={K("real"): {}, K("phantom"): {}},
            fn_outputs={K("real"): {"Table"}, K("phantom"): {"Table", "Other"}},
            disconnected_fkeys={K("phantom")},
            **extra,
        )

    def test_without_the_hide_the_phantom_drags_the_variable_red(self):
        """Control: this is the bug's shape when the hide is not passed."""
        assert self._phantom_and_real()[var("Table")] == "red"

    def test_a_hidden_node_does_not_colour_its_outputs(self):
        result = self._phantom_and_real(hidden_fkeys={K("phantom")})
        assert result[var("Table")] == "green"
        assert result[fn("real")] == "green"

    def test_a_hidden_node_has_no_state_of_its_own(self):
        result = self._phantom_and_real(hidden_fkeys={K("phantom")})
        assert fn("phantom") not in result
        # A variable only the hidden node produced is not produced at all.
        assert var("Other") not in result

    def test_a_downstream_step_reads_the_visible_producer(self):
        result = propagate_run_states(
            fn_own_states={K("real"): "green", K("phantom"): "red", K("down"): "green"},
            fn_input_params={K("real"): {}, K("phantom"): {}, K("down"): {"t": "Table"}},
            fn_outputs={K("real"): {"Table"}, K("phantom"): {"Table"}, K("down"): {"Out"}},
            hidden_fkeys={K("phantom")},
        )
        assert result[fn("down")] == "green"


def test_hidden_fn_fkeys_maps_a_hidden_node_id_to_its_call_sites():
    """The node token is derived as hidden_wirings derives it, so a hidden
    ``fn__{fn}__{token}`` id (placement-qualified or not) finds its call site."""
    from scistack_gui.domain.graph_builder import hidden_fn_fkeys, wiring_id

    params = {K("loader"): {}, K("other"): {}}
    outputs = {K("loader"): {"Table"}, K("other"): {"Else"}}
    token_for = lambda fn_name, wid: wid  # noqa: E731 — identity tokens
    wid = wiring_id("loader", {}, {"Table"}, {})
    for hidden in (f"fn__loader__{wid}", f"fn__loader__{wid}::main"):
        assert hidden_fn_fkeys(params, outputs, {}, {hidden}, token_for) == {K("loader")}
    assert hidden_fn_fkeys(params, outputs, {}, set(), token_for) == set()
