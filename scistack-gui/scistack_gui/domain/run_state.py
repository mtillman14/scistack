"""
Pure DAG propagation for pipeline run states.

Computes effective run states for function and variable nodes by propagating
staleness through the dependency graph. No I/O — takes pre-computed own-states
and returns a flat dict of node_id → state.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_STATE_ORDER = {"red": 0, "grey": 1, "green": 2}


FnKey = tuple[str, str]
"""(fn_name, call_id) — see graph_builder.AggregatedData."""


def propagate_run_states(
    fn_own_states: dict[FnKey, str],
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    fn_constants: dict[FnKey, set] | None = None,
    pending_constants: dict[str, set] | None = None,
) -> dict[str, str]:
    """Propagate run states through the DAG, per for_each call site.

    Each (fn_name, call_id) is a separate node in the propagation graph.
    Variable types with multiple producers (the same output written by
    multiple call sites) take the most pessimistic producer state so a
    broken upstream variant correctly degrades downstream nodes.

    Args:
        fn_own_states: {(fn_name, call_id): "green"|"grey"|"red"}.
        fn_input_params: {(fn_name, call_id): {param_name: variable_type_name}}.
        fn_outputs: {(fn_name, call_id): {output_type_name, ...}}.
        fn_constants: {(fn_name, call_id): {constant_param_name, ...}} — optional.
        pending_constants: {constant_name: {pending_value, ...}} — optional.

    Returns:
        {node_id: "green"|"grey"|"red"} for fn__ and var__ nodes, where
        function nodes use ``fn__{fn_name}__{call_id}`` IDs.
    """
    logger.info("[run_state] propagate_run_states: processing %d function call site(s)", len(fn_own_states))

    # Make a mutable copy so we don't modify the caller's dict.
    fn_own_state = dict(fn_own_states)

    # Downgrade "green" → "grey" for call sites that have unrun pending
    # constant values.  fn_constants is keyed per call site, so a call site
    # that doesn't use the pending constant is unaffected.
    logger.debug("[run_state] Step 1: Checking for pending constants that affect green nodes")
    if fn_constants and pending_constants:
        downgrade_count = 0
        for fkey in fn_own_state:
            if fn_own_state[fkey] == "green":
                for const_name in fn_constants.get(fkey, set()):
                    if pending_constants.get(const_name):
                        logger.debug(
                            "[run_state] downgrading %s green→grey: pending constant %r",
                            fkey, const_name,
                        )
                        fn_own_state[fkey] = "grey"
                        downgrade_count += 1
                        break
        if downgrade_count > 0:
            logger.debug("[run_state] downgraded %d node(s) due to pending constants", downgrade_count)
    else:
        logger.debug("[run_state] no pending constants to check")

    # --- DAG propagation ---
    # var_producers[var_type] = set of FnKeys producing this variable.
    # The variable's state is the worst (min) of all producer states.
    logger.info("[run_state] Step 2: Building variable producer map and propagating states through DAG")
    var_producers: dict[str, set[FnKey]] = {}
    for fkey, out_types in fn_outputs.items():
        for ot in out_types:
            var_producers.setdefault(ot, set()).add(fkey)
    logger.debug("[run_state] identified %d variable type(s) with producer(s)", len(var_producers))

    fn_effective_state: dict[FnKey, str] = {}
    var_state: dict[str, str] = {}

    fn_input_types: dict[FnKey, set] = {
        fkey: set(params.values()) for fkey, params in fn_input_params.items()
    }

    remaining = set(fn_own_state.keys())
    logger.debug("[run_state] starting DAG propagation for %d call site(s)", len(remaining))
    iteration = 0
    for _ in range(len(remaining) + 1):
        if not remaining:
            break
        progress = False
        iteration += 1
        logger.debug("[run_state] propagation iteration %d: %d node(s) remaining", iteration, len(remaining))
        for fkey in list(remaining):
            input_var_states: list[str] = []
            all_resolved = True
            for vtype in fn_input_types.get(fkey, set()):
                if vtype in var_state:
                    input_var_states.append(var_state[vtype])
                elif vtype not in var_producers:
                    # Root variable — no upstream producer, treat as green.
                    input_var_states.append("green")
                else:
                    all_resolved = False
                    break
            if not all_resolved:
                continue

            all_states = [fn_own_state[fkey]] + input_var_states
            fn_effective_state[fkey] = min(all_states, key=lambda s: _STATE_ORDER[s])
            for vtype in fn_outputs.get(fkey, set()):
                # Aggregate across producers — take most pessimistic.
                if vtype in var_state:
                    var_state[vtype] = min(
                        [var_state[vtype], fn_effective_state[fkey]],
                        key=lambda s: _STATE_ORDER[s],
                    )
                else:
                    var_state[vtype] = fn_effective_state[fkey]
            remaining.remove(fkey)
            progress = True

        if not progress:
            # Cycle or unresolvable — mark remaining as red.
            logger.warning(
                "[run_state] DAG propagation stalled at iteration %d — possible cycle among %d node(s): %s",
                iteration, len(remaining), sorted(remaining),
            )
            for fkey in remaining:
                fn_effective_state[fkey] = "red"
                for vtype in fn_outputs.get(fkey, set()):
                    var_state[vtype] = "red"
            break

    logger.info("[run_state] DAG propagation complete after %d iteration(s)", iteration)

    logger.info("[run_state] Step 3: Building final result mapping")
    result: dict[str, str] = {}
    for fkey, state in fn_effective_state.items():
        fn_name, call_id = fkey
        result[f"fn__{fn_name}__{call_id}"] = state
    for vtype, state in var_state.items():
        result[f"var__{vtype}"] = state

    state_counts = {"green": 0, "grey": 0, "red": 0}
    for s in result.values():
        state_counts[s] = state_counts.get(s, 0) + 1
    logger.info("[run_state] propagate_run_states complete: %d total nodes (%d green, %d grey, %d red)",
                len(result), state_counts["green"], state_counts["grey"], state_counts["red"])

    return result
