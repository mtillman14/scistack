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


def propagate_run_states(
    fn_own_states: dict[str, str],
    fn_input_params: dict[str, dict],
    fn_outputs: dict[str, set],
    fn_constants: dict[str, set] | None = None,
    pending_constants: dict[str, set] | None = None,
) -> dict[str, str]:
    """Propagate run states through the DAG.

    Args:
        fn_own_states: {function_name: "green"|"grey"|"red"} — pre-computed
            own state for each function (from scihist.check_node_state or similar).
        fn_input_params: {fn_name: {param_name: variable_type_name}} — variable
            inputs only (not constants).
        fn_outputs: {fn_name: {output_type_name, ...}}.
        fn_constants: {fn_name: {constant_param_name, ...}} — optional.
        pending_constants: {constant_name: {pending_value, ...}} — optional.

    Returns:
        {node_id: "green"|"grey"|"red"} for fn__ and var__ nodes.
    """
    # Make a mutable copy so we don't modify the caller's dict.
    fn_own_state = dict(fn_own_states)

    # Downgrade "green" → "grey" for functions that have unrun pending constant values.
    if fn_constants and pending_constants:
        for fn_name in fn_own_state:
            if fn_own_state[fn_name] == "green":
                for const_name in fn_constants.get(fn_name, set()):
                    if pending_constants.get(const_name):
                        logger.debug(
                            "Downgrading %s green→grey: pending constant %r",
                            fn_name, const_name,
                        )
                        fn_own_state[fn_name] = "grey"
                        break

    # --- DAG propagation ---
    var_producer: dict[str, str] = {}
    for fn_name, out_types in fn_outputs.items():
        for ot in out_types:
            var_producer[ot] = fn_name

    fn_effective_state: dict[str, str] = {}
    var_state: dict[str, str] = {}

    fn_input_types: dict[str, set] = {
        fn: set(params.values()) for fn, params in fn_input_params.items()
    }

    remaining = set(fn_own_state.keys())
    for _ in range(len(remaining) + 1):
        if not remaining:
            break
        progress = False
        for fn_name in list(remaining):
            input_var_states: list[str] = []
            all_resolved = True
            for vtype in fn_input_types.get(fn_name, set()):
                if vtype in var_state:
                    input_var_states.append(var_state[vtype])
                elif vtype not in var_producer:
                    # Root variable — no upstream producer, treat as green.
                    input_var_states.append("green")
                else:
                    all_resolved = False
                    break
            if not all_resolved:
                continue

            all_states = [fn_own_state[fn_name]] + input_var_states
            fn_effective_state[fn_name] = min(all_states, key=lambda s: _STATE_ORDER[s])
            for vtype in fn_outputs.get(fn_name, set()):
                var_state[vtype] = fn_effective_state[fn_name]
            remaining.remove(fn_name)
            progress = True

        if not progress:
            # Cycle or unresolvable — mark remaining as red.
            logger.warning(
                "DAG propagation stalled — possible cycle among: %s",
                sorted(remaining),
            )
            for fn_name in remaining:
                fn_effective_state[fn_name] = "red"
                for vtype in fn_outputs.get(fn_name, set()):
                    var_state[vtype] = "red"
            break

    result: dict[str, str] = {}
    for fn_name, state in fn_effective_state.items():
        result[f"fn__{fn_name}"] = state
    for vtype, state in var_state.items():
        result[f"var__{vtype}"] = state

    return result
