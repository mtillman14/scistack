"""
Pure DAG propagation for pipeline run states.

Computes effective run states for function and variable nodes by propagating
staleness through the dependency graph. No I/O — takes pre-computed own-states
and returns a flat dict of node_id → state.
"""

from __future__ import annotations

import logging

from scistack_gui.ids import fn_node_id, var_node_id

logger = logging.getLogger(__name__)

_STATE_ORDER = {"red": 0, "pending": 1, "green": 2}


FnKey = tuple[str, str]
"""(fn_name, call_id) — see graph_builder.AggregatedData."""


def propagate_run_states(
    fn_own_states: dict[FnKey, str],
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    disconnected_fkeys: set[FnKey] | None = None,
) -> dict[str, str]:
    """Propagate run states through the DAG, per for_each call site.

    Each (fn_name, call_id) is a separate node in the propagation graph.
    Variable types with multiple producers (the same output written by
    multiple call sites) take the most pessimistic producer state so a
    broken upstream variant correctly degrades downstream nodes.

    Args:
        fn_own_states: {(fn_name, call_id): "green"|"pending"|"red"}.
        fn_input_params: {(fn_name, call_id): {param_name: variable_type_name}}.
        fn_outputs: {(fn_name, call_id): {output_type_name, ...}}.
        disconnected_fkeys: {(fn_name, call_id), ...} — call sites whose
            wiring has a user-hidden required inbound edge (see
            graph_builder.hidden_wirings/wiring_disconnected_fkeys). Forced
            to "red" regardless of their own DB state (a disconnected input
            always wins over pending/green) — the DAG loop below then
            cascades that redness downstream through var_state exactly like
            any other red own-state, no separate cascade needed.

    Returns:
        {node_id: "green"|"pending"|"red"} for fn__ and var__ nodes, where
        function nodes use ``fn__{fn_name}__{call_id}`` IDs.

    State vocabulary: scidb node state is BINARY (green/red). "pending" is
    a GUI-ONLY third state meaning "a change staged in the GUI (an unrun
    pending constant value) — nothing in the database yet"; it never comes
    from scidb. A *real* call site's own state is never downgraded to
    "pending" here — a recorded, executed combo is either green or red on
    its own merits, full stop. "pending" for an unrun constant-value combo
    is represented purely by the SYNTHESIZED staged row that
    graph_builder.group_call_sites_by_wiring adds for that (not yet
    existing) combo; the real combo sitting next to it keeps its own true
    state, so "the old variant is green, the new unrun one is yellow"
    displays correctly instead of blurring into one color for the whole
    node (see graph_builder.pending_value_group_coverage).
    """
    logger.info(
        "[run_state] propagate_run_states: processing %d function call site(s)",
        len(fn_own_states),
    )

    # Make a mutable copy so we don't modify the caller's dict.
    fn_own_state = dict(fn_own_states)

    # Disconnected wins over everything else — a call site missing a
    # required inbound edge is forced red even if it was green or staged
    # pending, before propagation so the cascade below sees it as red.
    if disconnected_fkeys:
        forced = 0
        for fkey in disconnected_fkeys:
            if fkey not in fn_own_state:
                continue
            if fn_own_state[fkey] != "red":
                logger.debug(
                    "[run_state] forcing %s red: disconnected required input", fkey
                )
                forced += 1
            fn_own_state[fkey] = "red"
        if forced:
            logger.info(
                "[run_state] forced %d node(s) red due to disconnected input(s)",
                forced,
            )

    # --- DAG propagation ---
    # var_producers[var_type] = set of FnKeys producing this variable.
    # The variable's state is the worst (min) of all producer states.
    logger.info(
        "[run_state] Building variable producer map and propagating states through DAG"
    )
    var_producers: dict[str, set[FnKey]] = {}
    for fkey, out_types in fn_outputs.items():
        for ot in out_types:
            var_producers.setdefault(ot, set()).add(fkey)
    logger.debug(
        "[run_state] identified %d variable type(s) with producer(s)",
        len(var_producers),
    )

    fn_effective_state: dict[FnKey, str] = {}
    var_state: dict[str, str] = {}

    # A binding may be a LIST — EachOf, which a manual edge drawn beside a
    # still-visible history edge produces (graph_builder.manual_input_overrides).
    # Every source counts: if any producer of any of them is red, the consumer
    # cannot be current. Empty strings are unwired params and are dropped rather
    # than treated as a root variable; both spellings resolved to "green" before,
    # so this changes nothing except that `set(params.values())` no longer
    # raises on an unhashable list.
    fn_input_types: dict[FnKey, set] = {}
    for fkey, params in fn_input_params.items():
        types: set = set()
        for value in params.values():
            if isinstance(value, (list, set, tuple)):
                types.update(vt for vt in value if vt)
            elif value:
                types.add(value)
        fn_input_types[fkey] = types

    remaining = set(fn_own_state.keys())
    logger.debug(
        "[run_state] starting DAG propagation for %d call site(s)", len(remaining)
    )
    iteration = 0
    for _ in range(len(remaining) + 1):
        if not remaining:
            break
        progress = False
        iteration += 1
        logger.debug(
            "[run_state] propagation iteration %d: %d node(s) remaining",
            iteration,
            len(remaining),
        )
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
                iteration,
                len(remaining),
                sorted(remaining),
            )
            for fkey in remaining:
                fn_effective_state[fkey] = "red"
                for vtype in fn_outputs.get(fkey, set()):
                    var_state[vtype] = "red"
            break

    logger.info("[run_state] DAG propagation complete after %d iteration(s)", iteration)

    logger.info("[run_state] Building final result mapping")
    result: dict[str, str] = {}
    for fkey, state in fn_effective_state.items():
        fn_name, call_id = fkey
        result[fn_node_id(fn_name, call_id)] = state
    for vtype, state in var_state.items():
        result[var_node_id(vtype)] = state

    state_counts = {"green": 0, "pending": 0, "red": 0}
    for s in result.values():
        state_counts[s] = state_counts.get(s, 0) + 1
    logger.info(
        "[run_state] propagate_run_states complete: %d total nodes (%d green, %d pending, %d red)",
        len(result),
        state_counts["green"],
        state_counts["pending"],
        state_counts["red"],
    )

    return result
