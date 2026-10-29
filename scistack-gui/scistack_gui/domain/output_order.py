"""The order of a function node's outputs.

Output order is the order of ``outputs=`` in the for_each call: a function's
return order, or a for_columns split's group pairing. It is ONE concept with
ONE owner here (``.claude/plan-output-order-owner.md``). The node's output
handles are drawn in this order, and every consumer reads it from the
handles: the Inputs picker and output tooltips, the Python canvas run
(``execution_service.run_output_types``) and the MATLAB command
(``matlab_command_service``). Nothing else derives it.

The fact comes from scidb: each run records its ``outputs=`` order
(``_run_outputs``, read by ``provenance_query.recorded_output_orders``).
``_invocation_output.output_num`` is a slot, not this position.

No I/O except :func:`recorded_orders`, which is the one query.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def node_output_order(
    wired,
    recorded: "list[list[str]] | None" = None,
    edge_order: "list[str] | None" = None,
    declared: "list[str] | None" = None,
) -> list[str]:
    """The order to draw (and run) *wired*, a node's output types.

    1. The newest recorded ``outputs=`` order that names any of them (a run
       wrote them in that order), restricted to the ones still wired.
    2. Wired outputs no run recorded, in *declared* order (a MATLAB
       function's signature, already mapped to classes), then in *edge_order*
       (the order their output edges were drawn).
    3. Anything left, alphabetically (logged: it means no source knew).
    """
    wired_list = list(dict.fromkeys(str(t) for t in wired if t))
    wired_set = set(wired_list)
    out: list[str] = []
    for order in recorded or []:
        if wired_set & set(order):
            out = [t for t in order if t in wired_set]
            break
    for source in (declared or [], edge_order or []):
        out.extend(t for t in source if t in wired_set and t not in out)
    rest = sorted(wired_set - set(out))
    if rest:
        logger.info(
            "[output_order] no recorded, declared or drawn order for %s; "
            "appended alphabetically after %s",
            rest,
            out,
        )
        out.extend(rest)
    return out


def recorded_orders(db, function_names) -> "dict[str, list[list[str]]]":
    """``{function_name: [order, ...]}`` newest first, from scidb's run log
    (``provenance_query.recorded_output_orders``). Empty when the database
    cannot answer; output order then falls back as documented above."""
    try:
        from scidb.provenance_query import recorded_output_orders

        return recorded_output_orders(db._duck, function_names)
    except Exception:
        logger.warning("[output_order] recorded_output_orders failed", exc_info=True)
        return {}


def writes_together(order: "list[str]", recorded: "list[list[str]] | None") -> bool:
    """Did one run write two or more of *order*'s outputs together, i.e. is
    this a multi-output function rather than separate runs into separate
    variables? With no record at all, a node with several drawn outputs is
    taken to mean one call writing all of them."""
    if len(order) < 2:
        return False
    if not recorded:
        return True
    wanted = set(order)
    return any(len(wanted & set(r)) >= 2 for r in recorded)


def recorded_params(db, function_names) -> "dict[str, dict[str, str]]":
    """``{function_name: {MATLAB output name: output type}}`` from scidb's
    run log (``provenance_query.recorded_output_params``): which variable
    each NAMED output saved to. Empty when the database cannot answer."""
    try:
        from scidb.provenance_query import recorded_output_params

        return recorded_output_params(db._duck, function_names)
    except Exception:
        logger.warning("[output_order] recorded_output_params failed", exc_info=True)
        return {}


def matlab_output_order(
    signature: "list[str]",
    param_to_class: "dict[str, str]",
    classes,
    recorded: "list[list[str]] | None" = None,
    edge_order: "list[str] | None" = None,
) -> list[str]:
    """A MATLAB function's output CLASSES in the order its for_each outputs
    cell must list them.

    MATLAB pairs return values with the outputs cell BY POSITION, so for an
    ordinary function the order is the CURRENT signature, each name mapped
    to its class by name (*param_to_class*), never a recorded position: a
    signature reordered after a run must not swap the variables. A function
    writing MORE classes than its signature declares (a for_columns split:
    one return value per group) has no per-output names; its order is the
    recorded ``outputs=`` order, as for a Python function.
    """
    classes = list(dict.fromkeys(str(c) for c in classes if c))
    if len(classes) > len(signature):
        return node_output_order(classes, recorded, edge_order=edge_order)
    declared = [param_to_class[p] for p in signature if p in param_to_class]
    return node_output_order(classes, None, edge_order=edge_order, declared=declared)
