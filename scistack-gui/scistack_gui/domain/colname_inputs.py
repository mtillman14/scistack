"""
The GUI's ColName inputs: parameters that receive the current column name.

In code, ``inputs={"df": MyVar.for_columns(), "col": ColName()}`` hands
``col`` the name of the column each per-column call is processing. In the
GUI the same thing is stated on a function node's Inputs section: with an
input set to **Run once per column**, any other parameter's ``n/a`` row is a
dropdown ``n/a | ColName``. The choice is stored as the ``colname`` intent
aspect (``scidb.intent.ASPECT_COLNAME``, blob key ``colNameInputs``) and
rides on the run target as a ``colname`` BINDING from here on
(``edge_resolver.BINDING_COLNAME``), so ``build_run_inputs``, MATLAB
generation and code export only render it.

**The shape is not defined here.** ``scidb.intent.normalize_colname_inputs``
owns it; this module is the binding-shaped adapter.

**Not part of call identity.** scidb leaves ColName out of version keys
(``ForEachConfig._get_direct_constants``), and ``compute_call_id`` reads only
variable and PathInput bindings, so stating ColName never moves a node id.

No I/O — plain dicts, so every rule is pinned by unit tests without a DB.
"""

from __future__ import annotations

import logging

from scidb.intent import normalize_colname_inputs as normalize

logger = logging.getLogger(__name__)

__all__ = ["apply_to_target", "has_iterate_input", "normalize"]


def has_iterate_input(bindings: "dict[str, dict] | None") -> bool:
    """Does any variable binding iterate (``for_columns``)? A deferred
    ``ColName()`` has nothing to resolve against without one."""
    from scistack_gui.domain.edge_resolver import BINDING_VARIABLE

    return any(
        b.get("kind") == BINDING_VARIABLE and b.get("iterate")
        for b in (bindings or {}).values()
    )


def apply_to_target(target: dict, params, context: str = "") -> dict:
    """A COPY of *target* whose *params* are bound to ColName.

    Copies rather than mutates, for the reason
    ``column_selection.apply_to_bindings`` gives: targets are built from a
    shared base dict, so an in-place stamp would leak through the alias.

    * ColName **replaces** whatever bound the parameter before — a PathInput,
      a Parameter, a recorded constant — and the replacement is WARNed with
      what it replaced. The panel only offers ColName on a row with no
      variable feeding it, but a Parameter/PathInput-fed row also shows
      ``n/a`` there, so this is reachable.
    * A variable-bound parameter is **not** replaced (the panel never offers
      that; a stale statement left after a variable was wired is dropped
      with a WARN rather than silently discarding the data input).
    * No iterate input on the target: every ColName statement is dropped with
      a WARN. Run as-is, scifor would refuse the run ("deferred ColName
      requires an iterate input"); a stale statement must not fail a run.
    """
    from scistack_gui.domain.edge_resolver import (
        BINDING_VARIABLE,
        colname_binding,
    )

    names = normalize(params)
    out = dict(target)
    if not names:
        return out
    bindings = {p: dict(b) for p, b in (target.get("bindings") or {}).items()}
    constants = dict(target.get("constants") or {})
    prefix = f"[colname] {context}: " if context else "[colname] "

    if not has_iterate_input(bindings):
        logger.warning(
            "%sColName is stated for %s, but no input runs once per column — "
            "ignored. Tick \"Run once per column\" on an input, or set these "
            "back to n/a.",
            prefix,
            names,
        )
        return out

    for param in names:
        previous = bindings.get(param)
        if previous is not None and previous.get("kind") == BINDING_VARIABLE:
            logger.warning(
                "%sColName is stated for %r, which is fed by variable %s — the "
                "variable wins and ColName is ignored. Set it back to n/a on "
                "the node's Inputs section.",
                prefix,
                param,
                previous.get("ref"),
            )
            continue
        if previous is not None:
            logger.warning(
                "%s%r: ColName replaces its %s binding %r",
                prefix,
                param,
                previous.get("kind"),
                previous.get("ref"),
            )
        if param in constants:
            logger.warning(
                "%s%r: ColName replaces its recorded constant %r",
                prefix,
                param,
                constants[param],
            )
            del constants[param]
        bindings[param] = colname_binding()

    out["bindings"] = bindings
    out["constants"] = constants
    return out
