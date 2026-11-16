"""
The GUI's output-path inputs: parameters that receive ``PathOutput(template)``.

In code, ``inputs={"df": StepLength, "filename": PathOutput("figures/{subject}.png")}``
hands ``filename`` a finished path per combo. On the canvas the same thing
is stated on a function node's Inputs section (an output-path text field on
the argument), or written there by Plot/Stat Studio's "Add to pipeline". The
statement is the ``pathoutput`` intent aspect
(``scidb.provenance.intent.ASPECT_PATHOUTPUT``, blob key ``pathOutputs``) and
rides on the run target as a ``pathoutput`` BINDING from here on
(``edge_resolver.BINDING_PATHOUTPUT``, ref = the template).

**The shape is not defined here.** ``scidb.provenance.intent.normalize_pathoutput_inputs``
owns it; this module is the binding-shaped adapter.

**Where the files go** is not decided here either: a relative template goes
under the project's ``[outputs] folder`` (``scifor.project.outputs``) when
scifor resolves it, and stays relative to the project root otherwise.

**Not part of call identity.** scidb leaves ``PathOutput`` out of version
keys, so stating or editing a template never moves a node id or makes a node
stale.

No I/O — plain dicts, so every rule is pinned by unit tests without a DB.
"""

from __future__ import annotations

import logging

from scidb.provenance.intent import normalize_pathoutput_inputs as normalize

logger = logging.getLogger(__name__)

__all__ = ["apply_to_target", "normalize"]


def apply_to_target(target: dict, templates, context: str = "") -> dict:
    """A COPY of *target* whose stated parameters are bound to their output path.

    Copies rather than mutates (targets share a base dict; see
    ``colname_inputs.apply_to_target``).

    * An output path **replaces** a PathInput, Parameter, ColName or recorded
      constant binding of the same parameter, with a WARN naming it.
    * A variable-bound parameter is **not** replaced: the variable wins and
      the stale statement is dropped with a WARN, so a data input is never
      silently discarded.
    """
    from scistack_gui.domain.edge_resolver import BINDING_VARIABLE, pathoutput_binding

    stated = normalize(templates)
    out = dict(target)
    if not stated:
        return out
    bindings = {p: dict(b) for p, b in (target.get("bindings") or {}).items()}
    constants = dict(target.get("constants") or {})
    prefix = f"[pathoutput] {context}: " if context else "[pathoutput] "

    for param, template in stated.items():
        previous = bindings.get(param)
        if previous is not None and previous.get("kind") == BINDING_VARIABLE:
            logger.warning(
                "%san output path is stated for %r, which is fed by variable %s — "
                "the variable wins and the output path is ignored. Clear it on "
                "the node's Inputs section.",
                prefix,
                param,
                previous.get("ref"),
            )
            continue
        if previous is not None:
            logger.warning(
                "%s%r: output path %r replaces its %s binding %r",
                prefix,
                param,
                template,
                previous.get("kind"),
                previous.get("ref"),
            )
        if param in constants:
            logger.warning(
                "%s%r: output path %r replaces its recorded constant %r",
                prefix,
                param,
                template,
                constants[param],
            )
            del constants[param]
        bindings[param] = pathoutput_binding(template)

    out["bindings"] = bindings
    out["constants"] = constants
    return out
