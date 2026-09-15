"""
Pure normalisation of a GUI column selection.

A *column selection* is the GUI's way of saying ``MyVar["filename"]`` /
``MyVar[["a", "b"]]`` / ``MyVar.for_columns([...])`` for one function
parameter. It is stored per function node in ``_node_config`` (see
``docs/claude/gui-run-options-flow.md`` for that table's three id shapes) as::

    {"columnSelections": {"table_in": {"columns": ["filename"], "iterate": false}}}

and rides on the parameter's VARIABLE BINDING from there
(``edge_resolver.variable_binding``) down to ``build_run_inputs``, MATLAB
generation and code export.

No I/O — everything here works on plain dicts, so the tolerated input shapes
can be pinned by unit tests without a database.

**Deliberately not part of the binding's identity.** ``variable_types_view``
still returns bare type names, so ``graph_builder.wiring_id`` and
``variant_resolver.compute_call_id`` are untouched by a column pick. That
asymmetry is load-bearing rather than an oversight: scidb's FORWARD
``to_call_id`` does fold ``ColumnSelection.to_key()`` into ``__inputs``, but
the GUI never sees the forward id — provenance stores an input edge as
``(param -> record -> variable_type)`` with no trace of the columns, so
``provenance_query.pipeline_variants`` reconstructs ``__inputs = {"table_in":
"Trials"}`` and the canvas node id is built from THAT. Feeding columns into
the GUI's ``compute_call_id`` would predict an id no record ever carries and
silently break combo hiding on every column-selected node. See
``docs/claude/column-selection.md`` §From the GUI.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def normalize(raw) -> "dict | None":
    """``{"columns": [...], "iterate": bool}`` for a stored selection, or
    ``None`` when there is effectively no selection at all.

    Tolerates every shape the selection can arrive in, because it crosses a
    JSON boundary written by a frontend that has been through several
    revisions:

    * ``"filename"`` — a bare column name;
    * ``["a", "b"]`` — a bare column list;
    * ``{"columns": [...], "iterate": bool}`` — the canonical dict;
    * ``{"columns": "a"}`` / ``{"iterate": true}`` — partial dicts;
    * a binding dict that also carries ``kind``/``ref`` (so
      :func:`from_binding` is this same function).

    Returns ``None`` for an EMPTY, non-iterate selection. That case must not
    survive as a ``ColumnSelection`` at all: no columns and no iteration means
    "the whole variable", which is exactly what binding the bare class already
    does, and wrapping it anyway would fork the version key for no change in
    what the function receives.

    An empty selection WITH ``iterate`` is kept — ``MyVar.for_columns()`` means
    "every data column, one call each", resolved at for_each time.
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        columns: list = [raw] if raw else []
        iterate = False
    elif isinstance(raw, (list, tuple)):
        columns = [str(c) for c in raw if c]
        iterate = False
    elif isinstance(raw, dict):
        cols = raw.get("columns")
        if isinstance(cols, str):
            cols = [cols] if cols else []
        columns = [str(c) for c in (cols or []) if c]
        iterate = bool(raw.get("iterate"))
    else:
        logger.warning(
            "[column_selection] ignoring a selection of unexpected type %s: %r",
            type(raw).__name__,
            raw,
        )
        return None

    if not columns and not iterate:
        return None
    return {"columns": columns, "iterate": iterate}


def from_binding(binding: dict) -> "dict | None":
    """The selection carried by a variable binding, or ``None``.

    A binding without ``columns``/``iterate`` is the ordinary whole-variable
    case, so this returns ``None`` for it.
    """
    if not binding:
        return None
    if "columns" not in binding and "iterate" not in binding:
        return None
    return normalize(binding)


def describe(sel: "dict | None") -> str:
    """The ONE spelling of a selection used in logs and in the canvas chip.

    Two spellings of the same thing is how a user ends up reading a log line
    that does not match what the node shows, so there is exactly one.
    """
    if not sel:
        return "whole variable"
    columns = sel.get("columns") or []
    if sel.get("iterate"):
        return "per column" if not columns else f"per column ({len(columns)})"
    if len(columns) == 1:
        return f'"{columns[0]}"'
    return f"{len(columns)} columns"


def apply_to_bindings(
    bindings: "dict[str, dict] | None",
    selections: "dict | None",
    context: str = "",
) -> "dict[str, dict]":
    """A COPY of *bindings* with each selection stamped onto its variable
    binding.

    Copies rather than mutates on purpose: ``_inferred_targets`` builds every
    target from one shared ``base`` dict, so an in-place stamp would be
    applied through an alias and re-applied on the next derivation.

    A selection naming a parameter that is not variable-bound is dropped with
    a WARN, never raised on. That state is reachable without anything being
    broken — a parameter renamed in source, or rewired from a variable to a
    PathInput, leaves the old key behind in a config nobody rewrote — and a
    mid-run exception for a stale config entry would be a far worse failure
    than running with the whole variable.
    """
    from scistack_gui.domain.edge_resolver import BINDING_VARIABLE

    out = {p: dict(b) for p, b in (bindings or {}).items()}
    if not selections:
        return out

    prefix = f"[column_selection] {context}: " if context else "[column_selection] "
    for param, raw in selections.items():
        sel = normalize(raw)
        if sel is None:
            continue
        binding = out.get(param)
        if binding is None or binding.get("kind") != BINDING_VARIABLE:
            logger.warning(
                "%sselection %s names parameter %r, which is %s — the "
                "selection is ignored. Delete it on the node's Inputs "
                "section, or rewire that parameter to a variable.",
                prefix,
                describe(sel),
                param,
                "not bound at all"
                if binding is None
                else f"bound to a {binding.get('kind')}, not a variable",
            )
            continue
        binding["columns"] = list(sel["columns"])
        binding["iterate"] = sel["iterate"]
    return out
