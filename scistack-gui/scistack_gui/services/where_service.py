"""A function node's ``where=`` filters, for every way the node can run.

The filters are stated in the node's Filters section and stored in its saved
config under ``whereFilters`` as a list of dicts (``{variable, column, op,
value}``). Several rows mean AND: one run that every filter must pass.

**One route.** The per-node Run, a pipeline run, the generated MATLAB command
and both code exports all read the STORED config through this module. None
reads the live canvas data, so there is no second value that could disagree
(docs/claude/gui-run-options-flow.md, "these two can disagree").

**Not the owner of what a filter is.** ``scidb.model.filters`` turns a dict
into a Filter (``filters_from_dicts``), ANDs them (``all_of``) and writes
the Python text (``where_source``). This module only finds the node's dicts
and words the MATLAB spelling, which has no scidb counterpart.

See docs/claude/where-in-gui.md.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

CONFIG_KEY = "whereFilters"

__all__ = [
    "CONFIG_KEY",
    "check_targets",
    "matlab_where_for_node",
    "matlab_where_source",
    "stated_filters",
    "where_for_node",
]


def stated_filters(db, node_id: "str | None") -> list[dict]:
    """The node's filter rows as stored, without the rows the user added
    and has not filled in. Empty for no node or no filters."""
    if not node_id:
        return []
    from scidb.model.filters import is_blank_filter_dict
    from scistack_gui.stores import pipeline_store

    raw = (pipeline_store.get_node_config(db, node_id) or {}).get(CONFIG_KEY) or []
    if not isinstance(raw, list):
        logger.warning(
            "[where] node=%s: stored %s is %s, not a list; read as no filter",
            node_id,
            CONFIG_KEY,
            type(raw).__name__,
        )
        return []
    return [row for row in raw if not is_blank_filter_dict(row)]


def _variable_class(name: str):
    from scistack_gui.project import registry

    try:
        return registry.get_variable_class(name)
    except KeyError:
        return None


def where_for_node(db, node_id: "str | None", *, route: str):
    """The ``where=`` argument of the node's ``for_each``: one Filter (its
    rows ANDed), or None.

    *route* names the caller in the log line, whose ABSENCE is the diagnostic
    when the panel shows a filter and a run used every row. Raises ValueError
    for a half-filled row or an unknown variable: running unfiltered instead
    would compute on rows the user meant to leave out.
    """
    from scidb.model.filters import all_of, filters_from_dicts, where_source

    rows = stated_filters(db, node_id)
    if not rows:
        return None
    try:
        where = all_of(filters_from_dicts(rows, _variable_class))
    except ValueError:
        logger.warning(
            "[where] node=%s route=%s: stored filters cannot be used: %r",
            node_id,
            route,
            rows,
        )
        raise
    logger.info(
        "[where] node=%s route=%s filters=%d -> where=%s",
        node_id,
        route,
        len(rows),
        where_source(where),
    )
    return where


# ---------------------------------------------------------------------------
# MATLAB spelling
# ---------------------------------------------------------------------------

_MATLAB_OPS = {"==": "==", "!=": "~=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}


def _matlab_value(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return '"' + str(value).replace('"', '""') + '"'


def matlab_where_source(rows: "list[dict]") -> str:
    """The MATLAB text of a ``where`` argument for these filter dicts.

    One filter is written bare (``Age() > 80``), several as the cell that
    ``scidb.for_each`` reads as AND (``{Age() > 80, Sex() == "M"}``,
    ``+scidb/all_of.m``). A column of a table is ``Gait("Side") == "L"``.
    Empty text for no filter.
    """
    from scidb.model.filters import filter_parts, is_blank_filter_dict

    terms = []
    for row in rows or []:
        if is_blank_filter_dict(row):
            continue
        name, column, op, value = filter_parts(row)
        subject = f"{name}({_matlab_value(column)})" if column else f"{name}()"
        terms.append(f"{subject} {_MATLAB_OPS[op]} {_matlab_value(value)}")
    if not terms:
        return ""
    if len(terms) == 1:
        return terms[0]
    return "{" + ", ".join(terms) + "}"


def matlab_where_for_node(db, node_id: "str | None", *, route: str) -> str:
    """:func:`matlab_where_source` of the node's stored filters, logged like
    :func:`where_for_node`."""
    rows = stated_filters(db, node_id)
    if not rows:
        return ""
    try:
        text = matlab_where_source(rows)
    except ValueError:
        logger.warning(
            "[where] node=%s route=%s: stored filters cannot be used: %r",
            node_id,
            route,
            rows,
        )
        raise
    logger.info(
        "[where] node=%s route=%s filters=%d -> 'where', %s",
        node_id,
        route,
        len(rows),
        text,
    )
    return text


def check_targets(db, variable: str) -> list[dict]:
    """The function nodes (root canvas) that take *variable* as an input and
    state at least one filter: ``[{node_id, function_name, filters}]``.

    What "Check a where= filter…" in an ordinary Plot Studio figure lists, so
    Apply always has a node to write to. Read from the built graph's
    ``input_params`` and stored ``whereFilters``: the same two facts the canvas
    draws, never a second derivation.
    """
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    graph = get_pipeline_graph(db, run_states=False)
    found = []
    for node in graph.get("nodes", []):
        if node.get("type") != "functionNode":
            continue
        data = node.get("data") or {}
        if variable not in (data.get("input_params") or {}).values():
            continue
        rows = [r for r in (data.get("whereFilters") or []) if not _blank(r)]
        if rows:
            found.append(
                {"node_id": node["id"], "function_name": data.get("label"), "filters": rows}
            )
    logger.info("[where_check] nodes taking %s with filters: %d", variable, len(found))
    return found


def _blank(row) -> bool:
    from scidb.model.filters import is_blank_filter_dict

    return is_blank_filter_dict(row)
