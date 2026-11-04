"""Submodule ports: the one owner of "outside -> port -> inside".

Plan ``.claude/plan-submodule-ports.md``; concept doc
``docs/claude/submodule-ports.md``.

A connection crossing a submodule boundary is stored as ordinary rows on the
two canvases it touches:

    B -> [output node "C"]     on the submodule's canvas (an inner edge)
    [placement].out__C -> C    on the parent's canvas    (an outer edge)

:func:`resolve_wiring` joins them back into the **resolved connection**
``B -> C``, the same edge a canvas with no submodule would store. Everything
that asks what a node is wired to (runs, identity, the MATLAB command, reports)
reads resolved connections through ``edge_view``, so a port never reaches a
wiring id, a connection id or a computation's identity.

**A submodule has one wiring** (user decision 2026-11-03). A node's stated wiring
is one thing, so every placement of a submodule must connect a port to the same
things (:func:`placement_conflict`). Placements still differ by ``binding``; for
different connections, Duplicate makes an independent copy. So a port resolves
without asking which placement: its connections are the union of its placements'.

An input/output node owns its port's name, and its height on the submodule's
canvas is the port's order: top to bottom, so the canvas shows what the placement
shows. A placement's ports are read from those nodes (:func:`ports_of_scope`),
never inferred from edges.

Pure: no database. ``domain.edge_view`` is the one caller that reads the rows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from scistack_gui.ids import (
    FN_ID_PREFIX,
    IN_HANDLE_PREFIX,
    OUT_HANDLE_PREFIX,
    VAR_ID_PREFIX,
    handle_name,
    strip_placement,
)

logger = logging.getLogger(__name__)

INPUT_PORT_NODE = "inputPortNode"
OUTPUT_PORT_NODE = "outputPortNode"
PORT_NODE_TYPES = frozenset({INPUT_PORT_NODE, OUTPUT_PORT_NODE})

#: Prefix of a placement's id (``pipeline_store.add_pipeline_use``). An edge
#: naming one that no longer exists points at a removed placement.
_USE_ID_PREFIX = "use_"


def has_port_nodes(manual_nodes: dict) -> bool:
    return any(m.get("type") in PORT_NODE_TYPES for m in manual_nodes.values())


def ports_of_scope(
    scope_id: str,
    manual_nodes: dict,
    hidden_node_ids: "set[str] | frozenset" = frozenset(),
    positions: "dict | None" = None,
) -> dict:
    """A submodule's ports in order: ``{"inputs": [names], "outputs": [names]}``.

    The one reader of what a placement shows. Ordered by height on the canvas
    (*positions*: ``{node id: {"x", "y"}}`` for this scope), then by name; a node
    with no position sorts last. A hidden input/output node is a removed port.
    """
    positions = positions or {}
    found: dict[str, list[tuple[float, str]]] = {
        INPUT_PORT_NODE: [],
        OUTPUT_PORT_NODE: [],
    }
    for node_id, meta in manual_nodes.items():
        node_type = meta.get("type")
        if node_type not in PORT_NODE_TYPES or meta.get("pipeline_id") != scope_id:
            continue
        if node_id in hidden_node_ids:
            continue
        height = (positions.get(node_id) or {}).get("y", float("inf"))
        found[node_type].append((height, meta.get("label") or ""))
    return {
        "inputs": [name for _height, name in sorted(found[INPUT_PORT_NODE])],
        "outputs": [name for _height, name in sorted(found[OUTPUT_PORT_NODE])],
    }


def _edge_key(edge: dict) -> tuple:
    return (
        edge.get("source"),
        edge.get("target"),
        edge.get("sourceHandle"),
        edge.get("targetHandle"),
    )


@dataclass
class Wire:
    """One resolved connection, and the stored edges it was traced through (one
    for a direct edge)."""

    edge: dict
    via: tuple[str, ...] = ()

    @property
    def traced(self) -> bool:
        return len(self.via) > 1


class _Topology:
    """The lookups a trace needs: built once per resolve."""

    def __init__(self, edges, manual_nodes: dict, uses: list, hidden_node_ids) -> None:
        self.manual_nodes = manual_nodes
        self.uses = {u["use_id"]: u for u in uses}
        self.placements_of: dict[str, list[str]] = {}
        for use in uses:
            self.placements_of.setdefault(use["child_pipeline_id"], []).append(use["use_id"])
        self.ports: dict[tuple[str, str, str], str] = {}
        for node_id, meta in manual_nodes.items():
            if meta.get("type") in PORT_NODE_TYPES and node_id not in hidden_node_ids:
                key = (meta.get("pipeline_id"), meta["type"], meta.get("label"))
                self.ports[key] = node_id
        self.outgoing: dict[str, list[dict]] = {}
        for edge in edges:
            self.outgoing.setdefault(edge.get("source") or "", []).append(edge)
        self.dangling: list[dict] = []

    def type_of(self, node_id: str) -> "str | None":
        meta = self.manual_nodes.get(node_id) or self.manual_nodes.get(
            strip_placement(node_id)
        )
        return meta.get("type") if meta else None

    def is_junction(self, node_id: str) -> bool:
        return node_id in self.uses or self.type_of(node_id) in PORT_NODE_TYPES

    def is_removed_placement(self, node_id: str) -> bool:
        return (
            node_id.startswith(_USE_ID_PREFIX)
            and node_id not in self.uses
            and self.type_of(node_id) is None
        )

    def walk(self, edge: dict, via=()):
        """Follow *edge* to every ordinary node it reaches: ``(final edge, stored
        edge ids walked)``."""
        edge_id = edge.get("id")
        if edge_id in via:
            logger.warning("[ports] loop through ports at edge %s; stopped tracing", edge_id)
            return
        via = via + (edge_id,)
        target = edge.get("target") or ""
        use = self.uses.get(target)
        if use is not None:
            name = handle_name(edge.get("targetHandle"), IN_HANDLE_PREFIX)
            port = self.ports.get((use["child_pipeline_id"], INPUT_PORT_NODE, name))
            if port is None:
                self.dangling.append(edge)
                return
            for inner in self.outgoing.get(port, []):
                yield from self.walk(inner, via)
            return
        target_type = self.type_of(target)
        if target_type == OUTPUT_PORT_NODE:
            meta = self.manual_nodes.get(target) or {}
            for placement in self.placements_of.get(meta.get("pipeline_id"), []):
                for outer in self.outgoing.get(placement, []):
                    if handle_name(outer.get("sourceHandle"), OUT_HANDLE_PREFIX) == meta.get("label"):
                        yield from self.walk(outer, via)
            return
        if target_type == INPUT_PORT_NODE or self.is_removed_placement(target):
            self.dangling.append(edge)
            return
        yield edge, via


def _node_kind(node_id: str, topology: _Topology) -> "str | None":
    node_type = topology.type_of(node_id)
    bare = strip_placement(node_id)
    if node_type == "functionNode" or bare.startswith(FN_ID_PREFIX):
        return "function"
    if node_type == "variableNode" or bare.startswith(VAR_ID_PREFIX):
        return "variable"
    return None


def illegal_reason(source: str, target: str, topology: _Topology) -> "str | None":
    """Why a connection traced through ports could not exist as a direct edge,
    or ``None``. A function's output and a function's input need a variable
    between them, and two variables need a function."""
    kinds = (_node_kind(source, topology), _node_kind(target, topology))
    if kinds == ("function", "function"):
        return "two functions need a variable between them"
    if kinds == ("variable", "variable"):
        return "two variables need a function between them"
    return None


@dataclass
class Wiring:
    """Every resolved connection, plus what a reader needs to ask of a node."""

    wires: tuple[Wire, ...]
    #: ``{(bare node id, argument)}`` fed by a port with nothing connected.
    open_inputs: set[tuple[str, str]]
    #: Edges naming a port that doesn't exist.
    dangling: tuple[dict, ...]
    #: ``(source, target, reason, stored edge ids)`` for connections refused.
    illegal: tuple[tuple[str, str, str, tuple[str, ...]], ...]

    @property
    def edges(self) -> list[dict]:
        return [w.edge for w in self.wires]

    def open_arguments(self, node_id: str) -> list[str]:
        """Arguments of *node_id* fed by a port with nothing connected."""
        bare = strip_placement(node_id)
        return sorted(arg for node, arg in self.open_inputs if node == bare)


def resolve_wiring(
    edges,
    manual_nodes: dict,
    uses: list,
    hidden_node_ids: "set[str] | frozenset" = frozenset(),
) -> Wiring:
    """Trace *edges* through every port: see the module docstring."""
    from scistack_gui.domain.graph_builder import connection_id

    topology = _Topology(edges, manual_nodes, uses, hidden_node_ids)
    wires: dict[tuple, Wire] = {}
    illegal: list[tuple[str, str, str, tuple[str, ...]]] = []
    seen_illegal: set[tuple] = set()

    for edge in edges:
        source = edge.get("source") or ""
        if topology.is_junction(source):
            continue  # reached by a walk from its ordinary source
        if topology.is_removed_placement(source):
            topology.dangling.append(edge)
            continue
        for last, via in topology.walk(edge):
            target = last.get("target") or ""
            if len(via) == 1:
                resolved = edge
            else:
                reason = illegal_reason(source, target, topology)
                if reason is not None:
                    if (source, target) not in seen_illegal:
                        seen_illegal.add((source, target))
                        illegal.append((source, target, reason, via))
                    continue
                target_handle = last.get("targetHandle")
                resolved = {
                    "id": connection_id(source, target, target_handle, manual_nodes),
                    "source": source,
                    "target": target,
                }
                if edge.get("sourceHandle"):
                    resolved["sourceHandle"] = edge["sourceHandle"]
                if target_handle:
                    resolved["targetHandle"] = target_handle
            wires.setdefault(_edge_key(resolved), Wire(edge=resolved, via=via))

    arrived = {
        (strip_placement(w.edge.get("target") or ""), w.edge.get("targetHandle"))
        for w in wires.values()
    }
    open_inputs: set[tuple[str, str]] = set()
    for (_scope, node_type, _name), port in topology.ports.items():
        if node_type != INPUT_PORT_NODE:
            continue
        for inner in topology.outgoing.get(port, []):
            for last, _via in topology.walk(inner):
                target = strip_placement(last.get("target") or "")
                handle = last.get("targetHandle")
                argument = handle_name(handle, IN_HANDLE_PREFIX)
                if argument and (target, handle) not in arrived:
                    open_inputs.add((target, argument))

    return Wiring(
        wires=tuple(wires.values()),
        open_inputs=open_inputs,
        dangling=tuple(topology.dangling),
        illegal=tuple(illegal),
    )


def placement_conflict(edge: dict, edges, manual_nodes: dict, uses: list) -> "str | None":
    """What another placement already connects this port to, when *edge* would
    connect it to something else, else ``None``. A submodule has one wiring.

    Ends are compared by what they ARE (``graph_builder.endpoint_ref``), not by
    node id: a duplicated hypothesis holds its own node for the same variable.
    A placement with nothing connected yet conflicts with nothing.
    """
    from scistack_gui.domain.graph_builder import endpoint_ref

    use_by_id = {u["use_id"]: u for u in uses}

    def others(use_id: str) -> set[str]:
        child = use_by_id[use_id]["child_pipeline_id"]
        return {
            u["use_id"]
            for u in uses
            if u["child_pipeline_id"] == child and u["use_id"] != use_id
        }

    source, target = edge.get("source") or "", edge.get("target") or ""
    existing: list[str] = []
    mine: "str | None" = None
    if target in use_by_id:
        siblings = others(target)
        mine = endpoint_ref(source, manual_nodes)
        existing = [
            endpoint_ref(e.get("source") or "", manual_nodes)
            for e in edges
            if e.get("target") in siblings and e.get("targetHandle") == edge.get("targetHandle")
        ]
    elif source in use_by_id:
        siblings = others(source)
        mine = endpoint_ref(target, manual_nodes)
        existing = [
            endpoint_ref(e.get("target") or "", manual_nodes)
            for e in edges
            if e.get("source") in siblings and e.get("sourceHandle") == edge.get("sourceHandle")
        ]
    if existing and mine not in existing:
        return existing[0]
    return None


def port_fed_arguments(scope: str, edges, manual_nodes: dict, uses: list) -> set[tuple[str, str]]:
    """``{(function label, argument)}`` fed by an input port in *scope* or a
    submodule nested in it."""
    topology = _Topology(edges, manual_nodes, uses, frozenset())
    children: dict[str, list[str]] = {}
    for use in uses:
        children.setdefault(use["parent_pipeline_id"], []).append(use["child_pipeline_id"])
    closure: set[str] = set()
    stack = [scope]
    while stack:
        current = stack.pop()
        if current not in closure:
            closure.add(current)
            stack.extend(children.get(current, []))
    fed: set[tuple[str, str]] = set()
    for (port_scope, node_type, _name), port in topology.ports.items():
        if node_type != INPUT_PORT_NODE or port_scope not in closure:
            continue
        for inner in topology.outgoing.get(port, []):
            for last, _via in topology.walk(inner):
                argument = handle_name(last.get("targetHandle"), IN_HANDLE_PREFIX)
                target = last.get("target") or ""
                meta = manual_nodes.get(target) or manual_nodes.get(strip_placement(target)) or {}
                if argument and meta.get("label"):
                    fed.add((meta["label"], argument))
    return fed


def params_on_ports(params: "dict | None", fed: set[tuple[str, str]]) -> list[str]:
    """The ``binding.params`` keys naming an argument a port feeds. ``"fn.arg"``
    names one function's argument; a bare name, any function's."""
    conflicts = []
    for key in params or {}:
        if "." in key:
            fn, argument = key.rsplit(".", 1)
            hit = (fn, argument) in fed
        else:
            hit = any(argument == key for _fn, argument in fed)
        if hit:
            conflicts.append(key)
    return sorted(conflicts)


@dataclass
class PlannedPort:
    """One port an extraction creates, and how its crossing edges attach."""

    direction: str  # INPUT_PORT_NODE or OUTPUT_PORT_NODE
    name: str
    #: The crossing edges this port carries.
    edges: list[dict] = field(default_factory=list)


def plan_boundary(crossing: list[dict], is_moved, var_label_of, label_of) -> list[PlannedPort]:
    """The ports a selection needs: one per source crossing the boundary, in a
    stable order. A source feeding several nodes across the boundary is one
    port; two sources are two ports.

    *is_moved(node id)* says whether a node is in the selection.
    *var_label_of(node id)* is its variable type or ``None``; *label_of(node
    id)* its display name. A port is named after the variable crossing, else
    the node outside the selection for an input (a PathInput, a Parameter) or
    the source for an output.
    """
    grouped: dict[tuple, PlannedPort] = {}
    for edge in crossing:
        source, target = edge.get("source") or "", edge.get("target") or ""
        direction = OUTPUT_PORT_NODE if is_moved(source) else INPUT_PORT_NODE
        key = (direction, source, edge.get("sourceHandle"))
        planned = grouped.get(key)
        if planned is None:
            name = var_label_of(source) or var_label_of(target) or label_of(source)
            planned = grouped[key] = PlannedPort(direction, name)
        planned.edges.append(edge)

    ordered = sorted(grouped.values(), key=lambda p: (p.direction, p.name))
    taken: dict[str, set[str]] = {INPUT_PORT_NODE: set(), OUTPUT_PORT_NODE: set()}
    for planned in ordered:
        planned.name = unique_port_name(planned.name, taken[planned.direction])
        taken[planned.direction].add(planned.name)
    return ordered


def unique_port_name(name: str, taken: "set[str]") -> str:
    """*name*, suffixed if a port of that direction already has it."""
    if name not in taken:
        return name
    suffix = 2
    while f"{name}_{suffix}" in taken:
        suffix += 1
    return f"{name}_{suffix}"


def find_reentry(crossing: list[dict], edges: list[dict], is_moved) -> "list[str] | None":
    """A path leaving the selection and coming back into it, or ``None``.

    Selecting B and D but not C in ``B -> C -> D`` is such a path: the submodule
    would feed and read the same outside node (user decision 2026-11-03: refuse).
    """
    outgoing: dict[str, list[str]] = {}
    for edge in edges:
        outgoing.setdefault(edge.get("source") or "", []).append(edge.get("target") or "")
    for edge in crossing:
        start = edge.get("source") or ""
        if not is_moved(start):
            continue
        stack: list[list[str]] = [[start, edge.get("target") or ""]]
        seen: set[str] = set()
        while stack:
            path = stack.pop()
            node = path[-1]
            if is_moved(node):
                return path
            if node in seen:
                continue
            seen.add(node)
            stack.extend(path + [next_node] for next_node in outgoing.get(node, []))
    return None
