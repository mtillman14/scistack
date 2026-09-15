"""
Pure edge resolution logic for pipeline graphs.

Resolves input types, output types, and constant names from manual edges
and node metadata. No I/O — works entirely on plain Python data structures.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from scistack_gui.domain.graph_builder import (
    PARAM_ID_PREFIX,
    PATH_INPUT_ID_PREFIX,
    strip_placement,
)

logger = logging.getLogger(__name__)

# Binding kinds. A function parameter is fed by exactly one of these.
BINDING_VARIABLE = "variable"  # ref: [variable type names] (>1 ⇒ EachOf)
BINDING_PATHINPUT = "pathinput"  # ref: declared PathInput name
BINDING_PARAMETER = "parameter"  # ref: declared Parameter name

# Manual-node type for a glue node. A glue node is a *variant of a function
# node*, not a new kind of thing: same ``in__{param}`` / ``out__`` handles, so
# edge resolution below needs one extra source case and nothing else.
#
# Deliberately NOT a fourth binding kind. A glue node is transient — it has no
# saved output, so nothing can bind *to* it. What it does is interpose on the
# binding of whichever function parameter it feeds: the parameter still binds
# to whatever is at the HEAD of the chain, and the chain rides alongside in
# ``ResolvedEdges.glue_chains``. See docs/claude/free-code-glue-nodes.md §5.
#
# The head may be any binding kind, not only a variable. Restricting it to
# variables is what made ``Parameter -> glue -> fn`` drop the parameter
# entirely (2026-09-10): the head resolved to None, the binding was skipped,
# and a MATLAB run then bound the REMAINING arguments by position.
GLUE_NODE_TYPE = "glueNode"


def variable_binding(
    type_names: list[str],
    columns: "list[str] | None" = None,
    iterate: bool = False,
) -> dict:
    """A variable binding, optionally restricted to named columns.

    ``columns``/``iterate`` express the GUI's column selection
    (``MyVar["col"]`` / ``MyVar.for_columns([...])``) and are OMITTED entirely
    when there is no selection, so an ordinary whole-variable binding keeps
    exactly the two keys it always had. That matters beyond tidiness:
    ``resolve_function_edges._bind`` compares bindings with ``==`` to detect
    two edges fighting over one handle, and every consumer that reads the
    binding's shape (``bindings_of_kind``, ``variable_types_view``,
    ``variant_resolver.compute_call_id``) reads ``ref`` alone.

    See ``domain/column_selection.py`` for why the selection deliberately does
    NOT reach ``wiring_id``/``compute_call_id``.
    """
    binding = {"kind": BINDING_VARIABLE, "ref": type_names}
    if columns or iterate:
        binding["columns"] = list(columns or [])
        binding["iterate"] = bool(iterate)
    return binding


def pathinput_binding(declared_name: str) -> dict:
    return {"kind": BINDING_PATHINPUT, "ref": declared_name}


def parameter_binding(declared_name: str) -> dict:
    return {"kind": BINDING_PARAMETER, "ref": declared_name}


def bindings_of_kind(bindings: dict[str, dict], kind: str) -> dict:
    """``{param_name: ref}`` for one binding kind."""
    return {p: b["ref"] for p, b in (bindings or {}).items() if b["kind"] == kind}


def variable_types_view(bindings: dict[str, dict]) -> dict:
    """``{param_name: type}`` for variable bindings, in the shape the REST of
    the system uses: a bare string when one type is bound, a list only for a
    genuine multi-type (EachOf) input.

    That shape is load-bearing, not cosmetic. ``graph_builder.wiring_id``
    hashes the value as-is, so ``"RawEMG"`` and ``["RawEMG"]`` produce
    DIFFERENT wiring ids — and DB history (``list_pipeline_variants``) always
    spells it bare. A view that returned single-element lists would silently
    compute wiring ids that match nothing, breaking hidden-edge lookup.
    """
    out: dict = {}
    for param, ref in bindings_of_kind(bindings, BINDING_VARIABLE).items():
        if isinstance(ref, list) and len(ref) == 1:
            out[param] = ref[0]
        else:
            out[param] = ref
    return out


def loadable_bindings(bindings: dict[str, dict]) -> dict[str, dict]:
    """The bindings scidb serializes into ``__inputs`` — variables AND
    PathInputs, matching ``ForEachConfig._serialize_inputs``. Parameters are
    excluded: they expand into concrete values that travel in ``__constants``.
    """
    return {
        p: b
        for p, b in (bindings or {}).items()
        if b["kind"] in (BINDING_VARIABLE, BINDING_PATHINPUT)
    }


@dataclass
class ResolvedEdges:
    """Result of scanning edges for a function node.

    ``bindings`` is keyed by the FUNCTION PARAMETER the edge feeds, taken
    from the edge's ``targetHandle`` — never inferred from a node's name or
    from argument position. For PathInputs and Parameters the ``ref`` is the
    SOURCE-DECLARED name of the node feeding it, because the two can
    legitimately differ (a PathInput declared ``test_pi`` filling
    ``read_csv``'s ``filepath_or_buffer``). Callers that need the live object
    look it up in the registry by the declared name and bind it under the
    parameter name.

    **One dict, not three.** These used to be three parallel maps
    (``input_types`` / ``path_input_params`` / ``parameter_params``) and
    every consumer had to remember all of them. The ones that forgot were
    wrong: ``variant_resolver.compute_call_id`` hashed only ``input_types``,
    so its predicted call_id omitted the PathInput that scidb's
    ``ForEachConfig._serialize_inputs`` puts *into* ``__inputs`` — meaning a
    combo hidden before its first run landed on a different id than the
    record the run produced. ``wiring_id`` had the same blind spot, so two
    call sites fed by different PathInputs collapsed onto one canvas node.
    The three names survive below as read-only VIEWS over ``bindings`` for
    display and wire-format consumers; they are not separate state.
    """

    bindings: dict[str, dict]  # param_name → {"kind": ..., "ref": ...}
    output_types: list[str]  # ordered list of output variable labels
    # ``{param_name: [glue node name, ...]}`` in application order, for
    # parameters fed through one or more glue nodes. The parameter's own
    # binding names whatever sits at the HEAD of the chain — a variable, a
    # Parameter or a PathInput. Glue interposes on a binding, it is never a
    # binding of its own, and it is never a step.
    #
    # Which of those heads is RUNNABLE is scidb's call, not this module's:
    # ``scidb.glue`` applies a variable head at the Step-10/11 fusion point, a
    # Parameter head before fan-out, and refuses a PathInput head outright
    # (``GlueUnsupportedInputError``). Resolving the binding honestly here and
    # letting scidb refuse is what keeps an unsupported head a loud error
    # instead of a silently missing argument.
    glue_chains: dict[str, list[str]] = field(default_factory=dict)

    @property
    def input_types(self) -> dict[str, list[str]]:
        """View: ``{param: [variable type names]}`` (display / wire format)."""
        return bindings_of_kind(self.bindings, BINDING_VARIABLE)

    @property
    def path_input_params(self) -> dict[str, str]:
        """View: ``{param: declared PathInput name}``."""
        return bindings_of_kind(self.bindings, BINDING_PATHINPUT)

    @property
    def parameter_params(self) -> dict[str, str]:
        """View: ``{param: declared Parameter name}``."""
        return bindings_of_kind(self.bindings, BINDING_PARAMETER)


def node_id_to_var_label(
    node_id: str,
    existing_node_labels: dict[str, str],
    manual_nodes: dict[str, dict],
) -> str | None:
    """Resolve a node ID to its variable label, or None if not a variable node.

    Args:
        node_id: The node ID to resolve (e.g. "var__RawEMG" or a manual node UUID).
        existing_node_labels: {node_id: label} for all DB-derived nodes already built.
        manual_nodes: {node_id: {"type": ..., "label": ...}} from pipeline_store.
    """
    # DB-derived nodes use the convention "var__TypeName", optionally
    # placement-qualified as "var__TypeName::{pipeline_id}" — strip that
    # suffix before parsing so a placed node still resolves correctly.
    bare_id = strip_placement(node_id)
    if bare_id.startswith("var__"):
        # Check existing DB nodes first.
        if node_id in existing_node_labels:
            return existing_node_labels[node_id]
        # Fall back to extracting from the (bare) ID itself.
        parts = bare_id.split("__")
        if len(parts) >= 2:
            return parts[1]
    # Check the manual_nodes dict.
    meta = manual_nodes.get(node_id)
    if meta and meta.get("type") == "variableNode":
        return meta.get("label")
    return None


def resolve_source_binding(
    source_node_id: str,
    manual_nodes: dict[str, dict],
    existing_node_labels: dict[str, str],
) -> dict | None:
    """The binding a SOURCE node contributes, or ``None`` if it is not a
    bindable source (a function node, a glue node, an unknown id).

    This answers only "what kind of thing is feeding this edge, and what is its
    declared name" — never "which parameter does it fill", which is the
    ``targetHandle``'s job alone and stays with the caller.

    One function because there are two callers that must agree: an edge
    arriving directly at a function node, and the head of a glue chain. They
    disagreed until 2026-09-10 — the glue path recognised only variables — and
    the disagreement cost a silently-dropped ``Parameter`` argument.

    Resolution order matches the edge scan it was extracted from: PathInput
    prefix, then Parameter (by id prefix or by manual-node type), then
    variable. Order matters because ``node_id_to_var_label`` falls back to
    parsing the id, so it must not see a ``param__``/``pi__`` id first.
    """
    bare = strip_placement(source_node_id)

    if bare.startswith(PATH_INPUT_ID_PREFIX):
        return pathinput_binding(bare[len(PATH_INPUT_ID_PREFIX) :])

    # Parameter node (Constant and Sweep are one concept and one id prefix
    # since D6) — either DB-derived (``param__`` prefix) or a still-manual
    # node whose metadata says parameterNode.
    src_meta = manual_nodes.get(source_node_id)
    if bare.startswith(PARAM_ID_PREFIX):
        return parameter_binding(
            src_meta["label"] if src_meta else bare[len(PARAM_ID_PREFIX) :]
        )
    if src_meta and src_meta.get("type") == "parameterNode":
        return parameter_binding(src_meta["label"])

    var_label = node_id_to_var_label(
        source_node_id, existing_node_labels, manual_nodes
    )
    if var_label:
        return variable_binding([var_label])
    return None


def bare_fn_node_ids(fn_node_ids) -> set[str]:
    """A function's node-id set reduced to bare canonical ids.

    Every ``fn_node_ids`` membership test in this module goes through this.
    Callers assemble that set from mixed sources — ``graph_builder.fn_node_id``
    returns the BARE id (``fn__f__<call_id>``), while manual-node keys and edge
    endpoints may carry a ``::{scope}`` placement suffix — and an edge endpoint
    is rewritten from one form to the other by graduation
    (``pipeline_store.rename_edge_endpoints``).

    Comparing those forms with ``==`` is therefore a bug that only appears
    AFTER a function's first successful run. It cost us exactly one: the
    param→class map for a MATLAB fn came back empty once graduation had
    rewritten its output edge to the placement-qualified id, so the fn node
    rendered handle ``out__{param}`` while the edge pointed at
    ``out__{Class}`` — React Flow silently drops an edge whose sourceHandle
    does not exist, and the canvas showed the function disconnected from an
    output variable that was nonetheless green.

    See ``graph_builder.strip_placement``: "For every ad-hoc prefix-parser
    that only ever wants the bare id (never the scope), call this FIRST."
    """
    return {strip_placement(i) for i in fn_node_ids}


def is_glue_node(node_id: str, manual_nodes: dict[str, dict]) -> bool:
    """Whether ``node_id`` names a glue node."""
    meta = manual_nodes.get(node_id) or manual_nodes.get(strip_placement(node_id))
    return bool(meta) and meta.get("type") == GLUE_NODE_TYPE


def resolve_glue_chain(
    glue_node_id: str,
    manual_edges: list[dict],
    manual_nodes: dict[str, dict],
    existing_node_labels: dict[str, str],
    _seen: set[str] | None = None,
) -> tuple[list[str], dict | None]:
    """Walk back from a glue node to the thing it ultimately reshapes.

    Returns ``(chain_names, head_binding)`` — the glue node names in
    APPLICATION order (upstream first) and the binding of whatever sits at the
    head of the chain, or ``(chain, None)`` when the chain is not wired to a
    bindable source at all.

    The head is resolved by ``resolve_source_binding``, so a Parameter or a
    PathInput head resolves exactly as it would on a direct edge. This used to
    return a bare variable label and ``None`` for everything else, which made
    the caller skip the binding entirely — a ``Parameter -> glue -> fn`` chain
    lost its argument with only a warning to show for it.

    Glue chains: a glue node may be fed by another glue node, which is how a
    two-step reshape is expressed. The recursion is depth-guarded by
    ``_seen`` — a cycle on the canvas must not hang the graph build.

    Only the glue node's FIRST incoming edge is followed. A glue node may
    take N parameters (``scidb.glue.GlueSpec.extra_inputs``), but exactly one
    of them is the *piped* input whose provenance flows through; the GUI has
    no way to express the others yet, so extra incoming edges are reported
    rather than silently folded in.
    """
    if _seen is None:
        _seen = set()
    bare = strip_placement(glue_node_id)
    if bare in _seen:
        logger.warning(
            "[edge_resolver] glue node %s is part of a cycle — chain truncated",
            bare,
        )
        return [], None
    _seen.add(bare)

    meta = manual_nodes.get(glue_node_id) or manual_nodes.get(bare) or {}
    name = meta.get("label") or bare

    incoming = [
        e
        for e in manual_edges
        if strip_placement(e.get("target", "")) == bare
        and (e.get("targetHandle") or "").startswith("in__")
    ]
    if not incoming:
        logger.debug(
            "[edge_resolver] glue node %r has no incoming edge — nothing to reshape",
            name,
        )
        return [name], None
    if len(incoming) > 1:
        # Name the edges AND their handles. The common cause is a rename: the
        # glue's parameter was ``value``, an edge was drawn to ``in__value``,
        # the parameter was renamed and a second edge drawn to ``in__<new>``
        # — leaving the STALE edge first in the list and therefore the one
        # followed. Without the handles in the log that is invisible.
        logger.warning(
            "[edge_resolver] glue node %r has %d incoming edges %s; only the "
            "first is the piped input (extra glue inputs are not expressible "
            "on the canvas yet) — the others are ignored. If one names a "
            "parameter the glue no longer has, delete that edge",
            name,
            len(incoming),
            [
                f"{e.get('id')}:{e.get('targetHandle')}<-{e.get('source')}"
                for e in incoming
            ],
        )

    source = incoming[0].get("source", "")
    if is_glue_node(source, manual_nodes):
        upstream_chain, head = resolve_glue_chain(
            source, manual_edges, manual_nodes, existing_node_labels, _seen
        )
        return [*upstream_chain, name], head

    return [name], resolve_source_binding(source, manual_nodes, existing_node_labels)


def resolve_function_edges(
    fn_node_ids: set[str],
    manual_edges: list[dict],
    manual_nodes: dict[str, dict],
    existing_node_labels: dict[str, str],
) -> ResolvedEdges:
    """Resolve input/output/parameter connections for a function from edges.

    This is the single source of truth for edge inference, replacing the
    duplicated logic that was in api/pipeline.py, api/run.py, and server.py.

    **Every binding comes from an edge.** An incoming edge names the
    parameter it feeds in its ``targetHandle``; an edge that doesn't is
    dropped with a warning rather than guessed at. Two guesses used to live
    here and both produced silently-wrong wiring:

    * unhandled edges were matched to leftover signature params BY POSITION,
      so dropping a connection on a node with many params bound whichever
      param happened to be unfilled;
    * a Parameter edge with an unrecognized handle fell back to the source
      node's LABEL as the param name, which is only right when the declared
      name and the parameter name coincide.

    Args:
        fn_node_ids: Set of node IDs that represent this function
            (e.g. {"fn__my_func", "fn__my_func__abc123"}).
        manual_edges: List of edge dicts with id, source, target,
            sourceHandle, targetHandle.
        manual_nodes: {node_id: {"type": ..., "label": ...}} from pipeline_store.
        existing_node_labels: {node_id: label} for all DB-derived variable nodes.
    """
    bindings: dict[str, dict] = {}
    output_types: list[str] = []
    glue_chains: dict[str, list[str]] = {}
    dropped = 0

    def _bind(param: str, binding: dict, edge: dict) -> None:
        """Bind one parameter, warning if that rebinds it to a different
        source. One handle takes one edge, so this means two edges are
        fighting over the same parameter — silently keeping the last one is
        how wiring bugs hide."""
        existing = bindings.get(param)
        if existing is not None and existing != binding:
            logger.warning(
                "[edge_resolver] parameter %r rebound by edge %s: %s %r "
                "replaces %s %r — the canvas has two edges on one handle",
                param,
                edge.get("id"),
                binding["kind"],
                binding["ref"],
                existing["kind"],
                existing["ref"],
            )
        bindings[param] = binding

    def _drop(edge: dict, why: str) -> None:
        nonlocal dropped
        dropped += 1
        logger.warning(
            "[edge_resolver] ignoring edge %s (%s -> %s, handle=%r): %s — "
            "reconnect it to the parameter's handle on the function node",
            edge.get("id"),
            edge.get("source"),
            edge.get("target"),
            edge.get("targetHandle"),
            why,
        )

    fn_ids = bare_fn_node_ids(fn_node_ids)

    for edge in manual_edges:
        source = edge.get("source", "")
        target = edge.get("target", "")
        bare_source = strip_placement(source)

        if bare_source in fn_ids:
            # Edge from this function → a variable node (output).
            var_label = node_id_to_var_label(target, existing_node_labels, manual_nodes)
            if var_label and var_label not in output_types:
                output_types.append(var_label)

        elif strip_placement(target) in fn_ids:
            # Edge into this function (variable input, PathInput, Parameter).
            th = edge.get("targetHandle") or ""

            # Glue → fn. The parameter binds to whatever is at the HEAD of the
            # chain — variable, Parameter or PathInput — and the glue chain
            # rides alongside so the run can fuse it in memory. This is the
            # only new case a glue node adds to edge resolution — it reuses
            # the same in__/out__ handles a function node has.
            if is_glue_node(source, manual_nodes):
                if not th.startswith("in__"):
                    _drop(edge, "glue edge carries no 'in__<param>' handle")
                    continue
                param = th[len("in__") :]
                chain, head = resolve_glue_chain(
                    source, manual_edges, manual_nodes, existing_node_labels
                )
                if chain:
                    glue_chains[param] = chain
                if head is not None:
                    _bind(param, head, edge)
                    # §6's diagnostic trail assumed the head was always a
                    # variable. State the kind: it decides WHERE scidb applies
                    # the chain (fusion point / before fan-out / refused).
                    logger.info(
                        "[edge_resolver] parameter %r is fed through glue [%s] "
                        "from %s %r",
                        param,
                        " > ".join(chain),
                        head["kind"],
                        head["ref"],
                    )
                else:
                    logger.warning(
                        "[edge_resolver] parameter %r is fed by glue [%s], but "
                        "the chain is not wired to anything — connect a "
                        "variable, Parameter or PathInput to the first glue "
                        "node's input handle",
                        param,
                        " > ".join(chain) or "<unnamed>",
                    )
                continue

            # What kind of source is this, and what is its declared name? One
            # decision, shared with the glue-chain head above.
            src_binding = resolve_source_binding(
                source, manual_nodes, existing_node_labels
            )
            src_kind = src_binding["kind"] if src_binding else None

            # PathInput → fn. The declared name and the parameter it fills
            # routinely differ, so ONLY the handle can say which param this
            # is; build_edges encodes both names in the DB-derived edge id
            # for the same reason (see graph_builder.candidate_edge_id).
            if src_kind == BINDING_PATHINPUT:
                if th.startswith("in__"):
                    _bind(th[len("in__") :], src_binding, edge)
                else:
                    _drop(edge, "PathInput edge carries no 'in__<param>' handle")
                continue

            if src_kind == BINDING_PARAMETER:
                if th.startswith(PARAM_ID_PREFIX):
                    # DB-derived Parameter edge: build_edges writes the
                    # constant's own name into BOTH the node id and the
                    # handle, so here the parameter name and the declared
                    # name are the same string by construction.
                    _bind(th[len(PARAM_ID_PREFIX) :], src_binding, edge)
                elif th.startswith("in__"):
                    _bind(th[len("in__") :], src_binding, edge)
                else:
                    _drop(edge, "Parameter edge carries no parameter handle")
                continue

            # Variable → fn.
            if src_kind == BINDING_VARIABLE:
                if th.startswith("in__"):
                    param = th[len("in__") :]
                    var_label = src_binding["ref"][0]
                    # Several variable edges onto one handle is EachOf, not a
                    # conflict — accumulate instead of going through _bind.
                    existing = bindings.get(param)
                    if existing is not None and existing["kind"] == BINDING_VARIABLE:
                        if var_label not in existing["ref"]:
                            existing["ref"].append(var_label)
                    else:
                        _bind(param, src_binding, edge)
                else:
                    _drop(edge, "variable edge carries no 'in__<param>' handle")

    logger.debug(
        "resolve_function_edges: bindings=%s outputs=%s glue=%s dropped=%d",
        bindings,
        output_types,
        glue_chains,
        dropped,
    )
    return ResolvedEdges(
        bindings=bindings, output_types=output_types, glue_chains=glue_chains
    )


def infer_manual_fn_output_types(
    fn_node_ids: set[str],
    manual_edges: list[dict],
    manual_nodes: dict[str, dict],
    existing_node_labels: dict[str, str],
) -> list[str]:
    """Infer output types from manual edges for a function (no positional matching needed).

    Used by the run path when DB variants exist but the user has rewired outputs.
    """
    fn_ids = bare_fn_node_ids(fn_node_ids)
    output_types: list[str] = []
    for edge in manual_edges:
        if strip_placement(edge.get("source", "")) in fn_ids:
            var_label = node_id_to_var_label(
                edge.get("target", ""), existing_node_labels, manual_nodes
            )
            if var_label and var_label not in output_types:
                output_types.append(var_label)
    return output_types


def infer_manual_fn_param_to_class(
    fn_node_ids: set[str],
    manual_edges: list[dict],
    manual_nodes: dict[str, dict],
    existing_node_labels: dict[str, str],
) -> dict[str, str]:
    """Extract {param_name: class_name} for a fn from its outgoing manual edges.

    Each manual edge from a function node carries the MATLAB signature param
    name in ``sourceHandle`` (``out__{param_name}``) and the downstream
    Variable class via the edge target. This gives an explicit mapping that
    does not rely on naming conventions — e.g. ``output1 → Result`` works.

    Edges without an ``out__`` prefix or without a resolvable target class
    are skipped. On duplicate param names the first wins.

    Endpoint matching is placement-insensitive — see :func:`bare_fn_node_ids`,
    which documents the canvas bug an exact match caused here.
    """
    fn_ids = bare_fn_node_ids(fn_node_ids)
    mapping: dict[str, str] = {}
    for edge in manual_edges:
        if strip_placement(edge.get("source", "")) not in fn_ids:
            continue
        sh = edge.get("sourceHandle") or ""
        if not sh.startswith("out__"):
            continue
        param = sh[len("out__") :]
        if not param or param in mapping:
            continue
        class_name = node_id_to_var_label(
            edge.get("target", ""), existing_node_labels, manual_nodes
        )
        if class_name:
            mapping[param] = class_name
    return mapping
