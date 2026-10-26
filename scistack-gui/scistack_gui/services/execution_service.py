"""
Execution service — the document→backend pipeline compiler (G2).

Two jobs:

1. **Target derivation** (`derive_fn_targets`): what for_each call(s) does a
   function node represent? DB history (pipeline variants) first, manual
   edges + pending constants as the never-run fallback — extracted from the
   run thread so per-node runs and pipeline runs derive identically.
2. **Compilation** (`build_backend_pipeline`): turn a GUI pipeline scope
   (document nodes + use edges) into an in-session ``scidb.Pipeline`` —
   each function node's targets register via ``for_each(...,
   pipeline=pipe)``; use rows become ``parent.use(child.bind(**binding))``.
   Pipelines are built fresh per request (in-session objects; the document
   is the persistent form — spec persistence stays deliberately unbuilt).

``plan_pipeline`` is the plan-preview data source (R2): compile, then
``pipe.plan(target)`` — nothing executes.
"""

from __future__ import annotations

import ast
import json
import logging
from itertools import product
from pathlib import Path

from scidb.foreach_config import RunOptions
from scidb.provenance import constants_identity_key

from scistack_gui.ids import (
    FN_ID_PREFIX,
    ROOT_SCOPE,
    fn_nodes_prefix,
    handle_name,
)
from scistack_gui.services.run_failures import on_error_from_config

logger = logging.getLogger(__name__)


def _infer_wired_constants(
    parameter_params: dict[str, str],
    pending: dict[str, set[str]],
    constants_registry: dict,
    *,
    fn_variants: "list[dict] | None" = None,
    stated: "dict[str, set[str]] | None" = None,
    log_context: str,
) -> dict[str, list]:
    """Infer values for a never-run wiring's Parameter params, in priority
    order: staged pending values; then, if *fn_variants* is given, any
    known value from OTHER real DB history call sites of the same function;
    then the Parameter's STATED values (*stated*, keyed by declared name:
    ``pipeline_store.get_stated_constants``)
    (a constant's known values are a function-level property, e.g. a
    shared window_seconds constant already has a real value from a
    different call site — see ``derive_target_for_node``'s docstring for
    the full rationale); then the Parameter's source-declared value(s). A
    parameter with none of the above is dropped from the returned dict
    (logged at WARNING) — including one that IS declared but has no values
    yet, which must be dropped rather than contributing an empty list, since
    ``_inferred_targets`` products these together and one empty list yields
    zero targets.

    *parameter_params* is ``{param_name: declared_name}`` straight from the
    wiring (``ResolvedEdges.parameter_params``, the Parameter view over its
    ``bindings``). The distinction is
    load-bearing and the two names are NOT interchangeable:

    * ``pending`` and *constants_registry* are keyed by the **declared**
      name — the pending table is written by the Parameter node's own UI,
      and the registry by the source declaration;
    * DB history (``fn_variants[i]["constants"]``) and the returned dict are
      keyed by the **parameter** name, because that is what scidb records
      and what ``for_each`` is ultimately called with.

    Looking the registry up by parameter name is what made a Parameter
    declared ``test`` and wired to ``sep`` report "wired but has no
    source-declared default value" and get silently dropped.

    Shared by ``derive_fn_targets`` (name-scoped, ``fn_variants=None`` so
    the middle tier is skipped) and ``derive_target_for_node`` (node-scoped,
    passes ``fn_variants`` to enable it) so this fallback logic can't drift
    between the two — it used to be copy-pasted, and the source-declared-
    default tier was added to only one copy before this consolidation.
    """
    no_values_phrase = (
        "no pending or known values" if fn_variants is not None else "no pending values"
    )
    no_default_phrase = (
        "no pending, known, or source-declared default value"
        if fn_variants is not None
        else "no pending values and no source-declared default"
    )

    inferred: dict[str, list] = {}
    for param_name, decl_name in parameter_params.items():
        typed_vals = []
        for raw in pending.get(decl_name, set()):
            try:
                typed_vals.append(ast.literal_eval(raw))
            except (ValueError, SyntaxError):
                typed_vals.append(raw)
        if not typed_vals and fn_variants is not None:
            # Keyed by repr, not a set of values: a constant may be a dict
            # (docs/claude/constants-are-not-scalars.md), which is unhashable.
            known_vals = {
                repr(v["constants"][param_name]): v["constants"][param_name]
                for v in fn_variants
                if param_name in v.get("constants", {})
            }
            if known_vals:
                typed_vals = [known_vals[k] for k in sorted(known_vals)]
                logger.debug(
                    "[execution] %s: parameter '%s' has no pending value — "
                    "reusing known value(s) %s from other call site(s) of "
                    "this function",
                    log_context,
                    param_name,
                    typed_vals,
                )
        if not typed_vals and stated and stated.get(decl_name):
            # The Parameter's STATED values (canvas-as-document): what ran or
            # was adopted, kept on the canvas after its history is gone. This
            # is what a run after a clear-data uses, so it computes the same
            # combinations it did before the clear.
            for raw in sorted(stated[decl_name]):
                try:
                    typed_vals.append(ast.literal_eval(raw))
                except (ValueError, SyntaxError):
                    typed_vals.append(raw)
            logger.info(
                "[execution] %s: parameter '%s' (declared '%s') has no pending or "
                "recorded value; using its stated value(s) %r",
                log_context,
                param_name,
                decl_name,
                typed_vals,
            )
        if typed_vals:
            inferred[param_name] = typed_vals
        # `.values` and not the Parameter itself: `bool(Parameter())` raises
        # (there is no single value to be truthy about), so testing the
        # object here would take the run down instead of falling through.
        elif decl_name in constants_registry and constants_registry[decl_name].values:
            # EVERY declared value, not just the first: a Parameter with
            # several values is a fan-out, and silently taking one would
            # turn a multi-combo run into a single one.
            declared = list(constants_registry[decl_name].values)
            inferred[param_name] = declared
            logger.info(
                "[execution] %s: parameter '%s' (declared '%s') has %s; using "
                "source-declared value(s) %r",
                log_context,
                param_name,
                decl_name,
                no_values_phrase,
                declared,
            )
        elif decl_name in constants_registry:
            # Declared, but with no values yet. It must NOT enter `inferred`:
            # `_inferred_targets` takes the Cartesian product of these lists,
            # so a single empty one yields ZERO targets and the node reports
            # "nothing derivable" — the wrong diagnosis, and a silent one.
            # Left out, the target is still built and the run reaches
            # `build_run_inputs`, which raises naming the parameter.
            logger.warning(
                "[execution] %s: parameter '%s' (declared '%s') is wired but "
                "has no value yet — give it at least one value",
                log_context,
                param_name,
                decl_name,
            )
        else:
            logger.warning(
                "[execution] %s: parameter '%s' (declared '%s') wired but has %s",
                log_context,
                param_name,
                decl_name,
                no_default_phrase,
            )
    return inferred


def _db_path_input_params(db, function_name: str) -> dict[str, dict[str, str]]:
    """``{call_id: {param_name: declared PathInput name}}`` for *function_name*
    from real DB history.

    A PathInput is never a citizen of ``input_types``/``constants`` (it
    resolves *files*, not a versioned record), so a target derived from DB
    history carries no trace of one. The mapping does exist, though, in
    ``get_aggregated_variants()["path_inputs"]`` — keyed by PARAM name, with
    a ``functions`` list of the call sites that used it — and
    ``convert_scidb_path_inputs`` already resolves each recorded spec back to
    its source-declared name (via the registry, falling back to the D7
    name↔value history). This inverts that into per-call-site bindings.

    This is what lets the run path be wiring-only with no name-matching
    fallback: a source-declared pipeline that has already run has no MANUAL
    edge on the canvas — its PathInput edges are synthesised from exactly
    this data by ``graph_builder.build_edges`` — so without this it would
    have nothing to resolve from.
    """
    from scistack_gui import pipeline_store, registry
    from scistack_gui.domain.graph_builder import convert_scidb_path_inputs

    path_inputs = convert_scidb_path_inputs(
        db.get_aggregated_variants()["path_inputs"],
        registry.get_path_inputs_registry(),
        pipeline_store.path_input_history_index(db),
        registry.get_project_root(),
        path_input_renames=pipeline_store.path_input_rename_index(db),
    )
    by_call: dict[str, dict[str, str]] = {}
    for pi_name, pi in path_inputs.items():
        for fkey, param_name in pi["functions"]:
            fn, call_id = fkey
            if fn == function_name:
                by_call.setdefault(call_id, {})[param_name] = pi_name
    return by_call


def history_variant_wirings(
    db, function_name: str, variants: list[dict], pi_by_call: "dict | None" = None
) -> list[tuple[dict, str]]:
    """``[(variant, wiring_id), ...]`` — the wiring each DB-history variant of
    *function_name* belongs to, as ``scidb.provenance.split_call_site_outputs``
    decides it from the call site's recorded outputs and the wirings Runs have
    claimed. THE run path's answer; the canvas build asks the same owner
    (``graph_builder.split_call_sites_by_claims``), so a node's wiring and its
    history's wirings can no longer be hashed two ways.

    A variant carries ONE output type, but its call site may hold several.
    Hashing the variant's own output alone (what every run-path site did until
    2026-09-29) disagreed with the canvas whenever the call site had >1 output
    — after an output rewire the node's wiring matched none of its own history
    ("matches none of the 2 candidate variant(s)"). A variant appears once per
    wiring whose outputs include it (twice only where two claims overlap).
    """
    from scidb.provenance import split_call_site_outputs

    from scistack_gui import node_wiring as _node_wiring_store

    if not variants:
        return []
    if pi_by_call is None:
        pi_by_call = _db_path_input_params(db, function_name)
    claimed = _node_wiring_store.claimed_wirings(db)
    by_call: dict[str, list[dict]] = {}
    for i, v in enumerate(variants):
        # A variant with no call id is its own call site, never pooled with
        # another one's outputs.
        by_call.setdefault(v.get("call_id") or f"\0{i}", []).append(v)
    out: list[tuple[dict, str]] = []
    for cid, group in by_call.items():
        splits = split_call_site_outputs(
            function_name,
            group[0].get("input_types") or {},
            {v.get("output_type") for v in group},
            pi_by_call.get(cid, {}),
            claimed,
        )
        for v in group:
            for wid, subset in splits:
                if v.get("output_type") in subset:
                    out.append((v, wid))
    return out


def _attach_db_path_inputs(db, function_name: str, targets: list[dict]) -> list[dict]:
    """Give each DB-history target its unified ``bindings``, so every target
    reaching ``build_run_inputs`` has the same shape regardless of whether it
    came from history or from inference.

    A history target arrives with ``input_types`` holding its recorded inputs
    — INCLUDING a PathInput-fed parameter, whose raw spec sits there next to
    the real variable inputs. The declared-name mapping it needs lives in the
    aggregated variants instead, which ``_db_path_input_params`` inverts. Both
    become bindings here (the PathInput loop runs second and overwrites the
    variable binding this made for that param), and this is the ONLY place a
    target's bindings are assembled from history.

    Note what this does NOT do: it leaves ``input_types`` alone. Callers that
    hash an input shape must therefore not treat it as the partitioned view
    the canvas uses — ``graph_builder.wiring_id`` normalises this itself, and
    that discrepancy is exactly what once made a graduated PathInput-fed node
    unrunnable.

    Parameter bindings name the DECLARED Parameter each constant's run
    recorded (``scidb.parameter.parameter_node_name``), so a history target
    translates between declared name and argument exactly as an edge-derived
    one does (``edge_resolver.declared_by_argument``, cleanup-audit F20).
    They never look anything up: ``build_run_inputs`` skips a Parameter
    binding whose argument already holds a recorded value.
    """
    if not targets:
        return targets
    from scidb.parameter import parameter_node_name

    from scistack_gui.domain.edge_resolver import (
        parameter_binding,
        pathinput_binding,
        variable_binding,
    )

    by_call = _db_path_input_params(db, function_name)
    for t in targets:
        bindings: dict[str, dict] = {}
        selectors = t.get("selectors") or {}
        for param, type_val in (t.get("input_types") or {}).items():
            # The column selection the recorded call used
            # (`pipeline_variants[].selectors`) is the binding's DEFAULT; a
            # selection saved on the node (`_attach_column_selections`, run
            # later) still overrides it. Without this a step authored in
            # Python as `Var["knee"]` re-ran from the canvas with the whole
            # table and failed in the function (integration suite,
            # 2026-09-19).
            sel = selectors.get(param) or {}
            pooled = param in (t.get("across_variants") or [])
            bindings[param] = variable_binding(
                list(type_val) if isinstance(type_val, (list, tuple, set)) else [type_val],
                columns=list(sel.get("columns") or []) if sel else None,
                iterate=bool(sel.get("iterate", False)),
                pool_variants=pooled,
            )
            if pooled:
                logger.info(
                    "[execution] '%s': '%s' keeps its recorded AcrossVariants pooling",
                    function_name, param,
                )
            if sel:
                logger.info(
                    "[execution] '%s': '%s' keeps its recorded column selection %s%s",
                    function_name, param, sel.get("columns") or "every column",
                    " (per column)" if sel.get("iterate") else "",
                )
        for param, decl_name in by_call.get(t.get("call_id"), {}).items():
            bindings[param] = pathinput_binding(decl_name)
        recorded = t.get("parameter_names") or {}
        for arg in t.get("constants") or {}:
            bindings[arg] = parameter_binding(parameter_node_name(arg, recorded))
        t.setdefault("bindings", bindings)
    return targets


def column_selections_for_nodes(
    db, node_ids, function_name: str = ""
) -> dict[str, dict]:
    """``{param: {"columns": [...], "iterate": bool}}`` saved for any of
    *node_ids*, merged.

    Matching is PLACEMENT-INSENSITIVE. A config is stored under whichever id
    the canvas showed when it was saved, and that is the placement-qualified
    ``fn__{fn}__{cid}::{scope}`` form whenever the node has a qualified
    placement — while the id sets assembled by the derivation paths are a mix
    of bare canonical ids, manual-node uuids and edge endpoints. Comparing
    those with ``==`` is the same trap ``edge_resolver.bare_fn_node_ids``
    documents, and here it would silently run with the whole variable.

    Passing *function_name* additionally admits any ``fn__{function_name}__*``
    config id, whether or not it is in *node_ids*. That is the NAME-scoped
    reading, and the only one that works for a source-declared pipeline: such
    a function has no manual edge rows at all once it has run, so the id set
    assembled from edges and manual nodes does not contain the canvas id the
    panel actually saved under. Node-scoped callers omit it, so one call
    site's selection can never leak onto another's run.

    A parameter selected differently on two node ids of the same function is a
    genuine conflict (two call sites of one name, configured apart). The first
    wins and the loser is WARNed rather than merged, because a union of column
    sets is not a thing the user asked for on either node.
    """
    from scistack_gui import pipeline_store
    from scistack_gui.domain import column_selection as _cs
    from scistack_gui.ids import parse_fn_node_id, strip_placement

    wanted = {strip_placement(i) for i in (node_ids or set())}
    if not wanted and not function_name:
        return {}

    def _matches(nid: str) -> bool:
        bare = strip_placement(nid)
        if bare in wanted:
            return True
        if not function_name:
            return False
        parsed = parse_fn_node_id(bare)
        if parsed is not None:
            return parsed[0] == function_name
        # `parse_fn_node_id` answers only for a CANONICAL id — it requires a
        # 16-hex call_id and returns None for every other suffix. A config is
        # saved under whatever id the canvas showed, and a node that has never
        # run carries a 6-char manual suffix (`fn__{fn}__a1b2c3`), so the
        # name-scoped reading this function documents has to admit those too;
        # going through the canonical parser alone silently ran the whole
        # variable for exactly the case the panel was used for.
        #
        # One trailing segment only: `fn__{fn}__{suffix}` with no further
        # `__`, so a function named `load` can never claim a config saved for
        # `load__raw`.
        prefix = fn_nodes_prefix(function_name)
        return bare.startswith(prefix) and "__" not in bare[len(prefix) :]

    merged: dict[str, dict] = {}
    source: dict[str, str] = {}
    for nid, config in sorted(pipeline_store.get_node_configs(db).items()):
        if not _matches(nid):
            continue
        for param, raw in ((config or {}).get("columnSelections") or {}).items():
            sel = _cs.normalize(raw)
            if sel is None:
                continue
            previous = merged.get(param)
            if previous is not None and previous != sel:
                logger.warning(
                    "[execution] parameter %r has conflicting column "
                    "selections: %s (node %s) vs %s (node %s) — keeping the "
                    "first. Configure the two call sites' Inputs sections to "
                    "agree, or run them as separate nodes.",
                    param,
                    _cs.describe(previous),
                    source.get(param),
                    _cs.describe(sel),
                    nid,
                )
                continue
            merged[param] = sel
            source[param] = nid

    if not merged:
        # The silent case, made loud: a selection saved on a node that no id
        # in this derivation matches looks exactly like no selection at all,
        # and the run then loads whole tables with nothing in the log to say
        # why. Naming both sides turns that into a one-line diagnosis.
        unmatched = sorted(
            nid
            for nid, config in pipeline_store.get_node_configs(db).items()
            if (config or {}).get("columnSelections")
        )
        if unmatched:
            logger.info(
                "[execution] '%s': no saved column selection applies — "
                "selections exist on node(s) %s, and this derivation asked "
                "for %s%s. If the run loads whole tables and the panel shows "
                "a selection, these two id sets are why.",
                function_name or "?",
                unmatched,
                sorted(wanted) or "(no node ids)",
                f" plus any fn__{function_name}__* id" if function_name else "",
            )
    return merged


def _attach_column_selections(
    db,
    node_ids,
    targets: list[dict],
    function_name: str = "",
    name_scoped: bool = False,
) -> list[dict]:
    """Stamp the GUI's saved column selections onto every target's bindings.

    Modelled directly on :func:`_attach_db_path_inputs`. Since 2026-09-19
    history DOES carry a call's selection (``_invocation_input.selector``,
    surfaced as ``pipeline_variants[].selectors`` and stamped on the bindings
    as their default in :func:`_attach_db_path_inputs`); what is saved on the
    node is the user's OVERRIDE of it, applied here on top.

    Called from BOTH derivation paths, because either can be the one a run
    bottoms out in: ``derive_fn_targets`` (``name_scoped=True`` — any node id
    of this function counts) and ``derive_target_for_node`` (node-scoped —
    only the node clicked, so two call sites of one name stay independent).
    """
    if not targets:
        return targets
    from scistack_gui.domain import column_selection as _cs

    selections = column_selections_for_nodes(
        db, node_ids, function_name if name_scoped else ""
    )
    if not selections:
        return targets

    context = f"'{function_name}'" if function_name else ""
    for t in targets:
        t["bindings"] = _cs.apply_to_bindings(
            t.get("bindings"), selections, context=context
        )

    # One line per parameter, not per target: its ABSENCE is the diagnostic
    # when a selection shows in the panel and the run loads whole tables, and
    # N identical lines per derivation make that harder to see, not easier.
    for param, sel in sorted(selections.items()):
        logger.info(
            "[execution] '%s': '%s' restricted to %s (%s) on %d target(s)",
            function_name or "?",
            param,
            _cs.describe(sel),
            sel["columns"] or "all data columns",
            len(targets),
        )
    return targets


def _attach_colname_inputs(
    db, node_id: str, targets: list[dict], function_name: str = ""
) -> list[dict]:
    """Bind the node's stated ColName parameters on every target.

    Runs AFTER :func:`_attach_column_selections`, because the iterate input
    a ColName needs may come from the node's own column selection. Reads the
    ``colNameInputs`` the node's canvas resolves (``get_node_config``: the
    node's scope over ``global``) — node-scoped, so two call sites of one
    function name keep their own. History never records ColName (scidb keeps
    it out of version keys), so this statement is the only source and is
    re-applied on every run. Rules: ``domain/colname_inputs.apply_to_target``.
    """
    if not targets:
        return targets
    from scistack_gui import pipeline_store
    from scistack_gui.domain import colname_inputs as _cn
    from scistack_gui.domain.edge_resolver import BINDING_COLNAME

    params = _cn.normalize(
        (pipeline_store.get_node_config(db, node_id) or {}).get("colNameInputs")
    )
    if not params:
        return targets
    context = f"'{function_name or '?'}' (node {node_id})"
    out = [_cn.apply_to_target(t, params, context=context) for t in targets]
    # One line per derivation: its ABSENCE is the diagnostic when the panel
    # shows ColName and the function receives nothing for that argument.
    logger.info(
        "[execution] %s: ColName stated for %s; bound on %d of %d target(s)",
        context,
        params,
        sum(
            1
            for t in out
            if any(
                (t.get("bindings") or {}).get(p, {}).get("kind") == BINDING_COLNAME
                for p in params
            )
        ),
        len(out),
    )
    return out


def _hidden_constant_values(db, pipeline_id: "str | None" = None) -> dict[str, set[str]]:
    """{const_name: {hidden values}} from the ``ConstantNode.tsx`` checkbox
    state — grouped once per derivation call so ``filter_hidden_
    constant_value_targets`` (a pure content-match, no call_id hashing) can
    check every target in one pass. A run from a canvas passes that canvas
    (``derive_target_for_node`` reads it off the node id); only the
    name-scoped fallback (``derive_fn_targets``, no node) still unions every
    scope's hides."""
    from scistack_gui import pipeline_store

    hidden: dict[str, set[str]] = {}
    for row in pipeline_store.list_hidden_parameter_values(db, pipeline_id):
        hidden.setdefault(row["const_name"], set()).add(row["value"])
    return hidden


def _inferred_targets(resolved, inferred_constants: dict[str, list]) -> list[dict]:
    """Targets for a never-run wiring: the Cartesian product of its inferred
    Parameter values × its output types, each carrying the wiring's
    edge-derived bindings so ``build_run_inputs`` can resolve them.

    Shared by both derivation paths, which had byte-identical copies of this
    product (``feedback_avoid_scifor_scidb_duplication``).
    """
    # ``input_types`` rides along as the display/wire view of the variable
    # bindings (run metadata, node params); ``bindings`` is the source of
    # truth every identity and execution path reads.
    #
    # ``glue_chains`` is a property of the target's INPUT BINDING, never a
    # step of its own — a glue node has no run button and no run state, and
    # ``build_backend_pipeline`` must never emit a StepSpec for one (D5).
    base = {
        "bindings": resolved.bindings,
        "input_types": resolved.input_types,
        "glue_chains": dict(resolved.glue_chains),
    }
    if not inferred_constants:
        return [
            {**base, "output_type": out, "constants": {}}
            for out in resolved.output_types
        ]
    names = sorted(inferred_constants)
    return [
        {**base, "output_type": out, "constants": dict(zip(names, combo, strict=False))}
        for combo in product(*(inferred_constants[n] for n in names))
        for out in resolved.output_types
    ]


def _document_function_nodes(db, function_name: str) -> list[str]:
    """Stored function-node rows labelled *function_name*, every scope, that
    are not hidden in their own scope. The name-scoped run's node set."""
    from scistack_gui import pipeline_store

    manual_nodes = pipeline_store.get_manual_nodes(db)
    hidden_by_scope: dict[str, set[str]] = {}
    out = []
    for nid, meta in sorted(manual_nodes.items()):
        if meta.get("type") != "functionNode" or meta.get("label") != function_name:
            continue
        scope = meta.get("pipeline_id") or ROOT_SCOPE
        if scope not in hidden_by_scope:
            hidden_by_scope[scope] = pipeline_store.get_hidden_node_ids(db, scope)
        if nid in hidden_by_scope[scope]:
            continue
        out.append(nid)
    return out


def ensure_document(db, scope: "str | None" = None, *, reason: str) -> None:
    """Build *scope*'s graph (without run states) so canvas adoption has stored
    whatever history holds that the document does not yet.

    For a run path reached before any canvas was built since the last script
    run (a test, the CLI, a run request from a stale tab): it would otherwise
    find history the document does not hold and run nothing. The build is the
    ONE place adoption happens, so this asks it rather than adopting here.
    """
    from scistack_gui.api.pipeline import _build_graph

    scope = scope or ROOT_SCOPE
    logger.info("[execution] building %s's canvas so adoption is current (%s)", scope, reason)
    _build_graph(db, scope, run_states=False)


def _target_key(t: dict) -> str:
    return json.dumps(
        [
            t.get("output_type"),
            constants_identity_key(t.get("constants")),
            t.get("bindings") or {},
            t.get("glue_chains") or {},
        ],
        sort_keys=True,
        default=str,
    )


def derive_fn_targets(db, function_name: str) -> list[dict]:
    """The for_each target(s) of every stored function node labelled
    *function_name*: the NAME-scoped run (no node named).

    Since 2026-10-22 (plan-canvas-as-document Stage 4) this is exactly the
    union of :func:`derive_target_for_node` over those nodes, so the
    name-scoped and node-scoped runs can no longer disagree. The name-scoped
    path used to run history's recorded wirings with the drawn edges spliced in
    (``reconcile_manual_inputs``, deleted) and fall back to inferring from
    hand-placed nodes' edges, a second derivation of what a node runs.

    Returns ``[]`` when no stored node has that label (after making adoption
    current: :func:`ensure_document`). Duplicate targets (two nodes stating
    the same wiring) are kept once.
    """
    node_ids = _document_function_nodes(db, function_name)
    if not node_ids:
        ensure_document(db, ROOT_SCOPE, reason=f"name-scoped run of '{function_name}'")
        node_ids = _document_function_nodes(db, function_name)
    if not node_ids:
        logger.warning(
            "[execution] '%s': no function node with that label is on any canvas "
            "— no targets derivable",
            function_name,
        )
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for nid in node_ids:
        for t in derive_target_for_node(db, nid):
            key = _target_key(t)
            if key in seen:
                continue
            seen.add(key)
            out.append(t)
    logger.info(
        "[execution] '%s' (name-scoped): %d target(s) from %d stored node(s) %s",
        function_name,
        len(out),
        len(node_ids),
        node_ids,
    )
    return out


def document_disconnected_ports(
    node_id: str, hidden_edge_ids, resolved, manual_nodes: "dict | None" = None
) -> list[str]:
    """Input ports of *node_id* whose edge is HIDDEN with no visible stored
    edge covering the port: the node is disconnected and must not run.

    The document form of ``graph_builder.hidden_wirings``. Every edge is a
    stored row now, and removing one hides its connection
    (``layout_service.delete_edge``), so the hidden connection ids name the
    node's ports directly (``graph_builder.connection_id``: ``e__{src}__{tgt}
    __{port}``). A port fed by another visible edge is covered.
    """
    from scistack_gui.domain.graph_builder import endpoint_ref

    ref = endpoint_ref(node_id, manual_nodes)
    marker = f"__{ref}__"
    covered = set(resolved.bindings) if resolved is not None else set()
    ports: set[str] = set()
    for cid in hidden_edge_ids or ():
        if not cid.startswith("e__") or marker not in cid:
            continue
        port = cid.rsplit("__", 1)[-1]
        if port and port not in covered:
            ports.add(port)
    return sorted(ports)


def derive_target_for_node(db, node_id: str) -> list[dict]:
    """The for_each target(s) that ONE stored function node represents.

    **From the document** (plan-canvas-as-document Stage 4, user decision D1
    2026-10-22). The node's STORED edges decide what runs:

    1. its stated wiring is computed from its stored edges, nothing else;
    2. the recorded rows of exactly that wiring are run, so a re-run of an
       unchanged step reuses the exact typed constants, column selectors and
       AcrossVariants pooling history recorded (no superfluous re-runs: a
       recorded ``6.0`` is never re-typed from text);
    3. with no recorded row of that wiring (never run, rewired, or the data
       was cleared), targets are built from the edges and the Parameters'
       values (pending, then known values of the function, then declared,
       then stated).

    A node with a hidden input and no visible edge on that port is
    disconnected and runs nothing (:func:`document_disconnected_ports`).

    What is gone: running a node's last RECORDED wiring (``_node_wiring``'s
    current shape) with the drawn edges spliced in. Under adoption every node
    on a canvas is a stored row with stored edges, so the recorded wiring is
    something to compare against, not something to execute.

    Returns ``[]`` for anything that is not a stored function node (after one
    attempt to make adoption current), or when nothing is derivable. Every
    returned target has already been filtered against hidden constant values.
    """
    from scistack_gui import pipeline_store
    from scistack_gui.domain.edge_resolver import resolve_function_edges
    from scistack_gui.domain.graph_builder import wiring_id
    from scistack_gui.domain.variant_resolver import filter_hidden_constant_value_targets
    from scistack_gui.ids import strip_placement

    # The canvas this node is on decides which hides apply — a value
    # unchecked in one hypothesis must not exclude it from another.
    from scistack_gui import intent_store

    hidden_values = _hidden_constant_values(db, intent_store.scope_of_node(db, node_id))
    # The clicked node's canvas decides which hidden edges apply (decision a,
    # plan-unified-edge-model): an edge hidden in another hypothesis tab does
    # not disconnect this node.
    from scistack_gui.domain.edge_view import effective_edges, run_scope

    def _view():
        return effective_edges(
            db, run_scope(db, node_id), caller=f"derive_target_for_node({node_id})"
        )

    view = _view()
    bare = str(strip_placement(node_id))
    meta = view.manual_nodes.get(node_id) or view.manual_nodes.get(bare)
    if meta is None and bare.startswith(FN_ID_PREFIX):
        # A function node history knows but the document does not hold yet:
        # a run reached before any build adopted it.
        ensure_document(db, run_scope(db, node_id), reason=f"run of {node_id}")
        view = _view()
        meta = view.manual_nodes.get(node_id) or view.manual_nodes.get(bare)
    if meta is None:
        logger.warning(
            "[execution] node %s: no targets — it is not a stored node on any "
            "canvas (refresh the canvas; a node from a run is stored when a "
            "build adopts it)",
            node_id,
        )
        return []
    if meta["type"] != "functionNode":
        logger.info(
            "[execution] node %s: no targets — it is a %s, not a function node",
            node_id,
            meta["type"],
        )
        return []
    function_name = meta["label"]
    manual_nodes = view.manual_nodes
    all_edges = view.drawn_list

    resolved = resolve_function_edges(
        fn_node_ids={node_id},
        manual_edges=all_edges,
        manual_nodes=manual_nodes,
        existing_node_labels={},
    )
    disconnected = document_disconnected_ports(
        node_id, view.hidden_edge_ids, resolved, manual_nodes
    )
    if disconnected:
        logger.info(
            "[execution] node %s ('%s'): no targets — disconnected: input port(s) "
            "%s have a hidden edge and no visible edge in its place",
            node_id,
            function_name,
            disconnected,
        )
        return []
    if not resolved.output_types:
        logger.info(
            "[execution] node %s ('%s'): no targets — no output edge drawn",
            node_id,
            function_name,
        )
        return []
    # The variable_types_view shape — the same one record_dispatch_wirings
    # hashes, and the one history records.
    stated_inputs = {p: t for p, t in resolved.input_types.items() if t}
    stated_wiring = wiring_id(
        function_name,
        stated_inputs,
        set(resolved.output_types),
        resolved.path_input_params,
    )

    all_variants = db.list_pipeline_variants()
    # Bind first, so every filter below sees the target's real wiring rather
    # than an empty one.
    fn_variants = _attach_db_path_inputs(
        db,
        function_name,
        [v for v in all_variants if v["function_name"] == function_name],
    )
    # Each variant's wiring from the ONE owner the canvas uses (the call
    # site's outputs split by Run claims).
    pi_by_call = _db_path_input_params(db, function_name)
    candidate_wirings = history_variant_wirings(db, function_name, fn_variants, pi_by_call)
    matching = [v for v, wid in candidate_wirings if wid == stated_wiring]
    logger.info(
        "[execution] node %s ('%s'): stated wiring %s (inputs=%s, outputs=%s, "
        "path inputs=%s) — %d recorded row(s) of it among %d of this function%s",
        node_id,
        function_name,
        stated_wiring,
        stated_inputs,
        sorted(resolved.output_types),
        resolved.path_input_params,
        len(matching),
        len(candidate_wirings),
        "" if matching else "; first run of this wiring: it runs from its edges and values",
    )
    if matching:
        # A recorded row has the stated wiring's types by construction (equal
        # wiring ids); what it adds is the exact recorded detail. Glue chains
        # come from the stored edges when the row did not record one.
        for t in matching:
            if resolved.glue_chains and not t.get("glue_chains"):
                t["glue_chains"] = dict(resolved.glue_chains)
        return _attach_colname_inputs(
            db,
            node_id,
            _attach_column_selections(
                db,
                {node_id},
                filter_hidden_constant_value_targets(matching, hidden_values),
                function_name,
            ),
            function_name,
        )

    # No recorded row of this wiring: build from the edges and the values.
    inferred_constants: dict[str, list] = {}
    if resolved.parameter_params:
        from scistack_gui import registry

        inferred_constants = _infer_wired_constants(
            resolved.parameter_params,
            pipeline_store.get_pending_constants(db),
            registry.get_parameters_registry(),
            fn_variants=fn_variants,
            stated=pipeline_store.get_stated_constants(db),
            log_context=f"'{function_name}' (node {node_id})",
        )

    inferred = _apply_node_pooling(
        _inferred_targets(resolved, inferred_constants),
        RunOptions.from_config(
            (pipeline_store.get_node_config(db, node_id) or {}).get("runOptions")
        ).across_variants,
        f"node {node_id} ('{function_name}')",
    )
    return _attach_colname_inputs(
        db,
        node_id,
        _attach_column_selections(
            db,
            {node_id},
            filter_hidden_constant_value_targets(inferred, hidden_values),
            function_name,
        ),
        function_name,
    )


def _apply_node_pooling(targets: list[dict], pooled, log_context: str) -> list[dict]:
    """Mark the node's ``across_variants`` inputs as pooled on targets built
    from its edges. A recorded row already carries its pooling on the binding
    (``_attach_db_path_inputs``); a target built from edges (never run, or
    after a clear-data) gets it from the node's run options, which adoption
    copies from the recorded runs (``services.adoption.recorded_node_settings``).
    """
    pooled = set(pooled or ())
    if not pooled or not targets:
        return targets
    from scistack_gui.domain.edge_resolver import BINDING_VARIABLE

    applied: set[str] = set()
    out = []
    for t in targets:
        bindings = dict(t.get("bindings") or {})
        for param in pooled:
            b = bindings.get(param)
            if b and b.get("kind") == BINDING_VARIABLE and not b.get("pool_variants"):
                bindings[param] = {**b, "pool_variants": True}
                applied.add(param)
        out.append({**t, "bindings": bindings})
    missing = sorted(pooled - {p for t in out for p in (t.get("bindings") or {})})
    logger.info(
        "[execution] %s: inputs %s pool every variant (AcrossVariants, from the "
        "node's run options)%s",
        log_context,
        sorted(applied) or sorted(pooled),
        f"; not an input of this node: {missing}" if missing else "",
    )
    return out


def record_dispatch_wirings(
    db, node_id: "str | None", function_name: str, targets: list[dict], run_id: "str | None"
) -> int:
    """Record, at dispatch, which wiring(s) the node being run will produce.

    **The first half of D-2026-09-22-2.** For a GUI-started run there is
    nothing to infer: the GUI already holds the node id — it is in the run
    request — so the association is written now, and the graph build that
    happens after the run finds the new wiring already claimed. No duplicate
    node can form, no statement is stranded, and no repair path runs.

    Inference (``domain.node_identity`` rule 2) exists only for what this
    cannot cover: a script or a terminal MATLAB run, which carries no node id.

    Computed from the targets AFTER reconciliation, so it is the wiring the
    run will actually record — history's bindings with the edges the user drew
    substituted in — rather than the one history currently holds.

    **It raises.** An unrecorded dispatch means the next build cannot tell
    which node produced these records, and the answer it reaches instead is a
    guess — about which node is green, which one a click runs, and which one
    your settings apply to. Letting the run proceed anyway would write records
    nothing can attribute; refusing is the honest outcome.
    """
    if not node_id or not targets:
        return 0
    from scistack_gui import intent_store
    from scistack_gui import node_wiring as _node_wiring_store
    from scistack_gui.domain.edge_resolver import (
        BINDING_PATHINPUT,
        bindings_of_kind,
        path_input_arm_bindings,
        variable_types_view,
    )
    from scistack_gui.domain.graph_builder import wiring_id
    from scistack_gui.ids import FN_ID_PREFIX, strip_placement

    # A function node, DB-derived or manual. A manual node's id is already its
    # own allocated id (it got one when the user dragged it in), so it claims
    # its wiring here exactly as a graduated node does — and
    # `graduate_manual_node` carries the row across when it graduates.
    # Anything else (a variable, a Parameter, a glue node) has no wiring.
    if not str(strip_placement(node_id)).startswith(FN_ID_PREFIX):
        return 0

    scope = intent_store.scope_of_node(db, node_id)
    # ONE claim per input shape, covering EVERY output this Run writes for it:
    # the claim is what `scidb.provenance.split_call_site_outputs` later splits
    # the call site's recorded outputs by, so a node run with outputs {A, B}
    # must claim hash(A, B) — two per-output claims would split it into two
    # nodes. Constants are not in a wiring, so constant variants of one shape
    # share the claim (as before). For a one-output node this is the same hash
    # as the per-target claim it replaced.
    outputs_by_shape: dict[str, tuple[dict, dict, set]] = {}
    for target in targets:
        inputs = variable_types_view(target.get("bindings") or {})
        pis = bindings_of_kind(target.get("bindings") or {}, BINDING_PATHINPUT)
        shape = json.dumps([inputs, pis], sort_keys=True, default=str)
        entry = outputs_by_shape.setdefault(shape, (inputs, pis, set()))
        if target.get("output_type"):
            entry[2].add(target.get("output_type"))
    wirings = {}
    for inputs, pis, outputs in outputs_by_shape.values():
        # An EachOf of PathInputs records one wiring PER ARM (each arm
        # records its own name), so claim each — a list-valued shape is a
        # wiring no run ever records, and an unclaimed arm forks a node.
        for arm in path_input_arm_bindings(pis):
            wirings[wiring_id(function_name, inputs, outputs, arm)] = sorted(outputs)
    written = 0
    for wiring in sorted(wirings):
        if _node_wiring_store.record(db, node_id, wiring, run_id=run_id, scope=scope):
            written += 1
    logger.info(
        "[execution] run %s on node %s ('%s') claims %d wiring(s) %s "
        "(%d new) — recorded at dispatch so the next graph build attributes "
        "the run to THIS node",
        run_id,
        node_id,
        function_name,
        len(wirings),
        {w: outs for w, outs in sorted(wirings.items())},
        written,
    )
    return written


def disconnected_reason(db, function_name: str, node_id: "str | None" = None) -> "str | None":
    """Human-readable reason *function_name* (or one specific node's own
    wiring, if ``node_id`` is given) can't run right now because a
    required input edge is hidden — None if it's runnable, INCLUDING the
    case where it simply has no DB history yet (a different, unrelated
    situation the caller already messages separately).

    Cheap, independent of ``derive_fn_targets``/``derive_target_for_node``
    (which already silently exclude disconnected targets) — this exists so
    callers that get an empty target list can tell "disconnected" apart
    from "never run" and surface the right explicit error (see api/run.py).
    """
    from scistack_gui import pipeline_store
    from scistack_gui.domain.graph_builder import (
        inbound_edge_candidates_by_handle,
        manual_edge_handle_index,
    )
    from scistack_gui.ids import parse_fn_node_id

    from scistack_gui.domain.edge_view import effective_edges, run_scope

    # Same scope rule as the run it explains (derive_target_for_node).
    view = effective_edges(
        db,
        run_scope(db, node_id),
        caller=f"disconnected_reason({function_name}, {node_id})",
    )
    hidden_edge_ids = set(view.hidden_edge_ids)
    if not hidden_edge_ids:
        return None

    # The document rule first: the one derive_target_for_node applies
    # (document_disconnected_ports), on the node named or on every stored
    # node of this function.
    from scistack_gui.domain.edge_resolver import resolve_function_edges

    for nid in [node_id] if node_id else _document_function_nodes(db, function_name):
        resolved = resolve_function_edges(
            fn_node_ids={nid},
            manual_edges=view.drawn_list,
            manual_nodes=view.manual_nodes,
            existing_node_labels={},
        )
        ports = document_disconnected_ports(nid, hidden_edge_ids, resolved, view.manual_nodes)
        if ports:
            return (
                f"input '{ports[0]}' is disconnected — reconnect it before running"
            )

    manual_index = manual_edge_handle_index(view.drawn_list)

    # Hidden edge ids and drawn edges are keyed by the NODE, which no longer
    # encodes its wiring (docs/claude/node-identity.md) — so every lookup below
    # needs the node's TOKEN, not the variant's wiring.
    #
    # Node-scoped: the node is named, so its token is its id's suffix and it is
    # asked about its CURRENT wiring only — the same pair
    # `derive_target_for_node` runs with, so "why can't this run" is answered
    # about the shape that would actually run.
    #
    # Name-scoped: no node is named, so each variant's wiring is mapped to
    # whichever node holds it. Without that the candidate edge ids are built
    # under the wiring, match no stored hide, and the caller reports "no
    # pipeline history" for a function that is in fact disconnected.
    from scistack_gui import node_wiring as _node_wiring_store

    node_wiring_now: "str | None" = None
    node_token = None
    token_for = _node_wiring_store.token_resolver(db)
    if node_id:
        parsed = parse_fn_node_id(node_id)
        if parsed is not None:
            node_token = parsed[1]
            node_wiring_now = _node_wiring_store.current_wiring(db, node_id)

    all_variants = db.list_pipeline_variants()
    pi_by_call = _db_path_input_params(db, function_name)
    # The same owner derive_target_for_node matches with (the call site's
    # outputs split by Run claims), so "why can't this run" is asked about
    # exactly the wirings a Run would consider.
    for v, wid in history_variant_wirings(
        db,
        function_name,
        [v for v in all_variants if v["function_name"] == function_name],
        pi_by_call,
    ):
        if node_wiring_now is not None and wid != node_wiring_now:
            continue
        key = node_token or token_for(function_name, wid)
        # The one owner of a wiring's inbound connection ids (variables first,
        # then Parameters, named by their DECLARED name).
        candidates = inbound_edge_candidates_by_handle(
            function_name,
            key,
            v.get("input_types") or {},
            const_names=list((v.get("constants") or {}).keys()),
            parameter_names=v.get("parameter_names"),
        )
        for candidate, handle in candidates.items():
            if candidate in hidden_edge_ids and (function_name, key, handle) not in manual_index:
                return (
                    f"input '{handle_name(handle)}' is disconnected — reconnect it "
                    "before running"
                )
    return None


def disconnected_report_entries(db, pipeline_id: str) -> list[dict]:
    """Synthetic report entries (compatible in shape with scidb's
    Pipeline.last_run_report) for functions in *pipeline_id*'s scope that
    won't actually run because a required input is disconnected (direct)
    or because an upstream producer is (cascaded) — computed independently
    of the compiled scidb.Pipeline and merged into the response by
    run_pipeline/plan_pipeline, never mutating scidb's own report object
    (this "disconnected" concept is GUI-authored state, scistack-gui's own
    layer — see plan-edge-hide-delete.md).
    """
    from scistack_gui import pipeline_store
    from scistack_gui.api.pipeline import build_aggregate, ensure_node_identities
    from scistack_gui.domain.graph_builder import (
        hidden_wirings,
        inbound_edge_candidates_by_handle,
        wirings_downstream_of,
    )

    from scistack_gui.domain.edge_view import effective_edges

    # A pipeline run reports on ITS canvas: hides from that scope only.
    view = effective_edges(
        db, pipeline_id, caller=f"disconnected_report_entries({pipeline_id})"
    )
    hidden_edge_ids = set(view.hidden_edge_ids)
    if not hidden_edge_ids:
        return []

    # The SAME aggregate `_build_graph` uses. A narrower hand-rolled copy sat
    # here and omitted nothing important by accident — the PathInput
    # resolution was already duplicated line for line — but two copies of a
    # conversion is two chances to drift, and the hidden-edge lookups below
    # only match if this side keys PathInputs exactly as `build_edges` does.
    agg = build_aggregate(db, db.get_aggregated_variants())
    fn_input_params = agg.fn_input_params
    fn_outputs = agg.fn_outputs
    fn_constants = agg.fn_constants
    path_inputs = agg.path_inputs

    manual_edges = view.drawn_list
    manual_nodes = view.manual_nodes
    # Hidden edge ids were stored against the NODE's id, so both sides of
    # every lookup below have to be keyed by the node's token rather than by
    # the wiring — they differ for a node that was rewired and run.
    identity = ensure_node_identities(db)
    token_for = identity.token
    seed = hidden_wirings(
        fn_input_params, fn_outputs, fn_constants, path_inputs, hidden_edge_ids,
        token_for,
        manual_edges=manual_edges,
        manual_nodes=manual_nodes,
        is_current=identity.is_current,
        fn_parameter_names=agg.fn_parameter_names,
    )
    if not seed:
        return []
    downstream = wirings_downstream_of(
        fn_input_params, fn_outputs, seed, path_inputs, token_for
    )

    scope_labels = set(_scope_function_labels(db, pipeline_id, identity))

    def _entry(fn: str, reason: str) -> dict:
        return {
            "step": fn,
            "label": fn,
            "pipeline": pipeline_id,
            "completed": 0,
            "failed": 0,
            "no_data": 0,
            "total": 0,
            "cancelled": False,
            "skipped": True,
            "skip_reason": reason,
        }

    from scistack_gui.domain.graph_builder import manual_edge_handle_index

    manual_index = manual_edge_handle_index(manual_edges)

    entries: list[dict] = []
    seen_labels: set[str] = set()
    for fn, wid in sorted(seed):
        if fn not in scope_labels or fn in seen_labels:
            continue
        seen_labels.add(fn)
        reason = "required input disconnected"
        for fkey, params in fn_input_params.items():
            if fkey[0] != fn:
                continue
            candidates = inbound_edge_candidates_by_handle(
                fn,
                wid,
                params,
                const_names=fn_constants.get(fkey, set()),
                parameter_names=agg.fn_parameter_names.get(fkey),
            )
            for candidate, handle in candidates.items():
                if candidate in hidden_edge_ids and (fn, wid, handle) not in manual_index:
                    reason = f"input '{handle_name(handle)}' disconnected"
                    break
        entries.append(_entry(fn, reason))
    for fn, _wid in sorted(downstream):
        if fn not in scope_labels or fn in seen_labels:
            continue
        seen_labels.add(fn)
        entries.append(_entry(fn, "upstream input unavailable"))

    if entries:
        logger.info(
            "[execution] scope %s: %d function(s) skipped (disconnected/cascaded)",
            pipeline_id,
            len(entries),
        )
    return entries


def resolve_combo_call_ids(
    db, function_name: str, node_id: str | None, variant_key: dict
) -> list[str]:
    """Turn one Variants-table row (its constant axes) into the call_id(s)
    to hide/unhide — usually one, occasionally more if multiple output
    types share the same constants. Entries with no computable call_id
    (an unresolved multi-type input) are silently skipped — fail safe, see
    ``variant_resolver.compute_call_id``.
    """
    from scistack_gui import pipeline_store
    from scistack_gui.domain.variant_resolver import constants_match, resolve_target_call_id

    targets = (
        derive_target_for_node(db, node_id)
        if node_id
        else derive_fn_targets(db, function_name)
    )
    pending_consts = pipeline_store.get_pending_constants(db)
    targets = apply_pending_overrides(targets, pending_consts)
    matches = [t for t in targets if constants_match(t["constants"], variant_key)]

    # get_node_config reads _node_config (and falls back to the legacy
    # _pipeline_nodes column). The manual-nodes lookup this replaced returned
    # {} for every node that had ever run, so distribute/as_table -- both
    # identity-bearing -- silently defaulted here and the call_id computed
    # below was the one for a call the user had not asked for.
    node_config = pipeline_store.get_node_config(db, node_id) if node_id else {}
    run_opts = node_config.get("runOptions") or {}
    pending_names = set(pending_consts)

    cids: list[str] = []
    for t in matches:
        cid = resolve_target_call_id(
            function_name, t, pending_names, RunOptions.from_config(run_opts)
        )
        if cid is not None:
            cids.append(cid)
    return cids


def apply_pending_overrides(targets: list[dict], pending_constants: dict) -> list[dict]:
    """Staged pending values override DB history on every derived target
    that uses the constant — the SHARED seam for eager per-node runs and
    compiled pipeline runs, so both materialize staged values identically
    (Strategy 2: first staged value wins, replacing the DB value; string
    values are literal_eval'd so ``"10"`` runs as ``10``).

    Overriding can collapse targets that differed only in the overridden
    constant into duplicates — callers should re-deduplicate after.
    Returns a new list; input targets are not mutated.
    """
    if not pending_constants:
        return targets
    from scistack_gui.domain.edge_resolver import by_argument

    out = []
    for target in targets:
        constants = dict(target["constants"])
        overridden = []
        # Pending values are keyed by the DECLARED Parameter; constants by
        # the argument (cleanup-audit F20) — translated, never compared.
        for const_name, values in by_argument(pending_constants, target).items():
            if values:
                raw = next(iter(values))
                try:
                    typed = ast.literal_eval(raw)
                except (ValueError, SyntaxError):
                    typed = raw
                constants[const_name] = typed
                overridden.append(const_name)
        if overridden:
            logger.info(
                "[execution] pending override on %s target: %s",
                target.get("output_type"),
                {k: constants[k] for k in overridden},
            )
            out.append({**target, "constants": constants})
        else:
            out.append(target)
    return out


def _hidden_values_for_run(db) -> dict:
    """Hidden Parameter values for execution, or {} when no database is
    reachable. Best-effort by design: a missing db means "cannot know what
    is unchecked", and running the full declared set is the safe reading --
    never silently running FEWER combos than declared."""
    if db is None:
        try:
            from scistack_gui.db import get_db

            db = get_db()
        except Exception as e:
            logger.warning(
                "[execution] no database while resolving hidden Parameter "
                "values (%s) -- running every declared value",
                e,
            )
            return {}
    if db is None:
        return {}
    return _hidden_constant_values(db)


def _apply_hidden_values(param, name: str, function_name: str, hidden: dict):
    """*param* with its unchecked values removed.

    *name* is the Parameter's DECLARED name — what the node checkbox writes
    into the hidden-value store — not the signature parameter it feeds.

    Matching is ``variant_resolver.is_hidden_value``'s, shared with
    ``filter_hidden_constant_value_targets``: the store holds rendered
    strings while a Parameter holds real ints/floats/strs, and the two
    routes a Parameter can take to execution must apply the same rule.

    Every value unchecked is a contradiction -- the user has excluded the
    whole fan-out yet left the parameter wired -- so it raises rather than
    running the full set or an arbitrary one. Both silent alternatives
    produce records the user explicitly asked not to produce.
    """
    from scidb import Parameter

    from scistack_gui.domain.variant_resolver import is_hidden_value

    hidden_for_name = hidden.get(name, set())
    if not hidden_for_name:
        return param

    kept = [v for v in param.values if not is_hidden_value(v, hidden_for_name)]
    if not kept:
        raise ValueError(
            f"every value of parameter '{name}' is unchecked, so "
            f"'{function_name}' has nothing to run -- re-check at least one "
            f"value on its node."
        )
    if len(kept) == len(param.values):
        return param

    logger.info(
        "[execution] '%s': parameter '%s' running %d of %d declared "
        "value(s) -- %s excluded by unchecked boxes",
        function_name,
        name,
        len(kept),
        len(param.values),
        sorted(hidden_for_name),
    )
    return Parameter(*kept, description=param.description)


def _apply_column_selection(var_cls, sel: "dict | None"):
    """*var_cls*, or a ``scidb.ColumnSelection`` around it when *sel* asks for
    one.

    The two spellings are not interchangeable and the difference is what the
    function receives: ``MyVar[cols]`` passes the column(s) as ONE argument
    (a numpy array for one column, a DataFrame subset for several), while
    ``MyVar.for_columns(cols)`` runs the function once per column and
    reassembles the results into a one-row table with the source column
    names. See docs/claude/for-columns-iteration.md.

    ``for_columns([])`` is legal and means "all data columns, resolved at
    for_each time"; ``MyVar[[]]`` is not a thing the UI can produce, because
    :func:`column_selection.normalize` drops an empty non-iterate selection.
    """
    if sel is None:
        return var_cls
    columns = list(sel.get("columns") or [])
    if sel.get("iterate"):
        return var_cls.for_columns(columns)
    return var_cls[columns]


def _apply_pooling(spec, binding: dict):
    """*spec*, wrapped in ``scidb.AcrossVariants`` when the binding says the
    input pools every variant group into one call (a history-derived target
    whose run recorded ``across_variants``)."""
    if not binding.get("pool_variants"):
        return spec
    from scidb import AcrossVariants

    return AcrossVariants(spec)


def build_run_inputs(target: dict, function_name: str, db=None) -> dict:
    """The for_each ``inputs=`` dict for a derived target: variable-class
    inputs, scalar constants, and any remaining signature params resolved
    via a stored PathInput or Parameter.

    Shared by the per-node Run path (``api/run.py``) and the compiled-
    pipeline path (``build_backend_pipeline`` below) — one place instead of
    two independently-drifting copies, which is what let PathInput
    resolution go missing from both for a long time.

    Every input comes from the target's ``bindings`` — one dict, keyed by
    function parameter, each entry tagged ``variable`` / ``pathinput`` /
    ``parameter``, produced by the WIRING (an edge's ``targetHandle`` for a
    never-run node, ``_attach_db_path_inputs`` for one with history). For the
    latter two kinds the ``ref`` is a DECLARED name resolved against the
    registry here; this is the single place a live ``scifor.PathInput`` object
    or a fanned-out ``Parameter`` is ever constructed for execution.

    Scalar ``constants`` stay a separate field, mirroring scidb's own split
    between ``__inputs`` and ``__constants``.

    **The binding is the edge, never the name.** This function used to
    resolve by elimination — whatever signature params were left unfilled
    got looked up by name in the PathInput/Parameter registries, which are
    keyed by DECLARED name — so a PathInput declared ``test_pi`` feeding
    ``read_csv``'s ``filepath_or_buffer`` matched nothing and the function
    silently ran with ``inputs={}``, iterating zero times and writing no
    records while reporting success. A declared name and the parameter it
    fills are simply different things (``graph_builder.build_edges`` has
    encoded both in its PathInput edge ids all along); the registry lookup
    below is therefore by declared name, and the result is bound under the
    parameter name.

    Hidden (unchecked) Parameter values are filtered out here (D6). A
    SCALAR constant is already excluded upstream --
    ``filter_hidden_constant_value_targets`` drops the whole target -- but a
    MULTI-VALUED Parameter is handed to ``for_each`` whole, which then fans
    it out INSIDE scidb, where the GUI's hidden-value state is not visible.
    Without filtering here, unchecking one value of a multi-value Parameter
    looked right in the UI and still ran.
    """
    from scidb import ColName, EachOf
    from scistack_gui import registry
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui.domain import column_selection as _column_selection
    from scistack_gui.domain.edge_resolver import (
        BINDING_COLNAME,
        BINDING_PARAMETER,
        BINDING_PATHINPUT,
        BINDING_VARIABLE,
        path_input_names,
    )

    # The registry already holds live PathInput/Parameter objects — no
    # reconstruction needed (unlike the old layout.json-backed dicts, which
    # stored plain template/values data that had to be rebuilt here).
    path_inputs_by_name = registry.get_path_inputs_registry()
    params_by_name = registry.get_parameters_registry()
    hidden_values = _hidden_values_for_run(db)

    inputs: dict = {}
    # Constants first: a Parameter binding whose value was already recorded as
    # a concrete constant must not be re-expanded into the whole sweep.
    inputs.update(target["constants"])

    for param, binding in (target.get("bindings") or {}).items():
        kind = binding["kind"]
        ref = binding["ref"]

        if kind == BINDING_VARIABLE:
            type_names = ref if isinstance(ref, list) else [ref]
            sel = _column_selection.from_binding(binding)
            if len(type_names) > 1:
                # Each alternative is wrapped separately: EachOf documents
                # ColumnSelection as a legal alternative, and one selection
                # per PARAMETER (not per edge) is the stated v1 scope — a
                # column present in one type and absent in the other fails at
                # load with scifor's KeyError naming the available columns.
                inputs[param] = EachOf(
                    *(
                        _apply_pooling(
                            _apply_column_selection(
                                registry.get_variable_class(t), sel
                            ),
                            binding,
                        )
                        for t in type_names
                    )
                )
            elif type_names:
                inputs[param] = _apply_pooling(
                    _apply_column_selection(
                        registry.get_variable_class(type_names[0]), sel
                    ),
                    binding,
                )
            if sel is not None and type_names:
                logger.info(
                    "[execution] '%s': input '%s' (%s) restricted to %s",
                    function_name,
                    param,
                    ", ".join(type_names),
                    _column_selection.describe(sel),
                )

        elif kind == BINDING_PATHINPUT:
            arms = []
            for name in path_input_names(ref):
                pi = path_inputs_by_name.get(name)
                if pi is None:
                    logger.warning(
                        "[execution] '%s': parameter '%s' is wired to PathInput "
                        "'%s', which is no longer declared in source — leaving "
                        "it out",
                        function_name,
                        param,
                        name,
                    )
                    continue
                # A declaration with alternate templates is itself an EachOf;
                # its arms join the others on one axis.
                arms.extend(pi.alternatives if isinstance(pi, EachOf) else [pi])
            if not arms:
                continue
            inputs[param] = arms[0] if len(arms) == 1 else EachOf(*arms)
            logger.info(
                "[execution] '%s': input '%s' resolved via PathInput %s (%d "
                "template(s))",
                function_name,
                param,
                path_input_names(ref),
                len(arms),
            )

        elif kind == BINDING_PARAMETER:
            if param in inputs:
                # A recorded scalar from DB history already fills it.
                continue
            p = params_by_name.get(ref)
            if p is None:
                logger.warning(
                    "[execution] '%s': parameter '%s' is wired to Parameter '%s', "
                    "which is no longer declared in source — leaving it unbound",
                    function_name,
                    param,
                    ref,
                )
                continue
            if not p.values:
                # Declared but never given a value. Raising is the whole
                # point: bound as-is it is a zero-length EachOf axis, so
                # for_each would iterate zero times, write no records and
                # report success. (for_each refuses it too — this one gets
                # in first so the message can name the DECLARED parameter
                # the user sees on the canvas, not the signature param.)
                raise ValueError(
                    f"parameter '{ref}' has no value yet, so '{function_name}' "
                    f"has nothing to run -- give it at least one value on its "
                    f"node."
                )
            # Hidden values are keyed by the PARAMETER NODE's declared name
            # (the checkbox writes that name), not the signature param it feeds.
            inputs[param] = _apply_hidden_values(
                p, ref, function_name, hidden_values
            )

        elif kind == BINDING_COLNAME:
            # Deferred: scifor substitutes the current for_columns column on
            # each per-column call. `_attach_colname_inputs` already removed
            # any recorded constant for this argument, and refused to bind it
            # without an iterate input.
            inputs[param] = ColName()
            logger.info(
                "[execution] '%s': input '%s' receives the current column name "
                "(ColName)",
                function_name,
                param,
            )

        else:
            logger.warning(
                "[execution] '%s': parameter '%s' has unknown binding kind %r "
                "— leaving it unbound",
                function_name,
                param,
                kind,
            )

    unbound = [p for p in _fn_params_from_registry(function_name) if p not in inputs]
    if unbound:
        # Not necessarily an error — optional params with defaults are
        # legitimately unbound — but it is the first thing to check when a
        # run does nothing, so it is stated rather than left to be inferred
        # from an empty inputs dict.
        logger.info(
            "[execution] '%s': %d signature param(s) left unbound by the "
            "wiring (using their defaults): %s",
            function_name,
            len(unbound),
            ", ".join(unbound),
        )
    logger.info(
        "[execution] '%s': bindings — %s",
        function_name,
        describe_run_inputs(inputs, function_name),
    )
    return inputs


def _describe_input_value(value) -> str:
    """One input, in the spelling the canvas chip uses.

    ``scidb.intent.describe_columns`` owns the selection half, so the log line
    and the node cannot disagree about what a run was fed.
    """
    from scidb.intent import describe_columns
    from scifor import ColumnSelection, EachOf, Fixed, PathInput

    if isinstance(value, EachOf):
        # A Parameter IS an EachOf, and reports itself as one.
        kind = type(value).__name__
        alts = value.alternatives
        if kind != "EachOf":
            return f"{kind}({len(alts)} value(s))"
        return " | ".join(_describe_input_value(a) for a in alts) or f"{kind}(empty)"
    if isinstance(value, ColumnSelection):
        inner = getattr(value, "data", None)
        name = getattr(inner, "__name__", None) or str(inner)
        return f"{name} ({describe_columns(value)})"
    if isinstance(value, Fixed):
        inner = getattr(value, "data", None)
        return f"Fixed({_describe_input_value(inner)})"
    if isinstance(value, PathInput):
        return f"PathInput({value})"
    name = getattr(value, "__name__", None)
    if name:
        return f"{name} (whole variable)"
    return repr(value)


def describe_run_inputs(inputs: dict, function_name: str) -> str:
    """One line naming EVERY input this run feeds the function.

    ``value: TrialMeanSymmetry ("ankle") · cycles: CycleSymmetry (whole
    variable) · tol: 0.01 · side: (unbound)``

    Completeness is the point: the line is built from the function's
    SIGNATURE, not from the inputs dict, so a parameter nothing bound is
    named as unbound rather than silently absent. "What did this run actually
    feed the function?" took four round trips through the logs on 2026-09-19;
    this is the line that answers it. See
    ``docs/claude/input-binding-round-trip.md``.
    """
    from scistack_gui.api.pipeline import _fn_params_from_registry

    try:
        params = list(_fn_params_from_registry(function_name))
    except Exception:  # a log line must never break a run
        params = []
    for extra in inputs:
        if extra not in params:
            params.append(extra)
    if not params:
        return "(no inputs)"
    return " · ".join(
        f"{p}: {_describe_input_value(inputs[p]) if p in inputs else '(unbound)'}"
        for p in params
    )



def variable_inputs_view(targets: list[dict], function_name: str = "") -> dict:
    """The MATLAB generator's ``variable_inputs`` map, from the SAME resolved
    bindings :func:`build_run_inputs` consumes.

    ``{param: [type_names]}`` for a whole-variable binding and
    ``{param: {"types": [...], "columns": [...], "iterate": bool,
    "pool_variants": bool}}`` for one with a column selection or
    AcrossVariants pooling — the two shapes
    ``api.matlab_command._variable_binding_parts`` parses. Until 2026-09-19
    the MATLAB route assembled this from canvas edges plus the node config
    on its own, so a selection recorded in HISTORY (a Python-authored
    ``Var["knee"]`` step) reached a Python re-run and not a MATLAB one: two
    derivations of one fact, agreeing by convention. One derivation now;
    this is only a rendering of it.

    Targets of one call site agree on their bindings; if two disagree (a
    name-scoped derivation over two call sites of one name) the first wins
    and the loser is WARNed, matching ``column_selections_for_nodes``.

    A ``colname`` binding renders as ``{"colname": True}`` in the same map
    (``api.matlab_command._is_colname_entry``), so every MATLAB emit site
    that reads variable inputs emits ``scidb.ColName()`` with no new argument.
    """
    from scistack_gui.domain import column_selection as _cs
    from scistack_gui.domain.edge_resolver import BINDING_COLNAME, BINDING_VARIABLE

    out: dict = {}
    for t in targets or []:
        for param, binding in (t.get("bindings") or {}).items():
            if binding.get("kind") == BINDING_COLNAME:
                out[param] = {"colname": True}
                continue
            if binding.get("kind") != BINDING_VARIABLE:
                continue
            ref = binding.get("ref")
            types = [str(x) for x in (ref if isinstance(ref, (list, tuple)) else [ref]) if x]
            if not types:
                continue
            sel = _cs.from_binding(binding)
            pooled = bool(binding.get("pool_variants"))
            entry: "dict | list"
            if sel or pooled:
                entry = {"types": types}
                if sel:
                    entry["columns"] = list(sel["columns"])
                    entry["iterate"] = bool(sel["iterate"])
                # Omitted when false, the same rule `variable_binding` uses,
                # so a plain selection's rendering is byte-identical to what
                # it was before pooling existed.
                if pooled:
                    entry["pool_variants"] = True
            else:
                entry = list(types)
            previous = out.get(param)
            if previous is not None and previous != entry:
                logger.warning(
                    "[execution] '%s': parameter %r is bound differently on two "
                    "targets (%s vs %s) — keeping the first for the MATLAB command",
                    function_name or "?",
                    param,
                    previous,
                    entry,
                )
                continue
            out[param] = entry
    return out


def default_schema_level(
    db,
    function_name: str,
    targets: list[dict],
    *,
    stated=None,
    node_id: "str | None" = None,
    route: str = "",
):
    """``(SchemaLevel, rule)`` — which schema keys a run of *function_name*
    iterates. Every run route calls this, and so does the settings panel
    (``get_schema_level``), so what the panel shows is what a Run does.

    The decision is scidb's (``scidb.schema_level.resolve_schema_level``:
    stated > where this node last ran > its inputs > every key). This
    function only gathers the GUI-side facts it needs:

    * *stated* — the node's stored ``schemaLevel`` (``null`` / ``[]`` /
      ``[keys]``), read by ``SchemaLevel.from_stated``: ``[]`` is ONE CALL on
      every route (cleanup-audit F26);
    * the node's call sites — ``call_id`` of each HISTORY target. A target the
      user rewired carries none (``variant_resolver`` drops it), so a rewired
      or new node has no recorded level and falls to its inputs (user
      decision 2026-09-23: history counts only under the current wiring);
    * the bound inputs — variable types and PathInput objects (alternate
      templates included) from the targets' bindings.

    ``as_table`` plays NO part: it is the format inputs arrive in (tables
    keeping their schema-key columns), never how many calls there are. The
    Python Run used to read "as_table, no level" as one call over the whole
    dataset — a rule no other route had (cleanup-audit F32, dropped
    2026-09-23 by user decision). To aggregate, state the level on the node.

    Logs ONE ``[schema-level]`` line naming the route and the rule, so a run
    that iterates the wrong keys says why.
    """
    from scidb import provenance_query as _pq
    from scidb.schema_level import input_levels, resolve_schema_level
    from scistack_gui import registry
    from scistack_gui.domain.edge_resolver import (
        BINDING_PATHINPUT,
        BINDING_VARIABLE,
        path_input_names,
    )

    schema_keys = list(db.dataset_schema_keys)

    call_ids = {t.get("call_id") for t in targets or [] if t.get("call_id")}
    recorded = None
    if call_ids:
        try:
            recorded = _pq.recorded_schema_keys(
                db._duck, function_name, schema_keys, call_ids=call_ids
            )
        except Exception:
            logger.warning(
                "[schema-level] '%s': recorded level lookup failed; using inputs",
                function_name,
                exc_info=True,
            )

    type_names: set[str] = set()
    path_inputs: list = []
    registry_pis = registry.get_path_inputs_registry()
    for t in targets or []:
        for binding in (t.get("bindings") or {}).values():
            kind = binding.get("kind")
            ref = binding.get("ref")
            if kind == BINDING_VARIABLE:
                type_names.update(ref if isinstance(ref, (list, tuple)) else [ref])
            elif kind == BINDING_PATHINPUT:
                path_inputs.extend(
                    registry_pis[n] for n in path_input_names(ref) if n in registry_pis
                )
    try:
        levels = input_levels(db._duck, type_names, path_inputs, schema_keys)
    except Exception:
        logger.warning(
            "[schema-level] '%s': input level lookup failed",
            function_name,
            exc_info=True,
        )
        levels = []

    level, rule = resolve_schema_level(schema_keys, stated, recorded, levels)
    logger.info(
        "[schema-level] %s node=%s stated=%s -> iterating %s (%s) via %s "
        "[call sites %d, recorded %s, input levels %s]",
        function_name,
        node_id or "-",
        "unset" if stated is None else stated,
        level.describe(),
        rule,
        route or "?",
        len(call_ids),
        recorded,
        levels,
    )
    return level, rule


def node_schema_level(
    db, node_id: "str | None", function_name: str, stated=None
) -> dict:
    """The level a Run of this node would iterate, for the settings panel:
    ``{"state", "keys", "rule", "schema_keys"}``.

    Built from EXACTLY what a Run passes (``derive_target_for_node`` + staged
    pending overrides) through the same resolver, so the panel cannot show a level the run then ignores
    (cleanup-audit F23). *stated* is the panel's current, possibly unsaved,
    value.
    """
    from scistack_gui import pipeline_store

    targets = (
        derive_target_for_node(db, node_id)
        if node_id
        else derive_fn_targets(db, function_name)
    )
    targets = apply_pending_overrides(targets, pipeline_store.get_pending_constants(db))
    level, rule = default_schema_level(
        db,
        function_name,
        targets,
        stated=stated,
        node_id=node_id,
        route="settings panel",
    )
    return {**level.to_json(rule), "schema_keys": list(db.dataset_schema_keys)}


def build_run_declared_names(target: dict) -> dict[str, str]:
    """The for_each ``parameter_names=`` dict for a derived target:
    ``{argument: declared name}`` from its Parameter AND PathInput bindings.

    A PathInput's declared name is what groups PathInput-fed steps on the
    canvas and in ``scidb graph`` alike, so the run records it too
    (cleanup-audit F38).

    A value already recorded in history reaches ``for_each`` as a bare
    scalar (``build_run_inputs`` puts ``target["constants"]`` in first), so
    the name the canvas shows has to be stated or the run records only the
    argument — and the next graph build draws a second Parameter node named
    after it (cleanup-audit B1). The wiring is the only source; scidb merges
    it with any named Parameter (``scidb.parameter.declared_input_names``).
    """
    from scistack_gui.domain.edge_resolver import (
        BINDING_PARAMETER,
        BINDING_PATHINPUT,
        bindings_of_kind,
    )

    bindings = target.get("bindings") or {}
    # An EachOf of PathInputs names no single declaration; each arm records
    # its own name (scidb takes a PathInput's declared name from its key).
    return {
        **{
            p: ref
            for p, ref in bindings_of_kind(bindings, BINDING_PATHINPUT).items()
            if not isinstance(ref, (list, tuple))
        },
        **bindings_of_kind(bindings, BINDING_PARAMETER),
    }


def build_run_glue(target: dict, function_name: str) -> dict:
    """The for_each ``glue=`` dict for a derived target, or ``{}``.

    ``{param: [scidb.glue.GlueSpec, ...]}`` — resolved from the target's
    ``glue_chains`` (glue node names, in application order) against the
    registry, exactly as ``build_run_inputs`` resolves a PathInput or
    Parameter binding by declared name.

    Glue is a property of the consuming step's input binding, never a step
    of its own: this function is called *alongside* ``build_run_inputs`` for
    one function node, and never for a glue node (which has no run path at
    all — see ``api/run.py``). A MATLAB glue node crosses as source text so
    ``+scidb/for_each.m`` can run the body while Python still hashes it into
    the consumer's identity.
    """
    from scidb.glue import GlueSpec
    from scistack_gui import registry

    chains = target.get("glue_chains") or {}
    if not chains:
        return {}

    matlab_names, matlab_source = _matlab_glue_sources()

    out: dict[str, list] = {}
    for param, names in chains.items():
        specs = []
        for name in names:
            if name in matlab_names:
                specs.append(
                    GlueSpec(
                        name=name,
                        language="matlab",
                        source_text=matlab_source(name),
                    )
                )
                continue
            fn = registry.lookup_function(name)
            if fn is None:
                logger.warning(
                    "[execution] '%s': parameter '%s' is wired through glue "
                    "'%s', which is no longer discovered in source — the run "
                    "would silently reshape nothing, so the chain is dropped",
                    function_name,
                    param,
                    name,
                )
                specs = []
                break
            specs.append(GlueSpec(name=name, fn=fn))
        if specs:
            out[param] = specs
            logger.info(
                "[execution] '%s': input '%s' passes through glue [%s]",
                function_name,
                param,
                ", ".join(s.name for s in specs),
            )
    return out


def _matlab_glue_sources():
    """``(names, source_text_fn)`` for MATLAB-declared glue functions."""
    try:
        from scistack_gui import matlab_registry as _mr

        names = set(_mr.get_all_function_names())

        def _source(name: str) -> str:
            fn = _mr.get_matlab_function(name)
            path = getattr(fn, "path", None)
            if path:
                try:
                    return Path(path).read_text(encoding="utf-8")
                except OSError:
                    pass
            return name

        return names, _source
    except Exception:
        return set(), (lambda name: name)


def _scope_function_node_ids(db, pipeline_id: str, identity=None) -> list[tuple[str, str]]:
    """``[(node_id, function label)]`` for every function node drawn on
    ``pipeline_id``'s canvas — one entry per NODE, never per function name.

    **Read off the canvas itself** (plan-canvas-as-document Stage 4): the
    scope's built graph, without run states. Until 2026-10-22 this assembled
    the list from three sources (hand-placed rows, positioned history nodes,
    and unplaced history wirings resolved through ``ensure_node_identities``),
    a second copy of the canvas's membership rules. The build is now the one
    owner of "what is on this canvas", and it also adopts whatever history
    holds that the document does not, so a step a script just ran compiles.

    The ids are the ones the canvas draws (placement-qualified where a node is
    placed in this scope by ``{id}::{scope}``); ``derive_target_for_node``
    accepts either spelling. ``identity`` is accepted for the callers that
    still pass it and is not needed.
    """
    from scistack_gui.api.pipeline import _build_graph

    graph = _build_graph(db, pipeline_id, run_states=False)
    out = [
        (n["id"], n["data"]["label"])
        for n in graph.get("nodes", [])
        if n.get("type") == "functionNode"
    ]
    logger.info(
        "[execution] scope %s compiles %d function node(s): %s",
        pipeline_id,
        len(out),
        [nid for nid, _ in out],
    )
    return out


def _scope_function_labels(db, pipeline_id: str, identity=None) -> list[str]:
    """Distinct function labels represented in ``pipeline_id`` — for
    reporting only (e.g. the disconnected-steps summary), where
    collapsing sibling wirings of the same name to one label is fine.
    Execution must stay wiring-scoped — see _scope_function_node_ids."""
    labels: list[str] = []
    for _nid, label in _scope_function_node_ids(db, pipeline_id, identity):
        if label not in labels:
            labels.append(label)
    return labels


def pipeline_has_matlab_steps(db, pipeline_id: str) -> bool:
    """True if ``pipeline_id``'s scope, or any pipeline it (transitively)
    uses, contains a MATLAB function node.

    ``build_backend_pipeline``/``run_pipeline`` can only ever compile
    Python steps: ``registry.get_function(fn_label)`` raises ``KeyError``
    for a MATLAB-registered function name, which the per-node loop above
    already catches and silently skips (logged as a warning) — a MATLAB-
    containing pipeline run today just quietly omits its MATLAB steps
    rather than erroring. ``start_pipeline_run`` (``api/run.py``) uses this
    check to route such a pipeline to host-side MATLAB execution instead
    (see plan-matlab-pipeline-execution.md) — the same
    ``matlab_registry.is_matlab_function`` over ``_scope_function_node_ids``
    test ``code_export_service`` already uses to detect a scope's language.
    """
    from scistack_gui import matlab_registry
    from scistack_gui.services.portability_service import _closure_pipeline_ids

    for pid in _closure_pipeline_ids(db, pipeline_id):
        for _node_id, fn_label in _scope_function_node_ids(db, pid):
            if matlab_registry.is_matlab_function(fn_label):
                return True
    return False


def build_backend_pipeline(db, pipeline_id: str, _built: dict | None = None):
    """Compile one GUI pipeline scope into an in-session scidb.Pipeline.

    Function nodes register their derived targets as deferred steps
    (``pipeline=pipe`` — never ambient, so nothing else in the process is
    affected); use rows compose recursively with their bindings. Shared
    children compile once per request (``_built`` memo), preserving the
    backend's diamond dedup by object identity.
    """
    from scidb.pipeline import Pipeline

    from scidb import for_each
    from scistack_gui import pipeline_store, registry, step_names
    from scistack_gui.domain.variant_resolver import (
        filter_hidden_targets,
        hidden_call_ids_for_fn,
    )

    if _built is None:
        _built = {}
    if pipeline_id in _built:
        return _built[pipeline_id]

    names = {p["pipeline_id"]: p["name"] for p in pipeline_store.list_pipelines(db)}
    pipe = Pipeline(names.get(pipeline_id, pipeline_id), db=db)
    _built[pipeline_id] = pipe

    # Steps iterate the full schema grid, like a hand-written script's
    # ``for_each(..., subject=subjects)``. Passed as EXPLICIT metadata
    # iterables (not schema_level) so a use-edge binding's ``iterate``
    # overrides compose per key instead of conflicting with schema_level
    # (for_each forbids mixing the two). Without iterables, for_each pools
    # every schema row into ONE call — functions written per-combo then
    # crash on multi-row tables (found via gui_test_data 2026-07-18).
    all_iterables = {
        key: db.distinct_schema_values(key) for key in db.dataset_schema_keys
    }

    # Staged pending constants override DB history at compile time, so the
    # plan previews the staged variant and pull runs materialize it — same
    # helper as the eager run thread (Stage 2 of wiring-grouped plan).
    # Post-override dedup keys on (constants, output_type) — overriding can
    # collapse constant-only differences, but a multi-output fn's per-output
    # targets must all survive.
    pending_consts = pipeline_store.get_pending_constants(db)

    # Hidden combos (see plan-combo-hiding.md) — this path never passes
    # distribute/as_table to for_each (below), so filtering here must use
    # the same False/None it actually runs with, not a node's persisted
    # runOptions; a hidden pending combo hashed with a different
    # distribute/as_table simply won't match and fails safe (reappears)
    # rather than mis-hiding a different combo. THIS scope's hides: the
    # compiled pipeline runs one canvas's nodes (_scope_function_node_ids).
    hidden_ids = pipeline_store.get_hidden_node_ids(db, pipeline_id)

    for node_id, fn_label in _scope_function_node_ids(db, pipeline_id):
        try:
            fn = registry.get_function(fn_label)
        except KeyError:
            logger.warning(
                "[execution] scope %s: function '%s' not in registry — skipped",
                pipeline_id,
                fn_label,
            )
            continue
        # Scoped to THIS node's own wiring (derive_target_for_node), never
        # every node/call site sharing fn_label — see
        # _scope_function_node_ids for why (same fix as the single-node
        # Run path's node_id-scoped derivation in api/run.py).
        targets = apply_pending_overrides(
            derive_target_for_node(db, node_id), pending_consts
        )
        # The step's OWN run options, not defaults: they are identity-bearing
        # (folded into the call_id hidden-combo filtering matches on, and into
        # the invocation_id the run writes), so hard-coding them here filtered
        # against the id of a call this pipeline will never make AND ran every
        # step non-distributed regardless of what the node said. The
        # single-node Run path and the MATLAB path had both been fixed; this
        # one had not.
        step_on_error = on_error_from_config(
            (pipeline_store.get_node_config(db, node_id) or {}).get("runOptions")
        )
        step_options = RunOptions.from_config(
            (pipeline_store.get_node_config(db, node_id) or {}).get("runOptions")
        )
        targets = filter_hidden_targets(
            targets,
            fn_label,
            hidden_call_ids_for_fn(hidden_ids, fn_label),
            pending_consts,
            step_options,
        )
        # Same dispatch record as the single-node Run path (D-2026-09-22-2):
        # a pipeline run is still a GUI-started run and still holds the node
        # id, so nothing about it needs inferring afterwards.
        record_dispatch_wirings(db, node_id, fn_label, targets, None)
        # Which keys THIS node iterates, resolved ONCE from all its targets —
        # the same inputs the single-node Run passes, so a node runs at the
        # same level from its own Run button and from a pipeline run.
        level, why = default_schema_level(
            db,
            fn_label,
            targets,
            stated=(pipeline_store.get_node_config(db, node_id) or {}).get(
                "schemaLevel"
            ),
            node_id=node_id,
            route=f"python pipeline {pipeline_id}",
        )
        schema_iterables = {
            k: all_iterables[k] for k in level.iterate_keys() if k in all_iterables
        }
        seen_target_keys: set = set()
        for target in targets:
            target_key = (
                constants_identity_key(target["constants"]),
                target["output_type"],
            )
            if target_key in seen_target_keys:
                continue
            seen_target_keys.add(target_key)
            try:
                inputs = build_run_inputs(target, fn_label, db)
                output_cls = registry.get_variable_class(target["output_type"])
            except KeyError as exc:
                logger.warning(
                    "[execution] scope %s: '%s' target skipped (%s)",
                    pipeline_id,
                    fn_label,
                    exc,
                )
                continue
            # Glue rides on the step's INPUT BINDING (glue=), never as a step
            # of its own — a glue node feeding nothing is simply never
            # executed, and a glue node on the canvas adds no StepSpec here.
            step_handle = for_each(
                fn,
                inputs,
                [output_cls],
                db=db,
                pipeline=pipe,
                glue=build_run_glue(target, fn_label) or None,
                parameter_names=build_run_declared_names(target) or None,
                # This node's own location selection, as its Run button uses
                # (cleanup-audit F36: pipeline runs ignored it).
                locations=(pipeline_store.get_node_config(db, node_id) or {}).get(
                    "schemaSelection"
                )
                or None,
                distribute=step_options.distribute,
                as_table=step_options.as_table,
                # The node's step name (Q6), so a compiled run and its code
                # export are named like the node.
                step=step_names.step_name_of(db, node_id),
                # Not identity (scistack_gui.services.run_failures).
                on_error=step_on_error,
                **schema_iterables,
            )
            # The run report echoes the tag, so a step's failures land on
            # THIS node (two nodes may share a function and a step name).
            step_handle.spec.tag = node_id

    for use in pipeline_store.get_pipeline_uses(db, pipeline_id):
        child = build_backend_pipeline(db, use["child_pipeline_id"], _built)
        bound = _bound(child, use.get("binding") or {})
        pipe.use(bound if bound is not None else child)

    logger.info(
        "[execution] compiled scope %s -> pipeline '%s' (%d own step(s), %d use(s))",
        pipeline_id,
        pipe.name,
        len(pipe.steps),
        len(pipe.uses),
    )
    return pipe


def _bound(child, binding: dict):
    """``child.bind(...)`` for a placement's *binding*, or ``None`` when it
    is the identity. Step values that follow a Parameter (E5) are resolved
    to the project's Parameter here (``library_service.resolved_binding_params``)."""
    from scistack_gui.services.library_service import resolved_binding_params

    if not binding or not any(binding.get(k) for k in ("key_map", "params", "iterate")):
        return None
    return child.bind(
        key_map=binding.get("key_map"),
        params=resolved_binding_params(binding.get("params")) or None,
        iterate=binding.get("iterate"),
    )


def _as_placed(db, pipeline_id: str, pipe, built: dict):
    """Running a PLACED pipeline (its node's ▶ Run) runs it as placed: with
    each placement's step values (E3/E5). One placement, or several sharing
    a binding, is one run; several different ones are several variants, as
    a run of their parent would make. A pipeline whose placements set no
    step values runs as it is (unchanged behaviour)."""
    from scidb.pipeline import Pipeline

    from scistack_gui import pipeline_store

    placements = [
        u for u in pipeline_store.get_pipeline_uses(db)
        if u["child_pipeline_id"] == pipeline_id
    ]
    if not any((u.get("binding") or {}).get("params") for u in placements):
        return pipe
    runner = Pipeline(f"{pipe.name} (as placed)", db=db)
    built[f"{pipeline_id}::placed"] = runner
    seen: set[str] = set()
    for u in placements:
        bound = _bound(pipe, u.get("binding") or {})
        key = bound.signature() if bound is not None else "identity"
        if key in seen:
            continue
        seen.add(key)
        runner.use(bound if bound is not None else pipe)
    logger.info(
        "[execution] %s runs as placed: %d placement(s), %d distinct binding(s)",
        pipeline_id, len(placements), len(seen),
    )
    return runner


def _discard_compiled(built: dict) -> None:
    """Compiled pipelines are per-request transients: drop them from
    scidb's session bookkeeping so a long-running server doesn't
    accumulate them."""
    for pipe in built.values():
        pipe.discard()


def plan_pipeline(db, pipeline_id: str, target: str = "") -> list[dict]:
    """The plan-preview data (R2): compile + plan; nothing executes.

    Entries: step, pipeline, endpoint, state (green/red/unknown), n_combos.
    Functions excluded from compilation because a required input is
    disconnected (direct) or starved by one (cascaded) still appear here —
    synthetic entries with n_combos=0 and a skip_reason — rather than
    silently vanishing from the preview.
    """
    built: dict = {}
    try:
        pipe = build_backend_pipeline(db, pipeline_id, built)
        entries = pipe.plan(target=target or None)
    finally:
        _discard_compiled(built)
    result = [
        {
            "step": e["step"],
            "pipeline": e["pipeline"],
            "endpoint": bool(e["endpoint"]),
            "state": e["state"],
            "n_combos": len(e["combos"]),
        }
        for e in entries
    ]
    planned_steps = {e["step"] for e in result}
    for skipped in disconnected_report_entries(db, pipeline_id):
        if skipped["step"] in planned_steps:
            continue
        result.append(
            {
                "step": skipped["step"],
                "pipeline": skipped["pipeline"],
                "endpoint": False,
                "state": "red",
                "n_combos": 0,
                "skip_reason": skipped["skip_reason"],
            }
        )
    return result


def run_pipeline(
    db,
    pipeline_id: str,
    mode: str = "all",
    target: str = "",
    finalized: bool | None = None,
    skip_computed: bool = True,
    report_sink: "list | None" = None,
) -> dict:
    """Compile + execute through the backend verbs (synchronous — the run
    API wraps this in its background-thread/relay machinery).

    report_sink: filled with the per-step run report even when a step
    raises (on_error="stop"), when this function returns nothing.

    mode: "all" -> run_all; "until" -> run_until(target);
    "endpoints" -> run_endpoints(finalized=...); "show" -> show(target)
    (draft-run one endpoint + ancestors, zero endpoint records — the
    returned "rendered" list of paths/payloads is the ONLY handle on the
    draft outputs).
    """
    from scidb.intent import ORIGIN_GUI, run_origin

    from scistack_gui.services.library_service import missing_roots

    # D2: a library's PathInputs carry templates, never a root folder; say
    # which one needs the recipient's root instead of reading nothing.
    roots = missing_roots(db, pipeline_id)
    if roots:
        raise ValueError(
            f"PathInput(s) {', '.join(roots)} have no root folder in this project: set "
            "where the files are (sidebar → PathInputs → root folder) before running"
        )
    built: dict = {}
    rendered: list = []
    pipe = None
    try:
        pipe = _as_placed(db, pipeline_id, build_backend_pipeline(db, pipeline_id, built), built)
        # Compiled from the canvas, so it read the intent store: label every
        # run it starts (rule 3, docs/claude/intent-and-fact.md).
        with run_origin(ORIGIN_GUI):
            if mode == "all":
                if pipe is built.get(f"{pipeline_id}::placed"):
                    # The as-placed runner has no own steps: run what it uses.
                    pipe.run_composed(skip_computed=skip_computed)
                else:
                    pipe.run_all(skip_computed=skip_computed)
            elif mode == "until":
                pipe.run_until(target, finalized=finalized, skip_computed=skip_computed)
            elif mode == "endpoints":
                pipe.run_endpoints(
                    finalized=bool(finalized),
                    skip_computed=skip_computed,
                    include_used=True,
                )
            elif mode == "show":
                rendered = pipe.show(target, skip_computed=skip_computed)
            else:
                raise ValueError(f"unknown run mode {mode!r}")
    finally:
        _discard_compiled(built)
        if report_sink is not None and pipe is not None:
            report_sink.extend(pipe.last_run_report)
    # for_each never raises on iteration failures (continue-and-report),
    # so the caller decides success from the per-step report. Functions
    # excluded from compilation because a required input is disconnected
    # never reach scidb's own report — appended here instead of silently
    # vanishing from what the user sees (see disconnected_report_entries).
    report = list(pipe.last_run_report) + disconnected_report_entries(db, pipeline_id)
    return {
        "ok": True,
        "pipeline": pipe.name,
        "mode": mode,
        "report": report,
        "rendered": rendered,
    }
