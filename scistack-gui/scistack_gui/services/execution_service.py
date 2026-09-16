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
import logging
from itertools import product
from pathlib import Path

from scistack_gui.domain.graph_builder import PARAM_ID_PREFIX as _PARAM_PREFIX

logger = logging.getLogger(__name__)


def _infer_wired_constants(
    parameter_params: dict[str, str],
    pending: dict[str, set[str]],
    constants_registry: dict,
    *,
    fn_variants: "list[dict] | None" = None,
    log_context: str,
) -> dict[str, list]:
    """Infer values for a never-run wiring's Parameter params, in priority
    order: staged pending values; then, if *fn_variants* is given, any
    known value from OTHER real DB history call sites of the same function
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
            known_vals = {
                v["constants"][param_name]
                for v in fn_variants
                if param_name in v.get("constants", {})
            }
            if known_vals:
                typed_vals = sorted(known_vals, key=str)
                logger.debug(
                    "[execution] %s: parameter '%s' has no pending value — "
                    "reusing known value(s) %s from other call site(s) of "
                    "this function",
                    log_context,
                    param_name,
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
    )
    by_call: dict[str, dict[str, str]] = {}
    for pi_name, pi in path_inputs.items():
        for fkey, param_name in pi["functions"]:
            fn, call_id = fkey
            if fn == function_name:
                by_call.setdefault(call_id, {})[param_name] = pi_name
    return by_call


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

    No Parameter bindings: a history target's ``constants`` already hold
    concrete recorded values, so nothing needs looking up in the Parameter
    registry.
    """
    if not targets:
        return targets
    from scistack_gui.domain.edge_resolver import pathinput_binding, variable_binding

    by_call = _db_path_input_params(db, function_name)
    for t in targets:
        bindings: dict[str, dict] = {}
        for param, type_val in (t.get("input_types") or {}).items():
            bindings[param] = variable_binding(
                list(type_val) if isinstance(type_val, (list, tuple, set)) else [type_val]
            )
        for param, decl_name in by_call.get(t.get("call_id"), {}).items():
            bindings[param] = pathinput_binding(decl_name)
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
    from scistack_gui.domain.graph_builder import parse_fn_node_id, strip_placement

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
        return parsed is not None and parsed[0] == function_name

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
    return merged


def _attach_column_selections(
    db,
    node_ids,
    targets: list[dict],
    function_name: str = "",
    name_scoped: bool = False,
) -> list[dict]:
    """Stamp the GUI's saved column selections onto every target's bindings.

    Modelled directly on :func:`_attach_db_path_inputs` — the existing
    precedent for "GUI state that DB history cannot carry". Provenance records
    an input edge as ``(param -> record -> variable_type)``, so a run that used
    ``Trials["filename"]`` is indistinguishable in history from one that used
    the whole variable; the GUI's own config is the only record there is (see
    ``docs/claude/column-selection.md`` §From the GUI, "Known limitations").

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


def _hidden_constant_values(db) -> dict[str, set[str]]:
    """{const_name: {hidden values}} from the ``ConstantNode.tsx`` checkbox
    state — grouped once per derivation call so ``filter_hidden_
    constant_value_targets`` (a pure content-match, no call_id hashing) can
    check every target in one pass. ``pipeline_id=None`` unions every
    scope's hides, matching ``get_hidden_node_ids``' fail-open convention —
    execution is not yet scope-aware (see that function's docstring)."""
    from scistack_gui import pipeline_store

    hidden: dict[str, set[str]] = {}
    for row in pipeline_store.list_hidden_parameter_values(db, None):
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


def derive_fn_targets(db, function_name: str) -> list[dict]:
    """The for_each target(s) a function node represents.

    Each target: ``{"input_types": {param: type-or-list}, "output_type":
    str, "constants": {name: typed value}}`` — DB pipeline variants when
    history exists (manual output wiring overrides stale DB outputs),
    otherwise inferred from manual edges + pending constants. Returns []
    when nothing is derivable (no history AND no output wiring). PathInput-
    backed params are NOT resolved here — they're never part of
    ``input_types``/DB history at all (see ``build_run_inputs``), so
    resolving them at derivation time would mean threading a new field
    through every branch of this function AND ``derive_target_for_node``
    for no benefit; ``build_run_inputs`` resolves them once, right before
    execution, from the target this function already returns.

    Every returned target has already been filtered against hidden
    constant values (``filter_hidden_constant_value_targets``) — a hidden
    value is excluded from both never-run and previously-run combos, and
    from both this (name-scoped) path and ``derive_target_for_node``'s
    (node-scoped) path, so the per-node Run and pipeline Run threads (both
    of which bottom out in one of these two functions) can't accidentally
    run something the user unchecked.
    """
    from scistack_gui import pipeline_store
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui.domain.edge_resolver import (
        infer_manual_fn_output_types,
        resolve_function_edges,
    )
    from scistack_gui.domain.graph_builder import fn_node_id
    from scistack_gui.domain.variant_resolver import filter_hidden_constant_value_targets

    hidden_values = _hidden_constant_values(db)

    all_variants = db.list_pipeline_variants()
    # Bindings are attached HERE, not on the way out: every filter below
    # (disconnected wiring, hidden constant values) reads them, so a target
    # that reached them binding-less would be judged on an empty wiring.
    fn_variants = _attach_db_path_inputs(
        db,
        function_name,
        [v for v in all_variants if v["function_name"] == function_name],
    )

    all_edges = pipeline_store.get_manual_edges(db)
    manual_nodes = pipeline_store.get_manual_nodes(db)

    fn_node_ids = {f"fn__{function_name}"}  # legacy/manual edges
    for v in fn_variants:
        cid = v.get("call_id")
        if cid:
            fn_node_ids.add(fn_node_id(function_name, cid))
    for nid, meta in manual_nodes.items():
        if meta["type"] == "functionNode" and meta["label"] == function_name:
            fn_node_ids.add(nid)
    # Manual edges may reference WIRING-GROUPED node ids (fn__{fn}__{wid} —
    # the canvas groups call sites since 2026-07-18) whose suffix is not any
    # call_id: adopt any edge endpoint whose parsed fn name matches.
    from scistack_gui.domain.graph_builder import parse_fn_node_id

    for edge in all_edges:
        for endpoint in (edge.get("source"), edge.get("target")):
            if endpoint and endpoint not in fn_node_ids:
                parsed_ep = parse_fn_node_id(endpoint)
                if parsed_ep is not None and parsed_ep[0] == function_name:
                    fn_node_ids.add(endpoint)

    manual_output_types = infer_manual_fn_output_types(
        fn_node_ids, all_edges, manual_nodes, existing_node_labels={}
    )

    from scistack_gui.domain.variant_resolver import reconcile_manual_inputs

    # Manual edges onto a history node's handles are authoritative for the
    # parameters they feed (hidden-and-reconnected OR never bound) — see
    # graph_builder.manual_input_overrides. Runs whenever there is anything
    # to reconcile, not only when an edge is hidden: a manual edge onto a
    # parameter history never bound has no hidden edge to trigger on.
    hidden_edge_ids = pipeline_store.get_hidden_edge_ids(db)
    if fn_variants and (hidden_edge_ids or all_edges):
        before = len(fn_variants)
        fn_variants = reconcile_manual_inputs(
            fn_variants, function_name, hidden_edge_ids, all_edges, manual_nodes
        )
        if len(fn_variants) != before:
            logger.info(
                "[execution] '%s': %d target(s) excluded — disconnected wiring",
                function_name,
                before - len(fn_variants),
            )

    if fn_variants and manual_output_types:
        # User rewired outputs: current wiring overrides stale DB history.
        logger.info(
            "[execution] '%s': overriding DB output types with manual wiring %s",
            function_name,
            manual_output_types,
        )
        overridden, seen_constants = [], set()
        for v in fn_variants:
            key = tuple(sorted(v["constants"].items()))
            if key in seen_constants:
                continue
            seen_constants.add(key)
            for out in manual_output_types:
                overridden.append({**v, "output_type": out})
        fn_variants = overridden

    if fn_variants:
        return _attach_column_selections(
            db,
            fn_node_ids,
            filter_hidden_constant_value_targets(fn_variants, hidden_values),
            function_name,
            name_scoped=True,
        )

    # Never-run fallback: infer the call from manual edges.
    resolved = resolve_function_edges(
        fn_node_ids=fn_node_ids,
        manual_edges=all_edges,
        manual_nodes=manual_nodes,
        existing_node_labels={},
    )
    if not resolved.output_types:
        logger.warning(
            "[execution] '%s': no DB history and no output "
            "wiring — no targets derivable",
            function_name,
        )
        return []

    inferred_constants: dict[str, list] = {}
    if resolved.parameter_params:
        from scistack_gui import registry

        pending = pipeline_store.get_pending_constants(db)
        inferred_constants = _infer_wired_constants(
            resolved.parameter_params,
            pending,
            registry.get_parameters_registry(),
            log_context=f"'{function_name}'",
        )

    return _attach_column_selections(
        db,
        fn_node_ids,
        filter_hidden_constant_value_targets(
            _inferred_targets(resolved, inferred_constants), hidden_values
        ),
        function_name,
        name_scoped=True,
    )


def derive_target_for_node(db, node_id: str) -> list[dict]:
    """The for_each target(s) that ONE SPECIFIC function node represents.

    ``derive_fn_targets`` resolves by function NAME across every node/call
    site sharing that name — correct as long as a name has only ever had
    ONE wiring, but the same function name can now legitimately have
    multiple independent wirings on one canvas (e.g. compute_rolling_vo2
    fed by RawVO2 in one node and by RawHeartRate in another — see
    api/pipeline.py's wiring-conflict guard, which keeps such nodes from
    merging/showing each other's state). Resolving purely by name can't
    tell them apart for EXECUTION either: clicking Run on the RawHeartRate
    node used to silently re-run the RawVO2 node's real DB history instead
    (found via a real GUI session). This resolves by the exact node
    clicked, using its own embedded wiring (already-graduated nodes) or
    its own resolved edges (still-manual nodes) to select only the
    matching real history / infer a fresh target — never anything
    belonging to a different node that merely shares the label.

    Returns the same shape as ``derive_fn_targets`` (a list of
    ``{"input_types", "output_type", "constants"}`` dicts — one per known
    constant-value variant of THIS wiring, or one freshly-inferred target
    if it has never been run), or ``[]`` if ``node_id`` isn't a function
    node or nothing is derivable from it.

    Every returned target has already been filtered against hidden
    constant values, same as ``derive_fn_targets`` — see that function's
    docstring.
    """
    from scistack_gui import pipeline_store
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui.domain.edge_resolver import resolve_function_edges
    from scistack_gui.domain.graph_builder import parse_fn_node_id, wiring_id
    from scistack_gui.domain.variant_resolver import filter_hidden_constant_value_targets

    hidden_values = _hidden_constant_values(db)
    manual_nodes = pipeline_store.get_manual_nodes(db)
    all_edges = pipeline_store.get_manual_edges(db)

    meta = manual_nodes.get(node_id)
    parsed = parse_fn_node_id(node_id)
    resolved = None
    if meta is not None:
        if meta["type"] != "functionNode":
            return []
        function_name = meta["label"]
        node_wiring = None  # resolved from this node's own edges, below
    elif parsed is not None:
        function_name, node_wiring = parsed
    else:
        return []

    all_variants = db.list_pipeline_variants()
    # Same as derive_fn_targets: bind first, so every filter below sees the
    # target's real wiring rather than an empty one.
    fn_variants = _attach_db_path_inputs(
        db,
        function_name,
        [v for v in all_variants if v["function_name"] == function_name],
    )

    if node_wiring is None:
        # Manual (not yet graduated) node — resolve ITS OWN wiring from
        # its own edges only, never from any other node sharing the label.
        resolved = resolve_function_edges(
            fn_node_ids={node_id},
            manual_edges=all_edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
        )
        if not resolved.output_types:
            return []
        inferred_inputs = {p: ts[0] for p, ts in resolved.input_types.items() if ts}
        node_wiring = wiring_id(
            function_name,
            inferred_inputs,
            set(resolved.output_types),
            resolved.path_input_params,
        )

    # PathInputs are part of the wiring shape, so both sides of this
    # comparison must carry them or a PathInput-fed node matches nothing.
    pi_by_call = _db_path_input_params(db, function_name)
    candidate_wirings = [
        (
            v,
            wiring_id(
                function_name,
                v["input_types"],
                {v["output_type"]},
                pi_by_call.get(v.get("call_id"), {}),
            ),
        )
        for v in fn_variants
    ]
    matching = [v for v, wid in candidate_wirings if wid == node_wiring]
    if not matching and candidate_wirings:
        # "This node matches no history" is indistinguishable, from the
        # outside, from "this node has no history" — both surface as the
        # same empty list and the same generic error at api/run.py. Show the
        # comparison that failed, since a node visibly green on the canvas
        # reaching here means the two sides hashed the SAME call site
        # differently (see wiring_id's note on the PathInput term).
        logger.warning(
            "[execution] node %s ('%s'): wiring %s matches none of the %d "
            "candidate variant(s) — computed %s. This node cannot run even "
            "though history exists for its function.",
            node_id,
            function_name,
            node_wiring,
            len(candidate_wirings),
            [
                {
                    "wiring_id": wid,
                    "call_id": v.get("call_id"),
                    "input_types": v.get("input_types"),
                    "output_type": v.get("output_type"),
                    "path_inputs": pi_by_call.get(v.get("call_id"), {}),
                }
                for v, wid in candidate_wirings
            ],
        )
    hidden_edge_ids = pipeline_store.get_hidden_edge_ids(db)
    if matching and (hidden_edge_ids or all_edges):
        from scistack_gui.domain.variant_resolver import reconcile_manual_inputs

        before = len(matching)
        matching = reconcile_manual_inputs(
            matching, function_name, hidden_edge_ids, all_edges, manual_nodes
        )
        if len(matching) != before:
            logger.info(
                "[execution] node %s ('%s'): %d target(s) excluded — disconnected wiring",
                node_id,
                function_name,
                before - len(matching),
            )
    if matching:
        return _attach_column_selections(
            db,
            {node_id},
            filter_hidden_constant_value_targets(matching, hidden_values),
            function_name,
        )
    if resolved is None:
        # An already-graduated node whose embedded wiring matches nothing
        # in current history (stale) — nothing safe to run as this node.
        # The warning above has already spelled out the failed comparison.
        logger.info(
            "[execution] node %s ('%s'): no targets — graduated node with no "
            "matching history and no manual edges to infer from",
            node_id,
            function_name,
        )
        return []

    # Never run with this wiring before — infer constant values from (in
    # order): pending (staged-but-unrun) values, then real DB history for
    # this FUNCTION regardless of wiring (a constant's known values are a
    # function-level property — e.g. compute_rolling_vo2's window_seconds
    # already has a real, known value from the RawVO2 call site, and a
    # user wiring the SAME shared constant node into a new RawHeartRate
    # wiring clearly means to reuse it, not re-stage it from scratch).
    inferred_constants: dict[str, list] = {}
    if resolved.parameter_params:
        from scistack_gui import registry

        pending = pipeline_store.get_pending_constants(db)
        inferred_constants = _infer_wired_constants(
            resolved.parameter_params,
            pending,
            registry.get_parameters_registry(),
            fn_variants=fn_variants,
            log_context=f"'{function_name}' (node {node_id})",
        )

    return _attach_column_selections(
        db,
        {node_id},
        filter_hidden_constant_value_targets(
            _inferred_targets(resolved, inferred_constants), hidden_values
        ),
        function_name,
    )


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
        manual_edge_handle_index,
        parse_fn_node_id,
        wiring_id,
    )

    hidden_edge_ids = pipeline_store.get_hidden_edge_ids(db)
    if not hidden_edge_ids:
        return None

    manual_index = manual_edge_handle_index(pipeline_store.get_manual_edges(db))

    node_wiring = None
    if node_id:
        parsed = parse_fn_node_id(node_id)
        if parsed is not None:
            node_wiring = parsed[1]

    all_variants = db.list_pipeline_variants()
    pi_by_call = _db_path_input_params(db, function_name)
    for v in all_variants:
        if v["function_name"] != function_name:
            continue
        wid = wiring_id(
            function_name,
            v["input_types"],
            {v["output_type"]},
            pi_by_call.get(v.get("call_id"), {}),
        )
        if node_wiring is not None and wid != node_wiring:
            continue
        for pname, vtype in v["input_types"].items():
            candidate = f"e__{vtype}__{function_name}__{wid}"
            if candidate in hidden_edge_ids and (function_name, wid, f"in__{pname}") not in manual_index:
                return f"input '{pname}' is disconnected — reconnect it before running"
        for cname in v.get("constants", {}).keys():
            candidate = f"e__{cname}__{function_name}__{wid}"
            if candidate in hidden_edge_ids and (function_name, wid, f"{_PARAM_PREFIX}{cname}") not in manual_index:
                return f"input '{cname}' is disconnected — reconnect it before running"
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
    from scistack_gui import pipeline_store, registry
    from scistack_gui.domain.graph_builder import (
        convert_scidb_path_inputs,
        hidden_wirings,
        wirings_downstream_of,
    )

    hidden_edge_ids = pipeline_store.get_hidden_edge_ids(db)
    if not hidden_edge_ids:
        return []

    scidb_agg = db.get_aggregated_variants()
    fn_input_params: dict = {}
    fn_outputs: dict = {}
    fn_constants: dict = {}
    for (fn_name, call_id), fn_data in scidb_agg["functions"].items():
        fkey = (fn_name, call_id)
        fn_input_params[fkey] = fn_data["input_params"]
        fn_outputs[fkey] = set(fn_data["outputs"])
        fn_constants[fkey] = set(fn_data["constants"].keys())
    # Resolved by registry name (not param name) — must match how
    # build_edges actually keys its pathInput__ edges, or hidden-edge-id
    # lookups below silently never match (see convert_scidb_path_inputs).
    path_inputs = convert_scidb_path_inputs(
        scidb_agg["path_inputs"],
        registry.get_path_inputs_registry(),
        pipeline_store.path_input_history_index(db),
        registry.get_project_root(),
    )

    manual_edges = pipeline_store.get_manual_edges(db)
    seed = hidden_wirings(
        fn_input_params, fn_outputs, fn_constants, path_inputs, hidden_edge_ids,
        manual_edges=manual_edges,
    )
    if not seed:
        return []
    downstream = wirings_downstream_of(fn_input_params, fn_outputs, seed, path_inputs)

    scope_labels = set(_scope_function_labels(db, pipeline_id))

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
            for pname, vtype in params.items():
                if (
                    f"e__{vtype}__{fn}__{wid}" in hidden_edge_ids
                    and (fn, wid, f"in__{pname}") not in manual_index
                ):
                    reason = f"input '{pname}' disconnected"
                    break
            for cname in fn_constants.get(fkey, set()):
                if (
                    f"e__{cname}__{fn}__{wid}" in hidden_edge_ids
                    and (fn, wid, f"{_PARAM_PREFIX}{cname}") not in manual_index
                ):
                    reason = f"input '{cname}' disconnected"
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
            function_name,
            t,
            pending_names,
            distribute=run_opts.get("distribute", False),
            as_table=run_opts.get("as_table"),
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
    out = []
    for target in targets:
        constants = dict(target["constants"])
        overridden = []
        for const_name, values in pending_constants.items():
            if const_name in constants and values:
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
    from scidb import EachOf
    from scistack_gui import registry
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui.domain import column_selection as _column_selection
    from scistack_gui.domain.edge_resolver import (
        BINDING_PARAMETER,
        BINDING_PATHINPUT,
        BINDING_VARIABLE,
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
                        _apply_column_selection(
                            registry.get_variable_class(t), sel
                        )
                        for t in type_names
                    )
                )
            elif type_names:
                inputs[param] = _apply_column_selection(
                    registry.get_variable_class(type_names[0]), sel
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
            pi = path_inputs_by_name.get(ref)
            if pi is None:
                logger.warning(
                    "[execution] '%s': parameter '%s' is wired to PathInput '%s', "
                    "which is no longer declared in source — leaving it unbound",
                    function_name,
                    param,
                    ref,
                )
                continue
            inputs[param] = pi
            logger.info(
                "[execution] '%s': input '%s' resolved via PathInput '%s' (%s)",
                function_name,
                param,
                ref,
                pi,
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
        "[execution] '%s': built inputs for %d param(s): %s",
        function_name,
        len(inputs),
        ", ".join(sorted(inputs)) or "(none)",
    )
    return inputs


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


def _scope_function_node_ids(db, pipeline_id: str) -> list[tuple[str, str]]:
    """Distinct (node_id, function_label) pairs whose nodes live in
    ``pipeline_id`` — manual function nodes by pipeline_id, DB-derived
    fn__ nodes by where their position is saved (same membership rule as
    the canvas).

    One entry per WIRING, not per function name: the same function name
    can have more than one independent wiring on a canvas at once (e.g.
    compute_rolling_vo2 fed by RawVO2 in one node and by RawHeartRate in
    another — see api/pipeline.py's wiring-conflict guard). Collapsing to
    distinct names here would make ``build_backend_pipeline`` derive
    targets by name (``derive_fn_targets``, which resolves across EVERY
    wiring sharing that name) instead of by the exact node
    (``derive_target_for_node``) — silently pipeline-running a sibling
    wiring's real DB history that isn't even the one on screen, and
    resurrecting a stale node for it once the run lands. See
    derive_target_for_node's docstring for the same bug, previously fixed
    for the single-node Run path but not this one.
    """
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store
    from scistack_gui.domain.graph_builder import (
        fn_node_id,
        parse_fn_node_id,
        wiring_id,
    )
    from scistack_gui.domain.scope_filter import node_scope

    manual_nodes = pipeline_store.get_manual_nodes(db)
    positions_by_scope = layout_store.read_positions_by_scope()

    seen: set[str] = set()
    node_ids: list[tuple[str, str]] = []

    def _add(nid: str, label: str) -> None:
        if nid not in seen:
            seen.add(nid)
            node_ids.append((nid, label))

    for nid, meta in manual_nodes.items():
        if (
            meta.get("type") == "functionNode"
            and (meta.get("pipeline_id") or "main") == pipeline_id
        ):
            _add(nid, meta["label"])
    placed_wirings: set[tuple[str, str]] = set()
    for _scope_id, positions in positions_by_scope.items():
        for nid in positions:
            parsed = parse_fn_node_id(nid)
            if parsed is None or nid in manual_nodes:
                continue
            placed_wirings.add(parsed)
            if node_scope(nid, manual_nodes, positions_by_scope) == pipeline_id:
                _add(nid, parsed[0])
    # DB-derived wirings with NO saved position default to root — one
    # entry per distinct (fn_name, wiring_id) among the unplaced variants,
    # not one per fn_name (a name can have several unplaced wirings at
    # once, each needing its own step).
    if pipeline_id == "main":
        pi_by_fn: dict[str, dict] = {}
        for v in db.list_pipeline_variants():
            fn = v["function_name"]
            if fn not in pi_by_fn:
                pi_by_fn[fn] = _db_path_input_params(db, fn)
            wid = wiring_id(
                fn,
                v["input_types"],
                {v["output_type"]},
                pi_by_fn[fn].get(v.get("call_id"), {}),
            )
            if (fn, wid) not in placed_wirings:
                _add(fn_node_id(fn, wid), fn)
    return node_ids


def _scope_function_labels(db, pipeline_id: str) -> list[str]:
    """Distinct function labels represented in ``pipeline_id`` — for
    reporting only (e.g. the disconnected-steps summary), where
    collapsing sibling wirings of the same name to one label is fine.
    Execution must stay wiring-scoped — see _scope_function_node_ids."""
    labels: list[str] = []
    for _nid, label in _scope_function_node_ids(db, pipeline_id):
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
    from scistack_gui import pipeline_store, registry
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
    schema_iterables = {
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
    # rather than mis-hiding a different combo.
    hidden_ids = pipeline_store.get_hidden_node_ids(db)

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
        targets = filter_hidden_targets(
            targets,
            fn_label,
            hidden_call_ids_for_fn(hidden_ids, fn_label),
            pending_consts,
            distribute=False,
            as_table=None,
        )
        seen_target_keys: set = set()
        for target in targets:
            target_key = (
                tuple(sorted(target["constants"].items())),
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
            for_each(
                fn,
                inputs,
                [output_cls],
                db=db,
                pipeline=pipe,
                glue=build_run_glue(target, fn_label) or None,
                **schema_iterables,
            )

    for use in pipeline_store.get_pipeline_uses(db, pipeline_id):
        child = build_backend_pipeline(db, use["child_pipeline_id"], _built)
        binding = use.get("binding") or {}
        if binding:
            pipe.use(
                child.bind(
                    key_map=binding.get("key_map"),
                    params=binding.get("params"),
                    iterate=binding.get("iterate"),
                )
            )
        else:
            pipe.use(child)

    logger.info(
        "[execution] compiled scope %s -> pipeline '%s' (%d own step(s), %d use(s))",
        pipeline_id,
        pipe.name,
        len(pipe.steps),
        len(pipe.uses),
    )
    return pipe


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
) -> dict:
    """Compile + execute through the backend verbs (synchronous — the run
    API wraps this in its background-thread/relay machinery).

    mode: "all" -> run_all; "until" -> run_until(target);
    "endpoints" -> run_endpoints(finalized=...); "show" -> show(target)
    (draft-run one endpoint + ancestors, zero endpoint records — the
    returned "rendered" list of paths/payloads is the ONLY handle on the
    draft outputs).
    """
    built: dict = {}
    rendered: list = []
    try:
        pipe = build_backend_pipeline(db, pipeline_id, built)
        if mode == "all":
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
