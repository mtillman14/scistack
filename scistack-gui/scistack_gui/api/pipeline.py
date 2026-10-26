"""
GET /pipeline

Returns the pipeline graph as React Flow nodes and edges.

Node types:
  - "variableNode": a named variable type (RawEMG, FilteredEMG, ...)
  - "functionNode": a pipeline function (bandpass_filter, ...)

Positions are set to (0, 0) here; the layout endpoint overwrites them with
saved positions, and the frontend assigns dagre positions for new nodes.
"""

import logging
import time

from fastapi import APIRouter
from pydantic import BaseModel
from scidb.database import DatabaseManager
from scidb.roles import endpoint_kind

from scistack_gui import layout as layout_store
from scistack_gui import registry
from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.ids import (
    FN_ID_PREFIX,
    ROOT_SCOPE,
    fn_node_id,
    handle_name,
    legacy_fn_node_id,
    var_node_id,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _parse_path_input(value: str) -> dict | None:
    """If *value* (from __inputs) represents a PathInput, return parsed info.

    Delegates to domain.graph_builder.parse_path_input.
    """
    from scistack_gui.domain.graph_builder import parse_path_input

    return parse_path_input(value)


def _fn_params_from_registry(fn_name: str) -> list[str]:
    """Return non-private parameter names from the registered function's
    signature, read through ``registry.function_arguments`` (the GUI's one
    reader: user code, library references such as ``pandas.read_csv``, and
    MATLAB functions)."""
    return [a.name for a in _fn_arguments(fn_name) if not a.private]


def _fn_arguments(fn_name: str) -> list:
    """``registry.function_arguments``, ``[]`` when unknown."""
    return registry.function_arguments(fn_name) or []


def _build_matlab_fn_proxy(fn_name: str):
    """Build a MatlabLineageFcn proxy for use in check_node_state.

    Uses the source hash from the MATLAB registry so the proxy's ``.hash``
    matches what was stored at save time.
    """
    from scimatlab.bridge import MatlabLineageFcn

    from scistack_gui import matlab_registry

    # The file's CURRENT hash, not the last scan's: a run hashes the file as
    # it is now, so a scan-time hash false-reds a just-run node after an edit.
    info = matlab_registry.current_matlab_function(fn_name, purpose="state check")
    # unpack_output MUST match scimatlab/.../+scihist/for_each.m's default.
    # Native MATLAB multi-output (`[a,b,c] = fn(...)`) uses unpack_output=False
    # and is unpacked at runtime via LineageFcn's n_out>1 branch. unpack_output=True
    # is only for the rarer single-cell-array-return pattern.
    proxy = MatlabLineageFcn(info.source_hash, fn_name, unpack_output=False)
    logger.debug(
        "[pipeline] matlab proxy fn=%s source_hash=%s unpack=False hash=%s",
        fn_name,
        info.source_hash[:12],
        proxy.hash[:12],
    )
    proxy.__name__ = fn_name
    return proxy


#: Ambiguity signatures already reported in this process, so "two nodes are
#: wired identically" is said ONCE rather than on every graph build. The popup
#: still arrives with every response (the panel that shows it may have mounted
#: since), but the log — where a repeated warning is noise that buries the next
#: one — records it the first time only.
_AMBIGUITIES_LOGGED: set[str] = set()


def _should_log_ambiguity(signature: str) -> bool:
    """True the FIRST time this exact ambiguity is seen in this process.

    The popup is per response; the log line is per process. A graph build runs
    on every refresh and on every ``dag_updated``, so logging the same pair
    each time would bury whatever came next under a warning the reader has
    already acted on (or decided not to).
    """
    if signature in _AMBIGUITIES_LOGGED:
        return False
    _AMBIGUITIES_LOGGED.add(signature)
    return True


def _node_scope(db, node_id: str) -> str:
    """The scope a node sits in, for the ambiguity report."""
    from scistack_gui import intent_store

    return intent_store.scope_of_node(db, node_id)


def ensure_node_identities(db):
    """Resolve and persist a node id for every wiring in history, for a
    caller that needs node ids WITHOUT building the whole graph. Returns the
    :class:`~scistack_gui.domain.node_identity.IdentityPlan`.

    **Why a Run path needs this.** A node id is no longer derivable from a
    wiring (D-2026-09-22-3), so anything that used to spell
    ``fn__{fn}__{wiring_id}`` now has to look one up — and a lookup only
    answers for a wiring some build has already seen.
    ``execution_service._scope_function_node_ids`` is the case: it lists the
    nodes a scope compiles, from history, and can legitimately reach a wiring
    written since the canvas last refreshed (a script run, a terminal MATLAB
    run, another window).

    Fabricating an id there would be the old bug wearing a new hat — two
    surfaces inventing an id for one node — so it asks here instead, and this
    runs the same ONE resolution `_build_graph` does. Idempotent: on the
    common path every wiring is already associated and nothing is written.
    """
    from scistack_gui.domain.edge_view import effective_edges

    # Identity is global (node ids are shared across scopes), so every scope's
    # hides apply, as before edge_view.
    view = effective_edges(db, None, caller="ensure_node_identities")
    plan, _warnings = _resolve_node_identity(
        db,
        build_aggregate(db, db.get_aggregated_variants()),
        view.drawn_list,
        view.manual_nodes,
        set(view.hidden_edge_ids),
    )
    return plan


def build_aggregate(db, scidb_agg: dict):
    """``get_aggregated_variants()`` → :class:`graph_builder.AggregatedData`.

    ONE conversion. It was written inline in ``_build_graph`` and again, in a
    narrower form, in ``execution_service.disconnected_report_entries`` — and
    the narrow copy silently omitted the PathInput resolution, which is the
    difference between a hidden PathInput edge matching and not
    (``convert_scidb_path_inputs``). Identity resolution needs the same shape
    as both, so this is the moment to have one of it.

    PathInputs are keyed by DECLARED name, not by the parameter they fill:
    a PathInput's identity is its source declaration (see
    ``docs/claude/code-discovery-categories.md``), and the raw DB extraction
    knows nothing about source.
    """
    from scistack_gui import node_wiring
    from scistack_gui import pipeline_store as _store
    from scistack_gui.domain import graph_builder as gb

    agg = gb.aggregate_from_scidb(
        scidb_agg,
        registry.get_path_inputs_registry(),
        _store.path_input_history_index(db),
        registry.get_project_root(),
        path_input_renames=_store.path_input_rename_index(db),
    )
    # A call site holding several output types is split by what Runs claimed
    # together (scidb.provenance.split_call_site_outputs) BEFORE anything
    # hashes a wiring, so every FnKey below has exactly one. Without it a
    # rewired node's call site hashed old+new outputs together and minted a
    # phantom node (scidb.log 2026-09-29).
    return gb.split_call_sites_by_claims(agg, node_wiring.claimed_wirings(db))


def _resolve_node_identity(
    db: DatabaseManager,
    agg,
    manual_edges,
    manual_nodes: dict,
    hidden_edge_ids,
):
    """Assign a node id to every wiring in history (D-2026-09-22-1/-2).

    Returns ``(IdentityPlan, warnings)``. The plan's ``token`` is handed to
    every ``token_for=`` seam in ``graph_builder``; the warnings ride out on
    the graph response so the GUI can raise the §7b popup.

    One pass, reading two things: which nodes already exist (the association
    table) and which wiring each of them currently STATES (its latest recorded
    shape with the user's drawn edges folded in). A wiring history holds is
    that node's if either says so, and otherwise it is a node nobody has seen
    before and gets an id.

    **It does not migrate.** On the first build of an existing database the
    table is empty, so every wiring in history mints a fresh id and every
    setting keyed by an old ``fn__{fn}__{wiring_id}`` stops resolving. That is
    the clean break, taken deliberately (D-2026-09-22-3/-4): the alternative —
    minting the old spelling so the rows still match — is a compatibility shim
    that leaves node ids looking like wiring hashes forever, which is the
    confusion this whole change exists to end.

    **It does not swallow failures.** If identity cannot be resolved, the GUI
    cannot say which node a run belongs to, and node state, the Run button and
    every saved setting are then describing something unverified. The build
    fails, loudly, rather than falling back to a derivation that has just been
    removed for being wrong.
    """
    from scistack_gui import node_wiring
    from scistack_gui.domain import graph_builder as gb
    from scistack_gui.domain import node_identity

    pi_by_fkey = gb.path_input_bindings_by_fkey(agg.path_inputs)

    # Every distinct wiring history holds.
    history: set[tuple[str, str]] = set()
    for fkey, params in agg.fn_input_params.items():
        fn, _cid = fkey
        history.add(
            (
                fn,
                gb.wiring_id(
                    fn,
                    params,
                    agg.fn_outputs.get(fkey, set()),
                    pi_by_fkey.get(fkey, {}),
                ),
            )
        )

    associations = node_wiring.associations(db)
    current_by_node = node_wiring.current_wiring_by_node(db)
    stated_by = gb.stated_wiring_claims(
        agg.fn_input_params,
        agg.fn_outputs,
        agg.fn_constants,
        agg.path_inputs,
        manual_edges,
        manual_nodes,
        hidden_edge_ids,
        current_by_node,
    )
    # Stored nodes that have never run state a wiring too (rule 1). Without
    # this, a hand-placed node whose exact wiring a script then ran got a
    # third node beside it once graduation was removed (Stage 5).
    # A removed (hidden) row keeps its row (never delete) but must not claim.
    from scistack_gui import pipeline_store as _ps_hidden

    hidden_everywhere = _ps_hidden.get_hidden_node_ids(db, None)
    claimable = {
        nid: meta
        for nid, meta in manual_nodes.items()
        if nid not in hidden_everywhere
    }
    for key, holders in gb.document_stated_claims(
        claimable, manual_edges, current_by_node, history
    ).items():
        merged = stated_by.setdefault(key, [])
        for node_id in holders:
            if node_id not in merged:
                merged.append(node_id)
    # Named runs (for_each(step=...)) claim before anything a node merely
    # states: a step name says which node a script run IS, even when the run
    # changed the wiring (Q6, step_names). A wiring already associated with a
    # node keeps it (resolve_identities checks the record first).
    from scistack_gui import step_names as _step_names

    names = _step_names.step_names(db)
    by_step = _step_names.step_claims(
        agg,
        lambda fkey: gb.wiring_id(
            fkey[0],
            agg.fn_input_params.get(fkey, {}),
            agg.fn_outputs.get(fkey, set()),
            pi_by_fkey.get(fkey, {}),
        ),
        {nid: meta for nid, meta in manual_nodes.items() if nid not in hidden_everywhere},
        names,
    )
    for key, holders in by_step.items():
        stated_by[key] = list(holders)
    plan = node_identity.resolve_identities(
        history,
        associations=associations,
        stated_by=stated_by,
        current_by_node=current_by_node,
        scope_of=lambda node_id: _node_scope(db, node_id),
    )
    # Re-keys FIRST: they move the old id's `_node_wiring` rows (and every other
    # node-keyed row) onto the new id, which `to_record` then writes under.
    # The same two movers a PathInput rename uses — one owner for "a canonical
    # node id changed", not a graduation-specific copy.
    if plan.rekeys:
        from scistack_gui import layout as layout_store
        from scistack_gui import pipeline_store as _ps

        for old, new in plan.rekeys.items():
            counts = _ps.rebase_node(db, old, new)
            counts["positions"] = layout_store.rebase_node_positions(old, new)
            logger.info(
                "[pipeline] re-keyed function node %s -> %s (id grammar): %s",
                old,
                new,
                counts,
            )
    for node_id, wiring, scope in plan.to_record:
        node_wiring.record(db, node_id, wiring, scope=scope)

    # A NAMED script run that gave its node a wiring the node had not run as
    # is a rewire of that node from code. The build retires the node's stored
    # edges the new wiring lacks (services.adoption.retire_rewired_edges), so
    # the canvas follows the script instead of drawing both wirings.
    previously_recorded = {row["wiring_id"] for row in associations}
    for (fn, wiring), holders in by_step.items():
        node = plan.node_by_wiring.get((fn, wiring))
        if node is None or wiring in previously_recorded:
            continue
        node = str(node)
        if node not in holders and not any(plan.rekeys.get(h) == node for h in holders):
            continue
        before = current_by_node.get(node) or next(
            (current_by_node.get(h) for h in holders if plan.rekeys.get(h) == node), None
        )
        if before and before != wiring:
            plan.rewired_by_step[node] = wiring
            logger.info(
                "[pipeline] step %r of '%s' ran as a new wiring %s (was %s): node %s "
                "follows it",
                names.get(node) or next((names.get(h) for h in holders), None),
                fn,
                wiring,
                before,
                node,
            )

    warnings = []
    for ambiguity in plan.ambiguities:
        warnings.append(
            {
                "kind": "wiring_ambiguity",
                "function_name": ambiguity.function_name,
                "wiring_id": ambiguity.wiring_id,
                "chosen": ambiguity.chosen,
                "others": list(ambiguity.others),
                "scope": ambiguity.scope,
                "signature": ambiguity.signature,
                "message": ambiguity.message(),
            }
        )
        if _should_log_ambiguity(ambiguity.signature):
            logger.warning("[pipeline] %s", ambiguity.message())

    superseded = [
        (fn, wiring)
        for (fn, wiring) in sorted(plan.node_by_wiring)
        if not plan.is_current(fn, wiring)
    ]
    if superseded:
        # The shapes a node has run as but no longer HAS. Its handles, edges
        # and Run button follow its current wiring only; these stay visible as
        # history, in the Variants panel and in provenance.
        logger.info(
            "[pipeline] %d wiring(s) are history rather than a node's current "
            "shape: %s",
            len(superseded),
            superseded,
        )
    return plan, warnings


def _compute_run_states(
    db: DatabaseManager,
    fn_input_params: dict[tuple, dict],
    fn_outputs: dict[tuple, set],
    disconnected_fkeys: set[tuple] | None = None,
    *,
    propagation_input_params: dict[tuple, dict] | None = None,
    hidden_fkeys: set[tuple] | None = None,
    unproduced_outputs: dict[tuple, list[str]] | None = None,
) -> dict[str, str]:
    """
    Compute run_state for every function and variable node.

    Function-keyed inputs use FnKey = (fn_name, call_id) so the same fn
    reused across multiple for_each call sites gets a distinct state per
    call site.

    Pass 1 — own state per function-call-site:
      Calls scidb.check_multiple_nodes_state() for all nodes in batch.

    Pass 2 — propagate staleness through the DAG (delegated to domain layer).
      ``disconnected_fkeys`` (call sites with a user-hidden required inbound
      edge — see domain.graph_builder.hidden_wirings/wiring_disconnected_fkeys)
      forces those call sites red regardless of DB freshness, cascading
      downstream through the same propagation.

      ``propagation_input_params`` is ``fn_input_params`` with manual variable
      edges folded in (graph_builder.input_params_with_manual_edges), and it is
      used for the CASCADE only. Pass 1's own-state check never sees it — that
      question is "has this call site done its recorded work", which the drawn
      edge does not change — and neither does anything that derives an id, since
      ``wiring_id`` hashes these params. Defaults to ``fn_input_params``.

      ``unproduced_outputs`` (graph_builder.unproduced_output_fkeys) are call
      sites whose node is DRAWN to write a variable it has never recorded. Pass
      1 answers from the recorded outputs only, so those are forced red here,
      before the cascade, which then carries the red downstream.

    Returns {node_id: "green"|"red"} for fn__ and var__ nodes — real,
    recorded call sites only. "pending" (an unrun staged constant value)
    is a display-only concept layered on top by
    domain.graph_builder.group_call_sites_by_wiring, which synthesizes a
    separate row for the not-yet-existing combo rather than downgrading
    any real call site's own state (see pending_value_group_coverage).
    """
    from scidb import BaseVariable, check_multiple_nodes_state
    from scistack_gui.domain.run_state import propagate_run_states

    t0 = time.monotonic()

    # --- Pass 1: Build function registry and nodes list ---
    # Build registry combining Python and MATLAB functions
    fn_registry = dict(registry._functions)  # Copy Python functions

    from scistack_gui import matlab_registry

    # Fill in the two kinds of function that aren't in registry._functions:
    # library references (pandas.read_csv — resolved by import) and MATLAB
    # functions (a proxy carrying the right hash). Only for names actually
    # on this canvas, so neither lookup is paid for the whole registry.
    for fn_name in fn_input_params.keys():
        fn_name_str, _ = fn_name
        if fn_name_str in fn_registry:
            continue
        library_fn = registry.lookup_function(fn_name_str)
        if library_fn is not None:
            fn_registry[fn_name_str] = library_fn
        elif matlab_registry.is_matlab_function(fn_name_str):
            fn_registry[fn_name_str] = _build_matlab_fn_proxy(fn_name_str)

    # Build nodes list for batched state checking. Own state is a property of
    # the REAL call site ("has it done its recorded work"), so a call site
    # split by Run claims (graph_builder.split_call_sites_by_claims) is asked
    # about ONCE, over all its outputs — exactly what it was asked before the
    # split — and every sub-site takes that answer.
    from scistack_gui.domain.graph_builder import call_id_of

    outputs_by_site: dict[tuple, set] = {}
    for fkey in fn_input_params:
        fn_name, cid = fkey
        outputs_by_site.setdefault((fn_name, call_id_of(cid)), set()).update(
            fn_outputs.get(fkey, set())
        )
    nodes = []
    for (fn_name, cid), fn_out_types in outputs_by_site.items():
        # Convert output type names to classes
        output_classes = [
            BaseVariable._all_subclasses[t]
            for t in sorted(fn_out_types)
            if t in BaseVariable._all_subclasses
        ]

        if output_classes:  # Only add if we have valid output classes
            nodes.append(
                {
                    "fn_name": fn_name,
                    "call_id": cid,
                    "outputs": output_classes,
                }
            )

    # Batch call to check states for all nodes
    state_results = check_multiple_nodes_state(nodes, fn_registry=fn_registry, db=db)

    # Convert results to fn_own_state format (FnKey → state)
    fn_own_state: dict[tuple, str] = {}
    for fkey in fn_input_params:
        fn_name, cid = fkey
        node_id = fn_node_id(fn_name, call_id_of(cid or ''))
        if node_id in state_results:
            fn_own_state[fkey] = state_results[node_id]["state"]
            counts = state_results[node_id].get("counts", {})
            logger.debug(
                "state(%s call_id=%s): %s (up_to_date=%d, stale=%d, missing=%d)",
                fn_name,
                cid,
                fn_own_state[fkey],
                counts.get("up_to_date", 0),
                counts.get("stale", 0),
                counts.get("missing", 0),
            )
        else:
            # Function not in results (no outputs or error) → mark as red
            fn_own_state[fkey] = "red"

    for fkey, missing in (unproduced_outputs or {}).items():
        if fkey in fn_own_state and fn_own_state[fkey] != "red":
            logger.info(
                "state(%s call_id=%s): %s -> red — drawn output(s) %s never "
                "recorded (graph_builder.unproduced_output_fkeys)",
                fkey[0],
                fkey[1],
                fn_own_state[fkey],
                missing,
            )
            fn_own_state[fkey] = "red"

    # --- Pass 2: DAG propagation (pure) ---
    result = propagate_run_states(
        fn_own_state,
        propagation_input_params
        if propagation_input_params is not None
        else fn_input_params,
        fn_outputs,
        disconnected_fkeys,
        # Call sites of HIDDEN nodes (graph_builder.hidden_fn_fkeys): run
        # states are computed here, before filter_hidden, so the cascade has
        # to be told which producers are not on screen.
        hidden_fkeys=hidden_fkeys,
    )

    elapsed_ms = (time.monotonic() - t0) * 1000
    counts = {"green": 0, "pending": 0, "red": 0}
    for nid, s in result.items():
        if nid.startswith(FN_ID_PREFIX):
            counts[s] = counts.get(s, 0) + 1
    logger.debug(
        "run_states complete: %d call sites in %.1fms (%d green, %d pending, %d red)",
        len([k for k in result if k.startswith(FN_ID_PREFIX)]),
        elapsed_ms,
        counts["green"],
        counts["pending"],
        counts["red"],
    )
    return result


def _matlab_param_to_class_from_db(
    aggregated_functions: dict,
    matlab_functions: set,
    matlab_output_order: dict,
) -> dict[str, dict[str, str]]:
    """``{fn_name: {output_param_name: class_name}}`` derived from DB variants.

    The DB-derived half of ``matlab_param_to_class``;
    ``infer_manual_fn_param_to_class`` supplies the manual-edge half. When BOTH
    come up empty the fn node renders handle ``out__{param}`` while build_edges
    points its edge at ``out__{Class}``, and React Flow silently drops an edge
    whose sourceHandle does not exist — the fn appears disconnected from an
    output variable that run_state marks green.

    **``output_num`` is only a signature slot sometimes** (2026-09-22). It is
    the PK half of ``_invocation_output`` and names *which output of this
    invocation* a record is, which coincides with the signature slot only for
    an ordinary multi-output call. Two cases where it does not:

    * a ``distribute`` run emits one record per slice, so ``output_num`` is the
      slice index (``database.py``'s latest-collapse says so in as many words);
    * a batch loader shares one invocation across runs and a re-run's record
      takes the next free slot, so one file's record was output #34 on the
      first run and #60 on the second (``provenance_save``, Fix B).

    So for a function declaring exactly ONE output, ``output_num`` cannot
    disambiguate anything and must not be consulted: the single declared name
    maps to the variant's ``output_type``, whatever the number says. Until
    this was noticed every such function fell down the out-of-range branch,
    contributed nothing, and logged a line PER DB RECORD — 100,758 of the
    120,351 lines in the 2026-09-22 log, and `from_db` empty for all seven
    MATLAB functions in that project.

    Only a genuinely multi-output function consults the number, and an
    out-of-range one there is summarised once per function rather than per
    record.
    """
    from_db: dict[str, dict[str, str]] = {}
    for (fn_name, _call_id), fn_data in aggregated_functions.items():
        if fn_name not in matlab_functions:
            continue
        names = matlab_output_order.get(fn_name) or []
        variants = fn_data.get("variants", [])
        if len(names) == 1:
            # One declared output: the number is noise here.
            seen = {
                v.get("output_type") for v in variants if v.get("output_type")
            }
            if not seen:
                continue
            if len(seen) > 1:
                # One param cannot name two classes, so the map can only hold
                # one. Say which and why rather than picking silently.
                logger.warning(
                    "[pipeline] matlab_param_to_class: fn=%s declares one output "
                    "%r but its records carry %d different types %s — using %r; "
                    "the others' edges will fall back to the manual-edge source",
                    fn_name,
                    names[0],
                    len(seen),
                    sorted(seen),
                    sorted(seen)[0],
                )
            from_db.setdefault(fn_name, {})[names[0]] = sorted(seen)[0]
            continue

        unresolved: set = set()
        for variant in variants:
            onum = variant.get("output_num")
            out_type = variant.get("output_type")
            if out_type is None:
                continue
            if onum is not None and 0 <= int(onum) < len(names):
                from_db.setdefault(fn_name, {})[names[int(onum)]] = out_type
            else:
                unresolved.add(onum)
        if unresolved:
            # DEBUG and aggregated: this runs per DB record on every canvas
            # refresh, and one line each is what made scidb.log 24 MB.
            logger.debug(
                "[pipeline] matlab_param_to_class: fn=%s left %d variant(s) "
                "unmapped — output_num %s names no slot in its %d declared "
                "output(s) %s; those edges fall back to the manual-edge source",
                fn_name,
                len(unresolved),
                sorted(n for n in unresolved if n is not None) or "(absent)",
                len(names),
                names,
            )
    return from_db


def _build_graph(
    db: DatabaseManager, pipeline_id: str = ROOT_SCOPE, *, run_states: bool = True
) -> dict:
    """
    Build nodes and edges from list_pipeline_variants() and list_variables(),
    restricted to one pipeline SCOPE.

    Delegates pure logic to domain.graph_builder and domain.edge_resolver;
    this function orchestrates data fetching and side effects. The full
    graph is built once, then scope-filtered (domain.scope_filter): manual
    nodes belong by pipeline_id, DB-derived nodes by where their position
    is saved (unsaved -> root), edges by both-endpoints-kept. pipelineNode
    entries for the scope's use edges are appended last (scope_service).
    """
    logger.info("[pipeline] Starting graph build orchestration (scope=%s)", pipeline_id)

    from scistack_gui import matlab_registry as _mr
    from scistack_gui import pipeline_store as _ps
    from scistack_gui import ids
    from scistack_gui.domain import graph_builder as gb
    from scistack_gui.domain.edge_resolver import resolve_function_edges

    hidden_ids = _ps.get_hidden_node_ids(db, pipeline_id)
    logger.debug(
        "[pipeline] loaded %d hidden node ID(s) for scope=%s", len(hidden_ids), pipeline_id
    )
    # The edges this canvas draws: the ONE loader every reader shares
    # (domain.edge_view, plan-unified-edge-model step 1).
    from scistack_gui.domain.edge_view import effective_edges

    edge_view = effective_edges(db, pipeline_id, caller=f"_build_graph({pipeline_id})")
    hidden_edge_ids = set(edge_view.hidden_edge_ids)

    # --- Fetch aggregated data from scidb (replaces steps 2-5) ---
    logger.info("[pipeline] Fetching aggregated variants from scidb")
    scidb_agg = db.get_aggregated_variants()
    logger.info(
        "[pipeline] fetched data for %d functions, %d variables, %d constants, %d path inputs",
        len(scidb_agg["functions"]),
        len(scidb_agg["variables"]),
        len(scidb_agg["constants"]),
        len(scidb_agg["path_inputs"]),
    )

    # Convert scidb format to AggregatedData format. ONE conversion, shared
    # with `ensure_node_identities` and `disconnected_report_entries` — see
    # `build_aggregate`.
    logger.info("[pipeline] Converting to AggregatedData format")
    agg = build_aggregate(db, scidb_agg)

    # NO hidden-node filtering here. Everything from here to the grouping
    # step — identity, disconnected wirings, run states, grouping — reads
    # what history RECORDED, because every one of them hashes wiring_id and
    # a hide is a view decision that must not change any node's identity.
    # A pre-identity filter pass used to live here and leaked one kind at a
    # time: hidden output variables first (fixed with a flag), then hidden
    # PathInputs, which minted a phantom node for the same call site
    # (docs/claude/hidden-path-input-identity.md). The ONE filter runs after
    # grouping; test_hidden_path_input_identity pins that order.

    logger.info("[pipeline] Using record counts from scidb")
    record_counts = {
        vtype: vdata["record_count"] for vtype, vdata in scidb_agg["variables"].items()
    }

    pending_constants = _ps.get_pending_constants(db)
    logger.debug("[pipeline] loaded %d pending constant(s)", len(pending_constants))
    pending_constants, removals = gb.auto_clean_pending_constants(
        pending_constants, agg
    )
    # A value that has run everywhere it is used stops being PENDING (it no
    # longer overrides) but stays STATED: values are part of the canvas
    # document and outlive their history (plan-canvas-as-document §4).
    for const_name, pval in removals:
        _ps.promote_pending_constant(db, const_name, pval)
    stated_constants = _ps.get_stated_constants(db)
    logger.debug(
        "[pipeline] %d pending value(s) promoted to stated; %d Parameter(s) "
        "carry stated values",
        len(removals),
        len(stated_constants),
    )

    # --- Disconnected wirings (hidden required inbound edges) ---
    # Computed on the PRE-GROUPING agg — every call site sharing a wiring
    # recomputes the same wiring_id regardless of grouping (see
    # graph_builder.hidden_wirings). Drives both run-state forcing below
    # and the per-node "disconnected" flag attached to function nodes.
    # manual_edges is fetched here (rather than further down, where it used
    # to be the first read) so a manual reconnect onto a previously-hidden
    # handle can clear the disconnected state in the same pass — see
    # graph_builder.hidden_wirings' manual_edges param. manual_nodes moved up
    # beside it 2026-09-22: run-state propagation now follows manual edges, and
    # resolving one whose source is a hand-dragged node needs this map.
    manual_edges_for_fn_lookup = edge_view.drawn_list
    manual_nodes = edge_view.manual_nodes
    logger.debug("[pipeline] loaded %d manual node(s)", len(manual_nodes))

    # --- Node identity (D-2026-09-22-1) -----------------------------------
    # BEFORE anything derives a node id from a wiring. Every call below that
    # used to spell `fn__{fn}__{wiring_id}` now asks `identity.token` for the
    # trailing segment, so a node that was rewired and run keeps its id (and
    # its position, config, scope membership and every `_intent` statement)
    # instead of a second node appearing beside it.
    identity, identity_warnings = _resolve_node_identity(
        db,
        agg,
        manual_edges_for_fn_lookup,
        manual_nodes,
        hidden_edge_ids,
    )
    token_for = identity.token
    if identity.rekeys:
        # The re-key rewrote node-keyed rows this build already read; re-read
        # them so THIS build draws the node with its edges and hides, rather
        # than one build late.
        hidden_ids = _ps.get_hidden_node_ids(db, pipeline_id)
        edge_view = effective_edges(
            db, pipeline_id, caller=f"_build_graph({pipeline_id}) after re-key"
        )
        hidden_edge_ids = set(edge_view.hidden_edge_ids)
        manual_edges_for_fn_lookup = edge_view.drawn_list
        manual_nodes = edge_view.manual_nodes

    disconnected_wirings = gb.hidden_wirings(
        agg.fn_input_params,
        agg.fn_outputs,
        agg.fn_constants,
        agg.path_inputs,
        hidden_edge_ids,
        manual_edges=manual_edges_for_fn_lookup,
        manual_nodes=manual_nodes,
        token_for=token_for,
        is_current=identity.is_current,
        fn_parameter_names=agg.fn_parameter_names,
    )
    disconnected_fkeys = gb.wiring_disconnected_fkeys(
        agg.fn_input_params,
        agg.fn_outputs,
        disconnected_wirings,
        agg.path_inputs,
        token_for=token_for,
    )
    if disconnected_wirings:
        logger.info(
            "[pipeline] %d wiring(s) disconnected: %s",
            len(disconnected_wirings),
            sorted(disconnected_wirings),
        )

    # --- Compute run states (per call site — state never blurs) ---
    # Propagation follows the edges that are VISIBLE, which includes the ones
    # the user drew; without this, red stops at the last history edge and every
    # step fed by a drawn edge reads green under a red upstream (2026-09-22).
    # The overlay goes to the DAG cascade ONLY — never to agg.fn_input_params,
    # which wiring_id hashes into node identity.
    state_input_params = gb.input_params_with_manual_edges(
        agg.fn_input_params,
        agg.fn_outputs,
        agg.fn_constants,
        agg.path_inputs,
        manual_edges_for_fn_lookup,
        token_for,
        manual_nodes=manual_nodes,
        hidden_edge_ids=hidden_edge_ids,
    )
    # Run states are the most expensive part of a build (~6 s of 11 s on a
    # real 843-invocation database, cleanup-audit F15): every call site's
    # expected invocations are predicted from current data. A caller that
    # draws no state — the Plot Studio pickers — asks for the graph without
    # them, and gets nodes with NO run_state rather than a guessed one.
    with_run_states = run_states
    if with_run_states:
        logger.info("[pipeline] Computing run states (delegating to run_state)")
        run_states = _compute_run_states(
            db,
            agg.fn_input_params,
            agg.fn_outputs,
            disconnected_fkeys,
            propagation_input_params=state_input_params,
            hidden_fkeys=gb.hidden_fn_fkeys(
                agg.fn_input_params,
                agg.fn_outputs,
                agg.path_inputs,
                hidden_ids,
                token_for,
            ),
            unproduced_outputs=gb.unproduced_output_fkeys(
                agg.fn_input_params,
                agg.fn_outputs,
                agg.path_inputs,
                manual_edges_for_fn_lookup,
                token_for,
                manual_nodes=manual_nodes,
            ),
        )
        logger.info("[pipeline] computed run states for %d nodes", len(run_states))
    else:
        logger.info("[pipeline] run states not requested — skipping the state check")
        run_states = {}

    # --- Group call sites by wiring (one canvas node per fn + IO shape) ---
    # Constant-value call sites become variant rows (with their own state
    # chips) inside one node; staged pending values get synthesized rows.
    # The per-call-site aggregate, before grouping: canvas adoption reads the
    # recorded steps from it (services/adoption.gui_recorded_wirings).
    history_agg = agg
    logger.info("[pipeline] Grouping call sites by wiring")
    agg, run_states, wiring_member_map = gb.group_call_sites_by_wiring(
        agg,
        run_states,
        pending_constants,
        manual_edges=manual_edges_for_fn_lookup,
        manual_nodes=manual_nodes,
        hidden_edge_ids=hidden_edge_ids,
        hidden_node_ids=hidden_ids,
        token_for=token_for,
        is_current=identity.is_current,
    )
    # The ONE hidden-node filter, for display — identity is fixed by now.
    # Grouped keys are (fn, node token), which is what a hidden function
    # node's id parses to, so hiding a function node matches here.
    logger.info("[pipeline] Filtering hidden nodes")
    gb.filter_hidden(agg, hidden_ids)

    # --- Build fn_params_map and saved_configs ---
    # fn_params_map and saved_configs are keyed by fn_name (the signature
    # and saved settings don't vary across call sites).
    logger.info("[pipeline] Building function parameter maps and saved configs")
    fn_names = {fn for fn, _ in agg.fn_input_params.keys()}
    logger.debug(
        "[pipeline] building parameter maps for %d unique function(s)", len(fn_names)
    )
    fn_params_map: dict[str, list[str]] = {
        fn: _fn_params_from_registry(fn) for fn in fn_names
    }

    # Config keyed by node_id -- the authoritative store, and the only one that
    # can hold a setting for a node that has already run (a DB-derived node has
    # no _pipeline_nodes row at all). Passed alongside the fn_name-keyed map
    # below, which stays as the fallback for manual nodes.
    node_configs = _ps.get_node_configs(db, pipeline_id)

    saved_configs: dict[str, dict | None] = {}
    for fn in fn_names:
        # Only the legacy pre-call-id `fn__{fn}` node. Until 2026-10-22 this
        # also took the config of ANY hand-placed node with the same label,
        # so a fresh node's settings showed on the history node it would
        # graduate into. Function nodes no longer graduate (canvas-as-document
        # Stage 5), so that fallback put one node's settings on another.
        # A node's settings are keyed by its own id (node_configs).
        saved_configs[fn] = node_configs.get(legacy_fn_node_id(fn)) or manual_nodes.get(
            legacy_fn_node_id(fn), {}
        ).get("config")

    matlab_functions = set(_mr.get_all_function_names())
    matlab_output_order = {
        name: _mr.get_matlab_function(name).output_names for name in matlab_functions
    }

    # Build matlab_param_to_class from DB variants' output_num and, as a
    # fallback for ungraduated fns with no DB history yet, from persisted
    # manual edges.
    # Source 1: DB variants' output_num → the fn's signature output name.
    from_db = _matlab_param_to_class_from_db(
        scidb_agg["functions"], matlab_functions, matlab_output_order
    )
    matlab_param_to_class: dict[str, dict[str, str]] = {
        fn: dict(mapping) for fn, mapping in from_db.items()
    }

    from scistack_gui.domain.edge_resolver import infer_manual_fn_param_to_class
    from scistack_gui.ids import fn_node_id

    existing_node_labels_pre = {var_node_id(t): t for t in agg.all_var_types}
    for fn in matlab_functions:
        # Collect all DB-derived node IDs for this fn (one per call site)
        # plus any manual nodes that share the label.
        fn_ids = {
            fn_node_id(fn_name, cid)
            for (fn_name, cid) in agg.fn_input_params.keys()
            if fn_name == fn
        }
        fn_ids |= {
            nid
            for nid, meta in manual_nodes.items()
            if meta.get("type") == "functionNode" and meta.get("label") == fn
        }
        edge_map = infer_manual_fn_param_to_class(
            fn_node_ids=fn_ids,
            manual_edges=manual_edges_for_fn_lookup,
            manual_nodes=manual_nodes,
            existing_node_labels=existing_node_labels_pre,
        )
        if edge_map:
            existing = matlab_param_to_class.setdefault(fn, {})
            for p, c in edge_map.items():
                existing.setdefault(p, c)
        # INFO, not DEBUG, and attributed per source. An empty merged map is
        # the precondition for the fn↔output-variable edge disappearing from
        # the canvas, and at DEBUG the log showed only the empty result with
        # no way to tell which of the two sources failed.
        if fn in matlab_param_to_class or edge_map or fn in from_db:
            logger.info(
                "[pipeline] matlab_param_to_class: fn=%s from_db_output_num=%s "
                "from_manual_edges=%s merged=%s",
                fn,
                from_db.get(fn, {}),
                edge_map,
                matlab_param_to_class.get(fn, {}),
            )

    # --- Load sweeps (source-scanned — see docs/claude/code-discovery-categories.md).
    # "Delete" only hides the node (layout_service.delete_parameter) — the source
    # declaration is never touched — so this must filter by hidden_ids
    # explicitly; unlike var__/pathInput__ nodes, a Parameter's declared
    # values have no DB-derived aggregation path that filter_hidden covers.
    source_parameters = {
        name: p
        for name, p in registry.get_parameters_registry().items()
        if f"{ids.PARAM_ID_PREFIX}{name}" not in hidden_ids
    }
    logger.debug(
        "[pipeline] loaded %d parameter(s) from registry", len(source_parameters)
    )

    # --- Build nodes (pure) ---
    logger.info("[pipeline] Building nodes (delegating to graph_builder)")
    # Which variant each Variable node shows: the popup's "current" card(s)
    # (scidb _mark_defaults), never re-derived here.
    from scistack_gui.services.variant_cards_service import canvas_variants

    t_variants = time.perf_counter()
    node_variants = canvas_variants(
        db, [v for v in agg.all_var_types if record_counts.get(v, 0) > 0], record_counts
    )
    logger.info(
        "[pipeline] canvas_variants: %d variable(s) in %.1f ms",
        len(node_variants),
        (time.perf_counter() - t_variants) * 1000.0,
    )
    nodes = gb.build_variable_nodes(agg.all_var_types, record_counts, run_states, node_variants)
    var_node_count = len(nodes)
    hidden_const_values: dict[str, set] = {}
    for row in _ps.list_hidden_parameter_values(db, pipeline_id):
        hidden_const_values.setdefault(row["const_name"], set()).add(row["value"])
    # Constants and Sweeps are ONE node kind (Parameters, D6) — built
    # together so a Parameter never changes node type or id when a second
    # value turns its declaration from a Constant into a Sweep.
    entities_file = (
        str(registry._config.entities_file)
        if registry._config is not None and registry._config.entities_file is not None
        else None
    )
    nodes += gb.build_parameter_nodes(
        agg.const_counts,
        pending_constants,
        source_parameters,
        hidden_const_values,
        _ps.get_parameter_value_groups(db),
        entities_file,
        # A hidden Parameter stays off the canvas: its stated values must not
        # bring its node back (the same rule source_parameters follows above).
        stated_values={
            name: values
            for name, values in stated_constants.items()
            if f"{ids.PARAM_ID_PREFIX}{name}" not in hidden_ids
        },
    )
    const_node_count = len(nodes) - var_node_count
    nodes += gb.build_path_input_nodes(agg.path_inputs)
    path_input_node_count = len(nodes) - var_node_count - const_node_count
    sweep_node_count = 0
    nodes += gb.build_function_nodes(
        agg.fn_input_params,
        agg.fn_outputs,
        agg.fn_constants,
        agg.fn_variants_map,
        fn_params_map,
        run_states,
        matlab_functions,
        saved_configs,
        matlab_output_order=matlab_output_order,
        matlab_param_to_class=matlab_param_to_class,
        node_configs=node_configs,
    )
    fn_node_count = (
        len(nodes)
        - var_node_count
        - const_node_count
        - path_input_node_count
        - sweep_node_count
    )
    logger.info(
        "[pipeline] built %d nodes: %d variable, %d constant, %d path input, "
        "%d sweep, %d function",
        len(nodes),
        var_node_count,
        const_node_count,
        path_input_node_count,
        sweep_node_count,
        fn_node_count,
    )

    # Inputs drawn onto a node are shown from its stored edges, after the
    # document view (gb.document_input_params). The overlay that patched a
    # history node with the edges drawn onto it is gone (canvas-as-document
    # Stage 5): there is one source for an input, its edge.

    # --- Tag disconnected function nodes ---
    # By this point agg is grouped by NODE, so a function node id's trailing
    # segment is the same token `hidden_wirings` was given `token_for` for —
    # directly comparable to disconnected_wirings with no further translation.
    # Visual/state only (see run_state above
    # for the actual color); execution_service enforces un-runnability
    # independently at run time.
    if disconnected_wirings:
        tagged = 0
        for node in nodes:
            if node["type"] != "functionNode":
                continue
            parsed = ids.parse_fn_node_id(node["id"])
            if parsed is not None and parsed in disconnected_wirings:
                node["data"]["disconnected"] = True
                tagged += 1
        logger.debug("[pipeline] tagged %d function node(s) disconnected", tagged)

    # --- Build edges (pure) ---
    logger.info("[pipeline] Building edges (delegating to graph_builder)")
    manual_edges_list = manual_edges_for_fn_lookup
    edges = gb.build_edges(
        agg.fn_input_params,
        agg.fn_outputs,
        agg.const_fns,
        agg.path_inputs,
        manual_edges_list,
        hidden_ids,
        matlab_param_to_class=matlab_param_to_class,
        hidden_edge_ids=hidden_edge_ids,
        fn_parameter_names=agg.fn_parameter_names,
        fn_glue_chains=agg.fn_glue_chains,
        manual_nodes=manual_nodes,
    )
    logger.info("[pipeline] built %d edges", len(edges))

    # --- Merge manual nodes ---
    # pipelineNode entries are NOT generic manual nodes: they are built by
    # scope_service (ports + binding) after filtering, so exclude them here.
    logger.info("[pipeline] Merging manual nodes (delegating to graph_builder)")
    positions_by_scope = layout_store.read_positions_by_scope()
    saved_positions: dict = {}
    for _scope_positions_map in positions_by_scope.values():
        saved_positions.update(_scope_positions_map)
    logger.debug(
        "[pipeline] loaded %d saved position(s) across %d scope(s)",
        len(saved_positions),
        len(positions_by_scope),
    )
    # Hidden rows are not drawn. A removed node keeps its stored row (never
    # delete; layout.delete_node only hides it), so the hide is what takes it
    # off this canvas.
    hidden_rows = sorted(
        nid
        for nid in manual_nodes
        if nid in hidden_ids or ids.strip_placement(nid) in hidden_ids
    )
    if hidden_rows:
        logger.debug("[pipeline] %d hidden stored row(s) not drawn: %s", len(hidden_rows), hidden_rows)
    mergeable_manual_nodes = {
        nid: meta
        for nid, meta in manual_nodes.items()
        if meta.get("type") != "pipelineNode" and nid not in hidden_rows
    }
    to_add, graduations = gb.merge_manual_nodes(
        nodes, mergeable_manual_nodes, saved_positions
    )

    # Function nodes no longer graduate (plan-canvas-as-document Stage 5):
    # `merge_manual_nodes` folds entity boxes only. The wiring-matching passes
    # that used to sit here (reject a label match whose wiring conflicts,
    # promote a unique wiring match, prefer the graduation with wiring
    # evidence) existed to decide which same-named history node a hand-placed
    # function node "really" was. A node that ran claims its wiring at
    # dispatch, so it IS that history node under its own id, and nothing is
    # left to decide.
    existing_node_labels = {n["id"]: n["data"]["label"] for n in nodes}

    # Collision guard, entities only: two hand-placed boxes of one entity can
    # both resolve to the same canonical node. One folds; the others stay
    # separate boxes (today's behaviour, user decision D3) rather than being
    # deleted with nothing left to show for them.
    by_target: dict[str, list[gb.GraduationAction]] = {}
    for action in graduations:
        by_target.setdefault(action.new_id, []).append(action)
    deduped_graduations = []
    for target_id, actions in by_target.items():
        winner, losers = actions[0], actions[1:]
        deduped_graduations.append(winner)
        for loser in losers:
            logger.warning(
                "[pipeline] fold collision on target %s: %s and %s both fold "
                "into it — keeping %s, %s stays a separate box",
                target_id,
                winner.old_id,
                loser.old_id,
                winner.old_id,
                loser.old_id,
            )
            to_add.append(loser.old_id)
    graduations = deduped_graduations

    # Execute graduation side effects.
    logger.info("[pipeline] Executing %d graduation action(s)", len(graduations))
    for action in graduations:
        layout_store.graduate_manual_node(
            action.old_id, action.new_id, take_old_position=action.take_old_position
        )
        logger.debug(
            "[pipeline] graduated manual node: %s -> %s", action.old_id, action.new_id
        )
        # graduate_manual_node rewrites edge endpoints in the DB
        # (pipeline_store.rename_edge_endpoints), but `edges` here was
        # already built earlier in this same call — patch it in-memory too
        # so this response isn't missing edges that just got graduated.
        for e in edges:
            if e["source"] == action.old_id:
                e["source"] = action.new_id
            if e["target"] == action.old_id:
                e["target"] = action.new_id
        # Same in-memory patch for the node's config: build_function_nodes
        # and apply_placement_configs read the pre-graduation node_configs
        # snapshot, so without this the first response after a graduation
        # shows defaults and every toggle looks reset until the next
        # rebuild (pipeline_store.migrate_node_config moved the row).
        # The snapshot must also FORGET the fresh id, or apply_placement_
        # configs' orphan check (which reads this dict, not the DB) names a
        # row that was just moved.
        old_bare = ids.strip_placement(action.old_id)
        for stale_key in [k for k in node_configs if ids.strip_placement(k) == old_bare]:
            node_configs.pop(stale_key, None)
        moved_cfg = _ps.get_node_config(db, action.new_id)
        if moved_cfg:
            node_configs[action.new_id] = moved_cfg
            target_node = next(
                (n for n in nodes if n["id"] == action.new_id), None
            ) or next(
                (n for n in nodes if n["id"] == ids.strip_placement(action.new_id)),
                None,
            )
            if target_node is not None:
                gb._apply_saved_config(target_node["data"], moved_cfg)
                logger.debug(
                    "[pipeline] applied graduated config %s onto %s in this response",
                    sorted(moved_cfg),
                    target_node["id"],
                )

    # Build and append manual nodes that should be added.
    logger.info("[pipeline] Building %d manual node(s) to add", len(to_add))
    # Never-run function nodes with a drawn output: their colour is decided
    # here, after the grouped cascade, so it is carried downstream below
    # (run_state.cascade_unrun_producers).
    unrun_producers: dict[str, tuple[str, str, dict, set]] = {}
    for node_id in to_add:
        meta = manual_nodes[node_id]
        # For function nodes, resolve edges and compute state.
        resolved_input_params = None
        resolved_output_types = None
        manual_fn_state = None
        if meta["type"] == "glueNode":
            # Handles only. A glue node has no run state to compute (D5) and
            # no output type to resolve — its output is fused into whatever
            # consumes it and is never saved, so a glue node feeding nothing
            # renders inert rather than red.
            fn_label = meta["label"]
            resolved_input_params = {
                p: "" for p in _fn_params_from_registry(fn_label)
            }
            glue_resolved = resolve_function_edges(
                fn_node_ids={node_id},
                manual_edges=manual_edges_list,
                manual_nodes=manual_nodes,
                existing_node_labels=existing_node_labels,
            )
            # Display only: handle labels are strings, so a multi-type input
            # shows its first candidate. Never an identity input.
            for p, ts in glue_resolved.input_type_candidates.items():
                if ts:
                    resolved_input_params[p] = ts[0]
        elif meta["type"] == "functionNode":
            fn_label = meta["label"]
            sig_params = _fn_params_from_registry(fn_label)
            resolved = resolve_function_edges(
                fn_node_ids={node_id},
                manual_edges=manual_edges_list,
                manual_nodes=manual_nodes,
                existing_node_labels=existing_node_labels,
            )
            # Display only (see the glue branch above): first candidate per
            # handle. Never an identity input.
            inferred_inputs = {
                p: ts[0] for p, ts in resolved.input_type_candidates.items() if ts
            }
            resolved_input_params = {p: inferred_inputs.get(p, "") for p in sig_params}
            for p, t in inferred_inputs.items():
                if p not in resolved_input_params:
                    resolved_input_params[p] = t
            resolved_output_types = resolved.output_types
            # For state computation, always use the edge-resolved output types
            # (the actual variable class names like 'XSENSLoaded'), NOT the
            # MATLAB declared output parameter names (like 'extracted_data').
            state_output_types = resolved_output_types
            # For manual MATLAB function nodes, always use the declared output
            # names from the function signature as handles. Connected edges carry
            # the actual var-label mapping via sourceHandle, so the handle set
            # must always match the full signature regardless of what's wired up.
            if _mr.is_matlab_function(fn_label):
                info = _mr.get_matlab_function(fn_label)
                resolved_output_types = list(info.output_names)
                logger.debug(
                    "manual fn %s (MATLAB): using declared output_names=%s, "
                    "edge-resolved output_types=%s",
                    fn_label,
                    resolved_output_types,
                    state_output_types,
                )
            if state_output_types:
                # The colour of the step this node STATES: the run state of
                # whichever node history attributes that wiring to, else red.
                # Until 2026-10-22 this read the state by function NAME and
                # guarded it with a wiring-conflict heuristic shared with
                # graduation; both are gone with graduation (Stage 5). A
                # wiring is the one thing that says which history is this
                # node's.
                stated_wiring = gb.wiring_id(
                    fn_label,
                    {p: t for p, t in resolved.input_types.items() if t},
                    set(state_output_types),
                    resolved.path_input_params,
                )
                owner = identity.node_by_wiring.get((fn_label, stated_wiring))
                manual_fn_state = (
                    run_states.get(str(owner)) or "red" if owner is not None else "red"
                )
                # INFO: a manual node's colour is decided here and nowhere
                # the run-state pass logs, so without this line "why is this
                # node red/green" has no answer in scidb.log.
                logger.info(
                    "[pipeline] manual fn node %s (%s): state=%s — states wiring %s "
                    "(outputs=%s, inputs=%s), %s",
                    node_id,
                    fn_label,
                    manual_fn_state,
                    stated_wiring,
                    state_output_types,
                    inferred_inputs,
                    f"recorded under node {owner}"
                    if owner is not None
                    else "which history has never run",
                )
                # A node with an owner states a wiring the grouped cascade
                # already holds, outputs included; only a never-run one is
                # missing from it.
                if owner is None:
                    unrun_producers[node_id] = (
                        fn_label,
                        manual_fn_state,
                        {p: ts for p, ts in resolved.input_type_candidates.items() if ts},
                        set(state_output_types),
                    )
            else:
                manual_fn_state = "red"
                logger.info(
                    "[pipeline] manual fn node %s (%s): state=red — no output "
                    "edge wired, so no output to look up",
                    node_id,
                    fn_label,
                )

        node = gb.build_manual_node(
            node_id,
            meta,
            # A hand-placed Parameter shows its stated values too, not just
            # the pending ones (values outlive history: plan §4).
            {
                name: set(pending_constants.get(name, set()))
                | set(stated_constants.get(name, set()))
                for name in set(pending_constants) | set(stated_constants)
            },
            manual_fn_state,
            resolved_input_params,
            resolved_output_types,
            matlab_functions,
            node_config=node_configs.get(node_id),
        )
        nodes.append(node)
        logger.debug(
            "[pipeline] built manual node: %s (type=%s, label=%s)",
            node_id,
            meta["type"],
            meta["label"],
        )

    # --- Never-run producers colour what they feed ---
    # A drawn edge from a never-run node into a variable makes that node a
    # producer of it, so its red carries to the variable and everything
    # downstream, like any history producer's would. Only lowers states.
    # Only producers on THIS canvas (scope_filter.node_scope, the membership
    # owner): until 2026-10-26 a never-run load_emg on the `load` submodule
    # reddened EmgRaw, and so filter_emg, on the `emg` canvas
    # (test_library_fork_chain: carried results drawn red after ⇪ Share).
    if with_run_states and unrun_producers:
        from scistack_gui.domain.scope_filter import node_scope

        elsewhere = {
            nid: node_scope(nid, manual_nodes, positions_by_scope)
            for nid in unrun_producers
        }
        elsewhere = {nid: s for nid, s in elsewhere.items() if s != pipeline_id}
        if elsewhere:
            logger.info(
                "[pipeline] never-run producer(s) on other canvases left out of "
                "scope %s's cascade: %s",
                pipeline_id,
                elsewhere,
            )
            unrun_producers = {
                nid: v for nid, v in unrun_producers.items() if nid not in elsewhere
            }
    if with_run_states and unrun_producers:
        from scistack_gui.domain.run_state import _STATE_ORDER, cascade_unrun_producers

        lowered = cascade_unrun_producers(
            run_states,
            gb.input_params_with_manual_edges(
                agg.fn_input_params,
                agg.fn_outputs,
                agg.fn_constants,
                agg.path_inputs,
                manual_edges_for_fn_lookup,
                token_for,
                manual_nodes=manual_nodes,
                hidden_edge_ids=hidden_edge_ids,
            ),
            gb.outputs_with_manual_edges(
                agg.fn_outputs, manual_edges_for_fn_lookup, manual_nodes
            ),
            unrun_producers,
        )
        for n in nodes:
            state = lowered.get(n["id"])
            if state is None or "run_state" not in n["data"]:
                continue
            old = n["data"]["run_state"]
            if _STATE_ORDER[state] < _STATE_ORDER.get(old, 2):
                logger.info(
                    "[pipeline] %s: %s -> %s — downstream of a never-run node",
                    n["id"],
                    old,
                    state,
                )
                n["data"]["run_state"] = state
                run_states[n["id"]] = state

    # --- Scope filtering (nested pipelines) ---
    # Graduations above MOVE positions (the scope-membership record for
    # DB-derived nodes) and delete manual-node rows; filtering on the
    # pre-graduation snapshots would place a just-graduated node in the
    # root scope (no position found) while its position actually lives in
    # a sub scope — so refresh both inputs when any graduation ran.
    if graduations:
        positions_by_scope = layout_store.read_positions_by_scope()
        manual_nodes = _ps.get_manual_nodes(db)
        logger.debug(
            "[pipeline] refreshed scope-membership inputs after %d graduation(s)",
            len(graduations),
        )

    # --- Adoption (plan-canvas-as-document Stage 2) ---
    # Whatever history would draw that the document does not store yet is
    # stored now, once, under the id it already has, so this build and every
    # later one draw the same picture whether or not history is still there.
    # Inside an undoable handler's recording window it is NOT written (it is
    # not an undo step; see services/adoption.py) and the plan is only drawn.
    from scistack_gui import history as _history
    from scistack_gui.domain.adoption import document_view
    from scistack_gui.services import adoption as _adoption

    # A named script run that rewired a step (Q6): the step's old stored
    # edges are hidden first, so adoption below adds the new wiring's edges
    # instead of drawing both wirings on one node.
    retired = _adoption.retire_rewired_edges(
        db, identity.rewired_by_step, edges, manual_edges_list, manual_nodes
    )
    if retired:
        edge_view = effective_edges(
            db, pipeline_id, caller=f"_build_graph({pipeline_id}) after a named rewire"
        )
        manual_edges_list = edge_view.drawn_list
        hidden_edge_ids = set(edge_view.hidden_edge_ids)
    adoption_plan, combo_hides = _adoption.plan_for_build(
        db,
        nodes=nodes,
        edges=edges,
        manual_nodes=manual_nodes,
        drawn_edges=manual_edges_list,
        positions_by_scope=positions_by_scope,
        agg=history_agg,
        scidb_agg=scidb_agg,
    )
    adoption_event = None
    deferred_adoption = None
    if not adoption_plan.empty or combo_hides:
        if _history.is_recording():
            deferred_adoption = adoption_plan
            logger.info(
                "[pipeline] adoption deferred (inside an undoable edit's recording "
                "window; the next plain build stores it): %s",
                adoption_plan.summary(),
            )
        else:
            adoption_event = _adoption.write(db, adoption_plan, combo_hides)
            manual_nodes = _ps.get_manual_nodes(db)
            edge_view = effective_edges(
                db, pipeline_id, caller=f"_build_graph({pipeline_id}) after adoption"
            )
            manual_edges_list = edge_view.drawn_list

    # --- The document view (Stage 3) ---
    # Only stored rows are drawn. History annotates the nodes kept (run
    # state, counts, variant rows), joined by id; it no longer decides what
    # exists. This also replaces the duplicate-edge cleanup graduation needed
    # (drop_superseded_manual_edges): one edge per connection survives.
    nodes, edges, document_counts = document_view(
        nodes,
        edges,
        manual_nodes=manual_nodes,
        drawn_edges=manual_edges_list,
        pending=deferred_adoption,
    )
    logger.info("[pipeline] document view scope=%s: %s", pipeline_id, document_counts)
    inputs_changed = gb.document_input_params(
        nodes,
        manual_edges_list + (deferred_adoption.edges if deferred_adoption else []),
        manual_nodes,
        existing_node_labels,
    )
    if inputs_changed:
        logger.info(
            "[pipeline] %d function node(s) show inputs from their stored edges "
            "that differ from what history recorded",
            inputs_changed,
        )

    logger.info("[pipeline] Filtering graph to scope %s", pipeline_id)
    from scistack_gui.domain.scope_filter import resolve_scope_view
    from scistack_gui.services.scope_service import build_pipeline_nodes

    nodes, edges = resolve_scope_view(
        nodes, edges, pipeline_id, manual_nodes, positions_by_scope
    )
    # Config saved under a placement-qualified id (``...::scope``) is only
    # findable once the ids have been resolved -- build_function_nodes above
    # ran on bare ids. See gb.apply_placement_configs.
    applied = gb.apply_placement_configs(nodes, node_configs)
    if applied:
        logger.info(
            "[pipeline] rehydrated placement-qualified config on %d node(s)", applied
        )

    # Intent the last run did not use (decision A: a script run ignores the
    # GUI's statements) — one batched provenance query for every function
    # with a saved selection, then a pure pass over the nodes.
    fn_with_selections = {
        n["data"].get("label")
        for n in nodes
        if n.get("type") == "functionNode" and n["data"].get("columnSelections")
    }
    if fn_with_selections:
        from scidb import provenance_query as _pq

        try:
            latest = _pq.latest_runs(db._duck, fn_with_selections)
        except Exception:  # a marker must never break the graph build
            logger.warning("[pipeline] latest_runs lookup failed", exc_info=True)
            latest = {}
        marked = gb.mark_unused_intent(nodes, latest)
        if marked:
            logger.info(
                "[pipeline] %d node(s) carry a column selection their last run "
                "did not use",
                marked,
            )
    nodes += build_pipeline_nodes(db, pipeline_id)

    # --- Input port order ---
    # Last, so every overlay that can add an input (manual edges onto history
    # nodes, manual and glue nodes) is already in. gb.input_port_order owns
    # the order and gb.port_visibility which ports are drawn (fed = the edges
    # this canvas draws); the frontend draws data.input_ports as is.
    arguments: dict[str, list] = {}
    for n in nodes:
        label = n["data"].get("label")
        if n["type"] in ("functionNode", "glueNode") and label not in arguments:
            arguments[label] = _fn_arguments(label)
    fed_ports: dict[str, set[str]] = {}
    for e in edges:
        arg = handle_name(e.get("targetHandle"))
        if arg:
            fed_ports.setdefault(e.get("target"), set()).add(arg)
    ported = gb.assign_input_ports(nodes, arguments, fed_ports)
    logger.info("[pipeline] assigned input port order on %d node(s)", ported)

    # --- Endpoint classification (plot_/stat_ prefixes) ---
    # Detection lives in scidb (roles.endpoint_kind — same source of truth as
    # Pipeline.endpoints()/for_each's endpoint policy); the GUI only tags.

    endpoint_count = 0
    for n in nodes:
        if n["type"] == "functionNode":
            kind = endpoint_kind(n["data"]["label"])
            if kind is not None:
                n["data"]["endpoint_kind"] = kind
                endpoint_count += 1
    if endpoint_count:
        logger.info("[pipeline] tagged %d endpoint node(s)", endpoint_count)

    logger.info("[pipeline] Graph build complete - assembling final result")
    if not with_run_states:
        # Nothing was checked, so nothing is reported: variable nodes default
        # to "green" and the grouped cascade may still derive states from
        # staged values — neither is an answer to "is this node up to date".
        for n in nodes:
            n["data"].pop("run_state", None)
    node_types = {}
    for n in nodes:
        t = n["type"]
        node_types[t] = node_types.get(t, 0) + 1
    logger.info(
        "[pipeline] graph built successfully (scope=%s): %d total nodes (%s), %d edges",
        pipeline_id,
        len(nodes),
        ", ".join(f"{c} {t}" for t, c in sorted(node_types.items())),
        len(edges),
    )
    # Which Parameter nodes, by id, and what each is wired into. A count
    # alone could not tell a duplicate apart from a second real Parameter
    # (cleanup-audit B1 had to be diagnosed from counts).
    if logger.isEnabledFor(logging.DEBUG):
        param_targets: dict[str, list[str]] = {}
        for e in edges:
            if str(e.get("source", "")).startswith(ids.PARAM_ID_PREFIX):
                param_targets.setdefault(e["source"], []).append(
                    f"{e.get('target')}.{e.get('targetHandle')}"
                )
        logger.debug(
            "[pipeline] parameter nodes: %s",
            {
                n["id"]: param_targets.get(n["id"], [])
                for n in nodes
                if n["type"] == "parameterNode"
            },
        )

    result = {"nodes": nodes, "edges": edges, "pipeline_id": pipeline_id}
    # How much of this canvas is stored rows vs. re-derived from run history
    # on every build (plan-canvas-as-document Stage 0). The stages that
    # follow drive `nodes_derived` and `edges_derived` to zero.
    from scistack_gui.domain.canvas_shape import canvas_sources

    logger.info(
        "[pipeline] canvas_sources scope=%s: %s",
        pipeline_id,
        canvas_sources(result, set(manual_nodes)),
    )
    if identity_warnings:
        # §7b: a popup, not just a log line. Two nodes stating identical
        # wiring compute identical things, so the state is almost certainly
        # unintended — and a warning buried in scidb.log is a warning nobody
        # reads. It is also recoverable (rewire one), which is why nothing is
        # merged and the message says what will happen until they differ.
        result["warnings"] = identity_warnings
    # Q1 (user, 2026-10-21): adoption is automatic, with a notification.
    notice = _adoption.notification(adoption_plan, adoption_event) if adoption_event else None
    if notice is not None:
        result.setdefault("warnings", []).append(notice)
    return result


# ---------------------------------------------------------------------------
# The handler table — both transports (api/handlers.py)
# ---------------------------------------------------------------------------
#
#     GET    /api/pipeline                                   get_pipeline
#     GET    /api/function/{name}/params                     get_function_params
#     GET    /api/function/{name}/source                     get_function_source
#     GET    /api/function/{name}/doc                        get_function_doc
#     PUT    /api/parameters/{name}/pending/{value}          put_pending_constant
#     DELETE /api/parameters/{name}/pending/{value}          delete_pending_constant
#     POST   /api/functions/{function_name}/hidden_combos    hide_combo
#     DELETE /api/functions/hidden_combos/{node_id}          unhide_combo
#     GET    /api/functions/{function_name}/hidden_combos    list_hidden_combos
#     POST   /api/parameters/{name}/hidden_values/{value}    hide_parameter_value
#     DELETE /api/parameters/{name}/hidden_values/{value}    unhide_parameter_value
#     POST   /api/parameters/{name}/group_checked            set_parameter_group_checked
#     GET    /api/parameters/hidden_values                   list_hidden_parameter_values


class ScopeQuery(BaseModel):
    pipeline_id: str | None = ROOT_SCOPE


class PipelineQuery(ScopeQuery):
    #: False = the graph without per-node run states (the Plot Studio
    #: pickers draw none; the state check is most of a build's cost).
    run_states: bool = True


class FunctionName(BaseModel):
    name: str


class PendingValue(BaseModel):
    name: str
    value: str


class HideComboRequest(BaseModel):
    function_name: str
    node_id: str | None = None
    variant_key: dict


class NodeRef(BaseModel):
    node_id: str


class FunctionRef(BaseModel):
    function_name: str


class ParameterValueInScope(BaseModel):
    name: str
    value: str
    pipeline_id: str | None = ROOT_SCOPE


class ParameterGroupChecked(BaseModel):
    name: str
    values: list[str] = []
    checked: bool = True
    pipeline_id: str | None = ROOT_SCOPE


def _get_pipeline(db, req: PipelineQuery) -> dict:
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    return get_pipeline_graph(
        db, req.pipeline_id or ROOT_SCOPE, run_states=req.run_states
    )


def _get_function_params(req: FunctionName) -> dict:
    from scistack_gui.services.pipeline_service import get_function_full_info

    return get_function_full_info(req.name)


def _get_function_source(req: FunctionName) -> dict:
    from scistack_gui.services.pipeline_service import get_function_source

    return get_function_source(req.name)


def _get_function_doc(req: FunctionName) -> dict:
    from scistack_gui.services.pipeline_service import get_function_doc

    return get_function_doc(req.name)


def _put_pending_constant(db, req: PendingValue) -> dict:
    from scistack_gui.services.layout_service import put_pending_constant

    put_pending_constant(db, req.name, req.value)
    return {"ok": True}


def _delete_pending_constant(db, req: PendingValue) -> dict:
    from scistack_gui.services.layout_service import delete_pending_constant

    delete_pending_constant(db, req.name, req.value)
    return {"ok": True}


def _hide_combo(db, req: HideComboRequest) -> dict:
    from scistack_gui.services.layout_service import hide_variant_combo

    return hide_variant_combo(db, req.function_name, req.node_id, req.variant_key)


def _unhide_combo(db, req: NodeRef) -> dict:
    from scistack_gui.services.layout_service import unhide_variant_combo

    return unhide_variant_combo(db, req.node_id)


def _list_hidden_combos(db, req: FunctionRef) -> list:
    from scistack_gui.services.layout_service import get_hidden_combos

    return get_hidden_combos(db, req.function_name)


def _hide_parameter_value(db, req: ParameterValueInScope) -> dict:
    from scistack_gui.services.layout_service import hide_parameter_value

    return hide_parameter_value(db, req.name, req.value, req.pipeline_id or ROOT_SCOPE)


def _unhide_parameter_value(db, req: ParameterValueInScope) -> dict:
    from scistack_gui.services.layout_service import unhide_parameter_value

    return unhide_parameter_value(db, req.name, req.value, req.pipeline_id or ROOT_SCOPE)


def _set_parameter_group_checked(db, req: ParameterGroupChecked) -> dict:
    """Check/uncheck every member of a generated value set in one call —
    the per-value method would be one request per value."""
    from scistack_gui.services.layout_service import set_parameter_group_checked

    return set_parameter_group_checked(
        db, req.name, req.values, req.checked, req.pipeline_id or ROOT_SCOPE
    )


def _list_hidden_parameter_values(db, req: ScopeQuery) -> dict:
    from scistack_gui.services.layout_service import get_hidden_constant_values

    return get_hidden_constant_values(db, req.pipeline_id or ROOT_SCOPE)


PIPELINE_HANDLERS: tuple[Handler, ...] = (
    Handler("get_pipeline", "/pipeline", PipelineQuery, _get_pipeline, http_method="GET"),
    Handler("get_function_params", "/function/{name}/params", FunctionName, _get_function_params, needs_db=False, http_method="GET"),
    Handler("get_function_source", "/function/{name}/source", FunctionName, _get_function_source, needs_db=False, http_method="GET"),
    Handler("get_function_doc", "/function/{name}/doc", FunctionName, _get_function_doc, needs_db=False, http_method="GET"),
    Handler("put_pending_constant", "/parameters/{name}/pending/{value}", PendingValue, _put_pending_constant, http_method="PUT", notify_dag_updated=True, undoable=True, undo_label="add value"),
    Handler("delete_pending_constant", "/parameters/{name}/pending/{value}", PendingValue, _delete_pending_constant, http_method="DELETE", notify_dag_updated=True, undoable=True, undo_label="remove value"),
    Handler("hide_combo", "/functions/{function_name}/hidden_combos", HideComboRequest, _hide_combo, undoable=True, undo_label="hide combination"),
    Handler("unhide_combo", "/functions/hidden_combos/{node_id}", NodeRef, _unhide_combo, http_method="DELETE", undoable=True, undo_label="show combination"),
    Handler("list_hidden_combos", "/functions/{function_name}/hidden_combos", FunctionRef, _list_hidden_combos, http_method="GET"),
    Handler("hide_parameter_value", "/parameters/{name}/hidden_values/{value}", ParameterValueInScope, _hide_parameter_value, body=True, undoable=True, undo_label="hide value"),
    Handler("unhide_parameter_value", "/parameters/{name}/hidden_values/{value}", ParameterValueInScope, _unhide_parameter_value, http_method="DELETE", body=True, undoable=True, undo_label="show value"),
    Handler("set_parameter_group_checked", "/parameters/{name}/group_checked", ParameterGroupChecked, _set_parameter_group_checked, undoable=True, undo_label="check value group"),
    Handler("list_hidden_parameter_values", "/parameters/hidden_values", ScopeQuery, _list_hidden_parameter_values, http_method="GET"),
)

install_routes(router, PIPELINE_HANDLERS)
