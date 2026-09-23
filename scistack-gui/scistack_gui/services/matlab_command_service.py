"""
MATLAB command service — generates ready-to-paste MATLAB commands.

Extracts the orchestration logic from server.py's _h_generate_matlab_command.
"""

from __future__ import annotations

import logging
import sys

from scidb.foreach_config import RunOptions

logger = logging.getLogger(__name__)


def _entities_script() -> "str | None":
    """The configured legacy ``[matlab] entities_file`` script, if any — its
    declarations have to be in scope before a generated command references
    one by name."""
    from scistack_gui import matlab_registry

    config = getattr(matlab_registry, "_config", None)
    entities = getattr(config, "matlab_entities_file", None)
    return str(entities) if entities else None


def _entities_file() -> "str | None":
    """The configured TOML ``entities_file``, if any.

    Read from the *Python* registry's config rather than
    ``matlab_registry``'s: the entities file is language-neutral now, so a
    MATLAB-only project still has one, and the two registries hold the same
    ``SciStackConfig`` anyway.
    """
    from scistack_gui import registry

    config = getattr(registry, "_config", None)
    entities = getattr(config, "entities_file", None)
    return str(entities) if entities else None


def _multi_type_params(target: dict) -> list[str]:
    """Params of *target* bound to more than one variable type (EachOf).

    A target's ``input_types`` is already in
    ``edge_resolver.variable_types_view``'s shape — bare for one type, a
    list only for a genuine multi-type input — for history AND never-run
    targets alike. So a list here always means "several candidates", never
    "one candidate spelled as a list"; there is nothing to flatten.
    """
    return sorted(
        param
        for param, type_val in (target.get("input_types") or {}).items()
        if isinstance(type_val, (list, tuple))
    )


def _matlab_runnable_targets(
    targets: list[dict], fn_label: str
) -> "tuple[list[dict], list[str]]":
    """The targets a MATLAB script can express, and a warning per skipped one.

    The generator has no ``EachOf(...)`` support, so a multi-type input has
    no single MATLAB-safe value; that target is skipped instead of guessing a
    type. The ONE rule for both MATLAB routes — the single-node Run
    (``scope_variants_to_node``) and the pipeline Run
    (``generate_matlab_pipeline_command``). Until 2026-09-23 only the
    pipeline route applied it (as ``_normalize_input_types``, which also
    flattened one-item lists the producer should never have made), so a
    never-run node Run from its own button crashed with
    ``unhashable type: 'list'`` (grSides).

    Returns ``(runnable_targets, warnings)``.
    """
    runnable: list[dict] = []
    warnings: list[str] = []
    for target in targets:
        multi = _multi_type_params(target)
        if multi:
            warnings.append(
                f"'{fn_label}': param(s) {multi} have more than one candidate "
                "producer type — target skipped (MATLAB generation doesn't "
                "support EachOf-style multi-type inputs)"
            )
            continue
        runnable.append(target)
    return runnable, warnings


def _sort_inferred_by_params_order(
    inferred: list[str], params_types: list[str]
) -> list[str]:
    """Sort edge-inferred class names to match the function signature order.

    ``inferred`` contains BaseVariable class names (e.g. ``["Force_Right", "Time"]``).
    ``params_types`` contains MATLAB output parameter names in signature order
    (e.g. ``["time", "force_right"]``).  Both are normalized to lowercase with
    underscores removed before matching so that ``"Force_Right"`` matches
    ``"force_right"`` and ``"Time"`` matches ``"time"``.

    Inferred types that cannot be matched to any param name are appended at the end.
    """

    def normalize(s: str) -> str:
        return s.lower().replace("_", "")

    norm_params = [normalize(p) for p in params_types]
    norm_to_class = {normalize(c): c for c in inferred}

    ordered: list[str] = []
    used: set[str] = set()
    for norm_p in norm_params:
        cls = norm_to_class.get(norm_p)
        if cls and cls not in used:
            ordered.append(cls)
            used.add(cls)

    for cls in inferred:
        if cls not in used:
            ordered.append(cls)

    return ordered


def _fn_node_ids(function_name: str, manual_edges: list[dict], manual_nodes: dict):
    """Every canvas node id that represents *function_name* — the legacy
    ``fn__{name}`` form, manual nodes carrying it as their label, and any
    edge endpoint whose bare id is ``fn__{name}`` or ``fn__{name}__{suffix}``
    (a wiring id, a manual suffix, or either with a ``::{scope}`` placement).
    """
    from scistack_gui.ids import fn_nodes_prefix, legacy_fn_node_id, strip_placement

    legacy = legacy_fn_node_id(function_name)
    ids = {legacy}
    for nid, meta in manual_nodes.items():
        if meta.get("type") == "functionNode" and meta.get("label") == function_name:
            ids.add(nid)
    for edge in manual_edges:
        for endpoint in (edge.get("source"), edge.get("target")):
            if not endpoint or endpoint in ids:
                continue
            bare = strip_placement(endpoint)
            if bare == legacy or bare.startswith(fn_nodes_prefix(function_name)):
                ids.add(endpoint)
    return ids


def _resolve_matlab_wiring(function_name: str, manual_edges: list[dict], manual_nodes: dict):
    """``ResolvedEdges`` for *function_name*'s canvas wiring.

    The MATLAB command generators used to hand-roll this edge scan twice
    (once here per function, once in the pipeline generator), which is how
    they and the Python execution path drifted apart — they read the
    ``in__{param}`` handle to map a PathInput to the parameter it fills,
    while ``build_run_inputs`` matched by name and could not. One resolver
    now serves both (``feedback_avoid_scifor_scidb_duplication``).
    """
    from scistack_gui.domain.edge_resolver import resolve_function_edges

    return resolve_function_edges(
        fn_node_ids=_fn_node_ids(function_name, manual_edges, manual_nodes),
        manual_edges=manual_edges,
        manual_nodes=manual_nodes,
        existing_node_labels={},
    )


def _collect_sweep_params(
    function_name: str, saved_sweeps: dict, manual_edges: list[dict], manual_nodes: dict
) -> dict[str, list]:
    """``{param_name: [values]}`` for every Parameter node wired into
    *function_name*.

    Unlike PathInput, a Parameter has no DB-history representation at all —
    it always fans out to ``EachOf`` fresh at execution time, never staged as
    one recorded value (see docs/claude/code-discovery-categories.md) — so
    this only ever has ONE source (registry + edge), not the two-source
    DB-variants-then-edges resolution ``path_input_params`` needs. Shared
    between ``generate_matlab_command`` (single function) and
    ``generate_matlab_pipeline_command`` (whole pipeline, called per node).

    Keyed by parameter name, looked up by declared name — the two differ
    whenever a Parameter feeds a parameter of another name.
    """
    resolved = _resolve_matlab_wiring(function_name, manual_edges, manual_nodes)
    return {
        param_name: saved_sweeps[decl_name]
        for param_name, decl_name in resolved.parameter_params.items()
        if decl_name in saved_sweeps
    }


def _collect_parameter_names(
    function_name: str, manual_edges: list[dict], manual_nodes: dict
) -> dict[str, str]:
    """``{param_name: declared Parameter name}`` for every Parameter node wired
    into *function_name* — the generated call's ``parameter_names=``, so the
    run records the Parameter the canvas shows (cleanup-audit B1). Every
    binding, valued or not: the name is a fact about the wiring, the values
    are ``_collect_sweep_params``' business.
    """
    resolved = _resolve_matlab_wiring(function_name, manual_edges, manual_nodes)
    names = dict(resolved.parameter_params)
    if names:
        logger.info(
            "generate_matlab_command: %s: parameter_names %s",
            function_name,
            names,
        )
    return names


def _collect_variable_inputs(
    function_name: str, manual_edges: list[dict], manual_nodes: dict
) -> dict[str, list[str]]:
    """``{param_name: [variable type names]}`` for every Variable node wired
    into *function_name*.

    The third binding kind, and the one this module had no collector for. A
    function with DB history got its variable inputs from each variant's
    ``input_types``, which hid the gap; a function that had never run got
    ``path_inputs`` and ``sweeps`` only, and every variable-fed parameter was
    silently missing from the emitted ``for_each`` struct — which MATLAB then
    filled by shifting the remaining arguments up (2026-09-02:
    ``filterDelsys(loaded_data, config, Fs)`` ran with the sampling frequency
    in ``loaded_data`` and nothing in ``Fs``).

    Reads the same ``ResolvedEdges`` the PathInput and Parameter collectors
    read, so all three kinds now come from one scan of the wiring — the
    Python run path (``execution_service.build_run_inputs``) has always
    consumed all three from that dict.
    """
    resolved = _resolve_matlab_wiring(function_name, manual_edges, manual_nodes)
    return dict(resolved.input_type_candidates)


def _collect_glue_chains(
    function_name: str, manual_edges: list[dict], manual_nodes: dict
) -> dict[str, list[dict]]:
    """``{param_name: [{"name", "language", "source_file"}, ...]}`` for every
    glue chain wired into *function_name*, in application order.

    Serializable on purpose: ``execution_service.build_run_glue`` produces live
    ``GlueSpec`` objects for an in-process Python run, but a MATLAB run crosses
    a generated script, so the chain has to survive as text. The language and
    the file are both load-bearing at the far end —
    ``+scidb/for_each.m:build_glue_chains`` emits a handle for MATLAB glue and
    a struct for Python glue, and ``scimatlab.bridge`` loads the Python body
    from the file because MATLAB's interpreter has no function registry.

    Reads the same ``ResolvedEdges`` the PathInput, Parameter and Variable
    collectors read — one scan of the wiring for all four kinds.
    """
    resolved = _resolve_matlab_wiring(function_name, manual_edges, manual_nodes)
    if not resolved.glue_chains:
        return {}

    from scistack_gui.services.glue_service import list_glue_nodes

    by_name = {entry["name"]: entry for entry in list_glue_nodes()}

    out: dict[str, list[dict]] = {}
    for param, names in resolved.glue_chains.items():
        chain = []
        for name in names:
            entry = by_name.get(name)
            if entry is None:
                logger.warning(
                    "generate_matlab_command: %s: parameter '%s' is wired "
                    "through glue '%s', which is not discovered in source — "
                    "the chain is dropped rather than emitted as a call that "
                    "would fail in MATLAB",
                    function_name,
                    param,
                    name,
                )
                chain = []
                break
            chain.append(
                {
                    "name": name,
                    "language": entry.get("language", "python"),
                    "source_file": str(entry.get("path") or ""),
                }
            )
        if chain:
            out[param] = chain
            _warn_on_glue_language(function_name, param, chain, resolved.bindings)
    if out:
        logger.info(
            "generate_matlab_command: %s: glue chains %s",
            function_name,
            {p: [s["name"] + f"({s['language']})" for s in c] for p, c in out.items()},
        )
    return out


def _warn_on_glue_language(
    function_name: str, param: str, chain: list[dict], bindings: dict
) -> None:
    """Warn before the run about a chain scidb will refuse, or accept.

    Two opposite rules meet here, and which one applies depends on what feeds
    the parameter (``docs/claude/free-code-glue-nodes.md`` §1):

    * a **variable**-fed chain runs where its table exists — in MATLAB, for a
      MATLAB run — so Python glue is refused by ``check_run_language``;
    * a **constant**-fed chain (a Parameter) runs in ``for_each_prepare``,
      which is Python on both run paths, so it must be Python glue and
      ``apply_constant_glue`` refuses MATLAB.

    scidb raises either way, with a message that explains itself. This exists
    only so the contradiction is visible in ``scidb.log`` *before* the user
    waits on a MATLAB launch — the canvas gives no signal today.
    """
    from scistack_gui.domain.edge_resolver import BINDING_PARAMETER

    kind = (bindings.get(param) or {}).get("kind")
    wanted = "python" if kind == BINDING_PARAMETER else "matlab"
    wrong = [s["name"] for s in chain if s["language"] != wanted]
    if not wrong:
        return
    why = (
        "a Parameter-fed chain is applied before the version keys are built, "
        "which happens in Python on every run"
        if wanted == "python"
        else "a variable-fed chain is applied to the loaded table, which in a "
        "MATLAB run exists on the MATLAB side"
    )
    logger.warning(
        "generate_matlab_command: %s: parameter '%s' is fed through glue %s, "
        "which is not %s glue — %s. scidb will refuse this run; rewrite the "
        "glue in %s or reshape the value inside the function",
        function_name,
        param,
        wrong,
        wanted,
        why,
        wanted,
    )


def _collect_edge_path_inputs(
    function_name: str, saved_pis: dict, manual_edges: list[dict], manual_nodes: dict
) -> dict[str, dict]:
    """``{param_name: {"template", "root_folder"}}`` for every PathInput node
    wired into *function_name*, resolved from the edge that names the
    parameter — never from a name coincidence."""
    resolved = _resolve_matlab_wiring(function_name, manual_edges, manual_nodes)
    return {
        param_name: {
            "template": saved_pis[decl_name].get("template", ""),
            "root_folder": saved_pis[decl_name].get("root_folder"),
        }
        for param_name, decl_name in resolved.path_input_params.items()
        if decl_name in saved_pis
    }


def scope_variants_to_node(
    fn_variants: list[dict],
    targets: list[dict],
    node_id: "str | None",
    function_name: str,
) -> list[dict]:
    """Which variant rows the generated script should actually run.

    WHICH variants is a wiring question, not a name question, and this route
    asked it by name: ``[v for v in all_variants if v["function_name"] ==
    function_name]``. That spans every wiring the function has ever had, and
    ``api.matlab_command._group_variants`` emits one ``for_each`` per distinct
    ``(input_types, constants)`` — so one click ran them all.

    Seen 2026-09-22: ``grSides`` gained a second wiring after a run through a
    manual-edge overlay, ``_group_variants: 391 variant row(s) -> 2 for_each
    call(s)``, and every later run executed the function twice — the second
    pass saving ``0 new rows`` — while holding the DuckDB lock across both,
    which then made the GUI's own refreshes fail with ``DB LOCKED``. The
    node-scoped derivation was already being computed a few lines below and
    was correct (``1 target(s)`` in the same log); only the bindings used it.

    ``targets`` is ``execution_service.derive_target_for_node``'s answer — the
    same derivation the Python run path uses, and the only one that applies
    hidden constant values and manual-edge reconciliation, so deferring to it
    closes those gaps on this route too. The Python path has drawn this
    distinction since the RawVO2/RawHeartRate bug; MATLAB never did.

    Falls back to the name-scoped list when the request names no node (a
    "run this function" request with no canvas behind it) or when the node
    derives nothing at all (never run, no edges) — there, history is still
    the best guess and refusing to run would be a regression.

    A node whose targets are ALL multi-type (see :func:`_matlab_runnable_targets`)
    raises instead of falling back: history would be another wiring's, which
    is exactly the wrong-node run this function exists to prevent.
    """
    if not node_id or not targets:
        if node_id:
            logger.info(
                "generate_matlab_command: fn=%s node %s derives no target — "
                "using %d name-scoped history row(s)",
                function_name,
                node_id,
                len(fn_variants),
            )
        return fn_variants
    runnable, warnings = _matlab_runnable_targets(targets, function_name)
    for warning in warnings:
        logger.warning("generate_matlab_command: %s", warning)
    if not runnable:
        raise ValueError(
            f"Cannot generate a MATLAB command for node {node_id}: "
            + "; ".join(warnings)
            + ". Wire exactly one variable type into each input."
        )
    # Where the rows came from matters when reading a failure: history rows
    # and edge-inferred rows (never run) reach the generator by one path.
    logger.info(
        "generate_matlab_command: fn=%s scoped to node %s — %d target(s) "
        "(%s), %d name-scoped history row(s)",
        function_name,
        node_id,
        len(runnable),
        "from history" if fn_variants else "inferred from edges, never run",
        len(fn_variants),
    )
    return runnable


def generate_matlab_command(function_name: str, db, params: dict) -> dict:
    """Generate a ready-to-paste MATLAB command for a pipeline function.

    Args:
        function_name: Name of the pipeline function.
        db: DatabaseManager instance.
        params: Full RPC params dict (schema_filter, schema_level, etc.).

    Returns:
        {"command": str} with the MATLAB command string.
    """
    from scistack_gui import layout as layout_store
    from scistack_gui import matlab_registry
    from scistack_gui import registry
    from scistack_gui.api.matlab_command import generate_matlab_command as _fmt
    from scistack_gui.domain import schema_selection as _schema_selection
    from scistack_gui.db import get_db_path
    from scistack_gui.domain.edge_resolver import infer_manual_fn_output_types
    from scistack_gui.domain.graph_builder import parse_path_input, path_input_display

    db_path = str(get_db_path())

    # Collect addpath directories from MATLAB config.
    addpath_dirs: list[str] = []
    if matlab_registry._config is not None:
        addpath_dirs = [str(p) for p in matlab_registry._config.matlab_addpath]

    # Prepend the scimatlab MATLAB package directory.
    from scistack_gui.server import _find_scimatlab_matlab_dir

    scimatlab_dir = _find_scimatlab_matlab_dir()
    if scimatlab_dir:
        addpath_dirs = [scimatlab_dir] + addpath_dirs
        logger.info(
            "generate_matlab_command: prepended scimatlab dir: %s", scimatlab_dir
        )
    else:
        logger.warning(
            "generate_matlab_command: scimatlab MATLAB directory not found; "
            "scihist.* / scidb.* may be unavailable in MATLAB"
        )

    # Variable bindings come from the ONE derivation the Python run uses
    # (execution_service.derive_*: history with its recorded selectors, the
    # node's own statements on top, manual edges reconciled) and are only
    # RENDERED here. This route used to assemble its own map from edges plus
    # the node config, so a selection recorded in history — a Python-authored
    # `Var["knee"]` step — reached a Python re-run and not a MATLAB one.
    # Node-scoped when the request names a node, name-scoped otherwise —
    # the same distinction the Python run draws.
    from scistack_gui.services.execution_service import (
        default_schema_level,
        derive_fn_targets,
        derive_target_for_node,
        variable_inputs_view,
    )

    _node_id = params.get("node_id")
    try:
        _targets = (
            derive_target_for_node(db, _node_id)
            if _node_id
            else derive_fn_targets(db, function_name)
        )
    except Exception:
        logger.warning(
            "generate_matlab_command: target derivation failed for '%s' — "
            "falling back to canvas edges alone",
            function_name,
            exc_info=True,
        )
        _targets = []

    # Resolve variants from DB history, scoped to the node that was clicked —
    # see scope_variants_to_node for why the name-only filter was wrong.
    all_variants = db.list_pipeline_variants()
    fn_variants = scope_variants_to_node(
        [v for v in all_variants if v["function_name"] == function_name],
        _targets,
        _node_id,
        function_name,
    )

    # D-2026-09-22-2, the MATLAB half. A terminal run is still a GUI-started
    # run — the node id is in the request — so the association is recorded
    # here, at dispatch, exactly as the Python route does. Only a run the GUI
    # never saw (a script, or MATLAB started by hand) is left to inference.
    #
    # Recorded AFTER scoping, and only for the targets that will actually run:
    # scope_variants_to_node refuses a node whose inputs are all multi-type,
    # and a refused run must not claim a wiring it never ran as.
    if _node_id and _targets:
        from scistack_gui.services.execution_service import record_dispatch_wirings

        record_dispatch_wirings(
            db, _node_id, function_name, fn_variants, params.get("run_id")
        )

    # Collect PathInput param mappings.
    path_input_params: dict[str, dict] = {}
    for v in fn_variants:
        for param_name, type_val in (v.get("input_types") or {}).items():
            pi = parse_path_input(str(type_val))
            if pi is not None:
                path_input_params[param_name] = pi

    # Source 2: canvas edges — for functions not yet in the DB, and as the
    # live overlay for those that are (the edge's current PathInput wins
    # over whatever template history happens to have recorded).
    saved_pis = {
        name: path_input_display(obj)
        for name, obj in registry.get_path_inputs_registry().items()
    }
    manual_edges = layout_store.read_manual_edges()
    manual_nodes = layout_store.get_manual_nodes()
    path_input_params.update(
        _collect_edge_path_inputs(function_name, saved_pis, manual_edges, manual_nodes)
    )

    # Collect Parameter mappings (registry + edges only — no DB-history
    # source, see _collect_sweep_params).
    saved_sweeps = {
        name: list(sw.alternatives)
        for name, sw in registry.get_parameters_registry().items()
    }
    sweep_params = _collect_sweep_params(
        function_name, saved_sweeps, manual_edges, manual_nodes
    )

    # Bindings come from the same `_targets` derived above, and are only
    # RENDERED here. This route used to assemble its own map from edges plus
    # the node config, so a selection recorded in history — a Python-authored
    # `Var["knee"]` step — reached a Python re-run and not a MATLAB one.
    variable_inputs = variable_inputs_view(_targets, function_name)
    if not variable_inputs:
        # No derivable target (never run, and no output wired yet): the
        # canvas edges are still the only source of what feeds what.
        variable_inputs = _collect_variable_inputs(
            function_name, manual_edges, manual_nodes
        )

    # Collect glue chains. Without this the generated script ran with the glue
    # silently dropped — the MATLAB half of the feature was built, but nothing
    # ever filled in the ``glue`` option (2026-09-10).
    glue_chains = _collect_glue_chains(function_name, manual_edges, manual_nodes)

    # Infer output types from manual edges when no DB variants exist.
    # Always prefer edge inference over params-supplied output_types for
    # functions with no DB history — the node's output_types field may contain
    # MATLAB function output parameter names (e.g. "time") rather than the
    # BaseVariable class names (e.g. "Time") for first-run MATLAB functions.
    # Edge inference gives correct class names but in arbitrary edge order; we
    # re-sort them to match the function signature order from params.
    output_types: list[str] = params.get("output_types") or []
    if not fn_variants:
        inferred = infer_manual_fn_output_types(
            _fn_node_ids(function_name, manual_edges, manual_nodes),
            manual_edges,
            manual_nodes,
            existing_node_labels={},
        )
        if inferred:
            # Re-order inferred class names to match the function parameter order
            # from params.output_types (which has the correct signature order but
            # may use lowercase MATLAB param names instead of class names).
            params_output_types = params.get("output_types") or []
            if params_output_types:
                inferred = _sort_inferred_by_params_order(inferred, params_output_types)
            logger.info(
                "generate_matlab_command: inferred output_types=%s from manual edges "
                "(overrides params output_types=%s)",
                inferred,
                output_types,
            )
            output_types = inferred
        elif not output_types:
            logger.warning(
                "generate_matlab_command: no DB variants and no edge-inferred outputs "
                "for '%s' — outputs will be empty",
                function_name,
            )

    # Resolve the project root so the generated script can pin it as the
    # resolution base for rootless PathInput templates (MATLAB's cwd is a temp
    # dir, so cwd-relative resolution would be wrong). It is stated to scifor,
    # NOT written into each PathInput's root_folder — that rewrote the input's
    # recorded identity and grew __unresolved__ ghost nodes on the canvas; see
    # api.matlab_command._project_root_lines.
    _root = registry.get_project_root()
    project_root: str | None = str(_root) if _root is not None else None

    _drop_project_root_folder(path_input_params, _root)

    # Run options travel with the run request and are rendered into the
    # generated script's for_each call. They are logged here because this is
    # the last point at which "what the GUI asked for" and "what the script
    # will do" are still the same object -- a missing option downstream is
    # invisible, since the script stays well-formed and the run still
    # succeeds. See docs/claude/gui-run-options-flow.md.
    run_options = params.get("run_options") or None

    logger.info(
        "generate_matlab_command: fn=%s, total_variants=%d, fn_variants=%d, "
        "path_input_params=%d, sweep_params=%s, variable_inputs=%s, "
        "output_types=%s, project_root=%s, run_options=%s",
        function_name,
        len(all_variants),
        len(fn_variants),
        len(path_input_params),
        sorted(sweep_params),
        variable_inputs,
        output_types,
        project_root,
        run_options,
    )

    cmd = _fmt(
        function_name=function_name,
        db_path=db_path,
        schema_keys=list(db.dataset_schema_keys),
        variants=fn_variants if fn_variants else params.get("variants"),
        # One value list per schema key: the per-key projection of a ragged
        # selection, which bounds what MATLAB resolves. The EXACT selection
        # travels beside it as `locations` (below), which the bridge applies
        # to the combos it hands MATLAB (cleanup-audit F6).
        schema_filter=_schema_selection.report(
            params.get("schema_selection"),
            db,
            context=f"matlab command for {function_name}",
        ),
        # Resolved by the one owner, exactly as a Python Run of this node
        # would (cleanup-audit F22): never a raw null the generator used to
        # read as "every key".
        schema_level=default_schema_level(
            db,
            function_name,
            fn_variants,
            stated=params.get("schema_level"),
            node_id=_node_id,
            route="matlab run",
        )[0],
        addpath_dirs=addpath_dirs if addpath_dirs else None,
        python_executable=sys.executable,
        path_inputs=path_input_params if path_input_params else None,
        sweeps=sweep_params if sweep_params else None,
        parameter_names=_collect_parameter_names(
            function_name, manual_edges, manual_nodes
        )
        or None,
        # The exact selection, beside the schema_filter projection above.
        locations=params.get("schema_selection") or None,
        output_types=output_types if output_types else None,
        project_root=project_root,
        entities_script=_entities_script(),
        entities_file=_entities_file(),
        variable_inputs=variable_inputs if variable_inputs else None,
        glue=glue_chains if glue_chains else None,
        run_options=run_options,
        # Markers are only written for a run something is watching. The
        # caller (the VS Code host, via generate_matlab_command) passes the
        # run_id it got from start_run; a preview or a copy-to-clipboard
        # with no run behind it passes none and the script writes none.
        run_id=params.get("run_id"),
    )
    logger.info(
        "generate_matlab_command: fn=%s, command_length=%d", function_name, len(cmd)
    )
    # Advisory only -- MATLAB's path is the authority on what resolves (see
    # matlab_command._unresolvable_var_types). Returned so api/run.py can put
    # it in the run console before the user starts waiting on MATLAB, rather
    # than leaving it as a comment they may never read.
    #
    # Its own key, NOT "warnings": on the pipeline branch below that key
    # means "steps excluded from the script", which a caller may act on, and
    # mixing an unrelated diagnostic into it makes both unreadable.
    from scistack_gui.api.matlab_command import (
        _collect_var_types,
        _variable_input_type_names,
        unresolvable_var_type_warning,
    )

    checked = (
        set(output_types or [])
        | set(_variable_input_type_names(variable_inputs))
        | _collect_var_types(
            fn_variants if fn_variants else (params.get("variants") or [])
        )
    )
    diagnostics = [d for d in [unresolvable_var_type_warning(checked)] if d]
    return {"command": cmd, "diagnostics": diagnostics}


def _drop_project_root_folder(path_inputs: dict, project_root) -> None:
    """Normalize a DB-recorded ``root_folder`` that is just the project root
    back to ``None``, in place.

    Runs made before the generator stopped substituting the project root for a
    rootless declaration are on disk with that root baked into their
    ``PathInput`` key. ``path_input_params`` reads those rows back, so without
    this the next run re-emits the rooted form and records the divergent key
    all over again — one ``__unresolved__`` ghost node that never heals.

    Dropping it changes nothing about what resolves: the generated script pins
    the same directory as scifor's resolution base for rootless PathInputs (see
    ``api.matlab_command._project_root_lines``). Any other ``root_folder`` is a
    real declaration and is left alone.
    """
    if project_root is None:
        return
    from pathlib import Path

    root = Path(project_root).resolve()
    for param_name, pi in path_inputs.items():
        rf = pi.get("root_folder")
        if not rf:
            continue
        try:
            same = Path(rf).resolve() == root
        except OSError:  # pragma: no cover - unresolvable path on this machine
            same = str(rf) == str(project_root)
        if same:
            logger.info(
                "generate_matlab_command: dropping the project root recorded as "
                "%r's root_folder (%s) — it is the resolution base already, and "
                "keeping it would re-record a PathInput key no declaration has",
                param_name,
                rf,
            )
            pi["root_folder"] = None


def generate_matlab_pipeline_command(pipeline_id: str, db, params: dict) -> dict:
    """Generate a ready-to-paste whole-pipeline MATLAB script.

    Scopes to every MATLAB function node in ``pipeline_id`` (via
    ``execution_service._scope_function_node_ids`` +
    ``matlab_registry.is_matlab_function``), resolving each node's
    target(s) with ``execution_service.derive_target_for_node`` — the same
    per-node target derivation ``build_backend_pipeline`` uses for Python
    pipeline runs, so a MATLAB pipeline run sees identical targets to a
    Python pipeline run or a ``code_export_service`` export of the same
    scope. Python function nodes sharing the scope are excluded (a
    MATLAB-only script cannot register a Python step into the same
    in-process ``scidb.Pipeline`` the MATLAB session builds — see
    ``api.matlab_command.generate_matlab_pipeline_command``'s docstring)
    and reported back via ``warnings`` instead of silently vanishing.

    Args:
        pipeline_id: The GUI pipeline scope id.
        db: DatabaseManager instance.
        params: Full RPC params dict (``mode``, ``target``, ``finalized``,
            ``skip_computed``, ``schema_filter``, ``schema_level``).

    Returns:
        {"command": str, "warnings": list[str]}
    """
    from scistack_gui import layout as layout_store
    from scistack_gui import matlab_registry
    from scistack_gui import pipeline_store
    from scistack_gui import registry as _reg
    from scistack_gui.api.matlab_command import (
        generate_matlab_pipeline_command as _fmt,
    )
    from scistack_gui.db import get_db_path
    from scistack_gui.domain.graph_builder import parse_path_input, path_input_display
    from scistack_gui.domain.variant_resolver import (
        filter_hidden_targets,
        hidden_call_ids_for_fn,
    )
    from scistack_gui.services.execution_service import (
        _scope_function_node_ids,
        apply_pending_overrides,
        default_schema_level,
        derive_target_for_node,
        variable_inputs_view,
    )

    db_path = str(get_db_path())

    addpath_dirs: list[str] = []
    if matlab_registry._config is not None:
        addpath_dirs = [str(p) for p in matlab_registry._config.matlab_addpath]

    from scistack_gui.server import _find_scimatlab_matlab_dir

    scimatlab_dir = _find_scimatlab_matlab_dir()
    if scimatlab_dir:
        addpath_dirs = [scimatlab_dir] + addpath_dirs
        logger.info(
            "generate_matlab_pipeline_command: prepended scimatlab dir: %s",
            scimatlab_dir,
        )
    else:
        logger.warning(
            "generate_matlab_pipeline_command: scimatlab MATLAB directory not "
            "found; scihist.* / scidb.* may be unavailable in MATLAB"
        )

    _root = _reg.get_project_root()
    project_root: str | None = str(_root) if _root is not None else None

    pending_consts = pipeline_store.get_pending_constants(db)
    hidden_ids = pipeline_store.get_hidden_node_ids(db, pipeline_id)
    saved_pis = {
        name: path_input_display(obj)
        for name, obj in _reg.get_path_inputs_registry().items()
    }
    saved_sweeps = {
        name: list(sw.alternatives) for name, sw in _reg.get_parameters_registry().items()
    }
    manual_edges = layout_store.read_manual_edges()
    manual_nodes = layout_store.get_manual_nodes()

    steps: list[dict] = []
    warnings: list[str] = []
    excluded_python: set[str] = set()
    for node_id, fn_label in _scope_function_node_ids(db, pipeline_id):
        if not matlab_registry.is_matlab_function(fn_label):
            excluded_python.add(fn_label)
            continue

        # This node's saved run options. A pipeline run has no live canvas
        # state to read -- it runs nodes the user never selected -- so the
        # stored config is the only source (docs/claude/gui-run-options-flow.md).
        step_run_options = pipeline_store.get_node_config(db, node_id).get(
            "runOptions"
        ) or {}

        targets = apply_pending_overrides(
            derive_target_for_node(db, node_id), pending_consts
        )
        # distribute/as_table are identity-bearing: they are folded into the
        # call_id that hidden-combo filtering matches on. Hardcoding them here
        # computed the call_id of a call this script will never make, so a
        # combo hidden on a distribute=true node stayed visible (and vice
        # versa). Pass what the step will actually run with.
        targets = filter_hidden_targets(
            targets,
            fn_label,
            hidden_call_ids_for_fn(hidden_ids, fn_label),
            pending_consts,
            RunOptions.from_config(step_run_options),
        )
        seen_target_keys: set = set()
        unique_targets: list[dict] = []
        for target in targets:
            key = (tuple(sorted(target["constants"].items())), target["output_type"])
            if key in seen_target_keys:
                continue
            seen_target_keys.add(key)
            unique_targets.append(target)
        unique_targets, skipped = _matlab_runnable_targets(unique_targets, fn_label)
        warnings.extend(skipped)

        # Same dispatch record as the single-node route: this step IS one
        # node, and the script about to run will write history under the
        # wiring its targets carry (D-2026-09-22-2).
        if unique_targets:
            from scistack_gui.services.execution_service import (
                record_dispatch_wirings,
            )

            record_dispatch_wirings(
                db, node_id, fn_label, unique_targets, params.get("run_id")
            )

        # Path inputs for this function — same two-source resolution
        # (DB-variant input_types, then canvas edges as the live overlay) as
        # generate_matlab_command, applied per-node here.
        path_input_params: dict[str, dict] = {}
        for t in unique_targets:
            for param_name, type_val in (t.get("input_types") or {}).items():
                pi = parse_path_input(str(type_val))
                if pi is not None:
                    path_input_params[param_name] = pi
        path_input_params.update(
            _collect_edge_path_inputs(fn_label, saved_pis, manual_edges, manual_nodes)
        )
        _drop_project_root_folder(path_input_params, project_root)

        sweep_params = _collect_sweep_params(
            fn_label, saved_sweeps, manual_edges, manual_nodes
        )
        # A pipeline step IS one specific node, and its targets already carry
        # the resolved bindings (history's selectors, the node's own
        # statements on top) — rendered here, not re-derived.
        step_variable_inputs = variable_inputs_view(unique_targets, fn_label)
        if not step_variable_inputs:
            step_variable_inputs = _collect_variable_inputs(
                fn_label, manual_edges, manual_nodes
            )

        steps.append(
            {
                "function_name": fn_label,
                "variants": unique_targets,
                "schema_filter": params.get("schema_filter"),
                # Per NODE from its stored level, like the Python pipeline
                # route — not the pipeline request's one value for every step.
                "schema_level": default_schema_level(
                    db,
                    fn_label,
                    unique_targets,
                    stated=pipeline_store.get_node_config(db, node_id).get(
                        "schemaLevel"
                    ),
                    node_id=node_id,
                    route=f"matlab pipeline {pipeline_id}",
                )[0],
                "path_inputs": path_input_params if path_input_params else None,
                "sweeps": sweep_params if sweep_params else None,
                # Glue rides on the step's input bindings exactly as on a
                # single-node command; the pipeline script used to drop it,
                # so a glued step ran on unreshaped input (cleanup-audit F17).
                "glue": _collect_glue_chains(fn_label, manual_edges, manual_nodes)
                or None,
                # This node's own location selection (cleanup-audit F36): the
                # pipeline script used one request-wide schema_filter for
                # every step and ignored each node's selection.
                "locations": pipeline_store.get_node_config(db, node_id).get(
                    "schemaSelection"
                )
                or None,
                "parameter_names": _collect_parameter_names(
                    fn_label, manual_edges, manual_nodes
                )
                or None,
                "variable_inputs": (
                    step_variable_inputs if step_variable_inputs else None
                ),
                "run_options": step_run_options or None,
            }
        )

    for fn_label in sorted(excluded_python):
        warnings.append(
            f"'{fn_label}' is a Python function — excluded from the MATLAB "
            "pipeline script; run it separately"
        )

    logger.info(
        "generate_matlab_pipeline_command: pipeline=%s, matlab_steps=%d, "
        "excluded_python=%d, project_root=%s",
        pipeline_id,
        len(steps),
        len(excluded_python),
        project_root,
    )

    cmd = _fmt(
        pipeline_id=pipeline_id,
        steps=steps,
        db_path=db_path,
        schema_keys=list(db.dataset_schema_keys),
        mode=params.get("mode", "all"),
        target=params.get("target", ""),
        finalized=params.get("finalized"),
        skip_computed=params.get("skip_computed", True),
        addpath_dirs=addpath_dirs if addpath_dirs else None,
        python_executable=sys.executable,
        project_root=project_root,
        entities_script=_entities_script(),
        entities_file=_entities_file(),
        run_id=params.get("run_id"),
    )
    logger.info(
        "generate_matlab_pipeline_command: pipeline=%s, command_length=%d",
        pipeline_id,
        len(cmd),
    )
    # See the single-function branch: advisory, surfaced in the run console,
    # and kept out of "warnings" so that list stays exactly the excluded
    # Python steps its callers and tests expect.
    from scistack_gui.api.matlab_command import (
        _collect_var_types,
        _variable_input_type_names,
        unresolvable_var_type_warning,
    )

    pipeline_var_types: set[str] = set()
    for step in steps:
        pipeline_var_types |= _collect_var_types(step.get("variants") or [])
        pipeline_var_types |= set(
            _variable_input_type_names(step.get("variable_inputs"))
        )
    diagnostics = [
        d for d in [unresolvable_var_type_warning(pipeline_var_types)] if d
    ]
    return {"command": cmd, "warnings": warnings, "diagnostics": diagnostics}
