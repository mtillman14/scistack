"""The calls MATLAB makes: ``py.scimatlab.bridge.<name>``.

The two-pass ``for_each`` (``for_each_prepare`` -> MATLAB's ``+scifor`` loop
-> ``for_each_save``), batch save and load, entity loading, the PathInput
project root, and the batch cache MATLAB reads converted values out of.
State that lives between two MATLAB calls (``_for_each_state_cache``, the
batch cache) is module state here, keyed by an integer handle.
"""

from .convert import is_datetime_value, split_flat_to_lists
from .crossing import matlab_entry
from .wire import (
    _locations_from_matlab,
    _make_matlab_fn_sentinel,
    _parameter_names_from_matlab,
    _reconstruct_glue_chains,
    _reconstruct_input_for_keys,
    _rename_rid_columns_in_value,
    _sanitize_rid_key,
    get_surrogate_class,
)

# ---------------------------------------------------------------------------
# Two-pass for_each bridge: prepare → MATLAB scifor loop → save
#
# The MATLAB embedded Python interpreter only supports unidirectional
# MATLAB→Python calls, so the original "Python drives the loop with a
# MATLAB callback" design is not viable.  Instead, the bridge exposes
# two seam functions:
#
#   1. ``for_each_prepare`` runs scidb's pre-loop work in Python
#      (variant expansion, DB load, branch_params, __upstream prep).
#      Returns an integer ``handle`` that keys server-side state, plus
#      the loaded inputs and expanded combo list that MATLAB needs to
#      run the inner loop with the existing ``+scifor/for_each.m``.
#
#   2. ``for_each_save`` consumes the per-output result tables MATLAB
#      collected from that inner loop, merges them, and runs scidb's
#      save step (lineage-aware, with branch_params and ``__upstream``).
#
# State sits in ``_for_each_state_cache`` between the two calls so
# Python's scidb.run.foreach._ForEachState (built by the prepare step) is
# the source of truth — MATLAB never has to reconstruct any of it.
# ---------------------------------------------------------------------------


_for_each_state_cache: dict = {}
_for_each_state_id_counter = 0


@matlab_entry
def for_each_prepare(
    fn_name: str,
    fn_hash: str,
    inputs_spec,
    output_class_names,
    metadata_iterables,
    where=None,
    distribute: bool = False,
    as_table=None,
    db=None,
    dry_run: bool = False,
    skip_computed: bool = False,
    finalized: bool = False,
    schema_keys=None,
    schema_filter=None,
    glue=None,
    locations=None,
    parameter_names=None,
    source_text=None,
    schema_key_types=None,
    project_root=None,
):
    """Bridge entry: run scidb.for_each's prepare phase in Python.

    Parameters
    ----------
    fn_name : str
        Function name (e.g. ``"bandpass"``). Used for ``__fn`` and logging.
    fn_hash : str
        MATLAB-computed function source hash. Stored as ``__fn_hash`` in
        the resulting version_keys, overriding the auto-computed hash
        ``ForEachConfig`` derives from the no-op Python sentinel.
    inputs_spec : dict
        ``{param_name: kind-tagged spec}`` (see ``_reconstruct_input_for_keys``).
    output_class_names : list[str]
        MATLAB variable type names for each output, in declaration order.
        Each is resolved via ``get_surrogate_class``.
    metadata_iterables : dict[str, list]
        ``{key: [values...]}`` passed to scidb.for_each as ``**metadata_iterables``.
        Empty lists trigger DB-defaults resolution per scidb's usual rules.
    where : scidb.model.filters.Filter or None
        Actual Python Filter object (MATLAB passes ``where_filter.py_filter``).
        Python's ``ForEachConfig.to_config_keys`` calls ``.to_key()`` on
        it for ``__where`` stringification; ``_load_input`` consumes the
        same object directly when loading.
    distribute : bool
    as_table : bool, list, or None
    db : DatabaseManager or None
    schema_keys : list[str] or None
        Schema key names to iterate — structural sugar for seeding
        ``metadata_iterables`` with ``key: []`` for each one, via the same
        ``scifor.expand_schema_keys()`` scidb.for_each's pure-Python path
        uses. Mutually exclusive with an already-populated
        ``metadata_iterables``.
    locations : dict or None
        Schema location selection — ragged ``include`` prefixes plus a standing
        ``exclude_levels`` rule (docs/claude/location-filter-semantics.md).
        Applied to ``full_combos`` before they cross to MATLAB, because
        ``+scifor/for_each.m`` runs the loop and never sees Python's
        ``scifor.for_each(locations=)``.
    project_root : str or None
        ``for_each(project_root=)``: the folder rootless PathInputs resolve
        against. None: the root this process pinned
        (:func:`set_pathinput_project_root`).
    schema_filter : dict[str, list] or None
        ``{schema_key: [values]}`` overrides. A key also in schema_keys (or,
        if schema_keys is None, any schema key) gets these values instead of
        DB auto-resolution. A key NOT being iterated instead constrains
        which records load, via a SchemaKeyInFilter ANDed into ``where``.
    skip_computed : bool
        If True, build scidb's pre-combo skip hook (the same one
        ``scidb.for_each`` builds in its Step 1.6) and apply it during
        prepare, so combos whose outputs already exist with unchanged
        upstream provenance are dropped from ``full_combos`` and never run
        in MATLAB's loop. The hook is fed the MATLAB-computed ``fn_hash`` so
        its function-hash comparison matches what the save path stored.
        No-op (with a warning) when no database is available.
    finalized : bool
        Endpoint (``plot_``/``stat_`` fn_name prefix) draft/record flag (D3).
        Policy is computed by scidb's ``_endpoint_policy`` (shared with the
        Python path): drafts suppress the save phase in ``for_each_save``;
        stat_ functions default ``as_table`` on. MATLAB does its own fn
        wrapping using the returned ``endpoint_kind``/``path_param``.
    glue : dict or None
        ``{param_name: [{name, source_text, per_schema_key}, ...]}`` — the
        glue chains wired to this call. **A glue node executes in the language
        of the run**, so on this path every chain is MATLAB and its bodies run
        in ``+scidb/for_each.m``, after prepare returns and before the loop (a
        ``.m`` function cannot execute inside Python's prepare step). What
        happens here is the *identity* half: each chain's virtual glue records
        are computed and the consumer's bindings routed through them, exactly
        as on the Python path, so one provenance recipe serves both languages.
        The returned ``glue_chains`` entry tells MATLAB which params to apply
        which nodes to.
    schema_key_types : dict or None
        ``{key: "numeric" | "string"}`` from MATLAB's ``schema_key_types=``.
        Recorded in the database (``DatabaseManager.declare_schema_key_types``)
        before prepare, exactly like Python ``scidb.for_each``.

    Returns
    -------
    dict with keys:
        handle : int                — server-side cache id; pass back to for_each_save
        loaded_inputs : dict         — {param_name: DataFrame or constant}
        full_combos : list[dict]     — variant-expanded combos
        output_names : list[str]
        extended_metadata_iterables : dict
        fn_name : str
        fn_hash : str
        endpoint_kind : str          — "plot" | "stat" | "" (non-endpoint)
        path_param : str             — name of the PathOutput input, or ""
        as_table_effective : ...     — as_table after endpoint policy (stat_
                                       defaults it True); MATLAB forwards
                                       this to its scifor loop
        resolved_path_outputs : dict — {param: [path_per_combo]} aligned with
                                       full_combos; ``{ColName}`` tokens are
                                       left in the strings for MATLAB's
                                       for_columns loop to substitute
        glue_chains : dict           — {param: [{name, per_schema_key}]} the
                                       MATLAB side must apply, in order, to
                                       that param's loaded table before the
                                       loop. Empty when no glue is wired.

    Raises
    ------
    NotImplementedError
        If any input resolves to a ``PerComboLoader`` (per-combo loading
        is not yet supported on the MATLAB path).
    """
    # JSON text from +scidb/for_each.m -> mapping (cleanup-audit F6).
    locations = _locations_from_matlab(locations)
    from scidb.run.bindings import COMBO_KEY, RECORD_ID_COLUMN
    from scidb.run import external_loop
    from scidb.model.log import Log
    from scidb.run.per_combo import PerComboLoader, PerComboLoaderMerge

    # Reconstruct Python wrappers from the kind-tagged spec
    inputs = {
        name: _reconstruct_input_for_keys(spec)
        for name, spec in dict(inputs_spec).items()
    }

    # Normalize db to None or a live DatabaseManager (MATLAB may pass py.None).
    resolved_db = db if db is not None and not isinstance(db, type(None)) else None

    # schema_key_types=: recorded in the database, which owns it, exactly as
    # Python scidb.for_each does. Prepare below reads the stored declaration.
    if schema_key_types:
        from scidb.store.database import get_database

        decl_db = resolved_db if resolved_db is not None else get_database()
        decl_db.declare_schema_key_types(
            {str(k): str(v) for k, v in dict(schema_key_types).items()},
            dry_run=bool(dry_run),
            source="for_each (MATLAB)",
        )

    # Step 1.5: Resolve for_columns (iterate-mode ColumnSelection) inputs.
    # The Python-only path does this in scidb.for_each before _for_each_prepare;
    # the MATLAB path calls _for_each_prepare directly, so resolve here so the
    # concrete column set drives version keys and the per-column MATLAB loop.
    from scifor import output_column_name as _output_column_name

    # Output names let it refuse a for_columns() split across outputs here,
    # before anything loads (the combine/split owner is scifor's
    # resolve_column_groups; MATLAB's loop asks its port again).
    inputs = external_loop.resolve_for_columns(
        inputs,
        resolved_db,
        [_output_column_name(get_surrogate_class(str(n))) for n in list(output_class_names)],
    )

    # Step 1.6: record each Parameter value as DECLARED. MATLAB passes every
    # number as a double, so a declared 6 arrives as 6.0. The skip hook below
    # predicts identities from these inputs, so the declared spelling must be
    # in place before it is built, not only inside prepare. One owner:
    # scidb.project.entities.declared_spelling.
    merged_parameter_names = _parameter_names_from_matlab(parameter_names)
    inputs = external_loop.declared_spelling(inputs, merged_parameter_names)

    # Resolve output class names → surrogate classes
    outputs = [get_surrogate_class(str(n)) for n in list(output_class_names)]

    # Build the no-op Python sentinel for ForEachConfig
    fn = _make_matlab_fn_sentinel(fn_name, fn_hash, source_text)

    # Normalize metadata_iterables: each value must be a Python list of
    # values to iterate over.  MATLAB sends scalars (subject=1 →
    # py.float(1.0)), strings, numpy arrays, and lists — wrap scalars
    # and strings as single-element lists so Python's scidb.for_each
    # sees a uniform list-of-values contract regardless of how the user
    # spelled the iterable on the MATLAB side.
    meta = {}
    for key, val in dict(metadata_iterables).items():
        if val is None:
            meta[key] = []
        elif isinstance(val, str):
            meta[key] = [val]
        elif isinstance(val, (bool, int, float)):
            meta[key] = [val]
        elif hasattr(val, "tolist"):
            meta[key] = val.tolist()
        else:
            try:
                meta[key] = list(val)
            except TypeError:
                # Last-resort: treat as a single value
                meta[key] = [val]

    # Normalize as_table: accept True/False/list/None
    if as_table is None or isinstance(as_table, bool):
        as_table_arg = as_table
    else:
        as_table_arg = list(as_table)

    # Endpoint policy (plot_/stat_ prefixes + finalized): shared with the
    # Python path via scidb's _endpoint_policy — detection, the
    # plot-requires-PathOutput contract, the stat_ as_table default, draft
    # save suppression (applied in for_each_save), and warnings. MATLAB does
    # the language-specific fn wrapping itself from the returned kind.
    endpoint_kind, endpoint_path_param, as_table_arg, save_suppressed = (
        external_loop.endpoint_policy(str(fn_name), inputs, bool(finalized), as_table_arg)
    )

    # Glue chains: MATLAB structs → GlueSpec, language "matlab". Bodies stay
    # unexecuted here (a .m function cannot run in Python); only their source
    # hashes matter to prepare, which is all identity needs.
    glue_arg = _reconstruct_glue_chains(glue)

    # Where: accept None or a Filter object. (Empty string sometimes
    # arrives from MATLAB when no filter was supplied; treat it as None.)
    where_arg = where if where not in ("", None) else None

    # schema_keys/schema_filter: same Step-0-equivalent logic as
    # scidb.run.foreach.py's pure-Python path (foreach.py:~379), reused here so
    # MATLAB needs no DB-querying code of its own — the resulting []
    # placeholders resolve via _for_each_prepare's existing generic
    # empty-list-from-DB resolver, identical to the pure-Python path.
    py_schema_keys = None if isinstance(schema_keys, type(None)) else schema_keys
    py_schema_filter = None if isinstance(schema_filter, type(None)) else schema_filter
    if py_schema_filter is not None or py_schema_keys is not None:
        schema_db = resolved_db
        if schema_db is None:
            try:
                from scidb.store.database import get_database

                schema_db = get_database()
            except Exception:
                schema_db = None
        if schema_db is None:
            raise ValueError(
                "schema_filter/schema_keys require a database connection, but no db "
                "was provided and no global database is configured."
            )
        import scifor as _scifor

        iterate_keys = (
            list(py_schema_keys)
            if py_schema_keys is not None
            else schema_db.dataset_schema_keys
        )
        meta = _scifor.expand_schema_keys(iterate_keys, meta)

        if py_schema_filter:
            from scidb.model.filters import SchemaKeyInFilter

            for key, values in dict(py_schema_filter).items():
                if key in meta:
                    meta[key] = list(values)
                else:
                    constraint = SchemaKeyInFilter(key, list(values))
                    where_arg = constraint if where_arg is None else (where_arg & constraint)

    # On dry_run=True, Python's _for_each_prepare runs the dry-run
    # scifor.for_each call and returns None. The MATLAB caller likewise
    # returns early (no save phase). The scifor dry-run output prints
    # planned iterations using DB-resolved metadata values — something
    # MATLAB's scifor.for_each on its own cannot do.
    if dry_run:
        try:
            external_loop.prepare(
                fn=fn,
                fn_name=fn_name,
                inputs=inputs,
                outputs=outputs,
                dry_run=True,
                db=db if db is not None and not isinstance(db, type(None)) else None,
                options=external_loop.RunOptions(
                    distribute=bool(distribute), as_table=as_table_arg
                ),
                # locations= so the preview counts the combos the real run will do.
                selection=external_loop.RunSelection(
                    where=where_arg, locations=locations, metadata_iterables=meta
                ),
                context=external_loop.RunContext(),
                glue=glue_arg,
                glue_language="matlab",
                project_root=project_root,
            )
        except Exception:
            raise
        # No state to cache and no save phase needed.
        return {
            "handle": -1,
            "loaded_inputs": {},
            "full_combos": [],
            "output_names": [],
            "extended_metadata_iterables": {},
            "fn_name": fn_name,
            "fn_hash": fn_hash,
            "dry_run": True,
        }

    # skip_computed: build the same pre-combo skip hook scidb.for_each builds
    # in its Step 1.6 (which the MATLAB path bypasses by calling
    # _for_each_prepare directly). Feed it the MATLAB-computed fn_hash so the
    # function-hash comparison matches what the save path stored — the sentinel
    # fn's own hash would never match, forcing eternal recompute. The hook is
    # applied inside _for_each_prepare's Step 14 combo filter, so the returned
    # full_combos already exclude skippable combos.
    pre_combo_hook = None
    if skip_computed and outputs:
        skip_db = resolved_db
        if skip_db is None:
            try:
                from scidb.store.database import get_database

                skip_db = get_database()
            except Exception:
                skip_db = None
        if skip_db is None:
            Log.warn(
                "[bridge] skip_computed=True ignored: no database available "
                "(pass db= or configure a global database)"
            )
        else:
            pre_combo_hook = external_loop.build_skip_hook(
                fn,
                outputs,
                skip_db,
                inputs,
                as_table=as_table_arg,
                distribute=bool(distribute),
                fn_hash=fn_hash,
            )
            Log.info(
                f"[bridge] skip_computed=True: built skip hook for {fn_name} "
                f"(fn_hash={fn_hash[:12] if fn_hash else '<none>'})"
            )

    state = external_loop.prepare(
        fn=fn,
        fn_name=fn_name,
        inputs=inputs,
        outputs=outputs,
        dry_run=False,
        db=db if db is not None and not isinstance(db, type(None)) else None,
        options=external_loop.RunOptions(distribute=bool(distribute), as_table=as_table_arg),
        selection=external_loop.RunSelection(where=where_arg, metadata_iterables=meta),
        # {input: declared Parameter}. MATLAB expands its Parameters before
        # this call, so the names only arrive stated; the same owner as the
        # Python path merges them (scidb.model.parameter.declared_input_names).
        context=external_loop.RunContext(
            pre_combo_hook=pre_combo_hook, parameter_names=merged_parameter_names
        ),
        glue=glue_arg,
        glue_language="matlab",
        project_root=project_root,
        # The save happens in a SECOND RPC, which reads this off the cached
        # state rather than being told again — one fact, one place.
        endpoint_kind=endpoint_kind or None,
    )

    if state is None:
        raise RuntimeError(
            "for_each_prepare returned None — dry_run is not supported "
            "on the MATLAB-driven path."
        )

    # Override the sentinel's auto-computed hash with the MATLAB hash so
    # the recorded version_keys identify the real MATLAB function source.
    state.config_keys["__fn_hash"] = fn_hash

    # Schema location selection. The Python path hands `locations=` to
    # scifor.for_each, which applies it to the combo list; MATLAB's loop is
    # `+scifor/for_each.m` and never sees that argument, so the SAME filter is
    # applied here to the combos this bridge is about to hand over. Full
    # parity, including the RAGGED half a generated `subject = [...]` value
    # list cannot express, without touching a .m file.
    #
    # Placed before the PathOutput pre-resolution below, which builds one
    # resolved path per combo and asserts the two lists are the same length.
    if locations is not None and not isinstance(locations, type(None)):
        from scifor.locations import filter_combos

        state.full_combos = filter_combos(
            state.full_combos, locations, context=f"matlab for_each({fn_name})"
        )

    # PathInput special case: scidb's _load_input wraps PathInput in a
    # PerComboLoader because PathInput's load() is template substitution,
    # not a DB lookup — it cannot be bulk-loaded. MATLAB's +scifor/for_each.m
    # natively handles scifor.PathInput when _resolve_pathinput=true is
    # set, so unwrap the sentinel back to the PathInput instance and let
    # MATLAB resolve it per-combo. Other per-combo loader kinds are not
    # yet supported on the MATLAB path; surface a clear error so the user knows.
    from scifor import Fixed as _Fixed
    from scifor.pathinput import PathInput as _SciforPathInput

    unsupported_per_combo = []
    for k, v in list(state.loaded_inputs.items()):
        if not isinstance(v, (PerComboLoader, PerComboLoaderMerge)):
            continue
        # Unwrap PerComboLoader to its underlying Fixed / PathInput / etc.
        spec = v.spec if isinstance(v, PerComboLoader) else v.merge_spec
        # Direct PathInput
        if isinstance(spec, _SciforPathInput):
            state.loaded_inputs[k] = spec
            continue
        # Fixed wrapping a PathInput: Fixed is the same class in scifor and
        # scidb now, so the spec already IS what MATLAB's resolve_data_spec
        # expects — pass it through unchanged.
        if isinstance(spec, _Fixed) and isinstance(spec.data, _SciforPathInput):
            state.loaded_inputs[k] = spec
            continue
        # Anything else (e.g. ColumnSelection over a var type with no load support)
        # is not yet supported on the MATLAB path.
        unsupported_per_combo.append(k)
    if unsupported_per_combo:
        raise NotImplementedError(
            f"MATLAB-driven scidb.for_each does not yet support per-combo "
            f"loaders for inputs: {unsupported_per_combo}. These arise when "
            f"a PathInput or Fixed(PathInput) is wrapped in a ColumnSelection "
            f"or Merge, or when a variable type cannot be bulk-loaded. "
            f"Call from Python instead, or restructure to use bulk-loadable types."
        )

    # --- The two internal names that cross the MATLAB boundary ---
    # The state cache keeps the original names so Python's save path
    # (_for_each_save_resolved → _save_results) reads the result table it
    # expects; only the MATLAB-facing copies are renamed. `__record_id` is
    # the frame column `for_each.m` selects rows by; `__combo` is the combo's
    # handle into `row_selection` below (and comes back on every result row,
    # which is how the save finds each row's Selection).
    _bindings = state.bindings
    rid_rename_map = {
        RECORD_ID_COLUMN: _sanitize_rid_key(RECORD_ID_COLUMN),
        COMBO_KEY: _sanitize_rid_key(COMBO_KEY),
    }

    # "Python decides, MATLAB applies": which rows of each input a
    # combination reads is its `Selection`, serialised here aligned with
    # full_combos — `{param: [rid, ...]}` per combo —
    # so `for_each.m` selects rows with ismember on x__record_id and never
    # needs Python per combo. Python's own loop reads the same selections
    # through scifor's `LoopHooks.select_rows` hook: one rule, two runtimes.
    #
    # Aligned by walking `state.full_combos` — NOT by dumping
    # `bindings.selections`: selections are registered for every combination
    # at expansion, and the skip hook removes combinations from
    # `full_combos` AFTERWARDS, so the two lists differ in length whenever
    # skip_computed skipped anything (the first bridge test run, 2026-09-20).
    row_selection = []
    for combo in state.full_combos:
        sel = _bindings.selection_of(combo)
        row_selection.append(
            {param: list(rids) for param, rids in sel.rids.items()} if sel else {}
        )

    # Loaded inputs: rename DataFrame columns (and inside wrappers)
    matlab_loaded_inputs = {
        name: _rename_rid_columns_in_value(val, rid_rename_map)
        for name, val in state.loaded_inputs.items()
    }

    # Pre-resolve PathOutputs per combo Python-side (the PathInput
    # "Python decides, MATLAB applies" pattern): injected branch_param
    # placeholder keys (which may be dotted, e.g. ``bandpass.low_hz``) can
    # never cross as MATLAB struct fields, so the RESOLVED path strings
    # cross instead, aligned with full_combos. ``{ColName}`` tokens are left
    # unresolved for MATLAB's for_columns loop (literal replace composes).
    from scifor import PathOutput as _SciforPathOutput

    resolved_path_outputs = {}
    for _name, _spec in inputs.items():
        if isinstance(_spec, _SciforPathOutput):
            resolved_path_outputs[_name] = [
                str(_spec.resolve(combo, None)) for combo in state.full_combos
            ]
            assert len(resolved_path_outputs[_name]) == len(state.full_combos)

    # Full combos: rename keys (single pass with list comprehension), and
    # DROP injected PathOutput placeholder keys — MATLAB never needs them
    # (paths are pre-resolved above) and dotted names are invalid fields.
    _path_extras = state.path_extra_keys or set()
    matlab_full_combos = [
        {rid_rename_map.get(k, k): v for k, v in combo.items() if k not in _path_extras}
        for combo in state.full_combos
    ]

    # Extended metadata iterables: rename keys
    matlab_meta_iters = {
        rid_rename_map.get(k, k): v
        for k, v in state.extended_metadata_iterables.items()
    }

    # (scifor's schema is no longer extended for a run — MATLAB's
    # scifor.get_schema sees the dataset schema, and row selection travels
    # as `row_selection` instead.)

    # Cache state for the matching for_each_save call
    global _for_each_state_id_counter
    handle = _for_each_state_id_counter
    _for_each_state_id_counter += 1
    _for_each_state_cache[handle] = {
        "state": state,
        "inputs": inputs,
        "outputs": outputs,
        "db": db,
        "rid_rename_map": rid_rename_map,
        "where": where,
        "endpoint_kind": endpoint_kind,
        "save_suppressed": save_suppressed,
    }

    from scidb.model.log import Log as _BridgeLog

    _BridgeLog.info(
        f"[bridge] for_each_prepare returning: full_combos={len(matlab_full_combos)}, "
        f"extended_metadata_iterables keys={list(matlab_meta_iters.keys())}, "
        f"meta sizes={[len(v) if hasattr(v, '__len__') else 'N/A' for v in matlab_meta_iters.values()]}"
    )
    return {
        "handle": handle,
        "loaded_inputs": matlab_loaded_inputs,
        "full_combos": matlab_full_combos,
        "row_selection": row_selection,
        "record_id_column": rid_rename_map[RECORD_ID_COLUMN],
        "combo_key": rid_rename_map[COMBO_KEY],
        "output_names": state.output_names,
        "extended_metadata_iterables": matlab_meta_iters,
        "fn_name": state.fn_name,
        "fn_hash": fn_hash,
        "endpoint_kind": endpoint_kind or "",
        "path_param": endpoint_path_param or "",
        "as_table_effective": as_table_arg,
        "resolved_path_outputs": resolved_path_outputs,
        # {param_name: [data column names]} for inputs whose records are
        # MATLAB structs, stored one DuckDB column per field. MATLAB's
        # +scifor/for_each.m rebuilds the struct per combo; without this the
        # spread columns reach the user function as a 1xN table and every
        # field access yields a 1x1 cell. See scidb._resolve_mapping_inputs.
        "mapping_inputs": {k: list(v) for k, v in (state.mapping_inputs or {}).items()},
        # {param: [{name, per_schema_key}]} — the glue nodes MATLAB must apply
        # to that param's table, in order, between prepare and the loop. The
        # provenance side is already done (state.glue_virtual); this is purely
        # "which bodies to call".
        "glue_chains": {
            param: [
                {"name": s.name, "per_schema_key": bool(s.per_schema_key)}
                for s in chain
            ]
            for param, chain in (state.glue_chains or {}).items()
        },
    }


def _merge_output_tables(dfs: list, output_names: list):
    """MATLAB's per-output result tables -> the ONE result table the save
    path reads, outer-merged on their shared metadata columns.

    A combination missing from one output's table was skipped for that
    output only (for_columns split: no data, or the group raised; the MATLAB
    loop already logged why). Its cell becomes ``scifor.outputs.NoOutput(
    "skipped")`` so the save writes no record there, rather than the NaN the
    merge would leave, which would save as a record. A None or empty table
    is an output skipped everywhere. Positions in ``dfs`` match
    ``output_names``.
    """
    import pandas as pd
    from scifor.outputs import NoOutput

    from scidb.model.log import Log as _Log

    present = [
        (i, df) for i, df in enumerate(dfs) if isinstance(df, pd.DataFrame) and len(df)
    ]
    if not present:
        return pd.DataFrame()
    flags = {i: f"__present_{i}" for i, _ in present}
    result = None
    for i, df in present:
        tagged = df.assign(**{flags[i]: True})
        if result is None:
            result = tagged
            continue
        shared = [
            c for c in result.columns if c in tagged.columns and c not in flags.values()
        ]
        if shared:
            result = result.merge(tagged, on=shared, how="outer")
        else:
            # No shared columns to merge on; fall back to row-wise concat.
            result = pd.concat([result, tagged], axis=1)
    for i, name in enumerate(output_names):
        if i in flags:
            absent = result[flags[i]].isna()
            if not absent.any() or name not in result.columns:
                continue
            col = result[name].astype(object)
            col[absent] = [NoOutput("skipped") for _ in range(int(absent.sum()))]
            result[name] = col
            _Log.info(
                f"[bridge] {name}: skipped at {int(absent.sum())} of "
                f"{len(result)} combination(s) (absent from MATLAB's table)"
            )
        else:
            result[name] = [NoOutput("skipped") for _ in range(len(result))]
            _Log.info(f"[bridge] {name}: skipped at every combination")
    return result.drop(columns=list(flags.values()))


@matlab_entry
def for_each_describe_loaded_input(val):
    """Describe one ``loaded_inputs`` value so MATLAB can rebuild the
    matching MATLAB-side scifor wrapper.

    Python's ``_for_each_prepare`` returns ``loaded_inputs`` with values
    that may be:
      - pandas DataFrame
      - scifor.Fixed (wrapping a DataFrame plus fixed_metadata)
      - scifor.ColumnSelection (wrapping a DataFrame plus column names)
      - scifor.Merge (wrapping multiple DataFrames or wrappers)
      - PerComboLoader / PerComboLoaderMerge (already rejected upstream)
      - any other constant value

    MATLAB's ``+scifor/for_each.m`` expects the MATLAB classdef versions
    of these wrappers (with MATLAB tables inside). This helper produces a
    kind-tagged dict that MATLAB can switch on, with DataFrames left as
    Python objects (so MATLAB's ``from_python`` converts them to MATLAB
    tables on the other side of the bridge crossing).
    """
    import pandas as pd
    from scifor.colname import ColName as _SciforColName
    from scifor.column_selection import ColumnSelection as _SciforColSel
    from scifor.fixed import Fixed as _SciforFixed
    from scifor.merge import Merge as _SciforMerge
    from scifor.pathinput import PathInput as _SciforPathInput

    if isinstance(val, pd.DataFrame):
        return {"kind": "dataframe", "data": val}
    if isinstance(val, _SciforColName):
        # _convert_inputs only ever leaves a deferred (no-arg) ColName in
        # loaded_inputs (static ColName(MyVar) was resolved to a string).
        # MATLAB rebuilds scifor.ColName() to substitute the current column.
        return {"kind": "colname"}
    if isinstance(val, _SciforFixed):
        return {
            "kind": "fixed",
            "inner": for_each_describe_loaded_input(val.data),
            "fixed_metadata": dict(val.fixed_metadata),
        }
    if isinstance(val, _SciforColSel):
        from scifor.column_selection import groups_to_wire

        desc = {
            "kind": "column_selection",
            "inner": for_each_describe_loaded_input(val.data),
            "columns": list(val.columns),
            "iterate": bool(getattr(val, "iterate", False)),
        }
        if val.groups is not None:
            desc["groups"] = groups_to_wire(val.groups)
        return desc
    if isinstance(val, _SciforMerge):
        return {
            "kind": "merge",
            "tables": [for_each_describe_loaded_input(t) for t in val.tables],
        }
    if isinstance(val, _SciforPathInput):
        # PathInput is resolved per-combo by MATLAB's +scifor/for_each.m
        # via _resolve_pathinput=true. Ship the whole to_spec() so MATLAB
        # rebuilds a matching scifor.PathInput — aliases and key_regex included.
        return {"kind": "pathinput", "spec": val.to_spec()}
    from scifor import PathOutput as _SciforPathOutput

    if isinstance(val, _SciforPathOutput):
        # PathOutput crosses as its template; the ACTUAL per-combo values
        # come from prepare's ``resolved_path_outputs`` (Python pre-resolves
        # so injected branch_param placeholder keys never cross the bridge).
        # MATLAB rebuilds scifor.PathOutput(template) as the fallback for
        # pure-MATLAB resolution.
        return {"kind": "path_output", "template": str(val.template)}
    return {"kind": "raw", "value": val}


@matlab_entry
def for_each_save(
    handle,
    result_dataframes,
    save: bool = True,
    introspect: bool = False,
    on_duplicate: str = "error",
):
    """Bridge entry: run scidb.for_each's save phase.

    Parameters
    ----------
    handle : int
        Handle returned by ``for_each_prepare``. Consumed (cache entry is
        freed regardless of success or failure).
    result_dataframes : list[pandas.DataFrame]
        One DataFrame per output, in the same order as the
        ``output_class_names`` passed to ``for_each_prepare``. Each carries
        the metadata columns from MATLAB's ``+scifor/for_each.m`` plus
        one column named for that output.  When there is only one output,
        a single DataFrame may be passed (a length-1 list also works).
    save : bool
        If False, run the save phase with ``save=False`` (schema restore
        still happens; no records are written).
    on_duplicate : str
        ``"error"`` (default) or ``"combine"``: what the save does with two
        result rows at one location and variant (scidb.run.combine). Read by the
        shared save path through ``state.on_duplicate``.
    introspect : bool
        If True, append introspection columns to the returned DataFrame
        (_record_id_*, _branch_params_*, _call_id, _config_keys, _where).
        _branch_params_* and _config_keys are JSON strings for MATLAB
        compatibility; all other introspect columns are plain strings.

    Returns
    -------
    pandas.DataFrame
        The merged result table, with one row per combo and metadata +
        output columns. Same shape Python's ``scidb.for_each`` returns.
    """
    import pandas as pd
    from scidb.run import external_loop
    from scifor.foreach import spread_nested_results as _spread_nested_results

    cached = _for_each_state_cache.pop(int(handle), None)
    if cached is None:
        raise ValueError(
            f"for_each_save: handle {handle} not found in cache "
            f"(already freed or never registered)"
        )

    state = cached["state"]
    from scidb.run.combine import validate_on_duplicate

    state.on_duplicate = validate_on_duplicate(str(on_duplicate))
    inputs = cached["inputs"]
    outputs = cached["outputs"]
    db = cached["db"]
    rid_rename_map = cached.get("rid_rename_map", {})
    # Endpoint DRAFT (finalized=False): policy computed once in prepare via
    # _endpoint_policy; the save phase is suppressed here Python-side so
    # MATLAB never re-implements the rule. Draft artifact stamping still
    # runs inside _for_each_save_resolved.
    if cached.get("save_suppressed"):
        save = False

    # Reverse the bridge-boundary sanitization on the way back: MATLAB
    # produced result tables whose columns include the sanitized combo handle
    # (``x__combo``); Python's save path (state.bindings, _save_results)
    # expects ``__combo`` to find each row's Selection.
    reverse_map = {v: k for k, v in rid_rename_map.items()}

    # Merge per-output DataFrames into the single result_tbl shape that
    # _for_each_save_resolved expects (matching what Python's scifor
    # returns when called with output_names).
    if isinstance(result_dataframes, pd.DataFrame):
        dfs = [result_dataframes]
    else:
        # Positions stay aligned with state.output_names: a None (nothing
        # returned for that output) is handled by _merge_output_tables.
        dfs = list(result_dataframes)

    if reverse_map:
        dfs = [
            df.rename(
                columns={c: reverse_map[c] for c in df.columns if c in reverse_map}
            )
            if isinstance(df, pd.DataFrame)
            else df
            for df in dfs
        ]

    if not any(df is not None for df in dfs):
        result_tbl = pd.DataFrame()
    elif len(dfs) == 1:
        result_tbl = dfs[0]
    else:
        result_tbl = _merge_output_tables(dfs, list(state.output_names))

    from scidb.model.log import Log as _Log

    # MATLAB's loop always runs in nested mode (+scidb/for_each.m passes
    # _nest_table_outputs=true), so each output cell holds the combo's whole
    # return value. Python's loop would now decide whether a returned table's
    # ROWS are separately addressed (it carries a schema key the combo did
    # not pin) — that decision lives in scifor, and applying it here is what
    # keeps the two languages filing a labelled table identically. Before
    # this call a 73-row (subject, session) table saved as ONE dataset-level
    # record on the MATLAB path (scistack.log 2026-09-15 19:16).
    #
    # Single-output runs only. scifor's spread lays every output's columns
    # flat side by side, and _save_results then files ALL data columns under
    # EACH output — Python's multi-table-output limitation. The nested path
    # keeps each MATLAB output's table separate, so it stays in force there
    # rather than importing that limitation.
    nested_shape = result_tbl.shape
    # A custom-serialized (to_db/from_db) output's table is ONE record's
    # stored form, never rows to spread — whatever columns it has. Same rule
    # as BaseVariable.save's custom path, which never auto-distributes.
    from scidb.store.database import DatabaseManager as _DM

    codec_output = len(outputs) == 1 and _DM._has_custom_serialization(
        outputs[0] if isinstance(outputs[0], type) else type(outputs[0])
    )
    if codec_output:
        _Log.info(
            f"[bridge] for_each_save: {state.output_names[0]} has custom "
            f"to_db/from_db — each output table is one record, not spread"
        )
    if len(state.output_names) == 1 and not codec_output:
        result_tbl = _spread_nested_results(
            result_tbl,
            list(state.output_names),
            list(state.current_schema_keys or []),
        )
    elif len(state.output_names) > 1:
        _Log.info(
            f"[bridge] for_each_save: {len(state.output_names)} outputs — spread "
            f"rule not applied (single-output runs only); each output's table "
            f"is saved whole per combination"
        )

    _Log.info(
        f"[bridge] for_each_save: handle={handle}, "
        f"nested shape={nested_shape}, "
        f"result_tbl shape={result_tbl.shape}, "
        f"columns={list(result_tbl.columns)}"
    )

    result_tbl = external_loop.save_resolved(
        state=state,
        result_tbl=result_tbl,
        # state.inputs, not the cached spec: prepare folded any constant-fed
        # glue chain into the values (the version keys keep the RAW value and
        # the glue's identity). Same rule as scidb.for_each's own save call.
        inputs=state.inputs if state.inputs is not None else inputs,
        outputs=outputs,
        save=bool(save),
        db=db if db is not None and not isinstance(db, type(None)) else None,
    )

    if introspect and result_tbl is not None and not result_tbl.empty:
        import json as _json

        from scidb.run import external_loop

        cached_where = cached.get("where")
        result_tbl = external_loop.apply_introspect(result_tbl, state, cached_where)
        # Serialize dict-valued columns to JSON strings for MATLAB compatibility.
        for col in list(result_tbl.columns):
            if col.startswith("_branch_params_"):
                result_tbl[col] = result_tbl[col].apply(
                    lambda x: _json.dumps(x) if isinstance(x, dict) else str(x)
                )
        # _where is None when for_each ran without a where= filter. A Python
        # None crosses the bridge as an unconvertible cell, so MATLAB's
        # ``string(result.("_where")(1))`` raises MustBeConvertibleCellArray.
        # Coerce None -> "" so the column is a uniform string the empty-filter
        # introspect tests (and MATLAB consumers) can read directly.
        if "_where" in result_tbl.columns:
            result_tbl["_where"] = result_tbl["_where"].apply(
                lambda x: "" if x is None else str(x)
            )

    return result_tbl


def run_failure_group_json(exc, fn_name: str) -> str:
    """The run-level failure group (JSON) of a Python exception that escaped
    ``for_each_save`` into MATLAB, e.g. an ``OutputSaveError``.

    The MATLAB twin of the GUI's ``RunFailures.add_exception``: the same
    ``FailureGroup.to_dict`` shape with ``count=1``, so ``blame`` and any
    ``remedy`` (the GUI popup) reach a MATLAB run's ``.done`` marker exactly
    as they reach a Python run's ``run_done``. ``+scidb/for_each.m`` adds it
    to ``iteration_tally`` and rethrows the original error. Returns ``""``
    when *exc* is not a Python exception (nothing to describe).
    """
    import json

    from scidb.model.log import Log
    from scifor.failures import FailureGroup, run_failure

    if not isinstance(exc, BaseException):
        Log.debug(f"run_failure_group_json: {type(exc).__name__} is not an exception")
        return ""
    group = {**FailureGroup(example=run_failure(exc, fn_name=str(fn_name))).to_dict(), "count": 1}
    Log.debug(
        f"run_failure_group_json: {type(exc).__name__} in {fn_name} -> "
        f"blame={group['blame']} remedy={(group.get('remedy') or {}).get('kind')}"
    )
    return json.dumps(group, default=str)


@matlab_entry
def normalize_stat_result(
    json_str: str,
    report_path: str = "",
    finalized: bool = False,
    fn_name: str = "stat_fn",
) -> str:
    """Bridge entry: canonicalize a MATLAB stat_ result into the stored
    JSON payload — the exact bytes the Python stat_ wrapper would store for
    the same result (``scidb.run.foreach.normalize_stat_payload``).

    MATLAB's ``jsonencode`` differs from Python's ``json.dumps`` (key order,
    float formatting), so the MATLAB stat_ wrapper jsonencode's the user's
    struct and routes it through here; skip_computed's content identity
    across the two languages depends on this single normalization point.
    ``report_path`` is "" for none (draft or no PathOutput).
    """
    from scidb.run.foreach import normalize_stat_payload

    return normalize_stat_payload(
        str(json_str),
        str(report_path) or None,
        bool(finalized),
        fn_name=str(fn_name),
    )


def pathinput_project_root() -> str:
    """Return ``scifor.project_root()`` (the pinned root, else the cwd) as a
    string, so MATLAB reads the one owner of the project root rather than
    deriving its own."""
    from scifor.project.root import project_root

    return str(project_root())


def set_pathinput_project_root(root) -> str:
    """Bridge entry: pin the directory rootless PathInputs resolve against.

    A ``PathInput`` declared with no ``root_folder`` resolves relative paths
    against ``scifor.project_root()``, which is the **cwd** unless pinned.
    Under MATLAB the cwd is wherever the user's MATLAB is sitting -- for a
    GUI-generated command, a temp script directory -- so it names the wrong
    folder, and every relative template misses.

    The caller knows the project; the cwd does not. ``+scidb/entities.m``
    calls this with the project root it was given, and the GUI's generated
    command emits it in the preamble so the guarantee holds even for a
    project with no entities file.

    Note this pins *resolution* only -- it does NOT put the root into
    ``root_folder``, which is part of a PathInput's recorded identity. The
    GUI used to rewrite ``root_folder`` here instead, which made a run
    recorded from the GUI un-matchable against its own declaration and grew
    an ``__unresolved__`` ghost node on the canvas; see
    ``scifor.project.root.set_project_root``.
    """
    from scidb.model.log import Log
    from scifor.project.root import set_project_root

    resolved = set_project_root(str(root) if root is not None else None)
    Log.info(
        "[pathinput] MATLAB pinned the PathInput project root to %s",
        resolved,
        layer="matlab",
    )
    return str(resolved) if resolved is not None else ""


def load_entities(project_start=None) -> dict:
    """The project's TOML entities file, flattened for MATLAB marshaling.

    MATLAB has no TOML reader, so ``+scidb/entities.m`` gets the
    declarations through here and rebuilds them as MATLAB
    ``scidb.Parameter``/``scidb.PathInput`` objects. Only plain
    dicts/lists/scalars cross the bridge -- handing MATLAB the constructed
    Python objects would give it Python proxies, not the MATLAB classes
    ``scidb.for_each`` expects.

    ``root_folder`` is ``None`` when unset (MATLAB reads that as ``[]``),
    never ``""``, so "no root" stays distinguishable from "rooted at the
    empty string".

    Returns ``{path, variables, parameters, path_inputs, errors}``. Errors
    are pre-rendered strings: MATLAB shows them as warnings, and a rejected
    entry must be visible there too, not only in the GUI.
    """
    import json
    from pathlib import Path

    from scidb import entities
    from scidb.model.log import Log
    from scifor.project.root import project_root

    result = entities.load_for_project(project_start)

    # Loading a project's entities is also the moment MATLAB tells Python
    # which project is running, so pin scifor's PathInput resolution base
    # here. Any script that calls scidb.entities(PROJECT_ROOT) -- generated
    # or hand-written -- then resolves a rootless PathInput against that
    # project instead of MATLAB's cwd, with no second call to remember.
    # The folder MATLAB names IS the project root -- whether or not it holds a
    # config (scifor.project_root: no walking up to a pyproject.toml). With no
    # folder named there is nothing better than the cwd, which is what an
    # unpinned root already means, so nothing is pinned.
    if project_start is not None:
        set_pathinput_project_root(Path(str(project_start)))

    def _arms(obj):
        alternatives = getattr(obj, "alternatives", None)
        return list(alternatives) if alternatives is not None else [obj]

    path_inputs = {}
    for name, obj in result.path_inputs.items():
        path_inputs[name] = [
            {
                "template": getattr(arm, "path_template", ""),
                "root_folder": (
                    str(arm.root_folder)
                    if getattr(arm, "root_folder", None) is not None
                    else None
                ),
                # JSON text, not a dict: a dict crosses the bridge as a
                # struct, and a canonical such as "01" is not a field name.
                # +scifor/PathInput.m takes the text as-is ("" = none).
                "aliases_json": (
                    json.dumps(arm.aliases) if getattr(arm, "aliases", None) else ""
                ),
                # JSON text for symmetry with aliases ("" = none); MATLAB
                # jsondecodes it into the struct +scifor/PathInput.m takes
                # (its fields are placeholder names, so a struct is safe).
                "key_regex_json": (
                    json.dumps(arm.key_regex) if getattr(arm, "key_regex", None) else ""
                ),
            }
            for arm in _arms(obj)
        ]

    payload = {
        "path": str(result.path),
        "variables": list(result.variables),
        "parameters": {
            name: list(param.values) for name, param in result.parameters.items()
        },
        "path_inputs": path_inputs,
        "errors": [err.describe() for err in result.errors],
    }
    if not str(result.path):
        # ``EntitiesFile(path=Path())`` -- no project config was found from
        # the starting point, so nothing was loaded. This used to print as
        # "from ." at INFO, which is only legible if you know it is
        # str(Path()), and it is exactly what a generated script that
        # forgot to pass its project root produces.
        Log.warn(
            "[entities] MATLAB load found NO entities file: no project config "
            "at the project root %s (project_start=%r, cwd=%s). Declared "
            "Parameters/PathInputs will not be in scope; pass the project "
            "root as scidb.entities(PROJECT_ROOT).",
            Path(project_start) if project_start is not None else project_root(),
            project_start,
            Path.cwd(),
            layer="matlab",
        )
    else:
        Log.info(
            "[entities] MATLAB load: %d variable(s), %d parameter(s), %d path "
            "input(s), %d rejected, from %s",
            len(payload["variables"]),
            len(payload["parameters"]),
            len(payload["path_inputs"]),
            len(payload["errors"]),
            payload["path"],
            layer="matlab",
        )
    return payload


def ensure_variable_classdefs(names, project_start=None) -> dict:
    """Bridge entry: write MATLAB classdef stubs for declared variables that
    have none, and report where they went.

    ``+scidb/entities.m`` calls this with the names MATLAB itself could not
    resolve (``exist(name, 'class') ~= 8``) -- the decision of *which* names
    need a file is MATLAB's, because MATLAB's path is the only authority on
    what already resolves. See ``scimatlab.stubs``.

    ``names`` arrives from MATLAB as a cell of char/string; each entry is
    stringified here so both marshal correctly.
    """
    from scidb.model.log import Log
    from scimatlab.stubs import write_variable_classdefs

    raw = list(names) if names is not None else []
    wanted = [str(n) for n in raw]
    # Both sides of the marshalling boundary, because a MATLAB `string` vs
    # `char` mismatch mangles a name *here* and nowhere downstream can tell
    # that from a name that arrived wrong. Only logged when str() actually
    # changed something, so the common case stays quiet.
    changed = [(a, b) for a, b in zip(raw, wanted) if not isinstance(a, str) or a != b]
    if changed:
        Log.debug(
            "[bridge] ensure_variable_classdefs marshalled %s",
            "; ".join(f"{a!r} -> {b!r}" for a, b in changed),
            layer="matlab",
        )
    return write_variable_classdefs(wanted, project_start=project_start)



@matlab_entry
def save_batch_bridge(
    type_name,
    data_values,
    metadata_keys,
    metadata_columns,
    common_metadata=None,
    db=None,
    row_heights=None,
    on_duplicate="error",
):
    """Bridge function for MATLAB save_from_table.

    Accepts columnar data (one list per column) from MATLAB and assembles
    the (data_value, metadata_dict) tuples that DatabaseManager.save_batch()
    expects.  This avoids per-row MATLAB↔Python round-trips.

    Parameters
    ----------
    type_name : str
        Variable class name (e.g. "StepLength").
    data_values : list, numpy array, or pandas DataFrame
        One data value per row, or a bulk DataFrame/array when row_heights
        is provided (Strategy A / vertcat mode from MATLAB).
    metadata_keys : list of str
        Metadata column names, same order as metadata_columns.
    metadata_columns : list of (list or numpy array)
        One inner list/array per metadata key, each with one value per row.
    common_metadata : dict or None
        Extra metadata applied to every row.
    db : DatabaseManager or None
        Optional database; uses global default when None.
    row_heights : numpy array or None
        When provided, data_values is a bulk structure (DataFrame or ndarray)
        and row_heights[i] is the number of rows that belong to record i.
        Python splits it back into N per-record slices (zero-copy views).
        None = legacy per-row list mode (backward-compatible).
    on_duplicate : str
        ``"error"`` (default) or ``"combine"``: two rows at one location
        become one record first (scidb.run.combine; the rule is
        scifor.combine_duplicate_rows). The returned ids stay one per INPUT
        row: rows that were combined share their record's id.

    Returns
    -------
    list of str
        Record IDs for each saved row.
    """
    import time as _time

    import numpy as np
    import pandas as pd
    from scidb.store.database import get_database
    from scidb.model.log import Log
    from scidb.model.variable import BaseVariable

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        raise ValueError(
            f"Variable type '{type_name}' is not registered. "
            f"Call scidb.register_variable('{type_name}') first."
        )

    t_start = _time.perf_counter()

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()
    common = dict(common_metadata) if common_metadata else {}
    keys = list(metadata_keys)

    # Determine how to build data_list from data_values.
    t_split = 0.0
    bulk_mode = "list"

    heights_arr = None
    if row_heights is not None and not isinstance(row_heights, type(None)):
        heights_arr = np.asarray(row_heights, dtype=int).ravel()

    if heights_arr is not None and isinstance(data_values, pd.DataFrame):
        # Strategy A result: bulk DataFrame → split into N per-record DataFrames.
        # reset_index(drop=True) normalises the index to 0..height-1 on every
        # slice so canonical_hash produces the same result as the per-row path
        # (which always received a freshly constructed DataFrame with index [0]).
        offsets = np.concatenate([[0], np.cumsum(heights_arr)])
        _t = _time.perf_counter()
        data_list = [
            data_values.iloc[int(offsets[i]) : int(offsets[i + 1])].reset_index(
                drop=True
            )
            for i in range(len(heights_arr))
        ]
        t_split = _time.perf_counter() - _t
        bulk_mode = "vertcat_df"
    elif heights_arr is not None and isinstance(data_values, np.ndarray):
        # Strategy A result: bulk ndarray → split into N row-slice views.
        offsets = np.concatenate([[0], np.cumsum(heights_arr)])
        _t = _time.perf_counter()
        data_list = [
            data_values[int(offsets[i]) : int(offsets[i + 1])]
            for i in range(len(heights_arr))
        ]
        t_split = _time.perf_counter() - _t
        bulk_mode = "vertcat_arr"
    elif hasattr(data_values, "tolist"):
        # Numpy array (isnumeric fast path or Strategy B flatten result).
        # atleast_1d: a ONE-row MATLAB column crosses as a scalar, so
        # py.numpy.array makes a 0-d array whose tolist() is a bare number.
        data_list = np.atleast_1d(data_values).tolist()
        bulk_mode = "numpy_list"
    else:
        # Plain Python list (Strategy B split_flat_to_lists, Strategy C,
        # string/cellstr paths, or datetime_to_python's Timestamps — never
        # .item()'d: a datetime64's item() is a bare int).
        from scicanonicalhash import is_datetime_value as _is_dt

        data_list = [
            v.item() if hasattr(v, "item") and not _is_dt(v) else v
            for v in data_values
        ]

    meta_lists = []
    for j in range(len(keys)):
        col = metadata_columns[j]
        if isinstance(col, str):
            # Joined string from MATLAB (record-separator delimited)
            meta_lists.append(col.split("\x1e"))
        elif isinstance(col, (bool, int, float)):
            # One-row numeric column: number_to_python sends the scalar.
            meta_lists.append([col])
        elif hasattr(col, "tolist"):
            # atleast_1d: a one-row numeric column arrives as a 0-d array.
            meta_lists.append(np.atleast_1d(col).tolist())
        else:
            meta_lists.append([v.item() if hasattr(v, "item") else v for v in col])

    n = len(data_list)
    data_items = []
    for i in range(n):
        meta = dict(common)
        for j, key in enumerate(keys):
            meta[key] = meta_lists[j][i]
        data_items.append((data_list[i], meta))

    t_convert = _time.perf_counter()
    Log.debug(
        f"[timing] save_batch_bridge({type_name}): n={n}, mode={bulk_mode}, "
        f"split={t_split:.3f}s, assembly={t_convert - t_start - t_split:.3f}s"
    )

    # save_batch returns None in the slot of any record it skipped (schema-
    # incompatible). Emit an empty string for those so the newline-delimited
    # result stays row-aligned with the input on the MATLAB side.
    from scidb.run.combine import validate_on_duplicate

    validate_on_duplicate(str(on_duplicate))
    if on_duplicate == "combine" and n > 1:
        combined_items, output_row, row_lists = _combine_bridge_items(
            data_items, type_name
        )
        _out_rids = _db.save_batch(cls, combined_items, row_list_columns=row_lists)
        _rids = [_out_rids[j] for j in output_row]
    else:
        _rids = _db.save_batch(cls, data_items)
    result = "\n".join(r if isinstance(r, str) else "" for r in _rids)

    Log.info(
        f"[timing] save_batch_bridge({type_name}): n={n}, mode={bulk_mode}, "
        f"split={t_split:.3f}s, total={_time.perf_counter() - t_start:.3f}s"
    )
    return result


#: Prefix that keeps a table row's data columns apart from its metadata keys
#: while the bridge combines them as one table.
_DATA_PREFIX = "__scimatlab_data__."
_VALUE_COLUMN = "__scimatlab_value__"


def _combine_bridge_items(data_items, type_name):
    """``on_duplicate="combine"`` for save_from_table's per-row items.

    The items become ONE table (metadata columns + data columns, or a value
    column when every row's data is a single value), the scidb rule combines
    it (scidb.run.combine.combine_table), and the table goes back to items.
    Returns ``(items, output_row, row_list_columns)``: ``output_row[i]`` is
    the combined item input row ``i`` went into.

    Only table rows combine: when any row's data is an array, list, dict or
    a table of more than one row, the items are returned unchanged and
    save_batch's duplicate-location check decides.
    """
    import pandas as pd
    import numpy as np
    from scidb.run.combine import combine_table
    from scidb.model.log import Log

    datas = [d for d, _ in data_items]
    identity = (data_items, list(range(len(data_items))), [])
    if all(isinstance(d, pd.DataFrame) and len(d) == 1 for d in datas):
        cols = list(datas[0].columns)
        if any(list(d.columns) != cols for d in datas):
            Log.info(
                f"[bridge] save_batch_bridge({type_name}): on_duplicate=combine "
                f"skipped - the rows' tables have different columns"
            )
            return identity
        values = pd.concat(datas, ignore_index=True)
        values.columns = [f"{_DATA_PREFIX}{c}" for c in cols]
        scalar_mode = False
    elif not any(
        isinstance(d, (pd.DataFrame, np.ndarray, list, tuple, dict)) for d in datas
    ):
        values = pd.DataFrame({_VALUE_COLUMN: pd.Series(datas, dtype=object)})
        values[_VALUE_COLUMN] = values[_VALUE_COLUMN].infer_objects()
        scalar_mode = True
    else:
        Log.info(
            f"[bridge] save_batch_bridge({type_name}): on_duplicate=combine "
            f"skipped - rows hold arrays or multi-row tables, not single values"
        )
        return identity

    meta = pd.DataFrame([m for _, m in data_items])
    table = pd.concat([meta.reset_index(drop=True), values], axis=1)
    value_cols = list(values.columns)
    combined, report = combine_table(table, value_cols, type_name=type_name)
    if combined is table:
        return identity

    # Every metadata column is a key, so a group's rows share their metadata:
    # the first row's dict is the combined record's. A row that was not
    # combined keeps its own item untouched (same content, same record id).
    first_input: dict = {}
    n_inputs: dict = {}
    for i, j in enumerate(report.output_row):
        first_input.setdefault(j, i)
        n_inputs[j] = n_inputs.get(j, 0) + 1
    items = []
    for j in range(len(combined)):
        first = first_input[j]
        if n_inputs[j] == 1:
            items.append(data_items[first])
            continue
        if scalar_mode:
            d = combined[_VALUE_COLUMN].iloc[j]
        else:
            d = pd.DataFrame(
                {
                    c[len(_DATA_PREFIX):]: pd.Series([combined[c].iloc[j]], dtype=object)
                    for c in value_cols
                }
            ).infer_objects()
        items.append((d, dict(data_items[first][1])))
    row_lists = [
        _VALUE_COLUMN if scalar_mode else c[len(_DATA_PREFIX):]
        for c in report.list_columns
    ]
    Log.debug(
        f"[bridge] save_batch_bridge({type_name}): on_duplicate=combine "
        f"{len(data_items)} row(s) -> {len(items)} record(s), row_list "
        f"column(s) {row_lists}"
    )
    return items, list(report.output_row), row_lists


# ---------------------------------------------------------------------------
# Batch cache — keeps data/py_vars in Python so they never cross to MATLAB's
# proxy layer.  MATLAB accesses individual items via get_batch_item().
# ---------------------------------------------------------------------------

_batch_cache = {}
_batch_id_counter = 0


def _cache_batch(data_list, py_vars_list):
    """Store data and py_vars lists server-side, return an integer handle."""
    global _batch_id_counter
    bid = _batch_id_counter
    _batch_id_counter += 1
    _batch_cache[bid] = (data_list, py_vars_list)
    return bid


@matlab_entry
def get_batch_item(batch_id, index):
    """Return (data, py_var) for one element from a cached batch."""
    data_list, py_vars_list = _batch_cache[int(batch_id)]
    i = int(index)
    return data_list[i], py_vars_list[i]



def codec_storage_columns(type_name: str, db=None) -> list:
    """The stored data columns of a MATLAB codec variable (custom
    ``to_db``/``from_db``), in stored order.

    ``+scidb/for_each.m`` loads such an input as one row per STORED row (the
    surrogate's pass-through ``from_db`` returns the record's table, which the
    spread load lays out row by row). To hand each call the decoded object it
    groups rows by record id and calls ``from_db`` on these columns only —
    every other column of the loaded table is metadata. Owner of the column
    list: ``DatabaseManager.stored_data_columns``.
    """
    from scidb.store.database import get_database
    from scidb.model.variable import BaseVariable

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        return []
    _db = db if db is not None and not isinstance(db, type(None)) else get_database()
    return _db.stored_data_columns(cls.table_name())



@matlab_entry
def get_batch_data_item(batch_id, index):
    """Return just the data for one element from a cached batch."""
    data_list, _ = _batch_cache[int(batch_id)]
    return data_list[int(index)]


def free_batch(batch_id):
    """Release a cached batch."""
    _batch_cache.pop(int(batch_id), None)


@matlab_entry
def wrap_batch_bridge(py_vars_list):
    """Extract all fields from a list of BaseVariables into bulk format.

    Scalar fields are packed into newline-joined strings and metadata into
    a single JSON string.  The ``py_vars`` list is returned directly so
    MATLAB can convert it to a cell array in one call.  When all data
    values are scalars (int/float), they are packed into a numpy array
    (``scalar_data``) for single-crossing transfer; otherwise data is
    stored in a Python-side cache for per-item access.

    Parameters
    ----------
    py_vars_list : list of BaseVariable
        Python BaseVariable instances to extract.

    Returns
    -------
    dict with keys:
        n              : int
        py_vars        : list  — BaseVariable objects for MATLAB cell() conversion
        batch_id       : int   — handle for get_batch_data_item (non-scalar only)
        record_ids     : str   — newline-joined
        content_hashes : str   — newline-joined
        json_meta      : str   — JSON array of metadata dicts
        scalar_data    : numpy.ndarray (optional) — present when all data are scalars
    """
    import json

    import numpy as np

    py_vars = list(py_vars_list) if not isinstance(py_vars_list, list) else py_vars_list
    n = len(py_vars)

    record_ids = []
    content_hashes = []
    meta_dicts = []
    branch_params_list = []
    data = []

    for v in py_vars:
        record_ids.append(v.record_id or "")
        content_hashes.append(v.content_hash or "")
        meta = v.metadata
        meta_dicts.append(dict(meta) if meta is not None else {})
        branch_params_list.append(json.dumps(v.branch_params or {}))
        data.append(v.data)

    # Cache data for non-scalar fallback access
    batch_id = _cache_batch(data, py_vars)

    result = {
        "n": n,
        "py_vars": py_vars,
        "batch_id": batch_id,
        "record_ids": "\n".join(record_ids),
        "content_hashes": "\n".join(content_hashes),
        "json_meta": json.dumps(meta_dicts),
        "json_branch_params": "\n".join(branch_params_list),
    }

    # Scalar fast path: pack all data into a single numpy array. numpy
    # scalars count: an integer record loads back as np.int64 (a BIGINT
    # column's .iloc), and without them here it took the per-item path,
    # where MATLAB's double() on a py.numpy.int64 is not guaranteed.
    if n > 0 and all(
        isinstance(d, (int, float, np.integer, np.floating)) for d in data
    ):
        result["scalar_data"] = np.array(data, dtype=float)

    # DataFrame fast path: concatenate same-schema DataFrames into one
    # so MATLAB converts a single large table instead of N small ones
    if n > 0 and "scalar_data" not in result:
        import pandas as pd

        if all(isinstance(d, pd.DataFrame) for d in data):
            first_cols = list(data[0].columns)
            if all(list(d.columns) == first_cols for d in data):
                row_counts = [len(d) for d in data]
                concat_df = pd.concat(data, ignore_index=True)

                # Optimize object-dtype columns containing numpy arrays
                # For variable-length arrays, flatten all data into a single array
                # and send size/offset info separately. This avoids N boundary
                # crossings in MATLAB (one per cell) by doing bulk transfer.
                from scidb.model.log import Log

                optimized_cols = []
                flattened_data = {}
                json_columns = []
                object_cols = [
                    col for col in concat_df.columns if concat_df[col].dtype == object
                ]
                Log.debug(
                    f"wrap_batch_bridge: found {len(object_cols)} object-dtype columns to check"
                )

                for col_name in concat_df.columns:
                    col = concat_df[col_name]
                    if col.dtype == object and len(col) > 0:
                        # Check if first non-null value is a numpy array
                        first_val = None
                        for val in col:
                            if val is not None:
                                first_val = val
                                break

                        if first_val is not None:
                            first_val_type = type(first_val).__name__
                            Log.debug(
                                f'wrap_batch_bridge: column "{col_name}" first value type: {first_val_type}'
                            )

                            if isinstance(first_val, dict):
                                # JSON/dict columns - serialize to JSON strings for fast transfer
                                # MATLAB will parse these back to structs automatically
                                Log.debug(
                                    f'wrap_batch_bridge: column "{col_name}" contains dicts, serializing to JSON...'
                                )
                                import json

                                concat_df[col_name] = [
                                    json.dumps(val) if isinstance(val, dict) else val
                                    for val in col
                                ]
                                json_columns.append(col_name)
                                optimized_cols.append(col_name + " (JSON)")
                                Log.debug(
                                    f'wrap_batch_bridge: column "{col_name}" serialized to JSON strings'
                                )
                            elif isinstance(first_val, np.ndarray):
                                # Flatten array column: concatenate all arrays and store sizes
                                # This allows MATLAB to convert the entire column in 2 operations
                                # (one for flat data, one for sizes) instead of N operations
                                Log.debug(
                                    f'wrap_batch_bridge: flattening column "{col_name}"...'
                                )
                                arrays = [
                                    arr
                                    if isinstance(arr, np.ndarray)
                                    else np.array(arr)
                                    for arr in col
                                ]

                                # Get array sizes and flatten
                                sizes = np.array(
                                    [len(arr) for arr in arrays], dtype=np.int32
                                )
                                flat_data = (
                                    np.concatenate(arrays)
                                    if len(arrays) > 0
                                    else np.array([])
                                )

                                # Store flattened representation
                                flattened_data[col_name] = {
                                    "flat": flat_data,
                                    "sizes": sizes,
                                    "dtype": str(first_val.dtype),
                                }

                                # Mark column for reconstruction in MATLAB
                                concat_df[col_name] = ["__flattened__"] * len(col)

                                optimized_cols.append(col_name)
                                Log.debug(
                                    f'wrap_batch_bridge: column "{col_name}" flattened: '
                                    f"{len(flat_data)} total elements in {len(sizes)} arrays"
                                )

                if optimized_cols:
                    Log.info(
                        f"wrap_batch_bridge: optimized {len(optimized_cols)} array columns for MATLAB transfer: {optimized_cols[:5]}"
                    )
                    if flattened_data:
                        result["flattened_arrays"] = flattened_data
                    if json_columns:
                        result["json_columns"] = json_columns
                else:
                    Log.debug("wrap_batch_bridge: no array columns found to optimize")

                result["concat_df"] = concat_df
                result["concat_df_row_counts"] = np.array(row_counts, dtype=np.int64)

    return result


@matlab_entry
def load_and_extract(py_class, metadata_dict, version_id="latest", db=None, where=None):
    """Load all matching variables and extract fields in bulk.

    Combines load -> list -> wrap_batch_bridge in one Python call.
    The intermediate BaseVariable list and data arrays stay in Python
    (accessed later via get_batch_item).  Only lightweight strings/JSON
    cross back to MATLAB.

    Parameters
    ----------
    py_class : type
        BaseVariable subclass to load.
    metadata_dict : dict
        Metadata filter (values can be lists for "match any").
    version_id : str or int
        Version filter ('latest', 'all', or an integer).
    db : DatabaseManager or None
        Optional database; uses global default when None.
    where : Filter or None
        Optional where= filter (scidb.model.filters.Filter instance).

    Returns
    -------
    dict
        Same format as wrap_batch_bridge (with batch_id, no data/py_vars).
    """
    from scidb.store.database import get_database
    from scidb.model.exceptions import NotFoundError

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()

    # scidb's load() raises NotFoundError when nothing matches. The MATLAB
    # caller (+scidb/BaseVariable.m) detects the no-match case via the n==0
    # sentinel in the returned dict and raises a clean ``scidb:NotFoundError``
    # itself. If the Python exception is allowed to cross the bridge it surfaces
    # in MATLAB as the opaque, generic ``MATLAB:Python:PyException`` instead,
    # breaking the documented error contract (and every verifyError test that
    # expects ``scidb:NotFoundError``). Convert it back to the empty-result
    # sentinel so MATLAB's n==0 branch fires.
    try:
        gen = _db.load(
            py_class, dict(metadata_dict), version_id=version_id, where=where
        )
        py_vars = list(gen)  # materializes entirely in Python
    except NotFoundError:
        py_vars = []
    return wrap_batch_bridge(py_vars)


@matlab_entry
def to_csv_bridge(type_name, filename, **kwargs):
    """Export a variable to CSV on behalf of MATLAB ``BaseVariable.to_csv``.

    MATLAB cannot dispatch the inherited ``to_csv`` *classmethod* on a Python
    surrogate **class object**: ``methods(py_class)`` omits it (it reflects only
    the metaclass's own methods, not classmethods accessible via the MRO) even
    though Python ``hasattr(py_class, 'to_csv')`` is True. So rather than calling
    ``py_class.to_csv(...)`` from MATLAB, MATLAB calls this module-level bridge —
    the same delegation pattern ``save``/``load`` already use (they route through
    ``save_batch_bridge``/``load_and_extract`` instead of the surrogate's
    classmethods).

    ``filename`` is positional; all load options (``where=``, ``version=``,
    ``db=``, and schema-key metadata filters) arrive as ``**kwargs`` and are
    forwarded verbatim to ``to_csv`` → ``export_csv``. Column selection
    (``MyVar["col"].to_csv``) is dispatched on a ``ColumnSelection`` *instance*
    in MATLAB, which works without this bridge.
    """
    cls = get_surrogate_class(type_name)
    cls.to_csv(str(filename), **kwargs)



# ---------------------------------------------------------------------------
# Calls MATLAB used to make straight into scidb with a converted value.
# They come through the bridge so the value arrives unboxed (.crossing): a
# datetime scalar from to_python.m is a DatetimeCrossing, which scidb does
# not know.
# ---------------------------------------------------------------------------


@matlab_entry
def save_variable_bridge(db, py_class, data, **kwargs):
    """``db.save_variable(py_class, data, **kwargs)`` for ``BaseVariable.save``
    (MATLAB); ``db`` None means the configured database."""
    from scidb.store.database import get_database

    _db = db if db is not None else get_database()
    return _db.save_variable(py_class, data, **kwargs)


@matlab_entry
def variable_filter(py_class, column, op, value):
    """The filter ``MyVar == value`` / ``MyVar("col") < value`` builds in MATLAB:
    a ``ColumnFilter`` when ``column`` is non-empty, else a ``VariableFilter``."""
    from scidb.model.filters import ColumnFilter, VariableFilter

    if column:
        return ColumnFilter(py_class, str(column), str(op), value)
    return VariableFilter(py_class, str(op), value)


@matlab_entry
def schema_key_filter(key, op, value):
    """``scidb.schema_key(KEY) <op> VALUE`` (MATLAB ``SchemaKey``)."""
    from scidb.model.filters import SchemaKeyCompareFilter

    return SchemaKeyCompareFilter(str(key), str(op), value)


@matlab_entry
def variable_data(py_var):
    """``py_var.data`` for ``BaseVariable.wrap_py_var`` (MATLAB); reading the
    attribute in MATLAB would convert a datetime scalar unboxed."""
    return py_var.data
