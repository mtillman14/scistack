"""The pipeline registry for MATLAB: create, bind, use, plan and run
``scidb.Pipeline`` objects by integer handle.

Python owns the graph; MATLAB owns the execution of MATLAB steps, driving each
through its two-pass ``scidb.for_each`` (``bridge.entry``).
"""

from .crossing import matlab_entry
from .wire import (
    _locations_from_matlab,
    _make_matlab_fn_sentinel,
    _parameter_names_from_matlab,
    _reconstruct_glue_chains,
    _reconstruct_input_for_keys,
    get_surrogate_class,
)

# ---------------------------------------------------------------------------
# Pipeline registry bridge (endpoint-first stage 4 — MATLAB parity)
#
# Split per the D7 principle: Python owns the graph (registration, topo
# order, bindings, plan); MATLAB owns execution of MATLAB steps, driving
# them through its normal two-pass scidb.for_each. MATLAB function handles
# never cross the bridge — each MATLAB step's StepSpec holds the erroring
# sentinel (fn.__name__ only) plus __matlab__/__matlab_fn_hash__ options,
# and Python's Pipeline.run_* refuses such steps with a pointer back here.
# ---------------------------------------------------------------------------

_pipeline_cache: dict = {}
_pipeline_id_counter: int = 0
_binding_cache: dict = {}
_binding_id_counter: int = 0
_pipeline_run_cache: dict = {}
_pipeline_run_id_counter: int = 0


def _cache_pipeline(pipe) -> int:
    global _pipeline_id_counter
    _pipeline_id_counter += 1
    _pipeline_cache[_pipeline_id_counter] = pipe
    return _pipeline_id_counter


@matlab_entry
def pipeline_create(name: str, db=None, activate: bool = True) -> int:
    """Create (and by default activate) a Pipeline; returns a cache handle.

    MATLAB has no ``with``, so its scidb.Pipeline constructor keeps the
    activate-until-run form (Python registers only inside ``with pipe:``)."""
    from scidb.run.pipeline import Pipeline

    _db = db if db is not None and not isinstance(db, type(None)) else None
    if _db is None:
        try:
            from scidb import get_database

            _db = get_database()
        except Exception:
            _db = None
    pipe = Pipeline(name, db=_db)
    if activate:
        pipe._activate()
    return _cache_pipeline(pipe)


@matlab_entry
def pipeline_active_name() -> str:
    """Name of the ambient active pipeline, or '' — MATLAB for_each's
    cheap registration-seam check."""
    from scidb.run.pipeline import active_pipeline

    active = active_pipeline()
    return active.name if active is not None else ""


@matlab_entry
def pipeline_deactivate(handle: int) -> None:
    """MATLAB's explicit escape: stop registering, without running."""
    _pipeline_cache[int(handle)]._deactivate(acknowledge=True)


@matlab_entry
def pipeline_register_step(
    handle: int,
    fn_name: str,
    fn_hash: str,
    inputs_spec,
    output_class_names,
    metadata_iterables,
    where=None,
    distribute: bool = False,
    as_table=None,
    save: bool = True,
    finalized: bool = False,
    skip_computed: bool = False,
    schema_keys=None,
    schema_filter=None,
    glue=None,
    parameter_names=None,
    locations=None,
) -> int:
    """Register one MATLAB for_each call as a deferred step; returns the
    step's index in the pipeline's own step list (MATLAB stores the fn
    handle + raw call args under this index).

    ``glue`` rides along so the registered step's identity matches the eager
    call's: glue names contribute to the call_id, so a step registered
    without them would be a different call site than the same step run
    directly."""
    pipe = _pipeline_cache[int(handle)]
    inputs = {
        name: _reconstruct_input_for_keys(spec)
        for name, spec in dict(inputs_spec).items()
    }
    outputs = [get_surrogate_class(n) for n in list(output_class_names)]
    sentinel = _make_matlab_fn_sentinel(fn_name, fn_hash)
    meta = {k: list(v) for k, v in dict(metadata_iterables).items()}
    schema_keys_arg = list(schema_keys) if schema_keys is not None else None
    schema_filter_arg = (
        {k: list(v) for k, v in dict(schema_filter).items()}
        if schema_filter is not None
        else None
    )
    pipe.register_call(
        fn=sentinel,
        inputs=inputs,
        outputs=outputs,
        metadata_iterables=meta,
        options={
            "where": where,
            "distribute": bool(distribute),
            "as_table": as_table,
            "save": bool(save),
            "finalized": bool(finalized),
            "skip_computed": bool(skip_computed),
            "schema_keys": schema_keys_arg,
            "schema_filter": schema_filter_arg,
            "glue": _reconstruct_glue_chains(glue) or None,
            "parameter_names": _parameter_names_from_matlab(parameter_names),
            "locations": _locations_from_matlab(locations),
            "__matlab__": True,
            "__matlab_fn_hash__": fn_hash,
        },
    )
    return len(pipe.steps) - 1


@matlab_entry
def pipeline_bind(handle: int, key_map=None, params=None, iterate=None) -> int:
    """Create a use-edge binding for the pipeline; returns a binding handle.
    Bind-time validation errors (unknown/ambiguous params) raise here."""
    global _binding_id_counter

    pipe = _pipeline_cache[int(handle)]
    binding = pipe.bind(
        key_map=dict(key_map) if key_map else None,
        params=dict(params) if params else None,
        iterate={k: list(v) for k, v in dict(iterate).items()} if iterate else None,
    )
    _binding_id_counter += 1
    _binding_cache[_binding_id_counter] = binding
    return _binding_id_counter


@matlab_entry
def pipeline_use(
    parent_handle: int, child_handle: int = 0, binding_handle: int = 0
) -> None:
    """Declare a dependency: pass child_handle for an identity edge, or
    binding_handle for a bound edge (exactly one must be nonzero)."""
    parent = _pipeline_cache[int(parent_handle)]
    if int(binding_handle):
        parent.use(_binding_cache[int(binding_handle)])
    else:
        parent.use(_pipeline_cache[int(child_handle)])


@matlab_entry
def pipeline_plan(handle: int, target_name: str = "") -> list:
    """plan() forwarder; MATLAB-friendly list of dicts (combos summarized
    to a count — full combo dicts don't convert usefully)."""
    pipe = _pipeline_cache[int(handle)]
    entries = pipe.plan(target=target_name or None)
    return [
        {
            "step": e["step"],
            "pipeline": e["pipeline"],
            "endpoint": bool(e["endpoint"]),
            "state": e["state"],
            "n_combos": len(e["combos"]),
        }
        for e in entries
    ]


@matlab_entry
def pipeline_endpoints(handle: int, include_used: bool = True) -> list:
    return _pipeline_cache[int(handle)].endpoints(include_used=include_used)


@matlab_entry
def pipeline_execution_order(
    handle: int,
    mode: str = "all",
    target_name: str = "",
    include_used: bool = False,
    finalized=None,
    skip_computed: bool = True,
) -> dict:
    """Resolve the run: topo-ordered descriptors + a run handle.

    MATLAB iterates the descriptors: ``is_matlab`` steps run through
    MATLAB's own scidb.for_each (raw args stored MATLAB-side at the
    descriptor's (pipeline, step_index), overridden by the descriptor's
    post-binding surface); Python steps run via
    ``pipeline_run_python_step(run_handle, position)``. Selection,
    acknowledgment, and deactivation semantics match Python run_*.
    """
    global _pipeline_run_id_counter

    pipe = _pipeline_cache[int(handle)]
    fin = (
        None
        if finalized is None or isinstance(finalized, type(None))
        else bool(finalized)
    )
    descriptors = pipe.execution_order(
        mode=mode,
        target=target_name or None,
        include_used=bool(include_used),
        finalized=fin,
        skip_computed=bool(skip_computed),
    )
    # Re-derive the selection for the run cache (execution_order returns
    # plain data only; _execute_step needs the live pairs/order).
    pairs, order, targets = pipe._select(mode, target_name or None, bool(include_used))
    _pipeline_run_id_counter += 1
    _pipeline_run_cache[_pipeline_run_id_counter] = {
        "pipeline": pipe,
        "pairs": pairs,
        "order": order,
        "targets": targets,
        "skip_computed": bool(skip_computed),
        "finalized": fin,
    }
    # Constants must cross as plain values; drop non-primitive leftovers
    # (MATLAB re-substitutes into its own stored inputs struct).
    for d in descriptors:
        d["constant_inputs"] = {
            k: v
            for k, v in d["constant_inputs"].items()
            if isinstance(v, (int, float, str, bool, list))
        }
    return {"run_handle": _pipeline_run_id_counter, "steps": descriptors}


@matlab_entry
def pipeline_run_python_step(run_handle: int, position: int):
    """Execute the run's POSITIONth step Python-side (mixed pipelines:
    Python-registered steps run in their home language while MATLAB
    drives the order)."""
    run = _pipeline_run_cache[int(run_handle)]
    i = run["order"][int(position)]
    pipe = run["pipeline"]
    fin = (
        run["finalized"]
        if (run["finalized"] is not None and i in run["targets"])
        else None
    )
    pipe._execute_step(
        run["pairs"],
        i,
        skip_computed=run["skip_computed"],
        finalized=fin,
    )
    return True


@matlab_entry
def pipeline_run_free(run_handle: int) -> None:
    """Release a finished run's cached state."""
    _pipeline_run_cache.pop(int(run_handle), None)
