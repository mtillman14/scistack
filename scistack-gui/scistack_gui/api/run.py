"""
POST /api/run

Triggers a for_each call in a background thread and streams stdout
back to the frontend via WebSocket.

Payload:
  {
    "function_name": "compute_rolling_vo2",
    "variants": [
      {"window_seconds": 30, "sample_interval": 5},
      {"window_seconds": 60, "sample_interval": 5}
    ]
  }

Each entry in `variants` is a constants dict. We run one for_each call
per variant. If `variants` is empty we run all known variants from the DB.
"""

import sys
import uuid
import threading
from io import StringIO
from contextlib import redirect_stdout

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from scidb import for_each
from scidb.database import DatabaseManager

from scistack_gui.db import get_db
from scistack_gui import registry
from scistack_gui.api.ws import push_message

router = APIRouter()


class RunRequest(BaseModel):
    function_name: str
    variants: list[dict] = []   # list of constants dicts; empty = run all known
    run_id: str | None = None   # frontend-generated ID; we generate one if absent


def _run_in_thread(run_id: str, function_name: str, variants: list[dict], db: DatabaseManager):
    """
    Executed in a background thread. Runs for_each for each variant,
    captures stdout line-by-line, and pushes it to the WebSocket queue.
    """

    def emit(text: str):
        push_message({"type": "run_output", "run_id": run_id, "text": text})

    # The DatabaseManager is stored in thread-local storage by configure_database().
    # Background threads don't inherit that local, so we re-register it here.
    db.set_current_db()

    try:
        fn = registry.get_function(function_name)
    except KeyError as e:
        push_message({"type": "run_done", "run_id": run_id, "success": False,
                      "error": str(e)})
        return

    # Look up input/output types for this function from the DB.
    all_variants = db.list_pipeline_variants()
    fn_variants = [v for v in all_variants if v["function_name"] == function_name]

    from scidb.log import Log
    Log.info(f"run: fn_variants for '{function_name}' = {fn_variants}")

    # --- Check manual edges for current output wiring ---
    # If the user has rewired the function's outputs (e.g. deleted an old output
    # node and connected a new one), manual edges should override DB-derived
    # output types.  This prevents stale DB history from resurrecting old nodes.
    from scistack_gui import pipeline_store, layout as layout_store
    from scistack_gui.api.pipeline import _node_id_to_var_label, _fn_params_from_registry

    all_edges = pipeline_store.get_manual_edges(db)
    manual_nodes = pipeline_store.get_manual_nodes(db)

    fn_node_ids = set()
    fn_node_ids.add(f"fn__{function_name}")
    for nid, meta in manual_nodes.items():
        if meta["type"] == "functionNode" and meta["label"] == function_name:
            fn_node_ids.add(nid)

    manual_output_types: list[str] = []
    for edge in all_edges:
        if edge["source"] in fn_node_ids:
            var_label = _node_id_to_var_label(
                edge["target"], set(), [], manual_nodes)
            if var_label and var_label not in manual_output_types:
                manual_output_types.append(var_label)

    if fn_variants and manual_output_types:
        # Override output types in DB-derived variants with the current wiring.
        Log.info(f"run: overriding DB output types with manual edge outputs: {manual_output_types}")
        overridden = []
        seen_constants = set()
        for v in fn_variants:
            key = tuple(sorted(v["constants"].items()))
            if key in seen_constants:
                continue
            seen_constants.add(key)
            for out in manual_output_types:
                overridden.append({
                    **v,
                    "output_type": out,
                })
        fn_variants = overridden

    if not fn_variants:
        # No DB history yet — try to infer inputs/outputs from manual edges.
        Log.info(f"run: all_edges = {all_edges}")
        Log.info(f"run: manual_nodes = {manual_nodes}")
        Log.info(f"run: fn_node_ids = {fn_node_ids}")

        input_types: dict[str, str] = {}  # param_name → type_name
        # Collect inputs that have no targetHandle so we can match by signature.
        unmatched_inputs: list[str] = []
        output_types: list[str] = []
        constant_names: set[str] = set()  # constant param names wired to this fn
        for edge in all_edges:
            if edge["source"] in fn_node_ids:
                var_label = _node_id_to_var_label(
                    edge["target"], set(), [], manual_nodes)
                if var_label and var_label not in output_types:
                    output_types.append(var_label)
            elif edge["target"] in fn_node_ids:
                src = edge["source"]
                th = edge.get("targetHandle") or ""
                # Check if source is a constant node.
                is_const = False
                const_label = None
                if src.startswith("const__"):
                    is_const = True
                    # Prefer the manual_nodes label (e.g. "perc") over the
                    # suffixed node ID (e.g. "const__perc__xd93hn").
                    src_meta = manual_nodes.get(src)
                    if src_meta:
                        const_label = src_meta["label"]
                    else:
                        # DB-derived constant: ID is "const__<name>" (no suffix).
                        const_label = src.replace("const__", "", 1)
                else:
                    src_meta = manual_nodes.get(src)
                    if src_meta and src_meta["type"] == "constantNode":
                        is_const = True
                        const_label = src_meta["label"]
                if is_const and const_label is not None:
                    # Determine param name from targetHandle or fall back to label.
                    if th.startswith("const__"):
                        constant_names.add(th.replace("const__", "", 1))
                    elif th.startswith("in__"):
                        constant_names.add(th.replace("in__", "", 1))
                    else:
                        constant_names.add(const_label)
                    continue
                var_label = _node_id_to_var_label(
                    src, set(), [], manual_nodes)
                if var_label:
                    if th.startswith("in__"):
                        input_types[th.replace("in__", "")] = var_label
                    else:
                        unmatched_inputs.append(var_label)

        Log.info(f"run: after edge scan: input_types={input_types}, output_types={output_types}, "
                 f"constant_names={constant_names}, unmatched_inputs={unmatched_inputs}")

        # Match unmatched inputs to function signature params by position.
        if unmatched_inputs:
            sig_params = _fn_params_from_registry(function_name)
            # Remove params already matched via targetHandle.
            remaining_params = [p for p in sig_params if p not in input_types]
            for param, var_type in zip(remaining_params, unmatched_inputs):
                input_types[param] = var_type
            if len(unmatched_inputs) > len(remaining_params):
                Log.warn(f"run: {len(unmatched_inputs)} input edges but only "
                         f"{len(remaining_params)} unmatched params for '{function_name}'")

        if not output_types:
            Log.warn(f"run: no outputs found for '{function_name}' from DB or edges")
            push_message({"type": "run_done", "run_id": run_id, "success": False,
                          "error": f"No pipeline history or output connections found for '{function_name}'. "
                                    "Connect it to an output variable node first."})
            return

        # Collect constant values from pending constants for wired constant nodes.
        import ast as _ast
        inferred_constants: dict[str, list] = {}  # const_name → list of typed values
        if constant_names:
            pending = pipeline_store.get_pending_constants(db)
            Log.info(f"run: pending constants = {pending}")
            for cname in constant_names:
                vals = pending.get(cname, set())
                typed_vals = []
                for v in vals:
                    try:
                        typed_vals.append(_ast.literal_eval(v))
                    except (ValueError, SyntaxError):
                        typed_vals.append(v)
                if typed_vals:
                    inferred_constants[cname] = typed_vals
                else:
                    Log.warn(f"run: constant '{cname}' wired to '{function_name}' but has no pending values")

        Log.info(f"run: inferred inputs={input_types} outputs={output_types} "
                 f"constants={list(inferred_constants.keys())} for '{function_name}' from edges")

        # Build synthetic variants: cross-product of output types × constant combos.
        if inferred_constants:
            # Build all combinations of constant values.
            from itertools import product as _product
            const_names_list = sorted(inferred_constants.keys())
            const_value_lists = [inferred_constants[c] for c in const_names_list]
            fn_variants = []
            for combo in _product(*const_value_lists):
                constants = dict(zip(const_names_list, combo))
                for out in output_types:
                    fn_variants.append({
                        "input_types": input_types,
                        "output_type": out,
                        "constants": constants,
                    })
        else:
            fn_variants = [
                {"input_types": input_types, "output_type": out, "constants": {}}
                for out in output_types
            ]

    # Determine which variants to run. If caller sent specific variants, filter;
    # otherwise run all known variants.
    if variants:
        targets = [v for v in fn_variants
                   if any(_constants_match(v["constants"], sel) for sel in variants)]
        if not targets:
            targets = fn_variants  # fallback: run all
    else:
        targets = fn_variants

    # Deduplicate by constants (list_pipeline_variants may return duplicates
    # across different output types for the same function).
    seen = set()
    unique_targets = []
    for v in targets:
        key = tuple(sorted(v["constants"].items()))
        if key not in seen:
            seen.add(key)
            unique_targets.append(v)

    # Add synthetic targets for pending constant values (variants the user
    # declared but hasn't run yet).  For each pending value we cross-product
    # with the existing combinations of all other constants, preserving their
    # original types.  The pending value itself is stored as a string, so we
    # coerce it back to a Python literal where possible.
    if fn_variants:
        import ast
        from scistack_gui import pipeline_store as _ps

        def _coerce(s: str):
            try:
                return ast.literal_eval(s)
            except (ValueError, SyntaxError):
                return s

        pending_consts = _ps.get_pending_constants(db)
        fn_const_names = {k for v in fn_variants for k in v["constants"]}
        pending_for_fn = {k: vals for k, vals in pending_consts.items()
                          if k in fn_const_names}

        if pending_for_fn:
            existing_keys = {
                tuple(sorted((k, str(v)) for k, v in t["constants"].items()))
                for t in unique_targets
            }
            template = fn_variants[0]

            for const_name, pending_values in pending_for_fn.items():
                # Collect unique combinations of other constants (typed).
                other_seen: set[tuple] = set()
                other_combos: list[dict] = []
                for v in fn_variants:
                    other = {k: val for k, val in v["constants"].items()
                             if k != const_name}
                    okey = tuple(sorted((k, str(val)) for k, val in other.items()))
                    if okey not in other_seen:
                        other_seen.add(okey)
                        other_combos.append(other)

                for pval_str in pending_values:
                    pval = _coerce(pval_str)
                    for other in other_combos:
                        new_constants = dict(other)
                        new_constants[const_name] = pval
                        key = tuple(sorted(
                            (k, str(v)) for k, v in new_constants.items()
                        ))
                        if key not in existing_keys:
                            existing_keys.add(key)
                            unique_targets.append({
                                "input_types": template["input_types"],
                                "constants": new_constants,
                                "output_type": template["output_type"],
                            })

    # Build schema kwargs: run on all existing values for each schema key.
    schema_kwargs = {
        key: db.distinct_schema_values(key)
        for key in db.dataset_schema_keys
    }

    success = True
    for v in unique_targets:
        # Build inputs dict: variable class inputs + scalar constants
        try:
            inputs = {}
            for param, type_name in v["input_types"].items():
                inputs[param] = registry.get_variable_class(type_name)
            inputs.update(v["constants"])   # add constants

            OutputCls = registry.get_variable_class(v["output_type"])
        except KeyError as e:
            emit(f"Error: {e}\n")
            success = False
            continue

        label = f"{function_name}({', '.join(f'{k}={val}' for k, val in v['constants'].items())})" \
                if v["constants"] else function_name
        emit(f"▶ Running {label}\n")

        # Capture for_each stdout and relay it line-by-line.
        buf = StringIO()
        try:
            with redirect_stdout(buf):
                for_each(fn, inputs=inputs, outputs=[OutputCls], **schema_kwargs)
            output = buf.getvalue()
            if output:
                emit(output)
        except Exception as exc:
            emit(f"Error: {exc}\n")
            success = False

    push_message({"type": "run_done", "run_id": run_id, "success": success})
    push_message({"type": "dag_updated"})


def _constants_match(db_constants: dict, selected: dict) -> bool:
    """True if selected is a subset of db_constants (value equality as strings)."""
    return all(str(db_constants.get(k)) == str(v) for k, v in selected.items())


@router.post("/run")
def start_run(req: RunRequest, db: DatabaseManager = Depends(get_db)):
    run_id = req.run_id or str(uuid.uuid4())[:8]
    thread = threading.Thread(
        target=_run_in_thread,
        args=(run_id, req.function_name, req.variants, db),
        daemon=True,
    )
    thread.start()
    return {"run_id": run_id}
