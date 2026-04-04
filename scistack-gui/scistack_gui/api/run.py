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

    if not fn_variants:
        push_message({"type": "run_done", "run_id": run_id, "success": False,
                      "error": f"No pipeline history found for '{function_name}'. "
                                "Run it manually first to establish the pipeline structure."})
        return

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
