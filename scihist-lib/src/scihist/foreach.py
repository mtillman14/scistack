"""SciHist for_each — auto-wraps function in LineageFcn and records lineage."""

import json
import logging
import time
import sys
from typing import Any, Callable

logger = logging.getLogger(__name__)

try:
    from scidb.log import Log as _Log
except ImportError:
    _Log = None


def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[type],
    dry_run: bool = False,
    save: bool = True,
    as_table: list[str] | bool | None = None,
    db=None,
    distribute: bool = False,
    where=None,
    skip_computed: bool = True,
    _progress_fn: "Callable[[dict], None] | None" = None,
    _cancel_check: "Callable[[], bool] | None" = None,
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | None":
    """
    Execute a function for all combinations of metadata, with lineage tracking.

    This is the scihist (Layer 3) wrapper. It auto-wraps plain functions in
    LineageFcn so that lineage is recorded, then delegates to scidb.for_each
    with save=False. After scidb.for_each returns, it saves each output via
    scihist's lineage-aware save (which calls scidb.save() with the extracted
    lineage dict).

    Args:
        fn: The function to execute. If not already a LineageFcn, it is wrapped
            automatically.
        inputs: Dict mapping parameter names to variable types, Fixed wrappers,
                Merge wrappers, ColumnSelection wrappers, PathInput, or constants.
        outputs: List of output types/objects with ``.save()``.
        dry_run: If True, only print what would happen without executing.
        save: If True (default), save each function run's output with lineage.
        as_table: Controls which inputs are passed as full DataFrames.
        db: Optional database instance.
        distribute: If True, split outputs and save each piece at the schema
                    level below the deepest iterated key.
        where: Optional filter; passed to .load() calls on DB-backed inputs.
        skip_computed: If True (default), skip combos whose outputs already exist
                    and whose full upstream provenance graph is unchanged. Pass
                    False to force re-computation of every combo.
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, or None when dry_run=True.
    """
    from scilineage import LineageFcn
    from scidb.foreach import for_each as _scidb_for_each, _output_name

    fn_name = getattr(fn, "__name__", repr(fn))
    if _Log:
        _Log.info(f"scihist.for_each({fn_name}): skip_computed={skip_computed}")

    # Auto-wrap plain functions in LineageFcn
    if not isinstance(fn, LineageFcn):
        fn = LineageFcn(fn)
        logger.info("auto-wrapped %s in LineageFcn (hash=%s)", fn_name, fn.hash[:12])
    else:
        logger.debug("%s is already a LineageFcn (hash=%s)", fn_name, fn.hash[:12])

    # Wrap LineageFcn in a plain callable for scidb.for_each
    from scilineage import make_tuple_unpacking_wrapper
    fn_plain = make_tuple_unpacking_wrapper(fn)

    # Build the pre-combo skip hook when skip_computed is enabled.
    pre_combo_hook = None
    if skip_computed and not dry_run and outputs:
        active_db = db
        if active_db is None:
            try:
                from scidb.database import get_database
                active_db = get_database()
            except Exception:
                active_db = None
        if active_db is not None:
            pre_combo_hook = _build_skip_hook(fn, outputs, active_db, inputs)
            logger.debug("built skip_computed hook for %s", fn_name)
        else:
            logger.debug("skip_computed disabled: no database available")
    elif not skip_computed:
        logger.debug("skip_computed disabled by caller")
    elif dry_run:
        logger.debug("skip_computed disabled: dry_run=True")
    elif not outputs:
        logger.debug("skip_computed disabled: no outputs specified")

    # Compute Fixed input record_ids for lineage tracking before delegating to scidb.
    # These are needed for staleness checking but scidb doesn't include them in
    # __upstream (Fixed inputs have __record_id stripped for variant expansion).
    fixed_rids = {}
    if save and outputs:
        for name, value in inputs.items():
            if hasattr(value, 'fixed_metadata'):
                inner = value.var_type if hasattr(value, 'var_type') else value
                if hasattr(inner, 'var_type'):  # Unwrap ColumnSelection
                    inner = inner.var_type
                if isinstance(inner, type):
                    save_db = db
                    if save_db is None:
                        try:
                            from scidb.database import get_database
                            save_db = get_database()
                        except Exception:
                            save_db = None
                    if save_db is not None:
                        rid = save_db.find_record_id(inner, value.fixed_metadata)
                        if rid:
                            fixed_rids[f"__rid_{name}"] = rid
        logger.debug("computed %d fixed_rids for lineage tracking", len(fixed_rids))

    # Delegate to scidb.for_each with save=True (scidb will detect LineageFcnResult
    # and call save_lineage_result with complete metadata).
    # For generates_file functions, inject combo metadata as kwargs so fn receives
    # schema keys (subject, session, etc.) as named arguments.
    _inject_meta = getattr(fn, 'generates_file', False)
    logger.debug("delegating to scidb.for_each (save=%s, distribute=%s)", save, distribute)
    result_tbl = _scidb_for_each(
        fn_plain,
        inputs,
        outputs,
        dry_run=dry_run,
        save=save,
        as_table=as_table,
        db=db,
        distribute=distribute,
        where=where,
        _inject_combo_metadata=_inject_meta,
        _pre_combo_hook=pre_combo_hook,
        _progress_fn=_progress_fn,
        _cancel_check=_cancel_check,
        _lineage_fixed_rids=fixed_rids if fixed_rids else None,
        **metadata_iterables,
    )

    if result_tbl is None:
        logger.info("scidb.for_each returned None (dry_run)")
        return None

    logger.info("scidb.for_each returned %d rows", len(result_tbl))
    return result_tbl


def _build_skip_hook(fn: "LineageFcn", outputs: list, db, inputs: dict) -> Callable[[dict], bool]:
    """Return a pre-combo hook that returns True when a combo can be skipped.

    A combo is skipped when:
    1. Every output type already has a record for this combo's metadata.
    2. The function hash stored in lineage matches the current function's hash.
    3. The combo's ``__rid_*`` values match those stored in the output record's
       lineage inputs (as ``rid_tracking`` entries) — meaning all upstream
       inputs are unchanged.
    4. Constant input hashes match those stored in the output record's lineage.
    """
    from canonicalhash import canonical_hash as _chash

    schema_keys: set = set(db.dataset_schema_keys)

    # Pre-compute constant inputs (non-variable, non-wrapper) with their hashes.
    constant_values: dict = {}
    constant_hashes: dict[str, str] = {}
    # Collect Fixed inputs for record_id tracking.
    fixed_inputs: dict[str, tuple] = {}  # name -> (inner_type, fixed_metadata)
    try:
        from scifor import PathInput as _PathInput
    except ImportError:
        _PathInput = None
    for name, value in inputs.items():
        if isinstance(value, type):
            continue  # Variable type (BaseVariable subclass)
        if hasattr(value, 'var_type') or hasattr(value, 'var_specs'):
            # Check if it's a Fixed wrapper — track it for rid comparison.
            if hasattr(value, 'fixed_metadata'):
                inner = value.var_type
                # Unwrap ColumnSelection if present
                if hasattr(inner, 'var_type'):
                    inner = inner.var_type
                if isinstance(inner, type):
                    fixed_inputs[name] = (inner, value.fixed_metadata)
            continue  # Wrapper (Fixed, ColumnSelection, Merge, etc.)
        if _PathInput is not None and isinstance(value, _PathInput):
            continue  # PathInput — resolved per-combo by scidb.for_each
        constant_values[name] = value
        constant_hashes[name] = _chash(value)

    def _combo_str(schema_combo: dict) -> str:
        return ", ".join(f"{k}={v}" for k, v in sorted(schema_combo.items()))

    logger.debug("_build_skip_hook: fixed_inputs=%s, constant_hashes=%s",
                  list(fixed_inputs.keys()), list(constant_hashes.keys()))

    def _should_skip(combo: dict) -> bool:
        # Strip __rid_* and other internal keys — only schema keys for DB lookups.
        schema_combo = {k: v for k, v in combo.items()
                        if k in schema_keys}
        combo_str = _combo_str(schema_combo)

        # Current __rid_* values from the combo (freshly loaded inputs).
        combo_rids = {k: v for k, v in combo.items() if k.startswith("__rid_")}

        logger.debug("_should_skip: combo_str=%s, combo_rids=%s", combo_str, list(combo_rids.keys()))

        # Step 1: all outputs must exist.
        # Include constant values and __fn/__fn_hash in lookup so variants
        # are disambiguated (matches version_keys written by save path).
        lookup_combo = dict(schema_combo)
        lookup_combo.update(constant_values)
        lookup_combo["__fn"] = fn.fcn.__name__
        lookup_combo["__fn_hash"] = fn.hash

        output_record_id = None
        for OutputCls in outputs:
            try:
                rid = db.find_record_id(OutputCls, lookup_combo)
            except (KeyError, Exception):
                rid = None
            if rid is None:
                logger.debug("step1: output %s NOT FOUND for %s", OutputCls.__name__, lookup_combo)
                logger.debug("missing: %s — no output record for %s",
                             combo_str, OutputCls.__name__)
                return False  # output missing → compute
            output_record_id = rid
        logger.debug("step1: output found, record_id=%s", output_record_id)

        # Step 2: function hash check.
        stored_hash = db.get_function_hash_for_record(output_record_id)
        if stored_hash is None:
            msg = f"[recompute] {combo_str} — no lineage record"
            print(msg)
            logger.debug(msg)
            if _Log:
                _Log.info(msg)
            return False
        if stored_hash != fn.hash:
            msg = f"[recompute] {combo_str} — function hash changed"
            print(msg)
            logger.debug(msg)
            if _Log:
                _Log.info(msg)
            return False
        logger.debug("step2: function hash matches")

        # Step 3: compare __rid_* values against stored lineage inputs.
        if combo_rids:
            lineage_inputs = db.get_lineage_inputs(output_record_id)
            stored_rids = {}
            for inp in lineage_inputs:
                if inp.get("source_type") == "rid_tracking":
                    stored_rids[inp["name"]] = inp["record_id"]
            logger.debug("step3: combo_rids=%s, stored_rids=%s", combo_rids, stored_rids)
            for rid_key, rid_val in combo_rids.items():
                # Self-referential case: the loaded "input" IS the output
                # record (input type == output type). Pipeline is stable.
                if str(rid_val) == str(output_record_id):
                    continue
                stored_rid = stored_rids.get(rid_key)
                if stored_rid is None:
                    # Output was saved without __rid tracking → recompute.
                    msg = f"[recompute] {combo_str} — no stored {rid_key}"
                    print(msg)
                    logger.debug(msg)
                    if _Log:
                        _Log.info(msg)
                    return False
                if str(rid_val) != str(stored_rid):
                    msg = f"[recompute] {combo_str} — {rid_key} changed"
                    print(msg)
                    logger.debug(msg)
                    if _Log:
                        _Log.info(msg)
                    return False
            logger.debug("step3: all combo_rids match")

        # Step 3b: compare Fixed input record_ids against stored lineage.
        if fixed_inputs:
            if not combo_rids:
                # Need to fetch lineage_inputs (step 3 skipped because no combo_rids).
                lineage_inputs = db.get_lineage_inputs(output_record_id)
                stored_rids = {}
                for inp in lineage_inputs:
                    if inp.get("source_type") == "rid_tracking":
                        stored_rids[inp["name"]] = inp["record_id"]
            logger.debug("step3b: fixed_inputs=%s, stored_rids=%s",
                          list(fixed_inputs.keys()), stored_rids)
            for name, (inner_type, fixed_meta) in fixed_inputs.items():
                rid_key = f"__rid_{name}"
                # Look up the current record_id for this Fixed input.
                current_rid = db.find_record_id(inner_type, fixed_meta)
                logger.debug("step3b: %s: current_rid=%s, stored_rid=%s",
                              rid_key, current_rid, stored_rids.get(rid_key))
                if current_rid is None:
                    msg = f"[recompute] {combo_str} — fixed input {name} not found"
                    print(msg)
                    logger.debug(msg)
                    if _Log:
                        _Log.info(msg)
                    return False
                stored_rid = stored_rids.get(rid_key)
                if stored_rid is None:
                    msg = f"[recompute] {combo_str} — no stored {rid_key}"
                    print(msg)
                    logger.debug(msg)
                    if _Log:
                        _Log.info(msg)
                    return False
                if str(current_rid) != str(stored_rid):
                    msg = f"[recompute] {combo_str} — {rid_key} changed"
                    print(msg)
                    logger.debug(msg)
                    if _Log:
                        _Log.info(msg)
                    return False

        # Step 4: compare constant input hashes against stored lineage.
        if constant_hashes:
            stored_constants = db.get_lineage_constants(output_record_id)
            stored_const_hashes = {
                c["name"]: c["value_hash"]
                for c in stored_constants
                if "name" in c and "value_hash" in c
            }
            for name, current_hash in constant_hashes.items():
                stored_hash = stored_const_hashes.get(name)
                if stored_hash is not None and stored_hash != current_hash:
                    msg = f"[recompute] {combo_str} — constant {name} changed"
                    print(msg)
                    logger.debug(msg)
                    if _Log:
                        _Log.info(msg)
                    return False
                if stored_hash is None and stored_const_hashes:
                    # New constant not in stored lineage → recompute.
                    msg = f"[recompute] {combo_str} — new constant {name}"
                    print(msg)
                    logger.debug(msg)
                    if _Log:
                        _Log.info(msg)
                    return False

        msg = f"[skip] {combo_str}"
        print(msg)
        logger.debug(msg)
        if _Log:
            _Log.info(msg)
        return True

    return _should_skip




def _save_with_lineage(
    result_tbl: "pd.DataFrame",
    outputs: list[Any],
    output_names: list[str],
    db: Any | None,
    constant_inputs: dict | None = None,
    fixed_input_rids: dict | None = None,
) -> None:
    """Save results with lineage tracking, extracting lineage from LineageFcnResult."""
    from scilineage import LineageFcnResult
    from scidb.foreach import _output_name
    from scidb.database import get_database

    active_db = db
    if active_db is None:
        try:
            active_db = get_database()
        except Exception:
            pass

    db_kwargs = {"db": active_db} if active_db is not None else {}

    # Determine which columns are metadata (not output names)
    meta_cols = [c for c in result_tbl.columns if c not in output_names]

    for _, row in result_tbl.iterrows():
        raw_metadata = {col: row[col] for col in meta_cols}

        # Extract __rid_* for lineage tracking; strip __ keys from save metadata
        # (matching scidb.for_each's _save_results behaviour).
        input_rids = {k: str(v) for k, v in raw_metadata.items()
                      if k.startswith("__rid_")}
        # Merge Fixed input record_ids into rid tracking.
        if fixed_input_rids:
            input_rids.update(fixed_input_rids)
        logger.debug("_save_with_lineage: input_rids=%s", input_rids)
        save_metadata = {k: v for k, v in raw_metadata.items()
                         if not k.startswith("__")}

        # Add constant inputs as version keys for variant disambiguation.
        if constant_inputs:
            save_metadata.update(constant_inputs)

        for output_obj, output_name in zip(outputs, output_names):
            if output_name not in row.index:
                continue
            output_value = row[output_name]

            try:
                save_t0 = time.perf_counter()
                if isinstance(output_value, LineageFcnResult):
                    rid = _save_lineage_fcn_result(
                        output_obj, output_value, save_metadata, active_db,
                        input_rids=input_rids,
                    )
                else:
                    rid = output_obj.save(output_value, **db_kwargs, **save_metadata)
                save_elapsed = time.perf_counter() - save_t0

                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                out_name = _output_name(output_obj)
                rid_short = str(rid)[:12] if rid else "None"
                lineage_tag = " (lineage)" if isinstance(output_value, LineageFcnResult) else ""
                msg = f"[save] {meta_str}: {out_name}{lineage_tag} -> record_id={rid_short} in {save_elapsed:.3f}s"
                logger.debug(msg)
                if _Log:
                    _Log.info(msg)
            except Exception as e:
                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                out_name = _output_name(output_obj)
                msg = f"[error] {meta_str}: save failed for {out_name}: {e}"
                logger.error(msg)
                if _Log:
                    _Log.error(msg)


def _save_lineage_fcn_result(
    output_obj: Any,
    data: "LineageFcnResult",
    metadata: dict,
    db: Any | None,
    input_rids: dict | None = None,
) -> str | None:
    """Save a LineageFcnResult with full lineage tracking."""
    from scilineage import LineageFcnResult, extract_lineage, get_raw_value
    from scidb.database import get_database, get_user_id
    from datetime import datetime

    output_name = output_obj.__name__ if isinstance(output_obj, type) else type(output_obj).__name__
    fn_name = data.invoked.fcn.fn.__name__ if hasattr(data.invoked.fcn, 'fn') else "unknown"
    logger.debug("_save_lineage_fcn_result entry: output=%s, fn=%s, generates_file=%s, metadata=%s",
                 output_name, fn_name, data.invoked.fcn.generates_file, metadata)
    t0 = time.time()

    try:
        active_db = db
        if active_db is None:
            active_db = get_database()

        # Lineage-only save for side-effect functions (generates_file=True)
        if data.invoked.fcn.generates_file:
            lineage_record = extract_lineage(data)
            lineage_dict = _lineage_to_dict(lineage_record)
            _append_rid_tracking(lineage_dict, input_rids)
            pipeline_lineage_hash = data.invoked.compute_lineage_hash()
            generated_id = f"generated:{pipeline_lineage_hash[:32]}"
            user_id = get_user_id()
            nested_metadata = active_db._split_metadata(metadata)

            schema_keys = nested_metadata.get("schema", {})
            version_keys = nested_metadata.get("version", {})
            version_keys["__fn"] = lineage_dict.get("function_name", fn_name)
            version_keys["__fn_hash"] = lineage_dict.get("function_hash", "")
            schema_level = active_db._infer_schema_level(schema_keys)
            schema_id = (
                active_db._duck._get_or_create_schema_id(schema_level, schema_keys)
                if schema_level is not None and schema_keys
                else 0
            )
            active_db._save_record_metadata(
                record_id=generated_id,
                timestamp=datetime.now().isoformat(),
                variable_name=output_name,
                schema_id=schema_id,
                version_keys=version_keys or None,
                content_hash=None,
                lineage_hash=pipeline_lineage_hash,
                schema_version=getattr(output_obj, 'schema_version', 1),
                user_id=user_id,
            )
            active_db._save_lineage(
                output_record_id=generated_id,
                output_type=output_name,
                lineage=lineage_dict,
                lineage_hash=pipeline_lineage_hash,
                user_id=user_id,
                schema_keys=nested_metadata.get("schema"),
                output_content_hash=None,
            )
            elapsed = time.time() - t0
            logger.debug("_save_lineage_fcn_result exit: output=%s, record_id=%s (generates_file), elapsed=%.3fs",
                         output_name, generated_id[:12], elapsed)
            if _Log:
                stored_fn_hash = lineage_dict.get("function_hash") or ""
                _Log.info(f"[save-lineage] {output_name}: record_id={generated_id[:12]} function_hash={stored_fn_hash[:12] or 'None'} (generates_file)")
                _Log.debug(f"[save-lineage] {output_name}: pipeline_lineage_hash={pipeline_lineage_hash[:12]}")
            return generated_id

        lineage_record = extract_lineage(data)
        lineage_dict = _lineage_to_dict(lineage_record)
        _append_rid_tracking(lineage_dict, input_rids)
        lineage_hash = data.hash
        pipeline_lineage_hash = data.invoked.compute_lineage_hash()
        raw_data = get_raw_value(data)

        variable_class = output_obj if isinstance(output_obj, type) else type(output_obj)
        instance = variable_class(raw_data)
        fn_metadata = dict(metadata)
        fn_metadata["__fn"] = lineage_dict.get("function_name", fn_name)
        fn_metadata["__fn_hash"] = lineage_dict.get("function_hash", "")
        rid = active_db.save(
            instance,
            fn_metadata,
            lineage=lineage_dict,
            lineage_hash=lineage_hash,
            pipeline_lineage_hash=pipeline_lineage_hash,
        )
        elapsed = time.time() - t0
        logger.debug("_save_lineage_fcn_result exit: output=%s, record_id=%s, elapsed=%.3fs",
                     output_name, rid[:12] if rid else None, elapsed)
        if _Log and rid:
            stored_fn_hash = lineage_dict.get("function_hash") or ""
            _Log.info(f"[save-lineage] {output_name}: record_id={rid[:12]} function_hash={stored_fn_hash[:12] or 'None'}")
            _Log.debug(f"[save-lineage] {output_name}: lineage_hash={lineage_hash[:12] if lineage_hash else 'None'}, pipeline_lineage_hash={pipeline_lineage_hash[:12]}")
        return rid
    except Exception:
        elapsed = time.time() - t0
        logger.exception("_save_lineage_fcn_result FAILED: output=%s, fn=%s, elapsed=%.3fs",
                         output_name, fn_name, elapsed)
        raise


def save_lineage_result(
    output_obj: Any,
    lineage_result: "LineageFcnResult",
    metadata: dict,
    db: Any | None,
) -> str | None:
    """Save a LineageFcnResult with lineage tracking.

    This function is called by scidb.for_each when it detects a LineageFcnResult.
    It receives pre-built metadata from scidb (including version_keys and branch_params)
    and adds lineage-specific information.

    Args:
        output_obj: The output variable class
        lineage_result: The LineageFcnResult containing data and lineage info
        metadata: Pre-built metadata from scidb (includes __fn, __fn_hash,
                  __inputs, __constants, __branch_params, __upstream)
        db: Database instance (optional)

    Returns:
        record_id of the saved output
    """
    from scilineage import extract_lineage, get_raw_value
    from scidb.database import get_database
    from datetime import datetime

    output_name = output_obj.__name__ if isinstance(output_obj, type) else type(output_obj).__name__
    fn_name = lineage_result.invoked.fcn.fn.__name__ if hasattr(lineage_result.invoked.fcn, 'fn') else "unknown"
    logger.debug("save_lineage_result entry: output=%s, fn=%s, generates_file=%s",
                 output_name, fn_name, lineage_result.invoked.fcn.generates_file)

    active_db = db if db is not None else get_database()

    # Extract input_rids from __upstream in metadata (for rid_tracking)
    input_rids = {}
    if "__upstream" in metadata:
        try:
            # Handle both dict (new format) and JSON string (old format) for backward compatibility
            upstream_val = metadata["__upstream"]
            if isinstance(upstream_val, dict):
                input_rids = upstream_val
            else:
                input_rids = json.loads(upstream_val)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Failed to parse __upstream for rid tracking")

    # Merge in Fixed input record_ids from scihist (for staleness tracking)
    if "__lineage_fixed_rids" in metadata:
        fixed_rids = metadata.get("__lineage_fixed_rids", {})
        if fixed_rids:
            input_rids.update(fixed_rids)
            logger.debug("Merged %d fixed_rids into input_rids", len(fixed_rids))

    # Extract lineage and append rid_tracking entries
    lineage_record = extract_lineage(lineage_result)
    lineage_dict = _lineage_to_dict(lineage_record)
    _append_rid_tracking(lineage_dict, input_rids)

    # Handle generates_file case (lineage-only save)
    if lineage_result.invoked.fcn.generates_file:
        from scidb.database import get_user_id
        pipeline_lineage_hash = lineage_result.invoked.compute_lineage_hash()
        generated_id = f"generated:{pipeline_lineage_hash[:32]}"
        user_id = get_user_id()
        nested_metadata = active_db._split_metadata(metadata)

        schema_keys = nested_metadata.get("schema", {})
        version_keys = nested_metadata.get("version", {})
        # version_keys already contains __fn, __fn_hash from scidb
        schema_level = active_db._infer_schema_level(schema_keys)
        schema_id = (
            active_db._duck._get_or_create_schema_id(schema_level, schema_keys)
            if schema_level is not None and schema_keys
            else 0
        )
        active_db._save_record_metadata(
            record_id=generated_id,
            timestamp=datetime.now().isoformat(),
            variable_name=output_name,
            schema_id=schema_id,
            version_keys=version_keys or None,
            content_hash=None,
            lineage_hash=pipeline_lineage_hash,
            schema_version=getattr(output_obj, 'schema_version', 1),
            user_id=user_id,
        )
        active_db._save_lineage(
            output_record_id=generated_id,
            output_type=output_name,
            lineage=lineage_dict,
            lineage_hash=pipeline_lineage_hash,
            user_id=user_id,
            schema_keys=nested_metadata.get("schema"),
            output_content_hash=None,
        )
        logger.debug("save_lineage_result exit: record_id=%s (generates_file)", generated_id[:12])
        return generated_id

    # Normal case: save data + lineage
    lineage_hash = lineage_result.hash
    pipeline_lineage_hash = lineage_result.invoked.compute_lineage_hash()
    raw_data = get_raw_value(lineage_result)

    variable_class = output_obj if isinstance(output_obj, type) else type(output_obj)
    instance = variable_class(raw_data)

    # Use pre-built metadata from scidb (already contains version_keys and branch_params)
    rid = active_db.save(
        instance,
        metadata,
        lineage=lineage_dict,
        lineage_hash=lineage_hash,
        pipeline_lineage_hash=pipeline_lineage_hash,
    )

    logger.debug("save_lineage_result exit: record_id=%s", rid[:12] if rid else None)
    return rid


def save(variable_class, data, db=None, **metadata) -> str | None:
    """Save data to the database with lineage tracking.

    This is the scihist-level save that handles LineageFcnResult:
    - If ``data`` is a LineageFcnResult, extracts lineage and saves with full
      provenance tracking.
    - Otherwise, delegates to ``variable_class.save(data, **metadata)``.

    Args:
        variable_class: The BaseVariable subclass to save as.
        data: The data to save. Can be a LineageFcnResult or raw data.
        db: Optional database instance.
        **metadata: Addressing metadata (e.g., subject=1, trial=1).

    Returns:
        str: The record_id of the saved data.
    """
    from scilineage import LineageFcnResult

    var_name = variable_class.__name__ if isinstance(variable_class, type) else type(variable_class).__name__
    is_lineage = isinstance(data, LineageFcnResult)
    logger.debug("save() entry: variable=%s, is_lineage_result=%s, metadata_keys=%s",
                 var_name, is_lineage, list(metadata.keys()))

    if is_lineage:
        rid = _save_lineage_fcn_result(variable_class, data, metadata, db)
        logger.debug("save() exit: variable=%s, record_id=%s (lineage path)",
                     var_name, rid[:12] if rid else None)
        return rid
    else:
        db_kwargs = {"db": db} if db is not None else {}
        rid = variable_class.save(data, **db_kwargs, **metadata)
        logger.debug("save() exit: variable=%s, record_id=%s (plain path)",
                     var_name, rid[:12] if rid else None)
        return rid


def _append_rid_tracking(lineage_dict: dict, input_rids: dict | None) -> None:
    """Append __rid_* entries to lineage inputs for skip_computed tracking."""
    if not input_rids:
        return
    for rid_key, rid_val in input_rids.items():
        lineage_dict["inputs"].append({
            "name": rid_key,
            "source_type": "rid_tracking",
            "record_id": str(rid_val),
        })


def _lineage_to_dict(lineage_record) -> dict:
    """Convert a scilineage.LineageRecord to the dict format scidb expects."""
    return {
        "function_name": lineage_record.function_name,
        "function_hash": lineage_record.function_hash,
        "inputs": lineage_record.inputs,
        "constants": lineage_record.constants,
    }
