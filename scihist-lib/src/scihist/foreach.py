"""SciHist for_each — auto-wraps function in LineageFcn and records lineage."""

import logging
import sys
from typing import Any, Callable

logger = logging.getLogger(__name__)

def _diag(msg):
    """Temporary diagnostic print to file (bypasses capsys)."""
    with open("/tmp/scihist_diag.log", "a") as f:
        f.write(msg + "\n")
        f.flush()


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

    # Auto-wrap plain functions in LineageFcn
    if not isinstance(fn, LineageFcn):
        fn = LineageFcn(fn)

    # Wrap LineageFcn in a plain callable for scidb.for_each
    fn_plain = _make_plain(fn)

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

    # Delegate to scidb.for_each with save=False (we handle saves ourselves).
    # For generates_file functions, inject combo metadata as kwargs so fn receives
    # schema keys (subject, session, etc.) as named arguments.
    _inject_meta = getattr(fn, 'generates_file', False)
    result_tbl = _scidb_for_each(
        fn_plain,
        inputs,
        outputs,
        dry_run=dry_run,
        save=False,
        as_table=as_table,
        db=db,
        distribute=distribute,
        where=where,
        _inject_combo_metadata=_inject_meta,
        _pre_combo_hook=pre_combo_hook,
        **metadata_iterables,
    )

    if result_tbl is None:
        return None

    # Save with lineage
    if save and outputs and not result_tbl.empty:
        # Identify constant (non-variable, non-wrapper) inputs for version_keys.
        constant_inputs = {}
        # Resolve Fixed input record_ids for lineage rid_tracking.
        fixed_rids = {}
        for name, value in inputs.items():
            if isinstance(value, type):
                continue  # Variable type
            if hasattr(value, 'var_type') or hasattr(value, 'var_specs'):
                # Track Fixed inputs for rid_tracking in lineage.
                if hasattr(value, 'fixed_metadata'):
                    inner = value.var_type
                    if hasattr(inner, 'var_type'):
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
                continue  # Wrapper (Fixed, ColumnSelection, Merge, etc.)
            constant_inputs[name] = value

        _diag(f"[DIAG] save path: fixed_rids={fixed_rids}")
        output_names = [_output_name(o) for o in outputs]
        _save_with_lineage(result_tbl, outputs, output_names, db,
                           constant_inputs=constant_inputs,
                           fixed_input_rids=fixed_rids)

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
        constant_values[name] = value
        constant_hashes[name] = _chash(value)

    def _combo_str(schema_combo: dict) -> str:
        return ", ".join(f"{k}={v}" for k, v in sorted(schema_combo.items()))

    _diag(f"[DIAG] _build_skip_hook: fixed_inputs={list(fixed_inputs.keys())}, "
          f"constant_hashes={list(constant_hashes.keys())}")

    def _should_skip(combo: dict) -> bool:
        # Strip __rid_* and other internal keys — only schema keys for DB lookups.
        schema_combo = {k: v for k, v in combo.items()
                        if k in schema_keys}
        combo_str = _combo_str(schema_combo)

        # Current __rid_* values from the combo (freshly loaded inputs).
        combo_rids = {k: v for k, v in combo.items() if k.startswith("__rid_")}

        _diag(f"[DIAG] _should_skip: combo_str={combo_str}, combo_rids={list(combo_rids.keys())}")

        # Step 1: all outputs must exist.
        # Include constant values in lookup so variants are disambiguated.
        lookup_combo = dict(schema_combo)
        lookup_combo.update(constant_values)

        output_record_id = None
        for OutputCls in outputs:
            rid = db.find_record_id(OutputCls, lookup_combo)
            if rid is None:
                _diag(f"[DIAG] step1: output {OutputCls.__name__} NOT FOUND for {lookup_combo}")
                logger.debug("missing: %s — no output record for %s",
                             combo_str, OutputCls.__name__)
                return False  # output missing → compute
            output_record_id = rid
        _diag(f"[DIAG] step1: output found, record_id={output_record_id}")

        # Step 2: function hash check.
        stored_hash = db.get_function_hash_for_record(output_record_id)
        if stored_hash is None:
            msg = f"[recompute] {combo_str} — no lineage record"
            print(msg)
            logger.debug(msg)
            return False
        if stored_hash != fn.hash:
            msg = f"[recompute] {combo_str} — function hash changed"
            print(msg)
            logger.debug(msg)
            return False
        _diag(f"[DIAG] step2: function hash matches")

        # Step 3: compare __rid_* values against stored lineage inputs.
        if combo_rids:
            lineage_inputs = db.get_lineage_inputs(output_record_id)
            stored_rids = {}
            for inp in lineage_inputs:
                if inp.get("source_type") == "rid_tracking":
                    stored_rids[inp["name"]] = inp["record_id"]
            _diag(f"[DIAG] step3: combo_rids={combo_rids}, stored_rids={stored_rids}")
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
                    return False
                if str(rid_val) != str(stored_rid):
                    msg = f"[recompute] {combo_str} — {rid_key} changed"
                    print(msg)
                    logger.debug(msg)
                    return False
            _diag(f"[DIAG] step3: all combo_rids match")

        # Step 3b: compare Fixed input record_ids against stored lineage.
        if fixed_inputs:
            if not combo_rids:
                # Need to fetch lineage_inputs (step 3 skipped because no combo_rids).
                lineage_inputs = db.get_lineage_inputs(output_record_id)
                stored_rids = {}
                for inp in lineage_inputs:
                    if inp.get("source_type") == "rid_tracking":
                        stored_rids[inp["name"]] = inp["record_id"]
            _diag(f"[DIAG] step3b: fixed_inputs={list(fixed_inputs.keys())}, "
                  f"stored_rids={stored_rids}")
            for name, (inner_type, fixed_meta) in fixed_inputs.items():
                rid_key = f"__rid_{name}"
                # Look up the current record_id for this Fixed input.
                current_rid = db.find_record_id(inner_type, fixed_meta)
                _diag(f"[DIAG] step3b: {rid_key}: current_rid={current_rid}, "
                      f"stored_rid={stored_rids.get(rid_key)}")
                if current_rid is None:
                    msg = f"[recompute] {combo_str} — fixed input {name} not found"
                    print(msg)
                    logger.debug(msg)
                    return False
                stored_rid = stored_rids.get(rid_key)
                if stored_rid is None:
                    msg = f"[recompute] {combo_str} — no stored {rid_key}"
                    print(msg)
                    logger.debug(msg)
                    return False
                if str(current_rid) != str(stored_rid):
                    msg = f"[recompute] {combo_str} — {rid_key} changed"
                    print(msg)
                    logger.debug(msg)
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
                    return False
                if stored_hash is None and stored_const_hashes:
                    # New constant not in stored lineage → recompute.
                    msg = f"[recompute] {combo_str} — new constant {name}"
                    print(msg)
                    logger.debug(msg)
                    return False

        msg = f"[skip] {combo_str}"
        print(msg)
        logger.debug(msg)
        return True

    return _should_skip


def _make_plain(lineage_fn) -> Callable:
    """Wrap a LineageFcn in a plain function handle that returns LineageFcnResult."""
    def wrapped(*args, **kwargs):
        return lineage_fn(*args, **kwargs)
    wrapped.__name__ = getattr(lineage_fn, "__name__", "lineage_fcn")
    return wrapped


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
        _diag(f"[DIAG] _save_with_lineage: input_rids={input_rids}")
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
                if isinstance(output_value, LineageFcnResult):
                    _save_lineage_fcn_result(
                        output_obj, output_value, save_metadata, active_db,
                        input_rids=input_rids,
                    )
                else:
                    output_obj.save(output_value, **db_kwargs, **save_metadata)

                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                logger.debug("save: %s: %s", meta_str, _output_name(output_obj))
            except Exception as e:
                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                logger.error("save failed: %s: %s: %s", meta_str, _output_name(output_obj), e)


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

        output_name = output_obj.__name__ if isinstance(output_obj, type) else type(output_obj).__name__
        schema_keys = nested_metadata.get("schema", {})
        version_keys = nested_metadata.get("version", {})
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
        return generated_id

    lineage_record = extract_lineage(data)
    lineage_dict = _lineage_to_dict(lineage_record)
    _append_rid_tracking(lineage_dict, input_rids)
    lineage_hash = data.hash
    pipeline_lineage_hash = data.invoked.compute_lineage_hash()
    raw_data = get_raw_value(data)

    variable_class = output_obj if isinstance(output_obj, type) else type(output_obj)
    instance = variable_class(raw_data)
    return active_db.save(
        instance,
        metadata,
        lineage=lineage_dict,
        lineage_hash=lineage_hash,
        pipeline_lineage_hash=pipeline_lineage_hash,
    )


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

    if isinstance(data, LineageFcnResult):
        return _save_lineage_fcn_result(variable_class, data, metadata, db)
    else:
        db_kwargs = {"db": db} if db is not None else {}
        return variable_class.save(data, **db_kwargs, **metadata)


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
