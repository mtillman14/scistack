"""SciHist for_each — auto-wraps function in LineageFcn and records lineage."""

from typing import Any, Callable


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
            pre_combo_hook = _build_skip_hook(fn, outputs, active_db)

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
        output_names = [_output_name(o) for o in outputs]
        _save_with_lineage(result_tbl, outputs, output_names, db)

    return result_tbl


def _build_skip_hook(fn: "LineageFcn", outputs: list, db) -> Callable[[dict], bool]:
    """Return a pre-combo hook that returns True when a combo can be skipped.

    A combo is skipped when:
    1. Every output type already has a record for this combo.
    2. Walking get_upstream_provenance() from the output record, every
       function hash still matches and every input record_id in _lineage.inputs
       matches the current latest record for that variable variant.
    """
    schema_keys: set = set(db.dataset_schema_keys)

    def _combo_str(schema_combo: dict) -> str:
        return ", ".join(f"{k}={v}" for k, v in sorted(schema_combo.items()))

    def _should_skip(combo: dict) -> bool:
        # Strip __rid_* and other internal keys — only schema keys for DB lookups.
        schema_combo = {k: v for k, v in combo.items()
                        if k in schema_keys}

        # Step 1: all outputs must exist.
        output_record_id = None
        for OutputCls in outputs:
            rid = db.find_record_id(OutputCls, schema_combo)
            if rid is None:
                return False  # output missing → compute
            output_record_id = rid  # use the last output's record for provenance

        # Step 2: walk the full upstream provenance graph.
        try:
            nodes = db.get_upstream_provenance(output_record_id)
        except Exception:
            return False  # provenance lookup failed → compute to be safe

        for node in nodes:
            node_rid = node["record_id"]

            # a. Function hash check (only meaningful for computed nodes).
            if node["depth"] == 0:
                # This is the output node — compare against the function we're
                # about to run.
                stored_hash = db.get_function_hash_for_record(node_rid)
                if stored_hash is None:
                    # No lineage record: output was not saved via scihist.
                    # Cannot verify provenance → recompute.
                    return False
                if stored_hash != fn.hash:
                    print(f"[recompute] {_combo_str(schema_combo)} — function changed")
                    return False

            # b. Input record_id check via _lineage.inputs.
            lineage_inputs = db.get_lineage_inputs(node_rid)
            for inp in lineage_inputs:
                if inp.get("source_type") != "variable":
                    continue  # thunks / constants handled by hash check
                used_rid = inp.get("record_id")
                if not used_rid:
                    continue
                current_rid = db.get_latest_record_id_for_variant(used_rid)
                if current_rid != used_rid:
                    var_type = inp.get("type", "unknown")
                    print(f"[recompute] {_combo_str(schema_combo)} — "
                          f"upstream {var_type} updated")
                    return False

        print(f"[skip] {_combo_str(schema_combo)}")
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
        save_metadata = {col: row[col] for col in meta_cols}

        for output_obj, output_name in zip(outputs, output_names):
            if output_name not in row.index:
                continue
            output_value = row[output_name]

            try:
                if isinstance(output_value, LineageFcnResult):
                    _save_lineage_fcn_result(
                        output_obj, output_value, save_metadata, active_db
                    )
                else:
                    output_obj.save(output_value, **db_kwargs, **save_metadata)

                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                print(f"[save] {meta_str}: {_output_name(output_obj)}")
            except Exception as e:
                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                print(f"[error] {meta_str}: failed to save {_output_name(output_obj)}: {e}")


def _save_lineage_fcn_result(
    output_obj: Any,
    data: "LineageFcnResult",
    metadata: dict,
    db: Any | None,
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
        pipeline_lineage_hash = data.invoked.compute_lineage_hash()
        generated_id = f"generated:{pipeline_lineage_hash[:32]}"
        user_id = get_user_id()
        nested_metadata = active_db._split_metadata(metadata)

        output_name = output_obj.__name__ if isinstance(output_obj, type) else type(output_obj).__name__
        active_db._save_record_metadata(
            record_id=generated_id,
            timestamp=datetime.now().isoformat(),
            variable_name=output_name,
            schema_id=0,
            version_keys=None,
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


def _lineage_to_dict(lineage_record) -> dict:
    """Convert a scilineage.LineageRecord to the dict format scidb expects."""
    return {
        "function_name": lineage_record.function_name,
        "function_hash": lineage_record.function_hash,
        "inputs": lineage_record.inputs,
        "constants": lineage_record.constants,
    }
