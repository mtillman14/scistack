"""SciHist for_each — auto-wraps function in Thunk and records lineage."""

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
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | None":
    """
    Execute a function for all combinations of metadata, with lineage tracking.

    This is the scihist (Layer 3) wrapper. It auto-wraps plain functions in
    Thunk so that lineage is recorded, then delegates to scidb.for_each with
    save=False. After scidb.for_each returns, it saves each output via
    scihist's lineage-aware save (which calls scidb.save() with the extracted
    lineage dict).

    Args:
        fn: The function to execute. If not already a Thunk, it is wrapped
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
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, or None when dry_run=True.
    """
    from thunk import Thunk
    from scidb.foreach import for_each as _scidb_for_each, _output_name

    # Auto-wrap plain functions in Thunk
    if not isinstance(fn, Thunk):
        fn = Thunk(fn)

    # Wrap Thunk in a plain callable for scidb.for_each
    fn_plain = _make_plain(fn)

    # Delegate to scidb.for_each with save=False (we handle saves ourselves)
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
        **metadata_iterables,
    )

    if result_tbl is None:
        return None

    # Save with lineage
    if save and outputs and not result_tbl.empty:
        output_names = [_output_name(o) for o in outputs]
        _save_with_lineage(result_tbl, outputs, output_names, db)

    return result_tbl


def _make_plain(thunk_fn) -> Callable:
    """Wrap a Thunk in a plain function handle that returns ThunkOutput."""
    def wrapped(*args, **kwargs):
        return thunk_fn(*args, **kwargs)
    wrapped.__name__ = getattr(thunk_fn, "__name__", "thunk")
    return wrapped


def _save_with_lineage(
    result_tbl: "pd.DataFrame",
    outputs: list[Any],
    output_names: list[str],
    db: Any | None,
) -> None:
    """Save results with lineage tracking, extracting lineage from ThunkOutput."""
    from thunk import ThunkOutput
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
                if isinstance(output_value, ThunkOutput):
                    _save_thunk_output(
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


def _save_thunk_output(
    output_obj: Any,
    data: "ThunkOutput",
    metadata: dict,
    db: Any | None,
) -> str | None:
    """Save a ThunkOutput with full lineage tracking."""
    from thunk import ThunkOutput, extract_lineage, find_unsaved_variables, get_raw_value
    from scidb.database import get_database, get_user_id
    from scidb.exceptions import UnsavedIntermediateError
    from datetime import datetime

    active_db = db
    if active_db is None:
        active_db = get_database()

    # Lineage-only save for side-effect functions (generates_file=True)
    if data.pipeline_thunk.thunk.generates_file:
        lineage_record = extract_lineage(data)
        lineage_dict = _lineage_to_dict(lineage_record)
        pipeline_lineage_hash = data.pipeline_thunk.compute_lineage_hash()
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

    # Handle unsaved intermediates
    unsaved = find_unsaved_variables(data)

    if active_db.lineage_mode == "strict" and unsaved:
        var_descriptions = []
        for var, path in unsaved:
            var_type = type(var).__name__
            var_descriptions.append(f"  - {var_type} (path: {path})")
        vars_str = "\n".join(var_descriptions)
        raise UnsavedIntermediateError(
            f"Strict lineage mode requires all intermediate variables to be saved.\n"
            f"Found {len(unsaved)} unsaved variable(s) in the computation chain:\n"
            f"{vars_str}\n\n"
            f"Either save these variables first, or use lineage_mode='ephemeral' "
            f"in configure_database() to allow unsaved intermediates."
        )

    elif active_db.lineage_mode == "ephemeral" and unsaved:
        user_id = get_user_id()
        schema_keys = active_db._split_metadata(metadata).get("schema")
        for var, path in unsaved:
            inner_data = getattr(var, "data", None)
            if isinstance(inner_data, ThunkOutput):
                ephemeral_id = f"ephemeral:{inner_data.hash[:32]}"
                var_type = type(var).__name__
                intermediate_lineage = extract_lineage(inner_data)
                active_db.save_ephemeral_lineage(
                    ephemeral_id=ephemeral_id,
                    variable_type=var_type,
                    lineage=_lineage_to_dict(intermediate_lineage),
                    user_id=user_id,
                    schema_keys=schema_keys,
                )

    lineage_record = extract_lineage(data)
    lineage_dict = _lineage_to_dict(lineage_record)
    lineage_hash = data.hash
    pipeline_lineage_hash = data.pipeline_thunk.compute_lineage_hash()
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

    This is the scihist-level save that handles ThunkOutput:
    - If ``data`` is a ThunkOutput, extracts lineage and saves with full
      provenance tracking (respecting lineage_mode strict/ephemeral).
    - Otherwise, delegates to ``variable_class.save(data, **metadata)``.

    Args:
        variable_class: The BaseVariable subclass to save as.
        data: The data to save. Can be a ThunkOutput or raw data.
        db: Optional database instance.
        **metadata: Addressing metadata (e.g., subject=1, trial=1).

    Returns:
        str: The record_id of the saved data.
    """
    from thunk import ThunkOutput

    if isinstance(data, ThunkOutput):
        return _save_thunk_output(variable_class, data, metadata, db)
    else:
        db_kwargs = {"db": db} if db is not None else {}
        return variable_class.save(data, **db_kwargs, **metadata)


def _lineage_to_dict(lineage_record) -> dict:
    """Convert a thunk.LineageRecord to the dict format scidb expects."""
    return {
        "function_name": lineage_record.function_name,
        "function_hash": lineage_record.function_hash,
        "inputs": lineage_record.inputs,
        "constants": lineage_record.constants,
    }
