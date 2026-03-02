"""DB-backed for_each wrapper — loads inputs, delegates loop to scifor, saves outputs."""

from typing import Any, Callable

import scifor as _scifor
from scifor import for_each as _scifor_for_each
from scifor.pathinput import PathInput

from .column_selection import ColumnSelection
from .fixed import Fixed
from .foreach_config import ForEachConfig
from .merge import Merge


def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[type],
    dry_run: bool = False,
    save: bool = True,
    pass_metadata: bool | None = None,
    as_table: list[str] | bool | None = None,
    db=None,
    distribute: bool = False,
    where=None,
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | None":
    """
    Execute a function for all combinations of metadata, loading inputs
    and saving outputs automatically.

    This is the DB-backed wrapper. It:
    1. Resolves empty lists ``[]`` via ``db.distinct_schema_values()``
    2. Pre-filters schema combos via ``db.distinct_schema_combinations()``
    3. Builds ``ForEachConfig`` version keys
    4. Loads all input variables into DataFrames
    5. Converts scirun wrappers → scifor wrappers
    6. Delegates the core loop to ``scifor.for_each``
    7. Saves results from the returned table

    Args:
        fn: The function to execute, or a Thunk.
        inputs: Dict mapping parameter names to variable types, Fixed wrappers,
                Merge wrappers, ColumnSelection wrappers, PathInput, or constants.
        outputs: List of output types/objects with ``.save()``.
        dry_run: If True, only print what would happen without executing.
        save: If True (default), save each function run's output.
        pass_metadata: If True, pass metadata values as keyword arguments to fn.
        as_table: Controls which inputs are passed as full DataFrames.
        db: Optional database instance.
        distribute: If True, split outputs and save each piece at the schema
                    level below the deepest iterated key.
        where: Optional filter; passed to .load() calls on DB-backed inputs.
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, or None when dry_run=True.
    """
    # Resolve empty lists to all distinct values from the database
    needs_resolve = [k for k, v in metadata_iterables.items()
                     if isinstance(v, list) and len(v) == 0]
    resolved_db = None
    if needs_resolve:
        resolved_db = db
        if resolved_db is None:
            try:
                from scidb.database import get_database
                resolved_db = get_database()
            except Exception:
                raise ValueError(
                    f"Empty list [] was passed for {needs_resolve}, which means "
                    f"'use all levels', but no database is available. Either pass "
                    f"db= to for_each or call configure_database() first."
                )
        for key in needs_resolve:
            values = resolved_db.distinct_schema_values(key)
            if not values:
                print(f"[warn] no values found for '{key}' in database, 0 iterations")
            metadata_iterables[key] = values

    # Propagate schema keys to scifor so distribute and DataFrame detection work
    _propagate_schema(db, distribute)

    # Build ForEachConfig version keys (DB-specific; not part of scifor)
    config = ForEachConfig(
        fn=fn,
        inputs=inputs,
        where=where,
        distribute=distribute,
        as_table=as_table,
        pass_metadata=pass_metadata,
    )
    config_keys = config.to_version_keys()

    # Pre-filter to only schema combinations that actually exist in the database.
    all_combos = None
    if needs_resolve and not _has_pathinput(inputs):
        from scidb.database import _schema_str
        filter_db = resolved_db
        schema_keys_set = set(filter_db.dataset_schema_keys)
        keys = list(metadata_iterables.keys())
        schema_indices = [i for i, k in enumerate(keys) if k in schema_keys_set]
        filter_keys = [keys[i] for i in schema_indices]

        if filter_keys:
            from itertools import product
            value_lists = [metadata_iterables[k] for k in keys]
            raw_combos = list(product(*value_lists))

            existing = filter_db.distinct_schema_combinations(filter_keys)
            existing_set = set(existing)

            filtered = [
                dict(zip(keys, combo))
                for combo in raw_combos
                if tuple(_schema_str(combo[i]) for i in schema_indices) in existing_set
            ]
            removed = len(raw_combos) - len(filtered)
            if removed > 0:
                print(f"[info] filtered {removed} non-existent schema combinations "
                      f"(from {len(raw_combos)} to {len(filtered)})")
            all_combos = filtered

    # Build output_names for scifor
    output_names = [_output_name(o) for o in outputs] if outputs else ["result"]

    # Load all inputs into DataFrames and convert wrappers
    scifor_inputs = _convert_inputs(inputs, db, where)

    # Wrap Thunk if needed
    fn_wrapped = _wrap_thunk(fn) if _is_thunk(fn) else fn

    # Delegate core loop to scifor
    result_tbl = _scifor_for_each(
        fn_wrapped,
        scifor_inputs,
        dry_run=dry_run,
        pass_metadata=pass_metadata,
        as_table=as_table,
        distribute=distribute,
        output_names=output_names,
        _all_combos=all_combos,
        **metadata_iterables,
    )

    if result_tbl is None:
        return None

    # Save results
    if save and outputs and not result_tbl.empty:
        _save_results(result_tbl, outputs, output_names, config_keys, db)

    return result_tbl


# ---------------------------------------------------------------------------
# Input loading and conversion
# ---------------------------------------------------------------------------

def _convert_inputs(
    inputs: dict[str, Any],
    db: Any | None,
    where: Any | None,
) -> dict[str, Any]:
    """Convert all inputs: load var types into DataFrames, convert wrappers.

    Returns a dict suitable for scifor.for_each (DataFrames + constants).
    """
    result = {}
    for param_name, var_spec in inputs.items():
        if _is_loadable(var_spec):
            result[param_name] = _load_input(var_spec, db, where)
        else:
            # Constant — pass through unchanged
            result[param_name] = var_spec
    return result


def _load_input(var_spec: Any, db: Any | None, where: Any | None) -> Any:
    """Load a single input and return a scifor-compatible wrapper or DataFrame."""
    import pandas as pd

    # Already a DataFrame — pass through
    if isinstance(var_spec, pd.DataFrame):
        return var_spec

    # Merge: load each constituent and return scifor.Merge of DataFrames
    if isinstance(var_spec, Merge):
        loaded_tables = []
        for sub_spec in var_spec.var_specs:
            loaded = _load_input(sub_spec, db, where)
            loaded_tables.append(loaded)
        return _scifor.Merge(*loaded_tables)

    # Fixed: load inner, return scifor.Fixed with loaded data
    if isinstance(var_spec, Fixed):
        inner_loaded = _load_input(var_spec.var_type, db, where)
        return _scifor.Fixed(inner_loaded, **var_spec.fixed_metadata)

    # ColumnSelection: load inner var_type, return scifor.ColumnSelection
    if isinstance(var_spec, ColumnSelection):
        loaded_df = _load_var_type_all(var_spec.var_type, db, where)
        return _scifor.ColumnSelection(loaded_df, var_spec.columns)

    # PathInput: load is per-combo, so we can't preload.
    # Convert to a DataFrame with metadata columns and a path column.
    if isinstance(var_spec, PathInput):
        # PathInput is special — it resolves paths per-combo, not from DB.
        # We pass it through as a constant; the wrapped fn will handle it.
        # Actually, we need to handle it differently. For now, return as-is
        # and let the wrapped fn handle path resolution.
        return var_spec

    # Variable type (class with .load()): bulk load all records into a DataFrame
    if isinstance(var_spec, type) or hasattr(var_spec, 'load'):
        return _load_var_type_all(var_spec, db, where)

    # Unknown — return as-is
    return var_spec


def _load_var_type_all(
    var_type: Any,
    db: Any | None,
    where: Any | None,
) -> "pd.DataFrame":
    """Bulk load all records for a variable type into a DataFrame with metadata columns."""
    import pandas as pd

    db_kwargs = {"db": db} if db is not None else {}
    where_kwargs = {"where": where} if where is not None else {}

    loaded = var_type.load(**db_kwargs, **where_kwargs)

    # Single result → wrap in list
    if not isinstance(loaded, list):
        loaded = [loaded]

    if not loaded:
        return pd.DataFrame()

    # Check if data is DataFrames
    first = loaded[0]
    all_have_data = all(hasattr(v, 'data') for v in loaded)

    if all_have_data and isinstance(first.data, pd.DataFrame):
        # DataFrame data: build table with metadata cols + data cols
        parts = []
        for var in loaded:
            data_df = var.data
            meta = dict(var.metadata) if hasattr(var, 'metadata') and var.metadata else {}
            nr = len(data_df)
            meta_df = pd.DataFrame({k: [v] * nr for k, v in meta.items()})
            parts.append(pd.concat([meta_df.reset_index(drop=True),
                                    data_df.reset_index(drop=True)], axis=1))
        return pd.concat(parts, ignore_index=True)
    elif all_have_data:
        # Scalar/other data: build table with metadata cols + view_name/class_name col
        view_name = var_type.view_name() if hasattr(var_type, 'view_name') else getattr(var_type, '__name__', type(var_type).__name__)
        rows = []
        for var in loaded:
            row = dict(var.metadata) if hasattr(var, 'metadata') and var.metadata else {}
            row[view_name] = var.data
            rows.append(row)
        return pd.DataFrame(rows)
    else:
        # Raw results without .data attribute — just wrap
        rows = []
        var_name = getattr(var_type, '__name__', type(var_type).__name__)
        for var in loaded:
            rows.append({var_name: var})
        return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Saving results
# ---------------------------------------------------------------------------

def _save_results(
    result_tbl: "pd.DataFrame",
    outputs: list[Any],
    output_names: list[str],
    config_keys: dict,
    db: Any | None,
) -> None:
    """Save results from the result table to output variable types."""
    db_kwargs = {"db": db} if db is not None else {}

    # Determine which columns are metadata (not output names)
    meta_cols = [c for c in result_tbl.columns if c not in output_names]

    for _, row in result_tbl.iterrows():
        # Build save metadata from metadata columns + config keys
        save_metadata = {col: row[col] for col in meta_cols}
        save_metadata.update(config_keys)

        for output_obj, output_name in zip(outputs, output_names):
            if output_name not in row.index:
                continue
            output_value = row[output_name]
            try:
                output_obj.save(output_value, **db_kwargs, **save_metadata)
                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                print(f"[save] {meta_str}: {_output_name(output_obj)}")
            except Exception as e:
                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                print(f"[error] {meta_str}: failed to save {_output_name(output_obj)}: {e}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_loadable(var_spec: Any) -> bool:
    """Check if an input spec is loadable (var type, Fixed, Merge, ColumnSelection, etc.)."""
    try:
        import pandas as pd
        if isinstance(var_spec, pd.DataFrame):
            return True
    except ImportError:
        pass
    return isinstance(var_spec, (type, Fixed, ColumnSelection, Merge, PathInput)) or hasattr(var_spec, 'load')


def _is_thunk(fn: Any) -> bool:
    """Check if fn is a thunk-lib Thunk (without hard dependency)."""
    try:
        from thunk.core import Thunk
        return isinstance(fn, Thunk)
    except ImportError:
        return False


def _wrap_thunk(fn: Any) -> Callable:
    """Wrap a Thunk in a plain function for scifor.for_each."""
    def wrapped(**kwargs):
        return fn(**kwargs)
    wrapped.__name__ = getattr(fn, "__name__", "thunk")
    return wrapped


def _has_pathinput(inputs: dict) -> bool:
    """Check if any input is a PathInput, directly or wrapped in Fixed."""
    for v in inputs.values():
        if isinstance(v, PathInput):
            return True
        if isinstance(v, Fixed) and isinstance(v.var_type, PathInput):
            return True
    return False


def _output_name(output_obj: Any) -> str:
    """Get display name for an output object."""
    if hasattr(output_obj, 'view_name'):
        return output_obj.view_name()
    if isinstance(output_obj, type):
        return output_obj.__name__
    return getattr(output_obj, '__name__', type(output_obj).__name__)


def _propagate_schema(db, distribute: bool) -> None:
    """Propagate dataset_schema_keys from the db into scifor.set_schema()."""
    # If a db was passed explicitly and has schema keys, use them.
    if db is not None and hasattr(db, 'dataset_schema_keys'):
        _scifor.set_schema(list(db.dataset_schema_keys))
        return

    # No explicit db: try the global database.
    _global_db = None
    try:
        from scidb.database import get_database
        _global_db = get_database()
    except Exception:
        pass

    if _global_db is not None and hasattr(_global_db, 'dataset_schema_keys'):
        _scifor.set_schema(list(_global_db.dataset_schema_keys))
    elif distribute:
        raise ValueError(
            "distribute=True requires access to dataset_schema_keys, "
            "but no database is available. Either pass db= to for_each or "
            "call configure_database() first."
        )
