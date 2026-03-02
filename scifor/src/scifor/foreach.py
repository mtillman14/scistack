"""Core for_each loop — mode-agnostic, works with DataFrames and .load()/.save() protocols."""

from itertools import product
from typing import Any, Callable

from .column_selection import ColumnSelection
from .fixed import Fixed
from .merge import Merge
from .pathinput import PathInput
from .schema import get_schema


def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[Any],
    dry_run: bool = False,
    save: bool = True,
    pass_metadata: bool | None = None,
    as_table: list[str] | bool | None = None,
    db=None,
    distribute: bool = False,
    where=None,
    _extra_save_metadata: dict | None = None,
    _all_combos: list[dict] | None = None,
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | None":
    """
    Execute a function for all combinations of metadata, loading inputs
    and saving outputs automatically.

    Works in two modes:
    - **Standalone**: inputs can be plain pandas DataFrames (filtered per-iteration)
      or objects with a ``.load()`` method.
    - **DB-backed**: inputs are database variable types loaded via ``.load()``
      (used when called from scirun-lib's wrapper).

    Args:
        fn: The function to execute.
        inputs: Dict mapping parameter names to variable types, Fixed wrappers,
                DataFrames, or constant values.
        outputs: List of output types/objects with ``.save()`` or plain list
                 (if empty, no saving occurs).
        dry_run: If True, only print what would happen without executing.
        save: If True (default), save each function run's output.
        pass_metadata: If True, pass metadata values as keyword arguments to fn.
        as_table: Controls which DataFrame inputs are always passed as DataFrames
                  even when one-row/one-column extraction would apply.
                  True = all; list of names = selected; False/None = none.
        db: Optional database instance; passed to .load()/.save() as db= kwarg.
        distribute: If True, split outputs by element/row and save each piece at
                    the schema level below the deepest iterated key.
        where: Optional filter; passed to .load() calls on DB-backed inputs.
        _extra_save_metadata: Extra keys merged into save_metadata (for DB config
                              version keys set by scirun-lib).
        _all_combos: Pre-built list of metadata dicts; skips itertools.product().
                     Used by scirun-lib when it pre-filters schema combinations.
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, or None when dry_run=True.
    """
    schema_keys = get_schema()

    # Resolve empty lists [] in standalone mode (scan DataFrame inputs)
    if _all_combos is None:
        needs_resolve = [k for k, v in metadata_iterables.items()
                         if isinstance(v, list) and len(v) == 0]
        if needs_resolve:
            for key in needs_resolve:
                values = _distinct_values_from_inputs(inputs, key)
                if not values:
                    print(f"[warn] no values found for '{key}' in input DataFrames, 0 iterations")
                metadata_iterables[key] = values

    # Validate distribute parameter and resolve target key
    distribute_key = None
    if distribute:
        iter_keys_in_schema = [k for k in schema_keys if k in metadata_iterables]
        if not iter_keys_in_schema:
            raise ValueError(
                "distribute=True requires at least one metadata_iterable "
                "that is a schema key. Call set_schema() or configure_database() first."
            )
        deepest_iterated = iter_keys_in_schema[-1]
        deepest_idx = schema_keys.index(deepest_iterated)

        if deepest_idx + 1 >= len(schema_keys):
            raise ValueError(
                f"distribute=True but '{deepest_iterated}' is the deepest schema key. "
                f"There is no lower level to distribute to. "
                f"Schema order: {schema_keys}"
            )
        distribute_key = schema_keys[deepest_idx + 1]

    # Separate loadable inputs from constants
    loadable_inputs = {}
    constant_inputs = {}
    for param_name, var_spec in inputs.items():
        if _is_loadable(var_spec):
            loadable_inputs[param_name] = var_spec
        else:
            constant_inputs[param_name] = var_spec

    # Check distribute doesn't conflict with a constant input name
    if distribute_key is not None and distribute_key in constant_inputs:
        raise ValueError(
            f"distribute target '{distribute_key}' conflicts with a constant input named '{distribute_key}'."
        )

    # Build set of input names to convert to DataFrame (for .load() multi-results)
    if as_table is True:
        as_table_set = set(loadable_inputs.keys())
    elif as_table:
        as_table_set = set(as_table)
    else:
        as_table_set = set()

    # Build combo list
    if _all_combos is not None:
        all_combos = _all_combos
        # Reconstruct keys/value_lists for dry-run display only
        keys = list(metadata_iterables.keys())
    else:
        keys = list(metadata_iterables.keys())
        value_lists = [metadata_iterables[k] for k in keys]
        all_combos = [dict(zip(keys, combo)) for combo in product(*value_lists)]

    total = len(all_combos)
    fn_name = getattr(fn, "__name__", repr(fn))
    should_pass_metadata = pass_metadata if pass_metadata is not None else getattr(fn, 'generates_file', False)

    if dry_run:
        print(f"[dry-run] for_each({fn_name})")
        print(f"[dry-run] {total} iterations over: {keys}")
        print(f"[dry-run] inputs: {_format_inputs(inputs)}")
        if outputs:
            print(f"[dry-run] outputs: {[_output_name(o) for o in outputs]}")
        if distribute_key is not None:
            print(f"[dry-run] distribute: '{distribute_key}' (split outputs by element/row, 1-based)")
        print()

    completed = 0
    skipped = 0
    collected_rows: list[tuple[dict, tuple]] = []

    for metadata in all_combos:
        metadata_str = ", ".join(f"{k}={v}" for k, v in metadata.items())

        if dry_run:
            _print_dry_run_iteration(inputs, outputs, metadata, constant_inputs, should_pass_metadata, distribute_key)
            completed += 1
            continue

        # Load inputs
        loaded_inputs = {}
        load_failed = False

        for param_name, var_spec in loadable_inputs.items():
            # Handle Merge: load each constituent and combine into DataFrame
            if isinstance(var_spec, Merge):
                try:
                    loaded_inputs[param_name] = _load_and_merge(
                        var_spec, metadata, param_name, db, where, schema_keys
                    )
                except Exception as e:
                    print(f"[skip] {metadata_str}: failed to load {param_name} ({var_spec.__name__}): {e}")
                    load_failed = True
                    break
                continue

            # Guard against Fixed wrapping Merge
            if isinstance(var_spec, Fixed) and isinstance(var_spec.var_type, Merge):
                raise TypeError(
                    "Fixed cannot wrap a Merge. Use Fixed on individual "
                    "constituents inside the Merge instead: "
                    "Merge(Fixed(VarA, ...), VarB)"
                )

            # Handle DataFrame inputs (per-combo or constant)
            if _is_dataframe(var_spec):
                try:
                    wants_table = param_name in as_table_set
                    loaded_inputs[param_name] = _load_dataframe_input(
                        var_spec, metadata, schema_keys, wants_table
                    )
                except Exception as e:
                    print(f"[skip] {metadata_str}: failed to process {param_name} (DataFrame): {e}")
                    load_failed = True
                    break
                continue

            # Handle Fixed(DataFrame, ...)
            if isinstance(var_spec, Fixed) and _is_dataframe(var_spec.var_type):
                try:
                    fixed_meta = {**metadata, **var_spec.fixed_metadata}
                    wants_table = param_name in as_table_set
                    loaded_inputs[param_name] = _load_dataframe_input(
                        var_spec.var_type, fixed_meta, schema_keys, wants_table
                    )
                except Exception as e:
                    print(f"[skip] {metadata_str}: failed to process {param_name} (Fixed DataFrame): {e}")
                    load_failed = True
                    break
                continue

            # Resolve var_type, load_metadata, and column_selection from the spec
            var_type, load_metadata, column_selection = _resolve_var_spec(var_spec, metadata)

            try:
                db_kwargs = {"db": db} if db is not None else {}
                where_kwargs = {"where": where} if where is not None else {}
                loaded_inputs[param_name] = var_type.load(**db_kwargs, **load_metadata, **where_kwargs)
            except Exception as e:
                var_name = getattr(var_type, '__name__', type(var_type).__name__)
                print(f"[skip] {metadata_str}: failed to load {param_name} ({var_name}): {e}")
                load_failed = True
                break

            # Handle as_table conversion and/or column selection
            is_multi = isinstance(loaded_inputs[param_name], list)
            wants_table = param_name in as_table_set and is_multi

            if column_selection is not None and wants_table:
                _apply_column_selection_to_vars(
                    loaded_inputs[param_name], column_selection, param_name
                )
                loaded_inputs[param_name] = _multi_result_to_dataframe(
                    loaded_inputs[param_name], var_type
                )
            elif wants_table:
                loaded_inputs[param_name] = _multi_result_to_dataframe(
                    loaded_inputs[param_name], var_type
                )
            elif column_selection is not None:
                loaded_inputs[param_name] = _apply_column_selection(
                    loaded_inputs[param_name], column_selection, param_name
                )

        if load_failed:
            skipped += 1
            continue

        # Call the function
        all_param_names = list(loaded_inputs.keys()) + list(constant_inputs.keys())
        print(f"[run] {metadata_str}: {fn_name}({', '.join(all_param_names)})")

        # For plain functions (not Thunks), unwrap BaseVariable / ThunkOutput
        if not _is_thunk(fn):
            loaded_inputs = {
                k: v if _is_dataframe(v) else _unwrap(v)
                for k, v in loaded_inputs.items()
            }

        # Merge constants into function arguments
        loaded_inputs.update(constant_inputs)

        try:
            if should_pass_metadata:
                result = fn(**loaded_inputs, **metadata)
            else:
                result = fn(**loaded_inputs)
        except Exception as e:
            print(f"[skip] {metadata_str}: {fn_name} raised: {e}")
            skipped += 1
            continue

        # Normalize single output to tuple
        if not isinstance(result, tuple):
            result = (result,)

        collected_rows.append((metadata, result))

        # Save outputs
        extra = _extra_save_metadata or {}
        save_metadata = {**metadata, **constant_inputs, **extra}
        if save and outputs:
            db_kwargs = {"db": db} if db is not None else {}

            if distribute_key is not None:
                for output_obj, output_value in zip(outputs, result):
                    raw_value = _unwrap_for_distribute(output_value)
                    try:
                        pieces = _split_for_distribute(raw_value)
                    except TypeError as e:
                        print(f"[error] {metadata_str}: cannot distribute {_output_name(output_obj)}: {e}")
                        continue

                    for i, piece in enumerate(pieces):
                        dist_metadata = {**save_metadata, distribute_key: i + 1}
                        try:
                            output_obj.save(piece, **db_kwargs, **dist_metadata)
                            dist_str = ", ".join(f"{k}={v}" for k, v in dist_metadata.items())
                            print(f"[save] {dist_str}: {_output_name(output_obj)}")
                        except Exception as e:
                            dist_str = ", ".join(f"{k}={v}" for k, v in dist_metadata.items())
                            print(f"[error] {dist_str}: failed to save {_output_name(output_obj)}: {e}")
            else:
                for output_obj, output_value in zip(outputs, result):
                    try:
                        output_obj.save(output_value, **db_kwargs, **save_metadata)
                        print(f"[save] {metadata_str}: {_output_name(output_obj)}")
                    except Exception as e:
                        print(f"[error] {metadata_str}: failed to save {_output_name(output_obj)}: {e}")

        completed += 1

    # Summary
    print()
    if dry_run:
        print(f"[dry-run] would process {total} iterations")
        return None
    else:
        print(f"[done] completed={completed}, skipped={skipped}, total={total}")
        return _results_to_output_dataframe(collected_rows, outputs)


# ---------------------------------------------------------------------------
# DataFrame detection and filtering
# ---------------------------------------------------------------------------

def _is_dataframe(value: Any) -> bool:
    """Return True if value is a pandas DataFrame."""
    try:
        import pandas as pd
        return isinstance(value, pd.DataFrame)
    except ImportError:
        return False


def _is_per_combo_df(df: "pd.DataFrame", schema_keys: list[str]) -> bool:
    """True if df has at least one column that is a schema key."""
    return bool(set(df.columns) & set(schema_keys))


def _filter_df_for_combo(
    df: "pd.DataFrame", metadata: dict, schema_keys: list[str]
) -> "pd.DataFrame":
    """Filter df rows to match combo metadata for schema key columns present in df."""
    import pandas as pd
    mask = pd.Series([True] * len(df), index=df.index)
    for key in schema_keys:
        if key in df.columns and key in metadata:
            mask = mask & (df[key] == metadata[key])
    return df[mask].reset_index(drop=True)


def _load_dataframe_input(
    df: "pd.DataFrame",
    metadata: dict,
    schema_keys: list[str],
    as_table: bool,
) -> Any:
    """Load a DataFrame input for a specific metadata combo.

    If the DataFrame is per-combo (has schema key columns), filter it.
    If it is constant (no schema key columns), return it unchanged.

    After filtering:
    - 1 row, 1 non-schema-key column, not as_table → extract scalar/value
    - otherwise → return sub-DataFrame
    """
    if not _is_per_combo_df(df, schema_keys):
        # Constant DataFrame — pass unchanged every iteration
        return df

    filtered = _filter_df_for_combo(df, metadata, schema_keys)
    if as_table:
        return filtered

    data_cols = [c for c in filtered.columns if c not in schema_keys]
    if len(filtered) == 1 and len(data_cols) == 1:
        return filtered[data_cols[0]].iloc[0]
    return filtered


def _distinct_values_from_inputs(inputs: dict, key: str) -> list:
    """Find distinct values for `key` by scanning DataFrame inputs."""
    all_values = set()
    for _param_name, var_spec in inputs.items():
        df = _get_raw_df(var_spec)
        if df is not None and key in df.columns:
            all_values.update(df[key].dropna().unique().tolist())
    if not all_values:
        raise ValueError(
            f"Empty list [] was passed for '{key}', but no input DataFrame has "
            f"that column. Either provide values explicitly or ensure a DataFrame "
            f"input contains a '{key}' column."
        )
    try:
        return sorted(all_values)
    except TypeError:
        return list(all_values)


def _get_raw_df(var_spec: Any) -> "pd.DataFrame | None":
    """Extract the DataFrame from a var_spec, if it contains one."""
    if _is_dataframe(var_spec):
        return var_spec
    if isinstance(var_spec, Fixed) and _is_dataframe(var_spec.var_type):
        return var_spec.var_type
    return None


# ---------------------------------------------------------------------------
# Existing helpers (adapted from scirun-lib)
# ---------------------------------------------------------------------------

def _is_loadable(var_spec: Any) -> bool:
    """Check if an input spec is loadable (DataFrame, class, Fixed, Merge, etc.)."""
    if _is_dataframe(var_spec):
        return True
    if isinstance(var_spec, Fixed) and _is_dataframe(var_spec.var_type):
        return True
    return isinstance(var_spec, (type, Fixed, ColumnSelection, Merge)) or hasattr(var_spec, 'load')


def _is_thunk(fn: Any) -> bool:
    """Check if fn is a thunk-lib Thunk (without hard dependency)."""
    try:
        from thunk.core import Thunk
        return isinstance(fn, Thunk)
    except ImportError:
        return False


def _unwrap(value: Any) -> Any:
    """Extract raw data from a loaded variable, pass everything else through."""
    try:
        import numpy as np
        # np.generic covers all numpy scalars (float64, int64, etc.) which have
        # a .data memoryview attribute that must NOT be unwrapped.
        if isinstance(value, (np.ndarray, np.generic)):
            return value
    except ImportError:
        pass
    try:
        import pandas as pd
        if isinstance(value, (pd.DataFrame, pd.Series)):
            return value
    except ImportError:
        pass
    if isinstance(value, list):
        return value
    if hasattr(value, 'data'):
        return value.data
    return value


def _unwrap_for_distribute(value: Any) -> Any:
    """Unwrap ThunkOutput/BaseVariable for distribute, but not raw data types."""
    if isinstance(value, list):
        return value
    try:
        import numpy as np
        if isinstance(value, (np.ndarray, np.generic)):
            return value
    except ImportError:
        pass
    try:
        import pandas as pd
        if isinstance(value, pd.DataFrame):
            return value
    except ImportError:
        pass
    if hasattr(value, 'data'):
        return value.data
    return value


def _multi_result_to_dataframe(results: list, var_type: type):
    """Convert a list of loaded variables to a pandas DataFrame."""
    import pandas as pd

    all_dataframes = all(isinstance(var.data, pd.DataFrame) for var in results)

    if all_dataframes:
        parts = []
        for var in results:
            data_df = var.data
            meta = dict(var.metadata) if var.metadata else {}
            nr = len(data_df)
            meta_df = pd.DataFrame({k: [v] * nr for k, v in meta.items()})
            parts.append(pd.concat([meta_df.reset_index(drop=True),
                                    data_df.reset_index(drop=True)], axis=1))
        return pd.concat(parts, ignore_index=True)
    else:
        view_name = var_type.view_name() if hasattr(var_type, 'view_name') else var_type.__name__
        rows = []
        for var in results:
            row = dict(var.metadata) if var.metadata else {}
            row[view_name] = var.data
            rows.append(row)
        return pd.DataFrame(rows)


def _results_to_output_dataframe(
    collected_rows: list[tuple[dict, tuple]],
    outputs: list[Any],
) -> "pd.DataFrame":
    """Build a combined DataFrame from all for_each results."""
    import pandas as pd

    if not collected_rows:
        return pd.DataFrame()

    output_names = [_output_name(o) for o in outputs]

    if not outputs:
        # No outputs — just collect metadata and the single return value
        rows = []
        for metadata, result_tuple in collected_rows:
            row = dict(metadata)
            if result_tuple:
                row["result"] = result_tuple[0] if len(result_tuple) == 1 else result_tuple
            rows.append(row)
        return pd.DataFrame(rows)

    all_dataframes = all(
        isinstance(value, pd.DataFrame)
        for _, result_tuple in collected_rows
        for value in result_tuple
    )

    if all_dataframes:
        parts = []
        for metadata, result_tuple in collected_rows:
            combined_data = pd.concat(
                [df.reset_index(drop=True) for df in result_tuple], axis=1
            )
            nr = len(combined_data)
            meta_df = pd.DataFrame({k: [v] * nr for k, v in metadata.items()})
            parts.append(pd.concat(
                [meta_df.reset_index(drop=True), combined_data], axis=1
            ))
        return pd.concat(parts, ignore_index=True)
    else:
        rows = []
        for metadata, result_tuple in collected_rows:
            row = dict(metadata)
            for name, value in zip(output_names, result_tuple):
                row[name] = value
            rows.append(row)
        return pd.DataFrame(rows)


def _output_name(output_obj: Any) -> str:
    """Get display name for an output object."""
    if hasattr(output_obj, 'view_name'):
        return output_obj.view_name()
    if isinstance(output_obj, type):
        return output_obj.__name__
    return getattr(output_obj, '__name__', type(output_obj).__name__)


def _apply_column_selection(loaded_value: Any, columns: list[str], param_name: str) -> Any:
    """Extract selected columns from loaded data."""
    import pandas as pd

    if hasattr(loaded_value, 'data') and isinstance(loaded_value.data, pd.DataFrame):
        df = loaded_value.data
    elif isinstance(loaded_value, pd.DataFrame):
        df = loaded_value
    else:
        data_type = type(getattr(loaded_value, 'data', loaded_value)).__name__
        raise TypeError(
            f"Column selection on '{param_name}' requires DataFrame data, "
            f"but loaded data is {data_type}."
        )

    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(
            f"Column(s) {missing} not found in '{param_name}'. "
            f"Available columns: {list(df.columns)}"
        )

    if len(columns) == 1:
        return df[columns[0]].values
    else:
        return df[columns]


def _apply_column_selection_to_vars(variables: list, columns: list[str], param_name: str) -> None:
    """Filter columns on each variable's DataFrame data in-place."""
    import pandas as pd

    for var in variables:
        if hasattr(var, 'data') and isinstance(var.data, pd.DataFrame):
            missing = [c for c in columns if c not in var.data.columns]
            if missing:
                raise KeyError(
                    f"Column(s) {missing} not found in '{param_name}'. "
                    f"Available columns: {list(var.data.columns)}"
                )
            var.data = var.data[columns]
        else:
            data_type = type(getattr(var, 'data', var)).__name__
            raise TypeError(
                f"Column selection on '{param_name}' requires DataFrame data, "
                f"but loaded data is {data_type}."
            )


def _format_inputs(inputs: dict[str, Any]) -> str:
    """Format inputs dict for display."""
    parts = []
    for name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            parts.append(f"{name}: {var_spec.__name__}")
        elif isinstance(var_spec, Fixed):
            fixed_str = ", ".join(f"{k}={v}" for k, v in var_spec.fixed_metadata.items())
            if _is_dataframe(var_spec.var_type):
                inner_name = "DataFrame"
            else:
                inner_name = getattr(var_spec.var_type, '__name__', type(var_spec.var_type).__name__)
            parts.append(f"{name}: Fixed({inner_name}, {fixed_str})")
        elif isinstance(var_spec, ColumnSelection):
            parts.append(f"{name}: {var_spec.__name__}")
        elif _is_dataframe(var_spec):
            parts.append(f"{name}: DataFrame{list(var_spec.columns)}")
        elif _is_loadable(var_spec):
            var_name = getattr(var_spec, '__name__', type(var_spec).__name__)
            parts.append(f"{name}: {var_name}")
        else:
            parts.append(f"{name}: {var_spec!r}")
    return "{" + ", ".join(parts) + "}"


def _split_for_distribute(data: Any) -> list[Any]:
    """Split data into elements for distribute-style saving."""
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            return [data.iloc[[i]] for i in range(len(data))]
    except ImportError:
        pass

    try:
        import numpy as np
        if isinstance(data, np.ndarray):
            if data.ndim == 1:
                return [data[i] for i in range(len(data))]
            elif data.ndim == 2:
                return [data[i, :] for i in range(data.shape[0])]
            else:
                raise TypeError(
                    f"distribute does not support numpy arrays with {data.ndim} dimensions. "
                    f"Only 1D (split by element) and 2D (split by row) are supported."
                )
    except ImportError:
        pass

    if isinstance(data, list):
        return list(data)

    raise TypeError(
        f"distribute does not support type {type(data).__name__}. "
        f"Supported types: numpy 1D/2D array, list, pandas DataFrame."
    )


def _print_dry_run_iteration(
    inputs: dict[str, Any],
    outputs: list[Any],
    metadata: dict[str, Any],
    constant_inputs: dict[str, Any],
    pass_metadata: bool = False,
    distribute: str | None = None,
) -> None:
    """Print what would happen for one iteration in dry-run mode."""
    metadata_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
    save_metadata = {**metadata, **constant_inputs}
    save_metadata_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items())
    print(f"[dry-run] {metadata_str}:")

    for param_name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            print(f"  merge {param_name}:")
            for i, sub_spec in enumerate(var_spec.var_specs):
                _print_constituent_load(sub_spec, metadata, i)
        elif isinstance(var_spec, Fixed):
            load_metadata = {**metadata, **var_spec.fixed_metadata}
            if _is_dataframe(var_spec.var_type):
                print(f"  filter {param_name} = DataFrame with {load_metadata}")
            else:
                inner = var_spec.var_type
                if isinstance(inner, ColumnSelection):
                    var_name = getattr(inner.var_type, '__name__', repr(inner.var_type))
                    col_str = ", ".join(inner.columns)
                    suffix = f" -> columns: [{col_str}]"
                else:
                    var_name = getattr(inner, '__name__', type(inner).__name__)
                    suffix = ""
                load_str = ", ".join(f"{k}={v}" for k, v in load_metadata.items())
                print(f"  load {param_name} = {var_name}.load({load_str}){suffix}")
        elif isinstance(var_spec, ColumnSelection):
            load_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
            var_name = getattr(var_spec.var_type, '__name__', repr(var_spec.var_type))
            col_str = ", ".join(var_spec.columns)
            print(f"  load {param_name} = {var_name}.load({load_str}) -> columns: [{col_str}]")
        elif _is_dataframe(var_spec):
            print(f"  filter {param_name} = DataFrame with {metadata}")
        elif _is_loadable(var_spec):
            load_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
            var_name = getattr(var_spec, '__name__', type(var_spec).__name__)
            print(f"  load {param_name} = {var_name}.load({load_str})")
        else:
            print(f"  constant {param_name} = {var_spec!r}")

    if pass_metadata:
        print(f"  pass metadata: {metadata_str}")

    for output_obj in outputs:
        out_name = _output_name(output_obj)
        if distribute is not None:
            print(f"  distribute {out_name} by '{distribute}' (1-based indexing)")
        else:
            print(f"  save {out_name}.save(..., {save_metadata_str})")


def _print_constituent_load(spec: Any, metadata: dict[str, Any], index: int) -> None:
    """Print a single Merge constituent's load line for dry-run display."""
    if isinstance(spec, Fixed):
        load_metadata = {**metadata, **spec.fixed_metadata}
        inner = spec.var_type
        if isinstance(inner, ColumnSelection):
            var_name = getattr(inner.var_type, '__name__', repr(inner.var_type))
            col_str = ", ".join(inner.columns)
            suffix = f" -> columns: [{col_str}]"
        else:
            var_name = getattr(inner, '__name__', type(inner).__name__)
            suffix = ""
        load_str = ", ".join(f"{k}={v}" for k, v in load_metadata.items())
        print(f"    [{index}] {var_name}.load({load_str}){suffix}")
    elif isinstance(spec, ColumnSelection):
        load_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
        var_name = getattr(spec.var_type, '__name__', repr(spec.var_type))
        col_str = ", ".join(spec.columns)
        print(f"    [{index}] {var_name}.load({load_str}) -> columns: [{col_str}]")
    else:
        load_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
        var_name = getattr(spec, '__name__', type(spec).__name__)
        print(f"    [{index}] {var_name}.load({load_str})")


def _resolve_var_spec(
    var_spec: Any, metadata: dict[str, Any]
) -> tuple[type, dict[str, Any], list[str] | None]:
    """Resolve a var_spec into (var_type, load_metadata, column_selection)."""
    column_selection = None
    if isinstance(var_spec, Fixed):
        load_metadata = {**metadata, **var_spec.fixed_metadata}
        inner = var_spec.var_type
        if isinstance(inner, ColumnSelection):
            column_selection = inner.columns
            var_type = inner.var_type
        else:
            var_type = inner
    elif isinstance(var_spec, ColumnSelection):
        load_metadata = metadata
        column_selection = var_spec.columns
        var_type = var_spec.var_type
    else:
        load_metadata = metadata
        var_type = var_spec
    return var_type, load_metadata, column_selection


def _load_and_merge(
    merge_spec: Merge,
    metadata: dict[str, Any],
    param_name: str,
    db: Any | None,
    where: Any | None,
    schema_keys: list[str],
) -> "pd.DataFrame":
    """Load each constituent of a Merge and combine into a single DataFrame."""
    import pandas as pd

    parts = []

    for i, spec in enumerate(merge_spec.var_specs):
        label = f"{param_name}[{i}]"

        # Handle DataFrame constituent
        if _is_dataframe(spec):
            wants_table = True  # always keep as DataFrame inside Merge
            loaded = _load_dataframe_input(spec, metadata, schema_keys, wants_table)
            var_name = f"DataFrame{list(spec.columns)}"
            col_name = None
            part_df = _constituent_to_dataframe(loaded, var_name, label)
            parts.append((var_name, part_df))
            continue

        if isinstance(spec, Fixed) and _is_dataframe(spec.var_type):
            fixed_meta = {**metadata, **spec.fixed_metadata}
            loaded = _load_dataframe_input(spec.var_type, fixed_meta, schema_keys, True)
            var_name = "DataFrame"
            part_df = _constituent_to_dataframe(loaded, var_name, label)
            parts.append((var_name, part_df))
            continue

        var_type, load_metadata, column_selection = _resolve_var_spec(spec, metadata)

        db_kwargs = {"db": db} if db is not None else {}
        where_kwargs = {"where": where} if where is not None else {}
        loaded = var_type.load(**db_kwargs, **load_metadata, **where_kwargs)

        if isinstance(loaded, list):
            var_name = getattr(var_type, '__name__', type(var_type).__name__)
            raise ValueError(
                f"Merge constituent {var_name} returned {len(loaded)} records "
                f"for {label}, but Merge requires exactly 1 per iteration. "
                f"Use more specific metadata or Fixed() to narrow the match."
            )

        if column_selection is not None:
            loaded = _apply_column_selection(loaded, column_selection, label)
            col_name = column_selection[0] if len(column_selection) == 1 else None
        else:
            col_name = None

        var_name = getattr(var_type, '__name__', type(var_type).__name__)
        display_name = col_name if col_name is not None else var_name
        part_df = _constituent_to_dataframe(loaded, display_name, label)
        parts.append((var_name, part_df))

    return _merge_parts(parts, param_name)


def _constituent_to_dataframe(loaded: Any, var_name: str, label: str) -> "pd.DataFrame":
    """Convert a loaded value to a DataFrame for merging."""
    import numpy as np
    import pandas as pd

    raw = loaded
    if hasattr(loaded, 'data') and not isinstance(loaded, (np.ndarray, pd.DataFrame, pd.Series)):
        raw = loaded.data

    if isinstance(raw, pd.DataFrame):
        return raw.reset_index(drop=True)
    elif isinstance(raw, np.ndarray):
        if raw.ndim == 1:
            return pd.DataFrame({var_name: raw})
        elif raw.ndim == 2:
            cols = {f"{var_name}_{j}": raw[:, j] for j in range(raw.shape[1])}
            return pd.DataFrame(cols)
        else:
            raise TypeError(
                f"Merge constituent {label} has {raw.ndim}D array data. "
                f"Only 1D and 2D arrays are supported."
            )
    elif isinstance(raw, (int, float, str, bool)):
        return pd.DataFrame({var_name: [raw]})
    elif isinstance(raw, list):
        return pd.DataFrame({var_name: raw})
    else:
        raise TypeError(
            f"Merge constituent {label} has unsupported data type "
            f"{type(raw).__name__}. Supported: DataFrame, ndarray, scalar, list."
        )


def _merge_parts(
    parts: list[tuple[str, "pd.DataFrame"]], param_name: str
) -> "pd.DataFrame":
    """Merge multiple constituent DataFrames column-wise."""
    import pandas as pd

    if not parts:
        raise ValueError(f"Merge for '{param_name}' has no constituents.")

    seen_columns: set[str] = set()
    for var_name, df in parts:
        for col in df.columns:
            if col in seen_columns:
                raise KeyError(
                    f"Column name conflict in Merge for '{param_name}': "
                    f"column '{col}' appears in multiple constituents. "
                    f"Use ColumnSelection to select non-conflicting columns."
                )
            seen_columns.add(col)

    row_counts = [(var_name, len(df)) for var_name, df in parts if len(df) > 1]

    if row_counts:
        unique_counts = set(n for _, n in row_counts)
        if len(unique_counts) > 1:
            detail = ", ".join(f"{name}={n}" for name, n in row_counts)
            raise ValueError(
                f"Cannot merge constituents with different row counts in "
                f"'{param_name}': {detail}. All multi-row constituents must "
                f"have the same number of rows."
            )
        target_len = row_counts[0][1]
    else:
        target_len = 1

    expanded = []
    for _var_name, df in parts:
        if len(df) == 1 and target_len > 1:
            df = pd.concat([df] * target_len, ignore_index=True)
        expanded.append(df)

    return pd.concat(expanded, axis=1)


def _has_pathinput(inputs: dict) -> bool:
    """Check if any input is a PathInput, directly or wrapped in Fixed."""
    for v in inputs.values():
        if isinstance(v, PathInput):
            return True
        if isinstance(v, Fixed) and isinstance(v.var_type, PathInput):
            return True
    return False
