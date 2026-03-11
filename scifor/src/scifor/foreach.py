"""Pure for_each loop — works with DataFrames only, no I/O."""

import traceback
from itertools import product
from typing import Any, Callable

from .column_selection import ColumnSelection
from .fixed import Fixed
from .merge import Merge
from .schema import get_schema


def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    dry_run: bool = False,
    pass_metadata: bool | None = None,
    as_table: list[str] | bool | None = None,
    distribute: bool = False,
    where=None,
    output_names: list[str] | int | None = None,
    _all_combos: list[dict] | None = None,
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | None":
    """
    Execute a function for all combinations of metadata, filtering
    DataFrame inputs per iteration.

    This is a pure loop orchestrator — no I/O, no .load(), no .save().
    All inputs must be DataFrames or constants.

    Args:
        fn: The function to execute.
        inputs: Dict mapping parameter names to DataFrames, Fixed wrappers,
                Merge wrappers, ColumnSelection wrappers, or constant values.
        dry_run: If True, only print what would happen without executing.
        pass_metadata: If True, pass metadata values as keyword arguments to fn.
        as_table: Controls which DataFrame inputs keep schema key columns.
                  True = all; list of names = selected; False/None = none.
        distribute: If True, split outputs by element/row and expand them
                    into the result table at the schema level below the
                    deepest iterated key.
        where: Optional scifor.ColFilter/CompoundFilter to filter DataFrame
               rows after combo filtering.
        output_names: Names for result columns. list[str] names them;
                      int N auto-names (output_1, ..., output_N);
                      None defaults to ["output"] (single output).
        _all_combos: Pre-built list of metadata dicts; skips itertools.product().
                     Used by DB wrappers that pre-filter schema combinations.
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, or None when dry_run=True.
    """
    schema_keys = get_schema()

    # Resolve output_names
    if output_names is None:
        resolved_output_names = ["output"]
    elif isinstance(output_names, int):
        resolved_output_names = [f"output_{i+1}" for i in range(output_names)]
    else:
        resolved_output_names = list(output_names)
    n_outputs = len(resolved_output_names)

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

    # Separate data inputs from constants
    data_inputs = {}
    constant_inputs = {}
    for param_name, var_spec in inputs.items():
        if _is_data_input(var_spec):
            data_inputs[param_name] = var_spec
        else:
            constant_inputs[param_name] = var_spec

    # Check distribute doesn't conflict with a constant input name
    if distribute_key is not None and distribute_key in constant_inputs:
        raise ValueError(
            f"distribute target '{distribute_key}' conflicts with a constant input named '{distribute_key}'."
        )

    # Build set of input names to keep as full DataFrames (with schema cols)
    if as_table is True:
        as_table_set = set(data_inputs.keys())
    elif as_table:
        as_table_set = set(as_table)
    else:
        as_table_set = set()

    # Build combo list
    if _all_combos is not None:
        all_combos = _all_combos
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
        if distribute_key is not None:
            print(f"[dry-run] distribute: '{distribute_key}' (split outputs by element/row, 1-based)")
        print()

    completed = 0
    skipped = 0
    collected_rows: list[tuple[dict, tuple]] = []

    for metadata in all_combos:
        metadata_str = ", ".join(f"{k}={v}" for k, v in metadata.items())

        if dry_run:
            _print_dry_run_iteration(inputs, metadata, constant_inputs, should_pass_metadata, distribute_key)
            completed += 1
            continue

        # Filter/prepare inputs for this combo
        filtered_inputs = {}
        filter_failed = False

        for param_name, var_spec in data_inputs.items():
            try:
                wants_table = param_name in as_table_set
                filtered_inputs[param_name] = _prepare_input(
                    var_spec, metadata, schema_keys, wants_table, where
                )
            except Exception as e:
                print(f"[skip] {metadata_str}: failed to filter {param_name}: {e}")
                traceback.print_exc()
                filter_failed = True
                break

        if filter_failed:
            skipped += 1
            continue

        # Call the function
        all_param_names = list(filtered_inputs.keys()) + list(constant_inputs.keys())
        print(f"[run] {metadata_str}: {fn_name}({', '.join(all_param_names)})")

        # Merge constants into function arguments
        filtered_inputs.update(constant_inputs)

        try:
            if should_pass_metadata:
                result = _call_fn(fn, filtered_inputs, n_outputs, metadata)
            else:
                result = _call_fn(fn, filtered_inputs, n_outputs)
        except Exception as e:
            print(f"[skip] {metadata_str}: {fn_name} raised: {e}")
            traceback.print_exc()
            skipped += 1
            continue

        # Normalize single output to tuple
        if not isinstance(result, tuple):
            result = (result,)

        # Handle distribute: expand result into multiple rows
        if distribute_key is not None:
            for output_value in result:
                try:
                    pieces = _split_for_distribute(output_value)
                except TypeError as e:
                    print(f"[error] {metadata_str}: cannot distribute: {e}")
                    continue

                for i, piece in enumerate(pieces):
                    dist_metadata = {**metadata, distribute_key: i + 1}
                    collected_rows.append((dist_metadata, (piece,)))
        else:
            collected_rows.append((metadata, result))

        completed += 1

    # Summary
    print()
    if dry_run:
        print(f"[dry-run] would process {total} iterations")
        return None
    else:
        print(f"[done] completed={completed}, skipped={skipped}, total={total}")
        return _results_to_output_dataframe(collected_rows, resolved_output_names)


def _call_fn(fn, kwargs, n_outputs, extra_kwargs=None):
    """Call fn with the right number of output captures."""
    if extra_kwargs:
        kwargs = {**kwargs, **extra_kwargs}
    return fn(**kwargs)


# ---------------------------------------------------------------------------
# Input classification
# ---------------------------------------------------------------------------

def _is_data_input(var_spec: Any) -> bool:
    """Check if an input is a data input (DataFrame, Fixed, Merge, ColumnSelection)."""
    if _is_dataframe(var_spec):
        return True
    if isinstance(var_spec, (Fixed, Merge, ColumnSelection)):
        return True
    return False


def _is_dataframe(value: Any) -> bool:
    """Return True if value is a pandas DataFrame."""
    try:
        import pandas as pd
        return isinstance(value, pd.DataFrame)
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# DataFrame filtering
# ---------------------------------------------------------------------------

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


def _apply_where_filter(df: "pd.DataFrame", where) -> "pd.DataFrame":
    """Apply a scifor Col filter to a DataFrame."""
    if where is None:
        return df
    mask = where.apply(df)
    return df[mask].reset_index(drop=True)


def _extract_data(
    df: "pd.DataFrame",
    schema_keys: list[str],
    as_table: bool,
) -> Any:
    """Extract data from a filtered DataFrame.

    If as_table: return full DataFrame (including schema columns).
    Otherwise: drop schema key columns; if 1 row + 1 data col -> extract scalar.
    """
    if as_table:
        return df

    data_cols = [c for c in df.columns if c not in schema_keys]
    if len(df) == 1 and len(data_cols) == 1:
        return df[data_cols[0]].iloc[0]
    if data_cols and set(data_cols) != set(df.columns):
        return df[data_cols].reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Input preparation per combo
# ---------------------------------------------------------------------------

def _prepare_input(
    var_spec: Any,
    metadata: dict,
    schema_keys: list[str],
    as_table: bool,
    where=None,
) -> Any:
    """Prepare a single data input for the current combo."""
    if isinstance(var_spec, Merge):
        return _prepare_merge(var_spec, metadata, schema_keys, where)

    if isinstance(var_spec, Fixed) and isinstance(var_spec.data, Merge):
        raise TypeError(
            "Fixed cannot wrap a Merge. Use Fixed on individual "
            "constituents inside the Merge instead: "
            "Merge(Fixed(df1, ...), df2)"
        )

    # Resolve the raw DataFrame and effective metadata
    df, effective_metadata, column_selection = _resolve_data_spec(var_spec, metadata)

    if not _is_per_combo_df(df, schema_keys):
        # Constant DataFrame — pass unchanged every iteration
        if column_selection is not None:
            return _apply_column_selection(df, column_selection)
        return df

    filtered = _filter_df_for_combo(df, effective_metadata, schema_keys)
    filtered = _apply_where_filter(filtered, where)

    if column_selection is not None:
        if as_table:
            # Keep schema columns alongside selected data columns
            keep = [c for c in filtered.columns if c in schema_keys] + column_selection
            return filtered[keep]
        return _apply_column_selection(filtered, column_selection)

    return _extract_data(filtered, schema_keys, as_table)


def _resolve_data_spec(
    var_spec: Any, metadata: dict
) -> tuple["pd.DataFrame", dict, list[str] | None]:
    """Resolve a var_spec into (DataFrame, effective_metadata, column_selection)."""
    column_selection = None

    if isinstance(var_spec, Fixed):
        effective_metadata = {**metadata, **var_spec.fixed_metadata}
        inner = var_spec.data
        if isinstance(inner, ColumnSelection):
            column_selection = inner.columns
            df = inner.data
        else:
            df = inner
    elif isinstance(var_spec, ColumnSelection):
        effective_metadata = metadata
        column_selection = var_spec.columns
        df = var_spec.data
    else:
        # Plain DataFrame
        effective_metadata = metadata
        df = var_spec

    return df, effective_metadata, column_selection


def _apply_column_selection(df: "pd.DataFrame", columns: list[str]) -> Any:
    """Extract selected columns from a DataFrame."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(
            f"Column(s) {missing} not found. "
            f"Available columns: {list(df.columns)}"
        )
    if len(columns) == 1:
        return df[columns[0]].values
    return df[columns]


# ---------------------------------------------------------------------------
# Merge handling
# ---------------------------------------------------------------------------

def _prepare_merge(
    merge_spec: Merge,
    metadata: dict,
    schema_keys: list[str],
    where=None,
) -> "pd.DataFrame":
    """Filter each constituent of a Merge and combine into a single DataFrame."""
    import pandas as pd

    parts = []

    for i, spec in enumerate(merge_spec.tables):
        label = f"merge[{i}]"

        df, effective_metadata, column_selection = _resolve_data_spec(spec, metadata)

        if _is_per_combo_df(df, schema_keys):
            filtered = _filter_df_for_combo(df, effective_metadata, schema_keys)
            filtered = _apply_where_filter(filtered, where)
            # Drop schema key columns for merge
            data_cols = [c for c in filtered.columns if c not in schema_keys]
            if data_cols and set(data_cols) != set(filtered.columns):
                part_df = filtered[data_cols].reset_index(drop=True)
            else:
                part_df = filtered.reset_index(drop=True)
        else:
            part_df = df.reset_index(drop=True)

        if column_selection is not None:
            missing = [c for c in column_selection if c not in part_df.columns]
            if missing:
                raise KeyError(
                    f"Column(s) {missing} not found in {label}. "
                    f"Available: {list(part_df.columns)}"
                )
            if len(column_selection) == 1:
                part_df = pd.DataFrame({column_selection[0]: part_df[column_selection[0]]})
            else:
                part_df = part_df[column_selection]

        parts.append(part_df)

    return _merge_parts(parts)


def _merge_parts(parts: list["pd.DataFrame"]) -> "pd.DataFrame":
    """Merge multiple DataFrames column-wise."""
    import pandas as pd

    if not parts:
        raise ValueError("Merge has no constituents.")

    # Check for column name conflicts
    seen_columns: set[str] = set()
    for df in parts:
        for col in df.columns:
            if col in seen_columns:
                raise KeyError(
                    f"Column name conflict in Merge: "
                    f"column '{col}' appears in multiple constituents."
                )
            seen_columns.add(col)

    # Check row count compatibility
    row_counts = [(len(df), df) for df in parts if len(df) > 1]
    if row_counts:
        unique_counts = set(n for n, _ in row_counts)
        if len(unique_counts) > 1:
            detail = ", ".join(str(n) for n, _ in row_counts)
            raise ValueError(
                f"Cannot merge constituents with different row counts: {detail}."
            )
        target_len = row_counts[0][0]
    else:
        target_len = 1

    expanded = []
    for df in parts:
        if len(df) == 1 and target_len > 1:
            df = pd.concat([df] * target_len, ignore_index=True)
        expanded.append(df)

    return pd.concat(expanded, axis=1)


# ---------------------------------------------------------------------------
# Empty-list resolution from DataFrame inputs
# ---------------------------------------------------------------------------

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
    if isinstance(var_spec, Fixed) and _is_dataframe(var_spec.data):
        return var_spec.data
    if isinstance(var_spec, ColumnSelection) and _is_dataframe(var_spec.data):
        return var_spec.data
    return None


# ---------------------------------------------------------------------------
# Result collection
# ---------------------------------------------------------------------------

def _results_to_output_dataframe(
    collected_rows: list[tuple[dict, tuple]],
    output_names: list[str],
) -> "pd.DataFrame":
    """Build a combined DataFrame from all for_each results."""
    import pandas as pd

    if not collected_rows:
        return pd.DataFrame()

    # Check if all outputs are DataFrames (flatten mode)
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


# ---------------------------------------------------------------------------
# Distribute
# ---------------------------------------------------------------------------

def _split_for_distribute(data: Any) -> list[Any]:
    """Split data into elements for distribute-style expansion."""
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


# ---------------------------------------------------------------------------
# Display / dry-run
# ---------------------------------------------------------------------------

def _format_inputs(inputs: dict[str, Any]) -> str:
    """Format inputs dict for display."""
    parts = []
    for name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            parts.append(f"{name}: {var_spec.__name__}")
        elif isinstance(var_spec, Fixed):
            fixed_str = ", ".join(f"{k}={v}" for k, v in var_spec.fixed_metadata.items())
            inner = var_spec.data
            if isinstance(inner, ColumnSelection):
                inner_name = inner.__name__
            elif _is_dataframe(inner):
                inner_name = f"DataFrame{list(inner.columns)}"
            else:
                inner_name = repr(inner)
            parts.append(f"{name}: Fixed({inner_name}, {fixed_str})")
        elif isinstance(var_spec, ColumnSelection):
            parts.append(f"{name}: {var_spec.__name__}")
        elif _is_dataframe(var_spec):
            parts.append(f"{name}: DataFrame{list(var_spec.columns)}")
        elif _is_data_input(var_spec):
            parts.append(f"{name}: {repr(var_spec)}")
        else:
            parts.append(f"{name}: {var_spec!r}")
    return "{" + ", ".join(parts) + "}"


def _print_dry_run_iteration(
    inputs: dict[str, Any],
    metadata: dict[str, Any],
    constant_inputs: dict[str, Any],
    pass_metadata: bool = False,
    distribute: str | None = None,
) -> None:
    """Print what would happen for one iteration in dry-run mode."""
    metadata_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
    print(f"[dry-run] {metadata_str}:")

    for param_name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            print(f"  merge {param_name}:")
            for i, sub_spec in enumerate(var_spec.tables):
                _print_constituent_filter(sub_spec, metadata, i)
        elif isinstance(var_spec, Fixed):
            filter_metadata = {**metadata, **var_spec.fixed_metadata}
            inner = var_spec.data
            if isinstance(inner, ColumnSelection):
                col_str = ", ".join(inner.columns)
                print(f"  filter {param_name} with {filter_metadata} -> columns: [{col_str}]")
            elif _is_dataframe(inner):
                print(f"  filter {param_name} = DataFrame with {filter_metadata}")
            else:
                print(f"  filter {param_name} with {filter_metadata}")
        elif isinstance(var_spec, ColumnSelection):
            col_str = ", ".join(var_spec.columns)
            print(f"  filter {param_name} with {metadata} -> columns: [{col_str}]")
        elif _is_dataframe(var_spec):
            print(f"  filter {param_name} = DataFrame with {metadata}")
        elif _is_data_input(var_spec):
            print(f"  filter {param_name} with {metadata}")
        else:
            print(f"  constant {param_name} = {var_spec!r}")

    if pass_metadata:
        print(f"  pass metadata: {metadata_str}")

    if distribute is not None:
        print(f"  distribute by '{distribute}' (1-based indexing)")


def _print_constituent_filter(spec: Any, metadata: dict[str, Any], index: int) -> None:
    """Print a single Merge constituent's filter line for dry-run display."""
    if isinstance(spec, Fixed):
        filter_metadata = {**metadata, **spec.fixed_metadata}
        inner = spec.data
        if isinstance(inner, ColumnSelection):
            col_str = ", ".join(inner.columns)
            print(f"    [{index}] filter with {filter_metadata} -> columns: [{col_str}]")
        else:
            print(f"    [{index}] filter with {filter_metadata}")
    elif isinstance(spec, ColumnSelection):
        col_str = ", ".join(spec.columns)
        print(f"    [{index}] filter with {metadata} -> columns: [{col_str}]")
    else:
        print(f"    [{index}] filter with {metadata}")
