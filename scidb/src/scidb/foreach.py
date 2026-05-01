"""DB-backed for_each wrapper — loads inputs, delegates loop to scifor, saves outputs."""

import json
import time
import warnings
from typing import Any, Callable

from .log import Log

import scifor as _scifor
from scifor import for_each as _scifor_for_each
from scifor.pathinput import PathInput

from .colname import ColName
from .column_selection import ColumnSelection
from .fixed import Fixed
from .each_of import EachOf
from .foreach_config import ForEachConfig
from .merge import Merge


# ---------------------------------------------------------------------------
# Sentinel classes for per-combo loading
# ---------------------------------------------------------------------------

class PerComboLoader:
    """Sentinel for inputs that need per-combo loading (class lacks load_all).

    ``spec`` can be:
    - A plain class (has .load())
    - A ``Fixed`` wrapping a plain class (load with overridden metadata)
    - A ``ColumnSelection`` wrapping a plain class (load, then select cols)
    - A ``Fixed`` wrapping a ``ColumnSelection`` (both overrides)

    ``for_each`` wraps fn so these are resolved per-combo via cls.load(**combo).
    """
    __slots__ = ("spec",)

    def __init__(self, spec: Any):
        self.spec = spec


class PerComboLoaderMerge:
    """Sentinel for Merge where some/all constituents lack load_all.

    Holds the original ``scidb.Merge`` spec; ``for_each`` wraps fn to
    resolve each constituent per-combo via cls.load(**combo_metadata).
    """
    __slots__ = ("merge_spec",)

    def __init__(self, merge_spec: "Merge"):
        self.merge_spec = merge_spec


class _DryRunMerge(_scifor.Merge):
    """scifor.Merge subclass used only for dry_run display.

    Has the correct ``__name__`` from the scidb.Merge spec so scifor
    prints ``Merge(GaitData, ForceData)`` instead of a repr string.
    """

    def __init__(self, scidb_merge):
        # Do NOT call super().__init__ — bypass validation for display only
        import pandas as pd
        self._dry_name = scidb_merge.__name__
        # scifor loops over self.tables in _print_dry_run_iteration
        self.tables = [pd.DataFrame() for _ in scidb_merge.var_specs]

    @property
    def __name__(self) -> str:  # type: ignore[override]
        return self._dry_name


# ---------------------------------------------------------------------------
# Main for_each entry point
# ---------------------------------------------------------------------------

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
    _inject_combo_metadata: bool = False,
    _pre_combo_hook: "Callable[[dict], bool] | None" = None,
    _progress_fn: "Callable[[dict], None] | None" = None,
    _cancel_check: "Callable[[], bool] | None" = None,
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
    5. Converts scidb wrappers → scifor wrappers
    6. Delegates the core loop to ``scifor.for_each``
    7. Saves results from the returned table

    Args:
        fn: The function to execute (plain function handle, no Thunk wrapping).
        inputs: Dict mapping parameter names to variable types, Fixed wrappers,
                Merge wrappers, ColumnSelection wrappers, PathInput, or constants.
        outputs: List of output types/objects with ``.save()``.
        dry_run: If True, only print what would happen without executing.
        save: If True (default), save each function run's output.
        as_table: Controls which inputs are passed as full DataFrames.
        db: Optional database instance.
        distribute: If True, split outputs and save each piece at the schema
                    level below the deepest iterated key.
        where: Optional filter; passed to .load() calls on DB-backed inputs.
        _inject_combo_metadata: If True, inject current-combo metadata keys
                    as extra kwargs to fn (used by scihist for generates_file).
        _pre_combo_hook: Internal use only. Called with each fully-expanded
                    combo dict before inputs are loaded. If it returns True
                    the combo is skipped entirely (no load, no call, no save).
                    Used by scihist.for_each to implement skip_computed.
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, or None when dry_run=True.
    """
    # --- EachOf expansion: must be first, before any other logic ---
    each_of_axes = []
    for param, val in inputs.items():
        if isinstance(val, EachOf):
            each_of_axes.append(("input", param, val.alternatives))
    if isinstance(where, EachOf):
        each_of_axes.append(("where", None, where.alternatives))

    if each_of_axes:
        import pandas as pd
        from itertools import product as _eachof_product

        results = []
        for combo in _eachof_product(*(axis[2] for axis in each_of_axes)):
            concrete_inputs = dict(inputs)
            concrete_where = where
            for (kind, param, _alts), value in zip(each_of_axes, combo):
                if kind == "input":
                    concrete_inputs[param] = value
                elif kind == "where":
                    concrete_where = value
            result = for_each(
                fn,
                concrete_inputs,
                outputs,
                dry_run=dry_run,
                save=save,
                as_table=as_table,
                db=db,
                distribute=distribute,
                where=concrete_where,
                _inject_combo_metadata=_inject_combo_metadata,
                _pre_combo_hook=_pre_combo_hook,
                _progress_fn=_progress_fn,
                _cancel_check=_cancel_check,
                **metadata_iterables,
            )
            if result is not None:
                results.append(result)
            # Cooperative cancel: stop iterating across EachOf alternatives
            # as soon as the user cancels — don't start the next concrete run.
            if _cancel_check is not None and _cancel_check():
                break
        return pd.concat(results, ignore_index=True) if results else None

    fn_name = getattr(fn, "__name__", repr(fn))
    Log.info(f"===== for_each({fn_name}) start =====")

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
                msg = f"no values found for '{key}' in database, 0 iterations"
                print(f"[warn] {msg}")
                Log.warn(msg)
            else:
                Log.info(f"resolved {key}=[] -> {len(values)} values")
            metadata_iterables[key] = values

    # --- PathInput discovery: populate metadata from filesystem when DB is empty ---
    _discovered_combos = None
    if _has_pathinput(inputs):
        pi = _find_pathinput(inputs)
        if pi is not None:
            # Case 1: No metadata keys passed at all — discover everything
            if not metadata_iterables:
                combos = pi.discover()
                Log.debug(f"PathInput discovery: template={pi.path_template!r}, "
                          f"root_folder={pi.root_folder!r}, "
                          f"matching_files={len(combos)}")
                if combos:
                    for key in combos[0].keys():
                        metadata_iterables[key] = list(dict.fromkeys(c[key] for c in combos))
                        Log.info(f"discovered {key} -> {len(metadata_iterables[key])} values from filesystem")
                    _discovered_combos = combos

            # Case 2: Some keys have empty [] (resolved to empty from DB) — fill from discovery
            still_empty = [k for k, v in metadata_iterables.items()
                           if isinstance(v, list) and len(v) == 0]
            if still_empty:
                combos = pi.discover()
                Log.debug(f"PathInput discovery: template={pi.path_template!r}, "
                          f"root_folder={pi.root_folder!r}, "
                          f"matching_files={len(combos)}")
                if combos:
                    for key in still_empty:
                        if key in combos[0]:
                            values = list(dict.fromkeys(c[key] for c in combos))
                            metadata_iterables[key] = values
                            Log.info(f"discovered {key} -> {len(values)} values from filesystem")
                    _discovered_combos = combos

    # Propagate schema keys to scifor so distribute and DataFrame detection work
    _propagate_schema(db, distribute)

    # Stringify metadata_iterables values for schema keys.
    # _load_var_type_all stringifies schema columns in loaded DataFrames (DB returns
    # typed values like np.int64); combo metadata must match to filter correctly.
    _resolved_db_for_str = db
    if _resolved_db_for_str is None:
        try:
            from scidb.database import get_database
            _resolved_db_for_str = get_database()
        except Exception:
            _resolved_db_for_str = None
    if _resolved_db_for_str is not None and hasattr(_resolved_db_for_str, 'dataset_schema_keys'):
        from scidb.database import _schema_str
        _sk_set = set(_resolved_db_for_str.dataset_schema_keys)
        for key in list(metadata_iterables.keys()):
            if key in _sk_set:
                metadata_iterables[key] = [
                    _schema_str(v) for v in metadata_iterables[key]
                ]

    # Build output_names for scifor
    output_names = [_output_name(o) for o in outputs] if outputs else ["result"]

    # --- Dry-run shortcut: convert inputs for display only, call scifor, return ---
    if dry_run:
        display_inputs = _convert_inputs_for_display(inputs)
        _scifor_for_each(
            fn,
            display_inputs,
            dry_run=True,
            as_table=as_table,
            distribute=distribute,
            output_names=output_names,
            _cancel_check=_cancel_check,
            **metadata_iterables,
        )
        return None

    # Build ForEachConfig version keys (DB-specific; not part of scifor)
    config = ForEachConfig(
        fn=fn,
        inputs=inputs,
        where=where,
        distribute=distribute,
        as_table=as_table,
    )
    config_keys = config.to_version_keys()
    call_id = config.to_call_id()
    Log.debug(f"for_each({fn_name}): call_id={call_id}")

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
                msg = (f"filtered {removed} non-existent schema combinations "
                       f"(from {len(raw_combos)} to {len(filtered)})")
                print(f"[info] {msg}")
                Log.info(msg)
            all_combos = filtered

    # Load all inputs into DataFrames (with __record_id and __branch_params)
    loaded_inputs = _convert_inputs(inputs, db, where)

    # --- Variant tracking: build rid→bp mapping and __rid_{param} discriminator columns ---
    import pandas as pd
    from itertools import product as _iproduct

    Log.debug("building variant tracking (rid->branch_params mapping)")
    rid_to_bp: dict = {}   # {record_id: branch_params_dict}
    rid_keys: list = []    # __rid_{param_name} column names added to this call's schema

    for param_name, data in list(loaded_inputs.items()):
        if not isinstance(data, pd.DataFrame) or "__record_id" not in data.columns:
            continue

        # Build rid→bp from this input's DataFrame
        bp_col = "__branch_params" if "__branch_params" in data.columns else None
        for _, row in data.iterrows():
            rid = row["__record_id"]
            if rid is None:
                continue
            bp_raw = row[bp_col] if bp_col else "{}"
            rid_to_bp[rid] = json.loads(bp_raw or "{}") if isinstance(bp_raw, str) else {}

        # Rename __record_id → __rid_{param_name} so per-param tracking is unambiguous
        rid_col = f"__rid_{param_name}"
        loaded_inputs[param_name] = data.rename(columns={"__record_id": rid_col})
        rid_keys.append(rid_col)

    Log.debug(f"variant tracking: {len(rid_to_bp)} record_ids mapped, "
              f"{len(rid_keys)} rid keys: {rid_keys}")

    # Strip __branch_params from all DataFrames (now tracked via rid_to_bp)
    for param_name, data in list(loaded_inputs.items()):
        if isinstance(data, pd.DataFrame) and "__branch_params" in data.columns:
            loaded_inputs[param_name] = data.drop(columns=["__branch_params"])

    # --- Build full combos: base_combos × valid rid-combos per schema location ---
    current_schema_keys = list(_scifor.get_schema() or [])

    base_combos = all_combos
    if base_combos is None and _discovered_combos is not None:
        # Use filesystem-discovered combos directly (avoids non-existent Cartesian combos)
        base_combos = _discovered_combos
        Log.info(f"using {len(base_combos)} filesystem-discovered combos")
    if base_combos is None:
        keys = list(metadata_iterables.keys())
        value_lists = [metadata_iterables[k] for k in keys]
        base_combos = [dict(zip(keys, combo)) for combo in _iproduct(*value_lists)]

    # Detect aggregation mode: not all schema keys are being iterated, so
    # lower-level records should be aggregated into multi-row DataFrames
    # rather than being separated into individual combos via rid expansion.
    _iterated_schema_keys = set(metadata_iterables.keys()) & set(current_schema_keys)
    _aggregation_mode = len(current_schema_keys) > 0 and len(_iterated_schema_keys) < len(current_schema_keys)

    if _aggregation_mode:
        # Aggregation mode: skip rid expansion.  Strip __rid_* columns from
        # loaded DataFrames so the user's function doesn't see internal
        # tracking columns, and pass base_combos straight through.
        for param_name, data in list(loaded_inputs.items()):
            if isinstance(data, pd.DataFrame):
                rid_cols_in_df = [c for c in data.columns if c.startswith("__rid_")]
                if rid_cols_in_df:
                    loaded_inputs[param_name] = data.drop(columns=rid_cols_in_df)
        rid_keys = []
        rid_per_combo = {}
        full_combos = list(base_combos)
        Log.info(f"aggregation mode: skipped rid expansion, "
                 f"iterating {list(_iterated_schema_keys) or '(none)'} "
                 f"of schema {current_schema_keys}, "
                 f"{len(full_combos)} combo(s)")
    else:
        # Full iteration mode: expand combos with rid variants.

        # Lookup keys for rid disambiguation: schema keys + any non-schema metadata
        # iterable keys.  Using only schema keys misses non-schema iterables (e.g.
        # "session") that ARE present in the loaded DataFrame and should distinguish
        # which record belongs to which combo.
        _lookup_keys = list(dict.fromkeys(
            current_schema_keys +
            [k for k in metadata_iterables if k not in set(current_schema_keys)]
        ))

        # For each rid_key, map combo_tuple → [rid_values at that combo]
        rid_per_combo: dict = {}
        for rid_col in rid_keys:
            param_name = rid_col[len("__rid_"):]
            df = loaded_inputs.get(param_name)
            if not isinstance(df, pd.DataFrame) or rid_col not in df.columns:
                continue
            schema_cols_in_df = [k for k in _lookup_keys if k in df.columns]
            mapping: dict = {}
            if schema_cols_in_df:
                for combo_vals, group in df.groupby(schema_cols_in_df, sort=False):
                    raw_key = combo_vals if isinstance(combo_vals, tuple) else (combo_vals,)
                    # Expand to ALL _lookup_keys, filling missing cols with ""
                    col_val = {sk: ("" if v is None else str(v))
                               for sk, v in zip(schema_cols_in_df, raw_key)}
                    key = tuple(col_val.get(sk, "") for sk in _lookup_keys)
                    mapping[key] = group[rid_col].tolist()
            else:
                # No lookup cols in df — use all-empty key
                mapping[tuple("" for _ in _lookup_keys)] = df[rid_col].tolist()
            rid_per_combo[rid_col] = mapping

        # Expand each base combo with all valid rid-combos for that schema location
        Log.debug(f"expanding combos: {len(base_combos)} base combos, "
                  f"{len(rid_per_combo)} rid dimensions")
        full_combos: list = []
        for combo in base_combos:
            schema_vals = tuple(str(combo.get(k, "")) for k in _lookup_keys)

            rid_lists: list = []
            rid_col_names: list = []
            valid = True
            for rid_col, mapping in rid_per_combo.items():
                rids = mapping.get(schema_vals, [])
                if not rids:
                    valid = False
                    break
                rid_lists.append(rids)
                rid_col_names.append(rid_col)

            if not valid:
                continue

            if rid_lists:
                for rid_combo in _iproduct(*rid_lists):
                    full_combo = {**combo}
                    for rc_name, rc_val in zip(rid_col_names, rid_combo):
                        full_combo[rc_name] = rc_val
                    full_combos.append(full_combo)
            else:
                full_combos.append(combo)

        if len(full_combos) != len(base_combos):
            Log.info(f"expanded {len(base_combos)} base combos -> "
                     f"{len(full_combos)} full combos (rid variants)")
        else:
            Log.debug(f"{len(full_combos)} combos (no rid expansion needed)")

    # Persist the full expected combo set BEFORE skip_computed filtering,
    # so check_node_state knows all combos that should exist (including
    # ones that failed or were skipped).  Only needed when we actually
    # have outputs and are not in dry_run mode.
    if not dry_run and outputs:
        _persist_expected_combos(db, fn_name, call_id, full_combos)

    # Apply pre-combo hook (e.g. skip_computed from scihist): filter out any
    # combos where the hook returns True.
    if _pre_combo_hook is not None:
        pre_hook_count = len(full_combos)
        full_combos = [c for c in full_combos if not _pre_combo_hook(c)]
        skipped = pre_hook_count - len(full_combos)
        if skipped > 0:
            msg = f"skip_computed: {skipped}/{pre_hook_count} combos skipped"
            print(f"[info] {msg}")
            Log.info(msg)

    # Temporarily extend scifor's schema to include __rid_* keys so _filter_df_for_combo
    # treats them as schema columns (not data columns), giving single-row filtered DFs.
    if rid_keys:
        _scifor.set_schema(current_schema_keys + rid_keys)

    # Collect all rid values per key so scifor's metadata_iterables are complete
    extended_metadata_iterables = dict(metadata_iterables)
    for rid_col, mapping in rid_per_combo.items():
        all_rids: list = []
        for rids in mapping.values():
            all_rids.extend(rids)
        extended_metadata_iterables[rid_col] = list(dict.fromkeys(all_rids))  # preserve order, dedupe

    # --- Wrap fn to resolve PerComboLoader/PerComboLoaderMerge inputs per-combo,
    #     and/or inject combo metadata (for generates_file functions). ---
    _per_combo = {k: v for k, v in loaded_inputs.items()
                  if isinstance(v, (PerComboLoader, PerComboLoaderMerge))}
    if _per_combo or _inject_combo_metadata:
        _ordered_combos = full_combos
        _call_idx = [0]
        _orig_fn = fn

        def fn(**kwargs):  # noqa: F811 — intentional rebind
            idx = _call_idx[0]
            _call_idx[0] = idx + 1
            current_combo = _ordered_combos[idx] if idx < len(_ordered_combos) else {}
            load_kw = {k: v for k, v in current_combo.items() if not k.startswith("__")}
            resolved = {}
            for k, v in kwargs.items():
                if isinstance(v, PerComboLoader):
                    resolved[k] = _resolve_per_combo_loader(v, load_kw)
                elif isinstance(v, PerComboLoaderMerge):
                    resolved[k] = _resolve_per_combo_merge(v, load_kw)
                else:
                    resolved[k] = v
            if _inject_combo_metadata:
                for k, v in load_kw.items():
                    if k not in resolved:
                        resolved[k] = v
            return _orig_fn(**resolved)

    # Wrap _progress_fn to track final completed/skipped counts for logging.
    _run_summary = {"total": 0, "completed": 0, "skipped": 0}

    def _tracking_progress_fn(info: dict):
        _run_summary["total"] = info.get("total", _run_summary["total"])
        _run_summary["completed"] = info.get("completed", _run_summary["completed"])
        _run_summary["skipped"] = info.get("skipped", _run_summary["skipped"])
        if _progress_fn is not None:
            _progress_fn(info)

    # Delegate core loop to scifor
    result_tbl = _scifor_for_each(
        fn,
        loaded_inputs,
        dry_run=False,
        as_table=as_table,
        distribute=distribute,
        output_names=output_names,
        _all_combos=full_combos,
        _log_fn=Log.info,
        _progress_fn=_tracking_progress_fn,
        _cancel_check=_cancel_check,
        **extended_metadata_iterables,
    )

    # Log run summary with failed repetition count.
    if _run_summary["total"] > 0:
        Log.debug(f"for_each({fn_name}): completed={_run_summary['completed']}, "
                  f"failed={_run_summary['skipped']}, total={_run_summary['total']}")

    # Restore scifor's schema
    if rid_keys:
        _scifor.set_schema(current_schema_keys)

    if result_tbl is None:
        return None

    # Save results
    if save and outputs and not result_tbl.empty:
        save_t0 = time.perf_counter()
        _save_results(result_tbl, outputs, output_names, config_keys, db,
                      rid_to_bp=rid_to_bp, rid_keys=rid_keys)
        save_elapsed = time.perf_counter() - save_t0
        Log.info(f"for_each({fn_name}): saved {len(result_tbl)} results in {save_elapsed:.3f}s")

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
    total_t0 = time.perf_counter()
    for param_name, var_spec in inputs.items():
        if isinstance(var_spec, ColName):
            result[param_name] = _resolve_colname_from_db(var_spec, db)
        elif _is_loadable(var_spec):
            t0 = time.perf_counter()
            loaded = _load_input(var_spec, db, where)
            elapsed = time.perf_counter() - t0
            result[param_name] = loaded
            _log_loaded_input(param_name, var_spec, loaded, elapsed)
        else:
            # Constant — pass through unchanged
            result[param_name] = var_spec
            Log.debug(f"input '{param_name}': constant {type(var_spec).__name__}")
    total_elapsed = time.perf_counter() - total_t0
    Log.info(f"loaded {len(result)} inputs in {total_elapsed:.3f}s")
    return result


def _log_loaded_input(param_name: str, var_spec: Any, loaded: Any, elapsed: float) -> None:
    """Log details about a loaded input."""
    import pandas as pd

    type_name = _input_type_name(var_spec)

    if isinstance(loaded, pd.DataFrame):
        Log.info(f"input '{param_name}': loaded {type_name} -> "
                 f"{len(loaded)} rows, {len(loaded.columns)} cols in {elapsed:.3f}s")
    elif isinstance(loaded, (PerComboLoader, PerComboLoaderMerge)):
        Log.info(f"input '{param_name}': {type_name} (per-combo loader, will load during iteration)")
    else:
        Log.info(f"input '{param_name}': loaded {type_name} in {elapsed:.3f}s")


def _input_type_name(var_spec: Any) -> str:
    """Get a human-readable type name for a var_spec."""
    if isinstance(var_spec, Merge):
        return var_spec.__name__
    if isinstance(var_spec, Fixed):
        inner = var_spec.var_type
        inner_name = _input_type_name(inner)
        fixed_str = ", ".join(f"{k}={v}" for k, v in var_spec.fixed_metadata.items())
        return f"Fixed({inner_name}, {fixed_str})"
    if isinstance(var_spec, ColumnSelection):
        inner_name = _input_type_name(var_spec.var_type)
        return f"ColumnSelection({inner_name}, {var_spec.columns})"
    if isinstance(var_spec, type):
        return var_spec.__name__
    if hasattr(var_spec, '__name__'):
        return var_spec.__name__
    return type(var_spec).__name__


def _convert_inputs_for_display(inputs: dict[str, Any]) -> dict[str, Any]:
    """Convert inputs for dry_run display without actually loading any data.

    scidb-specific types (Merge, Fixed, ColumnSelection, classes) are converted
    to scifor-compatible display forms or left as constants.
    """
    import pandas as pd

    result = {}
    for param_name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            # Use _DryRunMerge so scifor prints "merge {param_name}:" and class names
            result[param_name] = _DryRunMerge(var_spec)
        elif isinstance(var_spec, ColumnSelection):
            dummy = pd.DataFrame(columns=var_spec.columns)
            result[param_name] = _scifor.ColumnSelection(dummy, var_spec.columns)
        elif isinstance(var_spec, Fixed) and not isinstance(var_spec.var_type, Merge):
            dummy = pd.DataFrame()
            result[param_name] = _scifor.Fixed(dummy, **var_spec.fixed_metadata)
        else:
            # Constants, plain classes, etc. — pass through (shown as constants by scifor)
            result[param_name] = var_spec
    return result


def _resolve_colname_from_db(colname: "ColName", db: Any | None) -> str:
    """Resolve a ColName wrapper to the single data column name string.

    Uses the variable's dtype metadata from the database to determine
    what data columns exist, then subtracts schema keys.
    """
    import json

    var_type = colname.var_type

    # Get the database
    resolved_db = db
    if resolved_db is None:
        try:
            from scidb.database import get_database
            resolved_db = get_database()
        except Exception:
            raise ValueError(
                "ColName requires a database to resolve column names. "
                "Either pass db= to for_each or call configure_database() first."
            )

    var_name = var_type.__name__ if isinstance(var_type, type) else type(var_type).__name__
    schema_keys = list(resolved_db.dataset_schema_keys)

    # Query the _variables table for dtype metadata
    try:
        row = resolved_db._execute(
            "SELECT dtype FROM _variables WHERE variable_name = ?",
            [var_name],
        ).fetchone()
    except Exception:
        row = None

    if row is None:
        # Variable not yet saved — try using view_name for single-column mode
        if hasattr(var_type, 'view_name'):
            return var_type.view_name()
        return var_name

    dtype_meta = json.loads(row[0])
    mode = dtype_meta.get("mode", "single_column")

    if mode == "single_column":
        # Single-column variables always have exactly one data column
        col_names = list(dtype_meta.get("columns", {}).keys())
        if col_names:
            return col_names[0]
        if hasattr(var_type, 'view_name'):
            return var_type.view_name()
        return var_name

    if mode == "dataframe":
        # DataFrame variables: subtract schema keys from df_columns
        df_columns = dtype_meta.get("df_columns", list(dtype_meta.get("columns", {}).keys()))
        data_cols = [c for c in df_columns if c not in schema_keys]
        if len(data_cols) == 1:
            return data_cols[0]
        elif len(data_cols) == 0:
            raise ValueError(
                f"ColName({var_name}): variable has no data columns "
                f"(all columns are schema keys). "
                f"Columns: {df_columns}, schema keys: {schema_keys}"
            )
        else:
            raise ValueError(
                f"ColName({var_name}): variable has {len(data_cols)} "
                f"data columns ({data_cols}), expected exactly 1. "
                f"Schema keys: {schema_keys}"
            )

    if mode == "multi_column":
        raise ValueError(
            f"ColName({var_name}): not supported for dict-type (multi_column) variables. "
            f"ColName only works with single-column or single-data-column DataFrame variables."
        )

    # Unknown mode — fall back to view_name
    if hasattr(var_type, 'view_name'):
        return var_type.view_name()
    return var_name


def _load_input(var_spec: Any, db: Any | None, where: Any | None) -> Any:
    """Load a single input and return a scifor-compatible wrapper or sentinel."""
    import pandas as pd

    # Already a DataFrame — pass through
    if isinstance(var_spec, pd.DataFrame):
        return var_spec

    # Merge: check if any constituent needs per-combo loading
    if isinstance(var_spec, Merge):
        if _merge_needs_per_combo(var_spec):
            return PerComboLoaderMerge(var_spec)
        # All constituents can be pre-loaded
        loaded_tables = []
        for sub_spec in var_spec.var_specs:
            loaded_tables.append(_load_input(sub_spec, db, where))
        return _scifor.Merge(*loaded_tables)

    # Fixed: check for Fixed(Merge(...)) error, then load inner
    if isinstance(var_spec, Fixed):
        if isinstance(var_spec.var_type, Merge):
            raise TypeError(
                "Fixed cannot wrap a Merge. Use Fixed on individual "
                "constituents inside the Merge instead: "
                "Merge(Fixed(df1, ...), df2)"
            )
        inner_loaded = _load_input(var_spec.var_type, db, where)
        if isinstance(inner_loaded, PerComboLoader):
            # Inner needs per-combo loading; wrap the whole Fixed spec
            return PerComboLoader(var_spec)
        # Strip internal tracking columns — Fixed inputs are not part of
        # the rid expansion (they're tracked separately in scihist), so
        # __record_id / __branch_params would confuse _extract_data.
        import pandas as pd
        if isinstance(inner_loaded, pd.DataFrame):
            _drop = [c for c in ("__record_id", "__branch_params")
                     if c in inner_loaded.columns]
            if _drop:
                inner_loaded = inner_loaded.drop(columns=_drop)
        # Stringify fixed_metadata schema keys to match the stringified
        # DataFrame columns produced by _load_var_type_all.
        fixed_meta = dict(var_spec.fixed_metadata)
        _sk = _get_schema_keys(db)
        if _sk:
            from .database import _schema_str
            fixed_meta = {
                k: _schema_str(v) if k in _sk else v
                for k, v in fixed_meta.items()
            }
        return _scifor.Fixed(inner_loaded, **fixed_meta)

    # ColumnSelection: load inner var_type if possible, else per-combo
    if isinstance(var_spec, ColumnSelection):
        if hasattr(var_spec.var_type, 'load_all'):
            loaded_df = _load_var_type_all(var_spec.var_type, db, where)
            return _scifor.ColumnSelection(loaded_df, var_spec.columns)
        return PerComboLoader(var_spec)

    # PathInput: needs per-combo resolution via load(**combo); wrap in PerComboLoader
    if isinstance(var_spec, PathInput):
        return PerComboLoader(var_spec)

    # Variable type (class with .load()): bulk load or per-combo
    if isinstance(var_spec, type) or hasattr(var_spec, 'load'):
        if hasattr(var_spec, 'load_all'):
            return _load_var_type_all(var_spec, db, where)
        return PerComboLoader(var_spec)

    # Unknown — return as-is
    return var_spec


def _merge_needs_per_combo(merge_spec: "Merge") -> bool:
    """Return True if any Merge constituent lacks load_all."""
    for spec in merge_spec.var_specs:
        cls = _get_loadable_class_from_spec(spec)
        if cls is not None and not hasattr(cls, 'load_all'):
            return True
    return False


def _get_loadable_class_from_spec(spec: Any) -> Any:
    """Extract the innermost loadable class from a spec (class, Fixed, ColumnSelection)."""
    if isinstance(spec, Fixed):
        spec = spec.var_type
    if isinstance(spec, ColumnSelection):
        spec = spec.var_type
    if isinstance(spec, type) or hasattr(spec, 'load'):
        return spec
    return None


def _load_var_type_all(
    var_type: Any,
    db: Any | None,
    where: Any | None,
) -> "pd.DataFrame":
    """Bulk load all records for a variable type into a DataFrame with metadata columns."""
    import pandas as pd

    db_kwargs = {"db": db} if db is not None else {}
    where_kwargs = {"where": where} if where is not None else {}

    # Use load_all (not load) to avoid AmbiguousVersionError when multiple
    # variants exist at the same schema location — we want ALL variants here.
    loaded = list(var_type.load_all(version_id="latest", **db_kwargs, **where_kwargs))

    if not loaded:
        return pd.DataFrame()

    # Determine schema keys to stringify — scifor compares schema cols as strings
    # (metadata_iterables values come from the user as strings, e.g. session="1"),
    # but the database may return typed values (e.g. session=np.int64(1)).
    _schema_keys: set = set()
    _resolved_db = db
    if _resolved_db is None:
        try:
            from scidb.database import get_database
            _resolved_db = get_database()
        except Exception:
            pass
    if _resolved_db is not None and hasattr(_resolved_db, 'dataset_schema_keys'):
        _schema_keys = set(_resolved_db.dataset_schema_keys)

    def _stringify_meta(meta: dict) -> dict:
        """Convert schema key values to strings and drop __ version keys.

        Also drops keys that came from constants (stored in __constants) so
        that constants stored in version_keys don't pollute the input DataFrame
        and confuse scifor's data-column detection.
        """
        const_keys: set = set()
        constants_json = meta.get("__constants")
        if constants_json:
            try:
                const_keys = set(json.loads(constants_json).keys())
            except Exception:
                pass
        return {k: str(v) if k in _schema_keys and v is not None else v
                for k, v in meta.items()
                if not k.startswith("__") and k not in const_keys}

    # Check if data is DataFrames
    first = loaded[0]
    all_have_data = all(hasattr(v, 'data') for v in loaded)

    if all_have_data and isinstance(first.data, pd.DataFrame):
        # DataFrame data: build table with metadata cols + data cols
        parts = []
        for var in loaded:
            data_df = var.data
            meta = _stringify_meta(dict(var.metadata) if hasattr(var, 'metadata') and var.metadata else {})
            meta["__record_id"] = getattr(var, 'record_id', None)
            meta["__branch_params"] = json.dumps(getattr(var, 'branch_params', None) or {})
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
            row = _stringify_meta(dict(var.metadata) if hasattr(var, 'metadata') and var.metadata else {})
            row[view_name] = var.data
            row["__record_id"] = getattr(var, 'record_id', None)
            row["__branch_params"] = json.dumps(getattr(var, 'branch_params', None) or {})
            rows.append(row)
        return pd.DataFrame(rows)
    else:
        # Raw results without .data attribute — just wrap
        rows = []
        var_name = getattr(var_type, '__name__', type(var_type).__name__)
        for var in loaded:
            rows.append({var_name: var, "__record_id": getattr(var, 'record_id', None), "__branch_params": "{}"})
        return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Per-combo resolution helpers
# ---------------------------------------------------------------------------

def _resolve_per_combo_loader(pcl: "PerComboLoader", load_kw: dict) -> Any:
    """Resolve a PerComboLoader per-combo by calling spec.load(**effective_kw)."""
    spec = pcl.spec

    if isinstance(spec, Fixed):
        effective_kw = {**load_kw, **spec.fixed_metadata}
        inner = spec.var_type
        columns = None
        if isinstance(inner, ColumnSelection):
            columns = inner.columns
            inner = inner.var_type
        lv = inner.load(**effective_kw)
        raw = lv.data if hasattr(lv, 'data') else lv
        if columns is not None:
            cls_name = getattr(inner, '__name__', type(inner).__name__)
            raw = _apply_per_combo_col_selection(raw, columns, cls_name)
        return raw

    if isinstance(spec, ColumnSelection):
        lv = spec.var_type.load(**load_kw)
        raw = lv.data if hasattr(lv, 'data') else lv
        cls_name = getattr(spec.var_type, '__name__', type(spec.var_type).__name__)
        return _apply_per_combo_col_selection(raw, spec.columns, cls_name)

    # Plain class
    lv = spec.load(**load_kw)
    return lv.data if hasattr(lv, 'data') else lv


def _resolve_per_combo_merge(pcl_merge: "PerComboLoaderMerge", load_kw: dict) -> "pd.DataFrame":
    """Resolve a PerComboLoaderMerge per-combo by loading each constituent."""
    from scifor.foreach import _merge_parts as _scifor_merge_parts

    parts = []
    for spec in pcl_merge.merge_spec.var_specs:
        effective_kw = dict(load_kw)
        columns = None
        actual_spec = spec

        # Unwrap Fixed
        if isinstance(actual_spec, Fixed):
            effective_kw = {**load_kw, **actual_spec.fixed_metadata}
            actual_spec = actual_spec.var_type

        # Unwrap ColumnSelection
        if isinstance(actual_spec, ColumnSelection):
            columns = actual_spec.columns
            actual_spec = actual_spec.var_type

        # Load the variable
        lv = actual_spec.load(**effective_kw)
        cls_name = getattr(actual_spec, '__name__', type(actual_spec).__name__)
        if isinstance(lv, list):
            raise ValueError(
                f"{cls_name}.load() returned multiple results (list), expected exactly 1."
            )
        raw = lv.data if hasattr(lv, 'data') else lv

        # Convert to DataFrame
        part_df = _to_dataframe(raw, cls_name)

        # Apply column selection
        if columns is not None:
            missing = [c for c in columns if c not in part_df.columns]
            if missing:
                raise KeyError(
                    f"Columns {missing} not found in {cls_name}. "
                    f"Available: {list(part_df.columns)}"
                )
            if len(columns) == 1:
                part_df = part_df[[columns[0]]]
            else:
                part_df = part_df[columns]

        parts.append(part_df)

    return _scifor_merge_parts(parts)


def _to_dataframe(data: Any, cls_name: str) -> "pd.DataFrame":
    """Convert raw data (scalar, array, list, DataFrame) to a named DataFrame."""
    import pandas as pd
    import numpy as np

    if isinstance(data, pd.DataFrame):
        return data.reset_index(drop=True)
    if isinstance(data, np.ndarray):
        if data.ndim == 1:
            return pd.DataFrame({cls_name: data})
        elif data.ndim == 2:
            cols = [f"{cls_name}_{i}" for i in range(data.shape[1])]
            return pd.DataFrame(data, columns=cols)
        else:
            raise ValueError(f"Cannot convert {data.ndim}D array from {cls_name} to DataFrame")
    if isinstance(data, (list, tuple)):
        return pd.DataFrame({cls_name: list(data)})
    # Scalar
    return pd.DataFrame({cls_name: [data]})


def _apply_per_combo_col_selection(raw: Any, columns: list, cls_name: str) -> Any:
    """Apply column selection to raw data, returning array (1 col) or DataFrame (multi-col)."""
    import pandas as pd
    df = _to_dataframe(raw, cls_name)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"Columns {missing} not found in {cls_name}. Available: {list(df.columns)}")
    if len(columns) == 1:
        return df[columns[0]].values
    return df[columns]


# ---------------------------------------------------------------------------
# Saving results
# ---------------------------------------------------------------------------

def _save_results(
    result_tbl: "pd.DataFrame",
    outputs: list[Any],
    output_names: list[str],
    config_keys: dict,
    db: Any | None,
    rid_to_bp: "dict | None" = None,
    rid_keys: "list | None" = None,
) -> None:
    """Save results from the result table to output variable types."""
    import pandas as pd

    db_kwargs = {"db": db} if db is not None else {}

    # Get schema keys for dynamic discriminator detection
    schema_keys_set: set = set()
    if db is not None and hasattr(db, 'dataset_schema_keys'):
        schema_keys_set = set(db.dataset_schema_keys)
    else:
        try:
            schema_keys_set = set(_scifor.get_schema() or [])
        except Exception:
            pass

    # Determine which columns are metadata (not output names)
    meta_cols = [c for c in result_tbl.columns if c not in output_names]

    fn_name = config_keys.get("__fn", "")
    direct_constants = json.loads(config_keys.get("__constants", "{}") or "{}")

    for _, row in result_tbl.iterrows():
        # 1. Collect upstream branch_params via __rid_* columns → rid_to_bp lookup
        merged_bp: dict = {}
        if rid_to_bp and rid_keys:
            for rid_col in rid_keys:
                if rid_col not in row.index:
                    continue
                rid = row[rid_col]
                if rid and rid in rid_to_bp:
                    for k, v in rid_to_bp[rid].items():
                        if k in merged_bp and merged_bp[k] != v:
                            warnings.warn(
                                f"branch_params key '{k}' overwritten: "
                                f"{merged_bp[k]!r} → {v!r}. "
                                f"Use version= for precise selection.",
                                UserWarning, stacklevel=4,
                            )
                        merged_bp[k] = v

        # 2. Add constants namespaced by function name (for branch_params tracking)
        for k, v in direct_constants.items():
            merged_bp[f"{fn_name}.{k}"] = v

        # 3. Add dynamic discriminators (non-schema, non-__ meta columns with scalar values)
        _scalar_types = (bool, int, float, str)
        for col in meta_cols:
            if col.startswith("__"):
                continue
            if col in schema_keys_set:
                continue
            val = row[col] if col in row.index else None
            if val is None:
                continue
            if isinstance(val, float) and pd.isna(val):
                continue
            if not isinstance(val, _scalar_types):
                continue  # Skip numpy arrays and other complex types
            if col in merged_bp and merged_bp[col] != val:
                warnings.warn(
                    f"branch_params key '{col}' overwritten: "
                    f"{merged_bp[col]!r} → {val!r}. "
                    f"Use version= for precise selection.",
                    UserWarning, stacklevel=4,
                )
            merged_bp[col] = val

        # Build save metadata: non-__ cols (schema keys etc.) + config_keys + __branch_params
        # Exclude __rid_* and other internal __ columns from version keys.
        save_metadata = {
            col: row[col] for col in meta_cols if not col.startswith("__")
        }
        save_metadata.update(config_keys)

        # Unpack constants as direct keys so downstream consumers (e.g. scihist's
        # _save_with_lineage) see them in the metadata dict.  They are also stored
        # as __constants (JSON) in config_keys, so _stringify_meta can strip them
        # when loading back — preventing them from polluting input DataFrames.
        for k, v in direct_constants.items():
            if k not in save_metadata:
                save_metadata[k] = v

        save_metadata["__branch_params"] = json.dumps(merged_bp)

        # Add upstream record_ids to version_keys so that records from different
        # upstream variants get distinct record_ids even when content is identical.
        if rid_keys:
            upstream = {}
            for rid_col in rid_keys:
                if rid_col in row.index:
                    rid_val = row[rid_col]
                    if rid_val is not None and not (isinstance(rid_val, float) and pd.isna(rid_val)):
                        upstream[rid_col] = rid_val
            if upstream:
                save_metadata["__upstream"] = json.dumps(upstream, sort_keys=True)

        for output_obj, output_name in zip(outputs, output_names):
            if output_name not in row.index:
                # Flatten/distribute mode: fn returned a DataFrame whose columns are
                # spread directly in result_tbl (scifor all_dataframes flatten mode).
                # Build a 1-row DataFrame from non-schema, non-__ data columns.
                data_cols = [c for c in meta_cols
                             if not c.startswith("__") and c not in schema_keys_set]
                if not data_cols:
                    continue
                output_value = pd.DataFrame({c: [row[c]] for c in data_cols})
                save_meta_for_output = {k: v for k, v in save_metadata.items()
                                        if k not in set(data_cols)}
                try:
                    save_t0 = time.perf_counter()
                    rid = output_obj.save(output_value, **db_kwargs, **save_meta_for_output)
                    save_elapsed = time.perf_counter() - save_t0
                    meta_str = ", ".join(f"{k}={v}" for k, v in save_meta_for_output.items()
                                         if not k.startswith("__"))
                    data_desc = _describe_save_data(output_value)
                    rid_short = rid[:12] if isinstance(rid, str) else str(rid)
                    msg = f"[save] {meta_str}: {_output_name(output_obj)} -> record_id={rid_short} ({data_desc}) in {save_elapsed:.3f}s"
                    print(msg)
                    Log.info(msg)
                except Exception as e:
                    meta_str = ", ".join(f"{k}={v}" for k, v in save_meta_for_output.items()
                                         if not k.startswith("__"))
                    msg = f"[error] {meta_str}: failed to save {_output_name(output_obj)}: {e}"
                    print(msg)
                    Log.error(msg)
                continue
            output_value = row[output_name]
            try:
                save_t0 = time.perf_counter()
                rid = output_obj.save(output_value, **db_kwargs, **save_metadata)
                save_elapsed = time.perf_counter() - save_t0
                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                data_desc = _describe_save_data(output_value)
                rid_short = rid[:12] if isinstance(rid, str) else str(rid)
                msg = f"[save] {meta_str}: {_output_name(output_obj)} -> record_id={rid_short} ({data_desc}) in {save_elapsed:.3f}s"
                print(msg)
                Log.info(msg)
            except Exception as e:
                meta_str = ", ".join(f"{k}={v}" for k, v in save_metadata.items()
                                     if not k.startswith("__"))
                msg = f"[error] {meta_str}: failed to save {_output_name(output_obj)}: {e}"
                print(msg)
                Log.error(msg)


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


def _get_schema_keys(db: Any | None) -> set:
    """Return the set of dataset_schema_keys from db or the global database."""
    if db is not None and hasattr(db, 'dataset_schema_keys'):
        return set(db.dataset_schema_keys)
    try:
        from .database import get_database
        _db = get_database()
        if hasattr(_db, 'dataset_schema_keys'):
            return set(_db.dataset_schema_keys)
    except Exception:
        pass
    return set()


def _has_pathinput(inputs: dict) -> bool:
    """Check if any input is a PathInput, directly or wrapped in Fixed."""
    for v in inputs.values():
        if isinstance(v, PathInput):
            return True
        if isinstance(v, Fixed) and isinstance(v.var_type, PathInput):
            return True
    return False


def _find_pathinput(inputs: dict) -> PathInput | None:
    """Find the first PathInput in inputs, unwrapping Fixed if needed."""
    for v in inputs.values():
        if isinstance(v, PathInput):
            return v
        if isinstance(v, Fixed) and isinstance(v.var_type, PathInput):
            return v.var_type
    return None


def _describe_save_data(val) -> str:
    """Compact description of data being saved."""
    import pandas as pd
    import numpy as np
    if isinstance(val, pd.DataFrame):
        return f"DataFrame {val.shape[0]}x{val.shape[1]}"
    if isinstance(val, np.ndarray):
        return f"ndarray shape={val.shape}"
    if isinstance(val, dict):
        return f"dict, {len(val)} keys"
    if isinstance(val, (list, tuple)):
        return f"{type(val).__name__} len={len(val)}"
    return type(val).__name__


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


def _persist_expected_combos(
    db, fn_name: str, call_id: str, full_combos: list[dict]
) -> None:
    """Persist the full expected combo set for a for_each call into _for_each_expected.

    Called during for_each BEFORE skip_computed filtering, so we capture ALL
    combos (including ones that will be skipped).  This lets check_node_state
    know how many combos are expected for PathInput-only functions where no
    DB-variable inputs exist to infer the expected set.

    Rows are scoped by (function_name, call_id) so that multiple for_each()
    call sites that reuse the same function don't clobber each other.  The
    DELETE only removes rows for *this* call site; rows for other call sites
    of the same function are left intact.
    """
    if not full_combos:
        return

    try:
        if db is None:
            from .database import get_database
            db = get_database()
    except Exception:
        Log.debug("_persist_expected_combos: no database available, skipping")
        return

    try:
        sk_set = set(db.dataset_schema_keys)
        rows_to_insert = []

        for combo in full_combos:
            # Extract only schema keys from the combo (ignore __rid_*, etc.)
            schema_keys = {k: v for k, v in combo.items() if k in sk_set}
            if not schema_keys:
                continue

            level = db._infer_schema_level(schema_keys)
            if level is None:
                continue

            schema_id = db._duck._get_or_create_schema_id(level, schema_keys)
            rows_to_insert.append((fn_name, call_id, schema_id, "{}"))

        if not rows_to_insert:
            return

        # Deduplicate (multiple combos can map to the same schema_id)
        rows_to_insert = list(set(rows_to_insert))

        # Replace old entries for THIS call site only.  Other call sites of
        # the same function (different call_id) are untouched.
        deleted = db._duck._fetchall(
            "SELECT COUNT(*) FROM _for_each_expected "
            "WHERE function_name = ? AND call_id = ?",
            [fn_name, call_id],
        )
        prev_count = deleted[0][0] if deleted else 0
        db._duck._execute(
            "DELETE FROM _for_each_expected WHERE function_name = ? AND call_id = ?",
            [fn_name, call_id],
        )
        for fn, cid, sid, bp in rows_to_insert:
            db._duck._execute(
                "INSERT INTO _for_each_expected "
                "(function_name, call_id, schema_id, branch_params) "
                "VALUES (?, ?, ?, ?)",
                [fn, cid, sid, bp],
            )

        Log.debug(
            f"_persist_expected_combos({fn_name}, call_id={call_id}): "
            f"replaced {prev_count} -> wrote {len(rows_to_insert)} expected combos"
        )
    except Exception as exc:
        Log.debug(
            f"_persist_expected_combos({fn_name}, call_id={call_id}): failed — {exc}"
        )
