"""for_each: loading inputs and converting them for the function.

Moved verbatim out of foreach.py (grug follow-up, docs/claude/grugbrain-principles.md).
"""

import logging
import json
import scifor as _scifor
import time
from pathlib import Path as _Path
from scidb.model.across_variants import AcrossVariants
from scidb.model.exceptions import DatabaseNotConfiguredError
from scidb.model.input_spec import is_loadable
from scidb.model.input_spec import spec_name
from scidb.model.input_spec import type_name
from scidb.model.input_spec import variable_type
from scidb.model.log import Log
from scidb.model.roles import endpoint_kind as _roles_endpoint_kind
from scidb.model.variant import Variant
from scidb.run.per_combo import PerComboLoader
from scidb.run.per_combo import PerComboLoaderMerge
from scidb.store.database import database_or_none
from scidb.store.schema_values import schema_str
from scifor import ColName
from scifor import ColumnSelection
from scifor import Fixed
from scifor import Merge
from scifor.pathinput import PathInput
from typing import Any

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from scidb.run.foreach_common import (
    _DryRunMerge,
    _PreresolvedFilter,
    _get_schema_keys,
    _merge_constituent_variable_filter,
)
from scidb.run.foreach_stamping import (
    _load_var_type_as_spread,
)

# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.model.filters import ColumnFilter
from scidb.model.filters import CompoundFilter
from scidb.model.filters import InFilter
from scidb.model.filters import NotFilter
from scidb.model.filters import VariableFilter
from scidb.model.filters import _expand_coarse_to_fine_schema_ids
from scidb.model.filters import _get_all_schema_ids_for_variable
from scidb.model.filters import _validate_filter_coverage
from scidb.store.database import get_database
from scifor import ColName as SciforColName
from scifor import PathOutput
# The same logger as before the move, so log output is unchanged.
logger = logging.getLogger("scidb.run.foreach")


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
            if var_spec.is_deferred:
                # No-arg ColName() resolves to the current for_columns column;
                # hand the scifor engine a deferred marker so it substitutes the
                # column name per-iteration (validated there against the
                # presence of an iterate input).

                result[param_name] = SciforColName()
                Log.debug(f"input '{param_name}': deferred ColName() -> scifor marker")
            else:
                result[param_name] = _resolve_colname_from_db(var_spec, db)
        elif is_loadable(var_spec):
            t0 = time.perf_counter()
            loaded = _load_input(var_spec, db, where, param_name=param_name)
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


def _resolve_mapping_inputs(
    inputs: dict[str, Any],
    loaded_inputs: dict[str, Any],
    db: Any | None,
) -> dict[str, list[str]]:
    """``{param_name: [data column names]}`` for dict-valued variable inputs.

    A variable whose records are dicts is stored one DuckDB column per key
    (``multi_column``) and loaded in the spread layout, so a single record is
    indistinguishable from a one-row table by the time scifor extracts it per
    combo. scifor cannot infer the difference — "one row, N data columns" is
    also exactly what a genuine DataFrame-mode variable looks like — so scidb,
    which owns the storage mode, states it and scifor honors it.

    See ``DatabaseManager.mapping_data_columns`` for why the columns are
    spread in the first place and cannot simply be collapsed at load time.

    Only bare variable types and ``Fixed``-wrapped ones are marked.
    ``ColumnSelection`` is deliberately excluded (the caller asked for
    specific columns and gets them), as is ``Merge`` (its frame interleaves
    columns from several types, so no single dict describes a row).
    """
    import pandas as pd

    resolved_db = db
    if resolved_db is None:
        resolved_db = database_or_none()
        if resolved_db is None:
            return {}
    if resolved_db is None or not hasattr(resolved_db, "mapping_data_columns"):
        return {}

    result: dict[str, list[str]] = {}
    for param_name, var_spec in inputs.items():
        # Only a bulk-loaded frame can be reassembled here; a PerComboLoader
        # returns whatever load() gives it, which is already the dict.
        loaded = loaded_inputs.get(param_name)
        if isinstance(loaded, Fixed):
            loaded = loaded.data  # Fixed re-wraps the loaded frame
        if not isinstance(loaded, pd.DataFrame):
            continue
        candidate = var_spec
        if isinstance(candidate, Fixed):
            candidate = candidate.data
        if isinstance(candidate, (Merge, ColumnSelection, ColName)):
            continue
        if not hasattr(candidate, "load"):
            continue
        try:
            cols = resolved_db.mapping_data_columns(candidate)
        except Exception as exc:  # a missing/garbled dtype row must not fail the run
            Log.debug(
                f"input '{param_name}': mapping_data_columns raised "
                f"{type(exc).__name__}: {exc} — not marked as dict-valued"
            )
            continue
        if cols:
            result[param_name] = cols

    if result:
        Log.info(
            "dict-valued input(s) will be rebuilt per combo: "
            + ", ".join(
                f"'{name}' ({len(cols)} key(s))" for name, cols in sorted(result.items())
            )
        )
    return result


def _log_loaded_input(
    param_name: str, var_spec: Any, loaded: Any, elapsed: float
) -> None:
    """Log details about a loaded input."""
    import pandas as pd

    type_name = spec_name(var_spec)

    if isinstance(loaded, pd.DataFrame):
        Log.debug(
            f"input '{param_name}': loaded {type_name} -> "
            f"{len(loaded)} rows, {len(loaded.columns)} cols in {elapsed:.3f}s"
        )
    elif isinstance(loaded, (PerComboLoader, PerComboLoaderMerge)):
        Log.debug(
            f"input '{param_name}': {type_name} (per-combo loader, will load during iteration)"
        )
    else:
        Log.debug(f"input '{param_name}': loaded {type_name} in {elapsed:.3f}s")


def _convert_inputs_for_display(inputs: dict[str, Any]) -> dict[str, Any]:
    """Convert inputs for dry_run display without actually loading any data.

    scidb-specific types (Merge, Fixed, ColumnSelection, classes) are converted
    to scifor-compatible display forms or left as constants.
    """
    import pandas as pd

    result = {}
    for param_name, var_spec in inputs.items():
        # Variant is a load-time filter; for display, unwrap to its inner spec.
        if isinstance(var_spec, Variant):
            var_spec = var_spec.var_type
        if isinstance(var_spec, Merge):
            # Use _DryRunMerge so scifor prints "merge {param_name}:" and class names
            result[param_name] = _DryRunMerge(var_spec)
        elif isinstance(var_spec, ColumnSelection):
            dummy = pd.DataFrame(columns=var_spec.columns or [])
            result[param_name] = _scifor.ColumnSelection(
                dummy, var_spec.column_spec, iterate=var_spec.iterate
            )
        elif isinstance(var_spec, Fixed) and isinstance(
            var_spec.data, ColumnSelection
        ):
            inner = var_spec.data
            dummy = pd.DataFrame(columns=inner.columns or [])
            dummy_cs = _scifor.ColumnSelection(
                dummy, inner.column_spec, iterate=inner.iterate
            )
            result[param_name] = _scifor.Fixed(dummy_cs, **var_spec.fixed_metadata)
        elif isinstance(var_spec, Fixed) and not isinstance(var_spec.data, Merge):
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

    var_type = colname.data

    # Get the database
    resolved_db = db
    if resolved_db is None:
        try:
            resolved_db = get_database()
        except DatabaseNotConfiguredError:
            raise ValueError(
                "ColName requires a database to resolve column names. "
                "Either pass db= to for_each or call configure_database() first."
            )

    var_name = (
        var_type.__name__ if isinstance(var_type, type) else type(var_type).__name__
    )
    schema_keys = list(resolved_db.dataset_schema_keys)

    # Query the _variables table for dtype metadata.
    # Goes through SciDuck.fetchone so execute+fetch stay under one lock.
    # `resolved_db` is a DatabaseManager (the dataset_schema_keys read above
    # already requires one); the SciDuck it wraps is ._duck — DatabaseManager
    # itself has no execute/fetchone. Errors are NOT swallowed here: this used
    # to be a bare `except Exception: row = None`, which silently turned an
    # AttributeError into "variable not saved" and made the fallback below
    # the only path ever taken.
    row = resolved_db._duck.fetchone(
        "SELECT dtype FROM _variables WHERE variable_name = ?",
        [var_name],
    )

    if row is None:
        # Variable genuinely not yet saved — try view_name for single-column mode
        logger.debug(
            "_resolve_colname_from_db: no _variables dtype row for '%s' "
            "(not yet saved) — falling back to view_name/type name",
            var_name,
        )
        if hasattr(var_type, "view_name"):
            return var_type.view_name()
        return var_name

    dtype_meta = json.loads(row[0])
    mode = dtype_meta.get("mode", "single_column")

    if mode == "single_column":
        # Single-column variables always have exactly one data column
        col_names = list(dtype_meta.get("columns", {}).keys())
        if col_names:
            return col_names[0]
        if hasattr(var_type, "view_name"):
            return var_type.view_name()
        return var_name

    if mode == "dataframe":
        # DataFrame variables: subtract schema keys from df_columns
        df_columns = dtype_meta.get(
            "df_columns", list(dtype_meta.get("columns", {}).keys())
        )
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
    if hasattr(var_type, "view_name"):
        return var_type.view_name()
    return var_name


def _drop_unpopulated_schema_columns(
    df: "pd.DataFrame",
    schema_keys: "set | list",
    *,
    context: str,
) -> "pd.DataFrame":
    """Drop schema-key columns a loaded input never populates.

    ``load_all_as_df(layout="spread")`` emits one column per *dataset* schema
    key, so a variable stored at a coarser level -- Demographics at ``subject``
    in a ``subject/session/speed/trial`` schema -- comes back carrying the finer
    keys as all-NULL columns.  An all-NULL schema column does not mean "these
    rows have no session"; it means the variable has no session axis at all, so
    its rows must BROADCAST across that dimension.

    Left in, both per-combo filters read it the other way, and differently --
    which is why this lives here, once, rather than as the same rule written
    twice in two scifor implementations:

    * Python -- ``scifor._filter_df_for_combo`` evaluates ``None == "BL"`` for
      every row, so every combo filters to zero rows and raises ``scifor:NoData``.
    * MATLAB -- the column crosses the bridge as a cell array of ``0x0 double``
      and ``filter_table_for_combo``'s ``string(col_data)`` raises
      ``MATLAB:string:MustBeConvertibleCellArray``, failing EVERY iteration with
      "failed to filter <param>".

    Dropping them lets the filter skip the dimension, which is the broadcast
    behaviour the Merge path has always had -- this is the generalisation of the
    drop that used to live inline in ``_load_input``'s Merge branch, to every
    input kind.  See docs/claude/coarse-level-inputs.md.

    Only columns that are ENTIRELY null are dropped.  A variable with records
    saved at mixed granularity keeps its partially-populated key and still
    filters on it: a row that genuinely has no session is a different thing from
    a variable with no session axis, and the two must not be conflated.
    """
    import pandas as pd

    if not isinstance(df, pd.DataFrame) or df.empty or not schema_keys:
        return df
    unpopulated = [c for c in df.columns if c in schema_keys and df[c].isna().all()]
    if not unpopulated:
        return df
    kept = sorted(c for c in df.columns if c in schema_keys and c not in unpopulated)
    Log.info(
        f"[coarse-input] {context}: dropped unpopulated schema column(s) "
        f"{sorted(unpopulated)} — stored at a coarser level, so its rows "
        f"broadcast across {'/'.join(sorted(unpopulated))} and filter only on "
        f"{kept if kept else '(nothing — constant table)'}"
    )
    return df.drop(columns=unpopulated)


def _drop_unpopulated_schema_columns_in_place(
    loaded: Any,
    schema_keys: "set | list",
    *,
    context: str,
) -> Any:
    """``_drop_unpopulated_schema_columns`` for a frame or a wrapper around one.

    Accepts a bare DataFrame, or anything with a ``.data`` DataFrame (scifor's
    ``Fixed`` / ``ColumnSelection``), and returns the same kind of thing.
    """
    import pandas as pd

    if isinstance(loaded, pd.DataFrame):
        return _drop_unpopulated_schema_columns(loaded, schema_keys, context=context)
    inner = getattr(loaded, "data", None)
    if isinstance(inner, pd.DataFrame):
        loaded.data = _drop_unpopulated_schema_columns(
            inner, schema_keys, context=context
        )
    return loaded


def _load_input(
    var_spec: Any,
    db: Any | None,
    where: Any | None,
    branch_params_filter: dict | None = None,
    param_name: str | None = None,
    use_default: bool = True,
) -> Any:
    """Load a single input and return a scifor-compatible wrapper or sentinel.

    ``use_default``: an input that names no variant gets the variable's PINNED
    default (``scidb.store.variant_pins``), applied at the leaf. ``AcrossVariants``
    turns it off for its subtree, because pooling every variant is the whole
    point of that wrapper.

    ``branch_params_filter`` is an orthogonal, load-time filter threaded through
    the recursion exactly like ``where``.  A ``Variant`` wrapper *injects* it into
    its subtree; the other wrappers pass it through; the leaf load applies it via
    ``load_all_as_df(branch_params_filter=…)``.

    ``param_name`` is threaded for LOGGING only — it never changes what loads.
    A coarse-input drop reported against the variable type alone is hard to act
    on, because the failure it prevents (MATLAB's "failed to filter <param>")
    names the parameter, not the type.
    """
    import pandas as pd

    def _ctx(var_label: str) -> str:
        """Log context for this input: parameter name plus what it is bound to."""
        return f"'{param_name}' ({var_label})" if param_name else var_label

    # Already a DataFrame — pass through
    if isinstance(var_spec, pd.DataFrame):
        return var_spec

    # AcrossVariants: a pooling MARKER, not a load-time filter — loading is
    # identical to the bare input (all variants load). Its effect happens in
    # foreach Step 12 (skip the aggregation auto-split, attach branch_param
    # columns), keyed off the original ``inputs`` spec, so here we just unwrap.
    if isinstance(var_spec, AcrossVariants):
        return _load_input(
            var_spec.var_type,
            db,
            where,
            branch_params_filter=branch_params_filter,
            param_name=param_name,
            use_default=False,  # pool EVERY variant, pinned default or not
        )

    # Variant: inject/merge its branch_params into the inherited filter (error on
    # conflicting values) and recurse into the inner spec.  Composition with the
    # other wrappers is order-agnostic because the filter is threaded, not
    # wrapper-aware.
    if isinstance(var_spec, Variant):
        merged = dict(branch_params_filter or {})
        for k, v in var_spec.branch_params.items():
            if k in merged and merged[k] != v:
                raise ValueError(
                    f"Conflicting branch_param '{k}' for Variant input: "
                    f"{merged[k]!r} vs {v!r}."
                )
            merged[k] = v
        Log.debug(
            f"[Variant] {var_spec.__name__}: injecting branch_params_filter={merged}"
        )
        return _load_input(
            var_spec.var_type,
            db,
            where,
            branch_params_filter=merged,
            param_name=param_name,
            use_default=use_default,
        )

    # Merge: check if any constituent needs per-combo loading
    if isinstance(var_spec, Merge):
        _merge_db = db
        if _merge_db is None:
            _merge_db = database_or_none()
        if _merge_needs_per_combo(var_spec) or _merge_db is None:
            # Use per-combo when a constituent lacks bulk-load support, or when
            # there is genuinely no database — without one, the bulk loader
            # cannot filter each constituent by schema keys.
            return PerComboLoaderMerge(var_spec)
        # All constituents can be pre-loaded.
        # All filter types (VariableFilter, ColumnFilter, InFilter, SchemaKey, Raw)
        # are now propagated to Merge constituents.  Coverage is validated once
        # against the actual Merge inner-join result rather than per-constituent
        # to avoid false-positive errors when a constituent has "extra" rows that
        # the Merge would eliminate anyway.
        loaded_tables = []
        _SCIDB_META = {"__record_id", "__branch_params", "version"}
        _schema_keys = set(
            getattr(_merge_db, "dataset_schema_keys", [])
            if _merge_db is not None
            else []
        )

        if where is not None:
            merge_effective_ids = _compute_merge_effective_ids(_merge_db, var_spec)
            _check_merge_filter_coverage(_merge_db, where, merge_effective_ids)
            # Variable-level portion of the filter, shared by every constituent.
            # Carrying it lets the constituent loader semantically select the single
            # computed variant the filter describes (by its consumed input schema_id
            # set) instead of returning every variant that shares the same schema keys.
            merge_var_filter = _merge_constituent_variable_filter(where)
            Log.debug(
                f"[Merge] {var_spec.__name__}: variant filter={merge_var_filter!r}"
            )

        for sub_spec in var_spec.tables:
            if where is not None:
                cls = _get_loadable_class_from_spec(sub_spec)
                matching_ids = where.resolve(
                    _merge_db,
                    cls,
                    cls.table_name(),
                    validate_coverage=False,  # coverage validated once above
                )
                constituent_where = _PreresolvedFilter(
                    matching_ids, variable_filter=merge_var_filter
                )
            else:
                constituent_where = None
            # Per-constituent Variant injects its own branch_params_filter inside
            # this recursion; the inherited filter (normally None — Variant(Merge)
            # is rejected at construction) is threaded for safety.
            loaded = _load_input(
                sub_spec,
                db,
                where=constituent_where,
                branch_params_filter=branch_params_filter,
                param_name=param_name,
                use_default=use_default,
            )
            # Strip scidb metadata columns that would conflict when merged column-wise.
            # __record_id/__branch_params/version appear in every constituent but carry
            # no per-row meaning after merge; scifor's _prepare_merge doesn't track them.
            if isinstance(loaded, pd.DataFrame):
                drop_cols = [
                    c for c in loaded.columns if c in _SCIDB_META or c.startswith("__")
                ]
                if drop_cols:
                    loaded = loaded.drop(columns=drop_cols)
            elif hasattr(loaded, "data") and isinstance(loaded.data, pd.DataFrame):
                # _scifor.Fixed wrapping a DataFrame
                drop_cols = [
                    c
                    for c in loaded.data.columns
                    if c in _SCIDB_META or c.startswith("__")
                ]
                if drop_cols:
                    loaded.data = loaded.data.drop(columns=drop_cols)
            # Unpopulated schema columns: the recursion above already dropped them
            # for every constituent that is a loaded variable type. Repeat it here
            # so a constituent the user handed in as a raw DataFrame gets the same
            # broadcast semantics — this branch owned that rule alone until it was
            # generalised (docs/claude/coarse-level-inputs.md).
            loaded = _drop_unpopulated_schema_columns_in_place(
                loaded,
                _schema_keys,
                context=_ctx(
                    "Merge constituent "
                    f"'{getattr(sub_spec, '__name__', type(sub_spec).__name__)}'"
                ),
            )
            loaded_tables.append(loaded)
        return _scifor.Merge(*loaded_tables)

    # Fixed: check for Fixed(Merge(...)) error, then load inner
    if isinstance(var_spec, Fixed):
        if isinstance(var_spec.data, Merge):
            raise TypeError(
                "Fixed cannot wrap a Merge. Use Fixed on individual "
                "constituents inside the Merge instead: "
                "Merge(Fixed(df1, ...), df2)"
            )
        inner_loaded = _load_input(
            var_spec.data,
            db,
            where,
            branch_params_filter=branch_params_filter,
            param_name=param_name,
            use_default=use_default,
        )
        if isinstance(inner_loaded, PerComboLoader):
            # Inner needs per-combo loading; wrap the whole Fixed spec
            return PerComboLoader(var_spec)
        # Keep __record_id for variant tracking (needed for BaseVariable reconstruction)
        # but strip __branch_params which is redundant for Fixed inputs.
        import pandas as pd

        if isinstance(inner_loaded, pd.DataFrame):
            if "__branch_params" in inner_loaded.columns:
                inner_loaded = inner_loaded.drop(columns=["__branch_params"])
        # Stringify fixed_metadata schema keys to match the stringified
        # DataFrame columns produced by load_all_as_df (spread layout).
        fixed_meta = dict(var_spec.fixed_metadata)
        _sk = _get_schema_keys(db)
        if _sk:
        
            fixed_meta = {
                k: schema_str(v) if k in _sk else v for k, v in fixed_meta.items()
            }
        return _scifor.Fixed(inner_loaded, **fixed_meta)

    # ColumnSelection: already a DataFrame -> pass through unchanged (a bare
    # scifor.ColumnSelection(df, ...) is now constructible under scidb too,
    # since ColumnSelection is the same class in both packages); load inner
    # var_type if possible; else per-combo.
    if isinstance(var_spec, ColumnSelection):
        if isinstance(var_spec.data, pd.DataFrame):
            return var_spec
        if hasattr(var_spec.data, "load"):
            loaded_df = _load_var_type_as_spread(
                var_spec.data,
                db,
                where,
                branch_params_filter=branch_params_filter,
                use_default=use_default,
            )
            loaded_df = _drop_unpopulated_schema_columns(
                loaded_df,
                _get_schema_keys(db),
                context=_ctx(
                    "ColumnSelection on "
                    f"{getattr(var_spec.data, '__name__', type(var_spec.data).__name__)}"
                ),
            )
            return _scifor.ColumnSelection(
                loaded_df, var_spec.column_spec, iterate=var_spec.iterate
            )
        return PerComboLoader(var_spec)

    # PathInput: needs per-combo resolution via load(**combo); wrap in PerComboLoader
    if isinstance(var_spec, PathInput):
        return PerComboLoader(var_spec)

    # Variable type (class with .load()): bulk load or per-combo.
    # Use bulk loading only when the DB provides load_all_as_df; without it
    # the slow-path cannot build a properly-keyed spread DataFrame (it only
    # calls load() once with version/db kwargs, not per combo metadata), so
    # fall back to per-combo loading — same logic as Merge above.
    if isinstance(var_spec, type) or hasattr(var_spec, "load"):
        if hasattr(var_spec, "load"):
            _check_db = db
            if _check_db is None:
                _check_db = database_or_none()
            if _check_db is not None and hasattr(_check_db, "load_all_as_df"):
                loaded_df = _load_var_type_as_spread(
                    var_spec,
                    db,
                    where,
                    branch_params_filter=branch_params_filter,
                    use_default=use_default,
                )
                return _drop_unpopulated_schema_columns(
                    loaded_df,
                    _get_schema_keys(_check_db),
                    context=_ctx(
                        getattr(var_spec, "__name__", type(var_spec).__name__)
                    ),
                )
            return PerComboLoader(var_spec)
        return PerComboLoader(var_spec)

    # Unknown — return as-is
    return var_spec


def _compute_fixed_input_rids(inputs: dict, db) -> dict:
    """Compute record_ids for Fixed inputs for lineage tracking.

    Fixed inputs have __record_id stripped during variant expansion (line 826-829),
    but lineage tracking needs to know which specific record was used for staleness
    checking. This function computes those record_ids.

    Args:
        inputs: The inputs dict passed to for_each (may contain Fixed wrappers)
        db: Database instance

    Returns:
        ``{param_name: record_id}`` for each Fixed input the database can
        resolve. Keyed by the PARAM — the last ``__rid_`` spelling on this
        path went with Stage 2c.
    """
    fixed_rids = {}

    # If db is None, we can't look up record_ids
    if db is None:
        return fixed_rids

    for name, value in inputs.items():
        # Detect Fixed wrapper
        if not hasattr(value, "fixed_metadata"):
            continue

        # Unwrap to get inner variable type
        inner = value.data if hasattr(value, "data") else value

        # Unwrap ColumnSelection if present
        if hasattr(inner, "data"):
            inner = inner.data

        # Must be a variable type (class)
        if not isinstance(inner, type):
            continue

        # Look up record_id for this Fixed input
        try:
            rid = db.find_record_id(inner, value.fixed_metadata)
            if rid:
                fixed_rids[name] = rid
        except Exception:
            # If lookup fails, skip this Fixed input
            pass

    return fixed_rids


def _get_schema_level_idx(db, schema_ids: set) -> int:
    """Return the deepest populated schema key index for the given schema_ids, or -1."""
    schema_keys = db.dataset_schema_keys
    if not schema_ids:
        return -1
    placeholders = ", ".join(["?"] * len(schema_ids))
    rows = db._duck.fetchdf(
        f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})",
        list(schema_ids),
    )
    if len(rows) == 0:
        return -1
    level_idx = -1
    for i, key in enumerate(schema_keys):
        if key in rows.columns and rows[key].notna().any():
            level_idx = i
    return level_idx


def _compute_merge_effective_ids(db, merge_spec: "Merge") -> set:
    """Compute the schema_ids that the Merge inner join will produce.

    For each constituent, expand coarser schema_ids to the finest level, then
    intersect all fine-level sets.  The result is the set of schema_ids that
    would survive the Merge inner join — used to validate filter coverage once
    against the right target rather than per-constituent.
    """

    # Collect (table_name, schema_ids) for each constituent
    constituent_data = []
    for sub_spec in merge_spec.tables:
        cls = _get_loadable_class_from_spec(sub_spec)
        if cls is None:
            continue
        table_name = cls.table_name()
        schema_ids = _get_all_schema_ids_for_variable(db, table_name)
        constituent_data.append((table_name, schema_ids))

    if not constituent_data:
        return set()

    # Determine level index for each constituent
    levels = [
        (_get_schema_level_idx(db, ids), table_name, ids)
        for table_name, ids in constituent_data
    ]

    max_level = max(lv for lv, _, _ in levels)
    if max_level < 0:
        return set()

    # Pick the first fine-level constituent's table as expansion target
    fine_table_name = next(tn for lv, tn, _ in levels if lv == max_level)

    # Compute each constituent's contribution at the finest level
    effective_sets = []
    for lv, table_name, schema_ids in levels:
        if lv == max_level:
            effective_sets.append(schema_ids)
        else:
            expanded = _expand_coarse_to_fine_schema_ids(
                db, schema_ids, fine_table_name
            )
            effective_sets.append(expanded)

    # Intersection = Merge inner join result
    result = effective_sets[0]
    for s in effective_sets[1:]:
        result = result & s
    return result


def _check_merge_filter_coverage(db, where, merge_effective_ids: set) -> None:
    """Validate that the filter covers all schema_ids the Merge inner join will produce.

    Recurses through the filter tree.  VariableFilter/ColumnFilter/InFilter are
    validated against merge_effective_ids via _validate_filter_coverage with the
    override parameter.  SchemaKey and Raw filters have no coverage concept and
    are skipped.

    Raises:
        ValueError: If the filter variable is missing data for a schema_id that
            genuinely survives the Merge inner join.
    """

    if where is None or not merge_effective_ids:
        return

    if isinstance(where, (VariableFilter, ColumnFilter, InFilter)):
        filter_table_name = where.variable_class.table_name()
        filter_ids = _get_all_schema_ids_for_variable(db, filter_table_name)
        filter_level_idx = _get_schema_level_idx(db, filter_ids)
        target_level_idx = _get_schema_level_idx(db, merge_effective_ids)

        _validate_filter_coverage(
            db,
            where.variable_class,
            None,
            filter_table_name,
            None,
            filter_level_idx,
            target_level_idx,
            target_schema_ids_override=merge_effective_ids,
        )

    elif isinstance(where, CompoundFilter):
        _check_merge_filter_coverage(db, where.left, merge_effective_ids)
        _check_merge_filter_coverage(db, where.right, merge_effective_ids)

    elif isinstance(where, NotFilter):
        _check_merge_filter_coverage(db, where.inner, merge_effective_ids)

    # SchemaKeyInFilter, SchemaKeyCompareFilter, RawFilter: no coverage concept; no-op


def _merge_needs_per_combo(merge_spec: "Merge") -> bool:
    """Return True if any Merge constituent lacks load."""
    for spec in merge_spec.tables:
        cls = _get_loadable_class_from_spec(spec)
        if cls is not None and not hasattr(cls, "load"):
            return True
    return False


def _get_loadable_class_from_spec(spec: Any) -> Any:
    """The innermost loadable class of a spec — ``input_spec.variable_type``,
    the ONE unwrap (this name is kept for its call sites)."""
    return variable_type(spec)


def _make_plot_wrapper(fn: Any, path_param: str) -> Any:
    """Wrap a plotting (``plot_``) function so it saves its Figure and returns a path.

    The wrapped function receives the same kwargs as ``fn`` — including the
    PathOutput-resolved destination under ``path_param`` (scifor resolves
    PathOutput per combo before calling). It calls ``fn``; if the return value
    is a matplotlib Figure, it is saved to that path and closed (to bound memory
    across many combos), and the path string is returned. If ``fn`` already
    returns a str/Path (it saved the figure itself), that is passed through.

    ``functools.wraps`` preserves the original name, signature, and
    ``__wrapped__`` so combo-metadata injection and function hashing
    (skip_computed / lineage) all resolve to the user's function.
    """
    import functools

    @functools.wraps(fn)
    def wrapped(**kwargs):
        result = fn(**kwargs)
        path = kwargs.get(path_param)
        # Already a path-like return: the function saved it itself.
        if isinstance(result, (str, _Path)):
            return str(result)
        # Treat as a matplotlib Figure (duck-typed: has savefig).
        if hasattr(result, "savefig"):
            if path is None:
                raise ValueError(
                    f"Plotting function '{getattr(fn, '__name__', 'plot_fn')}' "
                    f"returned a Figure but no path was resolved for input "
                    f"'{path_param}'."
                )
            result.savefig(str(path))
            try:
                import matplotlib.pyplot as _plt

                _plt.close(result)
            except Exception:
                pass  # closing is best-effort memory hygiene
            return str(path)
        raise TypeError(
            f"Plotting function '{getattr(fn, '__name__', 'plot_fn')}' must return "
            f"a matplotlib Figure or a path; got {type(result).__name__}."
        )

    return wrapped


def _endpoint_policy(fn_name: str, inputs: dict, save: bool, as_table):
    """Endpoint (plot_/stat_) policy shared by scidb.for_each AND the MATLAB
    bridge's for_each_prepare — one source of truth for detection, the
    plot-requires-PathOutput contract, the stat_ as_table default, and the
    draft notice. Callers do their own language-specific fn wrapping.

    Returns ``(endpoint_kind, path_param, as_table)`` where
    endpoint_kind is "plot" | "stat" | None and path_param names the (first)
    PathOutput input, if any.
    """

    endpoint_kind = _roles_endpoint_kind(fn_name)
    path_param = None
    if endpoint_kind is not None:
        path_param = next(
            (name for name, v in inputs.items() if isinstance(v, PathOutput)), None
        )
    if endpoint_kind == "plot":
        if path_param is None:
            raise ValueError(
                f"Plotting function '{fn_name}' (plot_ prefix) requires a "
                f"PathOutput input naming where to save the figure, e.g. "
                f"inputs={{..., 'filename': PathOutput('plots/{{subject}}.png')}}."
            )
        Log.info(
            f"'{fn_name}' detected as plotting function; "
            f"figure saved to PathOutput input '{path_param}'"
            + (", path stored as record" if save else " (draft: not recorded)")
        )
    elif endpoint_kind == "stat":
        # Statistics run on the long-format table: group/repeated-measures
        # columns (the schema keys) must reach the function — exactly
        # csv-stats' input contract. Without as_table, a single-data-column
        # aggregate is delivered as a bare ndarray with the schema columns
        # stripped. Default it ON for stat_ functions; an explicit user
        # as_table (including False) is respected.
        if as_table is None:
            as_table = True
            Log.debug(
                "as_table defaulted to True for stat_ function "
                "(schema columns delivered for grouping)"
            )
        Log.info(
            f"'{fn_name}' detected as statistics function; "
            + (
                "result JSON stored as record"
                if save
                else "draft: result printed, not recorded"
            )
            + (
                f"; PathOutput input '{path_param}'"
                + (" resolved normally" if save else " resolved to None")
                if path_param
                else ""
            )
        )

    if endpoint_kind is not None and not save:
        # DRAFT (save=False, e.g. Pipeline.show): no records, no lineage, no
        # graph. The in-memory result table is still returned. Drafts leave no
        # record to promote; recording later means running again with saving on.
        Log.info(
            f"[draft] {fn_name}: save=False — "
            f"{'figures rendered' if endpoint_kind == 'plot' else 'results printed'} "
            f"but NOT recorded."
        )

    return endpoint_kind, path_param, as_table


def _jsonify_stat(obj: Any) -> Any:
    """Recursively convert a stat-result structure to JSON-native types.

    numpy scalars and arrays convert via ``tolist()`` (same recipe as
    csv-stats' ``convert_types``); dict keys are stringified. Anything still
    non-serializable is caught by ``json.dumps(default=str)`` at the caller.
    """
    if isinstance(obj, dict):
        return {str(k): _jsonify_stat(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonify_stat(v) for v in obj]
    tolist = getattr(obj, "tolist", None)
    if callable(tolist):
        try:
            return _jsonify_stat(tolist())
        except Exception:
            return str(obj)
    return obj


def _make_stat_wrapper(fn: Any, path_param: "str | None", save: bool) -> Any:
    """Wrap a statistics (``stat_``) function so its result dict becomes a
    canonical JSON string (the stored record data in record mode).

    Contract (see docs/claude/endpoints-viz-and-stats-design.md, D3/D5):

    - ``fn`` must return a **dict** (csv-stats' native return) or a ready
      JSON **string**; anything else raises TypeError.
    - The top-level ``"date"`` key is STRIPPED: csv-stats stamps a wall-clock
      timestamp inside every result, which would make identical reruns store
      different bytes. The database's own save timestamp is the time
      authority.
    - Draft (``save=False``): any PathOutput input is resolved to None
      before calling ``fn`` — handing it straight to csv-stats'
      ``filename=None`` disables the PDF side effect — and the result is
      pretty-printed for interactive exploration.
    - Record (``save=True``): the PathOutput path (if any) passes
      through for the fn's report writer, and is embedded as
      ``"report_path"`` in the stored JSON so the artifact is discoverable
      from the record.

    ``functools.wraps`` preserves the original name, signature, and
    ``__wrapped__`` so combo-metadata injection and function hashing
    (skip_computed / lineage) all resolve to the user's function.
    """
    import functools

    @functools.wraps(fn)
    def wrapped(**kwargs):
        if path_param is not None and not save:
            kwargs[path_param] = None
        result = fn(**kwargs)

        report_path = (
            str(kwargs[path_param])
            if save
            and path_param is not None
            and kwargs.get(path_param) is not None
            else None
        )
        payload = normalize_stat_payload(
            result, report_path, fn_name=getattr(fn, "__name__", "stat_fn")
        )
        if not save:
            pretty = json.dumps(json.loads(payload), indent=2, sort_keys=True)
            print(f"[stat draft] {getattr(fn, '__name__', 'stat_fn')}:\n{pretty}")
        return payload

    return wrapped


def normalize_stat_payload(
    result: Any, report_path: "str | None", fn_name: str = "stat_fn"
) -> str:
    """Canonicalize a stat_ result into the stored JSON payload string.

    Shared by the Python stat_ wrapper and the MATLAB bridge
    (``scimatlab.bridge.normalize_stat_result``) so both paths store
    byte-identical payloads for the same result — skip_computed's content
    identity and reproducibility depend on this living in ONE place (MATLAB's
    ``jsonencode`` differs from ``json.dumps`` in key order and float
    formatting, so MATLAB results are re-canonicalized here).

    Contract: ``result`` is a dict or a JSON string (anything else raises
    TypeError); numpy values are converted; the top-level wall-clock ``date``
    key is STRIPPED (the DB save timestamp is the time authority);
    ``report_path`` (record mode only) is embedded for artifact discovery.
    """
    if isinstance(result, str):
        try:
            parsed = json.loads(result)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"Statistics function '{fn_name}' "
                f"returned a string that is not valid JSON: {exc}"
            ) from exc
    elif isinstance(result, dict):
        parsed = result
    else:
        raise TypeError(
            f"Statistics function '{fn_name}' must "
            f"return a dict (e.g. a csv-stats result) or a JSON string; got "
            f"{type(result).__name__}."
        )

    parsed = _jsonify_stat(parsed)
    if isinstance(parsed, dict):
        parsed.pop("date", None)
        if report_path:
            parsed["report_path"] = str(report_path)

    return json.dumps(parsed, sort_keys=True, default=str)
