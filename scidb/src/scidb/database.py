"""Database connection and management using SciDuck backend."""

import json
import os
import threading
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from sciduckdb import (
    SciDuck,
    bulk_df_to_storage_rows,
    dataframe_to_storage_rows,
    infer_data_columns,
    record_schema_mismatch,
    storage_to_python,
    storage_to_python_column,
    unflatten_dict,
    value_to_storage_row,
    count_null_list_elements,
)

from . import provenance_query
from .bindings import variant_signature
from .exceptions import (
    AmbiguousVersionError,
    DatabaseNotConfiguredError,
    NotFoundError,
    NotRegisteredError,
)
from .hashing import canonical_hash, frame_path_counts, generate_record_id
from .log import Log
from .parameter import parameter_node_name
from .schema_values import (
    VALID_SCHEMA_KEY_TYPES,
    canonical_numeric_value,
    from_schema_str,
    schema_str,
)
from .variable import BaseVariable

if TYPE_CHECKING:
    from .pipeline import Pipeline


def _describe_data(val):
    """Return a short type/shape string for logging data values."""
    if isinstance(val, pd.DataFrame):
        return f"DataFrame {val.shape[0]}x{val.shape[1]} cols={list(val.columns)}"
    if isinstance(val, np.ndarray):
        return f"ndarray shape={val.shape} dtype={val.dtype}"
    if isinstance(val, (list, tuple)):
        return f"{type(val).__name__} len={len(val)}"
    if isinstance(val, dict):
        return f"dict keys={list(val.keys())}"
    return type(val).__name__


def _match_branch_param(branch_params_dict: dict, key: str, value: Any) -> bool:
    """Match a single branch_params filter key/value against a branch_params dict.

    Which KEY a filter name refers to is ``variant.match_bare_name`` (exact
    first, then bare-suffix, ambiguity is an error). This adds only the VALUE
    rule: a list/tuple means membership (``stored in value``).
    """
    from .variant import match_bare_name

    matched = match_bare_name(branch_params_dict, key)
    if matched is None:
        return False
    stored = branch_params_dict[matched]
    if isinstance(value, (list, tuple)):
        return stored in value
    return stored == value


def _filter_records_by_branch_params(df, branch_params_filter: dict | None, duck=None):
    """Filter a records DataFrame by a branch_params_filter dict.

    Shared by both load paths (``_find_record``'s no-``where`` fast path and the
    ``where=`` path that routes through ``_load_with_where``) so ``where=`` and
    branch_param pinning (``Variant``) can coexist.

    Variant branch params are **derived from the bipartite graph** (the
    accumulated upstream constants, §6) — including direct-save non-schema kwargs,
    which P0 anchors as constants on a synthetic save invocation (namespaced
    ``__save__.<kwarg>``). For each filter key/value a record is kept when its
    derived branch params match (exact or bare-suffix via
    :func:`_match_branch_param`). Raises ``AmbiguousParamError`` if a bare key
    matches multiple namespaced branch params.
    """
    if not branch_params_filter or len(df) == 0:
        return df
    from .variant import VariantAxes

    # The three variant dimensions. They travel in one dict on purpose
    # (docs/claude/variant-space.md §2) but resolve against different graph
    # reads: constants accumulate through `branch_params_batch`, code versions
    # and run options through `chain_batch`. `VariantAxes` is the ONE parse.
    axes = VariantAxes.of(branch_params_filter)

    if axes.code and duck is not None:
        df = _filter_records_by_code_version(df, axes.code, duck)
        if len(df) == 0:
            return df
    if axes.run and duck is not None:
        df = _filter_records_by_run_options(df, axes.run, duck)
        if len(df) == 0:
            return df

    param_filter = axes.constants
    if not param_filter:
        return df

    # Derive every candidate record's branch params from the graph in one batched
    # closure build (instead of a per-record ancestry walk).
    bp_cache: dict = (
        provenance_query.branch_params_batch(duck, df["record_id"].tolist())
        if duck is not None
        else {}
    )

    def _bp_for(record_id):
        return bp_cache.get(record_id, {})

    for key, value in param_filter.items():

        def _match_row(row, k=key, v=value):
            return _match_branch_param(_bp_for(row["record_id"]), k, v)

        df = df[df.apply(_match_row, axis=1)]
    return df


def _pin_values(value) -> tuple[set, object]:
    """``(wanted_set, wanted)`` for a code/run pin value.

    A list/tuple/set means "any of these" — the membership rule branch params
    have always had (`_match_branch_param`), and the form the Plot Studio
    location picker sends: it hands scidb the row's selection as it holds it,
    with list values (``{"__run__.fn": ["distribute=true"]}``). ``str()`` of
    that is the literal text ``"['distribute=true']"``, which matched nothing
    (2026-09-14). ``wanted`` is the scalar when there is exactly one, else the
    sorted list — for messages only.
    """
    if isinstance(value, (list, tuple, set, frozenset)):
        wanted_set = {str(v) for v in value}
    else:
        wanted_set = {str(value)}
    wanted = sorted(wanted_set)[0] if len(wanted_set) == 1 else sorted(wanted_set)
    return wanted_set, wanted


def _filter_records_by_code_version(df, code_filter: dict, duck):
    """Keep only records whose producing *code* matches ``code_filter``.

    The code half of :func:`_filter_records_by_branch_params`. Keys are either
    ``code_filter`` is the CODE axis as ``VariantAxes`` parsed it —
    ``{fn_name: value}``, with ``None`` for the bare (resolve-the-function)
    form. Values are a per-function ordinal (``"v1"``) or ``"latest"``.

    Two resolutions, deliberately different:

    * a named ordinal compares against
      :func:`~scidb.provenance_query.code_version_ordinals`, which is scoped per
      *function* so ``v2`` names the same code everywhere;
    * ``"latest"`` uses ``variant_identity_batch``'s ``is_latest``, which is
      chain-wide and resolved per schema *location* — so a location never re-run
      under the newest code keeps its own newest record instead of dropping out
      of the run entirely.
    """
    from .exceptions import AmbiguousParamError
    from .variant import LATEST_VERSION

    record_ids = df["record_id"].tolist()
    pinned_fns: set = set()
    chain_wide = False

    for fn_name, value in code_filter.items():
        wanted_set, wanted = _pin_values(value)

        if wanted_set == {LATEST_VERSION}:
            # Chain-wide: pins every versioned dimension at once, so it is never
            # a partial pin and needs no per-function resolution.
            chain_wide = True
            ident = provenance_query.variant_identity_batch(duck, record_ids)
            keep = {
                rid
                for rid in record_ids
                # `is_latest` is None for a raw record — nothing upstream ever
                # versioned it, so it cannot be stale. Keep it.
                if (ident.get(rid) or {}).get("is_latest") is not False
            }
            df = df[df["record_id"].isin(keep)]
            record_ids = df["record_id"].tolist()
            continue

        chains = provenance_query.code_versions_batch(duck, record_ids)
        present_fns = {name for chain in chains.values() for name in chain}
        ordinals = provenance_query.code_version_ordinals(duck, present_fns)

        if fn_name is None:
            # Bare pin: unambiguous only when exactly one upstream function has
            # more than one version. Mirrors `_match_branch_param`'s bare-name
            # rule, and fails the same way rather than picking arbitrarily.
            candidates = sorted(ordinals)
            if len(candidates) > 1:
                raise AmbiguousParamError(
                    f"code_version={wanted!r} is ambiguous: more than one "
                    f"upstream function has multiple versions ({candidates}). "
                    f'Name one with fn=, e.g. Variant(X, fn="{candidates[0]}", '
                    f"code_version={wanted!r})."
                )
            if not candidates:
                # Nothing upstream is versioned, so the pin selects everything
                # rather than nothing — a project where no function was ever
                # edited must not silently produce an empty run.
                Log.debug(
                    f"code_version={wanted!r}: no upstream function has >1 "
                    f"version; pin is a no-op"
                )
                continue
            fn_name = candidates[0]

        # A named function that is nowhere upstream is a typo, not an empty
        # result set. Every failure below says what IS available, because the
        # alternative — silently keeping nothing — produces a run that reports
        # success having computed no records at all.
        if fn_name not in present_fns:
            raise ValueError(
                f"code_version pin names {fn_name!r}, which is not upstream of "
                f"these records. Available: {sorted(present_fns) or 'none'}."
            )

        by_hash = ordinals.get(fn_name)
        if not by_hash:
            # Single-version function: `code_version_ordinals` omits it, but its
            # one version IS v1. Treat that as a satisfied pin rather than
            # matching nothing — the qualified path used to empty the run here.
            if "v1" not in wanted_set:
                raise ValueError(
                    f"{fn_name!r} has only one recorded version, so "
                    f"code_version={wanted!r} matches nothing. Use 'v1', or "
                    f"'latest'."
                )
            pinned_fns.add(fn_name)
            continue

        available = sorted(set(by_hash.values()))
        if not wanted_set & set(available):
            raise ValueError(
                f"{fn_name!r} has versions {available}; code_version="
                f"{wanted!r} matches nothing."
            )

        pinned_fns.add(fn_name)
        keep = {
            rid
            for rid in record_ids
            if by_hash.get(chains.get(rid, {}).get(fn_name)) in wanted_set
        }
        df = df[df["record_id"].isin(keep)]
        record_ids = df["record_id"].tolist()

    # A pin naming ONE function when several upstream functions are versioned is
    # a *partial* pin: the unnamed dimensions stay free and still fan out. That
    # is legitimate ("every variant where bandpass=v1"), but it is not what
    # "pin the variant" sounds like, and a silently doubled record count is the
    # same failure class this whole feature exists to prevent. Say so.
    if record_ids and not chain_wide:
        chains = provenance_query.code_versions_batch(duck, record_ids)
        versioned = set(
            provenance_query.code_version_ordinals(
                duck, {name for chain in chains.values() for name in chain}
            )
        )
        unpinned = sorted(versioned - pinned_fns)
        if unpinned:
            Log.warn(
                f"code_version pin is PARTIAL: {sorted(pinned_fns)} pinned, but "
                f"{unpinned} still have multiple versions among the surviving "
                f"records, so those dimensions remain free and will fan out. "
                f"Pin them too, or use code_version='latest' to pin the whole "
                f"chain."
            )

    return df


def _filter_records_by_run_options(df, run_filter: dict, duck):
    """Keep only records whose upstream *run options* match ``run_filter``.

    The run-options third of :func:`_filter_records_by_branch_params`.
    ``run_filter`` is the RUN axis as ``VariantAxes`` parsed it —
    ``{fn_name: value}``, ``None`` for the bare form. Values are a
    :func:`~scidb.provenance_query.run_options_label` string
    (``"distribute=true"``) or ``"latest"``.

    ``"latest"`` is the same chain-wide, per-location ``is_latest`` that
    ``code_version="latest"`` uses — there is ONE notion of "current at this
    location", and since 2026-09-14 it already accounts for run options, so
    the two spellings deliberately share a resolution rather than growing a
    second one.

    A label pin compares exactly, per named function, against
    :func:`~scidb.provenance_query.run_options_batch`. Bare pins resolve against
    :func:`~scidb.provenance_query.run_option_axes` the way bare code pins
    resolve against ``code_version_ordinals``: exactly one function that ran
    more than one way is unambiguous, several raise, none is a no-op.
    """
    from .exceptions import AmbiguousParamError
    from .variant import LATEST_VERSION

    record_ids = df["record_id"].tolist()

    for fn_name, value in run_filter.items():
        wanted_set, wanted = _pin_values(value)  # list = membership, see there

        if wanted_set == {LATEST_VERSION}:
            ident = provenance_query.variant_identity_batch(duck, record_ids)
            keep = {
                rid
                for rid in record_ids
                if (ident.get(rid) or {}).get("is_latest") is not False
            }
            df = df[df["record_id"].isin(keep)]
            record_ids = df["record_id"].tolist()
            continue

        runs = provenance_query.run_options_batch(duck, record_ids)
        present_fns = {name for chain in runs.values() for name in chain}
        axes = provenance_query.run_option_axes(duck, present_fns)

        if fn_name is None:
            candidates = sorted(axes)
            if len(candidates) > 1:
                raise AmbiguousParamError(
                    f"run_options={wanted!r} is ambiguous: more than one "
                    f"upstream function has run under several option sets "
                    f"({candidates}). Name one with fn=, e.g. "
                    f'Variant(X, fn="{candidates[0]}", run_options={wanted!r}).'
                )
            if not candidates:
                Log.debug(
                    f"run_options={wanted!r}: no upstream function ran under >1 "
                    f"run-option set; pin is a no-op"
                )
                continue
            fn_name = candidates[0]

        if fn_name not in present_fns:
            raise ValueError(
                f"run_options pin names {fn_name!r}, which is not upstream of "
                f"these records. Available: {sorted(present_fns) or 'none'}."
            )
        available = sorted(
            {chain.get(fn_name) for chain in runs.values() if fn_name in chain}
            - {None}
        )
        if not wanted_set & set(available):
            raise ValueError(
                f"{fn_name!r} ran under {available}; run_options={wanted!r} "
                f"matches nothing."
            )
        keep = {
            rid
            for rid in record_ids
            if runs.get(rid, {}).get(fn_name) in wanted_set
        }
        n_before = len(record_ids)
        df = df[df["record_id"].isin(keep)]
        record_ids = df["record_id"].tolist()
        # INFO, not DEBUG: an empty result here is indistinguishable downstream
        # from "no records at all" (for_each just runs zero combos), so the
        # count and the labels it chose between have to be in the log.
        Log.info(
            f"run_options pin {fn_name}={wanted!r}: kept {len(record_ids)}/{n_before} "
            f"record(s); labels present for {fn_name}: {available}"
        )

    return df


# Global database instance (thread-local for safety)
_local = threading.local()


def _get_leaf_paths(d, prefix=()):
    """Recursively get all leaf paths in a nested dict.

    A leaf is any value that is NOT a dict.  Returns a list of tuples,
    each tuple being the sequence of keys from root to leaf.
    """
    paths = []
    for key, value in d.items():
        current = prefix + (key,)
        if isinstance(value, dict):
            paths.extend(_get_leaf_paths(value, current))
        else:
            paths.append(current)
    return paths


def _get_nested_value(d, path):
    """Get a value from a nested dict following *path* (tuple of keys)."""
    current = d
    for key in path:
        current = current[key]
    return current


def _set_nested_value(d, path, value):
    """Set a value in a nested dict by *path*, creating intermediate dicts."""
    for key in path[:-1]:
        d = d.setdefault(key, {})
    d[path[-1]] = value


def _flatten_struct_columns(df):
    """Flatten DataFrame columns that contain nested dicts into dot-separated columns.

    For each object-dtype column whose first non-null value is a ``dict``,
    recursively extract all leaf paths and create new columns named
    ``"original_col.key1.key2.leaf"``.

    **Leaf handling:**
    - Scalar leaves (int, float, str, bool, None) are stored directly.
    - Array leaves (numpy arrays, Python lists) are serialised to a JSON
      string so every cell in the resulting column is a simple scalar type
      that DuckDB can ingest.

    Returns
    -------
    (flattened_df, struct_columns_info)
        *struct_columns_info* maps each flattened original column name to
        metadata needed by ``_unflatten_struct_columns`` on load.
        Empty dict when no struct columns are found.
    """
    if len(df) == 0:
        return df, {}

    struct_info = {}
    cols_to_drop = []
    new_col_data = {}  # ordered: col_name -> list of values

    for col_idx, col in enumerate(df.columns):
        if df[col].dtype != object:
            continue

        # Find first non-null value
        first_val = None
        for v in df[col]:
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                first_val = v
                break

        if not isinstance(first_val, dict):
            continue

        # This column contains nested dicts — flatten it
        leaf_paths = _get_leaf_paths(first_val)
        if not leaf_paths:
            continue

        array_leaves = {}  # dot_path -> {"dtype": ..., "shape": ...}

        for path in leaf_paths:
            dot_path = ".".join(path)
            flat_col_name = f"{col}.{dot_path}"
            values = []
            for row_val in df[col]:
                if row_val is None or (
                    isinstance(row_val, float) and np.isnan(row_val)
                ):
                    values.append(None)
                    continue
                try:
                    leaf = _get_nested_value(row_val, path)
                except (KeyError, TypeError):
                    values.append(None)
                    continue

                if isinstance(leaf, np.ndarray):
                    # Track array metadata from first occurrence
                    if dot_path not in array_leaves:
                        array_leaves[dot_path] = {
                            "dtype": str(leaf.dtype),
                            "shape": list(leaf.shape),
                        }
                    values.append(json.dumps(leaf.tolist()))
                elif isinstance(leaf, list):
                    if dot_path not in array_leaves:
                        array_leaves[dot_path] = {"dtype": "list"}
                    values.append(json.dumps(leaf))
                else:
                    values.append(leaf)

            new_col_data[flat_col_name] = values

        cols_to_drop.append(col)
        struct_info[col] = {
            "paths": [list(p) for p in leaf_paths],
            "array_leaves": array_leaves,
            "col_position": col_idx,
        }

    if not cols_to_drop:
        return df, {}

    result = df.drop(columns=cols_to_drop)
    for name, values in new_col_data.items():
        result[name] = values

    return result, struct_info


def _unflatten_struct_columns(df, struct_info):
    """Reconstruct nested-dict columns from dot-separated flat columns.

    Inverse of ``_flatten_struct_columns``.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with dot-separated columns produced by ``_flatten_struct_columns``.
    struct_info : dict
        The metadata dict that was stored alongside the data.

    Returns
    -------
    pd.DataFrame with the original nested-dict object columns restored.
    """
    if not struct_info:
        return df

    result = df.copy()

    # Process struct columns in reverse position order so inserts don't shift indices
    for col_name, info in sorted(
        ((k, v) for k, v in struct_info.items() if k != "__list_columns__"),
        key=lambda x: x[1]["col_position"],
        reverse=True,
    ):
        paths = [tuple(p) for p in info["paths"]]
        array_leaves = info.get("array_leaves", {})
        col_position = info["col_position"]

        # Collect all flat column names belonging to this struct
        flat_col_names = [f"{col_name}.{'.'.join(p)}" for p in paths]
        existing_flat = [c for c in flat_col_names if c in result.columns]

        if not existing_flat:
            continue

        # Build nested dicts row by row
        nested_values = []
        n_rows = len(result)

        for row_idx in range(n_rows):
            row_dict = {}
            for path, flat_col in zip(paths, flat_col_names, strict=False):
                if flat_col not in result.columns:
                    continue
                val = result[flat_col].iloc[row_idx]
                dot_path = ".".join(path)

                # Restore arrays from JSON
                if dot_path in array_leaves and val is not None:
                    arr_meta = array_leaves[dot_path]
                    if isinstance(val, str):
                        parsed = json.loads(val)
                    else:
                        parsed = val
                    if arr_meta.get("dtype") == "list":
                        val = parsed
                    else:
                        val = np.array(parsed, dtype=np.dtype(arr_meta["dtype"]))
                        expected_shape = arr_meta.get("shape")
                        if (
                            expected_shape
                            and list(val.shape) != expected_shape
                            and val.size == np.prod(expected_shape)
                        ):
                            val = val.reshape(expected_shape)

                _set_nested_value(row_dict, path, val)
            nested_values.append(row_dict)

        # Drop the flat columns
        result = result.drop(columns=existing_flat)

        # Insert the reconstituted column at its original position
        # (clamped to current column count since other columns may have shifted)
        insert_pos = min(col_position, len(result.columns))
        result.insert(insert_pos, col_name, nested_values)

    # Convert list-valued cells to numpy arrays for MATLAB interop.
    # DuckDB DOUBLE[] columns come back as Python lists; old VARCHAR saves
    # come back as string representations like "[1.0, 2.0, 3.0]".
    for col in result.columns:
        if result[col].dtype != object:
            continue
        first_val = next(
            (
                v
                for v in result[col]
                if v is not None and not (isinstance(v, float) and np.isnan(v))
            ),
            None,
        )
        if first_val is None:
            continue

        if isinstance(first_val, (list, np.ndarray)):
            # DuckDB DOUBLE[] returns as lists or numpy arrays — ensure numpy
            result[col] = result[col].apply(
                lambda v: np.array(v, dtype=float) if isinstance(v, list) else v
            )
        elif isinstance(first_val, str) and first_val.strip().startswith("["):
            # Backwards compat: parse VARCHAR strings from old saves
            def _parse_list_str(v):
                if not isinstance(v, str):
                    return v
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return np.array(parsed, dtype=float)
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
                return v

            result[col] = result[col].apply(_parse_list_str)

    return result


def get_user_id() -> str | None:
    """
    Get the current user ID from environment.

    The user ID is used for attribution in cross-user provenance tracking.
    Set the SCIDB_USER_ID environment variable to identify the current user.

    Returns:
        The user ID string, or None if not set.
    """
    return os.environ.get("SCIDB_USER_ID")


def configure_database(
    dataset_db_path: str | Path,
    dataset_schema_keys: list[str],
    schema_key_types: "dict[str, str] | None" = None,
) -> "DatabaseManager":
    """
    Configure the global database connection.

    Single-call setup that creates the database, auto-registers all known
    BaseVariable subclasses, and enables lineage caching.

    Args:
        dataset_db_path: Path to the DuckDB database file
        dataset_schema_keys: List of metadata keys that define the dataset schema
            (e.g., ["subject", "visit", "channel"]). These keys identify the
            logical location of data and are used for the folder hierarchy.
            Any metadata keys not in this list are treated as version parameters
            that distinguish different computational versions of the same data.
        schema_key_types: Optional per-key type declaration, e.g.
            ``{"trial": "numeric"}``.  ``"numeric"`` keys canonicalize every
            spelling of the same number to one identity ("001", 1, and 1.0
            all store as "1"; zero-padded filenames still resolve via
            PathInput's numeric fallback).  ``"string"`` keys are verbatim —
            spelling IS identity, and PathInput matches them exactly only.
            Undeclared keys behave as strings until a PathInput resolution
            has to bridge spellings (e.g. trial=1 matching "6MWT-001.mat"),
            which raises SchemaKeyTypeError asking for a declaration.

    Returns:
        The DatabaseManager instance
    """
    # Set log file path next to the database file (a no-op if a caller —
    # e.g. the GUI server, which attaches before its startup discovery pass
    # — already pointed the sink here).
    #
    # Attached BEFORE the connection is built, not after: a MATLAB host calls
    # this as its first scidb entry point, so anything logged during
    # construction — including the phase timings below and any DDL failure —
    # would otherwise land on stderr only and be missing from the very log
    # file being set up.
    from .log import Log, attach_log_file

    attach_log_file(dataset_db_path)

    # Phase timing: a MATLAB run pays this cost on every invocation (the
    # script must close the database at the end to hand the write lock back
    # to the GUI, so nothing is reused), and it was measured at ~170ms
    # against a database on a network share. The breakdown says whether that
    # is the DuckDB open itself or the idempotent CREATE TABLE IF NOT EXISTS
    # round trips re-run each time.
    with Log.timer(
        "configure_database", extra=Path(dataset_db_path).name
    ) as timer:
        with timer.phase("database_manager"):
            db = DatabaseManager(
                dataset_db_path,
                dataset_schema_keys=dataset_schema_keys,
                dataset_schema_key_types=schema_key_types,
            )
        with timer.phase("register_types"):
            for cls in BaseVariable._all_subclasses.values():
                db.register(cls)
        db.set_current_db()

        # Propagate schema keys to scifor so that DataFrame detection and
        # distribute=True work identically in DB-backed and standalone modes.
        with timer.phase("scifor_set_schema"):
            try:
                import scifor

                scifor.set_schema(list(dataset_schema_keys))
            except ImportError:
                pass

    # Run-context header: makes every log file self-describing (which
    # versions produced it) when debugging an archived log or a colleague's.
    Log.info(
        f"configure_database: path={dataset_db_path}, "
        f"schema_keys={list(dataset_schema_keys)}, "
        f"schema_key_types={schema_key_types or {}} | {_run_context()}"
    )

    return db


def _run_context() -> str:
    """One-line host/version fingerprint for the log header."""
    import os
    import platform
    from importlib import metadata

    versions = []
    for pkg in ("scidb", "scifor", "sciduckdb", "scilineage", "scistacklog"):
        try:
            versions.append(f"{pkg}={metadata.version(pkg)}")
        except Exception:
            pass
    return f"python={platform.python_version()}, pid={os.getpid()}" + (
        ", " + ", ".join(versions) if versions else ""
    )


def get_database() -> "DatabaseManager":
    """
    Get the global database connection.

    Returns:
        The DatabaseManager instance

    Raises:
        DatabaseNotConfiguredError: If configure_database() hasn't been called
    """
    db = getattr(_local, "database", None)
    if db is None:
        raise DatabaseNotConfiguredError(
            "Database not configured. Call configure_database(path) first."
        )
    if getattr(db, "_closed", False):
        db.reopen()
    return db


def set_current_database(db) -> None:
    """Install *db* as the thread's current database — what ``get_database``
    returns. Public so a package that supplies its own database object (the
    scidb-net remote client) does not reach into ``_local``
    (cleanup-audit F2/F7: private cross-package imports)."""
    _local.database = db


def clear_current_database() -> None:
    """Forget the thread's current database, if any (a tool that configured
    one only to scaffold a project leaves none behind)."""
    if hasattr(_local, "database"):
        delattr(_local, "database")


def database_or_none(db: "DatabaseManager | None" = None) -> "DatabaseManager | None":
    """*db* if given, else the configured database, else ``None`` — for a
    caller that can proceed without one.

    THE one spelling of "which database does this call use". Only "nothing
    configured" (:class:`DatabaseNotConfiguredError`) reads as ``None``; any
    other failure — a reopen that cannot get the file lock, above all —
    propagates. ``foreach`` used to spell this as ``try: get_database()
    except Exception: pass`` in a dozen places, so a locked database looked
    exactly like no database and a run silently carried on with different
    schema keys (cleanup-audit F5).
    """
    if db is not None:
        return db
    try:
        return get_database()
    except DatabaseNotConfiguredError:
        return None


def dataset_schema_keys_of(db: "DatabaseManager | None" = None) -> list[str]:
    """The dataset's schema keys, from the database (the one holder).

    ``scifor.set_schema`` keeps a COPY for scifor's standalone mode; scidb
    reads the database, never that copy back (cleanup-audit F14). ``[]``
    when no database is configured.
    """
    active = database_or_none(db)
    return list(getattr(active, "dataset_schema_keys", None) or [])


def aggregate_pipeline_variants(variants: list[dict]) -> dict:
    """Pipeline variant rows (``list_pipeline_variants``) -> the aggregated
    shape :meth:`DatabaseManager.get_aggregated_variants` returns — PURE, no
    database. THE one conversion: the GUI used to keep a second, test-only copy
    of it (``graph_builder.aggregate_variants``), so its tests exercised a path
    production never ran (cleanup-audit F19).

    ``variables`` carries ``record_count: None``; the method fills the real
    counts. ``path_inputs`` is keyed by the PathInput SPEC (its JSON), each
    entry ``{"template", "root_folder", "functions": [(fkey, param_name), ...],
    "declared_names": {(fkey, param_name): declared PathInput name}}`` — the
    latter only where the run recorded one (cleanup-audit F38).
    """
    from collections import defaultdict

    from .provenance import parse_path_input_spec as _parse_path_input

    # Initialize result structure
    functions = defaultdict(
        lambda: {
            "input_params": {},
            "outputs": [],
            "constants": defaultdict(list),
            "parameter_names": {},
            # {argument: declared PathInput name} where the run recorded it
            # (cleanup-audit F38) — what `provenance.call_site_wiring_ids`
            # groups steps by.
            "path_input_names": {},
            "variant_count": 0,
            "variants": [],
        }
    )
    all_var_types = set()
    const_counts = defaultdict(lambda: defaultdict(int))
    const_fns = defaultdict(set)
    fn_constants_map = defaultdict(set)
    path_inputs = {}

    # Aggregate variants by (fn_name, call_id)
    for v in variants:
        fn = v["function_name"]
        cid = v.get("call_id", "")
        if not cid:
            continue  # Skip legacy variants without call_id

        fkey = (fn, cid)
        out = v["output_type"]
        inputs = v["input_types"]
        constants = v["constants"]
        count = v["record_count"]

        # Track variable types
        all_var_types.add(out)

        # Process inputs
        for param_name, type_val in inputs.items():
            # Check if it's a PathInput
            pi = _parse_path_input(type_val)
            if pi is not None:
                # Keyed by the SPEC, never the parameter name: two
                # functions whose PathInput parameters share a name
                # (`filePath`) but not a template used to be filed under
                # the FIRST one's template (cleanup-audit F37).
                spec_key = json.dumps(pi, sort_keys=True, default=str)
                entry = path_inputs.setdefault(
                    spec_key, {**pi, "functions": set(), "declared_names": {}}
                )
                entry["functions"].add((fkey, param_name))
                recorded = (v.get("path_input_names") or {}).get(param_name)
                if recorded:
                    entry["declared_names"][(fkey, param_name)] = recorded
                    functions[fkey]["path_input_names"][param_name] = recorded
            else:
                all_var_types.add(type_val)
                functions[fkey]["input_params"][param_name] = type_val

        # Track outputs
        if out not in functions[fkey]["outputs"]:
            functions[fkey]["outputs"].append(out)

        # Track constants. A constant's NODE is the declared Parameter its
        # run recorded, else the argument it filled (cleanup-audit B1):
        # the value lives under the argument in `functions[...]` (that is
        # what the function receives), and under the node everywhere a
        # Parameter is named (`constants`, `parameter_names`).
        recorded_names = v.get("parameter_names") or {}
        for k, val in constants.items():
            node = parameter_node_name(k, recorded_names)
            seen = functions[fkey]["parameter_names"].get(k)
            if seen is not None and seen != node:
                Log.warn(
                    f"aggregate_pipeline_variants: {fn} call {cid} argument "
                    f"{k!r} was fed by Parameter {seen!r} in one run and "
                    f"{node!r} in another; showing {seen!r}"
                )
                node = seen
            functions[fkey]["parameter_names"][k] = node
            const_counts[node][str(val)] += count
            const_fns[node].add(fkey)
            fn_constants_map[fkey].add(k)
            if val not in functions[fkey]["constants"][k]:
                functions[fkey]["constants"][k].append(val)

        # Track variant. output_num is carried through because for a
        # MULTI-OUTPUT fn it tells the GUI which slot of the signature
        # produced this output, and for MATLAB fns that is the sole
        # DB-derived source for the fn->output edge (see api/pipeline.py
        # matlab_param_to_class). Dropping it silently forced that edge to
        # depend on a hand-drawn manual edge instead.
        #
        # It is NOT variant identity (2026-09-22): a distributed run
        # numbers its slices, so keying on it made one call N variants.
        # Each output_type is now its own group and this is that type's
        # own (lowest) slot — which is what the GUI wanted all along.
        # A single-output fn must ignore it entirely; see
        # _matlab_param_to_class_from_db for the two cases where the
        # number is not a signature position at all.
        functions[fkey]["variants"].append(
            {
                "input_types": inputs,
                "constants": constants,
                "parameter_names": dict(recorded_names),
                "output_type": out,
                "output_num": v.get("output_num"),
                "record_count": count,
            }
        )
        functions[fkey]["variant_count"] += 1

    # Record counts need the database; the caller fills them in.
    variables = {t: {"record_count": None} for t in sorted(all_var_types)}

    # Build constants result
    constants_result = {}
    for const_name in const_counts:
        values = [
            {"value": val, "record_count": cnt}
            for val, cnt in sorted(const_counts[const_name].items())
        ]
        constants_result[const_name] = {
            "values": values,
            "functions": list(const_fns[const_name]),
        }

    # Convert functions dict to regular dict (remove defaultdict)
    functions_result = {}
    for fkey, data in functions.items():
        functions_result[fkey] = {
            "input_params": dict(data["input_params"]),
            "outputs": data["outputs"],
            "constants": {k: list(v) for k, v in data["constants"].items()},
            "parameter_names": dict(data["parameter_names"]),
            "path_input_names": dict(data["path_input_names"]),
            "variant_count": data["variant_count"],
            "variants": data["variants"],
        }

    # Functions as a sorted list of (fkey, param_name) pairs.
    for entry in path_inputs.values():
        entry["functions"] = sorted(entry["functions"])

    return {
        "functions": functions_result,
        "variables": variables,
        "constants": constants_result,
        "path_inputs": path_inputs,
    }


def call_site_path_input_names(path_inputs: dict, fallback=None) -> dict:
    """``{fkey: {param: PathInput name}}`` for the ``path_inputs`` of
    :func:`aggregate_pipeline_variants` — the PathInput term of a step's wiring.

    The name a run RECORDED (``declared_names``) always wins: it is the fact.
    Only a run recorded without one — before 2026-09-24, or a hand-written
    script whose PathInput was never declared — takes ``fallback(entry)``,
    the caller's best guess (the GUI matches its registry; the CLI has none and
    uses the spec, the default here). The spec is template AND root folder —
    the key the GUI's registry match uses too: one template under two roots
    names two different sets of files.
    """
    fallback = fallback or (
        lambda entry: "spec:"
        + json.dumps(
            {"template": entry.get("template"), "root_folder": entry.get("root_folder")},
            sort_keys=True,
            default=str,
        )
    )
    out: dict = {}
    for entry in path_inputs.values():
        recorded = entry.get("declared_names") or {}
        guess = None
        for fkey, param in entry.get("functions") or ():
            fkey = tuple(fkey)
            name = recorded.get((fkey, param))
            if name is None:
                if guess is None:
                    guess = fallback(entry)
                name = guess
            out.setdefault(fkey, {})[param] = name
    return out


def call_site_wiring_ids(aggregate: dict, fallback=None) -> dict:
    """``{fkey: wiring_id}`` — which recorded call sites form ONE pipeline
    step, for the output of :func:`aggregate_pipeline_variants`.

    THE step grouping (cleanup-audit F38). ``scidb graph`` groups through
    this function; the GUI groups by the same recipe
    (``provenance.compute_wiring_id``) over the same facts, its PathInput
    names coming from :func:`call_site_path_input_names`' rule (recorded
    first). Constants are not in the key: a sweep is one step with several
    variants.
    """
    from .provenance import compute_wiring_id

    names = call_site_path_input_names(aggregate.get("path_inputs") or {}, fallback)
    return {
        tuple(fkey): compute_wiring_id(
            fkey[0],
            data.get("input_params") or {},
            data.get("outputs") or [],
            names.get(tuple(fkey), {}),
        )
        for fkey, data in (aggregate.get("functions") or {}).items()
    }


def _declared_rank_column(col, declared: list[str]) -> list[int]:
    """Sort positions for *col* under a declared level order.

    One integer per row, because a DataFrame sort needs a comparable column and
    a ``(group, position)`` tuple would make every undeclared level tie — they
    would then come out in whatever order the query happened to return.

    Declared levels take ``0…n-1``; everything else is appended, ordered among
    itself by the same numeric-or-string rule the undeclared path below uses,
    so the promise "the rest keeps the default order" holds literally.
    """
    position = {level: index for index, level in enumerate(declared)}
    extra = sorted(
        {str(v) for v in col if v is not None and str(v) not in position},
        key=_natural_text_key,
    )
    for offset, value in enumerate(extra):
        position[value] = len(declared) + offset
    # A null has no level, so it sorts after every real one rather than
    # colliding with the first declared position.
    return [position.get(str(v), len(position)) for v in col]


def _natural_text_key(value: str) -> tuple:
    """Numeric when the whole value is a number, text otherwise — the same
    distinction ``_sort_by_schema_keys`` makes per column, applied per value so
    a mixed set ("1", "S01") still has a total order."""
    try:
        return (0, float(value), "")
    except (TypeError, ValueError):
        return (1, 0.0, value)


class DatabaseManager:
    """
    Manages data storage and lineage persistence (both in DuckDB via SciDuck).

    Example:
        db = configure_database("experiment.duckdb", ["subject", "session"])

        RawSignal.save(np.eye(3), subject=1, session=1)
        loaded = RawSignal.load(subject=1, session=1)
    """

    def __init__(
        self,
        dataset_db_path: str | Path,
        dataset_schema_keys: list[str],
        read_only: bool = False,
        dataset_schema_key_types: "dict[str, str] | None" = None,
    ):
        """
        Initialize database connection.

        Args:
            dataset_db_path: Path to DuckDB database file (created if doesn't exist)
            dataset_schema_keys: List of metadata keys that define the dataset schema
                (e.g., ["subject", "visit", "channel"]). These keys identify the
                logical location of data. Any other metadata keys are treated as
                version parameters.
            read_only: Open the database read-only (it must already exist). No
                DDL runs and every write on the connection fails at the DuckDB
                level — used by inspection tooling (scidb.inspect / the scidb
                CLI) so it can never contend for the write lock.
        """
        self.dataset_db_path = Path(dataset_db_path)

        if isinstance(dataset_schema_keys, (set, frozenset)):
            raise TypeError(
                "dataset_schema_keys must be an ordered sequence (list or tuple), "
                "not a set. Schema key order defines the dataset hierarchy."
            )
        self.dataset_schema_keys = list(dataset_schema_keys)

        key_types = dict(dataset_schema_key_types or {})
        unknown_keys = set(key_types) - set(self.dataset_schema_keys)
        if unknown_keys:
            raise ValueError(
                f"schema_key_types declares keys that are not schema keys: "
                f"{sorted(unknown_keys)}. Schema keys: {self.dataset_schema_keys}"
            )
        bad_types = {
            k: t for k, t in key_types.items() if t not in VALID_SCHEMA_KEY_TYPES
        }
        if bad_types:
            raise ValueError(
                f"schema_key_types values must be one of "
                f"{VALID_SCHEMA_KEY_TYPES}, got: {bad_types}"
            )
        self.dataset_schema_key_types = key_types

        # Declared level order (`[schema_keys]`) is NOT snapshotted here: see
        # the `dataset_schema_key_order` property. Asked once now so the
        # config it came from is logged, and a typo reported, when the
        # database opens.
        self._validated_key_order: "dict | None" = None
        _ = self.dataset_schema_key_order

        self.read_only = bool(read_only)
        self._registered_types: dict[str, type[BaseVariable]] = {}

        # Initialize SciDuck backend for data storage and lineage (all in
        # DuckDB). Timed per phase: this runs on every MATLAB invocation (see
        # configure_database), and "opening the file" and "re-running the
        # idempotent DDL" have very different fixes when the database lives on
        # a network share.
        with Log.timer(
            "DatabaseManager.__init__", extra=self.dataset_db_path.name
        ) as timer:
            with timer.phase("duck_open"):
                self._duck = SciDuck(
                    self.dataset_db_path,
                    dataset_schema=dataset_schema_keys,
                    read_only=self.read_only,
                )

            # Create metadata tables for type registration (in DuckDB).
            # Skipped on read-only connections: the tables must already exist,
            # and DuckDB rejects DDL (even CREATE TABLE IF NOT EXISTS) in
            # read-only mode.
            if not self.read_only:
                with timer.phase("ensure_meta_tables"):
                    self._ensure_meta_tables()
                with timer.phase("ensure_record_save_table"):
                    self._ensure_record_save_table()
                with timer.phase("ensure_schema_overrides_table"):
                    self._ensure_schema_overrides_table()
                with timer.phase("ensure_provenance_tables"):
                    self._ensure_provenance_tables()

        self._closed = False  # Track connection open/closed state
        self._inspector = None  # lazy scidb.inspect.Inspector (see .inspect)

    def canonicalize_metadata(self, metadata: dict) -> dict:
        """Apply declared schema-key types to addressing metadata.

        Keys declared ``"numeric"`` collapse every spelling of the same
        number to one canonical identity (``"001"`` → 1 → stored ``"1"``);
        list values (load()'s OR semantics) canonicalize element-wise.
        ``"string"`` and undeclared keys pass through verbatim.  Returns a
        new dict; the input is not mutated.
        """
        if not self.dataset_schema_key_types:
            return metadata
        out = dict(metadata)
        for key, key_type in self.dataset_schema_key_types.items():
            if key_type != "numeric" or key not in out or out[key] is None:
                continue
            value = out[key]
            if isinstance(value, (list, tuple)):
                out[key] = [canonical_numeric_value(key, v) for v in value]
            else:
                out[key] = canonical_numeric_value(key, value)
        return out

    def _ensure_meta_tables(self):
        """Create internal metadata tables for type registration."""
        # Registered types table (remains in DuckDB for data type discovery)
        # Note: Only type_name is unique (PRIMARY KEY). table_name is not unique
        # to avoid DuckDB's ON CONFLICT ambiguity with multiple unique constraints.
        self._duck._execute("""
            CREATE TABLE IF NOT EXISTS _registered_types (
                type_name VARCHAR PRIMARY KEY,
                table_name VARCHAR NOT NULL,
                schema_version INTEGER NOT NULL,
                registered_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

    def _ensure_record_save_table(self):
        """Create ``_record_save`` — the append-only **save-event audit log**.

        One row per save event ``(record_id, timestamp)``; ``user_id`` is the only
        per-event payload. Everything else about a record (its type/variable_name,
        schema_id, content_hash, schema_version, and the mutable ``excluded`` flag)
        lives on the content-addressed ``_record`` entity row and is obtained by
        joining on ``record_id``. The only thing this table uniquely provides is
        **per-save recency** for the "latest" variant collapse (``_record`` is
        inserted ``ON CONFLICT DO NOTHING`` so its ``created_at`` is frozen at the
        first save and cannot track re-saves).
        """
        self._duck._execute("""
            CREATE TABLE IF NOT EXISTS _record_save (
                record_id VARCHAR NOT NULL,
                timestamp VARCHAR NOT NULL,
                user_id VARCHAR,
                PRIMARY KEY (record_id, timestamp)
            )
        """)

    def _ensure_schema_overrides_table(self):
        """Create __scidb_schema_overrides for persistent schema-level exclusions."""
        from .exclusions import ensure_overrides_table

        ensure_overrides_table(self)

    def _ensure_provenance_tables(self):
        """Create the seven bipartite provenance tables (see scidb.provenance)."""
        from .provenance import ensure_provenance_tables

        ensure_provenance_tables(self._duck)

    def _create_variable_view(self, variable_class: type[BaseVariable]):
        """Create a view joining a variable table with _schema via _record.

        ``schema_id`` and the mutable ``excluded`` flag now come straight from the
        content-addressed ``_record`` entity (one row per record_id), so no
        latest-by-timestamp CTE over the save-event log is needed.
        """
        table_name = variable_class.table_name()
        view_name = variable_class.view_name()
        schema_cols = ", ".join(f's."{col}"' for col in self.dataset_schema_keys)
        self._duck._execute(f"""
            CREATE OR REPLACE VIEW "{view_name}" AS
            SELECT
                t.*,
                s.schema_level, {schema_cols},
                r.excluded
            FROM "{table_name}" t
            LEFT JOIN _record r ON t.record_id = r.record_id
            LEFT JOIN _schema s ON r.schema_id = s.schema_id
        """)

    def _split_metadata(self, flat_metadata: dict) -> dict:
        """
        Split flat metadata into nested schema/version structure.

        Keys in schema_keys go to "schema", all other keys go to "version".
        """
        schema = {}
        version = {}
        for key, value in flat_metadata.items():
            if key in self.dataset_schema_keys:
                schema[key] = value
            else:
                version[key] = value
        return {"schema": schema, "version": version}

    def _infer_schema_level(self, schema_keys: dict) -> str | None:
        """
        Infer the schema level from provided keys.

        Walks dataset_schema_keys top-down. Returns the deepest provided key.
        Keys need not be contiguous — any subset of schema keys is valid.

        Returns None if no schema keys are provided.
        """
        if not schema_keys:
            return None

        level = None
        for key in self.dataset_schema_keys:
            if key in schema_keys:
                level = key
        return level

    def _save_record_event(
        self,
        record_id: str,
        timestamp: str,
        user_id: str | None,
    ) -> None:
        """Append a save-event row to ``_record_save``. Always inserts (audit trail).

        The record's type/schema/content metadata is carried by the ``_record``
        entity row (written alongside by the caller), not here.
        """
        Log.debug(
            f"_save_record_event: record_id={record_id[:12]}, timestamp={timestamp}"
        )
        self._duck._execute(
            """
            INSERT INTO _record_save (record_id, timestamp, user_id)
            VALUES (?, ?, ?)
            ON CONFLICT (record_id, timestamp) DO NOTHING
            """,
            [record_id, timestamp, user_id],
        )

    def _save_columnar(
        self,
        record_id: str,
        table_name: str,
        variable_class: type[BaseVariable],
        df: pd.DataFrame,
        schema_level: str | None,
        schema_keys: dict,
        content_hash: str,
        dict_of_arrays: bool = False,
        ndarray_keys: dict | None = None,
        struct_columns: dict | None = None,
    ) -> int:
        """
        Save a DataFrame into a columnar table identified by record_id.

        Used for custom-serialized data (to_db/from_db), native DataFrames,
        and dict-of-arrays data. The table uses record_id as the row identifier;
        multiple data rows sharing the same record_id are allowed.

        Returns schema_id.
        """
        # New schema rows can change a key's numeric/string answer.
        self._forget_schema_key_kinds()
        schema_id = (
            self._duck._get_or_create_schema_id(schema_level, schema_keys)
            if schema_level is not None and schema_keys
            else 0
        )

        # Ensure table exists
        if not self._duck._table_exists(table_name):
            col_defs = []
            for col in df.columns:
                dtype = df[col].dtype
                if pd.api.types.is_integer_dtype(dtype):
                    ddb_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    ddb_type = "DOUBLE"
                elif pd.api.types.is_bool_dtype(dtype):
                    ddb_type = "BOOLEAN"
                elif dtype == object:
                    first_val = next(
                        (
                            v
                            for v in df[col]
                            if v is not None
                            and not (isinstance(v, float) and np.isnan(v))
                        ),
                        None,
                    )
                    if isinstance(first_val, np.ndarray) and np.issubdtype(
                        first_val.dtype, np.number
                    ):
                        ddb_type = "DOUBLE[]"
                    elif (
                        isinstance(first_val, list)
                        and len(first_val) > 0
                        and all(isinstance(x, (int, float)) for x in first_val)
                    ):
                        ddb_type = "DOUBLE[]"
                    else:
                        ddb_type = "VARCHAR"
                else:
                    ddb_type = "VARCHAR"
                col_defs.append(f'"{col}" {ddb_type}')

            data_cols_sql = ", ".join(col_defs)
            self._duck._execute(f"""
                CREATE TABLE "{table_name}" (
                    record_id VARCHAR NOT NULL,
                    {data_cols_sql}
                )
            """)
            self._create_variable_view(variable_class)
            Log.debug(f"_save_columnar: created table '{table_name}'")

        # Only insert if this record_id doesn't already exist
        existing_count = self._duck._fetchall(
            f'SELECT COUNT(*) FROM "{table_name}" WHERE record_id = ?',
            [record_id],
        )[0][0]

        if existing_count == 0:
            insert_df = df.copy()
            insert_df.insert(0, "record_id", record_id)
            col_str = ", ".join(f'"{c}"' for c in insert_df.columns)
            self._duck.con.execute(
                f'INSERT INTO "{table_name}" ({col_str}) SELECT * FROM insert_df'
            )
            Log.debug(
                f"_save_columnar: inserted {len(df)} rows into '{table_name}', record_id={record_id[:12]}"
            )
        else:
            Log.debug(
                f"_save_columnar: record_id={record_id[:12]} already exists in '{table_name}', skipped"
            )

        # Upsert into _variables (one row per variable)
        effective_level = schema_level or self.dataset_schema_keys[-1]
        if dict_of_arrays:
            dtype_json = json.dumps(
                {
                    "custom": True,
                    "dict_of_arrays": True,
                    "ndarray_keys": ndarray_keys or {},
                }
            )
        elif struct_columns:
            dtype_json = json.dumps(
                {
                    "custom": True,
                    "struct_columns": struct_columns,
                }
            )
        else:
            dtype_json = json.dumps({"custom": True})
        self._duck._execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
            [variable_class.__name__, effective_level, dtype_json, ""],
        )

        return schema_id

    def _save_native(
        self,
        record_id: str,
        table_name: str,
        variable_class: type[BaseVariable],
        data: Any,
        content_hash: str,
        schema_level: str | None = None,
        schema_keys: dict | None = None,
    ) -> int:
        """
        Save native data as a single record using sciduck's type inference.

        Handles scalars, arrays, lists, dicts (flat & nested), and
        dict-of-arrays.  Each dict key becomes its own DuckDB column;
        vector values become DuckDB array types (e.g. DOUBLE[]).

        The table uses record_id as PRIMARY KEY so identical data is stored once.

        Returns schema_id.
        """
        if schema_level is not None and schema_keys:
            self._forget_schema_key_kinds()
            schema_id = self._duck._get_or_create_schema_id(
                schema_level, {k: schema_str(v) for k, v in schema_keys.items()}
            )
        else:
            schema_id = 0

        data_col_types, dtype_meta = infer_data_columns(data)
        is_dataframe = isinstance(data, pd.DataFrame)

        # Ensure table exists
        if not self._duck._table_exists(table_name):
            data_cols_sql = ", ".join(
                f'"{col}" {dtype}' for col, dtype in data_col_types.items()
            )
            if is_dataframe:
                # One DuckDB row per table row: record_id is not unique per row.
                record_id_col = "record_id VARCHAR NOT NULL"
            else:
                record_id_col = "record_id VARCHAR PRIMARY KEY"
            self._duck._execute(f'''
                CREATE TABLE "{table_name}" (
                    {record_id_col},
                    {data_cols_sql}
                )
            ''')
            self._create_variable_view(variable_class)
            Log.debug(f"_save_native: created table '{table_name}'")

        if is_dataframe:
            # Idempotency: skip all inserts if this record_id already exists.
            existing_count = self._duck._fetchall(
                f'SELECT COUNT(*) FROM "{table_name}" WHERE record_id = ?',
                [record_id],
            )[0][0]
            if existing_count == 0:
                col_names = ["record_id"] + list(data_col_types.keys())
                col_str = ", ".join(f'"{c}"' for c in col_names)
                placeholders = ", ".join(["?"] * len(col_names))
                for storage_row in dataframe_to_storage_rows(data, dtype_meta):
                    self._duck._execute(
                        f'INSERT INTO "{table_name}" ({col_str}) VALUES ({placeholders})',
                        [record_id] + storage_row,
                    )
                Log.debug(
                    f"_save_native: inserted {len(data)} rows (dataframe) into '{table_name}', record_id={record_id[:12]}"
                )
            else:
                Log.debug(
                    f"_save_native: record_id={record_id[:12]} already exists in '{table_name}', skipped"
                )
        else:
            storage_values = value_to_storage_row(data, dtype_meta)
            col_names = ["record_id"] + list(data_col_types.keys())
            col_str = ", ".join(f'"{c}"' for c in col_names)
            placeholders = ", ".join(["?"] * len(col_names))
            self._duck._execute(
                f'INSERT INTO "{table_name}" ({col_str}) VALUES ({placeholders}) '
                f"ON CONFLICT (record_id) DO NOTHING",
                [record_id] + storage_values,
            )
            Log.debug(
                f"_save_native: inserted single record into '{table_name}', record_id={record_id[:12]}"
            )

        # Upsert into _variables (one row per variable)
        effective_level = schema_level or self.dataset_schema_keys[-1]
        self._duck._execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
            [variable_class.__name__, effective_level, json.dumps(dtype_meta), ""],
        )

        return schema_id

    def save_batch(
        self,
        variable_class: type[BaseVariable],
        data_items: list[tuple[Any, dict]],
        profile: bool = False,
    ) -> list[str]:
        """
        Bulk-save a list of (data_value, metadata_dict) pairs for a single variable type.

        Amortizes setup work (registration, table creation) and batches SQL
        operations using DataFrame-based inserts for speed.

        Data is deduplicated by record_id (same content → same record_id → stored once).
        Every call appends a (record_id, timestamp) save-event row to _record_save.

        Args:
            variable_class: The BaseVariable subclass to save as
            data_items: List of (data_value, flat_metadata_dict) tuples
            profile: If True, print phase-by-phase timing summary

        Returns:
            List of record_ids for each saved item (in input order)
        """
        if not data_items:
            return []

        # Declared-numeric schema keys canonicalize at every DB entry point —
        # this covers the MATLAB bridge (save_batch_bridge) as well as the
        # Python for_each save path with one seam.
        if self.dataset_schema_key_types:
            data_items = [(d, self.canonicalize_metadata(m)) for d, m in data_items]

        timings = {}
        t0 = time.perf_counter()

        table_name = self._ensure_registered(variable_class)
        type_name = variable_class.__name__
        schema_version = variable_class.schema_version
        user_id = get_user_id()

        # --- One-time setup from first item ---
        first_data, first_meta = data_items[0]

        data_col_types, dtype_meta = infer_data_columns(first_data)
        is_dataframe = dtype_meta.get("mode") == "dataframe"

        # Reference-schema selection: a leading degenerate record (e.g. an
        # empty dict -> no columns) must not define the table schema. Fall back
        # to the first record that yields a usable (non-empty) schema. The
        # degenerate record itself is caught by the validation pass below.
        if not data_col_types and not is_dataframe:
            for _dv, _ in data_items:
                _ct, _dm = infer_data_columns(_dv)
                if _ct:
                    first_data = _dv
                    data_col_types, dtype_meta = _ct, _dm
                    is_dataframe = dtype_meta.get("mode") == "dataframe"
                    break

        # No usable schema anywhere (e.g. every record is an empty dict): there
        # is nothing to store. Skip the whole batch rather than create an
        # empty-column table or write contentless rows.
        if not data_col_types and not is_dataframe:
            Log.warn(
                f"[batch_save] {type_name}: no usable data schema in any of "
                f"{len(data_items)} record(s) (all empty); nothing saved."
            )
            return [None] * len(data_items)

        col_types_str = ", ".join(f"{c}: {t}" for c, t in data_col_types.items())
        Log.debug(
            f"save_batch({type_name}): {len(data_items)} items, "
            f"mode={dtype_meta.get('mode', 'single_column')}, "
            f"data: {_describe_data(first_data)}, "
            f"DuckDB columns: [{col_types_str}]"
        )

        # --- Validate each record against the reference schema ---
        # A single record whose value doesn't fit the batch's columns (empty
        # dict, missing/extra keys, or a scalar where the column stores a
        # vector, etc.) would abort the atomic batch insert and lose ALL saves
        # in the batch. Skip such records with a clear per-record warning and
        # persist the rest. record_ids is returned aligned to the ORIGINAL
        # data_items order with None in each skipped slot, so callers that zip
        # items with record_ids (e.g. foreach graph_records) stay aligned.
        #
        # DataFrame mode stores one DuckDB row per frame row with its own
        # empty-frame handling, so it is left unvalidated here to avoid
        # false-positive skips (e.g. an empty frame inferring VARCHAR columns).
        n_original = len(data_items)
        skipped_slots: dict[int, None] = {}  # orig_idx -> None (kept for clarity)
        if not is_dataframe:
            valid_items = []
            valid_orig_idx = []
            for _idx, (_dv, _fm) in enumerate(data_items):
                _rec_col_types, _ = infer_data_columns(_dv)
                _reason = record_schema_mismatch(data_col_types, _rec_col_types)
                if _reason is None:
                    valid_items.append((_dv, _fm))
                    valid_orig_idx.append(_idx)
                    continue
                skipped_slots[_idx] = None
                _meta_str = (
                    ", ".join(
                        f"{k}={v}"
                        for k, v in _fm.items()
                        if not str(k).startswith("__")
                    )
                    or f"item #{_idx}"
                )
                Log.warn(
                    f"[batch_save] {type_name}: SKIPPED record ({_meta_str}) — "
                    f"incompatible with batch schema and NOT saved: {_reason}"
                )
            if not valid_items:
                Log.warn(
                    f"[batch_save] {type_name}: all {n_original} record(s) "
                    f"incompatible with the batch schema; nothing saved."
                )
                return [None] * n_original
            data_items = valid_items
        else:
            valid_orig_idx = list(range(n_original))

        if not self._duck._table_exists(table_name):
            data_cols_sql = ", ".join(
                f'"{col}" {dtype}' for col, dtype in data_col_types.items()
            )
            if is_dataframe:
                record_id_col = "record_id VARCHAR NOT NULL"
            else:
                record_id_col = "record_id VARCHAR PRIMARY KEY"
            self._duck._execute(f'''
                CREATE TABLE "{table_name}" (
                    {record_id_col},
                    {data_cols_sql}
                )
            ''')
            self._create_variable_view(variable_class)

        timings["setup"] = time.perf_counter() - t0

        # --- Detect PyArrow fast path for batch insert ---
        # The Arrow path below indexes `data_val[col]` for each column name,
        # which only works when `data_val` is a dict (i.e. multi_column mode).
        # Single-column mode stores data_val as a bare ndarray/scalar, so
        # `data_val["value"]` would raise IndexError — exclude it here and
        # let it fall through to the generic value_to_storage_row path.
        _use_arrow = False
        pa = None
        if not is_dataframe and dtype_meta.get("mode") == "multi_column":
            try:
                import pyarrow as pa

                col_metas = dtype_meta.get("columns", {})
                _use_arrow = all(
                    m.get("python_type") == "ndarray" and m.get("ndim", 1) == 1
                    for m in col_metas.values()
                )
            except ImportError:
                pass
        if _use_arrow:
            _arrow_col_arrays: dict[str, list] = {col: [] for col in data_col_types}
            _arrow_record_ids: list[str] = []

        # --- Batch schema_id resolution ---
        t1 = time.perf_counter()
        all_nested = []
        unique_schema_combos = {}  # {combo_key: schema_keys_dict}
        for data_val, flat_meta in data_items:
            # Drop transient for_each bookkeeping that must not become a
            # version key: __branch_params (derived from the graph). The
            # record's producing invocation is a version key on purpose —
            # `__invocation_id`, stamped by the save path — so identical
            # content from different inputs, code or options is a different
            # record; the edges themselves travel typed on the GraphRecord.
            flat_meta_cleaned = {
                k: v for k, v in flat_meta.items() if k != "__branch_params"
            }
            nested = self._split_metadata(flat_meta_cleaned)
            all_nested.append(nested)
            schema_keys = nested.get("schema", {})
            schema_level = self._infer_schema_level(schema_keys)
            if schema_level is not None and schema_keys:
                key_tuple = tuple(
                    schema_str(schema_keys.get(k, ""))
                    for k in self.dataset_schema_keys
                    if k in schema_keys
                )
                combo_key = (schema_level, key_tuple)
                if combo_key not in unique_schema_combos:
                    unique_schema_combos[combo_key] = schema_keys

        timings["split_metadata"] = time.perf_counter() - t1

        # Resolve schema_ids for all unique combos (batch)
        t2 = time.perf_counter()
        self._forget_schema_key_kinds()
        schema_id_cache = self._duck.batch_get_or_create_schema_ids(
            {
                k: {col: schema_str(v) for col, v in vals.items()}
                for k, vals in unique_schema_combos.items()
            }
        )
        timings["schema_resolution"] = time.perf_counter() - t2

        # --- Per-row Python computation (no SQL) ---
        t4 = time.perf_counter()
        timestamp = datetime.now().isoformat()
        record_ids = []
        data_table_rows = []  # (record_id, ...data_cols)
        metadata_rows = []  # (rid,ts,type,schema_id,content_hash,sv,user) → _record_save + _record

        t4_hash = 0.0
        # Which DataFrame serialization path the hashes took (F30): "fast"
        # skips pandas' per-column Series; "reference" is the public-API path
        # (unaddressable frames, or a signature whose fast path disagreed).
        _frames_before = frame_path_counts()
        t4_record_id = 0.0
        t4_storage = 0.0
        t4_meta = 0.0

        # Accumulate DataFrames for bulk storage-row conversion after the loop.
        # Per-row dataframe_to_storage_rows (54 iloc calls × 7k records) was
        # the dominant cost; bulk_df_to_storage_rows processes column-by-column.
        _df_bulk: list = []
        _df_bulk_rids: list = []

        for i, (data_val, flat_meta) in enumerate(data_items):
            nested = all_nested[i]
            schema_keys = nested.get("schema", {})
            schema_level = self._infer_schema_level(schema_keys)

            if schema_level is not None and schema_keys:
                key_tuple = tuple(
                    schema_str(schema_keys.get(k, ""))
                    for k in self.dataset_schema_keys
                    if k in schema_keys
                )
                schema_id = schema_id_cache[(schema_level, key_tuple)]
            else:
                schema_id = 0

            _t = time.perf_counter()
            content_hash = canonical_hash(data_val)
            t4_hash += time.perf_counter() - _t

            # Diagnostic (first record of the batch only): surface what is being
            # content-hashed. A DataFrame whose `to_numpy()` dtype is `object`
            # (mixed-type columns) was the source of non-deterministic hashes —
            # object arrays hash Python pointer bytes — so this line makes the
            # culprit (object dtype, column order, index) observable.
            if i == 0:
                try:
                    import pandas as _pd_diag

                    if isinstance(data_val, _pd_diag.DataFrame):
                        Log.debug(
                            f"[content_hash] {type_name}: first-record input is "
                            f"DataFrame columns={list(data_val.columns)} "
                            f"dtypes={[str(d) for d in data_val.dtypes]} "
                            f"to_numpy_dtype={data_val.to_numpy().dtype} "
                            f"index_head={list(data_val.index)[:5]} -> "
                            f"content_hash={content_hash}"
                        )
                    else:
                        Log.debug(
                            f"[content_hash] {type_name}: first-record input "
                            f"type={type(data_val).__name__} -> "
                            f"content_hash={content_hash}"
                        )
                except Exception:
                    pass

            _t = time.perf_counter()
            record_id = generate_record_id(
                class_name=type_name,
                schema_version=schema_version,
                content_hash=content_hash,
                metadata=nested,
            )
            record_ids.append(record_id)
            t4_record_id += time.perf_counter() - _t

            # DataFrames: defer storage-row conversion to the bulk path below.
            _t = time.perf_counter()
            if _use_arrow:
                _arrow_record_ids.append(record_id)
                for col in data_col_types:
                    _arrow_col_arrays[col].append(data_val[col])
            elif is_dataframe:
                _df_bulk.append(data_val)
                _df_bulk_rids.append(record_id)
            else:
                storage_values = value_to_storage_row(data_val, dtype_meta)
                data_table_rows.append((record_id,) + tuple(storage_values))
            t4_storage += time.perf_counter() - _t

            _t = time.perf_counter()
            metadata_rows.append(
                (
                    record_id,
                    timestamp,
                    type_name,
                    schema_id,
                    content_hash,
                    schema_version,
                    user_id,
                )
            )
            t4_meta += time.perf_counter() - _t

        # --- Bulk DataFrame → storage rows (replaces 7k per-row iloc calls) ---
        if _df_bulk:
            _t = time.perf_counter()
            data_table_rows = bulk_df_to_storage_rows(
                _df_bulk, _df_bulk_rids, dtype_meta
            )
            t4_storage += time.perf_counter() - _t

        timings["per_row_hashing"] = time.perf_counter() - t4
        timings["canonical_hash"] = t4_hash
        _frames_after = frame_path_counts()
        _hash_paths = (
            f"hash frames fast={_frames_after['fast'] - _frames_before['fast']} "
            f"reference={_frames_after['reference'] - _frames_before['reference']}"
        )
        timings["record_id"] = t4_record_id
        timings["storage_row"] = t4_storage
        timings["meta_row"] = t4_meta

        # --- Find which data rows are new (dedup check) ---
        t5 = time.perf_counter()
        if _use_arrow:
            # Arrow path: ON CONFLICT DO NOTHING handles dedup in the INSERT.
            new_data_rows = _arrow_record_ids  # for count only
        elif is_dataframe:
            # No PRIMARY KEY: filter out rows whose record_id already exists.
            all_new_rids = list({row[0] for row in data_table_rows})
            if all_new_rids:
                placeholders_rids = ", ".join(["?"] * len(all_new_rids))
                existing_rids = {
                    r[0]
                    for r in self._duck._fetchall(
                        f'SELECT DISTINCT record_id FROM "{table_name}" '
                        f"WHERE record_id IN ({placeholders_rids})",
                        all_new_rids,
                    )
                }
            else:
                existing_rids = set()
            new_data_rows = [
                row for row in data_table_rows if row[0] not in existing_rids
            ]
        else:
            # PRIMARY KEY: ON CONFLICT DO NOTHING handles dedup in the INSERT.
            new_data_rows = data_table_rows

        timings["dedup_check"] = time.perf_counter() - t5

        # --- Batch inserts ---
        t6 = time.perf_counter()
        self._duck._begin()
        try:
            t6a = time.perf_counter()
            if _use_arrow and _arrow_record_ids:
                # PyArrow fast path: numpy arrays → Arrow buffers → DuckDB (no Python list conversion)
                _NUMPY_TO_ARROW = {
                    "float64": pa.float64(),
                    "float32": pa.float32(),
                    "int64": pa.int64(),
                    "int32": pa.int32(),
                    "int16": pa.int16(),
                    "int8": pa.int8(),
                    "uint64": pa.uint64(),
                    "uint32": pa.uint32(),
                    "uint16": pa.uint16(),
                    "uint8": pa.uint8(),
                    "bool": pa.bool_(),
                }
                arrow_data = {
                    "record_id": pa.array(_arrow_record_ids, type=pa.string())
                }
                for col_name, np_list in _arrow_col_arrays.items():
                    col_meta = dtype_meta["columns"][col_name]
                    numpy_dtype = col_meta.get("numpy_dtype", "float64")
                    arrow_inner = _NUMPY_TO_ARROW.get(numpy_dtype, pa.float64())
                    arrow_data[col_name] = pa.array(np_list, type=pa.list_(arrow_inner))
                arrow_table = pa.table(arrow_data)
                all_columns = list(arrow_data.keys())
                col_str = ", ".join(f'"{c}"' for c in all_columns)
                timings["data_df_create"] = time.perf_counter() - t6a

                t6b = time.perf_counter()
                self._duck.con.register("arrow_table", arrow_table)
                try:
                    self._duck.con.execute(
                        f'INSERT INTO "{table_name}" ({col_str}) SELECT * FROM arrow_table '
                        f"ON CONFLICT (record_id) DO NOTHING"
                    )
                finally:
                    self._duck.con.unregister("arrow_table")
                timings["data_insert"] = time.perf_counter() - t6b
            elif new_data_rows:
                all_columns = ["record_id"] + list(data_col_types.keys())
                timings["data_df_create"] = time.perf_counter() - t6a

                t6b = time.perf_counter()
                if is_dataframe:
                    self._duck._bulk_insert(table_name, all_columns, new_data_rows)
                else:
                    self._duck._bulk_insert(
                        table_name,
                        all_columns,
                        new_data_rows,
                        conflict_cols=["record_id"],
                    )
                timings["data_insert"] = time.perf_counter() - t6b
            else:
                timings["data_df_create"] = time.perf_counter() - t6a
                timings["data_insert"] = 0.0

            # Append save-event rows (audit trail — every execution logged).
            # metadata_rows is (record_id, timestamp, variable_name, schema_id,
            # content_hash, schema_version, user_id); _record_save keeps only the
            # (record_id, timestamp, user_id) save-event columns.
            t6c = time.perf_counter()
            self._duck._bulk_insert(
                "_record_save",
                ["record_id", "timestamp", "user_id"],
                [(r[0], r[1], r[6]) for r in metadata_rows],
                conflict_cols=["record_id", "timestamp"],
            )
            timings["record_save_insert"] = time.perf_counter() - t6c
            # The type/schema/content metadata lives on the bipartite entities table
            # (_record); map metadata_rows to the _record column order.
            t6c2 = time.perf_counter()
            from .provenance import insert_record_entities

            insert_record_entities(
                self._duck,
                [(r[0], r[1], r[2], r[3], r[4], r[5], False) for r in metadata_rows],
            )
            timings["record_entities_insert"] = time.perf_counter() - t6c2
            timings["meta_insert"] = time.perf_counter() - t6c

            # Upsert _variables (one row per variable)
            t6d = time.perf_counter()
            effective_level = (
                self._infer_schema_level(all_nested[0].get("schema", {}))
                or self.dataset_schema_keys[-1]
            )
            self._duck._execute(
                "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
                [type_name, effective_level, json.dumps(dtype_meta), ""],
            )
            timings["variables_upsert"] = time.perf_counter() - t6d

            t6e = time.perf_counter()
            self._duck._commit()
            timings["commit"] = time.perf_counter() - t6e
        except Exception:
            try:
                self._duck._execute("ROLLBACK")
            except Exception:
                pass
            raise

        timings["batch_inserts"] = time.perf_counter() - t6
        timings["total"] = time.perf_counter() - t0

        n = len(data_items)
        n_new = len(new_data_rows)
        n_total_rows = len(_arrow_record_ids) if _use_arrow else len(data_table_rows)
        # Phase breakdown at INFO, not DEBUG. This path is measured in tens of
        # seconds on payload-heavy batches (77.3s for 419 RawEMG records,
        # 2026-09-13) and a single opaque total gives no way to tell hashing
        # from inserts from the commit. ~18 phases is far too many for one
        # readable line, so the INFO line carries the costliest few and every
        # phase still gets its DEBUG line.
        Log.timings(
            f"save_batch({type_name})",
            timings,
            extra=(
                f"{n} items ({n_new} new rows, {n_total_rows} total storage rows), "
                f"{len(unique_schema_combos)} schemas, {_hash_paths}"
            ),
            top=6,
        )

        if profile:
            print(
                f"\n--- save_batch() profile ({n} items, "
                f"{len(unique_schema_combos)} unique schemas) ---"
            )
            for phase, elapsed in timings.items():
                print(f"  {phase:30s} {elapsed:8.3f}s")
            print()

        # record_ids is aligned with the (possibly filtered) data_items. Remap
        # to the ORIGINAL input order, leaving None for records skipped by the
        # schema-validation pass, so callers can zip items with record_ids.
        if skipped_slots:
            aligned = [None] * n_original
            for _pos, _orig_idx in enumerate(valid_orig_idx):
                aligned[_orig_idx] = record_ids[_pos]
            return aligned

        return record_ids

    @staticmethod
    def _has_custom_serialization(variable_class: type) -> bool:
        """Check if a BaseVariable subclass overrides to_db or from_db."""
        return (
            "to_db" in variable_class.__dict__ or "from_db" in variable_class.__dict__
        )

    @property
    def dataset_schema_key_order(self) -> dict[str, list[str]]:
        """``{schema key: [levels]}`` from the project's ``[schema_keys]``, LIVE.

        Read through :func:`scidb.schema_order.project_level_order` on every
        access rather than once at open: this used to be a snapshot, so an
        edit to scistack.toml changed no table and no figure until the GUI (or
        the session) was restarted. The reader caches the config's location
        for a couple of seconds and its content on the file's mtime, so an
        access is a ``stat``.

        Each time the declaration CHANGES it is validated against this
        dataset's keys, so a typo is reported when it is written, not just
        when the database opens.
        """
        from . import schema_order as _schema_order

        order = _schema_order.project_level_order()
        if order is not self._validated_key_order:
            self._validated_key_order = order
            if order:
                _schema_order.validate(order, self.dataset_schema_keys)
        return order

    @property
    def dataset_aliases(self) -> "dict[str, dict]":
        """``{thing: {"name": …, "levels": {…}}}`` from the project's
        ``[aliases]``, LIVE — the display names every plot starts from.

        Same discipline as :attr:`dataset_schema_key_order`: read through
        :func:`scidb.aliases.project_aliases` on every access (a ``stat``, the
        parse is cached on mtime), and validated against this dataset's schema
        keys and variables each time the content CHANGES, so a typo is
        reported when it is written. Display only: nothing in scidb reads it.
        """
        from . import aliases as _aliases

        table = _aliases.project_aliases()
        if table is not getattr(self, "_validated_aliases", None):
            self._validated_aliases = table
            if table:
                try:
                    variables = [
                        str(name) for name in self.list_variables()["variable_name"]
                    ]
                except Exception as exc:  # a closed or half-open database
                    Log.debug(f"[aliases] variable names unavailable for validation: {exc}")
                    variables = []
                _aliases.validate(
                    table, schema_keys=self.dataset_schema_keys, variables=variables
                )
        return table

    def _sort_by_schema_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort DataFrame by schema keys with numeric sorting for numeric-only columns.

        For each schema key:
        - If all values are purely numeric (convertible to float), sort numerically
        - Otherwise, sort alphabetically (lexicographically)

        This ensures "1", "2", "10" sorts as 1, 2, 10 (not "1", "10", "2").
        But "S01", "S02", "S10" or mixed "1", "S01", "10" sorts alphabetically.
        """
        if len(df) == 0:
            return df

        # Create temporary sort columns for each schema key
        sort_cols = []
        sort_ascending = []
        temp_col_names = []
        # Asked once per sort: the property re-reads the project config.
        key_order = self.dataset_schema_key_order or {}

        for key in self.dataset_schema_keys:
            if key not in df.columns:
                continue

            temp_col_name = f"__sort_{key}"
            temp_col_names.append(temp_col_name)
            col = df[key]

            # A DECLARED order wins outright: `[schema_keys] session = ["BL",
            # "POST", "FU"]` is the user saying these levels are chronological,
            # not alphabetical, and no amount of value inspection can work that
            # out. Undeclared levels rank after every declared one and then
            # sort among themselves by the rules below — which is also what
            # every other consumer of the declaration does (scidb.schema_order).
            declared = key_order.get(key)
            if declared:
                df[temp_col_name] = _declared_rank_column(col, declared)
                sort_cols.append(temp_col_name)
                sort_ascending.append(True)
                continue

            # Get non-null values to check if column is numeric-only
            non_null_mask = col.notna()
            non_null_values = col[non_null_mask]

            # Check if all non-null values are numeric (can be converted to float)
            is_numeric = True
            if len(non_null_values) > 0:
                for val in non_null_values:
                    try:
                        float(str(val))
                    except (ValueError, TypeError):
                        is_numeric = False
                        break

            if is_numeric and len(non_null_values) > 0:
                # Numeric-only: convert to float for numeric sorting
                df[temp_col_name] = pd.to_numeric(col, errors="coerce")
            else:
                # Contains non-numeric: use string sorting
                df[temp_col_name] = col.astype(str)

            sort_cols.append(temp_col_name)
            sort_ascending.append(True)

        # Add timestamp as final tiebreaker (descending)
        if "timestamp" in df.columns:
            sort_cols.append("timestamp")
            sort_ascending.append(False)

        # Sort by all columns
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=sort_ascending)

        # Drop temporary sort columns
        df = df.drop(columns=temp_col_names, errors="ignore")

        return df

    def _any_records_exist(self, type_name: str) -> bool:
        """Return True if any records of this type have ever been saved."""
        rows = self._duck._fetchall(
            "SELECT 1 FROM _record WHERE type = ? LIMIT 1",
            [type_name],
        )
        return len(rows) > 0

    def _find_record(
        self,
        type_name: str,
        record_id: str | None = None,
        nested_metadata: dict | None = None,
        version_id: str = "all",
        branch_params_filter: dict | None = None,
        include_excluded: bool = False,
    ) -> pd.DataFrame:
        """
        Query the _record_save log (joined to _record) to find matching records.

        Supports two modes:
        - By record_id: direct primary key lookup (with JOINs for full row data)
        - By metadata: filter by schema keys via JOIN with _schema, then match
          non-schema metadata + branch_params against the bipartite graph,
          order by timestamp DESC

        version_id controls which versions are returned:
        - "all" (default): no version filtering (return every version)
        - "latest": only the latest row per (variable_name, schema_id, variant),
          where the variant is graph-derived (producing function + accumulated
          constants)
        - any other string: treated as a record_id for direct lookup

        branch_params_filter: optional dict of branch_params key/value filters
        include_excluded: if False (default), skip records with excluded=TRUE

        Schema key values and filter values may be lists, interpreted as
        "match any" (SQL IN / Python in).

        Returns a DataFrame of matching rows including schema columns.
        """
        # Build schema column SELECT list
        schema_col_select = ", ".join(f's."{col}"' for col in self.dataset_schema_keys)

        # The save-event log (_record_save, one row per (record_id, timestamp))
        # joined to the _record entity (one row per record_id) for the
        # type/schema/content/excluded columns the log no longer stores. Aliased
        # back to the legacy column names so downstream row access is unchanged.
        meta_select = (
            "rm.record_id, rm.timestamp, rm.user_id, "
            "r.type AS variable_name, r.schema_id, r.content_hash, "
            "r.schema_version, r.excluded"
        )
        meta_from = (
            "FROM _record_save rm "
            "JOIN _record r ON r.record_id = rm.record_id "
            "LEFT JOIN _schema s ON r.schema_id = s.schema_id "
        )

        excluded_clause = (
            "" if include_excluded else " AND COALESCE(r.excluded, FALSE) = FALSE"
        )

        if record_id is not None:
            sql = (
                f"SELECT {meta_select}, {schema_col_select} "
                f"{meta_from}"
                f"WHERE rm.record_id = ? AND r.type = ?{excluded_clause}"
            )
            df = self._duck._fetchdf(sql, [record_id, type_name])
            return self._sort_by_schema_keys(df)

        # Unrecognized version_id → treat as record_id lookup
        if version_id not in ("latest", "all"):
            Log.debug(
                f"_find_record({type_name}): treating version_id={version_id!r} as record_id"
            )
            sql = (
                f"SELECT {meta_select}, {schema_col_select} "
                f"{meta_from}"
                f"WHERE rm.record_id = ? AND r.type = ?{excluded_clause}"
            )
            df = self._duck._fetchdf(sql, [version_id, type_name])
            return self._sort_by_schema_keys(df)

        # By metadata
        schema_keys = nested_metadata.get("schema", {}) if nested_metadata else {}
        version_keys = nested_metadata.get("version", {}) if nested_metadata else {}

        conditions = ["r.type = ?"]
        params: list[Any] = [type_name]

        # Exclude excluded variants by default
        if not include_excluded:
            conditions.append("COALESCE(r.excluded, FALSE) = FALSE")

        # Filter schema keys via _schema columns in SQL (lists → IN)
        for key, value in schema_keys.items():
            if isinstance(value, (list, tuple)):
                placeholders = ", ".join(["?"] * len(value))
                conditions.append(f's."{key}" IN ({placeholders})')
                params.extend([schema_str(v) for v in value])
            else:
                conditions.append(f's."{key}" = ?')
                params.append(schema_str(value))

        # branch_params_filter is handled entirely in Python (lines below).
        # We do NOT push it to SQL because:
        # 1. Keys stored in version_keys (direct user saves) would be incorrectly
        #    excluded by a branch_params SQL filter before the Python fallback runs.
        # 2. Suffix matching (e.g. "low_hz" → "bandpass.low_hz") cannot be expressed
        #    in SQL without fetching all records anyway.
        # The Python _match_row already checks version_keys first, then branch_params.

        where = " AND ".join(conditions)

        # One row per distinct record_id at the SQL level (dedups audit re-saves
        # of identical content). For version_id="latest" the *variant* collapse —
        # keeping the newest record per (fn, accumulated constants) — is done in
        # Python below using the bipartite graph, so neither version_keys nor the
        # branch_params column is read here.
        partition = "rm.record_id"

        sql = (
            f"WITH ranked AS ("
            f"SELECT {meta_select}, {schema_col_select}, "
            f"ROW_NUMBER() OVER ("
            f"PARTITION BY {partition} "
            f"ORDER BY rm.timestamp DESC"
            f") as rn "
            f"{meta_from}"
            f"WHERE {where}"
            f") SELECT * FROM ranked WHERE rn = 1"
        )
        _t_sql = time.perf_counter()
        df = self._duck._fetchdf(sql, params)
        t_sql = time.perf_counter() - _t_sql
        Log.debug(
            f"_find_record({type_name}): SQL returned {len(df)} records, version_id={version_id}"
        )

        # Collapse to the latest record per *variant* when version_id="latest".
        # Variant identity is derived from the bipartite graph: the producing
        # function + the accumulated upstream constants (§6) + the consumed input
        # *schema locations*. Records that differ only by which upstream record_id
        # was consumed (i.e. input data was re-saved at the same location) share a
        # variant and collapse to the newest. Distinct constant-variants
        # (low_hz=20 vs 30) — and records computed from genuinely different input
        # locations at the same output schema_id (cross-level where=) — keep
        # separate identities.
        # Raw / manually saved records have no producing invocation; any non-schema
        # save kwargs are anchored on a synthetic __save__ invocation, so those take
        # the graph branch and only true raw saves fall into the "__raw__" variant.
        t_collapse = 0.0
        rows_before_collapse = len(df)
        if version_id == "latest" and len(df) > 0:
            _t_collapse = time.perf_counter()
            from collections import defaultdict

            # Consumed input *schema locations* per record (schema-edit-stable:
            # a re-save keeps the same schema_id, a genuinely different input
            # location changes it). Part of the variant key so two for_each runs
            # that emit output at the SAME output schema_id but consumed DIFFERENT
            # input locations (e.g. where=Side=='L' vs =='R' producing one
            # subject-level record each) stay distinct instead of collapsing to
            # the newest — which would hide the other from where=-based load.
            all_rids = df["record_id"].tolist()
            consumed_map = provenance_query.consumed_input_schema_ids(
                self._duck, all_rids
            )
            # Batched provenance: one closure build instead of 3 per-row queries
            # (each previously walking the ancestry) — the collapse hot path.
            inv_map = provenance_query.producing_invocation_batch(self._duck, all_rids)
            bp_map = provenance_query.branch_params_batch(self._duck, all_rids)
            onum_map = provenance_query.output_num_batch(self._duck, all_rids)
            run_map = provenance_query.invocation_run_options_batch(
                self._duck, [inv[0] for inv in inv_map.values()]
            )
            groups = defaultdict(list)
            # Supersession FAMILIES: same (variable, schema_id, fn, constants,
            # consumed inputs), i.e. everything in the variant key EXCEPT
            # output_num, bucketed by the producing invocation's run options.
            # Rationale below, at the family collapse.
            families: dict = defaultdict(lambda: defaultdict(list))
            for row in df.itertuples(index=True):
                inv = inv_map.get(row.record_id)
                if inv is not None:
                    bp = bp_map.get(row.record_id, {})
                    # output_num is deliberately NOT part of the key. It used
                    # to be, to keep "distinct outputs of one call" apart -- but
                    # those sit at distinct schema_ids (a distribute piece per
                    # location) or under distinct variables (outputs=[A, B]),
                    # and the group key below holds both already. What it DID
                    # split was re-saves: a batch loader shares one invocation
                    # across runs and a re-run's record takes the next free
                    # slot (provenance_save, Fix B), so the record for one
                    # changed file was output #34 on the first run and #60 on
                    # the second -- two "latest" records at one location
                    # (integration suite, 2026-09-19, plan D3). Two records of
                    # one family at one location can only be one superseding
                    # the other, and the newest wins below.
                    consumed = tuple(sorted(consumed_map.get(row.record_id, ())))
                    bp_json = variant_signature(bp)
                    variant_key = (
                        inv[1],
                        bp_json,
                        consumed,
                    )
                    family_key = (row.variable_name, row.schema_id, inv[1], bp_json, consumed)
                    families[family_key][run_map.get(inv[0])].append(
                        (row.timestamp, row.Index)
                    )
                else:
                    # No producing invocation → a plain raw save. (Any non-schema
                    # save kwargs are anchored on a synthetic __save__ invocation,
                    # so those take the graph branch above.) All such records at a
                    # (variable, schema) location are one variant → collapse to newest.
                    variant_key = ("__raw__", None)
                group_key = (row.variable_name, row.schema_id, variant_key)
                groups[group_key].append((row.timestamp, row.Index))

            # --- Run-option supersession (2026-09-14) ---
            # distribute/as_table are folded into invocation_id, so re-running
            # unchanged code at unchanged constants under a different flag is a
            # DIFFERENT invocation that writes a second record to the same
            # location. Those two records never shared a variant key, because a
            # distributed run's output_num is the slice index: at most one trial
            # could coincide with the non-distributed record's output_num, and
            # every other trial kept both (`scidb show GAITRiteLoaded ...` listed
            # two "latest" records per trial). So within a family holding
            # records from more than one run-option set, only the set holding
            # the most recently saved record survives; the per-output_num rule
            # below then applies within it. A family with ONE option set — the
            # ordinary case, and every genuine multi-output invocation — is
            # untouched. Raw saves have no family.
            superseded_idx: set = set()
            superseded_families = 0
            for family_key, by_run in families.items():
                if len(by_run) < 2:
                    continue
                newest_run = max(by_run, key=lambda run: max(by_run[run]))
                superseded_families += 1
                dropped = 0
                for run, members in by_run.items():
                    if run != newest_run:
                        superseded_idx.update(idx for _ts, idx in members)
                        dropped += len(members)
                Log.debug(
                    f"_find_record({type_name}, latest): {family_key[0]} at "
                    f"schema_id={family_key[1]} was run under {sorted(by_run, key=str)} — "
                    f"keeping {newest_run!r}, superseding {dropped} "
                    f"record(s) from the other option set(s)"
                )
            # Second, GLOBAL rule: a record whose producing invocation ran under
            # an option set other than the one its function was most recently
            # run under is stale even where it is the only record — the family
            # rule above cannot see a location the newer run never produced (a
            # trial that only existed under the old option set).
            #
            # ONE OWNER since 2026-09-22: node state needs exactly this test and
            # had its own answer (none), which is how a completed step could
            # read red forever. The rule now lives in
            # provenance_query.run_option_superseded_records and both callers
            # ask it. The maps are handed over rather than rebuilt — this is the
            # collapse hot path.
            stale_rids = provenance_query.run_option_superseded_records(
                self._duck, all_rids, inv_map=inv_map, run_map=run_map
            )
            if stale_rids:
                # record_id → row indices: `df` is the save LOG, so one record
                # can hold several rows and every one of them is superseded.
                stale_global = 0
                for row in df.itertuples(index=True):
                    if row.Index in superseded_idx:
                        continue
                    if row.record_id in stale_rids:
                        superseded_idx.add(row.Index)
                        stale_global += 1
                if stale_global:
                    Log.info(
                        f"_find_record({type_name}, latest): {stale_global} row(s) "
                        f"built under a superseded run-option set dropped"
                    )
            if superseded_idx:
                Log.info(
                    f"_find_record({type_name}, latest): run-option supersession "
                    f"dropped {len(superseded_idx)} record(s) across "
                    f"{superseded_families} location(s) — an older "
                    f"distribute/as_table run of the same code and constants"
                )
                groups = {
                    key: [m for m in members if m[1] not in superseded_idx]
                    for key, members in groups.items()
                }
                groups = {key: members for key, members in groups.items() if members}

            # Keep only the latest record per variant group
            keep_indices = [max(group)[1] for group in groups.values()]
            collapsed_count = len(df) - len(keep_indices)
            if collapsed_count > 0:
                Log.debug(
                    f"_find_record: collapsed {collapsed_count} non-latest variant row(s)"
                )
            df = df.loc[keep_indices]

            # --- Diagnostic: why do some locations still have >1 record? ---
            # After "latest per (variable, schema_id, variant)" collapse, a schema
            # KEY location should normally map to ONE record. If it doesn't, the
            # survivors differ either by schema_id (two schema_ids allocated for
            # identical key values) or by variant_key (same schema_id but distinct
            # producing fn / branch_params / output_num / consumed input locations).
            # The branch is vectorized and only does work when duplicates remain.
            _sk = self.dataset_schema_keys
            if _sk and len(df) > 1:
                _dupmask = df.duplicated(subset=_sk, keep=False)
                if _dupmask.any():
                    _dup = df[_dupmask]
                    # dropna=False: a record saved above the deepest schema key
                    # has NULL there (a trial-level record in a schema that ends
                    # in cycle), and pandas drops NULL-keyed groups by default —
                    # which made this WARN say "0 location(s) ... Examples:"
                    # with nothing after it, for exactly the rows it exists to
                    # describe (2026-09-14, GAITRiteLoaded).
                    _n_locs = _dup.groupby(_sk, sort=False, dropna=False).ngroups
                    _samples = []
                    for _vals, _g in _dup.groupby(_sk, sort=False, dropna=False):
                        if len(_samples) >= 3:
                            break
                        _sids = sorted(set(_g["schema_id"].tolist()))
                        _rids = _g["record_id"].tolist()
                        if len(_sids) > 1:
                            _kind = f"DISTINCT schema_id for identical keys {_sids}"
                        else:
                            _vks = []
                            for _r in _rids:
                                _inv = inv_map.get(_r)
                                if _inv is not None:
                                    _vks.append(
                                        (
                                            _inv[1],
                                            json.dumps(
                                                bp_map.get(_r, {}), sort_keys=True
                                            ),
                                            onum_map.get(_r),
                                            tuple(sorted(consumed_map.get(_r, ()))),
                                        )
                                    )
                                else:
                                    _vks.append(("__raw__", None))
                            _kind = f"same schema_id {_sids[0]}, DISTINCT variant_keys={_vks}"
                        _loc = dict(
                            zip(
                                _sk,
                                _vals if isinstance(_vals, tuple) else (_vals,),
                                strict=False,
                            )
                        )
                        _samples.append(
                            f"{_loc}: {len(_g)} records, record_ids={_rids[:4]} -> {_kind}"
                        )
                    Log.warn(
                        f"_find_record({type_name}, latest): {_n_locs} schema "
                        f"location(s) retain >1 record after collapse — these feed "
                        f"multi-row per-combo tables. Examples: " + "; ".join(_samples)
                    )

            # Apply smart sorting by schema keys (numeric or alphabetic per column)
            df = self._sort_by_schema_keys(df)
            t_collapse = time.perf_counter() - _t_collapse

        t_vk_filter = 0.0

        # Filter by non-schema metadata. Both the "version" portion of the
        # metadata (non-schema kwargs passed plainly, e.g. find_record_id(C,
        # {subject:1, low_hz:20})) and an explicit ``branch_params_filter`` are
        # matched against graph-derived branch params (constants on the producing
        # invocation — incl. direct-save kwargs via the synthetic __save__
        # invocation), with bare-name suffix matching.
        combined_bp = {**version_keys, **(branch_params_filter or {})}
        t_bp_filter = 0.0
        if combined_bp and len(df) > 0:
            _t_bp = time.perf_counter()
            df = _filter_records_by_branch_params(df, combined_bp, self._duck)
            t_bp_filter = time.perf_counter() - _t_bp

        # Apply smart sorting by schema keys before returning
        _t_sort = time.perf_counter()
        result = self._sort_by_schema_keys(df)
        t_sort = time.perf_counter() - _t_sort

        total = t_sql + t_collapse + t_vk_filter + t_bp_filter + t_sort
        Log.info(
            f"[timing] _find_record({type_name}, version_id={version_id!r}): "
            f"total={total:.3f}s, sql={t_sql:.3f}s ({rows_before_collapse} rows), "
            f"collapse={t_collapse:.3f}s, vk_filter={t_vk_filter:.3f}s, "
            f"bp_filter={t_bp_filter:.3f}s, sort={t_sort:.3f}s, "
            f"returned={len(result)} rows"
        )
        return result

    def _reconstruct_metadata_from_row(
        self, row: pd.Series, branch_params: "dict | None" = None
    ) -> tuple[dict, dict]:
        """
        Reconstruct flat and nested metadata for a record.

        Schema keys come from the JOINed _schema columns; the non-schema
        "version" portion is the record's direct-save kwargs, derived from the
        bipartite graph (the synthetic ``__save__`` invocation's constants —
        see :func:`provenance_query.derived_branch_params`). Internal pipeline
        markers (``__fn`` etc.) are not user metadata and are not exposed.

        *branch_params* is the record's already-derived branch params, for a
        caller that batched them (``provenance_query.branch_params_batch``);
        without it this walks the record's ancestry itself, one record at a
        time — the N+1 a loop over rows must not pay (cleanup-audit F3).

        Returns (flat_metadata, nested_metadata).
        """
        schema = {}
        for key in self.dataset_schema_keys:
            if key in row.index:
                val = row[key]
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    schema[key] = self.restore_schema_value(key, val)

        # Direct-save kwargs: __save__.<kwarg> entries in derived branch params.
        version = {}
        rid = row.get("record_id")
        if branch_params is None and rid is not None:
            branch_params = provenance_query.derived_branch_params(self._duck, rid)
        for k, v in (branch_params or {}).items():
            if k.startswith("__save__."):
                version[k[len("__save__.") :]] = v

        nested_metadata = {"schema": schema, "version": version}
        flat_metadata = {}
        flat_metadata.update(schema)
        flat_metadata.update(version)
        return flat_metadata, nested_metadata

    def _deserialize_custom_subdf(
        self,
        variable_class: type[BaseVariable],
        sub_df: pd.DataFrame,
        dtype_meta: dict,
    ):
        """Deserialize a sub-DataFrame using custom dtype metadata.

        Handles four sub-paths based on dtype_meta flags:
        - dict_of_arrays: reconstruct dict of numpy arrays
        - from_db: class-level custom deserialization
        - struct_columns: unflatten dot-separated columns
        - raw: return DataFrame as-is

        The sub_df must already have internal columns
        (record_id) dropped.
        """
        if dtype_meta.get("dict_of_arrays"):
            ndarray_keys = dtype_meta.get("ndarray_keys", {})
            data = {}
            for col in sub_df.columns:
                arr = sub_df[col].values
                if col in ndarray_keys:
                    col_meta = ndarray_keys[col]
                    arr = arr.astype(np.dtype(col_meta["dtype"]))
                    orig_shape = col_meta.get("shape")
                    if orig_shape and len(orig_shape) == 2:
                        if orig_shape[0] == 1:
                            arr = arr.reshape(1, -1)
                        elif orig_shape[1] == 1:
                            arr = arr.reshape(-1, 1)
                        else:
                            try:
                                arr = arr.reshape(orig_shape)
                            except ValueError:
                                pass
                data[col] = arr
            return data
        elif self._has_custom_serialization(variable_class):
            return variable_class.from_db(sub_df)
        elif dtype_meta.get("struct_columns"):
            return _unflatten_struct_columns(sub_df, dtype_meta["struct_columns"])
        else:
            return sub_df

    def register(self, variable_class: type[BaseVariable]) -> None:
        """
        Register a variable type for storage.

        Args:
            variable_class: The BaseVariable subclass to register
        """
        type_name = variable_class.__name__
        table_name = variable_class.table_name()
        schema_version = variable_class.schema_version

        # Register in metadata table (skip if already registered)
        existing = self._duck._fetchall(
            "SELECT 1 FROM _registered_types WHERE type_name = ?",
            [type_name],
        )
        if not existing:
            self._duck._execute(
                """
                INSERT INTO _registered_types (type_name, table_name, schema_version)
                VALUES (?, ?, ?)
                """,
                [type_name, table_name, schema_version],
            )

        # Cache locally
        self._registered_types[type_name] = variable_class

    def _ensure_registered(
        self, variable_class: type[BaseVariable], auto_register: bool = True
    ) -> str:
        """
        Ensure a variable type is registered.

        Returns:
            The table name for this variable type
        """
        type_name = variable_class.__name__

        if type_name in self._registered_types:
            return variable_class.table_name()

        # Check database
        rows = self._duck._fetchall(
            "SELECT table_name FROM _registered_types WHERE type_name = ?",
            [type_name],
        )

        if not rows:
            if auto_register:
                self.register(variable_class)
                return variable_class.table_name()
            else:
                raise NotRegisteredError(
                    f"Variable type '{type_name}' is not registered. "
                    f"No data has been saved for this type yet."
                )

        self._registered_types[type_name] = variable_class
        return rows[0][0]

    def save_variable(
        self,
        variable_class: type[BaseVariable],
        data: Any,
        index: Any = None,
        **metadata,
    ) -> str:
        """
        Save data as a variable.

        Accepts a BaseVariable instance or raw data.

        Args:
            variable_class: The BaseVariable subclass to save as
            data: The data to save (BaseVariable or raw data)
            index: Optional index to set on the DataFrame
            **metadata: Addressing metadata (e.g., subject=1, trial=1)

        Returns:
            The record_id of the saved data
        """
        metadata = self.canonicalize_metadata(metadata)
        type_name = variable_class.__name__
        user_keys = {k: v for k, v in metadata.items() if not k.startswith("__")}
        Log.debug(f"save_variable({type_name}): metadata={user_keys}")

        raw_data = data.data if isinstance(data, BaseVariable) else data

        instance = variable_class(raw_data)
        record_id = self.save(instance, metadata, index=index)

        instance.record_id = record_id
        instance.metadata = metadata

        Log.info(f"save_variable({type_name}): saved -> record_id={record_id[:12]}")
        return record_id

    def save(
        self,
        variable: BaseVariable,
        metadata: dict,
        index: Any = None,
    ) -> str:
        """
        Save a variable to the database.

        Args:
            variable: The variable instance to save
            metadata: Addressing metadata (flat dict)
            index: Optional index to set on the DataFrame

        Returns:
            The record_id of the saved data
        """
        table_name = self._ensure_registered(type(variable))
        type_name = variable.__class__.__name__
        user_id = get_user_id()

        # Drop transient for_each bookkeeping that must not become version keys:
        # __branch_params (accumulated constants, now derived from the graph).
        if isinstance(metadata, dict):
            metadata = {k: v for k, v in metadata.items() if k != "__branch_params"}

        # Split metadata
        nested_metadata = self._split_metadata(metadata)

        # Warn if no metadata keys match the schema (skip internal __* keys)
        user_metadata_keys = [k for k in metadata if not k.startswith("__")]
        if user_metadata_keys and not nested_metadata.get("schema"):
            warnings.warn(
                f"None of the metadata keys {user_metadata_keys} match the "
                f"configured dataset_schema_keys {self.dataset_schema_keys}. "
                f"All keys will be treated as version parameters.",
                UserWarning,
                stacklevel=2,
            )

        # Normalize array.array values to numpy arrays (MATLAB bridge can produce these)
        import array as _array_mod

        if isinstance(variable.data, dict):
            for k, v in variable.data.items():
                if isinstance(v, _array_mod.array):
                    variable.data[k] = np.array(v)

        # Compute content hash
        content_hash = canonical_hash(variable.data)

        # Generate record_id
        record_id = generate_record_id(
            class_name=type_name,
            schema_version=variable.schema_version,
            content_hash=content_hash,
            metadata=nested_metadata,
        )

        serialization = (
            "custom" if self._has_custom_serialization(type(variable)) else "native"
        )
        Log.debug(
            f"save({type_name}): record_id={record_id[:12]}, content_hash={content_hash[:12]}, serialization={serialization}"
        )

        # Wrap all writes in a single transaction to avoid repeated
        # WAL checkpoints (each auto-committed statement can trigger a
        # checkpoint/fsync, causing random multi-second stalls).
        self._duck._begin()

        try:
            schema_keys = nested_metadata.get("schema", {})
            version_keys = nested_metadata.get("version", {})
            schema_level = self._infer_schema_level(schema_keys)
            created_at = datetime.now().isoformat()

            if self._has_custom_serialization(type(variable)):
                # Custom serialization: user provides to_db() → DataFrame
                df = variable.to_db()

                if index is not None:
                    index_list = list(index) if not isinstance(index, list) else index
                    if len(index_list) != len(df):
                        raise ValueError(
                            f"Index length ({len(index_list)}) does not match "
                            f"DataFrame row count ({len(df)})"
                        )
                    df.index = index

                schema_id = self._save_columnar(
                    record_id,
                    table_name,
                    type(variable),
                    df,
                    schema_level,
                    schema_keys,
                    content_hash,
                )
            else:
                # ALL other data: scalars, arrays, lists, dicts, dict-of-arrays,
                # and native DataFrames (stored as a single record with array-typed
                # columns, e.g. DOUBLE[], BIGINT[], VARCHAR[]).
                schema_id = self._save_native(
                    record_id,
                    table_name,
                    type(variable),
                    variable.data,
                    content_hash,
                    schema_level=schema_level,
                    schema_keys=schema_keys,
                )

            self._save_record_event(
                record_id=record_id,
                timestamp=created_at,
                user_id=user_id,
            )

            # Mirror into the bipartite entities table (_record). Covers raw /
            # manually saved records, which have no producing invocation.
            from .provenance import insert_record_entity

            insert_record_entity(
                self._duck,
                record_id=record_id,
                created_at=created_at,
                type_name=type_name,
                schema_id=schema_id,
                content_hash=content_hash,
                schema_version=variable.schema_version,
            )

            # A save carrying non-schema kwargs: anchor those kwargs in the graph
            # as a synthetic save invocation so they are graph-derivable branch
            # params (the variant role formerly held by the version_keys column).
            if version_keys:
                save_kwargs = {
                    k: v for k, v in version_keys.items() if not str(k).startswith("__")
                }
                if save_kwargs:
                    from .provenance_save import record_direct_save

                    record_direct_save(self._duck, record_id, save_kwargs, created_at)

            self._duck._commit()
            Log.debug(f"save({type_name}): transaction committed")

        except Exception as e:
            Log.error(f"save({type_name}): transaction rolled back: {e}")
            try:
                self._duck._rollback()
            except Exception:
                pass  # Connection may already be closed
            raise

        return record_id

    def _load_with_where(
        self,
        variable_class: type[BaseVariable],
        metadata: dict,
        table_name: str,
        where,
        version_id: str = "latest",
    ):
        """Load records using where= with **semantic** variant matching.

        ``where=`` is a *variant/computation* selector: its variable-level portion
        identifies which computed variant to return by the **schema_id set it
        resolves to now** (§10 "where= redesign"). Any SchemaKey portion is a *row*
        selector applied on top. The producing run's ``where_clause`` STRING is
        never read here — it survives only for visual inspection (get_execution_audit).

        Matching rule (subset + raw fallback), which collapses the old Strategy 1
        (provenance) and Strategy 2 (schema fallback) into one mechanism:
          - a record **with** a producing invocation matches iff every input
            schema_id that invocation consumed is within the resolved variant set
            ``S_var`` (an aggregation consumed all of ``S_var``; a per-combo output
            consumed a single location in it);
          - a record with **no** producing invocation (raw/direct save) matches iff
            its own schema_id ∈ ``S_var``.
        The result is then intersected with the SchemaKey / pre-resolved row set.

        Returns:
            A pandas DataFrame of matching record rows.

        Raises:
            NotFoundError: If no records match.
        """
        type_name = variable_class.__name__

        from .filters import Filter, split_schema_key_filters

        # Separate the variant role (variable-level filter → S_var) from the row
        # role (SchemaKey portion, or a Merge constituent's pre-resolved id set).
        # ``variant_validate_coverage`` is False for Merge constituents (the merge
        # path validates coverage once over the merged result, not per constituent).
        variant_validate_coverage = True
        if getattr(where, "_restrict_to_resolved_ids", False):
            # Merge constituent: variant filter carried explicitly; resolve() is the
            # pre-resolved row restriction (full filter, variable-level AND SchemaKey).
            var_filter = getattr(where, "_variable_filter", None)
            row_ids = where.resolve(self, variable_class, table_name)
            variant_validate_coverage = False
        elif isinstance(where, Filter):
            sk_filter, var_filter = split_schema_key_filters(where)
            row_ids = (
                sk_filter.resolve(self, variable_class, table_name)
                if sk_filter is not None
                else None
            )
        elif where is None or hasattr(where, "resolve"):
            var_filter, row_ids = where, None
        else:
            raise TypeError(
                f"load(where=...) expects a Filter (e.g. Var=='x', raw_sql(...)); "
                f"got {type(where).__name__}."
            )

        nested_base = self._split_metadata(metadata)
        records_all = self._find_record(
            type_name, nested_metadata=nested_base, version_id=version_id
        )
        if len(records_all) == 0:
            raise NotFoundError(f"No {type_name} found matching metadata: {metadata}")

        records = records_all

        # --- Variant match (semantic) on the variable-level portion ---
        if var_filter is not None:
            s_var = frozenset(
                var_filter.resolve(
                    self,
                    variable_class,
                    table_name,
                    validate_coverage=variant_validate_coverage,
                )
            )
            # The consumed-input subset test runs at the INPUT variable's level,
            # which the target-resolved ``s_var`` does not capture when the output
            # is coarser than the filter (cross-level where=): there ``resolve``
            # short-circuits a finer filter to "all target ids", so consumed
            # (input-level) ids could never be a subset. Resolve the filter at its
            # OWN granularity for that test; fall back to ``s_var`` for filters with
            # no intrinsic level (raw SQL) and for the same-level case (identical).
            try:
                s_var_native = frozenset(var_filter.resolve_native(self))
            except NotImplementedError:
                s_var_native = s_var
            consumed_map = provenance_query.consumed_input_schema_ids(
                self._duck, records_all["record_id"].tolist()
            )

            fell_back: list[str] = []

            def _variant_match(row, _s=s_var, _sn=s_var_native, _c=consumed_map):
                consumed = _c.get(row["record_id"])
                if consumed is not None:
                    return (
                        consumed <= _sn
                    )  # subset: every consumed loc ∈ S_var (native level)
                fell_back.append(row["record_id"])
                return row["schema_id"] in _s  # raw/direct-save fallback (target level)

            records = records_all[records_all.apply(_variant_match, axis=1)]
            Log.debug(
                f"[_load_with_where] {type_name}: variant match kept {len(records)} "
                f"of {len(records_all)} records (|S_var|={len(s_var)})"
            )
            # The raw/direct-save fallback is correct for a record nothing
            # produced, but it CANNOT narrow a cross-level where=: there
            # ``resolve`` short-circuits a finer filter to "all target ids", so
            # ``schema_id in S_var`` is true for every candidate and the filter
            # silently selects nothing at all. Two variants at one output
            # location then both come back and the caller gets an ambiguous
            # result instead of the one it asked for.
            #
            # Reaching it for a record that HAS a producing invocation means the
            # invocation recorded no variable input edges — which is the only
            # thing that makes this variant identity work. Say so: without this
            # line the symptom is "load returned 2 records" with nothing
            # anywhere connecting it to missing provenance edges.
            if fell_back and s_var_native != s_var:
                Log.warn(
                    f"[_load_with_where] {type_name}: {len(fell_back)} record(s) "
                    f"had no recorded consumed-input locations, so a CROSS-LEVEL "
                    f"where= fell back to matching on the record's own schema_id "
                    f"— which cannot narrow (the filter resolves to every target "
                    f"location). Expect an over-broad result. Record(s): "
                    f"{fell_back[:5]}"
                )

            # A where= that selects a variant is supposed to land on ONE of
            # them. More than one surviving means the subset test admitted
            # records it should have excluded, and the only way to see WHY is
            # the two sets it compared — the filter's own resolved locations
            # against each record's consumed locations. Both are small here (a
            # handful of schema_ids), this fires only on the ambiguous case, and
            # without it the symptom is a bare "load returned 2 records" with
            # nothing to attribute it to.
            if len(records) > 1:
                Log.info(
                    f"[_load_with_where] {type_name}: where= left "
                    f"{len(records)} records AMBIGUOUS (expected one variant). "
                    f"filter={var_filter!r} resolved natively to "
                    f"{sorted(s_var_native)}; target-level S_var="
                    f"{sorted(s_var)}; per-record consumed inputs: "
                    + ", ".join(
                        f"{rid}(schema={sid}) consumed="
                        f"{sorted(consumed_map[rid]) if rid in consumed_map else 'NONE'}"
                        for rid, sid in zip(
                            records["record_id"].tolist(),
                            records["schema_id"].tolist(),
                            strict=False,
                        )
                    )
                )

        # --- Row restriction (SchemaKey portion / pre-resolved Merge ids) ---
        if row_ids is not None:
            _before = len(records)
            records = records[records["schema_id"].isin(row_ids)]
            Log.debug(
                f"[_load_with_where] {type_name}: row restriction {_before} -> "
                f"{len(records)} records ({len(row_ids)} allowed schema_ids)"
            )

        if len(records) == 0:
            raise NotFoundError(
                f"No {type_name} found matching metadata: {metadata} "
                f"with the given where= filter."
            )
        return records

    # -------------------------------------------------------------------------
    # Bulk DataFrame loading engine
    # -------------------------------------------------------------------------

    def _load_as_df_via_iterator(
        self,
        variable_class: type[BaseVariable],
        metadata: dict,
        *,
        layout: str,
        include_rid: bool,
        include_bp: bool,
        stringify_schema: bool,
        version_id: str,
        where,
        branch_params_filter: dict | None,
    ) -> pd.DataFrame:
        """Slow fallback: load via instance iterator, then assemble DataFrame.

        Mirrors the output shape of the fast path but uses ``db.load()`` so
        it is always correct regardless of dtype or subclass customisation.
        """
        schema_keys_set = set(self.dataset_schema_keys)
        view_name = (
            variable_class.view_name()
            if hasattr(variable_class, "view_name")
            else variable_class.__name__
        )

        loaded = list(
            self.load(
                variable_class,
                metadata,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        )

        if not loaded:
            return pd.DataFrame()

        def _process_meta(var, *, spread: bool) -> dict:
            meta = dict(var.metadata) if var.metadata else {}
            if spread:
                # Drop __* keys and const-param keys (same as _stringify_meta).
                const_val = meta.get("__constants", {})
                if isinstance(const_val, str):
                    try:
                        const_val = json.loads(const_val)
                    except Exception:
                        const_val = {}
                const_keys = set(const_val.keys()) if const_val else set()
                result = {}
                for k, v in meta.items():
                    if k.startswith("__") or k in const_keys:
                        continue
                    result[k] = (
                        str(v)
                        if (k in schema_keys_set and stringify_schema and v is not None)
                        else v
                    )
                return result
            else:
                # Packed: include all metadata (current BaseVariable.load(as_df=True) behaviour).
                return meta

        is_spread = layout == "spread"
        first = loaded[0]

        if hasattr(first, "data") and isinstance(first.data, pd.DataFrame):
            # DataFrame-mode variable
            if layout == "spread":
                all_data: list = []
                all_meta_rows: list = []
                for var in loaded:
                    data_df = var.data
                    meta = _process_meta(var, spread=True)
                    if include_rid:
                        meta["__record_id"] = var.record_id
                    if include_bp:
                        meta["__branch_params"] = json.dumps(var.branch_params or {})
                    nr = len(data_df)
                    for _ in range(nr):
                        all_meta_rows.append(meta)
                    all_data.append(data_df.reset_index(drop=True))
                if not all_meta_rows:
                    return pd.DataFrame()
                combined_meta = pd.DataFrame(all_meta_rows)
                combined_data = pd.concat(all_data, ignore_index=True)
                return pd.concat(
                    [
                        combined_meta.reset_index(drop=True),
                        combined_data.reset_index(drop=True),
                    ],
                    axis=1,
                )
            else:  # packed
                rows = []
                for var in loaded:
                    row = _process_meta(var, spread=False)
                    row["data"] = var.data
                    if include_rid:
                        row["__record_id"] = var.record_id
                    if include_bp:
                        row["__branch_params"] = json.dumps(var.branch_params or {})
                    rows.append(row)
                return pd.DataFrame(rows)
        else:
            # Scalar / array / dict mode
            col_name = "data" if layout == "packed" else view_name
            rows = []
            for var in loaded:
                row = _process_meta(var, spread=is_spread)
                row[col_name] = var.data if hasattr(var, "data") else var
                if include_rid:
                    row["__record_id"] = (
                        var.record_id if hasattr(var, "record_id") else None
                    )
                if include_bp:
                    row["__branch_params"] = json.dumps(
                        var.branch_params if hasattr(var, "branch_params") else {}
                    )
                rows.append(row)
            return pd.DataFrame(rows)

    def _assemble_df_from_records_and_data(
        self,
        records: pd.DataFrame,
        data_df: pd.DataFrame,
        dtype_meta: dict,
        variable_class: type[BaseVariable],
        *,
        layout: str,
        include_rid: bool,
        include_bp: bool,
        stringify_schema: bool,
    ) -> pd.DataFrame:
        """Assemble the output DataFrame from bulk-fetched records and data.

        Fast path — no per-record Python loops, no BaseVariable construction.
        Called only when all dispatch checks pass (native dtype, no subclass overrides).
        """
        mode = dtype_meta.get("mode", "single_column")
        columns_meta = dtype_meta.get("columns", {})
        data_cols = list(columns_meta.keys())
        schema_keys = self.dataset_schema_keys
        view_name = (
            variable_class.view_name()
            if hasattr(variable_class, "view_name")
            else variable_class.__name__
        )

        # -- Apply vectorized type restoration to data columns --
        # NULL list elements come back from DuckDB as masked arrays and are
        # restored as NaN by sciduckdb; count them BEFORE restoration so the
        # log says how many were touched (they used to become 0 — 2026-09-15).
        null_counts: dict = {}
        for col, col_meta in columns_meta.items():
            if col in data_df.columns:
                n_null = count_null_list_elements(data_df[col])
                if n_null:
                    null_counts[col] = n_null
                data_df[col] = storage_to_python_column(data_df[col], col_meta)
        if null_counts:
            Log.info(
                f"load_all_as_df({variable_class.__name__}): restored "
                f"{sum(null_counts.values())} NULL list element(s) as NaN in "
                f"{len(null_counts)} column(s): {null_counts}"
            )

        # -- Build metadata DataFrame --
        meta_dict: dict = {}

        # Always include record_id temporarily for joining; rename/drop at end.
        meta_dict["__record_id"] = records["record_id"].values

        # Schema columns
        for key in schema_keys:
            if key in records.columns:
                col_series = records[key]
                if stringify_schema:
                    # Values come back as strings from DuckDB VARCHAR columns
                    meta_dict[key] = col_series.values
                else:
                    meta_dict[key] = col_series.apply(
                        lambda v, _key=key: (
                            self.restore_schema_value(_key, v)
                            if v is not None
                            and not (isinstance(v, float) and pd.isna(v))
                            else None
                        )
                    ).values

        # Non-schema metadata columns: a record's direct-save kwargs, derived
        # from the graph (the synthetic ``__save__`` invocation's constants).
        # Internal pipeline markers (__fn etc.) and fn sweep-constants are not
        # exposed here — the latter appear in the ``__branch_params`` column below.

        # Batched: derive every record's branch params in one closure build,
        # reused for both the __save__ kwarg columns and the __branch_params
        # column (previously two per-record ancestry walks each — the dominant
        # cost when assembling large spread results).
        rec_id_values = records["record_id"].values
        bp_map = provenance_query.branch_params_batch(self._duck, list(rec_id_values))
        per_row_kwargs: list[dict] = []
        kwarg_col_names: dict[str, list] = {}
        for rid in rec_id_values:
            kw = {
                k[len("__save__.") :]: v
                for k, v in bp_map.get(rid, {}).items()
                if k.startswith("__save__.")
            }
            per_row_kwargs.append(kw)
            for name in kw:
                if name not in kwarg_col_names:
                    kwarg_col_names[name] = [None] * len(records)
        for i, kw in enumerate(per_row_kwargs):
            for name, val in kw.items():
                kwarg_col_names[name][i] = val

        meta_dict.update(kwarg_col_names)

        # Branch params column — derived from the bipartite graph (§6) per record,
        # not read from a stored column.
        if include_bp:
            meta_dict["__branch_params"] = [
                json.dumps(bp_map.get(rid, {})) for rid in rec_id_values
            ]

        meta_df = pd.DataFrame(meta_dict)

        # Detect and warn about orphaned records: present in the save log / _record
        # but absent from the data table.  These arise when a save partially failed
        # or when a record was logged without a corresponding data row (e.g. a buggy
        # prior for_each run with an unexpected schema key).
        # Using an inner join below excludes them; we surface a warning so users can
        # investigate the root cause in their database.
        type_name = variable_class.__name__
        data_rids = set(data_df["record_id"].tolist()) if not data_df.empty else set()
        meta_rids = set(meta_df["__record_id"].tolist())
        orphaned = meta_rids - data_rids
        if orphaned:
            sample = sorted(orphaned)[:3]
            Log.warn(
                f"load_all_as_df({type_name}): {len(orphaned)} record(s) exist in "
                f"the save log but have no data in {type_name}_data — "
                f"excluding from results. Sample IDs: {sample}"
                f"{'...' if len(orphaned) > 3 else ''}. "
                f"This usually means a previous save partially failed or used an "
                f"unexpected schema key. Inspect the database for orphaned rows."
            )

        # -- Assemble by storage mode --
        if mode == "dataframe":
            df_columns = dtype_meta.get("df_columns", data_cols)

            if layout == "spread":
                # INNER JOIN meta to data on record_id — excludes orphaned records
                # (those with no data rows) so they never appear as NaN-filled rows.
                result = meta_df.merge(
                    data_df,
                    left_on="__record_id",
                    right_on="record_id",
                    how="inner",
                )
                result = result.drop(columns=["record_id"], errors="ignore")
                if not include_rid:
                    result = result.drop(columns=["__record_id"], errors="ignore")
                return result.reset_index(drop=True)
            else:  # packed
                # Group data by record_id and build a nested DataFrame per record.
                grouped = data_df.groupby("record_id", sort=False)
                packed_map: dict = {}
                for rid, group in grouped:
                    g = group.drop(columns=["record_id"], errors="ignore").reset_index(
                        drop=True
                    )
                    cols_present = [c for c in df_columns if c in g.columns]
                    packed_map[rid] = g[cols_present] if cols_present else g
                meta_df["data"] = meta_df["__record_id"].map(packed_map)
                if not include_rid:
                    meta_df = meta_df.drop(columns=["__record_id"], errors="ignore")
                return meta_df.reset_index(drop=True)

        elif mode == "single_column":
            # data_df: [record_id, value_col] — one row per record.
            col_name = data_cols[0] if data_cols else None
            if col_name is None:
                if not include_rid:
                    meta_df = meta_df.drop(columns=["__record_id"], errors="ignore")
                return meta_df.reset_index(drop=True)

            out_col = "data" if layout == "packed" else view_name
            data_renamed = data_df[["record_id", col_name]].rename(
                columns={"record_id": "__record_id", col_name: out_col}
            )
            # INNER JOIN: excludes orphaned records so they never produce NaN data rows.
            result = meta_df.merge(data_renamed, on="__record_id", how="inner")
            if not include_rid:
                result = result.drop(columns=["__record_id"], errors="ignore")
            return result.reset_index(drop=True)

        elif mode == "multi_column":
            # data_df: [record_id, col1, col2, …] — one row per record.
            if layout == "packed":
                data_only = data_df.drop(columns=["record_id"], errors="ignore")
                data_dicts = data_only.to_dict("records")
                data_map = dict(zip(data_df["record_id"], data_dicts, strict=False))
                meta_df["data"] = meta_df["__record_id"].map(data_map)
                if not include_rid:
                    meta_df = meta_df.drop(columns=["__record_id"], errors="ignore")
                return meta_df.reset_index(drop=True)
            else:  # spread
                data_renamed = data_df.rename(columns={"record_id": "__record_id"})
                # INNER JOIN: excludes orphaned records so they never produce NaN data rows.
                result = meta_df.merge(data_renamed, on="__record_id", how="inner")
                if not include_rid:
                    result = result.drop(columns=["__record_id"], errors="ignore")
                return result.reset_index(drop=True)

        else:
            # Unknown mode — return just metadata.
            if not include_rid:
                meta_df = meta_df.drop(columns=["__record_id"], errors="ignore")
            return meta_df.reset_index(drop=True)

    def get_dtype_meta(self, variable_name: str) -> "dict | None":
        """The stored ``dtype`` metadata for a variable type, or ``None``.

        ``dtype_meta`` records how sciduckdb laid the value out on disk —
        ``mode`` (``single_column`` / ``multi_column`` / ``dataframe``),
        the per-column type map, and the ``nested`` / ``custom`` flags. It is
        the only record of the fact that a variable's rows are the fields of
        one dict rather than the columns of one table, which is a distinction
        the loaded DataFrame cannot express on its own.

        Read-only accessor; the same query is inlined at three other call
        sites that predate it.
        """
        rows = self._duck._fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?",
            [variable_name],
        )
        if not rows:
            return None
        return json.loads(rows[0][0])

    def mapping_data_columns(self, variable_class) -> "list[str] | None":
        """Column names to reassemble into a dict for a dict-valued variable.

        Returns the ordered dict keys when *variable_class* stores in
        ``multi_column`` mode (one DuckDB column per dict key), otherwise
        ``None``.

        Why callers need it: ``load_all_as_df(layout="spread")`` deliberately
        spreads those keys across top-level columns, because column selection
        (``Type("col")``), ``ColName``, ``Merge``, ``as_table=True``,
        ``share_limits`` and ``for_columns`` all operate on them. The cost is
        that a single record then looks exactly like a one-row table, and the
        per-combo extraction in scifor has no way to tell the two apart —
        so a struct saved from MATLAB (or a dict from Python) came back as a
        1-row table. Compare ``_load_record``'s single-record path, which does
        rebuild the dict, and ``_load_as_df_via_iterator``, which keeps a
        ``nested`` dict in a single column: both already round-trip.

        ``nested`` and ``custom`` layouts are excluded — they never reach the
        spreading fast path (``load_all_as_df`` routes them to the iterator,
        which already returns the value whole).
        """
        name = getattr(variable_class, "__name__", None)
        if not name:
            return None
        dtype_meta = self.get_dtype_meta(name)
        if not dtype_meta:
            return None
        if dtype_meta.get("custom") or dtype_meta.get("nested"):
            return None
        if dtype_meta.get("mode") != "multi_column":
            return None
        return list(dtype_meta.get("columns", {}).keys()) or None

    def data_column_names(self, variable_class: type[BaseVariable]) -> "list[str] | None":
        """The data columns a ``layout="spread"`` load of *variable_class*
        returns, read from storage metadata (``_variables.dtype``) — no data
        is loaded. ``None`` when metadata cannot answer and only a load can.

        THE owner of that answer, mirroring :meth:`load_all_as_df`'s fast path
        and :meth:`_assemble_df_from_records_and_data`: a ``dataframe`` or
        ``multi_column`` type spreads its stored columns in stored order; a
        ``single_column`` type spreads one column named after the variable
        (``view_name``). Whatever routes the load to the iterator fallback
        (custom or nested storage, an overridden ``__init__``, custom
        serialization) answers ``None`` here too, so a caller never gets a
        column list the real load would not produce.

        Unlike reading ``loaded.columns``, this never includes a record's
        direct-save kwarg columns (``version="v1"`` spreads as a ``version``
        column): they are metadata, not data (cleanup-audit F28).
        """
        type_name = variable_class.__name__
        rows = self._duck._fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?", [type_name]
        )
        if not rows:
            return None
        dtype_meta = json.loads(rows[0][0])
        if (
            variable_class.__init__ is not BaseVariable.__init__
            or self._has_custom_serialization(variable_class)
            or dtype_meta.get("custom")
            or dtype_meta.get("nested")
        ):
            return None
        data_cols = list(dtype_meta.get("columns", {}).keys())
        mode = dtype_meta.get("mode", "single_column")
        if mode == "single_column":
            if not data_cols:
                return None
            return [
                variable_class.view_name()
                if hasattr(variable_class, "view_name")
                else type_name
            ]
        if mode in ("dataframe", "multi_column"):
            return data_cols or None
        return None

    def load_all_as_df(
        self,
        variable_class: type[BaseVariable],
        metadata: dict | None = None,
        *,
        layout: str = "packed",
        include_rid: bool = False,
        include_bp: bool = False,
        stringify_schema: bool = False,
        version_id: str = "latest",
        where=None,
        branch_params_filter: dict | None = None,
    ) -> pd.DataFrame:
        """Bulk load all records of a variable type into a DataFrame.

        Shared engine for two callers:

        * ``BaseVariable.load(as_df=True)`` — ``layout="packed"``, one row per
          record, data stored in a ``"data"`` column.
        * ``foreach._convert_inputs`` — ``layout="spread"``, DataFrame-mode variables
          produce one output row per inner-table row; data columns are spread across
          top-level columns rather than nested.

        Fast path
        ---------
        Fetches records and data in bulk SQL queries and assembles the output with
        vectorised pandas operations.  Applies when all of the following hold:

        * ``variable_class.__init__`` is not overridden (standard construction).
        * ``variable_class`` does not override ``from_db`` (no custom deserialisation).
        * ``dtype_meta`` does not have ``"custom": True`` (native storage only).
        * ``dtype_meta`` does not have ``"nested": True`` (flat multi-column only).

        Fall-back
        ---------
        Any violated condition routes to ``_load_as_df_via_iterator``, which uses the
        ``db.load()`` iterator and assembles row-by-row.  Output shape is identical.

        Parameters
        ----------
        variable_class :
            BaseVariable subclass to load.
        metadata :
            Optional flat metadata dict for filtering (default: load all records).
        layout :
            ``"packed"`` — one output row per record; ``"spread"`` — data columns
            spread, multiple rows per record for DataFrame-mode variables.
        include_rid :
            Add ``"__record_id"`` column to the output.
        include_bp :
            Add ``"__branch_params"`` (JSON string) column to the output.
        stringify_schema :
            Keep schema key values as strings (foreach uses this; notebook users
            generally do not).
        version_id :
            ``"latest"`` or ``"all"`` — passed to ``_find_record``.
        where :
            Optional Filter for restricting records.
        branch_params_filter :
            Optional branch_params key/value filter dict.

        Returns
        -------
        pd.DataFrame
            Empty DataFrame when no matching records exist.
        """
        metadata = self.canonicalize_metadata(metadata or {})
        type_name = variable_class.__name__
        table_name = type_name + "_data"

        # Look up dtype metadata — needed for dispatch and deserialisation.
        dtype_rows = self._duck._fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?",
            [type_name],
        )
        if not dtype_rows:
            return pd.DataFrame()
        dtype_meta = json.loads(dtype_rows[0][0])

        # -- Dispatch: fast path or iterator fallback? --
        # Class-introspection bailouts
        if variable_class.__init__ is not BaseVariable.__init__:
            return self._load_as_df_via_iterator(
                variable_class,
                metadata,
                layout=layout,
                include_rid=include_rid,
                include_bp=include_bp,
                stringify_schema=stringify_schema,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        if self._has_custom_serialization(variable_class):
            return self._load_as_df_via_iterator(
                variable_class,
                metadata,
                layout=layout,
                include_rid=include_rid,
                include_bp=include_bp,
                stringify_schema=stringify_schema,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        # Storage-mode bailouts
        if dtype_meta.get("custom"):
            return self._load_as_df_via_iterator(
                variable_class,
                metadata,
                layout=layout,
                include_rid=include_rid,
                include_bp=include_bp,
                stringify_schema=stringify_schema,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        if dtype_meta.get("nested"):
            return self._load_as_df_via_iterator(
                variable_class,
                metadata,
                layout=layout,
                include_rid=include_rid,
                include_bp=include_bp,
                stringify_schema=stringify_schema,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )

        # -- Fast path --
        t0 = time.perf_counter()
        if where is not None:
            try:
                records = self._load_with_where(
                    variable_class,
                    metadata,
                    table_name,
                    where,
                    version_id=version_id,
                )
            except Exception as _exc:
                Log.debug(
                    f"[load_all_as_df] {type_name}: _load_with_where raised "
                    f"{type(_exc).__name__}: {_exc} — returning empty DataFrame"
                )
                return pd.DataFrame()
            # where= and branch_params_filter (Variant) coexist: apply the
            # branch_params filter as a post-step on the where-matched records.
            if branch_params_filter:
                _n_before = len(records)
                records = _filter_records_by_branch_params(
                    records, branch_params_filter, self._duck
                )
                Log.debug(
                    f"[load_all_as_df] {type_name}: branch_params_filter "
                    f"{branch_params_filter} kept {len(records)}/{_n_before} records"
                )
        else:
            nested_metadata = self._split_metadata(metadata)
            records = self._find_record(
                type_name,
                nested_metadata=nested_metadata,
                version_id=version_id,
                branch_params_filter=branch_params_filter,
            )

        if len(records) == 0:
            return pd.DataFrame()

        # Bulk-fetch data rows in chunks to avoid very long IN clauses.
        all_record_ids = records["record_id"].tolist()
        data_cols = list(dtype_meta.get("columns", {}).keys())
        if not data_cols:
            return pd.DataFrame()

        data_select = ", ".join(f'"{c}"' for c in data_cols)
        chunk_size = 500
        chunks: list = []
        t_sql = 0.0
        for start in range(0, len(all_record_ids), chunk_size):
            chunk = all_record_ids[start : start + chunk_size]
            placeholders = ", ".join(["?"] * len(chunk))
            sql = (
                f'SELECT record_id, {data_select} FROM "{table_name}" '
                f"WHERE record_id IN ({placeholders})"
            )
            _t = time.perf_counter()
            chunk_df = self._duck._fetchdf(sql, chunk)
            t_sql += time.perf_counter() - _t
            if not chunk_df.empty:
                chunks.append(chunk_df)

        if not chunks:
            return pd.DataFrame()

        data_df = (
            pd.concat(chunks, ignore_index=True)
            if len(chunks) > 1
            else chunks[0].copy()
        )

        result = self._assemble_df_from_records_and_data(
            records,
            data_df,
            dtype_meta,
            variable_class,
            layout=layout,
            include_rid=include_rid,
            include_bp=include_bp,
            stringify_schema=stringify_schema,
        )
        t_total = time.perf_counter() - t0
        Log.info(
            f"[timing] load_all_as_df({type_name}): {len(records)} records, "
            f"layout={layout!r}, sql={t_sql:.3f}s, total={t_total:.3f}s"
        )
        return result

    def load(
        self,
        variable_class: type[BaseVariable],
        metadata: dict,
        version_id: str = "all",
        where=None,
        branch_params_filter: dict | None = None,
    ):
        """
        Load variables matching the given metadata as a generator.

        Args:
            variable_class: The type to load
            metadata: Flat metadata dict
            version_id: Which versions to return:
                - "all" (default): return every version
                - "latest": return only the latest version per (schema_id, variant)
                - any other string: treated as a specific record_id
            where: Optional Filter selecting the computation *variant* by the
                schema_id set its variable-level portion resolves to (semantic
                matching by consumed inputs); any SchemaKey portion further
                restricts rows. See ``_load_with_where``.
            branch_params_filter: Optional dict of branch_params key/value filters.

        Yields:
            BaseVariable instances matching the metadata
        """
        metadata = self.canonicalize_metadata(metadata)
        type_name = variable_class.__name__
        user_summary = {k: v for k, v in metadata.items() if not k.startswith("__")}
        Log.debug(f"load({type_name}): metadata={user_summary}")
        _t_load_all_total = time.perf_counter()
        table_name = self._ensure_registered(variable_class, auto_register=False)

        _t_find = time.perf_counter()

        # Specific record_id: bypass normal filtering
        if version_id not in ("all", "latest") and version_id is not None:
            records = self._find_record(
                variable_class.__name__,
                record_id=version_id,
                include_excluded=True,
            )
            if len(records) == 0:
                raise NotFoundError(f"No data found with record_id '{version_id}'")
        elif where is not None:
            # where= specified: semantic variant match + row restriction
            try:
                records = self._load_with_where(
                    variable_class,
                    metadata,
                    table_name,
                    where,
                    version_id=version_id,
                )
            except NotFoundError:
                Log.info(f"load({type_name}): no records found")
                if not self._any_records_exist(type_name):
                    raise NotFoundError(
                        f"Variable type '{type_name}' is registered but has no saved records in this database."
                    )
                return
            # where= and branch_params_filter (Variant) coexist: apply the
            # branch_params filter as a post-step on the where-matched records.
            if branch_params_filter:
                _n_before = len(records)
                records = _filter_records_by_branch_params(
                    records, branch_params_filter, self._duck
                )
                Log.debug(
                    f"load({type_name}): branch_params_filter {branch_params_filter} "
                    f"kept {len(records)}/{_n_before} records"
                )
                if len(records) == 0:
                    return
        else:
            nested_metadata = self._split_metadata(metadata)
            try:
                records = self._find_record(
                    variable_class.__name__,
                    nested_metadata=nested_metadata,
                    version_id=version_id,
                    branch_params_filter=branch_params_filter,
                )
            except NotFoundError:
                Log.info(f"load({type_name}): no records found")
                return  # No data

            if len(records) == 0:
                Log.info(f"load({type_name}): no records found")
                if not self._any_records_exist(type_name):
                    raise NotFoundError(
                        f"Variable type '{type_name}' is registered but has no saved records in this database."
                    )
                return
        t_find = time.perf_counter() - _t_find

        Log.debug(f"load({type_name}): found {len(records)} record(s)")

        # --- Bulk loading path ---

        # 1. Get dtype from _variables (one row per variable)
        _t_dtype = time.perf_counter()
        dtype_rows = self._duck._fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?",
            [variable_class.__name__],
        )
        if not dtype_rows:
            return
        dtype_meta = json.loads(dtype_rows[0][0])
        is_custom = dtype_meta.get("custom", False)
        t_dtype = time.perf_counter() - _t_dtype

        # 2. Collect all unique record_ids to fetch
        all_record_ids = records["record_id"].tolist()
        if not all_record_ids:
            return

        # 3. Batch fetch data rows by record_id
        data_lookup: dict[str, Any] = {}  # record_id -> deserialized value

        chunk_size = 500
        t_chunk_sql = 0.0
        t_chunk_deserialize = 0.0
        _t_chunks_total = time.perf_counter()
        for start in range(0, len(all_record_ids), chunk_size):
            chunk = all_record_ids[start : start + chunk_size]
            placeholders = ", ".join(["?"] * len(chunk))

            if is_custom:
                # Custom (columnar) path: fetch all rows for this chunk
                sql = (
                    f'SELECT * FROM "{table_name}" WHERE record_id IN ({placeholders})'
                )
                _t = time.perf_counter()
                chunk_df = self._duck._fetchdf(sql, chunk)
                t_chunk_sql += time.perf_counter() - _t

                if len(chunk_df) > 0:
                    _t = time.perf_counter()
                    grouped = chunk_df.groupby("record_id", sort=False)
                    for rid, sub_df in grouped:
                        sub_df = sub_df.drop(
                            columns=["record_id"], errors="ignore"
                        ).reset_index(drop=True)
                        data_lookup[rid] = self._deserialize_custom_subdf(
                            variable_class,
                            sub_df,
                            dtype_meta,
                        )
                    t_chunk_deserialize += time.perf_counter() - _t
            else:
                # Native path
                data_cols = list(dtype_meta.get("columns", {}).keys())
                data_select = ", ".join(f'"{c}"' for c in data_cols)
                sql = (
                    f'SELECT record_id, {data_select} FROM "{table_name}" '
                    f"WHERE record_id IN ({placeholders})"
                )
                _t = time.perf_counter()
                chunk_df = self._duck._fetchdf(sql, chunk)
                t_chunk_sql += time.perf_counter() - _t

                if len(chunk_df) > 0:
                    _t = time.perf_counter()
                    mode = dtype_meta.get("mode", "single_column")
                    columns_meta = dtype_meta.get("columns", {})

                    if mode == "dataframe":
                        # One DuckDB row per DataFrame row: group by record_id.
                        df_columns = dtype_meta.get(
                            "df_columns", list(columns_meta.keys())
                        )
                        for rid, group_df in chunk_df.groupby("record_id", sort=False):
                            group_df = group_df.drop(
                                columns=["record_id"], errors="ignore"
                            ).reset_index(drop=True)
                            result = {}
                            for c, meta in columns_meta.items():
                                if c in group_df.columns:
                                    # Optimized: use tolist() instead of iloc[i] (5-10x faster)
                                    result[c] = [
                                        storage_to_python(val, meta)
                                        for val in group_df[c].tolist()
                                    ]
                            data_lookup[rid] = pd.DataFrame(result, columns=df_columns)
                    else:
                        # Non-DataFrame: restore types, then one value per record_id row.
                        restored = chunk_df[data_cols].copy()
                        restored = self._duck._restore_types(restored, dtype_meta)

                        if mode == "single_column":
                            col_name = next(iter(columns_meta))
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                data_lookup[rid] = restored[col_name].iloc[i]
                        elif mode == "multi_column":
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                result = {}
                                for c, meta in columns_meta.items():
                                    result[c] = storage_to_python(
                                        restored[c].iloc[i], meta
                                    )
                                if dtype_meta.get("nested"):
                                    data_lookup[rid] = unflatten_dict(
                                        result, dtype_meta["path_map"]
                                    )
                                else:
                                    data_lookup[rid] = result
                        else:
                            col_names = list(columns_meta.keys())
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                data_lookup[rid] = {
                                    c: restored[c].iloc[i] for c in col_names
                                }
                    t_chunk_deserialize += time.perf_counter() - _t

        t_chunks_total = time.perf_counter() - _t_chunks_total
        n_chunks = (len(all_record_ids) + chunk_size - 1) // chunk_size
        _mode_str = "custom" if is_custom else dtype_meta.get("mode", "single_column")
        Log.info(
            f"[timing] load({type_name}): pre-yield setup: "
            f"find={t_find:.3f}s, dtype_lookup={t_dtype:.3f}s, "
            f"chunks_total={t_chunks_total:.3f}s "
            f"(sql={t_chunk_sql:.3f}s, deserialize={t_chunk_deserialize:.3f}s, "
            f"n_chunks={n_chunks}, mode={_mode_str})"
        )

        # 4. Construct instances using itertuples + inline metadata.
        # This is a generator: per-yield body cost is measured cumulatively
        # and emitted in a TOTAL summary after the last yield.
        schema_keys = self.dataset_schema_keys
        n_yielded = 0
        t_yield_body = 0.0
        for row in records.itertuples(index=False):
            _t_body = time.perf_counter()
            record_id = row.record_id

            if record_id not in data_lookup:
                t_yield_body += time.perf_counter() - _t_body
                continue

            data_value = data_lookup[record_id]
            content_hash = row.content_hash

            flat_metadata = {}
            for sk in schema_keys:
                val = getattr(row, sk, None)
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    flat_metadata[sk] = self.restore_schema_value(sk, val)

            # branch_params + direct-save kwargs derived from the bipartite graph
            # (§6), not a column. __save__.<kwarg> entries are the record's
            # non-schema save metadata.
            try:

                bp = provenance_query.derived_branch_params(self._duck, record_id)
            except Exception:
                bp = {}
            for k, v in bp.items():
                if k.startswith("__save__."):
                    flat_metadata[k[len("__save__.") :]] = v

            instance = variable_class(data_value)
            instance.record_id = record_id
            instance.metadata = flat_metadata
            instance.content_hash = content_hash
            instance.branch_params = bp

            n_yielded += 1
            t_yield_body += time.perf_counter() - _t_body
            yield instance

        t_total = time.perf_counter() - _t_load_all_total
        _caller_overhead = t_total - t_find - t_chunks_total - t_yield_body - t_dtype
        Log.info(
            f"[timing] load({type_name}): TOTAL={t_total:.3f}s "
            f"(find={t_find:.3f}s, chunks={t_chunks_total:.3f}s, "
            f"yield_body={t_yield_body:.3f}s for {n_yielded} records, "
            f"caller_overhead~={_caller_overhead:.3f}s)"
        )

    def list_versions(
        self,
        variable_class: type[BaseVariable],
        include_excluded: bool = False,
        **metadata,
    ) -> list[dict]:
        """
        List all versions at a schema location.

        Args:
            variable_class: The type to query
            include_excluded: If True, include excluded variants in results.
            **metadata: Schema metadata to match; non-schema keys are treated
                as branch_params filters.

        Returns:
            List of dicts with record_id, schema, branch_params, timestamp
            (plus "excluded" bool when include_excluded=True).
        """
        self._ensure_registered(variable_class, auto_register=True)

        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in metadata.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in metadata.items() if k not in schema_keys_set
        } or None

        nested_metadata = self._split_metadata(schema_metadata)

        try:
            records = self._find_record(
                variable_class.__name__,
                nested_metadata=nested_metadata,
                version_id="all",
                branch_params_filter=branch_params_filter,
                include_excluded=include_excluded,
            )
        except Exception as exc:
            # Still answered as "no versions" (callers and MATLAB rely on a
            # list), but no longer silently: an error here is not the same as
            # a location with nothing saved.
            Log.warn(
                f"list_versions({variable_class.__name__}): record lookup failed "
                f"({type(exc).__name__}: {exc}); reporting no versions"
            )
            return []

        # Every row's branch params from ONE upstream closure. This loop used
        # to walk each record's ancestry twice (once for its save kwargs, once
        # for branch_params) — the N+1 the batched helpers exist to avoid
        # (cleanup-audit F3).
        bp_map = provenance_query.branch_params_batch(
            self._duck, list(records["record_id"]) if len(records) else []
        )
        results = []
        for _, row in records.iterrows():
            # branch_params derived from the bipartite graph (§6), not a column.
            bp = bp_map.get(row["record_id"], {})
            _, nested = self._reconstruct_metadata_from_row(row, branch_params=bp)
            entry = {
                "record_id": row["record_id"],
                "schema": nested.get("schema", {}),
                "branch_params": bp,
                "timestamp": row["timestamp"],
            }
            if include_excluded:
                exc = row.get("excluded") if hasattr(row, "get") else row["excluded"]
                entry["excluded"] = bool(exc) if exc is not None else False
            results.append(entry)

        # Sort by timestamp descending
        results.sort(key=lambda x: x["timestamp"], reverse=True)
        return results

    def _resolve_record_id(
        self,
        record_id_or_type: "str | type[BaseVariable]",
        **kwargs,
    ) -> str:
        """Resolve a record_id string or (variable_class, **kwargs) to a single record_id.

        Raises AmbiguousVersionError if multiple records match, NotFoundError if none.
        Always searches including excluded records.
        """
        if isinstance(record_id_or_type, str):
            return record_id_or_type

        variable_class = record_id_or_type
        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in kwargs.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in kwargs.items() if k not in schema_keys_set
        } or None

        nested_metadata = self._split_metadata(schema_metadata)
        records = self._find_record(
            variable_class.__name__,
            nested_metadata=nested_metadata,
            version_id="all",
            branch_params_filter=branch_params_filter,
            include_excluded=True,
        )

        if len(records) == 0:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching: {kwargs}"
            )
        if len(records) > 1:
            ids = records["record_id"].tolist()
            raise AmbiguousVersionError(
                f"{len(records)} records match for {variable_class.__name__} "
                f"with {kwargs}. "
                f"Pass a record_id directly or narrow with more branch parameters. "
                f"Matching record_ids: {ids}"
            )
        return records.iloc[0]["record_id"]

    def exclude_variant(
        self,
        record_id_or_type: "str | type[BaseVariable]",
        **kwargs,
    ) -> int:
        """Mark variant(s) as excluded from automatic inclusion in for_each and load().

        If multiple variants match the provided metadata, ALL matching variants
        are excluded (e.g., all branch_params for a given schema combo).

        Usage:
            db.exclude_variant("abc123")                                  # by record_id (1 variant)
            db.exclude_variant(DetectedSpikes, subject="S01", low_hz=20)  # specific variant
            db.exclude_variant(DetectedSpikes, subject="S01")             # all variants for S01

        Returns:
            Number of variants excluded.
        """
        if isinstance(record_id_or_type, str):
            # Direct record_id provided - exclude just that one
            self._duck._execute(
                "UPDATE _record SET excluded = TRUE WHERE record_id = ?",
                [record_id_or_type],
            )
            return 1

        # Variable class + kwargs: find and exclude ALL matching records
        variable_class = record_id_or_type
        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in kwargs.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in kwargs.items() if k not in schema_keys_set
        } or None

        nested_metadata = self._split_metadata(schema_metadata)
        records = self._find_record(
            variable_class.__name__,
            nested_metadata=nested_metadata,
            version_id="all",
            branch_params_filter=branch_params_filter,
            include_excluded=True,
        )

        if len(records) == 0:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching: {kwargs}"
            )

        # Exclude all matching records
        record_ids = records["record_id"].tolist()
        for rid in record_ids:
            self._duck._execute(
                "UPDATE _record SET excluded = TRUE WHERE record_id = ?",
                [rid],
            )

        if len(record_ids) > 1:
            Log.info(
                f"Excluded {len(record_ids)} variant(s) of {variable_class.__name__} "
                f"matching {kwargs}"
            )
        return len(record_ids)

    def include_variant(
        self,
        record_id_or_type: "str | type[BaseVariable]",
        **kwargs,
    ) -> int:
        """Re-include previously excluded variant(s).

        If multiple variants match the provided metadata, ALL matching variants
        are re-included.

        Usage:
            db.include_variant("abc123")                                  # by record_id (1 variant)
            db.include_variant(DetectedSpikes, subject="S01", low_hz=20)  # specific variant
            db.include_variant(DetectedSpikes, subject="S01")             # all variants for S01

        Returns:
            Number of variants re-included.
        """
        if isinstance(record_id_or_type, str):
            # Direct record_id provided - include just that one
            self._duck._execute(
                "UPDATE _record SET excluded = FALSE WHERE record_id = ?",
                [record_id_or_type],
            )
            return 1

        # Variable class + kwargs: find and include ALL matching records
        variable_class = record_id_or_type
        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in kwargs.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in kwargs.items() if k not in schema_keys_set
        } or None

        nested_metadata = self._split_metadata(schema_metadata)
        records = self._find_record(
            variable_class.__name__,
            nested_metadata=nested_metadata,
            version_id="all",
            branch_params_filter=branch_params_filter,
            include_excluded=True,
        )

        if len(records) == 0:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching: {kwargs}"
            )

        # Re-include all matching records
        record_ids = records["record_id"].tolist()
        for rid in record_ids:
            self._duck._execute(
                "UPDATE _record SET excluded = FALSE WHERE record_id = ?",
                [rid],
            )

        if len(record_ids) > 1:
            Log.info(
                f"Re-included {len(record_ids)} variant(s) of {variable_class.__name__} "
                f"matching {kwargs}"
            )
        return len(record_ids)

    def get_provenance(
        self,
        variable_class: type[BaseVariable] | None,
        version: str | None = None,
        **metadata,
    ) -> dict | None:
        """
        Get the provenance (lineage) of a variable.

        Returns:
            Dict with function_name, function_hash, inputs, constants
            or None if no lineage recorded
        """
        if version:
            record_id = version
        else:
            var = next(self.load(variable_class, metadata, version_id="latest"))
            record_id = var.record_id


        return provenance_query.provenance(self._duck, record_id)

    def get_provenance_by_schema(self, **schema_keys) -> list[dict]:
        """
        Get all provenance records at a schema location (schema-aware view).

        Args:
            **schema_keys: Schema key filters (e.g., subject="S01", session="1")

        Returns:
            List of provenance record dicts matching the schema keys, each with
            ``output_record_id``, ``output_type``, ``function_name``,
            ``function_hash``, ``inputs``, ``constants`` — sourced from the
            bipartite graph (records produced by an invocation at that location).
        """

        conditions = []
        params: list[Any] = []
        for key, value in schema_keys.items():
            conditions.append(f's."{key}" = ?')
            params.append(schema_str(value))
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        # Output records produced by an invocation at this schema location.
        rows = self._duck._fetchall(
            f"SELECT DISTINCT io.output_record_id, r.type "
            f"FROM _invocation_output io "
            f"JOIN _record r ON r.record_id = io.output_record_id "
            f"LEFT JOIN _schema s ON r.schema_id = s.schema_id"
            f"{where}",
            params,
        )

        results = []
        for record_id, variable_name in rows:
            prov = provenance_query.provenance(self._duck, record_id)
            if prov is None:
                continue
            results.append(
                {
                    "output_record_id": record_id,
                    "output_type": variable_name,
                    "function_name": prov["function_name"],
                    "function_hash": prov["function_hash"],
                    "inputs": prov["inputs"],
                    "constants": prov["constants"],
                }
            )
        return results

    def get_pipeline_structure(self) -> list[dict]:
        """
        Get the abstract pipeline structure (schema-blind view).

        Returns unique (function_name, function_hash, output_type, input_types)
        combinations, describing how variable types flow through functions
        without reference to specific data instances or schema locations.

        Returns:
            List of dicts with keys: function_name, function_hash, output_type,
            input_types (list of type names)
        """

        return provenance_query.pipeline_structure(self._duck)

    def list_pipeline_variants(
        self,
        output_type: str | None = None,
    ) -> list[dict]:
        """
        List all distinct pipeline step variants recorded in the database.

        Each entry represents a unique (function, constants, output_type)
        combination — a "branch" of the pipeline. Two for_each runs on the
        same function with different constants produce two separate entries.

        Derived entirely from the bipartite provenance graph (no version_keys).

        Args:
            output_type: Optional variable type name to filter results
                         (e.g. "Filtered"). If None, all types are returned.

        Returns:
            List of dicts with keys:
                function_name (str),
                output_type   (str),
                call_id       (str: 16 hex chars; identifies the for_each call
                               site that produced this variant.  Same across
                               cosmetic source edits to the fn body),
                input_types   (dict: param_name → type_name),
                constants     (dict: param_name → value),
                run_options   (str: the distribute/as_table set this variant
                               ran under — provenance_query.run_options_label),
                output_num    (int | None: which output of the PRODUCING
                               INVOCATION this record is — the PK half of
                               _invocation_output. That coincides with the
                               0-based signature slot only for an ordinary
                               multi-output call: a `distribute` run numbers
                               its SLICES, and a batch loader sharing one
                               invocation across runs gives a re-run's record
                               the next free slot. Do not index a signature
                               with it without checking the range, and never
                               for a single-output function — see
                               scistack_gui.api.pipeline.
                               _matlab_param_to_class_from_db),
                record_count  (int: distinct records for this variant)
        """

        return provenance_query.pipeline_variants(self._duck, output_type)

    def get_aggregated_variants(
        self,
        fn_name: str | None = None,
        call_id: str | None = None,
    ) -> dict:
        """
        Get aggregated variant data for pipeline visualization.

        Aggregates variants by (fn_name, call_id) and collects metadata about
        functions, variables, constants, and path inputs. This provides all the
        data needed to build a pipeline graph in one query.

        Args:
            fn_name: Optional function name to filter results.
            call_id: Optional call_id to filter to a specific for_each call site.

        Returns:
            Dict with keys:

            ``"functions"`` (dict)
                Keyed by (fn_name, call_id) tuples::

                    {
                        (fn_name, call_id): {
                            "input_params": {param: var_type},
                            "outputs": [var_type1, var_type2],
                            "constants": {param: [val1, val2]},
                            "parameter_names": {param: parameter node name},
                            "variant_count": int,
                            "variants": [
                                {
                                    "input_types": {param: type_name},
                                    "constants": {param: value},
                                    "output_type": str,
                                    "output_num": int | None,
                                    "record_count": int,
                                }
                            ],
                        }
                    }

            ``"variables"`` (dict)
                Variable metadata keyed by variable name::

                    {
                        var_type: {
                            "record_count": int,
                        }
                    }

            ``"constants"`` (dict)
                Constants used across functions, keyed by PARAMETER NODE name:
                the declared Parameter the run recorded, else the argument.
                Each function's ``"parameter_names"`` (``{argument: node}``)
                says which argument each node filled — the handle an edge
                from it lands on::

                    {
                        const_name: {
                            "values": [{"value": val, "record_count": N}],
                            "functions": [(fn_name, call_id), ...],
                        }
                    }

            ``"path_inputs"`` (dict)
                PathInputs, keyed by SPEC (its JSON), not by parameter name —
                two functions may name different PathInputs alike::

                    {
                        spec_json: {
                            "template": str,
                            "root_folder": str | None,
                            "functions": [((fn_name, call_id), param_name), ...],
                        }
                    }

            The conversion itself is :func:`aggregate_pipeline_variants`
            (pure); this method adds the database-only record counts.
        """
        variants = self.list_pipeline_variants()
        if fn_name is not None:
            variants = [v for v in variants if v["function_name"] == fn_name]
        if call_id is not None:
            variants = [v for v in variants if v.get("call_id") == call_id]

        result = aggregate_pipeline_variants(variants)

        # Every variable type's record count in ONE query (it was one per type).
        types = list(result["variables"])
        counts: dict = {}
        if types:
            ph = ", ".join("?" for _ in types)
            counts = {
                t: n
                for t, n in self._duck._fetchall(
                    "SELECT type, COUNT(DISTINCT record_id) FROM _record "  # noqa: S608
                    f"WHERE type IN ({ph}) AND COALESCE(excluded, FALSE) = FALSE "
                    "GROUP BY type",
                    types,
                )
            }
        for t in types:
            result["variables"][t] = {"record_count": counts.get(t, 0)}
        return result

    def filter_variants_for_execution(
        self,
        fn_name: str,
        call_id: str,
        schema_filter: dict[str, list] | None = None,
        constant_overrides: dict[str, Any] | None = None,
    ) -> list[dict]:
        """
        Filter variants for execution based on schema and constant selection.

        This method prepares variants for execution by:
        1. Getting variants for the specified function and call_id
        2. Filtering by schema_filter if provided
        3. Applying constant_overrides (replacing DB constants)
        4. Deduplicating the result

        Args:
            fn_name: Function name to filter.
            call_id: Call site identifier (16 hex chars).
            schema_filter: Optional dict of {schema_key: [selected_values]} to
                filter variants by. Only variants matching these schema values
                will be included.
            constant_overrides: Optional dict of {constant_name: value} to
                override database constants. When provided, these values replace
                the constants from the database for all matching variants.

        Returns:
            List of variant dicts ready for for_each execution::

                [
                    {
                        "input_types": {param: var_type},
                        "output_type": var_type,
                        "constants": {param: value},
                    }
                ]

            The list is deduplicated so identical variants appear only once.

        Example::

            # Get variants for a function, overriding threshold constant
            variants = db.filter_variants_for_execution(
                fn_name="process_signal",
                call_id="abc123def456789",
                schema_filter={"subject": [1, 2]},  # Only subjects 1 and 2
                constant_overrides={"threshold": 0.75},  # Override with 0.75
            )
        """
        from .provenance import constants_identity_key

        # Get all variants for this function and call_id
        all_variants = self.list_pipeline_variants()
        fn_variants = [
            v
            for v in all_variants
            if v["function_name"] == fn_name and v.get("call_id") == call_id
        ]

        if not fn_variants:
            return []

        # Build result variants
        result_variants = []
        seen = set()  # For deduplication

        for variant in fn_variants:
            input_types = variant["input_types"]
            output_type = variant["output_type"]
            constants = dict(variant["constants"])  # Make a copy

            # Apply constant overrides
            if constant_overrides:
                for const_name, const_value in constant_overrides.items():
                    if const_name in constants:
                        constants[const_name] = const_value

            # Create variant dict
            result_variant = {
                "input_types": input_types,
                "output_type": output_type,
                "constants": constants,
            }

            # Deduplicate by converting to a hashable key. Constants are
            # canonicalized through scidb.provenance.constants_identity_key:
            # a constant may be a dict (a config table declared in
            # scistack_entities.toml), and a bare tuple(sorted(...)) of those
            # items is unhashable.
            key = (
                output_type,
                tuple(sorted(input_types.items())),
                constants_identity_key(constants),
            )

            if key not in seen:
                seen.add(key)
                result_variants.append(result_variant)

        # Note: schema_filter is intentionally NOT applied here because
        # for_each handles schema filtering via **metadata_iterables.
        # The schema_filter parameter is kept for API compatibility with the
        # original plan, but filtering by schema happens at execution time,
        # not during variant selection.

        return result_variants

    def get_upstream_provenance(
        self,
        record_id: str,
        max_depth: int = 20,
    ) -> list[dict]:
        """
        Traverse the full upstream provenance chain for a record.

        Walks backwards through the pipeline over the bipartite provenance graph:
        each record's producing invocation names its input records directly, so
        the chain is followed by stored edges (no metadata heuristics).

        Args:
            record_id: The record_id to trace backwards from.
            max_depth: Maximum number of hops to follow (guards against cycles).

        Returns:
            Flat list of provenance nodes ordered from the queried record
            outward (BFS order). Each dict has keys:
                record_id     (str),
                variable_type (str),
                schema        (dict),
                branch_params (dict),
                function_name (str | None),
                constants     (dict),
                depth         (int, 0 = queried record),
                inputs        (list of {record_id, param_name, variable_type})

        Reimplemented over the bipartite provenance graph (every edge is a
        stored fact); the old branch_params-subset heuristic is gone.
        """

        return provenance_query.upstream_provenance(self, record_id, max_depth)

    def get_pipeline(self, record_id: str, max_depth: int = 20) -> dict:
        """Reconstruct the full upstream pipeline DAG for ``record_id`` (§8).

        Returns ``{"nodes": [...], "edges": [...]}`` where nodes use the
        :meth:`get_upstream_provenance` shape and edges are
        ``{from_record_id, to_record_id, param_name}``. Provably correct —
        every edge is a stored fact, terminating at raw data / constants.
        """

        return provenance_query.pipeline(self, record_id, max_depth)

    def get_derived_branch_params(self, record_id: str, max_depth: int = 20) -> dict:
        """Derive the ``{function.param: value}`` branch_params for a record (§6).

        Walks the invocation graph upward collecting constant inputs. This is
        the exact map the old ``branch_params`` column stored, now derived.
        """

        return provenance_query.derived_branch_params(self._duck, record_id, max_depth)

    def get_execution_audit(self, record_id: str) -> list[dict]:
        """List every execution that (re)produced ``record_id`` (§9b).

        Each entry: ``{timestamp, user_id, where_clause, function_name}``,
        oldest first. Re-runs append rows, so a changed ``where=`` filter is
        preserved rather than lost to first-wins.
        """

        return provenance_query.execution_audit(self._duck, record_id)

    def invocation_exists(self, invocation_id: str) -> bool:
        """True if an invocation (unique function call) is already recorded.

        The §9c membership test: an invocation and its ``_invocation_output``
        rows are written together, so *invocation present ⇒ outputs present*.
        ``skip_computed`` and node-completeness both use this.
        """
        rows = self._duck._fetchall(
            "SELECT 1 FROM _invocation WHERE invocation_id = ? LIMIT 1",
            [invocation_id],
        )
        return bool(rows)

    def has_lineage(self, record_id: str) -> bool:
        """Check if a record has provenance.

        True if the record was produced by a recorded invocation in the
        bipartite graph (all computed records — including ``generates_file`` —
        now write an invocation). Raw / manually saved records return False.
        """

        return provenance_query.has_producing_invocation(self._duck, record_id)

    def find_record_id(
        self,
        variable_class: type,
        metadata: dict,
        branch_params_filter: dict | None = None,
    ) -> str | None:
        """Lightweight lookup returning the record_id of the latest record for
        a variable + metadata combination, without loading any data.

        Args:
            variable_class: The BaseVariable subclass to look up.
            metadata: Flat dict of schema keys (and optionally non-schema kwargs,
                matched as branch params against the graph).
            branch_params_filter: Optional namespaced branch_params dict
                (e.g. ``{"bandpass_filter.low_hz": 20}``) for variant
                disambiguation via suffix matching against graph-derived branch
                params (the producing invocation's constants).

        Returns None if no matching record exists.
        """
        nested = self._split_metadata(metadata)
        rows = self._find_record(
            variable_class.__name__,
            nested_metadata=nested,
            version_id="latest",
            branch_params_filter=branch_params_filter or None,
        )
        if rows.empty:
            return None
        return rows.iloc[0]["record_id"]

    def get_latest_record_id_for_variant(self, used_record_id: str) -> str | None:
        """Given a record_id, find the most recently saved record that shares the
        same ``(variable_name, schema_id, producing-variant)``.

        This is the "current latest" for that specific variable variant —
        the same record that ``load(..., version_id="latest")`` would return.
        The variant is derived from the bipartite graph (the producing
        invocation's constants, or ``None`` for raw records — see
        :func:`provenance_query._producing_variant_key`), so direct-save kwargs
        (anchored as graph constants) distinguish variants too. Returns None if
        the record no longer exists.
        """

        rows = self._duck._fetchall(
            "SELECT type, schema_id FROM _record WHERE record_id = ? LIMIT 1",
            [used_record_id],
        )
        if not rows:
            return None
        vn, sid = rows[0][0], rows[0][1]

        target_variant = provenance_query._producing_variant_key(
            self._duck, used_record_id
        )

        # Candidates at the same (variable_name, schema_id), newest first. Recency
        # comes from the save-event log; type/schema/excluded from the _record entity.
        candidates = self._duck._fetchall(
            "SELECT rm.record_id FROM _record_save rm "
            "JOIN _record r ON r.record_id = rm.record_id "
            "WHERE r.type = ? AND r.schema_id IS NOT DISTINCT FROM ? "
            "AND COALESCE(r.excluded, FALSE) = FALSE "
            "ORDER BY rm.timestamp DESC",
            [vn, sid],
        )
        for (rid,) in candidates:
            if (
                provenance_query._producing_variant_key(self._duck, rid)
                == target_variant
            ):
                return rid
        return None

    # -------------------------------------------------------------------------
    # Export Methods
    # -------------------------------------------------------------------------

    def export_to_csv(
        self,
        variable_class: type[BaseVariable],
        path: str,
        **metadata,
    ) -> int:
        """Export matching variables to a CSV file."""
        results = list(self.load(variable_class, metadata))

        if not results:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching metadata: {metadata}"
            )

        all_dfs = []
        for var in results:
            df = variable_class(var.data).to_db()
            df["_record_id"] = var.record_id
            if var.metadata:
                for key, value in var.metadata.items():
                    df[f"_meta_{key}"] = value
            all_dfs.append(df)

        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_csv(path, index=False)

        return len(results)

    def _get_variable_class(self, type_name: str):
        """Get a variable class by name (class name, not table name)."""
        if type_name in self._registered_types:
            return self._registered_types[type_name]

        return BaseVariable.get_subclass_by_name(type_name)

    def schema_key_is_numeric(self, key: str) -> bool:
        """Whether loaded values of ``key`` are restored to numbers.

        The rule, in order:

        1. a declared type (``schema_key_types``) wins — ``"numeric"`` or
           ``"string"``;
        2. otherwise the key is numeric only if EVERY value stored for it
           round-trips through :func:`from_schema_str` unchanged in spelling.
           One zero-padded ``"01"`` among the values makes the whole key a
           string key, so ``"10"`` beside it stays ``"10"`` rather than
           becoming 10 and giving the column two types.

        Cached per key; the cache is dropped whenever records are saved, since
        a new value can change the answer (a first ``"01"`` after ``1, 2``).
        """
        cache = self.__dict__.setdefault("_schema_key_numeric_cache", {})
        if key in cache:
            return cache[key]
        declared = (self.dataset_schema_key_types or {}).get(key)
        if declared in ("numeric", "string"):
            numeric = declared == "numeric"
            reason = f"declared {declared}"
        else:
            values = [v for v in self._duck.distinct_schema_values(key) if v is not None]
            kept = [v for v in values if isinstance(from_schema_str(v), str)]
            numeric = bool(values) and not kept
            reason = (
                "no values stored yet"
                if not values
                else "every stored value is a plain number"
                if numeric
                else f"{len(kept)} of {len(values)} stored value(s) keep their spelling "
                f"(e.g. {kept[0]!r})"
            )
        Log.debug(
            "schema key %r loads as %s: %s",
            key,
            "numbers" if numeric else "strings",
            reason,
        )
        cache[key] = numeric
        return numeric

    def restore_schema_value(self, key: str, value):
        """A loaded schema value with the type its KEY has — see
        :meth:`schema_key_is_numeric`. The one restore every load path uses."""
        if value is None or not isinstance(value, str):
            return value
        return from_schema_str(value) if self.schema_key_is_numeric(key) else value

    def _forget_schema_key_kinds(self) -> None:
        """Drop the per-key numeric/string cache — called after any save."""
        self.__dict__.pop("_schema_key_numeric_cache", None)

    def distinct_schema_values(self, key: str) -> list:
        """Return all distinct values stored for a schema key.

        Args:
            key: A schema key name (e.g. "subject", "session")

        Returns:
            Distinct non-null values for that key: the levels declared in the
            project's ``[schema_keys]`` first, in that order, then the rest in
            the database's own (sorted) order.

        The declared order applies here, and not only to loaded frames,
        because this is where ``for_each(subject=[], session=[])`` gets its
        levels — so it decides the iteration order, and therefore the row
        order of every table ``for_each`` hands back, and the GUI's level lists.
        """
        from .schema_order import order_levels

        return order_levels(
            key,
            self._duck.distinct_schema_values(key),
            declared=self.dataset_schema_key_order or {},
            fallback=lambda values: values,
        )

    def distinct_schema_combinations(self, keys: list[str]) -> list[tuple]:
        """Return all distinct combinations for multiple schema keys.

        Args:
            keys: List of schema key names (e.g. ["subject", "session"])

        Returns:
            List of tuples of distinct non-null value combinations (strings)
        """
        return self._duck.distinct_schema_combinations(keys)

    def list_variables(self) -> "pd.DataFrame":
        """Return all variable types stored in this database.

        Queries the ``_variables`` table and returns a DataFrame with columns:
        ``variable_name``, ``schema_level``, ``created_at``, ``description``.

        Useful for discovering what variable types exist in a database file
        without needing the original Python class definitions.
        """
        return self._duck.list_variables()

    # -------------------------------------------------------------------------
    # Variable Groups
    # -------------------------------------------------------------------------

    @staticmethod
    def _resolve_var_name(v) -> str:
        """Resolve a single variable to its name string.

        Accepts a Python str, a BaseVariable subclass (class object),
        or a MATLAB BaseVariable instance (matlab.object with class name).
        """
        if isinstance(v, str):
            return v
        if isinstance(v, type) and issubclass(v, BaseVariable):
            return v.table_name()
        # MATLAB objects cross the bridge as matlab.object; try str()
        # to extract the class name (e.g. "StepLength").
        s = str(v)
        if s:
            return s
        raise TypeError(f"Expected a string or BaseVariable subclass, got {type(v)}")

    @staticmethod
    def _resolve_var_names(variables) -> list:
        """Resolve a single or list/iterable of variables to name strings.

        Each element can be a string, a BaseVariable subclass, or a MATLAB
        object.  Accepts Python lists, MATLAB cell arrays, and MATLAB string
        arrays (any iterable).
        """
        # Scalar: single string or single class
        if isinstance(variables, (str, type)):
            return [DatabaseManager._resolve_var_name(variables)]
        # Any iterable (Python list, MATLAB cell array, MATLAB string array)
        try:
            return [DatabaseManager._resolve_var_name(v) for v in variables]
        except TypeError:
            # Not iterable — treat as a single item
            return [DatabaseManager._resolve_var_name(variables)]

    def add_to_var_group(self, group_name: str, variables):
        """Add one or more variables to a variable group.

        Args:
            group_name: Name of the group.
            variables: A BaseVariable subclass, a variable name string,
                or a list of either.
        """
        self._duck.add_to_group(group_name, self._resolve_var_names(variables))

    def remove_from_var_group(self, group_name: str, variables):
        """Remove one or more variables from a variable group.

        Args:
            group_name: Name of the group.
            variables: A BaseVariable subclass, a variable name string,
                or a list of either.
        """
        self._duck.remove_from_group(group_name, self._resolve_var_names(variables))

    def list_var_groups(self) -> list:
        """List all variable group names.

        Returns:
            Sorted list of distinct group names.
        """
        return self._duck.list_groups()

    def get_var_group(self, group_name: str) -> list:
        """Get all variable classes in a variable group.

        Args:
            group_name: Name of the group.

        Returns:
            Sorted list of BaseVariable subclasses in the group.
        """
        names = self._duck.get_group(group_name)
        classes = []
        for name in names:
            cls = BaseVariable.get_subclass_by_name(name)
            if cls is None:
                raise NotRegisteredError(
                    f"Variable '{name}' in group '{group_name}' has no "
                    f"registered BaseVariable subclass."
                )
            classes.append(cls)
        return classes

    def close(self):
        """Close the database connection."""
        self._duck.close()
        # remove global reference
        if getattr(_local, "database", None) is self:
            self._closed = True

    def reopen(self):
        # reopen DuckDB (preserving read-only mode)
        if self._duck is None:
            self._duck = SciDuck(
                self.dataset_db_path,
                dataset_schema=self.dataset_schema_keys,
                read_only=getattr(self, "read_only", False),
            )
        else:
            self._duck.reopen()
        self._closed = False

    @property
    def inspect(self):
        """Lazy read-side observability facade (scidb.inspect.Inspector)."""
        if self._inspector is None:
            from .inspect.api import Inspector

            self._inspector = Inspector(self)
        return self._inspector

    def pipeline(self, name: str, uses=()) -> "Pipeline":
        """Create a named Pipeline bound to this database and ACTIVATE it.

        While active, ``for_each`` calls register as deferred steps instead
        of executing (see ``scidb.pipeline.Pipeline``); ``run_all()``/
        ``run_until()`` execute and deactivate. ``uses=`` declares other
        Pipelines as dependencies: their steps join this pipeline's graph.
        """
        from .pipeline import Pipeline

        return Pipeline(name, db=self, uses=uses).activate()

    def set_current_db(self):
        """Set this DatabaseManager as the active global database."""
        set_current_database(self)
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
