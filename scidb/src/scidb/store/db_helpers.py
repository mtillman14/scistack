"""DatabaseManager's private module-level helpers: branch-param, code-version and run-option record filters, nested-dict paths, column fingerprints and struct-column flattening.

Moved verbatim out of database.py (grug follow-up, docs/claude/grugbrain-principles.md).
"""

import importlib.util
import json
import numpy as np
import pandas as pd
from scicanonicalhash import canonical_hash
from scidb.model.log import Log
from scidb.provenance import query as provenance_query
from scidb.store.schema_values import schema_str
from typing import Any


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.model.exceptions import AmbiguousParamError
from scidb.model.variant import LATEST_VERSION
from scidb.model.variant import VariantAxes
from scidb.model.variant import match_bare_name
def _pyarrow_available() -> bool:
    """Is the optional PyArrow fast path for batch inserts installed?"""
    return importlib.util.find_spec("pyarrow") is not None


def _log_first_content_hash(type_name: str, data_val: Any, content_hash: str) -> None:
    """Diagnostic (first record of a batch only): what is being content-hashed.

    A DataFrame whose ``to_numpy()`` dtype is ``object`` (mixed-type columns)
    was the source of non-deterministic hashes — object arrays hash Python
    pointer bytes — so this line makes the culprit (object dtype, column
    order, index) observable.
    """
    try:
        if isinstance(data_val, pd.DataFrame):
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
    except Exception as exc:  # a diagnostic must never fail a save
        Log.debug(f"[content_hash] {type_name}: diagnostic skipped: {exc}")


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
      :func:`~scidb.provenance.query.code_version_ordinals`, which is scoped per
      *function* so ``v2`` names the same code everywhere;
    * ``"latest"`` uses ``variant_identity_batch``'s ``is_latest``, which is
      chain-wide and resolved per schema *location* — so a location never re-run
      under the newest code keeps its own newest record instead of dropping out
      of the run entirely.
    """

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
    :func:`~scidb.provenance.query.run_options_label` string
    (``"distribute=true"``) or ``"latest"``.

    ``"latest"`` is the same chain-wide, per-location ``is_latest`` that
    ``code_version="latest"`` uses — there is ONE notion of "current at this
    location", and since 2026-09-14 it already accounts for run options, so
    the two spellings deliberately share a resolution rather than growing a
    second one.

    A label pin compares exactly, per named function, against
    :func:`~scidb.provenance.query.run_options_batch`. Bare pins resolve against
    :func:`~scidb.provenance.query.run_option_axes` the way bare code pins
    resolve against ``code_version_ordinals``: exactly one function that ran
    more than one way is unambiguous, several raise, none is a no-op.
    """

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
        # Exact, except a pin naming no level matches any (`run_options_label_matches`,
        # the one rule): "distribute=true" matches at any level.
        match = provenance_query.run_options_label_matches

        def _wanted(label) -> bool:
            return any(match(pin, label) for pin in wanted_set)

        if not any(_wanted(label) for label in available):
            raise ValueError(
                f"{fn_name!r} ran under {available}; run_options={wanted!r} "
                f"matches nothing."
            )
        keep = {
            rid
            for rid in record_ids
            if _wanted(runs.get(rid, {}).get(fn_name))
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


def _sample_values(frames: list, col, limit: int = 3) -> list:
    """Up to ``limit`` distinct non-empty values of column ``col`` across
    ``frames`` — the examples a ColumnTypeChangedError shows."""
    seen: list = []
    for frame in frames:
        if not isinstance(frame, pd.DataFrame) or col not in frame.columns:
            continue
        for v in frame[col].dropna().tolist():
            if v not in seen:
                seen.append(v)
                if len(seen) >= limit:
                    return seen
    return seen


def _value_fingerprint(value) -> str:
    """Comparable stand-in for one column's value in _differing_columns."""
    if isinstance(value, pd.Series):
        return repr(value.tolist())
    try:
        return canonical_hash(value)
    except Exception:  # noqa: BLE001 — only used to name differing columns
        return repr(value)


def _differing_columns(values: list) -> list[str]:
    """Data columns whose values differ across ``values`` (the records that
    collide at one location). DataFrames compare per column, dicts per key;
    any other kind has no columns to name, so the list is empty."""
    if all(isinstance(v, pd.DataFrame) for v in values):
        columns = list(dict.fromkeys(c for v in values for c in v.columns))
        cells = lambda v, c: v[c].reset_index(drop=True) if c in v.columns else None  # noqa: E731
    elif all(isinstance(v, dict) for v in values):
        columns = list(dict.fromkeys(k for v in values for k in v))
        cells = lambda v, c: v.get(c)  # noqa: E731
    else:
        return []
    return [
        str(c)
        for c in columns
        if len({_value_fingerprint(cells(v, c)) for v in values}) > 1
    ]


def _data_column_names(value) -> set[str]:
    """The column names a saved value carries (DataFrame columns, dict keys);
    empty for any other kind."""
    if isinstance(value, pd.DataFrame):
        return {str(c) for c in value.columns}
    if isinstance(value, dict):
        return {str(k) for k in value}
    return set()


def _colliding_locations(
    data_items: "list[tuple[Any, dict]]", schema_keys: "list[str]"
) -> "tuple[list[dict], dict]":
    """THE rule for "two records at one location" within one save batch
    (docs/claude/duplicate-location-vs-stale.md).

    Items collide when they share the schema location AND every other
    metadata key (version keys, ``__invocation_id``, ``__branch_params``) but
    their data differ. Different ``__branch_params`` are separate variants and
    byte-identical data merge into one record, so neither collides. Items
    with no schema key are not checked. Only items that share a group are
    hashed, so a batch without collisions costs one pass over its metadata.

    A ``__branch_params`` entry that names one of the item's OWN data columns
    is ignored: for_each's flatten mode copies every scalar data column of a
    spread row into its branch params (foreach.py, "dynamic discriminators"),
    so two differing CSV rows for one subject would otherwise always look like
    two variants and never collide. A variant is told apart by its inputs and
    constants, never by the value it stores.

    Returns ``(collisions, stats)``: ``collisions`` as DuplicateLocationError
    takes them, ``stats`` the counts for the save_batch DEBUG line.
    """
    groups: dict[tuple, list[int]] = {}
    no_location = 0
    for i, (data, meta) in enumerate(data_items):
        location = tuple(
            (k, schema_str(meta[k])) for k in schema_keys if k in meta
        )
        if not location:
            no_location += 1
            continue
        rest_meta = {k: v for k, v in meta.items() if k not in schema_keys}
        bp = rest_meta.get("__branch_params")
        own = _data_column_names(data)
        if isinstance(bp, dict) and own:
            rest_meta["__branch_params"] = {
                k: v for k, v in bp.items() if str(k) not in own
            }
        rest = json.dumps(
            rest_meta,
            sort_keys=True,
            default=str,
        )
        groups.setdefault((location, rest), []).append(i)

    collisions: list[dict] = []
    for (location, _rest), idx in groups.items():
        if len(idx) < 2:
            continue
        distinct: dict[str, Any] = {}
        for i in idx:
            distinct.setdefault(canonical_hash(data_items[i][0]), data_items[i][0])
        if len(distinct) < 2:
            continue
        collisions.append(
            {
                "location": dict(location),
                "count": len(distinct),
                "differing_columns": _differing_columns(list(distinct.values())),
            }
        )
    stats = {
        "items": len(data_items),
        "locations": len({loc for loc, _ in groups}),
        "variant_groups": len(groups),
        "no_location": no_location,
    }
    return collisions, stats


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
