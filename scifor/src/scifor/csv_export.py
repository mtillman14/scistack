"""Flat-table export for ``scifor.Merge``: join constituents into one frame (THE Merge rule).

``merge_to_dataframe`` builds the joined pandas DataFrame; ``export_merge_csv`` is
a thin wrapper that writes it to a ``.csv``. Both back ``Merge.as_df`` and
``Merge.to_csv`` respectively.

The join is THE Merge rule (``scifor.merge.merge_frames``; spec:
docs/claude/merge-cases.json), the same one the per-combo loop uses: join on
shared schema columns, a shared data column or a key present on one side only
is an error (never a silent drop). The only difference here is
``positional=False``: whole tables have no row-position meaning, so
constituents sharing no schema column are an error.
"""

from typing import Any

from scistacklog import Log

from .merge import merge_frames


def merge_to_dataframe(
    merge: Any,
    where=None,
    _log_fn: "Any" = None,
    verbose: bool = False,
    **metadata: Any,
) -> "Any":
    """Join a ``Merge``'s constituents and return the joined DataFrame.

    Args:
        merge: A ``scifor.Merge`` instance.
        where: Optional scifor ColName/Col filter applied once to the joined
            table (so it may reference columns from any constituent).
        _log_fn: Deprecated — ignored. Diagnostics go through the scistacklog
            facade (layer="scifor"): DEBUG normally, INFO when verbose=True.
        verbose: If True, join diagnostics are logged at INFO (visible at the
            default level) instead of DEBUG.
        **metadata: Row filters applied per constituent on any matching schema
            column. A scalar matches by equality; a list/tuple/set matches by
            membership. Keys absent from a given constituent are skipped.

    Returns:
        The joined pandas DataFrame with one copy of the shared schema
        columns plus every selected data column.
    """
    import pandas as pd

    from .foreach import (
        _all_data_columns,
        _apply_exclusions,
        _apply_where_filter,
        _excluded_columns,
        _resolve_data_spec,
    )
    from .schema import get_schema

    def _log(msg: str) -> None:
        if verbose:
            Log.info(msg, layer="scifor")
        else:
            Log.debug(msg, layer="scifor")

    schema_keys = get_schema()
    _log(f"merge: schema keys = {schema_keys or '(unset)'}")

    parts: list[tuple[str, pd.DataFrame]] = []
    for i, spec in enumerate(merge.tables):
        label = f"merge[{i}]"

        df, effective_metadata, column_selection = _resolve_data_spec(spec, metadata)
        excl = _excluded_columns(spec)

        filtered = _filter_rows_by_metadata(df, effective_metadata)

        if column_selection is not None:
            # An empty selection means "all data columns".
            cols = column_selection or _all_data_columns(filtered, schema_keys)
            cols = _apply_exclusions(cols, excl)
            missing = [c for c in cols if c not in filtered.columns]
            if missing:
                raise KeyError(
                    f"Column(s) {missing} not found in {label}. "
                    f"Available: {list(filtered.columns)}"
                )
        else:
            cols = _all_data_columns(filtered, schema_keys)

        # Keep schema columns present (needed as join keys) plus the selected
        # data columns, preserving original column order.
        schema_present = [c for c in filtered.columns if c in schema_keys]
        data_cols = [c for c in cols if c not in schema_present]
        keep = schema_present + data_cols
        part_df = filtered[keep].reset_index(drop=True)

        _log(
            f"merge: {label} -> {part_df.shape[0]} row(s) x {part_df.shape[1]} "
            f"col(s); schema={schema_present}, data={data_cols}"
        )
        parts.append((label, part_df))

    # THE Merge rule, over whole tables (no positional fallback).
    labels = [label for label, _ in parts]
    _log(f"merge: joining {labels} on shared schema columns")
    acc = merge_frames(
        [part for _, part in parts],
        schema_keys or None,
        labels=labels,
        positional=False,
    )

    # The where= predicate spans the merged row, so apply it once to the joined
    # table (a constituent need not contain every column the filter references).
    if where is not None:
        before = len(acc)
        acc = _apply_where_filter(acc, where)
        _log(f"merge: where= filter kept {len(acc)}/{before} row(s)")

    _log(f"merge: result {acc.shape[0]} row(s) x {acc.shape[1]} col(s)")
    return acc


def export_merge_csv(
    merge: Any,
    filename: str,
    where=None,
    _log_fn: "Any" = None,
    verbose: bool = False,
    **metadata: Any,
) -> None:
    """Join a ``Merge``'s constituents and write the result to ``filename``.

    Thin wrapper over ``merge_to_dataframe``; ``filename`` must end with ``.csv``.
    """
    if not isinstance(filename, str) or not filename.endswith(".csv"):
        raise ValueError(
            f"to_csv() filename must be a string ending with '.csv', got {filename!r}."
        )

    df = merge_to_dataframe(merge, where=where, verbose=verbose, **metadata)

    _write_log = Log.info if verbose else Log.debug
    _write_log(
        "to_csv: writing %d row(s) x %d col(s) to %r",
        df.shape[0],
        df.shape[1],
        filename,
        layer="scifor",
    )
    df.to_csv(filename, index=False)


def _filter_rows_by_metadata(df: "Any", metadata: dict) -> "Any":
    """Filter df rows by metadata for any matching column (scalar or list).

    Same key-match rule as ``_filter_df_for_combo`` (``scifor.keys``), and
    list/tuple/set values match by membership (any value matches). Keys not
    present as columns in ``df`` are skipped.
    """
    import numpy as np

    from .keys import series_mask

    if len(df.columns) == 0:
        return df.reset_index(drop=True)

    mask = np.ones(len(df), dtype=bool)
    for key, val in metadata.items():
        if key not in df.columns:
            continue
        values = list(val) if isinstance(val, (list, tuple, set)) else [val]
        col_mask = np.zeros(len(df), dtype=bool)
        for v in values:
            col_mask |= series_mask(df[key], v)
        mask &= col_mask
    return df[mask].reset_index(drop=True)
