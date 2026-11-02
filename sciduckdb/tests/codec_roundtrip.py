"""Store one value through the sciduckdb codec in a real DuckDB table, read it back.

The codec is SciDuck's public surface (``infer_data_columns``,
``value_to_storage_row`` / ``dataframe_to_storage_rows``, ``merge_tz_columns``,
``restore_types``, ``storage_to_python``, ``unflatten_dict``); scidb's save and
load compose the same functions, and ``fetch`` mirrors scidb's load branch by
branch. A real table matters: NULL list elements and datetime companions only
exist once DuckDB has stored the value.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from sciduckdb import (
    SciDuck,
    dataframe_to_storage_rows,
    infer_data_columns,
    is_datetime_meta,
    merge_tz_columns,
    restore_types,
    storage_column_names,
    storage_to_python,
    unflatten_dict,
    value_to_storage_row,
)


def store(duck: SciDuck, table: str, value: Any) -> dict:
    """Create ``table`` for ``value``, insert it, register it in ``_variables``.

    Returns the ``dtype_meta`` that :func:`fetch` needs.
    """
    col_types, dtype_meta = infer_data_columns(value)
    cols_sql = ", ".join(f'"{c}" {t}' for c, t in col_types.items())
    duck.execute(f'CREATE TABLE "{table}" ({cols_sql})')
    col_str = ", ".join(f'"{c}"' for c in col_types)
    placeholders = ", ".join(["?"] * len(col_types))
    insert = f'INSERT INTO "{table}" ({col_str}) VALUES ({placeholders})'
    if dtype_meta.get("mode") == "dataframe":
        rows = dataframe_to_storage_rows(value, dtype_meta)
    else:
        rows = [value_to_storage_row(value, dtype_meta)]
    for row in rows:
        duck.execute(insert, row)
    duck.execute(
        "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
        "VALUES (?, ?, ?, '')",
        [table, duck.dataset_schema[-1], json.dumps(dtype_meta)],
    )
    return dtype_meta


def fetch(duck: SciDuck, table: str, dtype_meta: dict) -> Any:
    """Read the value :func:`store` wrote, restored to its Python types."""
    select = ", ".join(f'"{c}"' for c in storage_column_names(dtype_meta))
    df = duck.fetchdf(f'SELECT {select} FROM "{table}"')
    merge_tz_columns(df, dtype_meta, where=f"fetch({table})")
    mode = dtype_meta.get("mode", "single_column")
    columns_meta = dtype_meta.get("columns", {})

    if mode == "dataframe":
        result = {}
        for c, meta in columns_meta.items():
            if is_datetime_meta(meta):
                result[c] = df[c].reset_index(drop=True)
            else:
                result[c] = [storage_to_python(v, meta) for v in df[c].tolist()]
        df_columns = dtype_meta.get("df_columns", list(columns_meta))
        return pd.DataFrame(result, columns=df_columns)

    df = restore_types(df, dtype_meta)
    if mode == "single_column":
        return df[next(iter(columns_meta))].iloc[0]
    result = {
        c: storage_to_python(df[c].iloc[0], meta) for c, meta in columns_meta.items()
    }
    if dtype_meta.get("nested"):
        return unflatten_dict(result, dtype_meta["path_map"])
    return result


def roundtrip(duck: SciDuck, table: str, value: Any) -> Any:
    return fetch(duck, table, store(duck, table, value))


def column_types(duck: SciDuck, table: str) -> dict:
    """``{column: DuckDB type}`` of ``table``."""
    rows = duck.fetchall(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [table],
    )
    return {r[0]: r[1] for r in rows}
