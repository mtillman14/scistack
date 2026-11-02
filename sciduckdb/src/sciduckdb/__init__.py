"""sciduckdb: the codec and the locked connection. It creates no tables.

Modules: `codec` (values <-> DuckDB columns), `connection` (`DuckConnection`),
`datetimes`, `row_lists`. The tables of a project database, `_schema`
included, are scidb's (`scidb.tables`, `scidb.sciduck.SciDuck`). This file is the
facade; import from `sciduckdb`, not from the modules.
"""

from .connection import DuckConnection, TransactionRolledBackError
from .codec import (
    bulk_df_to_storage_rows,
    column_type_change,
    dataframe_to_storage_rows,
    infer_data_columns,
    record_schema_mismatch,
    restore_types,
    storage_to_python,
    storage_to_python_column,
    unflatten_dict,
    value_to_storage_row,
    array_from_storage,
    count_nan_array_elements,
    count_null_list_elements,
)
from .datetimes import (
    DatetimeStorageError,
    is_datetime_meta,
    merge_tz_columns,
    storage_column_names,
    storage_column_types,
    stored_tz_companions,
    tz_column_name,
    tz_companions,
    user_column_types,
)
from .row_lists import (
    ROW_LIST_PTYPE,
    RowListTypeError,
    convert_column_to_row_list,
    is_row_list_meta,
    row_list_change,
    row_list_column_type,
    row_list_meta,
    unwrap_row_list,
)

__all__ = [
    "DuckConnection",
    "TransactionRolledBackError",
    "array_from_storage",
    "count_null_list_elements",
    "count_nan_array_elements",
    "restore_types",
    "storage_to_python",
    "storage_to_python_column",
    "infer_data_columns",
    "record_schema_mismatch",
    "column_type_change",
    "value_to_storage_row",
    "dataframe_to_storage_rows",
    "bulk_df_to_storage_rows",
    "unflatten_dict",
    "DatetimeStorageError",
    "is_datetime_meta",
    "merge_tz_columns",
    "storage_column_names",
    "storage_column_types",
    "stored_tz_companions",
    "tz_column_name",
    "tz_companions",
    "user_column_types",
    "ROW_LIST_PTYPE",
    "RowListTypeError",
    "convert_column_to_row_list",
    "is_row_list_meta",
    "row_list_change",
    "row_list_column_type",
    "row_list_meta",
    "unwrap_row_list",
]
