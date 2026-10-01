# A variable keeps the columns of its first save

*Written 2026-10-01. Read this before changing how a DataFrame or dict variable's
table is created or saved into.*

## How a variable's columns are fixed

Each variable is one DuckDB table, `<Type>_data`: `record_id` plus one column
per data column (a DataFrame's columns, or a dict's keys, or `value`). The
first save creates the table from that save's columns. `_variables.dtype` holds
the variable's single column list, and loading rebuilds every record's
DataFrame from it (`_assemble_df_from_records_and_data`). There is no
per-record column set, which is why one table cannot hold records with
different columns.

## What happens when a later save's columns differ

`DatabaseManager._check_column_set(table_name, variable_class, columns)` is the
one owner. It is called before the create-if-missing block in all three save
paths: `_save_columnar`, `_save_native` and `save_batch`.

| situation | result |
|---|---|
| same column set | saves normally |
| different, table holds records | `ColumnSetChangedError`, nothing written |
| different, table holds no rows | table and view dropped, rebuilt with the new columns (INFO log) |

The error names the columns added and missing and how many records exist. It
gives the two ways out: save the output into a **new variable**, or **delete
the variable's existing records** (GUI: Variants popup -> Delete, which deletes
`<Type>_data` rows and cascades downstream) and run again. The empty-table
rebuild is what makes the second one work.

In a `for_each` run the error is caught per output and re-raised inside
`OutputSaveError` (see `matlab-run-completion.md`), so a MATLAB run is reported
failed with this message.

## Before 2026-10-01

- A **new** column failed as DuckDB `Binder Error: Table "X_data" does not have a
  column with name "..."`, which said nothing about why.
- A **missing** column *succeeded*. The insert left it NULL, and the
  `_variables.dtype` overwrite dropped it from the column list, so OLD records
  loaded without it. That was silent data loss on read.

## Not done (user decision 2026-10-01)

Schema evolution (ALTER TABLE ADD COLUMN, a union column list, per-record
column sets) and a confirm-and-delete popup were both considered and declined.
The message explains the fix instead.

Tests: `scidb/tests/test_column_set_changed.py`.
