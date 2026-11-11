# DataFrame Storage Redesign Plan

## Goal

Merge the two DataFrame save paths (native array-packing and custom `to_db()`)
into a single unified path that stores one DuckDB row per MATLAB/pandas table row.
This makes the database visually transparent and enables SQL-level filtering on
individual table rows via `where=`.

---

## Part 1: New table format ↔ DuckDB column format mapping

### The fundamental rule

> **One DuckDB row per DataFrame row. DuckDB column type = cell value type.**

The number of table rows does not affect the DuckDB column type — only the type
of the value stored in each cell does.

### Column type mapping

| Cell value type         | DuckDB column type | Example                           |
|-------------------------|--------------------|-----------------------------------|
| scalar float/double     | `DOUBLE`           | `42.0`                            |
| scalar int              | `BIGINT`           | `7`                               |
| scalar string           | `VARCHAR`          | `"hello"`                         |
| 1-D numeric array       | `DOUBLE[]`         | `[1.0, 2.0, 3.0]`                 |
| 1-D string array        | `VARCHAR[]`        | `["a", "b"]`                      |
| 2-D numeric array       | `DOUBLE[][]`       | `[[1,2],[3,4]]`                   |

This is independent of table height.

### Examples (full tables)

**12-row table, scalar double column `A`:**
```
record_id  |  A
-----------+------
abc123     |  1.0
abc123     |  2.0
...  (12 rows, all same record_id)
```
DuckDB type: `DOUBLE`

**12-row table, vector column `sig` (1-D array per cell):**
```
record_id  |  sig
-----------+----------------
abc123     |  [1.0, 2.0, 3.0]
abc123     |  [4.0, 5.0, 6.0]
...  (12 rows, all same record_id)
```
DuckDB type: `DOUBLE[]`

**1-row table, scalar double column `x`:**
```
record_id  |  x
-----------+------
abc123     |  42.0
```
DuckDB type: `DOUBLE`

**1-row table, vector column `weights`:**
```
record_id  |  weights
-----------+----------------
abc123     |  [0.1, 0.9]
```
DuckDB type: `DOUBLE[]`

### record_id is NOT a primary key for DataFrames

Multiple DuckDB rows share the same `record_id` when they belong to the same
variable record (i.e., one `.save()` call for a multi-row table). The table
schema is:

```sql
CREATE TABLE "MyVar_data" (
    record_id  VARCHAR NOT NULL,
    col_A      DOUBLE,
    col_B      VARCHAR
)
```

No PRIMARY KEY constraint. Before inserting, check `COUNT(*) WHERE record_id = ?`
and skip all inserts if > 0 (idempotency).

---

## Part 2: for_each cases

### `distribute=false` (whole table saved as one record)

A 12-row table → 12 DuckDB rows, all with the same `record_id` and `schema_id`.

### `distribute=true` (each table row saved as a separate record)

Each table row becomes its own `.save()` call with a distinct schema key (e.g.
`session=1`, `session=2`, ...). Each call saves a **1-row** DataFrame → 1 DuckDB
row. The `record_id` is distinct for each row because the metadata (schema) differs.

DuckDB types are identical between `distribute=true` and `distribute=false`:
- scalar column → `DOUBLE`/`VARCHAR`
- vector column → `DOUBLE[]`/`VARCHAR[]`

The **only** difference is whether DuckDB rows share a `record_id` or not.

---

## Part 3: dtype_meta format

```json
{
  "mode": "dataframe",
  "columns": {
    "A":     {"python_type": "float"},
    "label": {"python_type": "str"},
    "sig":   {"python_type": "ndarray", "numpy_dtype": "float64", "ndim": 1}
  },
  "df_columns": ["A", "label", "sig"]
}
```

No `n_rows` field. The row count is encoded in the number of DuckDB rows with
the matching `record_id`. Each DuckDB row corresponds to exactly one DataFrame row.

---

## Part 4: files to change

### `sciduck/src/sciduck/sciduck.py`

1. **`_infer_data_columns`** — DataFrame branch:
   - Remove the multi-row vs 1-row distinction
   - For every column: `_infer_duckdb_type(first_cell)` directly — no extra `[]` wrapping

2. **`_value_to_storage_row`** — DataFrame branch:
   - Currently returns one packed-array value per column (single DuckDB row)
   - Add helper `_dataframe_to_storage_rows(df, dtype_meta) -> list[list]`
     that iterates rows and calls `_python_to_storage(cell, col_meta)` per cell

3. **`SciDuck.load()`** — DataFrame reconstruction:
   - Fetch all DuckDB rows with matching `record_id` → apply `_restore_types`
     per cell → reconstruct DataFrame

### `src/scidb/database.py`

4. **`_save_native`** — DataFrame path:
   - Schema: `record_id VARCHAR NOT NULL` (no PRIMARY KEY)
   - Use `_dataframe_to_storage_rows` to expand DataFrame into per-row inserts
   - Idempotency: skip all inserts if `count(*) WHERE record_id = ? > 0`

5. **`save_batch`** — DataFrame path:
   - Same schema change (no PRIMARY KEY)
   - Expand each DataFrame into multiple `data_table_rows` entries (one per row)
   - Dedup via per-record_id existence check before inserting

6. **`_load_by_record_row`** — DataFrame reconstruction:
   - Fetch all rows `WHERE record_id = ?`
   - Apply `_storage_to_python` per cell; build DataFrame

7. **`load_all`** — bulk DataFrame reconstruction:
   - Fetch chunk rows; groupby `record_id`; reconstruct each group into a DataFrame

### Tests

8. **`TestTableRoundTrip.m`** — DuckDB type assertions change:
   - Multi-row scalar columns: `DOUBLE[]` → `DOUBLE`, `VARCHAR[]` → `VARCHAR`
   - Multi-row vector columns: `DOUBLE[][]` → `DOUBLE[]`
   - 1-row cases: unchanged
   - `for_each(distribute=true)` and `distribute=false` now produce identical column types

---

## Part 5: what does NOT change

- Custom `to_db()`/`from_db()` path (`_save_columnar`): already uses multi-row
  approach; remains untouched
- `for_each(distribute=true)` semantics: each row still gets a distinct record_id
- `where=` filter: works better now since individual rows are queryable via SQL
- Schema keys / `_record_metadata` / `_schema` tables: no change
- Non-DataFrame native types (scalars, arrays, dicts): `record_id PRIMARY KEY`
  one-row storage is unchanged; this redesign is DataFrame-only
