# DuckDB Column Types for Table (DataFrame) Variables

## The fundamental rule

> **One DuckDB row per DataFrame row. DuckDB column type = cell value type.**

The number of table rows does not affect the DuckDB column type — only the type
of the individual cell value does. This makes the database visually match the
MATLAB table: each row in MATLAB is a row in DuckDB.

---

## Column type mapping

| Cell value type         | DuckDB column type | Example                      |
|-------------------------|--------------------|------------------------------|
| scalar float/double     | `DOUBLE`           | `42.0`                       |
| scalar int              | `BIGINT`           | `7`                          |
| scalar string           | `VARCHAR`          | `"hello"`                    |
| 1-D numeric array       | `DOUBLE[]`         | `[1.0, 2.0, 3.0]`            |
| 1-D string array        | `VARCHAR[]`        | `["a", "b"]`                 |
| 2-D numeric array       | `DOUBLE[][]`       | `[[1,2],[3,4]]`              |

---

## Physical storage examples

**3-row table, scalar double column `A`:**
```
record_id  |  A
-----------+------
abc123     |  1.0
abc123     |  2.0
abc123     |  3.0
```
DuckDB column type: `DOUBLE`

**3-row table, vector column `sig` (1-D array per cell):**
```
record_id  |  sig
-----------+----------------
abc123     |  [1.0, 2.0, 3.0]
abc123     |  [4.0, 5.0, 6.0]
abc123     |  [7.0, 8.0, 9.0]
```
DuckDB column type: `DOUBLE[]`

**1-row table, scalar column `x`:**
```
record_id  |  x
-----------+------
abc123     |  42.0
```
DuckDB column type: `DOUBLE`

All N rows from one `.save()` call share the same `record_id`. Rows from
`for_each(distribute=true)` have distinct `record_id`s because each row is saved
under a different schema key (e.g. different session number).

---

## record_id is NOT a primary key for DataFrame tables

```sql
CREATE TABLE "MyVar_data" (
    record_id  VARCHAR NOT NULL,   -- NOT PRIMARY KEY
    col_A      DOUBLE,
    col_B      VARCHAR
)
```

Multiple DuckDB rows share the same `record_id` (one per table row). Idempotency
is enforced by skipping all inserts for a `record_id` that already exists.

Non-DataFrame native types (scalars, arrays, dicts) still use `record_id VARCHAR
PRIMARY KEY` since they always occupy exactly one DuckDB row.

---

## dtype_meta format

```json
{
  "mode": "dataframe",
  "columns": {
    "A":   {"python_type": "float"},
    "sig": {"python_type": "ndarray", "numpy_dtype": "float64", "ndim": 1},
    "lbl": {"python_type": "str"}
  },
  "df_columns": ["A", "sig", "lbl"]
}
```

No `n_rows` field. The row count is implicit in the number of DuckDB rows
matching the `record_id`.

---

## for_each cases

| Scenario                            | DuckDB rows per record | record_id |
|-------------------------------------|------------------------|-----------|
| `.save()` of N-row table            | N                      | shared    |
| `for_each(distribute=false)`, N-row | N                      | shared    |
| `for_each(distribute=true)`, N-row  | 1 per call (N calls)   | distinct  |

In the `distribute=true` case, each call saves a 1-row DataFrame, producing
exactly 1 DuckDB row with its own `record_id`.

---

## Key files

- `sciduck/src/sciduck/sciduck.py`: `infer_data_columns` (type inference),
  `dataframe_to_storage_rows` (per-row storage), `SciDuck.save/load`
- `src/scidb/database.py`: `_save_native`, `save_batch`,
  `_load_by_record_row`, `load_all_as_df`
- Tests: `TestTableRoundTrip.m`, `TestDuckDBTypes.m`
