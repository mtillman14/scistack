# Multi-Column Save: Schema Inference, Skip-on-Mismatch, and the Length-1 Unwrap

This documents how `save_batch` stores **dict-valued** variables (`multi_column`
mode), what happens when records in a batch disagree on shape, and a subtle
length-1-array normalization that must be kept symmetric between two functions.

Relevant code:
- `sciduckdb/src/sciduckdb/sciduckdb.py`: `infer_data_columns`,
  `_python_to_storage`, `value_to_storage_row`, `_storage_signature`,
  `record_schema_mismatch`
- `scidb/src/scidb/database.py`: `save_batch`
- `scidb/src/scidb/foreach.py`: `_batch_save_outputs` (the for_each save phase)

---

## 1. Storage modes

`infer_data_columns(value)` picks one of three modes from a **single sample**:

| Sample value      | Mode            | Columns                                   |
|-------------------|-----------------|-------------------------------------------|
| `dict`            | `multi_column`  | one DuckDB column per dict key             |
| `pd.DataFrame`    | `dataframe`     | one DuckDB column per frame column         |
| anything else     | `single_column` | one column named `value`                   |

This note is about **`multi_column`** — e.g. a Delsys EMG record
`{"RHAM": np.ndarray, "RVL": np.ndarray, ...}` becomes a table with one
`DOUBLE[]` column per muscle.

---

## 2. The schema is inferred from the FIRST record

In `save_batch`, the column set and per-column types come from the **first
record only** (`data_col_types, dtype_meta = infer_data_columns(first_data)`),
and that `dtype_meta` is persisted in `_variables`. Consequences:

- Every other record in the batch is assumed to have the **same dict keys** and
  the **same per-column shape**.
- A record missing a key → `data_val[key]` `KeyError` in the row-build loop.
- A record with an **extra** key → silently dropped (the loop only iterates the
  first record's columns) unless validation rejects it first.

Because the batch insert is **atomic**, a single bad record used to abort the
entire batch (the original Delsys bug: two empty EMG files → empty dicts →
`KeyError('RHAM')` → 0 of 268 saved).

### Reference-schema selection
If the **first** record is degenerate (an empty dict → no columns), it must not
define the table. `save_batch` falls back to the first record that yields a
non-empty schema. If *no* record has a usable schema (all empty), the whole
batch is skipped and `save_batch` returns all `None`.

---

## 3. Contract: incompatible records are SKIPPED with a warning

Before the heavy save loop, `save_batch` validates each record against the
reference schema via `record_schema_mismatch(ref_col_types, rec_col_types)`.
A record is **skipped (not saved, not fatal)** when it would break the insert:

- **empty / partial dict** → missing keys
- **unexpected keys** → extra keys
- **shape/type change** → e.g. a scalar where the column stores a vector, or a
  string where the column is numeric

Skipped records produce a per-record `Log.warn` naming the record's metadata and
the reason. The rest of the batch is saved normally.

### Return-value alignment
`save_batch` returns `record_ids` aligned to the **original input order**, with
`None` in each skipped slot. Callers must tolerate `None`:
- `foreach.py` zips items with record_ids guarded by `isinstance(rid, str)` and
  reports a saved/skipped summary to console + log.
- `scimatlab/bridge.py::save_batch_bridge` emits an empty string for `None`
  slots so the newline-delimited result stays row-aligned on the MATLAB side.

### How "would it break?" is decided — `_storage_signature`
Comparison is by a **coarse signature** `(category, array_depth)`, not exact
type:
- `array_depth` = count of trailing `[]` (0 scalar, 1 vector, 2 matrix).
- `category` buckets numeric / string / json / temporal / other.

So `BIGINT` vs `DOUBLE` are compatible (both `numeric`, depth 0 — DuckDB
coerces), but `DOUBLE` vs `DOUBLE[]` (shape change) and `DOUBLE` vs `VARCHAR`
(category change) are rejected. The goal is to skip only records that genuinely
can't share the column, not ones DuckDB would coerce.

**Note:** `dataframe` mode is intentionally **not** validated this way (an empty
frame infers `VARCHAR` columns and would false-positive against real-typed
frames). It has its own one-row-per-frame-row handling.

---

## 4. The length-1 array unwrap — keep it SYMMETRIC

`infer_data_columns` deliberately unwraps a length-1 ndarray to a scalar when
choosing the column type:

```python
if isinstance(val, np.ndarray) and val.size == 1:
    val = val.item()          # np.array([1.0]) -> 1.0 -> DOUBLE (not DOUBLE[])
```

This exists because MATLAB sends a scalar as a 1×1 array, and we want it stored
as a scalar `DOUBLE`, not `DOUBLE[]`.

The trap: the **write** path (`_python_to_storage`) must apply the **same**
unwrap. If inference says `DOUBLE` (scalar) but storage writes the original
`np.array([1.0])` (a `DOUBLE[]`), DuckDB rejects the row:

```
Conversion Error: Unimplemented type for cast (DOUBLE[] -> DOUBLE)
```

Fix (kept symmetric): `_python_to_storage` unwraps a size-1 ndarray / `np.generic`
to a scalar when the column's `python_type` is `float`/`int`/`bool`/`str`.

### Round-trip implication
A length-1 array dict value **round-trips as a scalar**, by design:
`{"x": np.array([1.0])}` → saved into scalar `DOUBLE` → loaded as `{"x": 1.0}`.
The scalar-storage intent is preserved; the 1-element array container is not.

### Why only `multi_column` was affected
`single_column` and `dataframe` modes don't unwrap on **either** side, so they're
internally consistent. Only `multi_column` inference unwrapped while storage did
not — hence the bug appeared only for dict values that were specifically a
size-1 ndarray (real EMG/time-series vectors are long, so it stayed hidden).

### Knock-on for validation
Because `record_schema_mismatch` infers per-record types the same way, a batch
mixing a 1-sample dict value (scalar column) with multi-sample ones (`DOUBLE[]`)
flags the 1-sample record as a **shape mismatch** and skips it. Consistent
behavior, and an extreme edge case for real time-series, but worth knowing.

---

## 5. Tests

`scidb/tests/test_integration.py`:
- `TestSaveBatchSchemaValidation` — empty dict, partial keys, shape mismatch,
  leading-empty reference, all-empty (all `None`).
- `TestSaveBatchSingleElementArrayDict` — length-1 float/int array dict values
  round-trip as scalars.
