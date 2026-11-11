# Save Process Speedup Plan

## 1. The Problem in One Sentence

`save_from_table` crosses the MATLAB→Python boundary **once per record**, but the
data is naturally column-oriented — causing O(N × columns) bridge crossings when
O(columns) would suffice.

---

## 2. Current Architecture (Conceptual)

`save_from_table` is **row-oriented** at the MATLAB→Python boundary:

```
for i = 1 : N_records
    py_data.append( to_python( data_col{i} ) )   % ← entire record converted here
end
```

`to_python` on a 1-row, 54-column MATLAB table requires ~100–200 Python bridge
crossings (one `py.dict` creation, 27 scalar assignments, 25 cell-column
conversions, 2 struct-to-dict conversions, one `py.pandas.DataFrame` call).

For 7 000 records that is **~1.4 M bridge crossings just to marshal the data**.

Python then re-processes the same 7 000 individual DataFrames row-by-row:
- 7 000 × `canonical_hash(1-row-DataFrame)`
- 7 000 × `_dataframe_to_storage_rows(1-row-DataFrame, …)` — 54 `iloc[i]` calls each

Every stage of the pipeline — MATLAB table, DuckDB table, pandas DataFrame —
is a **column store**. The row-by-row loop is the mismatch.

---

## 3. Proposed Architecture (Conceptual)

Replace the per-record loop with a single **bulk conversion**:

```
concat_data = vertcat( data_col{:} )      % MATLAB-only, zero Python crossings
py_data     = to_python( concat_data )    % one conversion of the whole batch
```

`to_python` on a 7 000-row table converts **column by column**:
- 27 scalar double columns → 27 `py.numpy.array` calls (one per column)
- 25 cell-of-vector columns → 25 fast-path `split_flat_to_lists` calls
- 2 struct-cell columns → 7 000 struct-to-dict conversions (same as before, but
  the per-record table overhead is gone)

Total bridge crossings for the test case:
| Phase        | Before         | After          |
|---|---|---|
| MATLAB→Python | ~1 400 000    | ~14 102        |
| Python storage | 378 000 iloc | vectorized     |

Python receives one `N-row` pandas DataFrame and splits it into `N` 1-row slices
(pure Python, zero MATLAB crossings). Everything downstream in `save_batch` is
unchanged for Phase 1.

---

## 4. Do We Need Per-Type `try_xxx` Functions?

**No.** That is precisely the pattern to avoid.

The current concern with the existing `try_concat_homogeneous_tables` is that it
is applied conditionally — only when the caller has already determined the data
is a cell of tables. This is a form of type dispatch buried in the call site.

The proposed approach uses a single, type-agnostic primitive:

```matlab
try
    concat_data = vertcat( data_col{:} );
    py_data     = scidb.internal.to_python( concat_data );
    bulk_ok     = true;
catch
    bulk_ok     = false;
end
```

`vertcat` is MATLAB's built-in concatenation for **every** column-concatenable
type: numeric arrays, MATLAB tables, struct arrays, string arrays, logical arrays,
categorical arrays. It either succeeds (homogeneous data) or fails (heterogeneous).
`to_python` on the concatenated result uses all its existing fast paths.

The caller (`save_from_table`) does **not** need to know the type. It simply
attempts the bulk operation and falls back on failure. The existing per-type
knowledge lives entirely inside `to_python`, where it already belongs.

### Why `try_concat_homogeneous_tables` is still useful

`try_concat_homogeneous_tables` exists inside `to_python` for a different
purpose: column-level fast paths *within* a single table conversion. That is
correct and should remain. The architectural change here is about where the
*outer* loop is: currently at record granularity (the slow loop in
`save_from_table`), proposed at column granularity (one `vertcat` + one
`to_python`).

### What happens for each real data type

| Data type in `data_col` | Strategy | Result |
|---|---|---|
| cell of 1-row tables (test case) | A: vertcat | N-row DataFrame; Python slices one row per record |
| cell of equal-length numeric arrays | A: vertcat | N×M numpy array; Python slices one row per record |
| cell of scalar doubles | A: vertcat | N×1 numpy array; Python slices one element per record |
| cell of structs (same fields) | A: vertcat | N-element struct array; Python iterates one per record |
| cell of variable-length numeric/logical arrays | B: flatten | flat array + lengths passed as two numpy arrays; `split_flat_to_lists` reconstructs N lists |
| heterogeneous cell / incompatible types | C: per-row fallback | N individual `to_python` calls (unchanged from today) |

The fallback is the existing per-row loop, unchanged. The fast path is a strict
improvement with no new type-specific code.

---

## 5. Implementation

### 5.1 — Guard: make `try_concat_homogeneous_tables` safe for non-cell input

Currently `try_concat_homogeneous_tables` indexes into `col{1}` without checking
that `col` is a cell array first; calling it on a numeric vector would error.

**Change** (`to_python.m`): add a one-line guard at the top of
`try_concat_homogeneous_tables`:

```matlab
function [can_concat, concat_table] = try_concat_homogeneous_tables(col)
    can_concat = false;
    concat_table = [];
    if ~iscell(col)           % ← NEW guard
        return;
    end
    n = numel(col);
    ...
```

This makes the function safe to call on any input and allows the outer
`save_from_table` to call it unconditionally.

---

### 5.2 — MATLAB: replace the `else` loop in `save_from_table`

**File**: `sci-matlab/src/sci_matlab/matlab/+scidb/BaseVariable.m`
**Location**: the `else` branch of the data-column detection block, around line 215.

The `else` branch handles data that is not plain numeric/string/cellstr. It is
replaced with three strategies tried in order:

- **Strategy A (vertcat)**: `vertcat(data_col{:})` produces a bulk MATLAB
  structure (table, numeric array, or struct array) which `to_python` converts in
  one call. Python receives one big DataFrame/array + a `row_heights` vector it
  uses to slice back to N individual records. Covers: cell of tables (the common
  test case), cell of equal-length numeric arrays, cell of structs.

- **Strategy B (flatten)**: `try_flatten_cell_column` already exists in
  `to_python.m` and handles variable-length numeric/logical vector cells. It
  produces a flat array + lengths vector that cross in two numpy bridge calls.
  Python reconstructs N lists via the existing `split_flat_to_lists` helper.

- **Strategy C (per-row fallback)**: Only reached for genuinely heterogeneous data
  (mixed types, non-concatenable shapes). Identical to the old slow path.

Timing is logged after the data-conversion phase with `[timing]` prefix so it
appears in the same log grep as load-path timing.

**Proposed replacement**:
```matlab
else
    t_conv = tic;
    bulk_mode = 'per_row';
    py_heights = py.None;

    % Strategy A: vertcat — bulk-concatenate all records into one MATLAB
    % structure, then convert to Python in one call.
    if iscell(data_col)
        try
            concat_data = vertcat(data_col{:});
            heights_vec = int64(cellfun(@(x) size(x, 1), data_col(:)', 'UniformOutput', true));
            py_data = scidb.internal.to_python(concat_data);
            py_heights = py.numpy.array(heights_vec, pyargs('dtype', 'int64'));
            bulk_mode = 'vertcat';
        catch
        end
    end

    % Strategy B: flatten — cell of variable-length numeric/logical vectors.
    if strcmp(bulk_mode, 'per_row') && iscell(data_col)
        [can_flat, flat, lengths, flat_dtype] = scidb.internal.to_python_flatten(data_col);
        if can_flat
            py_flat    = py.numpy.array(flat,    pyargs('dtype', flat_dtype));
            py_lengths = py.numpy.array(lengths, pyargs('dtype', 'int64'));
            py_data    = py.sci_matlab.bridge.split_flat_to_lists(py_flat, py_lengths);
            bulk_mode  = 'flatten';
        end
    end

    % Strategy C: per-row fallback (heterogeneous / non-concatenable data).
    if strcmp(bulk_mode, 'per_row')
        py_data = py.list();
        for i = 1:height(tbl)
            if iscell(data_col)
                py_data.append(scidb.internal.to_python(data_col{i}));
            else
                py_data.append(scidb.internal.to_python(data_col(i)));
            end
        end
    end

    scidb.Log.info('[timing] save_from_table(%s): data_convert=%.3fs, mode=%s, n=%d', ...
        type_name, toc(t_conv), bulk_mode, height(tbl));
end
```

The bridge call always passes `py_heights`; Python ignores it when it is `py.None`:

```matlab
% --- Call Python bridge ---
py_result = py.sci_matlab.bridge.save_batch_bridge( ...
    type_name, py_data, py_meta_keys, py_meta_cols, ...
    py_common, py_db, py_heights);
```

`py_heights` is `py.None` for Strategy B and C so the existing behaviour of
`save_batch_bridge` is unchanged for those modes.

---

### 5.3 — Python: extend `save_batch_bridge` to handle bulk input

**File**: `sci-matlab/src/sci_matlab/bridge.py`
**Function**: `save_batch_bridge`

Add a `row_heights` parameter (defaults to `None` for backward compatibility)
and a bulk-split path before the existing list construction:

```python
def save_batch_bridge(type_name, data_values, metadata_keys,
                      metadata_columns, common_metadata=None, db=None,
                      row_heights=None):
    ...
    # Bulk DataFrame path: MATLAB sent one big DataFrame/array instead of
    # a list of N individual objects.  Split it back into per-record items.
    import pandas as pd
    import numpy as np

    if isinstance(data_values, pd.DataFrame) and row_heights is not None:
        heights = np.asarray(row_heights, dtype=int)
        offsets = np.concatenate([[0], np.cumsum(heights)])
        data_list = [
            data_values.iloc[offsets[i]:offsets[i+1]]  # view, zero-copy
            for i in range(len(heights))
        ]
    elif isinstance(data_values, np.ndarray) and row_heights is not None:
        heights = np.asarray(row_heights, dtype=int)
        offsets = np.concatenate([[0], np.cumsum(heights)])
        data_list = [
            data_values[offsets[i]:offsets[i+1]]
            for i in range(len(heights))
        ]
    elif hasattr(data_values, 'tolist'):
        # existing numpy path (numeric data from isnumeric branch)
        data_list = data_values.tolist()
    else:
        # existing list path (per-row objects or fallback)
        data_list = [v.item() if hasattr(v, 'item') else v for v in data_values]
    ...
```

`save_batch(cls, data_items)` is called with the same interface as before.
No changes to `save_batch` are required for Phase 1.

**Why `row_heights` instead of always-1**: a future record could store a
multi-row table per record (e.g., a time-series). Passing heights now means the
same code handles that case without change.

---

### 5.4 — Phase 2: Python vectorization in `save_batch` (separate PR)

Once Phase 1 is in and the timing improvement is measured, a second pass can
eliminate the remaining per-row Python overhead.

**Observation**: after Phase 1, `data_list` for DataFrame-mode data contains N
1-row DataFrames that are all views into one big DataFrame. `save_batch` still
calls `_dataframe_to_storage_rows(1-row-df)` N times (378 000 `iloc[0]` calls).

**Proposed change** to `save_batch` (`database.py`): detect the all-same-schema
DataFrame case, concat, and call `_dataframe_to_storage_rows` once:

```python
# Inside save_batch, after dtype_meta is inferred and is_dataframe is True:
if is_dataframe and len(data_items) > 1:
    first_cols = list(dtype_meta.get("columns", {}).keys())
    if all(
        isinstance(d, pd.DataFrame) and list(d.columns) == first_cols
        for d, _ in data_items
    ):
        # All same-schema 1-row DataFrames: concat and process once.
        big_df = pd.concat([d for d, _ in data_items], ignore_index=True)
        all_storage_rows = _dataframe_to_storage_rows(big_df, dtype_meta)
        # ... zip all_storage_rows with record_ids
        bulk_df_mode = True
```

`_storage_to_python_column` (already exists in `sciduckdb.py`) was written for
exactly this pattern on the load side. The save side should mirror it.

---

## 6. What Must Not Break

| Concern | Mitigation |
|---|---|
| Existing `save_from_table` callers using numeric/string data | Those branches (`isnumeric`, `isstring`, `iscellstr`) are above the `else` block and are untouched. |
| Existing `save_from_table` callers with heterogeneous cell data | `vertcat` fails → caught → original per-row loop runs unchanged. |
| `save_batch_bridge` called without `row_heights` (Python direct callers) | Parameter defaults to `None`; the `isinstance(…, pd.DataFrame)` branch is only entered when `row_heights` is not `None`. All existing call sites unaffected. |
| `save_batch` contract | Phase 1 produces identical `data_items` list → identical record_ids, hashes, and metadata. `save_batch` is unchanged. |
| Round-trip correctness | The 1-row DataFrame slices from `data_values.iloc[a:b]` contain exactly the same data as the original per-row converted DataFrames. `canonical_hash` and `_dataframe_to_storage_rows` see the same values. |
| `try_concat_homogeneous_tables` safety change | Adding the early `~iscell` return cannot change behaviour for any existing callers (they always pass cell arrays). |

---

## 7. Test Plan

1. **Existing tests must continue to pass** — `TestSaveLoad`, `TestDataRoundTrip`,
   `TestTableRoundTrip`, `TestSaveFromTable` in `sci-matlab/tests/matlab/scidb/`.
   Run these before and after the change.

2. **Timing regression**: Run `TestForEachTimingInstrumentation` before and after.
   Expected: save phase drops from ~40 s to under 5 s.

3. **Round-trip correctness**: Add a test that saves via `save_from_table` with
   table-type data, reloads, and asserts data equality column-by-column.

4. **Fallback path**: Add a test that saves heterogeneous cell data (e.g., a mix
   of numeric and string cells in `data_col`) to confirm the per-row fallback still
   works.

5. **Multi-row table records**: Add a test where each record is a 3-row table
   (non-uniform `heights`) to verify the `row_heights`-based split is correct.

---

## 8. Rollout Order

1. Merge the `try_concat_homogeneous_tables` guard fix (tiny, safe).
2. Implement and test Phase 1 (MATLAB + bridge).
3. Measure timing; confirm expected savings.
4. Implement Phase 2 (Python vectorization) in a follow-up.
