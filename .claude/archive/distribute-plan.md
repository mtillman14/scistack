# Plan: `distribute` kwarg for `for_each`

## Context

When a function runs at e.g. the trial level in a `[subject, trial, cycle]` schema, it may return a vector (or table) where each element/row corresponds to a cycle. Currently, the user must manually loop to save each element at the cycle level. The `distribute` kwarg automates this: it splits the output and saves each piece with the appropriate lower-level schema key.

## Changes

### File: `/workspace/scirun-lib/src/scirun/foreach.py`

#### 1. Add `distribute` parameter

```python
def for_each(
    fn, inputs, outputs,
    dry_run=False, save=True, pass_metadata=None,
    as_table=None, db=None,
    distribute: str | None = None,       # NEW
    **metadata_iterables,
):
```

#### 2. Add validation block (after empty-list resolution, ~line 98)

Obtain `dataset_schema_keys` from `db` param or global database. Check:
- `distribute` is a valid schema key
- `distribute` is not in `metadata_iterables`
- `distribute` is deeper (later index) in schema hierarchy than the deepest iterated schema key
- After constant/loadable separation: `distribute` is not also a constant input name

#### 3. Add `_split_for_distribute(data)` helper

Returns a list of pieces. Supports:
- **numpy 1D**: each element (returns Python scalars/items)
- **numpy 2D**: each row (returns 1D arrays)
- **list**: each element
- **pandas DataFrame**: each row as a single-row DataFrame (`iloc[[i]]`)
- **3D+ numpy / other types**: raises `TypeError`

#### 4. Modify save section (lines 208-217)

When `distribute` is set and `save` is True:
1. `_unwrap(output_value)` to get raw data (strips ThunkOutput — no lineage for distributed saves)
2. `_split_for_distribute(raw_value)` to get pieces
3. For each piece at index `i`: save with `{**save_metadata, distribute: i + 1}` (1-based)
4. Print `[save]` per piece with full metadata including distribute key

When `distribute` is None: existing behavior unchanged.

#### 5. Update dry_run output

- Header: print `[dry-run] distribute: 'cycle' (split outputs by element/row, 1-based)`
- Per-iteration: show `distribute <VarType> by 'cycle'` instead of normal save line
- Pass `distribute` to `_print_dry_run_iteration`

### File: `/workspace/scirun-lib/tests/test_foreach.py`

New `TestForEachDistribute` class with tests:

| Test | What it verifies |
|------|-----------------|
| `test_distribute_numpy_1d` | 1D array → N saves with correct 1-based indices |
| `test_distribute_numpy_2d` | 2D array → N row saves |
| `test_distribute_list` | List → N element saves |
| `test_distribute_dataframe` | DataFrame → N single-row DataFrame saves |
| `test_distribute_multiple_iterations` | Cartesian product * distributed elements |
| `test_distribute_multiple_outputs` | Both outputs distributed independently |
| `test_distribute_with_constants` | Constants appear in save metadata alongside distribute key |
| `test_distribute_1_based_indexing` | Index values start at 1, not 0 |
| `test_distribute_validation_not_schema_key` | `ValueError` for invalid key |
| `test_distribute_validation_in_iterables` | `ValueError` when distribute key is also iterated |
| `test_distribute_validation_not_deeper` | `ValueError` when distribute level is shallower than iteration |
| `test_distribute_validation_no_db` | `ValueError` with helpful message |
| `test_distribute_dry_run` | Shows distribute info, no saves |
| `test_distribute_save_false` | Function runs, no saves |
| `test_distribute_unsupported_type` | Scalar result → `[error]` message |

### Files NOT modified
- `database.py`, `variable.py`, `sciduck.py` — no changes needed, existing interfaces suffice

## Verification

1. Run existing tests: `pytest /workspace/scirun-lib/tests/test_foreach.py` — all pass (no regressions)
2. Run new distribute tests: same file, new test class
3. Optionally run integration tests: `pytest /workspace/tests/`
