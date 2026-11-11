# Plan: Propagate VariableFilter to Merge Constituents

## Problem

`where=SomeVar() == value` (VariableFilter/ColumnFilter/InFilter) is silently
dropped when the input is a `Merge`. Coverage validation inside `filter.resolve()`
fires per-constituent, but a constituent can have "extra" rows the Merge inner
join would eliminate anyway, causing false-positive coverage failures.

## Approach

Two-part fix, both changes confined to `foreach.py` and `filters.py`:

**Part 1 — avoid false-positive coverage errors during loading.**
Add `validate_coverage: bool = True` to `Filter.resolve()`. When loading Merge
constituents, call `resolve(validate_coverage=False)` to skip the per-constituent
coverage check. This avoids false positives without silently hiding real data gaps.

**Part 2 — still raise on genuine data gaps.**
Before loading, compute the actual Merge result schema_ids (`merge_effective_ids`)
and run one explicit coverage check against them via
`_check_merge_filter_coverage`. A `SomeVar` record missing for a schema_id that
genuinely survives the Merge inner join will still raise an error — it just
happens once, against the right target, instead of per-constituent against the
wrong target.

**Key properties of this approach:**
- `effective_schema_ids` is NOT threaded through `Filter.resolve()` — the Filter
  API change is minimal (one bool, backward-compatible).
- Coverage validation semantics are preserved: missing filter data for a
  Merge-result row is still an error, not silent exclusion.
- All filter types (VariableFilter, ColumnFilter, InFilter, Compound, Not,
  SchemaKey, Raw) work for Merge via the same path. `_is_schema_key_only_filter`
  is removed.

---

## Python Changes

### 1. `scidb/src/scidb/filters.py`

**`_validate_filter_coverage`** — add `target_schema_ids_override: set[int] | None = None`:
- When provided, use it as the coverage target instead of calling
  `_get_all_schema_ids_for_variable(db, target_table_name)`.
- All other logic (coarse-level expansion check, error message) unchanged.
- Called from `foreach.py`'s `_check_merge_filter_coverage` with the
  Merge-result schema_ids as the override.

**`Filter.resolve()` abstract method** — add `validate_coverage: bool = True`:
- All concrete subclasses gain this parameter (backward-compatible default).
- No other signature changes; `effective_schema_ids` is NOT added here.

**Per-subclass `resolve()` updates**:

| Class | Change |
|---|---|
| `VariableFilter` | Gate `_validate_filter_coverage` call on `validate_coverage` |
| `ColumnFilter` | Same |
| `InFilter` | Same |
| `CompoundFilter` | Forward `validate_coverage` to both `left.resolve()` and `right.resolve()` |
| `NotFilter` | Forward `validate_coverage` to `inner.resolve()` |
| `RawFilter` | Add param, ignore it (no coverage check exists) |
| `SchemaKeyInFilter` | Add param, ignore it (no coverage check exists) |
| `SchemaKeyCompareFilter` | Add param, ignore it (no coverage check exists) |

### 2. `scidb/src/scidb/foreach.py`

**New helper: `_compute_merge_effective_ids(db, merge_spec) -> set[int]`**:
- For each constituent, get its schema_ids via `_get_all_schema_ids_for_variable`.
- Determine the finest schema level across all constituents.
- For coarser constituents, expand their schema_ids to the finest level using
  the existing `_expand_coarse_to_fine_schema_ids` from `filters.py`.
- Return the intersection of all fine-level schema_id sets.
- This is the set of schema_ids that the Merge inner join will produce.

**New helper: `_check_merge_filter_coverage(db, where, merge_effective_ids)`**:
- Recurses through the filter tree:
  - `VariableFilter`, `ColumnFilter`, `InFilter`: call `_validate_filter_coverage`
    with `target_schema_ids_override=merge_effective_ids`. Raises if the filter
    variable doesn't cover all Merge-result schema_ids.
  - `CompoundFilter`: recurse into both children.
  - `NotFilter`: recurse into inner.
  - `SchemaKeyInFilter`, `SchemaKeyCompareFilter`, `RawFilter`: no coverage
    concept; no-op.
- Called once before the constituent loading loop.

**New internal class: `_PreresolvedFilter(Filter)`**:
- Wraps a pre-computed `set[int]` of schema_ids.
- `resolve()` returns those ids directly — no DB query, no coverage check.
- `to_key()` returns `""` — never stored; the outer `__where` version key
  covers the whole Merge call.
- Used to load each constituent with the already-resolved, already-validated
  schema_id set.

**`_load_input` Merge branch — replace `_is_schema_key_only_filter` gate**:
```python
# All filter types now work for Merge.
# Coverage is validated once against the actual Merge result (not per-constituent).
loaded_tables = []
_SCIDB_META = {"__record_id", "__branch_params", "version"}
_schema_keys = set(getattr(_merge_db, 'dataset_schema_keys', []) or [])

if where is not None:
    merge_effective_ids = _compute_merge_effective_ids(_merge_db, var_spec)
    _check_merge_filter_coverage(_merge_db, where, merge_effective_ids)

for sub_spec in var_spec.var_specs:
    if where is not None:
        cls = _get_loadable_class_from_spec(sub_spec)
        matching_ids = where.resolve(
            _merge_db, cls, cls.table_name(),
            validate_coverage=False,   # per-constituent check skipped; done above
        )
        constituent_where = _PreresolvedFilter(matching_ids)
    else:
        constituent_where = None
    loaded = _load_input(sub_spec, db, where=constituent_where)
    # ... existing strip / drop logic unchanged ...
```

**Remove `_is_schema_key_only_filter`** — no longer needed.

**Update Merge branch comment** to describe the new two-step approach.

### 3. Python Tests

**`scidb/tests/test_schema_key_filter.py`**:
- `TestSchemaKeyFilterWithMerge` tests are unchanged in intent — SchemaKey
  filters still work, now via the general path. Verify they still pass.

**New: `scidb/tests/test_variable_filter_merge.py`**:
- `VariableFilter` (same level as constituents) filters Merge rows correctly.
- `VariableFilter` (coarser than constituents) expands and filters correctly.
- `NotFilter` with Merge gives the correct complement.
- `CompoundFilter` (&) with Merge filters correctly.
- **Coverage error still raised**: filter variable missing for a schema_id that
  survives the Merge inner join → `ValueError`.
- **No false-positive**: filter variable missing for a schema_id that would be
  eliminated by the Merge inner join → no error, no data loss.

**`scidb/tests/test_filters.py`** (or inline in above):
- `_validate_filter_coverage` with `target_schema_ids_override` uses the
  override set, not the target table's schema_ids.

---

## MATLAB Changes

No MATLAB source code changes — the fix is entirely in the Python layer.

### Test updates: `sci-matlab/tests/matlab/scidb/TestForEachWhere.m`

**Three existing tests assert the old bypass behavior and must be updated**:

| Test | Old assertion | New assertion |
|---|---|---|
| `test_where_merge_only_not_filtered` | Iteration runs despite Side=R | Iteration skipped — filter applied |
| `test_where_merge_with_fixed_constituent` | Both sessions run despite Side=R | Sessions where Side=R excluded |
| `test_where_merge_multi_record_join_with_filter` | Iteration runs despite Side=R | Iteration skipped |

Keep Side=R data; flip assertions to verify no output is saved.

**Two new MATLAB SchemaKey+Merge tests added earlier are unchanged**:
- `test_schema_key_isin_filters_merge_as_table` — still correct behavior.
- `test_schema_key_compare_filters_merge_as_table` — still correct behavior.

**New VariableFilter+Merge tests to add**:
- `test_variable_filter_filters_merge_rows`: Side=="L" with Merge correctly
  restricts the table passed to the function; Side=R subjects absent.
- `test_variable_filter_merge_coverage_error`: Side missing for a subject that
  exists in both Merge constituents → `MException` raised.

**Class-level comment**: Update to describe:
- All filter types now applied to Merge.
- Coverage validation runs against Merge result (not per-constituent).
- Missing filter variable data for a Merge-result row still raises an error.

---

## Rollout Risk

Behavior change for callers that passed `where=VariableFilter` with a Merge and
relied on the bypass. They will now see the filter applied. Genuine data gaps
(filter variable missing for a row that survives the Merge join) surface as
errors rather than silent pass-through — which is the correct, consistent
behavior with the non-Merge path.
