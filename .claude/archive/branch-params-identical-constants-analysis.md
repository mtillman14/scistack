# Analysis: branch_params with Identical Constants from Different Call Sites

## Scenario

Two `for_each()` call sites use the same function with the same constants but different inputs:

```python
for_each(normalize, inputs={"signal": RawEMG, "method": "zscore"}, outputs=[NormalizedData])
for_each(normalize, inputs={"signal": ForceData, "method": "zscore"}, outputs=[NormalizedData])
```

Both produce `branch_params = {"normalize.method": "zscore"}` (identical).

## Key Question

Is this a real functional gap, or does the system handle it correctly through other mechanisms?

## Findings

### 1. branch_params Construction During Save (`_save_results`, foreach.py:1073-1146)

Three sources contribute to `branch_params`:

1. **Upstream branch_params** (Step 1): Inherited from input records via `rid_to_bp` lookup. Each input record_id maps to its stored branch_params. If `RawEMG` and `ForceData` records both have `branch_params={}`, then upstream contribution is `{}` for both call sites.

2. **Own constants** (Step 2, line 1092-1094): Namespaced as `fn_name.param` from `__constants`. Both call sites have `method="zscore"`, so both produce `{"normalize.method": "zscore"}`.

3. **Dynamic discriminators** (Step 3, lines 1096-1117): Non-schema, non-`__` scalar meta columns. These don't include input-type or where-clause info.

**Result**: Both call sites produce IDENTICAL `branch_params = {"normalize.method": "zscore"}`. There is NO input-type or where-clause info in branch_params.

### 2. However, version_keys ARE Different

The `save_metadata` dict includes `config_keys` (line 1124: `save_metadata.update(config_keys)`), which come from `ForEachConfig.to_version_keys()`:

- Call site 1: `__inputs = {"signal": "RawEMG"}`, `__fn = "normalize"`, `__constants = {"method": "zscore"}`
- Call site 2: `__inputs = {"signal": "ForceData"}`, `__fn = "normalize"`, `__constants = {"method": "zscore"}`

So `version_keys` differ because `__inputs` differs. This means:
- Different `call_id` (hash of `__fn`, `__inputs`, `__constants`, `__where`, `__distribute`, `__as_table`)
- Different `version_keys` stored in `_record_metadata`

### 3. `__upstream` Also Differs

Line 1136-1146: `save_metadata["__upstream"]` stores the exact upstream `record_id` per `__rid_*` column. Different input types produce different upstream record_ids. But `__upstream` is per-record bookkeeping within `version_keys`, not part of `branch_params`.

### 4. `_persist_expected_combos` Always Writes `branch_params = '{}'`

Line 1337: `rows_to_insert.append((fn_name, call_id, schema_id, "{}"))` -- always `"{}"` regardless of actual constants. But it's scoped by `(function_name, call_id)`, so the two call sites have separate expected combo sets. This is correct because `call_id` differs.

### 5. `check_node_state` Matching

When `call_id` is provided (GUI path, line 154 of pipeline.py):
- `_get_output_combos` filters by `call_id` (state.py:511-515) via `call_id_from_version_keys(vk)`. Since version_keys differ, records from the two call sites are correctly separated.
- `_get_expected_combos` filters `scidb_variants` by `call_id` (state.py:563). Since `list_pipeline_variants` computes `call_id` from version_keys, variants are correctly scoped.

When `call_id` is NOT provided (e.g., programmatic usage):
- `_get_output_combos` returns ALL records matching `fn_name` across ALL call sites. Both call sites' records appear.
- The matching uses `(schema_id, branch_params_json)` as the key (line 401). Since both call sites produce identical `branch_params`, their records at the same schema_id would COLLAPSE into one entry in the `actual_combo_keys` set.
- `_get_expected_combos` would also union across both call sites' variants, but the expected set uses `(schema_id, expected_bp_json)` which would also collapse.

### 6. The `find_record_id` Path in `check_combo_state`

`check_combo_state` calls `db.find_record_id(OutputCls, schema_combo, branch_params_filter=bp)` (state.py:84). With identical `branch_params`, this would find records from EITHER call site. However, the `_find_record` uses `version_id="latest"` with `PARTITION BY rm.variable_name, rm.schema_id, rm.version_keys`. Since the two call sites have different `version_keys`, they're in different partitions, so both records survive. Then `branch_params_filter` would match BOTH. The function returns just the first one (`rows.iloc[0]["record_id"]`).

This means `check_combo_state` without `call_id` might check staleness against the wrong call site's record. But it would still return a valid answer (either up_to_date or stale) because both records were produced by the same function.

### 7. Downstream Propagation Problem

A third function: `for_each(analyze, inputs={"data": NormalizedData}, outputs=[Result])`

When `_load_var_type_all` loads `NormalizedData`, it calls `load_all(version_id="latest")`. The SQL partitions by `(variable_name, schema_id, version_keys)`. Since the two NormalizedData records have different `version_keys` (different `__inputs`), BOTH records are returned as separate "latest" versions.

Each record gets its own `__record_id` and `__branch_params`. Even though `branch_params` is identical, the `__rid_data` column would have different `record_id` values. The combo expansion (line 447-452) produces one combo per unique `__rid_data` value. So the downstream function correctly iterates over BOTH NormalizedData records.

The downstream function's `__upstream` metadata would correctly store which specific upstream record_id was used, enabling proper staleness tracking via `get_latest_record_id_for_variant`.

## Conclusion

**This is NOT a real functional gap. The system handles this correctly through multiple complementary mechanisms:**

1. **`version_keys` differ** because `__inputs` differs. This is the primary discriminator in the DB, not `branch_params`.
2. **`call_id` differs** (derived from version_keys including `__inputs`). The GUI always passes `call_id`, so node states are correctly separated per call site.
3. **`__upstream` record_ids differ**, enabling correct staleness tracking even without call_id.
4. **`load_all(version_id="latest")` returns both records** because they have different version_keys, so downstream functions correctly process both.

**The only cosmetic gap**: `branch_params` alone cannot distinguish the two call sites. But `branch_params` is not the sole discriminator -- it's a human-readable summary used primarily for variant selection in `load()`/`find_record_id()` with `branch_params_filter`. If a user explicitly calls `NormalizedData.load(subject=1, method="zscore")`, they would get an `AmbiguousVersionError` or the most recent record, which is the expected behavior when multiple variants exist.

**Minor concern**: When `check_node_state` is called WITHOUT `call_id`, the `(schema_id, branch_params)` set-based matching would collapse the two call sites' combos into one, potentially under-counting expected or actual combos. But in practice, the GUI always provides `call_id`, and programmatic usage without `call_id` still gets a reasonable aggregate answer.
