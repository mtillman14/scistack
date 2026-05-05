# Function Hash Storage: Single vs Dual Analysis

## Current Storage Locations

### Location 1: `_record_metadata.version_keys.__fn_hash`
- **Type:** JSON field within version_keys dict
- **Scope:** ALL outputs (both scidb.for_each and scihist.for_each)
- **Written by:** `ForEachConfig.to_version_keys()` → saved by both scidb and scihist
- **Length:** 16 hex chars (truncated)

### Location 2: `_lineage.function_hash`
- **Type:** Dedicated VARCHAR column
- **Scope:** ONLY scihist.for_each outputs (lineage-tracked)
- **Written by:** `save_lineage_result()` / `_save_with_lineage()`
- **Length:** 64 hex chars (full hash) currently, but could be 16

## Access Pattern Analysis

### Reads

| Code Location | Storage Used | Query Type | Frequency |
|---------------|-------------|------------|-----------|
| `scihist.state.check_combo_state()` line 99 | `_lineage.function_hash` | Direct column SELECT | Per-combo check (high) |
| `scihist.state._check_via_fn_hash()` line 254 | `version_keys.__fn_hash` | JSON extract | Per-combo check (fallback) |
| `scihist.foreach` skip_computed line 219 | `_lineage.function_hash` | Direct column SELECT | Per-combo (high) |
| GUI graph builder (possible) | Both (via state module) | Via helper functions | Per-refresh |

### Writes

| Code Location | Storage Written | When |
|---------------|----------------|------|
| `scidb.foreach._save_results()` | `version_keys.__fn_hash` | Every scidb.for_each output |
| `scihist.foreach.save_lineage_result()` | BOTH (inherits version_keys + writes _lineage) | Every scihist.for_each output |

## Pros/Cons Analysis

### Option A: Keep Dual Storage (Status Quo)

**Pros:**
- ✅ Fast queries: `_lineage.function_hash` is indexed, direct column access
- ✅ No null handling: lineage outputs have it in dedicated column
- ✅ Separation of concerns: lineage data stays in lineage table
- ✅ Already works: no migration needed

**Cons:**
- ❌ Redundant storage: scihist outputs have hash in TWO places
- ❌ Inconsistent lengths: 16 chars in version_keys, 64 in _lineage (currently)
- ❌ Maintenance burden: must keep both in sync
- ❌ Confusion: which one is "truth"?

### Option B: Single Storage in `version_keys.__fn_hash` Only

**Pros:**
- ✅ Single source of truth
- ✅ Consistent: all outputs have hash in same location
- ✅ Simpler: one write path, one read path
- ✅ Less storage: eliminates redundancy

**Cons:**
- ❌ Slower queries: must parse JSON to extract `__fn_hash`
  ```sql
  -- Before (fast)
  SELECT function_hash FROM _lineage WHERE output_record_id = ?

  -- After (slower)
  SELECT json_extract(version_keys, '$.__fn_hash')
  FROM _record_metadata WHERE record_id = ?
  ```
- ❌ Works for ALL outputs, but lineage outputs also have lineage table
- ❌ Breaks separation: function metadata in record table instead of lineage table

### Option C: Single Storage in `_lineage.function_hash` Only

**Pros:**
- ✅ Fast queries: dedicated column, indexed
- ✅ Logical location: function hash belongs with lineage data
- ✅ Clean separation: lineage info in lineage table

**Cons:**
- ❌ Only works for lineage outputs (scihist.for_each)
- ❌ Breaks compatibility: scidb.for_each outputs have no lineage table entry
- ❌ Requires version_keys fallback anyway for non-lineage outputs
- ❌ **DEALBREAKER:** Can't unify because scidb outputs don't have lineage

### Option D: Hybrid - Unified Computation, Dual Storage by Output Type

**Pros:**
- ✅ Same hash computation everywhere (bytecode-based)
- ✅ Each storage location optimized for its use case
- ✅ Fast queries for lineage outputs (direct column)
- ✅ Works for all outputs (version_keys fallback)
- ✅ Minimal code change: just unify `_compute_fn_hash()`

**Cons:**
- ⚠️ Still have redundancy for scihist outputs (both places)
- ⚠️ Still need maintenance to keep in sync

## Performance Impact Estimate

### JSON Extract Performance
```sql
-- Test on 10k records
SELECT json_extract(version_keys, '$.__fn_hash') FROM _record_metadata;
-- vs
SELECT function_hash FROM _lineage;
```

**Expected:** JSON extract is 2-5x slower than direct column access, but:
- Absolute time: ~0.1ms vs ~0.02ms (negligible for single queries)
- Bulk queries: could matter for 1000+ combos

### Storage Impact
- Hash is 16-64 bytes
- 10,000 lineage outputs × 16 bytes = 160 KB redundant storage
- **Verdict:** Storage cost is negligible

## Query Pattern Reality Check

Let me check actual usage in staleness checks:

```python
# scihist.state.check_combo_state() line 99
stored_lineage_hash = db.get_function_hash_for_record(output_record_id)
if stored_lineage_hash is not None:
    return _check_via_lineage(...)  # Uses lineage hash
else:
    return _check_via_fn_hash(...)  # Reads version_keys
```

**Pattern:**
1. Try `_lineage.function_hash` first (one query)
2. If None (non-lineage output), fall back to `version_keys.__fn_hash` (second query)

**With Single Storage in version_keys:**
1. Always read `version_keys.__fn_hash` (one query)
2. No fallback needed

**Net change:** Same number of queries, but first query is slightly slower (JSON extract vs direct column)

## Use Case: GUI Staleness Checks

**Code:** `scihist.state.check_node_state()` calls `check_combo_state()` in a loop (line 413)

**Pattern:**
```python
for combo_info in output_combos:  # Could be 10-1000 combos
    state = check_combo_state(fn, outputs, schema_combo, branch_params=bp)
    # ^ Makes 1 query for function_hash per combo
```

**With Dual Storage (current):**
- Lineage outputs: 1 direct column query per combo → fast
- Non-lineage outputs: 1 JSON extract query per combo → slightly slower

**With Single Storage in version_keys:**
- All outputs: 1 JSON extract query per combo → uniform, slightly slower

**Performance impact:**
- For 100 combos: ~100 queries either way
- JSON extract adds ~0.05ms per query (negligible)
- Total overhead: ~5ms for 100 combos (acceptable)

## Recommendation: **Option D - Unified Computation, Keep Dual Storage**

### Decision

1. **Unify the hashing method** → Use bytecode-based everywhere
2. **Keep dual storage** → Don't change storage locations

### Rationale

**Why unify computation:**
- ✅ Critical: Prevents divergence between scidb and scihist hashes
- ✅ Major benefit: Bytecode-based is better for the use case
- ✅ Low risk: Just change the hash function implementation

**Why keep dual storage:**
- ✅ Performance: Dedicated `_lineage.function_hash` column is faster for lineage queries
- ✅ Separation of concerns: Lineage data belongs in lineage table
- ✅ Low cost: Storage overhead is negligible (16 bytes × num lineage outputs)
- ✅ Already works: No migration, no breaking changes
- ⚠️ The redundancy only affects lineage outputs, which are a subset of all outputs

**Why NOT move to single storage:**
- ❌ Small performance cost for high-frequency operations (staleness checks)
- ❌ Would require migration of existing code paths
- ❌ Doesn't provide significant benefits (storage savings are tiny)
- ❌ Loses optimization: direct column vs JSON extract

## Implementation Plan

### Phase 1: Unify Hash Computation (MUST DO)

1. Update `scidb/src/scidb/foreach_config.py` `_compute_fn_hash()`:
   ```python
   def _compute_fn_hash(fn: Callable) -> str:
       """SHA-256 of function bytecode + constants, truncated to 16 hex chars."""
       try:
           # Extract wrapped function if LineageFcn
           actual_fn = fn.fcn if hasattr(fn, 'fcn') else fn

           # Hash bytecode + constants
           fcn_code = actual_fn.__code__.co_code
           fcn_consts = str(actual_fn.__code__.co_consts).encode()
           combined_code = fcn_code + fcn_consts
           return hashlib.sha256(combined_code).hexdigest()[:16]
       except AttributeError:
           # Fallback for built-ins, C extensions
           name = getattr(fn, "__name__", repr(fn))
           return hashlib.sha256(name.encode()).hexdigest()[:16]
   ```

2. Update `scilineage/src/scilineage/core.py` LineageFcn.hash to use 16 chars:
   ```python
   # Change line 86 from:
   self.hash = sha256(string_repr.encode()).hexdigest()
   # To:
   self.hash = sha256(string_repr.encode()).hexdigest()[:16]
   ```

3. Update tests to expect 16-char hashes

4. Document that code reformatting won't invalidate cache

### Phase 2: Storage Cleanup (OPTIONAL - Low Priority)

**Current state after Phase 1:**
- Both locations store the same 16-char hash ✅
- No divergence possible ✅
- Slight redundancy for lineage outputs (acceptable)

**Possible future optimization:**
- Could add batch query methods for GUI performance
- Could consolidate if _lineage table proves unnecessary for other reasons
- Not urgent - current approach works well

## Testing Strategy

1. **Unit tests:** Verify bytecode hash is stable across reformatting
2. **Integration tests:** Verify staleness detection still works
3. **Regression tests:** Verify existing saved hashes don't break (add compat check)
4. **Performance tests:** Benchmark staleness check with 100+ combos

## Migration Considerations

**Breaking change?** No
- New hashes will differ from old hashes
- But staleness check compares stored vs current, both computed with new method
- Old records will appear "stale" once (expected), then re-run creates new hash

**Rollout:**
- Deploy to test project first
- Verify staleness detection works
- Document that first run post-upgrade may recompute some outputs (expected)

## Summary

| Aspect | Decision | Why |
|--------|----------|-----|
| **Hash method** | Bytecode-based | Better for the use case, already used by scilineage |
| **Hash length** | 16 chars (truncated) | Sufficient entropy, saves space, matches scidb |
| **Storage location** | Keep both (dual) | Performance > minimal storage savings |
| **Priority** | Phase 1 = HIGH, Phase 2 = LOW | Unifying computation is critical, storage optimization is not |
