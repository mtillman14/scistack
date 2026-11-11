# Unify Variant Tracking Implementation Plan

## Goal
Unify variant tracking so that scihist outputs have the same `version_keys` and `branch_params` structure as scidb outputs, eliminating the dual variant tracking system.

## Approach: Option 2
Make scidb detect and handle `LineageFcnResult` saves, so that scihist outputs receive the same complete metadata that scidb generates for regular outputs.

## Architecture Changes

### Single Source of Truth
- **scidb** builds all variant tracking metadata (`version_keys`, `branch_params`)
- **scihist** only adds lineage-specific information (`_lineage` table)
- **Result**: All outputs have consistent metadata structure

### Key Design Decisions

1. **Optional Dependency**: scidb conditionally imports `LineageFcnResult` from scilineage
   - If not installed: scidb works normally (graceful degradation)
   - If installed: scidb detects and handles LineageFcnResult specially

2. **Delegation Pattern**: scidb delegates lineage-specific save to scihist
   - scidb builds complete metadata
   - Detects LineageFcnResult via isinstance check
   - Calls `scihist.foreach.save_lineage_result` with pre-built metadata
   - scihist adds lineage info and completes save

3. **Fixed Input Tracking**: Fixed inputs require special handling
   - scihist computes Fixed input record_ids before calling scidb
   - Passes them via `_lineage_fixed_rids` parameter
   - scidb includes them in metadata when calling save_lineage_result
   - save_lineage_result merges them with __upstream for staleness tracking

## Implementation Details

### File 1: scidb/src/scidb/foreach.py

**Changes:**
1. Added conditional import at top of file:
   ```python
   try:
       from scilineage import LineageFcnResult
       HAS_LINEAGE = True
   except ImportError:
       LineageFcnResult = None
       HAS_LINEAGE = False
   ```

2. Added `_lineage_fixed_rids` parameter to `for_each()` signature

3. Modified call to `_save_results()` to pass `lineage_fixed_rids`

4. Updated `_save_results()` signature to accept `lineage_fixed_rids`

5. Added LineageFcnResult detection in _save_results (line ~1191):
   ```python
   if HAS_LINEAGE and isinstance(output_value, LineageFcnResult):
       from scihist.foreach import save_lineage_result
       lineage_metadata = dict(save_metadata)
       if lineage_fixed_rids:
           lineage_metadata["__lineage_fixed_rids"] = lineage_fixed_rids
       rid = save_lineage_result(output_obj, output_value, lineage_metadata, db)
   ```

### File 2: scihist-lib/src/scihist/foreach.py

**Changes:**
1. Added `json` import

2. Created new `save_lineage_result()` function:
   - Receives pre-built metadata from scidb
   - Extracts input_rids from `__upstream`
   - Merges Fixed input rids from `__lineage_fixed_rids`
   - Extracts lineage from LineageFcnResult
   - Adds rid_tracking entries via `_append_rid_tracking()`
   - Calls database save with complete metadata + lineage

3. Modified `for_each()`:
   - Computes `fixed_rids` before calling scidb
   - Changed `save=False` to `save=True`
   - Passes `_lineage_fixed_rids=fixed_rids` to scidb.for_each
   - Removed the `_save_with_lineage()` call (no longer needed)
   - Simplified the function by ~60 lines

## Benefits

### Single Source of Truth
- Only scidb generates version_keys and branch_params
- No risk of drift between scidb and scihist metadata
- Changes to metadata structure only need to be made in one place

### Consistent Metadata
- scihist outputs now have:
  - `version_keys.__inputs` ✅ (was missing)
  - `version_keys.__constants` ✅ (was missing)
  - `branch_params` populated ✅ (was empty `{}`)
- All outputs queryable with single pattern

### Graceful Degradation
- scidb works without scilineage installed
- Zero runtime dependency on scihist
- Optional code path for lineage support

### Simpler scihist
- No longer needs to understand ForEachConfig
- No longer needs to build version_keys or branch_params
- Just receives pre-built metadata and adds lineage

## Testing Required

### Test 1: scidb works without scilineage
Verify that scidb.for_each works normally when scilineage is not installed:
- Regular (non-lineage) functions save correctly
- No import errors or runtime failures

### Test 2: scihist outputs have complete metadata
Verify that scihist.for_each outputs now have:
1. `version_keys.__inputs` (JSON string of input specs)
2. `version_keys.__constants` (JSON string of constants)
3. `branch_params` (accumulated upstream + namespaced constants)
4. All metadata matches scidb's structure

Compare a scihist output to a scidb output in the database.

### Test 3: Fixed inputs tracked correctly
Verify that Fixed inputs' record_ids are tracked in lineage:
- Check that `_lineage.inputs` contains rid_tracking entries for Fixed inputs
- Verify skip_computed works with Fixed inputs

### Test 4: Integration test
Run a full pipeline with both scidb and scihist outputs:
- Verify all outputs saved correctly
- Verify staleness checking works
- Verify variant discovery finds all outputs

## Migration Impact

### Breaking Changes
None - this is purely internal refactoring.

### API Changes
- scidb.for_each accepts new `_lineage_fixed_rids` parameter (internal, underscore-prefixed)
- New public function: `scihist.foreach.save_lineage_result()` (called by scidb)

### User-Facing Changes
None - users should see no difference in behavior, only:
- More consistent metadata in database
- Potentially faster variant discovery queries

## Success Criteria

1. ✅ scidb works without scilineage installed
2. ✅ scihist outputs have complete version_keys and branch_params
3. ✅ Fixed inputs tracked correctly in lineage
4. ✅ All existing tests pass
5. ✅ No performance regression
6. ✅ Documentation updated (docs/claude/layer-friction-analysis.md)

## Future Work

### Eliminate Other Friction Points
From layer-friction-analysis.md:
- **Priority 2**: Fix input classification quirk (HIGH)
- **Priority 3**: Eliminate triple storage of constants (HIGH)
- **Priority 4**: Schema as parameter instead of global (MEDIUM)

These can be addressed in future iterations now that the critical variant tracking unification is complete.
