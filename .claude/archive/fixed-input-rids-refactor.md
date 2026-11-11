# Move Fixed Input Record_ID Computation from scihist to scidb

## Goal
Move the computation of Fixed input record_ids from scihist into scidb, eliminating the need for scihist to pre-compute and pass `_lineage_fixed_rids`.

## Current Flow

1. **scihist/foreach.py lines 102-124**: scihist loops through inputs, detects Fixed wrappers, unwraps them, calls `db.find_record_id()`, stores in `fixed_rids` dict
2. **scihist/foreach.py line 146**: Passes `_lineage_fixed_rids=fixed_rids` to scidb.for_each
3. **scidb/foreach.py line 100**: Accepts `_lineage_fixed_rids` parameter
4. **scidb/foreach.py line 1244-1245**: Adds `__lineage_fixed_rids` to metadata when calling save_lineage_result
5. **scihist/foreach.py (save_lineage_result) lines 596-601**: Extracts and merges fixed_rids from metadata into input_rids

## Problems with Current Approach

1. **Duplication**: scihist duplicates the Fixed input unwrapping logic that scidb already understands
2. **Layer violation**: scihist reaches into the structure of Fixed wrappers (checking `fixed_metadata`, unwrapping `var_type`, handling ColumnSelection)
3. **Tight coupling**: Requires coordination between scihist computing fixed_rids and scidb passing them through

## New Flow

1. **scidb/foreach.py**: Add `_compute_fixed_input_rids(inputs, db)` helper function
2. **scidb/foreach.py**: When detecting LineageFcnResult, compute fixed_rids internally before calling save_lineage_result
3. **scihist/foreach.py**: Remove lines 102-124 (fixed_rids computation)
4. **scihist/foreach.py**: Remove `_lineage_fixed_rids` parameter passing (line 146)
5. **scidb/foreach.py**: Keep `_lineage_fixed_rids` parameter for backward compatibility but make it optional and prefer internal computation

## Implementation Steps

### Step 1: Add helper function to scidb/foreach.py

Add new function after line 840 (near other Fixed input handling):

```python
def _compute_fixed_input_rids(inputs: dict, db) -> dict:
    """Compute record_ids for Fixed inputs for lineage tracking.

    Fixed inputs have __record_id stripped during variant expansion (line 826-829),
    but lineage tracking needs to know which specific record was used for staleness
    checking. This function computes those record_ids.

    Args:
        inputs: The inputs dict passed to for_each (may contain Fixed wrappers)
        db: Database instance

    Returns:
        Dict mapping "__rid_{param_name}" to record_id for each Fixed input
    """
    fixed_rids = {}

    for name, value in inputs.items():
        # Detect Fixed wrapper
        if not hasattr(value, 'fixed_metadata'):
            continue

        # Unwrap to get inner variable type
        inner = value.var_type if hasattr(value, 'var_type') else value

        # Unwrap ColumnSelection if present
        if hasattr(inner, 'var_type'):
            inner = inner.var_type

        # Must be a variable type (class)
        if not isinstance(inner, type):
            continue

        # Look up record_id for this Fixed input
        try:
            rid = db.find_record_id(inner, value.fixed_metadata)
            if rid:
                fixed_rids[f"__rid_{name}"] = rid
        except Exception:
            # If lookup fails, skip this Fixed input
            pass

    return fixed_rids
```

### Step 2: Use helper in scidb save path

Modify scidb/foreach.py around line 1238 (LineageFcnResult detection):

```python
# Detect LineageFcnResult and delegate to scihist if present
if HAS_LINEAGE and isinstance(output_value, LineageFcnResult):
    try:
        from scihist.foreach import save_lineage_result
        save_t0 = time.perf_counter()

        # Compute Fixed input rids internally (unless provided by caller)
        lineage_metadata = dict(save_metadata)
        if lineage_fixed_rids is None:
            # Compute fixed_rids from inputs (new path)
            computed_fixed_rids = _compute_fixed_input_rids(inputs, db)
            if computed_fixed_rids:
                lineage_metadata["__lineage_fixed_rids"] = computed_fixed_rids
        else:
            # Use provided fixed_rids (backward compatibility)
            lineage_metadata["__lineage_fixed_rids"] = lineage_fixed_rids

        rid = save_lineage_result(output_obj, output_value, lineage_metadata, db)
        # ... rest of save logging
```

### Step 3: Simplify scihist/foreach.py

IMPORTANT: The old code had database initialization logic that must be preserved!

Remove lines 102-124 (fixed_rids computation) but KEEP database initialization:

The old code did:
```python
save_db = db
if save_db is None:
    try:
        from scidb.database import get_database
        save_db = get_database()
    except Exception:
        save_db = None
```

This initialization is NECESSARY because tests may not pass `db` explicitly and rely on
the global database instance. We need to keep this logic but consolidate it with the
existing skip_computed database initialization.

Changes to make:

```python
# DELETE lines 102-124:
#   # Compute Fixed input record_ids for lineage tracking before delegating to scidb.
#   # These are needed for staleness checking but scidb doesn't include them in
#   # __upstream (Fixed inputs have __record_id stripped for variant expansion).
#   fixed_rids = {}
#   if save and outputs:
#       for name, value in inputs.items():
#           ... [entire loop]
#       logger.debug("computed %d fixed_rids for lineage tracking", len(fixed_rids))

# MODIFY line 146 (remove _lineage_fixed_rids parameter):
result_tbl = _scidb_for_each(
    fn_plain,
    inputs,
    outputs,
    dry_run=dry_run,
    save=save,
    as_table=as_table,
    db=db,
    distribute=distribute,
    where=where,
    _inject_combo_metadata=_inject_meta,
    _pre_combo_hook=pre_combo_hook,
    _progress_fn=_progress_fn,
    _cancel_check=_cancel_check,
    # _lineage_fixed_rids=fixed_rids if fixed_rids else None,  # REMOVE THIS LINE
    **metadata_iterables,
)
```

## Benefits

1. **Reduced scihist complexity**: ~25 lines removed from scihist
2. **Better encapsulation**: scidb owns all Fixed input handling logic
3. **Consistency**: Fixed input processing happens in one place (scidb)
4. **Availability**: Plain scidb users could eventually use this for their own lineage tracking
5. **Backward compatibility**: Keep `_lineage_fixed_rids` parameter for existing code that passes it

## Testing Requirements

### Test 1: Existing tests still pass
Run scihist integration tests:
```bash
cd scihist-lib && python -m pytest tests/test_unified_variant_tracking.py::TestFixedInputTracking -v
```

These tests verify:
- Fixed inputs appear in `_lineage.inputs` as rid_tracking entries
- Changing a Fixed input causes skip_computed to re-run

### Test 2: Fixed input record_ids are computed correctly
Add a test that verifies the internal computation produces the same result as the old scihist path:
- Create a Fixed input
- Run scihist.for_each
- Check that `_lineage.inputs` contains the correct `__rid_*` entry

### Test 3: Backward compatibility
Verify that if someone passes `_lineage_fixed_rids` explicitly to scidb.for_each, it still works (uses the provided value instead of computing).

## Migration Impact

### Breaking Changes
None — this is purely internal refactoring.

### API Changes
- `scidb.for_each`: `_lineage_fixed_rids` parameter now optional (computed internally if not provided)
- New internal function: `scidb.foreach._compute_fixed_input_rids()` (private helper)

### User-Facing Changes
None — users should see identical behavior.

## Rollback Plan

If tests fail or unexpected issues arise:
1. Revert the scihist/foreach.py changes (restore lines 102-124 and 146)
2. Revert the scidb/foreach.py changes (remove new helper and internal computation)
3. Keep the `_compute_fixed_input_rids()` helper function for future use

## Success Criteria

✅ All existing scihist tests pass
✅ All existing scidb tests pass
✅ Fixed input record_ids appear correctly in `_lineage.inputs`
✅ skip_computed works correctly with Fixed inputs
✅ ~25 lines removed from scihist/foreach.py
✅ No user-facing behavior changes
