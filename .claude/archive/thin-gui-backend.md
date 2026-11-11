# Plan: Thin the scistack-gui Backend by Delegating to Lower Layers

## Overview

This plan moves redundant business logic from scistack-gui backend to the appropriate layers (scihist/scidb/scifor), keeping the GUI focused on presentation, UI state management, and protocol handling.

**Goal:** Reduce scistack-gui backend complexity by ~40-50%, eliminating ~400-500 lines of redundant business logic.

---

## Phase 1: Replace Run State Computation (HIGHEST IMPACT)

**Problem:** The GUI reimplements staleness checking in `domain/run_state.py` (158 lines) using basic heuristics, when scihist already provides sophisticated provenance-based state checking.

### Changes Required

#### 1.1 Add bulk state checking API to scihist

**File:** `/workspace/scihist-lib/src/scihist/state.py`

Add new function:
```python
def check_multiple_nodes_state(
    nodes: list[dict],
    db=None,
) -> dict[str, dict]:
    """Check state for multiple function nodes in parallel.

    Args:
        nodes: List of dicts with:
            - fn_name: str
            - call_id: str
            - outputs: list[type]
        db: DatabaseManager instance

    Returns:
        Dict mapping node_id (fn__{fn_name}__{call_id}) to:
        {
            "state": "green" | "grey" | "red",
            "counts": {"up_to_date": N, "stale": N, "missing": N},
        }
    """
```

**Why:** The GUI needs to check state for all nodes in the graph. Calling `check_node_state()` individually would be inefficient (N database queries). This batches the work.

**Implementation notes:**
- Reuse existing `check_node_state()` logic
- Optimize to share database queries across nodes where possible
- Handle Python and MATLAB functions

#### 1.2 Update GUI to use scihist state checking

**Files to modify:**
- `/workspace/scistack-gui/scistack_gui/services/pipeline_service.py`
- Delete: `/workspace/scistack-gui/scistack_gui/domain/run_state.py`

**Changes:**
1. In `get_pipeline_graph()`, replace:
   ```python
   from .domain.run_state import propagate_run_states
   run_states = propagate_run_states(...)
   ```
   With:
   ```python
   from scihist import check_multiple_nodes_state
   # Build nodes list from aggregated data
   nodes = [
       {"fn_name": fn, "call_id": cid, "outputs": list(outputs)}
       for (fn, cid), outputs in fn_outputs.items()
   ]
   state_results = check_multiple_nodes_state(nodes, db=db)
   run_states = {nid: result["state"] for nid, result in state_results.items()}
   ```

2. Add variable node states (scihist only checks function nodes):
   ```python
   # Variables are always green (data sources)
   for var_type in all_var_types:
       run_states[f"var__{var_type}"] = "green"
   ```

**Benefits:**
- Eliminates 158 lines from GUI
- Uses scihist's sophisticated checking (lineage graph, function hash, input rids)
- Catches more staleness cases (code changes, upstream changes)

**Testing:**
- Unit test the new `check_multiple_nodes_state()` function
- Integration test: verify GUI graph shows correct states
- Test pending constants still downgrade green→grey

---

## Phase 2: Add Variant Query APIs to scidb

**Problem:** The GUI aggregates variants in `domain/graph_builder.py` (`aggregate_variants()`, 84 lines) and has complex filtering/deduplication in `domain/variant_resolver.py` (247 lines). This is business logic that scidb should expose as query APIs.

### Changes Required

#### 2.1 Add aggregated variant query to scidb

**File:** `/workspace/scidb/src/scidb/database.py`

Add new method:
```python
def get_aggregated_variants(
    self,
    fn_name: str | None = None,
    call_id: str | None = None,
) -> dict:
    """Get aggregated variant data for pipeline visualization.

    Args:
        fn_name: Optional function name to filter
        call_id: Optional call_id to filter

    Returns:
        Dict with:
        {
            "functions": {
                (fn_name, call_id): {
                    "input_params": {param: var_type},
                    "outputs": [var_type1, var_type2],
                    "constants": {param: [val1, val2]},
                    "variant_count": int,
                    "variants": [
                        {
                            "input_types": {...},
                            "constants": {...},
                            "output_type": str,
                            "record_count": int,
                        }
                    ],
                }
            },
            "variables": {
                var_type: {
                    "record_count": int,
                    "schema_keys": [key1, key2],
                    "data_columns": [col1, col2],
                }
            },
            "constants": {
                const_name: {
                    values: [
                        {"value": val, "record_count": N},
                    ],
                    functions: [(fn_name, call_id), ...],
                }
            },
            "path_inputs": {
                param_name: {
                    "template": str,
                    "root_folder": str | None,
                    "functions": [(fn_name, call_id), ...],
                }
            },
        }
    """
```

**Why:** This encapsulates the aggregation logic that the GUI currently does in `aggregate_variants()`. Single query returns all the data needed for graph building.

**Implementation notes:**
- Reuse `list_pipeline_variants()` internally
- Add query for variable metadata (schema_keys, data_columns from _variables table)
- Parse PathInput from __inputs JSON
- Group constants by name, tracking which functions use them

#### 2.2 Add variant filtering method to scidb

**File:** `/workspace/scidb/src/scidb/database.py`

Add new method:
```python
def filter_variants_for_execution(
    self,
    fn_name: str,
    call_id: str,
    schema_filter: dict[str, list] | None = None,
    constant_overrides: dict[str, Any] | None = None,
) -> list[dict]:
    """Filter variants for execution based on schema and constant selection.

    Args:
        fn_name: Function name
        call_id: Call ID (16 hex chars)
        schema_filter: {schema_key: [selected_values]} to filter by
        constant_overrides: {const_name: value} to override DB constants

    Returns:
        List of variant dicts ready for for_each execution:
        [
            {
                "input_types": {param: var_type},
                "output_type": var_type,
                "constants": {param: value},
            }
        ]
    """
```

**Why:** This replaces the GUI's `filter_variants()`, `deduplicate_variants()`, and `merge_pending_constants()` logic. Returns exactly what needs to be executed.

**Implementation notes:**
- Use `list_pipeline_variants()` to get base variants
- Filter by schema_filter (subset match on variant's schema keys)
- Apply constant_overrides, deduplicating after merge
- Return deduplicated list

#### 2.3 Update GUI to use new query APIs

**Files to modify:**
- `/workspace/scistack-gui/scistack_gui/services/pipeline_service.py`
- Delete most of: `/workspace/scistack-gui/scistack_gui/domain/graph_builder.py`
- Delete: `/workspace/scistack-gui/scistack_gui/domain/variant_resolver.py`

**Changes:**
1. In `get_pipeline_graph()`, replace:
   ```python
   variants = db.list_pipeline_variants()
   variables = db.list_variables()
   agg = aggregate_variants(variants, ...)
   # ... complex aggregation logic ...
   ```
   With:
   ```python
   agg = db.get_aggregated_variants()
   ```

2. The simplified `graph_builder.py` just transforms the aggregated data into React Flow node/edge format (pure presentation logic).

3. In `run_service.start_run()`, replace variant filtering logic with:
   ```python
   variants_to_run = db.filter_variants_for_execution(
       fn_name=fn_name,
       call_id=call_id,
       schema_filter=schema_filter,
       constant_overrides=pending_constants,
   )
   ```

**Benefits:**
- Eliminates ~330 lines from GUI (aggregate_variants + all of variant_resolver.py)
- Business logic moves to scidb where it belongs
- Easier to optimize (database can do filtering efficiently)
- Reusable by other tools (CLI, notebooks)

**Testing:**
- Unit test new scidb methods with various filter combinations
- Integration test: GUI graph shows same nodes/edges as before
- Test execution with schema_filter and constant_overrides

---

## Phase 3: Enhance scihist.for_each to Accept Higher-Level Parameters

**Problem:** The GUI calls `scihist.for_each()` but has to pre-compute schema iteration kwargs and handle constant cross-products. scihist should accept higher-level parameters.

### Changes Required

#### 3.1 Add schema_filter parameter to scihist.for_each

**File:** `/workspace/scihist-lib/src/scihist/foreach.py`

Update signature:
```python
def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[type],
    # ... existing params ...
    schema_filter: dict[str, list] | None = None,  # NEW
    schema_level: list[str] | None = None,          # NEW
    **metadata_iterables: list[Any],
) -> pd.DataFrame | None
```

**Behavior:**
- `schema_level`: Which schema keys to iterate (default: all)
- `schema_filter`: Which values to include per key
- If both are None, use `**metadata_iterables` as before (backward compatible)
- If provided, build the `**metadata_iterables` internally

**Implementation:**
```python
# Inside for_each, before delegating to scidb:
if schema_filter is not None or schema_level is not None:
    if metadata_iterables:
        raise ValueError("Cannot use both schema_filter and **metadata_iterables")

    # Get all schema keys from database
    db = active_db or get_database()
    all_schema_keys = db.get_schema()

    # Determine which keys to iterate
    iterate_keys = schema_level if schema_level is not None else all_schema_keys

    # Get distinct values for each key
    distinct_values = db.distinct_schema_values(inputs)

    # Build metadata_iterables
    for key in iterate_keys:
        if schema_filter and key in schema_filter:
            metadata_iterables[key] = schema_filter[key]
        else:
            metadata_iterables[key] = distinct_values.get(key, [])
```

**Why:** The GUI currently does this logic in `variant_resolver.build_schema_kwargs()`. Moving it to scihist makes the API more ergonomic.

#### 3.2 Update GUI to use new parameters

**Files to modify:**
- `/workspace/scistack-gui/scistack_gui/services/run_service.py`

**Changes:**
Replace:
```python
# Old: GUI builds schema kwargs
schema_kwargs = build_schema_kwargs(schema_level, all_schema_keys, schema_filter, distinct_values)
result = scihist.for_each(fn, inputs, outputs, **schema_kwargs)
```

With:
```python
# New: Pass high-level params
result = scihist.for_each(
    fn, inputs, outputs,
    schema_filter=schema_filter,
    schema_level=schema_level,
)
```

**Benefits:**
- Eliminates `build_schema_kwargs()` from GUI (~40 lines)
- More intuitive API
- Easier to test (scihist owns the logic)

**Testing:**
- Test scihist.for_each with schema_filter
- Test schema_level parameter
- Test backward compatibility (no filters = iterate all)

---

## Phase 4: Simplify Pending Constants Handling

**Problem:** The GUI has complex cross-product generation for pending constants in `merge_pending_constants()`. This can be simplified.

### Changes Required

#### 4.1 Document the recommended pattern

**File:** `/workspace/docs/claude/scistack-gui-pending-constants.md`

Create documentation explaining:
- Pending constants are values the user wants to try before running
- The GUI should pass them directly to for_each as constant inputs
- If multiple values are provided, call for_each multiple times (or accept list of constants)

#### 4.2 Simplify GUI pending constants logic

**Files to modify:**
- `/workspace/scistack-gui/scistack_gui/services/run_service.py`

**Current behavior:**
- GUI merges pending constants with DB constants
- Generates cross-product of all constant combinations
- Calls for_each once with all variants

**New behavior:**
- If user provides pending constant values, pass them directly to for_each
- Let scihist/scidb handle variant expansion
- Optionally: if user selects multiple constant values, call for_each multiple times

**Changes:**
```python
# Old: Complex merging and cross-product
targets = merge_pending_constants(fn_variants, pending_constants)
# ... deduplicate, cross-product ...

# New: Simple override
constants_to_use = {**fn_constants, **pending_constants}
result = scihist.for_each(
    fn,
    inputs={**variable_inputs, **constants_to_use},
    outputs=outputs,
    schema_filter=schema_filter,
)
```

**Benefits:**
- Eliminates ~90 lines of cross-product logic
- Simpler mental model
- scihist already handles constant tracking in version_keys

**Testing:**
- Test running with pending constants
- Verify constants are saved with correct version_keys
- Test clearing pending constants after successful run

---

## Phase 5: Add Variant Count Query

**Problem:** The GUI computes variant counts for display. This should be a simple query.

### Changes Required

#### 5.1 Add count method to scidb

**File:** `/workspace/scidb/src/scidb/database.py`

Add method:
```python
def count_variants(
    self,
    fn_name: str,
    call_id: str,
) -> int:
    """Count distinct variants for a function call site.

    Returns the number of distinct (constants, output_type) combinations
    for this function.
    """
```

**Implementation:**
- Query `list_pipeline_variants()`
- Count entries matching fn_name and call_id
- Could be optimized with direct SQL query

#### 5.2 Use in aggregated variant query

Include the count in the `get_aggregated_variants()` response (already planned in Phase 2.1).

---

## Implementation Order

### Week 1: Phase 1 (Run State)
**Why first:** Highest impact, most redundant logic, clearest win

1. Day 1-2: Implement `check_multiple_nodes_state()` in scihist
   - Write function
   - Add unit tests
   - Test with Python and MATLAB functions

2. Day 3-4: Update GUI to use new API
   - Modify `pipeline_service.py`
   - Delete `run_state.py`
   - Integration testing

3. Day 5: Validate and document
   - Compare GUI states before/after
   - Update `scistack-gui-backend-internals.md`
   - Create `scihist-state-api.md` if needed

### Week 2: Phase 2 (Variant Queries)
**Why second:** Eliminates the most code (~330 lines)

1. Day 1-3: Implement scidb query APIs
   - `get_aggregated_variants()`
   - `filter_variants_for_execution()`
   - Unit tests

2. Day 4-5: Update GUI
   - Simplify `graph_builder.py`
   - Delete `variant_resolver.py`
   - Update `run_service.py`
   - Integration testing

### Week 3: Phases 3-5 (Polish)
**Why last:** Lower impact, but completes the cleanup

1. Day 1-2: Phase 3 (schema_filter parameter)
2. Day 3-4: Phase 4 (pending constants simplification)
3. Day 5: Phase 5 (variant count) + final documentation

---

## Success Metrics

### Code Reduction
- **Target:** Eliminate ~400-500 lines from scistack-gui
  - `run_state.py`: 158 lines
  - `variant_resolver.py`: 247 lines
  - `graph_builder.py`: ~100 lines (partial, some stays)
  - Total: ~500 lines

### Complexity Reduction
- **Before:** GUI has 4 domain modules with business logic
- **After:** GUI has 2 domain modules (edge_resolver, simplified graph_builder)

### API Improvements
- scihist gains `check_multiple_nodes_state()` - reusable by CLI/notebooks
- scidb gains `get_aggregated_variants()` - reusable query API
- scidb gains `filter_variants_for_execution()` - reusable filtering

### Performance
- Batched state checking should be faster than N individual queries
- Database-side aggregation may be faster than Python aggregation

---

## Risks and Mitigations

### Risk 1: Breaking existing GUI functionality
**Mitigation:**
- Comprehensive integration tests before each phase
- Keep old code commented out initially, delete after validation
- Test with real databases (not just mocks)

### Risk 2: scihist/scidb API changes affect other users
**Mitigation:**
- All new APIs are additive (no breaking changes)
- Existing for_each behavior unchanged (backward compatible)
- Document new parameters clearly

### Risk 3: Performance regressions
**Mitigation:**
- Benchmark before/after for graph loading time
- Optimize database queries if needed
- Consider caching in scihist if hot paths are slow

### Risk 4: MATLAB integration breaks
**Mitigation:**
- Test with MATLAB functions at each phase
- MATLAB state checking already works in scihist
- MATLAB variant queries are the same as Python

---

## Testing Strategy

### Unit Tests
- New scihist functions (check_multiple_nodes_state)
- New scidb methods (get_aggregated_variants, filter_variants_for_execution)
- Schema filter logic in for_each

### Integration Tests
- GUI graph loading shows correct nodes/edges
- GUI graph states match scihist states
- Running functions with various filters works
- Pending constants work correctly

### Manual Testing
- Load real experiment database in GUI
- Verify graph looks correct
- Run functions and verify state updates
- Test MATLAB functions

---

## Documentation Updates

### New Documentation
1. `.claude/thin-gui-backend.md` (this file)
2. `docs/claude/scihist-state-api.md` - document state checking APIs
3. `docs/claude/scidb-variant-queries.md` - document new query APIs

### Updated Documentation
1. `docs/claude/scistack-gui-backend-internals.md` - reflect simplified architecture
2. `docs/claude/scihist-for-each-internals.md` - document schema_filter parameter
3. Update READMEs for scihist and scidb with new API examples

---

## Open Questions

1. **Should `get_aggregated_variants()` be a single method or multiple methods?**
   - Pro single: One query, easier to use
   - Pro multiple: More flexible, can cache independently
   - **Decision:** Start with single method, can split later if needed

2. **Should pending constants be stored in the database at all?**
   - Current: `_pipeline_pending_constants` table
   - Alternative: Pure UI state (JSON file like layout.json)
   - **Decision:** Keep in database for now (enables sharing pending state between GUI instances)

3. **Should scihist.for_each handle constant cross-products internally?**
   - Currently: GUI does cross-product, passes individual variants
   - Alternative: scihist accepts `{param: [val1, val2]}` and expands
   - **Decision:** Phase 4 explores this, but may defer to later

---

## Next Steps

1. **User approval:** Review this plan, adjust priorities if needed
2. **Start Phase 1:** Implement `check_multiple_nodes_state()` in scihist
3. **Create tracking tasks:** Use TaskCreate if desired
4. **Begin implementation:** Follow week-by-week schedule above
