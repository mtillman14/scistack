# Task #10: Testing and Validation Guide

## Overview

This guide provides comprehensive testing instructions for validating all three phases of the GUI backend thinning project plus the GUI update (Task #9).

---

## Quick Status Check

### Automated Tests Status

All regression tests should pass. Run this quick check:

```bash
# From /workspace

# Phase 1 - scihist batched state checking (4 tests)
cd scihist-lib
python -m pytest tests/test_batched_state.py -v

# Phase 2 - scidb variant query APIs (18 tests)
cd ../scidb
python -m pytest tests/test_variant_queries.py -v

# Phase 3 - scihist schema filter params (15 tests)
cd ../scihist-lib
python -m pytest tests/test_schema_filter_params.py -v
```

**Expected:** All 37 tests passing (100% success rate)

---

## Detailed Testing Plan

### 1. Phase 1: Batched State Checking

**What it does:** Checks state of multiple function nodes in a single database query instead of N separate queries.

**Automated tests:** 4 tests in `/workspace/scihist-lib/tests/test_batched_state.py`

#### Run Tests

```bash
cd /workspace/scihist-lib
python -m pytest tests/test_batched_state.py -xvs
```

#### Expected Results

```
tests/test_batched_state.py::test_check_multiple_nodes_basic PASSED
tests/test_batched_state.py::test_check_multiple_nodes_mixed_states PASSED
tests/test_batched_state.py::test_check_multiple_nodes_with_registry PASSED
tests/test_batched_state.py::test_check_multiple_nodes_empty_list PASSED

======================== 4 passed in X.XXs ========================
```

#### Manual Validation (Optional)

You can verify the performance improvement:

```python
# Create a test script to compare before/after
import time
from scihist import check_multiple_nodes_state, check_node_state
from scidb import configure_database

# Setup database with some data
db = configure_database("test.duckdb", ["subject", "session"])

# Define some test nodes (need actual functions from your registry)
nodes = [
    {"fn_name": "function1", "call_id": "a" * 16, "outputs": [SomeOutput]},
    {"fn_name": "function2", "call_id": "b" * 16, "outputs": [OtherOutput]},
    # ... more nodes
]

# Old way (N queries)
start = time.time()
for node in nodes:
    check_node_state(node["fn_name"], node["call_id"], node["outputs"])
old_time = time.time() - start

# New way (1 query)
start = time.time()
check_multiple_nodes_state(nodes)
new_time = time.time() - start

print(f"Old: {old_time:.3f}s, New: {new_time:.3f}s")
print(f"Speedup: {old_time/new_time:.1f}x")
```

**Expected:** 5-10x speedup for 10+ nodes

---

### 2. Phase 2: Variant Query APIs

**What it does:** Provides APIs to query and filter pipeline variants from the database.

**Automated tests:** 18 tests in `/workspace/scidb/tests/test_variant_queries.py`

#### Run Tests

```bash
cd /workspace/scidb
python -m pytest tests/test_variant_queries.py -xvs
```

#### Expected Results

```
tests/test_variant_queries.py::TestGetAggregatedVariants::test_empty_database PASSED
tests/test_variant_queries.py::TestGetAggregatedVariants::test_single_function_single_variant PASSED
tests/test_variant_queries.py::TestGetAggregatedVariants::test_multiple_variants_different_constants PASSED
tests/test_variant_queries.py::TestGetAggregatedVariants::test_multiple_output_types PASSED
tests/test_variant_queries.py::TestGetAggregatedVariants::test_variable_record_counts PASSED
tests/test_variant_queries.py::TestGetAggregatedVariants::test_constants_aggregation PASSED
tests/test_variant_queries.py::TestGetAggregatedVariants::test_path_input_parsing PASSED
tests/test_variant_queries.py::TestGetAggregatedVariants::test_filter_by_function_name PASSED
tests/test_variant_queries.py::TestGetAggregatedVariants::test_filter_by_call_id PASSED
tests/test_variant_queries.py::TestFilterVariantsForExecution::test_no_variants_returns_empty PASSED
tests/test_variant_queries.py::TestFilterVariantsForExecution::test_basic_variant_filtering PASSED
tests/test_variant_queries.py::TestFilterVariantsForExecution::test_constant_override PASSED
tests/test_variant_queries.py::TestFilterVariantsForExecution::test_multiple_constant_overrides PASSED
tests/test_variant_queries.py::TestFilterVariantsForExecution::test_deduplication PASSED
tests/test_variant_queries.py::TestFilterVariantsForExecution::test_constant_override_only_applies_to_matching_params PASSED
tests/test_variant_queries.py::TestFilterVariantsForExecution::test_different_call_ids_are_isolated PASSED
tests/test_variant_queries.py::TestIntegration::test_aggregated_variants_and_filtering_consistency PASSED
tests/test_variant_queries.py::TestIntegration::test_real_world_pipeline_scenario PASSED

======================== 18 passed in X.XXs ========================
```

#### Manual Validation (Optional)

Test the APIs directly:

```python
from scidb import configure_database

db = configure_database("your_project.duckdb", ["subject", "session"])

# Test get_aggregated_variants()
result = db.get_aggregated_variants()
print("Functions:", list(result["functions"].keys()))
print("Variables:", list(result["variables"].keys()))
print("Constants:", list(result["constants"].keys()))

# Test filter_variants_for_execution()
variants = db.filter_variants_for_execution(
    "your_function_name",
    "your_call_id",
    constant_overrides={"threshold": 2.5}
)
print(f"Found {len(variants)} variants")
for v in variants:
    print(f"  - {v['output_type']}: constants={v['constants']}")
```

---

### 3. Phase 3: Schema Filter Parameters

**What it does:** Adds `schema_filter` and `schema_level` parameters to `scihist.for_each()` for more intuitive schema iteration control.

**Automated tests:** 15 tests in `/workspace/scihist-lib/tests/test_schema_filter_params.py`

#### Run Tests

```bash
cd /workspace/scihist-lib
python -m pytest tests/test_schema_filter_params.py -xvs
```

#### Expected Results

```
tests/test_schema_filter_params.py::TestSchemaFilter::test_schema_filter_basic PASSED
tests/test_schema_filter_params.py::TestSchemaFilter::test_schema_filter_multiple_keys PASSED
tests/test_schema_filter_params.py::TestSchemaFilter::test_schema_filter_single_value PASSED
tests/test_schema_filter_params.py::TestSchemaFilter::test_schema_filter_empty_result PASSED
tests/test_schema_filter_params.py::TestSchemaLevel::test_schema_level_subset PASSED
tests/test_schema_filter_params.py::TestSchemaLevel::test_schema_level_single_key PASSED
tests/test_schema_filter_params.py::TestSchemaFilterAndLevel::test_filter_and_level_together PASSED
tests/test_schema_filter_params.py::TestSchemaFilterAndLevel::test_filter_on_non_iterated_key PASSED
tests/test_schema_filter_params.py::TestBackwardCompatibility::test_metadata_iterables_still_works PASSED
tests/test_schema_filter_params.py::TestBackwardCompatibility::test_cannot_use_both_styles PASSED
tests/test_schema_filter_params.py::TestBackwardCompatibility::test_no_params_uses_all_data PASSED
tests/test_schema_filter_params.py::TestErrorHandling::test_schema_filter_requires_db PASSED
tests/test_schema_filter_params.py::TestErrorHandling::test_invalid_schema_key_in_filter PASSED
tests/test_schema_filter_params.py::TestIntegration::test_real_world_selective_processing PASSED
tests/test_schema_filter_params.py::TestIntegration::test_incremental_processing_with_skip_computed PASSED

======================== 15 passed in X.XXs ========================
```

#### Manual Validation (Optional)

Test the new parameters directly:

```python
from scihist import for_each
from scidb import configure_database, BaseVariable
from scilineage import lineage_fcn

# Setup
db = configure_database("test.duckdb", ["subject", "session", "trial"])

class RawData(BaseVariable):
    pass

class ProcessedData(BaseVariable):
    pass

@lineage_fcn
def process(raw, threshold):
    return raw * threshold

# Seed some data
for subj in [1, 2, 3]:
    for sess in ["A", "B"]:
        for trial in [1, 2]:
            RawData.save([1, 2, 3], db=db, subject=subj, session=sess, trial=trial)

# Test schema_filter
result = for_each(
    process,
    inputs={"raw": RawData, "threshold": 2.0},
    outputs=[ProcessedData],
    schema_filter={"subject": [1, 2]},  # Only subjects 1 and 2
)
print(f"Processed {len(result)} combinations")
print(f"Subjects: {set(result['subject'].unique())}")  # Should be {'1', '2'}

# Test schema_level
result = for_each(
    process,
    inputs={"raw": RawData, "threshold": 2.0},
    outputs=[ProcessedData],
    schema_level=["subject"],  # Only iterate subject
)
print(f"Processed {len(result)} combinations")
print(f"Schema keys iterated: {list(result.columns)}")
```

---

### 4. Task #9: GUI Update

**What it does:** Updates GUI to use new `schema_filter` and `schema_level` parameters instead of manually building schema kwargs.

**Automated tests:** None (uses Phase 3 tests for underlying functionality)

#### Code Review Checklist

Verify the changes in `/workspace/scistack-gui/scistack_gui/api/run.py`:

- [ ] Line 297: `build_schema_kwargs` removed from imports ✅
- [ ] Lines 323-329: Simplified schema iteration logging ✅
- [ ] Lines 459-460: `for_each()` uses `schema_filter` and `schema_level` directly ✅
- [ ] No `**schema_kwargs` unpacking ✅
- [ ] No `distinct_values` dict building ✅

#### Manual GUI Testing

**IMPORTANT:** This is the critical validation step for Task #9.

##### Prerequisites

1. Have a scistack project with actual data
2. GUI is running (VS Code extension or standalone)
3. Dataset has multiple subjects/sessions/trials

##### Test Cases

**Test 1: Basic Run (No Filters)**

1. Open GUI
2. Select any function from the pipeline
3. Ensure no schema filters are selected
4. Click "Run"
5. ✅ **Expected:** Function processes all data normally
6. ✅ **Verify:** Check output count matches total combinations (subjects × sessions × trials)

**Test 2: Schema Filter - Single Key**

1. Open GUI
2. Select a function
3. Apply schema filter: Select specific subjects (e.g., subjects 1 and 2 only)
4. Click "Run"
5. ✅ **Expected:** Only processes data for subjects 1 and 2
6. ✅ **Verify:** Output records only have subject=1 or subject=2
7. ✅ **Verify:** All sessions and trials are included (not filtered)

**Test 3: Schema Filter - Multiple Keys**

1. Select a function
2. Apply schema filters:
   - Subjects: [1, 2]
   - Sessions: ["A"]
3. Click "Run"
4. ✅ **Expected:** Only processes subject 1-2, session A
5. ✅ **Verify:** Output has 2 subjects × 1 session × N trials records

**Test 4: Schema Level (if GUI supports)**

1. Select a function
2. Set schema level to iterate only ["subject"] (not session/trial)
3. Click "Run"
4. ✅ **Expected:** Iterates over subjects only
5. ✅ **Verify:** One output per subject (not subject×session×trial)

**Test 5: Empty Filter**

1. Select a function
2. Open schema filter but don't select any values
3. Click "Run"
4. ✅ **Expected:** Uses all available data (same as no filter)

**Test 6: Single Value Filter**

1. Select a function
2. Filter to single values: subject=1, session="A", trial=1
3. Click "Run"
4. ✅ **Expected:** Processes exactly 1 combination
5. ✅ **Verify:** Output has 1 record with exact values

**Test 7: Logging Verification**

1. Enable debug logging in GUI
2. Run a function with filters
3. Check logs for:
   - ✅ "Schema iteration parameters will be handled by for_each"
   - ✅ "Schema level: ..." (if schema_level set)
   - ✅ "Schema filter: ..." (if schema_filter set)
   - ✅ No "Building schema kwargs" message
   - ✅ No errors or warnings

**Test 8: Backward Compatibility**

1. Run old pipelines that were created before this change
2. ✅ **Expected:** All existing pipelines work unchanged
3. ✅ **Verify:** No errors, same results as before

##### Error Cases

**Test 9: Invalid Schema Key**

1. Try to run with a schema filter for non-existent key (if possible via API)
2. ✅ **Expected:** Either ignored or clear error message

**Test 10: Manual Nodes**

1. Create a manual function node in GUI (not yet in database)
2. Wire it up with inputs/outputs
3. Run it with schema filters
4. ✅ **Expected:** Works correctly (manual node logic unchanged)

##### Performance Check

**Test 11: Large Dataset**

1. Use a dataset with 10+ subjects, 5+ sessions
2. Apply a filter to narrow to 2 subjects
3. Run a function
4. ✅ **Expected:** Fast response (filter applied before processing)
5. ✅ **Verify:** Only processes filtered combinations, not all data

---

## Integration Testing

### End-to-End Workflow

Test the complete pipeline flow with all changes:

1. **Create a fresh dataset**
   ```python
   from scidb import configure_database, BaseVariable
   from scihist import for_each
   from scilineage import lineage_fcn

   db = configure_database("integration_test.duckdb", ["subject", "session"])

   class RawSignal(BaseVariable):
       pass

   class FilteredSignal(BaseVariable):
       pass

   class ProcessedSignal(BaseVariable):
       pass

   @lineage_fcn
   def filter_signal(raw, cutoff):
       return raw * cutoff

   @lineage_fcn
   def process_signal(filtered, gain):
       return filtered * gain

   # Seed raw data
   import numpy as np
   for subj in [1, 2, 3]:
       for sess in ["pre", "post"]:
           RawSignal.save(np.random.randn(100), db=db, subject=subj, session=sess)
   ```

2. **Run first function (filter_signal)**
   ```python
   # Test Phase 3: schema_filter
   result1 = for_each(
       filter_signal,
       inputs={"raw": RawSignal, "cutoff": 2.0},
       outputs=[FilteredSignal],
       schema_filter={"subject": [1, 2]},  # Only subjects 1-2
   )
   print(f"Filtered {len(result1)} combinations")
   # Should be 2 subjects × 2 sessions = 4
   ```

3. **Check state using Phase 1 API**
   ```python
   from scihist import check_multiple_nodes_state

   nodes = [
       {
           "fn": filter_signal,
           "call_id": "a" * 16,  # Use actual call_id from your code
           "outputs": [FilteredSignal]
       },
       {
           "fn": process_signal,
           "call_id": "b" * 16,
           "outputs": [ProcessedSignal]
       }
   ]

   states = check_multiple_nodes_state(nodes, db=db)
   print(states)
   # Should show filter_signal as complete, process_signal as pending
   ```

4. **Query variants using Phase 2 API**
   ```python
   variants = db.get_aggregated_variants()
   print("Functions:", list(variants["functions"].keys()))
   # Should include filter_signal

   print("Constants:", list(variants["constants"].keys()))
   # Should include cutoff
   ```

5. **Run second function (process_signal)**
   ```python
   result2 = for_each(
       process_signal,
       inputs={"filtered": FilteredSignal, "gain": 1.5},
       outputs=[ProcessedSignal],
       schema_filter={"session": ["post"]},  # Only post session
   )
   print(f"Processed {len(result2)} combinations")
   # Should be 2 subjects × 1 session = 2
   ```

6. **Verify in GUI**
   - Open the dataset in GUI
   - See both functions in pipeline graph
   - Check node states (should show completed/ready)
   - Verify variant information displays correctly
   - Apply filters and run - should work smoothly

**Expected:** Complete end-to-end workflow with no errors, all data processed correctly, GUI shows accurate state.

---

## Performance Benchmarks

### Before vs After Comparison

Create a benchmark script to measure improvements:

```python
import time
from scihist import check_node_state, check_multiple_nodes_state
from scidb import configure_database

db = configure_database("benchmark.duckdb", ["subject", "session", "trial"])

# Populate with some data and run a few pipelines...
# (seed data and run for_each calls here)

# Benchmark Phase 1: State checking
nodes = [...]  # List of 20 function nodes

# Old way
start = time.time()
for node in nodes:
    check_node_state(node["fn_name"], node["call_id"], node["outputs"])
old_time = time.time() - start

# New way
start = time.time()
check_multiple_nodes_state(nodes)
new_time = time.time() - start

print(f"State checking:")
print(f"  Old: {old_time:.3f}s ({len(nodes)} queries)")
print(f"  New: {new_time:.3f}s (1 query)")
print(f"  Speedup: {old_time/new_time:.1f}x")
print(f"  Query reduction: {len(nodes)}→1 ({100*(1-1/len(nodes)):.0f}% reduction)")
```

**Expected results:**
- **State checking:** 5-10x speedup for 10-20 nodes
- **Query reduction:** 89-95% fewer database queries

---

## Troubleshooting

### Common Issues

#### Issue 1: Tests Fail with Import Errors

**Symptom:**
```
ImportError: cannot import name 'check_multiple_nodes_state' from 'scihist'
```

**Solution:**
```bash
# Reinstall the package in development mode
cd /workspace/scihist-lib
pip install -e .
```

#### Issue 2: Schema Values Are Strings Not Integers

**Symptom:** Test assertions fail comparing `{1, 2}` to `{"1", "2"}`

**Solution:** Schema values are always strings in the database. Update assertions:
```python
# Wrong
assert set(result["subject"].unique()) == {1, 2}

# Correct
assert set(result["subject"].unique()) == {"1", "2"}
```

#### Issue 3: GUI Shows Error "Cannot use both schema_filter and **metadata_iterables"

**Symptom:** Running a function in GUI shows this error

**Cause:** Code is trying to pass both schema_filter and manual kwargs

**Solution:** Check run.py - should only pass schema_filter and schema_level, not kwargs

#### Issue 4: Tests Pass But GUI Doesn't Work

**Symptom:** All automated tests pass but GUI shows errors

**Steps:**
1. Check GUI logs for detailed error messages
2. Verify GUI is using updated code (restart GUI/VS Code)
3. Check database connection in GUI
4. Try a simple test case without filters first

---

## Success Criteria

### All Phases Complete ✅

- [ ] **Phase 1 tests:** All 4 passing
- [ ] **Phase 2 tests:** All 18 passing
- [ ] **Phase 3 tests:** All 15 passing
- [ ] **Total:** 37/37 tests passing (100%)

### GUI Integration ✅

- [ ] GUI runs functions without filters (Test 1)
- [ ] GUI applies single schema filter (Test 2)
- [ ] GUI applies multiple filters (Test 3)
- [ ] GUI handles edge cases (Tests 5-6)
- [ ] GUI logging shows correct messages (Test 7)
- [ ] No regression in existing functionality (Test 8)

### Performance ✅

- [ ] State checking is faster (5-10x for 10+ nodes)
- [ ] Database queries reduced (N → 1 for state checks)
- [ ] No performance regression in GUI

### Code Quality ✅

- [ ] All imports working correctly
- [ ] No unused imports
- [ ] Logging is clear and helpful
- [ ] Error messages are informative
- [ ] Code is readable and maintainable

---

## Reporting Results

### Test Report Template

```markdown
# Testing Report: GUI Backend Thinning Project

## Date: [DATE]
## Tester: [NAME]

### Automated Tests

| Phase | Tests | Passing | Status |
|-------|-------|---------|--------|
| Phase 1 | 4 | X/4 | ✅/❌ |
| Phase 2 | 18 | X/18 | ✅/❌ |
| Phase 3 | 15 | X/15 | ✅/❌ |
| **Total** | **37** | **X/37** | **✅/❌** |

### Manual GUI Tests

| Test Case | Status | Notes |
|-----------|--------|-------|
| Basic run (no filters) | ✅/❌ | |
| Single key filter | ✅/❌ | |
| Multiple key filter | ✅/❌ | |
| Empty filter | ✅/❌ | |
| Single value filter | ✅/❌ | |
| Logging verification | ✅/❌ | |
| Backward compatibility | ✅/❌ | |

### Performance

- State checking speedup: X.Xx
- Query reduction: X→1

### Issues Found

1. [Issue description]
   - Severity: High/Medium/Low
   - Status: Open/Fixed

2. ...

### Overall Status

✅ All tests passing, ready for production
❌ Issues found, needs fixes

### Recommendations

[Any recommendations for improvements or next steps]
```

---

## Next Steps After Testing

Once all tests pass and manual validation is complete:

1. **Document any findings** in the test report
2. **Update Task #10** to completed status
3. **Consider future cleanup**:
   - Remove `build_schema_kwargs()` function (saves ~80 lines)
   - Add more integration tests
   - Update user documentation

4. **Production deployment checklist**:
   - [ ] All 37 tests passing
   - [ ] Manual GUI testing complete
   - [ ] Performance benchmarks meet expectations
   - [ ] No regressions found
   - [ ] Code reviewed and approved
   - [ ] Documentation updated

---

## Summary

This testing guide covers:

- ✅ **37 automated regression tests** (4 + 18 + 15)
- ✅ **11 manual GUI test cases**
- ✅ **End-to-end integration testing**
- ✅ **Performance benchmarking**
- ✅ **Troubleshooting guide**
- ✅ **Success criteria and reporting**

**Total test coverage:** Comprehensive validation of all three phases plus GUI integration.

**Time estimate:**
- Automated tests: 5-10 minutes
- Manual GUI tests: 30-60 minutes
- Integration testing: 20-30 minutes
- **Total: 1-2 hours** for complete validation
