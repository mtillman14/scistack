# Pending Constants in scistack-gui

## Overview

**Pending constants** are temporary constant values that users want to test in the GUI before committing them to the database. They allow experimenting with different parameter values without immediately persisting them to the pipeline history.

## Storage

Pending constants are stored in the `_pipeline_pending_constants` table:
```sql
CREATE TABLE _pipeline_pending_constants (
    constant_name VARCHAR,
    value VARCHAR
)
```

- **constant_name**: The parameter name (e.g., "window_seconds")
- **value**: The string representation of the value to try
- Multiple values can be pending for the same constant name

## User Workflow

1. **Add pending constant**: User enters a constant value in the GUI (e.g., `window_seconds = 45`)
2. **Run with pending constants**: When a function is executed, pending constants override database constants
3. **Persist on success**: After a successful run, the pending constant is automatically saved to the database
4. **Clear pending**: User can manually clear pending constants or they persist until explicitly removed

## Implementation Pattern

### Old Approach (Complex Cross-Product)

Previously, the GUI would:
1. Get all variants from the database
2. Get all pending constants
3. Generate a **cross-product** of all constant combinations
4. Create synthetic variants for every possible combination
5. Run for_each for each synthetic variant

**Problem**: This created exponential growth in variants when multiple constants had multiple pending values. For example:
- DB variant: `{window_seconds: 30, sample_interval: 5}`
- Pending: `window_seconds: [45, 60]` and `sample_interval: [10]`
- Result: 2 × 1 = 2 additional synthetic variants, plus the original

The cross-product logic was complex (~90 lines) and made it hard to reason about which variants would actually run.

### New Approach (Simple Override)

The simplified approach:
1. Get variants from the database (or infer from manual edges)
2. Get pending constants
3. When building inputs for for_each, **override** constants with pending values
4. Let scihist/scidb handle constant tracking via version_keys

**Code pattern**:
```python
# Build inputs for a variant
inputs = {}
for param, type_names in variant["input_types"].items():
    inputs[param] = resolve_variable_class(type_names)

# Add constants from the variant
constants_to_use = dict(variant["constants"])

# Override with pending constants
pending_consts = pipeline_store.get_pending_constants(db)
for const_name, pending_values in pending_consts.items():
    if const_name in constants_to_use:
        # Use the first pending value (or implement multi-value handling)
        constants_to_use[const_name] = coerce_value(next(iter(pending_values)))

# Merge into inputs
inputs.update(constants_to_use)

# Call for_each
for_each(fn, inputs=inputs, outputs=outputs, ...)
```

## Handling Multiple Pending Values

When a constant has multiple pending values (e.g., user wants to try `window_seconds: [30, 45, 60]`), there are several strategies:

### Strategy 1: Sequential Execution (Recommended)
Run for_each multiple times, once for each pending value:
```python
for const_name, pending_values in relevant_pending_consts.items():
    for pending_value in pending_values:
        constants = {**variant["constants"], const_name: pending_value}
        inputs = {**variable_inputs, **constants}
        for_each(fn, inputs=inputs, outputs=outputs, ...)
```

### Strategy 2: First Value Only (Simplest)
Only use the first pending value:
```python
for const_name, pending_values in pending_consts.items():
    if const_name in constants_to_use:
        constants_to_use[const_name] = coerce_value(next(iter(pending_values)))
```

### Strategy 3: User Confirmation
If multiple values exist, ask the user which one to use or whether to run all.

## Version Key Tracking

scihist automatically tracks constants in `version_keys`, so:
- Each unique constant value creates a separate version
- No need for the GUI to manually create cross-products
- The database naturally deduplicates based on (function_hash, input_rids, version_keys)

## Benefits of Simplified Approach

1. **Eliminates ~90 lines** of complex cross-product logic
2. **Simpler mental model**: pending constants override DB constants
3. **Easier testing**: clear input → output mapping
4. **Leverages existing infrastructure**: scihist already handles constant versioning
5. **No exponential explosion**: number of runs scales linearly with pending values

## Migration Notes

The simplified approach changes behavior:
- **Before**: Pending constants created *additional* variants alongside DB variants
- **After**: Pending constants *override* DB constants during execution

If users need to run both DB constants and pending constants:
1. Run with pending constants (they override)
2. Clear pending constants
3. Run again with DB constants

Alternatively, the GUI could add a "Run without overrides" option to skip pending constant merging.

## Example Scenarios

### Scenario 1: First-time function run
- No DB history exists
- User wires function manually and adds pending constants
- Pending constants are used directly (nothing to override)

### Scenario 2: Re-running with new constant
- DB has variant: `{window_seconds: 30}`
- User adds pending: `{window_seconds: 45}`
- Run uses: `{window_seconds: 45}` (overrides DB value)
- After successful run, both versions exist in DB

### Scenario 3: Multiple pending values
- User adds pending: `{window_seconds: [30, 45, 60]}`
- Run calls for_each 3 times (Strategy 1) or uses first value (Strategy 2)

## Related Concepts

- **Variants**: Different combinations of (constants, output_type) for a function
- **Schema iteration**: Orthogonal to constants; schema keys (e.g., "Subject", "Session") define data subsets
- **Version keys**: scihist's internal constant tracking mechanism
