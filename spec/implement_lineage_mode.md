# Lineage Mode Implementation

## Overview

Implementing two lineage modes to control how intermediate variables are handled:

- **strict** (default): All upstream BaseVariables must be saved before saving downstream results. Raises error if unsaved intermediate detected.
- **ephemeral**: Allows unsaved intermediates. Stores computation graph (function, inputs) without storing the actual data for unsaved variables.

## Design Decisions

### Configuration

- `lineage_mode` parameter added to `configure_database()`
- Stored as instance attribute on `DatabaseManager`
- Default: `"strict"`

### In-Memory Lineage Traversal

To detect unsaved intermediates, we need to traverse the in-memory chain:

1. Start from an ThunkOutput
2. Look at its pipeline_thunk.inputs
3. For each input that's an ThunkOutput, recurse
4. For each input that's a BaseVariable, check if it has a vhash
5. If BaseVariable has no vhash but wraps an ThunkOutput, recurse into that

### Strict Mode

When `lineage_mode="strict"`:

- During `BaseVariable.save()`, traverse upstream chain
- If any upstream BaseVariable lacks a vhash, raise `UnsavedIntermediateError`
- Message includes which variable type needs to be saved

### Ephemeral Mode

When `lineage_mode="ephemeral"`:

- During `BaseVariable.save()`, traverse upstream chain
- For unsaved intermediates, store lineage entry with `source_type="ephemeral"`
- Ephemeral entries include: type name, source function, source hash, but NO data/vhash
- Lineage queries can still trace through ephemeral nodes

## Files to Modify

1. `src/scidb/exceptions.py` - Add `UnsavedIntermediateError`
2. `src/scidb/database.py` - Add `lineage_mode` config, validation logic
3. `thunk-lib/src/thunk/lineage.py` - Add traversal functions
4. `src/scidb/lineage.py` - Re-export traversal functions
5. `src/scidb/variable.py` - Call validation during save()

---

## Implementation Progress

### Step 1: Add UnsavedIntermediateError exception

**Status: DONE**

Added to `src/scidb/exceptions.py`:

```python
class UnsavedIntermediateError(SciStackError):
    """Raised when strict mode detects an unsaved intermediate variable."""
    pass
```

### Step 2: Add lineage_mode to DatabaseManager

**Status: DONE**

- Added `lineage_mode` parameter to `DatabaseManager.__init__()`
- Added `lineage_mode` parameter to `configure_database()`
- Validates mode is one of: "strict", "ephemeral"
- Stored as `self.lineage_mode` on DatabaseManager instance

### Step 3: Implement in-memory traversal in thunk library

**Status: DONE**

Added to `thunk-lib/src/thunk/lineage.py`:

- `find_unsaved_variables(thunk_output)` - Returns list of unsaved BaseVariables in the upstream chain
- `traverse_upstream(thunk_output, visitor_fn)` - Generic traversal with callback

### Step 4: Implement strict mode validation

**Status: DONE**

In `BaseVariable.save()` (variable.py lines 176-195):

- When data is an ThunkOutput and lineage_mode="strict"
- Call `find_unsaved_variables()` to check upstream chain
- If any found, raise `UnsavedIntermediateError` with helpful message listing:
  - Each unsaved variable's type
  - The path through the computation chain
  - Instructions to either save intermediates or use ephemeral mode

### Step 5: Update extract_lineage for unsaved variables

**Status: DONE**

Modified `extract_lineage()` in thunk library:

- Unsaved BaseVariable now goes to `inputs` (not constants) with `source_type="unsaved_variable"`
- If unsaved variable wraps an ThunkOutput, includes source function info
- If just raw data, includes content_hash

### Step 6: Implement ephemeral lineage storage

**Status: DONE**

Added `DatabaseManager.save_ephemeral_lineage()` method (database.py):

- Takes ephemeral_id (e.g., "ephemeral:abc123"), variable_type, lineage
- Stores in \_lineage table without corresponding data entry
- Checks for duplicates to avoid redundant writes

In `BaseVariable.save()` when ephemeral mode (variable.py):

- Finds unsaved variables using `find_unsaved_variables()`
- For each unsaved variable wrapping an ThunkOutput:
  - Generates ephemeral ID: `"ephemeral:" + thunk.hash[:32]`
  - Extracts lineage from the inner ThunkOutput
  - Saves ephemeral lineage record to database

### Step 7: Update lineage querying for ephemeral entries

**Status: DONE**

Modified `_build_lineage_tree()` (database.py):

- Handle `source_type="unsaved_variable"` with `inner_source="thunk"`
- Look up ephemeral lineage by ID: `"ephemeral:" + source_hash[:32]`
- Mark nodes with `ephemeral=True` flag
- Handle unsaved variables with raw data (no thunk lineage) as leaf nodes

Modified `_format_lineage_node()` (database.py):

- Display ephemeral nodes with `[ephemeral]` marker
- Handle ephemeral IDs specially in vhash display
- Show "unsaved raw data" source for variables without thunk lineage

### Step 8: Update exports

**Status: DONE**

Updated `src/scidb/__init__.py`:

- Added `UnsavedIntermediateError` to exceptions exports
- Added `find_unsaved_variables` to lineage exports

Updated `src/scidb/lineage.py`:

- Added re-exports for `find_unsaved_variables` and `get_upstream_lineage`

Updated `thunk-lib/src/thunk/__init__.py`:

- Added exports for new lineage functions

---

## Usage Examples

### Strict Mode (Default)

```python
from scidb import configure_database, BaseVariable, thunk

db = configure_database("experiment.db")  # lineage_mode="strict" by default

@thunk
def process(data):
    return data * 2

raw = RawData(some_array)
# raw.save(...)  # MUST save first in strict mode!

result = process(raw)
ProcessedData(result).save(...)  # Raises UnsavedIntermediateError!
```

### Ephemeral Mode

```python
from scidb import configure_database, BaseVariable, thunk

db = configure_database("experiment.db", lineage_mode="ephemeral")

@thunk
def process(data):
    return data * 2

raw = RawData(some_array)
# No need to save raw - lineage still tracked!

result = process(raw)
ProcessedData(result).save(...)  # Works! Ephemeral lineage stored.

# Query lineage - includes ephemeral nodes
print(db.format_lineage(ProcessedData, ...))
# ProcessedData (vhash: abc123...)
# └── process [hash: def456...]
#     └── inputs:
#         └── RawData (vhash: ephemeral:xyz...) [ephemeral]
#             └── [source: unsaved raw data]
```

---

## Tests

Comprehensive tests are in `tests/test_lineage_mode.py`:

- **TestLineageModeConfiguration**: Configuration validation tests
- **TestFindUnsavedVariables**: In-memory traversal tests
- **TestStrictMode**: Strict mode validation tests
- **TestEphemeralMode**: Ephemeral mode storage/query tests
- **TestMixedScenarios**: Mixed saved/unsaved scenarios
- **TestEdgeCases**: Edge cases (multi-output, deep chains, etc.)
- **TestProvenanceQueries**: Provenance query compatibility
