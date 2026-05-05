# Layer Interaction Analysis: scifor ↔ scidb ↔ scihist

## Executive Summary

This document analyzes the three-layer for_each architecture (scifor → scidb → scihist) to identify friction points, redundancies, and architectural mismatches. Based on reading the three internals documents, we identified **10 friction points** ranging from minor redundancy to significant architectural inconsistencies.

**Key findings:**

- The scifor/scidb boundary is mostly clean, with one fragility around global schema state
- The scidb/scihist boundary has significant friction due to **dual variant tracking systems**
- Metadata is stored redundantly in up to 3 places
- Layer boundaries cause information loss and reconstruction (input classification quirk)

---

## What Works Well: Clean Separations

### 1. scifor as a pure iteration engine

**Design:** scifor has ZERO awareness of:

- Databases
- Version keys
- Variant tracking
- Lineage

It only knows: "filter DataFrames by schema keys, call function, collect results."

**Implementation:** scidb handles all version disambiguation _above_ scifor by:

- Pre-expanding combos to include `__rid_*` keys
- Temporarily extending scifor's schema to treat `__rid_*` as filter keys
- Passing pre-built combos via `_all_combos` (bypassing scifor's Cartesian product)

This is architecturally sound - scifor can be used standalone with plain DataFrames, while scidb adds persistence on top.

### 2. Lineage as an optional top layer

**Design:** scihist is a clean wrapper around scidb:

- Calls scidb with `save=False`
- Adds lineage tracking on top
- Provides skip_computed and staleness APIs

You can use scidb without scihist if you don't need lineage.

---

## Friction Points (Ordered by Severity)

### CRITICAL: Dual Variant Tracking Systems

**Where:** scidb ↔ scihist boundary

**The problem:** There are TWO incompatible systems for tracking computation variants:

#### System 1: scidb's branch_params (scidb-for-each-internals.md, lines 83-106)

```python
# scidb.for_each outputs have:
{
    "version_keys": {
        "__fn": "bandpass",
        "__fn_hash": "a1b2c3d4e5f6",
        "__inputs": '{"signal": "RawEMG"}',
        "__constants": '{"low_hz": 20}'
    },
    "branch_params": {
        "bandpass.low_hz": 20,
        "compute_rms.window": 100  # accumulated upstream
    }
}
```

#### System 2: scihist's \_lineage (scihist-for-each-internals.md, lines 698-717)

```python
# scihist.for_each outputs have:
{
    "version_keys": {
        "__fn": "bandpass",
        "__fn_hash": "a1b2c3d4e5f6"
        # NO __inputs, NO __constants
    },
    "branch_params": {}  # ALWAYS EMPTY
}
# Variants tracked in _lineage table instead
```

**Why this happens:** scihist calls scidb with `save=False` and does its own save, bypassing scidb's `branch_params` propagation logic (scidb Step 19).

**Consequences:**

1. **Split variant discovery:** `_get_output_combos()` must check BOTH `version_keys.__fn` AND `_lineage.function_name` to find all outputs
2. **Split expected combos:** `_get_expected_combos()` must consult BOTH `list_pipeline_variants()` AND `_get_lineage_variants()`
3. **Namespace mismatch:** scidb uses `fn.param` in branch_params, scihist expects un-namespaced constants (scihist doc line 495)
4. **Incomplete metadata:** scihist outputs lack `__inputs` and `__constants` in version_keys

**Reference:** scihist-for-each-internals.md lines 698-717

---

### HIGH: Input Classification Quirk

**Where:** scidb → scihist boundary

**The problem:** Variable inputs are misclassified as constants in lineage records.

**Flow:**

1. `scihist.for_each` passes `BaseVariable` types to `scidb.for_each`
2. `scidb.for_each` loads them into DataFrames with `.data` attribute
3. scidb extracts raw `numpy.array` from `BaseVariable` instances
4. scidb passes raw numpy to the function (so scifor can filter it)
5. `LineageFcn` receives raw numpy (no `record_id`, `data`, `to_db`, `from_db` attrs)
6. `scilineage.classify_input()` sees raw numpy → classifies as **CONSTANT**
7. Variable inputs end up in `_lineage.constants` instead of `_lineage.inputs`

**Workaround:** `_append_rid_tracking()` manually adds `rid_tracking` entries to `_lineage.inputs` during save:

```python
{"name": "__rid_signal", "source_type": "rid_tracking", "record_id": "abc123..."}
```

**Consequences:**

- Information is lost at layer boundary, then reconstructed
- `_lineage.constants` contains misclassified variable inputs
- `_lineage.inputs` only has `rid_tracking` entries (no actual input entries)
- `_get_lineage_variants()` must recover variable types by parsing `__rid_{param}` and looking up `record_id` in `_record_metadata`

**Reference:** scihist-for-each-internals.md lines 546-556, MEMORY.md "Scihist + Scilineage Input Classification Quirk"

**Better design:** Preserve `BaseVariable` wrapper through to scilineage, or pass metadata alongside raw data.

---

### HIGH: Triple Storage of Constants

**Where:** scidb layer (affects scihist)

**The problem:** The same constant values are stored in 3 different places in 3 different formats:

```python
constant_value = 20

# Location 1: version_keys.__constants (JSON string)
version_keys["__constants"] = '{"low_hz": 20}'

# Location 2: branch_params (namespaced)
branch_params["bandpass.low_hz"] = 20

# Location 3: Top-level metadata (for scihist)
metadata["low_hz"] = 20
```

**Reference:** scidb-for-each-internals.md lines 664-666

**Consequences:**

- Redundant storage
- Different layers expect constants in different places
- Scihist doesn't write #1 or #2, only #3 (scihist doc lines 702-706)
- Inconsistent access patterns

**Why it exists:**

- `version_keys.__constants`: For scidb variant disambiguation
- `branch_params`: For upstream variant propagation
- Top-level: So scihist can see them in metadata dict

**Better design:** Single canonical location, with views/accessors for different use cases.

---

### MEDIUM: Dual Function Hash Storage

**Where:** scidb ↔ scihist boundary

**The problem:** The same function hash is computed and stored twice:

```python
# Computed in scidb via ForEachConfig
version_keys["__fn_hash"] = _compute_fn_hash(fn)  # "a1b2c3d4e5f6"

# Computed in scihist via LineageRecord
lineage["function_hash"] = fn.hash  # "a1b2c3d4e5f6" (same value)
```

**Why this happens:** Two independent code paths for the same hash:

- scidb: `ForEachConfig.to_version_keys()` → `_compute_fn_hash()` (scidb doc lines 299-323)
- scihist: `LineageFcn.hash` → stored in `LineageRecord` (scihist doc lines 83-91)

**Consequences:**

- Redundant computation
- Two storage locations (`version_keys` and `_lineage` table)
- Scihist staleness check falls back from `_lineage.function_hash` to `version_keys.__fn_hash` (scihist doc line 441)

**Better design:** Single source of truth, computed once.

---

### MEDIUM: Fixed Inputs - Dual Treatment

**Where:** scidb ↔ scihist boundary

**The problem:** Fixed inputs are treated differently by the two layers:

**scidb (Step 10):** Fixed inputs have `__record_id` stripped, NOT part of variant expansion

```python
# Fixed(RawEMG, session="baseline")
# → loaded DataFrame has NO __record_id column
# → no variant expansion based on this input
```

**Reference:** scidb-for-each-internals.md lines 794-795

**scihist (Step 6):** Fixed inputs ARE tracked via `fixed_rids` for staleness

```python
# Fixed(RawEMG, session="baseline")
# → current record_id looked up via db.find_record_id()
# → stored in fixed_rids["__rid_signal"]
# → added to lineage inputs as rid_tracking entry
# → checked by skip_computed
```

**Reference:** scihist-for-each-internals.md lines 151-154

**Consequences:**

- Conceptual inconsistency: same input treated as "not variant-creating" (scidb) vs "must track for staleness" (scihist)
- Both are intentional for their use cases, but the mismatch is confusing

---

### MEDIUM: Global Schema State Fragility

**Where:** scidb → scifor boundary

**The problem:** scidb temporarily modifies scifor's module-level global schema:

```python
# Step 15: Extend schema
scifor.set_schema(["subject", "session", "__rid_signal"])

# Step 17: scifor.for_each() runs with extended schema

# Step 18: Restore schema
scifor.set_schema(["subject", "session"])  # MUST NOT FAIL
```

**Reference:** scidb-for-each-internals.md lines 549-626

**Risk:** If Step 18 fails (exception, early return, cancel), subsequent `for_each` calls in the same Python session will see the extended schema and fail.

From scidb doc line 625:

> The schema is a module-level global in scifor, so cleanup is essential.

**Better design:**

- Context manager for schema extension
- Thread-local schema instead of global
- Schema passed as parameter instead of global state

---

### MEDIUM: PerComboLoader Opacity

**Where:** scidb → scifor boundary

**The problem:** Some inputs are wrapped in `PerComboLoader` sentinels that scifor treats as opaque black boxes.

**Flow:**

1. scidb wraps certain inputs (PathInput, types lacking `load_all()`) in `PerComboLoader`
2. scifor sees these as plain constants (no filtering, just pass-through)
3. scidb wraps the function (Step 16) to unwrap them just before the call
4. The function receives the unwrapped data

**Reference:** scidb-for-each-internals.md lines 844-856

**Fragility:**

- If scifor ever tried to inspect these objects (logging, validation), it would see meaningless sentinel objects
- The unwrapping logic is split between scidb (create sentinel) and the wrapper function (resolve sentinel)
- No type safety - sentinels are just Python objects with a `.spec` attribute

**Better design:** Explicit protocol for per-combo resolution, or lazy-loading proxy objects.

---

### LOW: Empty-List Resolution Duplication

**Where:** scifor ↔ scidb boundary

**The problem:** Both layers implement empty-list resolution for `subject=[]`:

**scifor:** Scans in-memory DataFrames for distinct values

```python
# scifor-for-each-internals.md lines 64-78
# Calls _distinct_values_from_inputs() to scan loaded DataFrames
```

**scidb:** Queries database via SQL

```python
# scidb-for-each-internals.md lines 225-241
# Calls db.distinct_schema_values(key) → SELECT DISTINCT FROM _schema
```

**In practice:** No runtime duplication because scidb passes `_all_combos` to scifor, bypassing scifor's resolution.

**Reference:** scifor doc lines 67-68:

> This resolution only happens in standalone mode (when `_all_combos is None`). When called from `scidb.for_each()`, the database layer resolves empty lists via SQL queries instead.

**Why it exists:** scifor must support standalone use (no database), so it needs its own resolution.

**Assessment:** Minor redundancy in code, no runtime impact.

---

### LOW: Schema Propagation Dependency

**Where:** scidb → scifor boundary

**The problem:** scifor relies on external schema setup with no validation.

**Flow:**

1. scidb calls `scifor.set_schema(db.dataset_schema_keys)` (Step 4)
2. scifor's module-level `_schema` global is updated
3. scifor's DataFrame filtering and `distribute` validation rely on this

**Risk:** If you call `scifor.for_each()` directly without setting schema:

- DataFrame filtering might not work correctly
- `distribute` validation would fail or produce wrong results
- No error would be raised - just silent incorrect behavior

**Reference:** scidb-for-each-internals.md lines 274-280

**Better design:** Schema as explicit parameter, not global state.

---

### LOW: Scihist Save Path Incompleteness

**Where:** scihist save logic

**The problem:** scihist saves less metadata than scidb would have.

**What scidb writes:**

```python
version_keys = {
    "__fn": "bandpass",
    "__fn_hash": "a1b2c3d4e5f6",
    "__inputs": '{"signal": "RawEMG"}',
    "__constants": '{"low_hz": 20}'
}
branch_params = {"bandpass.low_hz": 20}
```

**What scihist writes:**

```python
version_keys = {
    "__fn": "bandpass",
    "__fn_hash": "a1b2c3d4e5f6"
    # NO __inputs
    # NO __constants
}
branch_params = {}  # ALWAYS EMPTY
```

**Reference:** scihist-for-each-internals.md lines 702-709

**Consequences:**

- Scihist outputs are less compatible with scidb's variant tracking
- Queries must check multiple sources to find all outputs
- Inconsistent metadata structure across output types

**Why it exists:** scihist doesn't use `ForEachConfig` for save - it uses `_lineage` instead.

---

## Architectural Questions

### 1. Should scihist populate branch_params?

**Current:** scihist writes `branch_params = {}` (empty)

**Alternative:** scihist could populate `branch_params` the same way scidb does (Step 19a-19c), accumulating upstream choices.

**Tradeoff:**

- ✅ Pro: Unified variant tracking, single query to find all variants
- ❌ Con: Duplication between `branch_params` and `_lineage` (both store constants)

### 2. Should we unify function hash storage?

**Current:** Hash computed twice, stored in `version_keys` and `_lineage`

**Alternative:** Single computation, single storage, with views/accessors

**Tradeoff:**

- ✅ Pro: No redundancy, single source of truth
- ❌ Con: Need to decide which layer owns the hash

### 3. Should we fix the input classification quirk?

**Current:** BaseVariable unwrapped to numpy, then misclassified as constant

**Alternative 1:** Pass BaseVariable wrapper through to scilineage

- ❌ Con: scilineage becomes aware of scidb's BaseVariable class (tight coupling)

**Alternative 2:** Pass metadata alongside raw data (e.g., `(data, {"is_variable": True, "record_id": "..."})`)

- ✅ Pro: Preserves information without tight coupling
- ❌ Con: Changes scifor's function call signature

**Alternative 3:** Accept the quirk, improve documentation

- ✅ Pro: No code changes
- ❌ Con: Confusing `_lineage.constants` structure persists

### 4. Should schema be a parameter instead of global state?

**Current:** scifor uses module-level `_schema` global

**Alternative:** Pass schema as parameter to `scifor.for_each()`

- ✅ Pro: No global state, thread-safe, no fragile cleanup
- ❌ Con: Breaking API change for standalone scifor users

---

## Recommendations by Priority

### Priority 1: Unify variant tracking (CRITICAL)

**Problem:** Dual systems (branch_params vs. \_lineage) require split queries

**Options:**

1. Make scihist populate `branch_params` (same as scidb does)
2. Make scidb use `_lineage` for variant tracking (requires scihist always)
3. Accept dual systems, document the tradeoff clearly

**Recommendation:** Document the architectural decision and tradeoffs. If a change is made, Option 1 (scihist populates branch_params) is least invasive.

### Priority 2: Fix input classification quirk (HIGH)

**Problem:** Variable inputs misclassified as constants in `_lineage`

**Options:**

1. Pass metadata alongside raw data (preserves info, loose coupling)
2. Accept quirk, improve documentation of `rid_tracking` workaround

**Recommendation:** Document the quirk prominently. If fixed, use metadata-passing approach.

### Priority 3: Eliminate triple storage of constants (HIGH)

**Problem:** Constants in 3 places (version_keys, branch_params, top-level)

**Options:**

1. Single canonical location, accessor methods for different views
2. Accept redundancy, document why each location exists

**Recommendation:** Document why each exists. Consider consolidation in future refactor.

### Priority 4: Schema as parameter (MEDIUM)

**Problem:** Global state causes fragility and thread-safety issues

**Options:**

1. Breaking change: schema as parameter to `for_each()`
2. Context manager for schema modifications
3. Thread-local schema storage

**Recommendation:** Add context manager for temporary schema extensions (Step 15/18) as a first step.

---

## Open Questions

1. **Do we want scihist and scidb outputs to have compatible variant tracking?** Or is it acceptable for them to use different systems?

2. **Is the input classification quirk a bug or a feature?** Should we preserve the scidb→scilineage boundary by unwrapping, or should we pass richer metadata?

3. **Should we invest in eliminating global state (schema)?** The current design works but has known fragility.

4. **What is the long-term vision for constants storage?** Will we always need them in 3 places, or can we consolidate?

---

## Summary Table

| Friction Point              | Severity | Layers          | Root Cause                                | Fix Difficulty      |
| --------------------------- | -------- | --------------- | ----------------------------------------- | ------------------- |
| Dual variant tracking       | CRITICAL | scidb ↔ scihist | scihist bypasses scidb save               | Medium              |
| Input classification quirk  | HIGH     | scidb → scihist | BaseVariable unwrapped too early          | Medium              |
| Triple constant storage     | HIGH     | scidb           | Multiple consumers need different formats | High                |
| Dual function hash storage  | MEDIUM   | scidb ↔ scihist | Independent code paths                    | Low                 |
| Fixed input dual treatment  | MEDIUM   | scidb ↔ scihist | Different layer concerns                  | Low (doc)           |
| Global schema state         | MEDIUM   | scidb → scifor  | Module-level global                       | Medium              |
| PerComboLoader opacity      | MEDIUM   | scidb → scifor  | Lazy loading pattern                      | Low (doc)           |
| Empty-list duplication      | LOW      | scifor ↔ scidb  | Standalone vs. wrapped use                | Low (no fix needed) |
| Schema propagation          | LOW      | scidb → scifor  | Global state dependency                   | Medium              |
| Scihist save incompleteness | LOW      | scihist         | Bypasses scidb save                       | Medium              |

---

## Conclusion

The three-layer architecture is fundamentally sound, with clean separation of concerns:

- **scifor**: Pure iteration
- **scidb**: Persistence + versioning
- **scihist**: Lineage + staleness

The main friction comes from **scihist bypassing scidb's save logic**, which creates:

1. Dual variant tracking systems
2. Inconsistent metadata structure
3. Split query logic

Most friction points are **intentional tradeoffs** rather than bugs, but several could benefit from:

- Better documentation of the architectural decisions
- Consolidation of redundant storage
- Elimination of global state dependencies
