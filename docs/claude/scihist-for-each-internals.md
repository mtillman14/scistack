# How `scihist.for_each()` Works — A Detailed Walkthrough

## Why this document exists

`scihist.for_each()` is the top layer in SciStack's three-layer batch execution system. It wraps `scidb.for_each()` (documented in `scidb-for-each-internals.md`) by adding automatic lineage tracking, provenance-aware skip logic, and lineage-aware saving. If you have read the scidb document, you know how data is loaded, iterated, and saved with version keys; this document explains everything that happens *around* that — the lineage recording, the `skip_computed` mechanism, and the staleness-checking system.

All source references are to `/workspace/scihist-lib/src/scihist/foreach.py` unless otherwise noted.

---

## What problem does it solve?

`scidb.for_each()` tracks what function produced each output via version keys (`__fn`, `__fn_hash`, `__inputs`, `__constants`), and it tracks upstream variant choices via `branch_params`. But it does not record the *full provenance graph* of a computation — which specific input records were consumed, what the function's internal state was, whether any upstream function code has changed since the output was computed.

`scihist.for_each()` adds this provenance layer by:

1. **Auto-wrapping functions in `LineageFcn`**, which records every input, every constant, and the function's source hash on each invocation, producing a `LineageFcnResult` that bundles the output data with a `LineageRecord`.

2. **Providing `skip_computed` logic** that checks, before each combo, whether a valid output already exists with matching function hash, matching input record IDs, and matching constant hashes. If everything matches, the combo is skipped — no function call, no re-save.

3. **Saving with lineage** by extracting the `LineageRecord` from each `LineageFcnResult` and writing it to the `_lineage` table alongside the data, so that staleness can be checked later.

4. **Providing a staleness API** (`check_combo_state`, `check_node_state`) that walks the full upstream provenance graph to detect whether any ancestor record has been superseded.

The layering is:

```
scihist.for_each()        -- lineage tracking + skip_computed + staleness API
  └─> scidb.for_each()    -- DB load + version keys + variant tracking + save
       └─> scifor.for_each()  -- pure iteration loop over in-memory DataFrames
```

Each layer adds one concern and delegates the rest downward.

---

## The function signature

```python
def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[type],
    dry_run: bool = False,
    save: bool = True,
    as_table: list[str] | bool | None = None,
    db=None,
    distribute: bool = False,
    where=None,
    skip_computed: bool = True,
    _progress_fn: Callable[[dict], None] | None = None,
    _cancel_check: Callable[[], bool] | None = None,
    **metadata_iterables: list[Any],
) -> pd.DataFrame | None
```

Source: lines 16–30.

Compared to `scidb.for_each()`, three things change:

| Difference | scidb.for_each() | scihist.for_each() |
|---|---|---|
| `fn` | Any callable | Auto-wrapped in `LineageFcn` if not already |
| `save` | Saves directly via `output_obj.save()` | Delegates to scidb with `save=False`, then saves with lineage |
| `skip_computed` | Not available | Default `True` — skips combos whose outputs exist with matching provenance |

The `inputs`, `outputs`, `dry_run`, `as_table`, `db`, `distribute`, `where`, `_progress_fn`, `_cancel_check`, and `**metadata_iterables` parameters are all passed through to `scidb.for_each()` unchanged.

---

## Step-by-step execution

### Step 1: Auto-wrap in LineageFcn (lines 69–73)

If `fn` is not already a `LineageFcn` (i.e., not decorated with `@lineage_fcn`), it is automatically wrapped:

```python
if not isinstance(fn, LineageFcn):
    fn = LineageFcn(fn)
```

`LineageFcn` (defined in the `scilineage` package) is a callable wrapper that, when invoked, does the following:

1. **Before the call**: Records all input values, classifying each as either a "variable" input (has `record_id`, `data`, `to_db`, `from_db` attributes) or a "constant" (everything else).
2. **Calls the wrapped function** with the raw input values (extracting `.data` from variable inputs if present).
3. **After the call**: Wraps the return value in a `LineageFcnResult` that bundles:
   - The raw output data
   - A `LineageFcnInvocation` (function reference + inputs + constants)
   - A `LineageRecord` (function name, function hash, classified inputs, constants)
   - A content hash of the output

The auto-wrap means users can pass plain functions to `scihist.for_each()` and still get full lineage tracking. The log message confirms: `auto-wrapped bandpass_filter in LineageFcn (hash=a1b2c3d4e5f6)`.

### Step 2: Create plain callable wrapper (lines 77-78)

scidb.for_each() expects a plain callable — it doesn't know about `LineageFcn`. The `make_tuple_unpacking_wrapper()` helper from scilineage creates a wrapper that:

1. Calls the `LineageFcn`, which returns a `LineageFcnResult`
2. If the function is configured to unpack tuples (default), extracts the raw data from the result
3. Otherwise returns the full `LineageFcnResult`

scidb.for_each() collects results into its output DataFrame — `LineageFcnResult` objects sit in the output column cells, carrying their lineage metadata alongside the data. Since scidb is called with `save=False` (Step 5), it never tries to interpret or save these objects itself.

### Step 3: Build skip_computed hook (lines 89-98)

When `skip_computed=True` (the default), `dry_run=False`, and there are output types specified, scihist builds a pre-combo hook via `_build_skip_hook()`. This hook is a callable `(combo: dict) -> bool` that returns `True` when a combo should be skipped.

The hook is built only when a database is available (either passed as `db=` or retrieved via `get_database()`). If no database is available, skip_computed is silently disabled.

The hook construction and logic are detailed in the "The skip_computed system" section below.

### Step 4: Detect generates_file mode (line 103)

If the `LineageFcn` has `generates_file=True` (set via the `@lineage_fcn(generates_file=True)` decorator), scihist sets `_inject_combo_metadata=True`. This tells scidb.for_each() to pass the current combo's schema keys (e.g., `subject=1, session="A"`) as extra keyword arguments to the function, so it can construct output file paths from the metadata.

### Step 5: Delegate to scidb.for_each (lines 112-127)

The core delegation:

```python
result_tbl = _scidb_for_each(
    fn_plain,
    inputs,
    outputs,
    dry_run=dry_run,
    save=save,                           # <-- Note: now delegates save to scidb
    as_table=as_table,
    db=active_db,
    distribute=distribute,
    where=where,
    _inject_combo_metadata=_inject_meta, # <-- for generates_file
    _pre_combo_hook=pre_combo_hook,      # <-- skip_computed hook
    _progress_fn=_progress_fn,
    _cancel_check=_cancel_check,
    **metadata_iterables,
)
```

**Important change:** As of the variant tracking unification (commit 6f51da1), scihist now passes `save=save` instead of `save=False`, delegating the save logic to scidb. Fixed input record IDs are computed internally by scidb (see scidb Step 10 and the `_lineage_fixed_rids` parameter), so scihist no longer needs to pre-compute or track them separately.

scidb.for_each() does everything — load inputs, build combos, expand variants, iterate via scifor, and save results with lineage. The result DataFrame comes back with `LineageFcnResult` objects in the output columns if saving was disabled.

If `dry_run=True`, scidb returns `None` and scihist returns `None` immediately (lines 129-131).

### Step 6: Return result DataFrame (lines 133-134)

The result DataFrame is returned to the caller after scidb.for_each completes. With the unified variant tracking architecture (commit 6f51da1), the save logic has been delegated to scidb, which calls back to scihist when it encounters `LineageFcnResult` objects (see the "Callback-based save architecture" section below).

---

## Callback-based save architecture

### Overview

As of the variant tracking unification (commit 6f51da1), scihist uses a **callback-based save model** instead of handling saves directly:

1. scihist.for_each() calls scidb.for_each() with `save=save` (delegating save responsibility)
2. scidb.for_each() processes all combos and prepares to save results
3. When scidb encounters a `LineageFcnResult` in the output data, it calls back to `scihist.save_lineage_result()`
4. scihist.save_lineage_result() extracts lineage metadata and delegates to scidb.save() with the lineage dict

This architecture unifies variant tracking while preserving lineage capabilities. scidb handles all the version_keys, branch_params, and rid tracking logic, while scihist adds lineage-specific metadata when called back.

### How scidb detects LineageFcnResult

In scidb's save logic (Step 19 of scidb-for-each-internals.md), before saving each output, scidb checks:

```python
if HAS_LINEAGE and isinstance(output_value, LineageFcnResult):
    from scihist.foreach import save_lineage_result
    record_id = save_lineage_result(output_obj, output_value, save_metadata, db)
```

This imports scihist.save_lineage_result() on-demand and calls it with pre-built metadata from scidb's save logic.

### Fixed input tracking

Fixed input record IDs are now computed by scidb during loading (Step 10) and stored in the `_lineage_fixed_rids` dict. scidb passes this to scihist via the callback metadata, so scihist doesn't need to pre-compute Fixed record IDs.

---

## The skip_computed system

### Overview

`skip_computed` is the mechanism that prevents re-running functions when nothing has changed. It is implemented as a pre-combo hook — a function called by scidb.for_each() before each combo is processed. If the hook returns `True`, the combo is skipped (no function call, no save).

### How the hook is built: `_build_skip_hook()` (lines 175–378)

The hook is constructed with the current function (`LineageFcn`), output types, database, and input specifications. During construction, it pre-computes:

**Constant hashes** (lines 153-178): For each constant input (non-variable, non-wrapper, non-PathInput), the canonical hash is computed via `canonical_hash()`. These hashes are compared against stored lineage constants at check time.

**Function hash** (line 149): Uses `compute_function_hash()` from `scilineage.hashing`, which performs bytecode-based hashing (same method used by scidb). This ensures consistency between scidb and scihist function hash computations.

**Fixed input specs** (lines 156-174): For each `Fixed` wrapper, the inner variable type and fixed metadata are extracted and stored as `fixed_inputs[name] = (inner_type, fixed_metadata)`. At check time, the current record ID for each fixed input is looked up via `db.find_record_id()` and compared against stored lineage `rid_tracking` entries.

### The 4-step skip check: `_should_skip()` (lines 223–376)

For each combo, the hook executes four checks. A combo is skipped only if ALL four pass.

#### Step 1: Output record exists (lines 234–253)

For each output type, the hook calls `db.find_record_id(OutputCls, lookup_combo)` where `lookup_combo` includes:
- Schema keys from the current combo
- Constant values (for variant disambiguation)
- `__fn` = function name
- `__fn_hash` = current function hash

If any output type has no record for this combo → **do not skip** (output missing, must compute).

#### Step 2: Function hash matches (lines 256–272)

The hook calls `db.get_function_hash_for_record(output_record_id)` to retrieve the function hash stored in the `_lineage` table when the output was last saved.

- If no lineage record exists → `[recompute] — no lineage record`
- If stored hash differs from current `fn.hash` → `[recompute] — function hash changed`

This catches the case where you edit the function source code but haven't re-run it yet.

#### Step 3: Input record IDs match (lines 274–343)

This step has two sub-parts:

**Step 3a: Combo __rid_* values (lines 274–303)**

The current combo may contain `__rid_signal`, `__rid_reference`, etc. — record IDs of the freshly-loaded input records (added by scidb's variant expansion in Step 12 of the scidb pipeline). These are compared against `rid_tracking` entries in the stored lineage inputs.

```python
lineage_inputs = db.get_lineage_inputs(output_record_id)
stored_rids = {inp["name"]: inp["record_id"]
               for inp in lineage_inputs
               if inp.get("source_type") == "rid_tracking"}
```

For each `__rid_*` in the combo:
- If the rid value equals the output record ID itself → skip this check (self-referential case)
- If no stored rid exists → `[recompute] — no stored __rid_signal`
- If stored rid differs from current rid → `[recompute] — __rid_signal changed`

**Step 3b: Fixed input record IDs (lines 305–343)**

For each Fixed input, the current record ID is looked up via `db.find_record_id(inner_type, fixed_meta)` and compared against stored `rid_tracking` entries (using the same stored_rids dict from Step 3a).

- If current record ID is `None` → `[recompute] — fixed input {name} not found`
- If no stored rid → `[recompute] — no stored __rid_{name}`
- If stored rid differs → `[recompute] — __rid_{name} changed`

This catches the case where a Fixed input (e.g., baseline calibration data) has been re-saved since the output was last computed.

#### Step 4: Constant hashes match (lines 345–369)

For each constant input, the pre-computed canonical hash is compared against the stored lineage constants:

```python
stored_constants = db.get_lineage_constants(output_record_id)
stored_const_hashes = {c["name"]: c["value_hash"] for c in stored_constants
                       if "name" in c and "value_hash" in c}
```

- If stored hash differs → `[recompute] — constant {name} changed`
- If constant is new (not in stored hashes, but other constants exist) → `[recompute] — new constant {name}`

#### All checks pass → Skip

```python
print(f"[skip] {combo_str}")
return True
```

### Diagnostic output

Every skip/recompute decision is printed to stdout and logged. The format is:

```
[skip] subject=1, session=A
[recompute] subject=2, session=A — function hash changed
[recompute] subject=2, session=B — __rid_signal changed
```

This makes it easy to see at a glance why combos were or were not recomputed.

---

## The lineage-aware save path

### Overview

With the callback-based architecture, scidb.for_each() detects `LineageFcnResult` objects during its save step (Step 19) and calls back to scihist.save_lineage_result(). scidb provides pre-built metadata including version_keys, branch_params, and `__rid_*` tracking.

### `save_lineage_result()` (lines 534-632)

This is the callback function that scidb calls when it encounters a LineageFcnResult. It receives:
- `output_obj`: The output variable class
- `lineage_result`: The LineageFcnResult containing data and lineage info
- `metadata`: Pre-built metadata from scidb (includes `__fn`, `__fn_hash`, `__inputs`, `__constants`, `__branch_params`, `__upstream`, and `__rid_*` keys)
- `db`: Database instance (optional)

The function has two distinct paths:

#### Normal path (lines 574-614)

For functions that produce data (the common case):

1. **Extract lineage**: Call `extract_lineage(lineage_result)` to get a `LineageRecord` from the `LineageFcnResult`.

2. **Convert to dict**: `_lineage_to_dict()` (lines 635-642) converts the `LineageRecord` to a flat dict:
   ```python
   {
       "function_name": "bandpass_filter",
       "function_hash": "a1b2c3d4e5f6g7h8",
       "inputs": [...],     # list of input entries
       "constants": [...],  # list of constant entries
   }
   ```

3. **Append rid_tracking**: `_append_rid_tracking()` (lines 623-632) adds `__rid_*` entries from the metadata to the lineage inputs list:
   ```python
   {"name": "__rid_signal", "source_type": "rid_tracking", "record_id": "abc123..."}
   ```
   These entries (already computed by scidb) allow skip_computed to verify that the same input records were used.

4. **Compute hashes**:
   - `lineage_hash` = `lineage_result.hash` (content hash of the lineage result)
   - `pipeline_lineage_hash` = `lineage_result.invoked.compute_lineage_hash()` (hash of the full invocation — function + inputs + constants)

5. **Wrap data in variable class**: Extract the raw data via `get_raw_value(lineage_result)`, create an instance of the output variable class (`output_obj(raw_data)`).

6. **Save via db.save()**: Call `db.save(instance, metadata, lineage=lineage_dict, lineage_hash=lineage_hash, pipeline_lineage_hash=pipeline_lineage_hash)`. The metadata already contains `__fn`, `__fn_hash`, `__inputs`, `__constants`, `__branch_params`, and `__upstream` from scidb. This writes:
   - A row to `_record_metadata` with the computed record_id, version_keys, branch_params
   - The data to the variable's data table
   - A row to `_lineage` with the function name, function hash, inputs (including rid_tracking), constants, lineage hash

#### generates_file path (lines 561-573)

For functions decorated with `@lineage_fcn(generates_file=True)` — functions that produce files on disk rather than data to store in DuckDB:

1. **Extract lineage and compute hashes** (same as normal path).

2. **Generate a synthetic record ID**: `generated_id = f"generated:{pipeline_lineage_hash[:32]}"`. There is no actual data content, so the record ID is derived from the lineage hash instead of from a content hash.

3. **Update metadata** with the synthetic record_id and set `content_hash=None`.

4. **Write _record_metadata directly**: Call `db._save_record_metadata()` with the pre-built metadata from scidb (which already includes `__fn`, `__fn_hash`, `__inputs`, `__constants`, `__branch_params`).

5. **Write _lineage directly**: Call `db._save_lineage()` with the lineage dict (including rid_tracking entries).

6. **No data table write**: The function's output is a file on disk — nothing is written to a DuckDB data table. Only metadata and lineage are persisted.

This enables skip_computed to work for file-generating functions: the lineage record tracks what inputs and function version produced the file, so a re-run can be skipped if nothing has changed.

### `save()` — public API (lines 653-679)

The module-level `save()` function is exported as `scihist.save` for standalone use outside of `for_each`:

```python
from scihist import save

result = my_lineage_fn(input_data)
save(OutputType, result, subject=1, session="A")
```

It detects `LineageFcnResult` and calls `save_lineage_result()` with constructed metadata, or delegates to `variable_class.save()` for raw data.

---

## The staleness-checking system

Source: `/workspace/scihist-lib/src/scihist/state.py`

### `check_combo_state()` (lines 40–106)

Checks whether a single (function, schema_combo) pair is up-to-date, stale, or missing.

```python
state = check_combo_state(
    fn=bandpass_filter,
    outputs=[FilteredEMG],
    schema_combo={"subject": 1, "session": "A"},
)
# Returns: "up_to_date" | "stale" | "missing"
```

**Algorithm:**

1. **Find output record**: For each output type, call `db.find_record_id(OutputCls, schema_combo, branch_params_filter=branch_params)`. If any output is missing → return `"missing"`.

2. **Priority 1 — Lineage-based check** (for scihist.for_each outputs): If `db.get_function_hash_for_record(output_record_id)` returns a value (meaning the output has a `_lineage` row), delegate to `_check_via_lineage()`.

3. **Priority 2 — Version-keys fallback** (for scidb.for_each outputs): If no lineage row exists, read `__fn_hash` from the output record's `version_keys` and delegate to `_check_via_fn_hash()`.

### `_check_via_lineage()` (lines 108–162)

The lineage-based check has two parts:

**a. Function hash comparison** (lines 138–155): Compare the current `fn.hash` against the stored `function_hash` in `_lineage`. If they differ and `fn` is a Python `LineageFcn` (not a MATLAB proxy), the output is stale. MATLAB proxies are excluded because MATLAB's function hashing can produce false mismatches between save-time and check-time.

**b. Deep ancestor walk** (lines 157–159): Call `_has_superseded_ancestor()` to BFS the upstream lineage graph and check whether any ancestor record has been superseded.

### `_has_superseded_ancestor()` (lines 165–225)

This is a BFS traversal of the `_lineage` graph, starting from the output record and walking backwards through its inputs.

For each record in the BFS queue:

1. Fetch `lineage_inputs` from `db.get_lineage_inputs(current_rid)`.
2. For each input with `source_type` of `"variable"` or `"rid_tracking"`:
   - Get the `record_id` that was used when the output was computed.
   - Call `db.get_latest_record_id_for_variant(used_rid)` to check if a newer record now exists for the same variant.
   - If the latest record differs from the used record → **stale** (an upstream input has been re-saved).
   - Also call `_get_latest_record_at_location()` to check for newer records at the same (variable_name, schema_id) regardless of version_keys — this catches direct `.save()` updates that don't carry `__fn` in version_keys.

The BFS is bounded by `max_depth=50` and a `visited` set to prevent cycles.

**What this catches:**
- Ancestor data re-saved (record_id superseded) → stale
- Python function's own code changed → stale

**What this does NOT catch:**
- Ancestor function code changed but not yet re-run → NOT detected. The check only receives the current function, not all ancestor functions. The GUI's DAG walk handles this case.

### `_check_via_fn_hash()` (lines 228–329)

Fallback for scidb.for_each outputs (no `_lineage` row). Three sub-checks:

1. **Function hash via version_keys** (lines 253–268): Compare `__fn_hash` from the output's `version_keys` against `_compute_fn_hash(fn)`.

2. **Input freshness via __upstream** (lines 276–290): If the output's `version_keys` contain `__upstream` (a JSON dict of `rid_column → record_id`), check each upstream record ID against `db.get_latest_record_id_for_variant()`. This is variant-precise.

3. **Timestamp fallback** (lines 296–328): If no `__upstream` is available, compare the output's timestamp against the latest timestamps of its input types at the same schema_id. This is the least precise method — variant-unaware and may produce false positives.

### `check_node_state()` (lines 332–454)

Aggregates staleness across all combos for a pipeline function:

```python
result = check_node_state(
    fn=bandpass_filter,
    outputs=[FilteredEMG],
)
# Returns:
# {
#     "state": "green" | "grey" | "red",
#     "combos": [...],
#     "counts": {"up_to_date": N, "stale": N, "missing": N},
# }
```

**Algorithm:**

1. **Get actual combos**: `_get_output_combos()` (lines 468–522) queries `_record_metadata` LEFT JOIN `_lineage` to find all (schema_id, branch_params) pairs for records produced by this function. Two match sources:
   - `version_keys.__fn` matches (scidb.for_each outputs)
   - `_lineage.function_name` matches (scihist.for_each outputs, which don't write `__fn` to version_keys)

2. **Get expected combos**: `_get_expected_combos()` (lines 525–680) determines which combos *should* exist by consulting:
   - `list_pipeline_variants()` for scidb variants (constants are namespaced as `fn.param` in expected branch_params)
   - `_get_lineage_variants()` for scihist variants (constants are NOT namespaced — scihist writes `branch_params={}`)
   - `_for_each_expected` table as final fallback (for PathInput-only functions)

3. **Compute missing**: Expected combos minus actual combos = missing combos.

4. **Check each actual combo**: Call `check_combo_state()` for each actual combo.

5. **Aggregate**:
   - Any stale → `"red"`
   - All missing, none up_to_date → `"red"`
   - Some missing, some up_to_date → `"grey"`
   - All up_to_date → `"green"`
   - No combos at all → `"red"`

### `_get_lineage_variants()` (lines 683–767)

Extracts variable input types from `_lineage` rows for a function. Two sources of variable inputs:

1. **Entries with `source_type="variable"`** (rare for scihist — scilineage classifies raw numpy arrays as CONSTANTs, not variables).

2. **Entries with `source_type="rid_tracking"`** (added by `_append_rid_tracking` during save). The entry name is `__rid_{param}`; the variable type is recovered by looking up the `record_id` in `_record_metadata.variable_name`.

Constants from `_lineage.constants` are intentionally NOT used for variant discrimination. scilineage classifies per-combo values (e.g., PathInput-resolved filepaths) as constants, which would create one spurious variant per combo.

---

## Database configuration

Source: `/workspace/scihist-lib/src/scihist/database.py`

### `configure_database()` (lines 6–31)

Wraps `scidb.configure_database()` with one addition: registers the database as the lineage cache backend.

```python
def configure_database(db_path, schema_keys=None, **kwargs):
    db = scidb.configure_database(db_path, schema_keys, **kwargs)
    scilineage.configure_backend(db)  # Register for cache lookups
    return db
```

The `configure_backend()` call tells scilineage where to look up previously computed results (for `LineageFcn`'s built-in caching, independent of skip_computed).

### `find_by_lineage()` (lines 34–51)

Queries the database for previously computed outputs matching a `LineageFcnInvocation`:

```python
def find_by_lineage(invocation):
    db = get_database()
    lineage_hash = invocation.compute_lineage_hash()
    return db.find_by_lineage_hash(lineage_hash)
```

---

## Public API

Source: `/workspace/scihist-lib/src/scihist/__init__.py`

scihist re-exports from all three lower layers for convenience:

| Symbol | Source | Purpose |
|---|---|---|
| `for_each`, `save` | `scihist.foreach` | Lineage-tracked batch execution and save |
| `configure_database`, `find_by_lineage` | `scihist.database` | DB setup with lineage backend |
| `check_combo_state`, `check_node_state` | `scihist.state` | Staleness checking |
| `Fixed`, `Merge`, `ColumnSelection`, `ForEachConfig` | `scidb` | Input wrapper types |
| `Col`, `set_schema`, `get_schema`, `PathInput` | `scifor` | Schema helpers and path inputs |
| `lineage_fcn`, `LineageFcn`, `LineageFcnResult`, `LineageFcnInvocation` | `scilineage` | Lineage system |

---

## The input classification quirk

When `scihist.for_each` calls a `LineageFcn`, scidb extracts raw data (e.g., `numpy.array`) from `BaseVariable` instances and passes that to the function. Inside `LineageFcn`, scilineage's `classify_input` sees a raw numpy array — it has no `record_id`, `data`, `to_db`, or `from_db` attributes — so it classifies it as a **CONSTANT**, not a variable input.

This means `_lineage.constants` for scihist outputs contains the actual variable input values (misclassified), while `_lineage.inputs` contains only `rid_tracking` entries (added by `_append_rid_tracking` during save).

To recover the true variable type from a `rid_tracking` entry:
1. Parse the parameter name from `__rid_{param}` (e.g., `__rid_signal` → `signal`)
2. Look up `record_id` → `variable_name` in `_record_metadata`
3. The `variable_name` is the true variable type name (e.g., `"RawEMG"`)

This quirk is important for `_get_lineage_variants()` in the staleness system, which must recover variable input types from `rid_tracking` entries rather than from the (misclassified) constants.

---

## Logging

scihist uses Python's standard `logging` module (`logger = logging.getLogger(__name__)`) plus the optional `scidb.log.Log` class for dual-destination logging (file + structured).

Key log points:

| Event | Level | Example |
|---|---|---|
| Auto-wrap in LineageFcn | INFO | `auto-wrapped bandpass_filter in LineageFcn (hash=a1b2c3d4e5f6)` |
| Skip hook built | DEBUG | `built skip_computed hook for bandpass_filter` |
| Delegation to scidb | DEBUG | `delegating to scidb.for_each (save=False, distribute=False)` |
| Result row count | INFO | `scidb.for_each returned 24 rows` |
| Input classification | DEBUG | `input classification: 2 constants, 1 fixed_rids` |
| Skip/recompute | INFO (via print) | `[skip] subject=1, session=A` / `[recompute] subject=2, session=A — function hash changed` |
| Save with lineage | DEBUG/INFO | `[save] subject=1, session=A: FilteredEMG (lineage) -> record_id=abc123... in 0.042s` |
| Save error | ERROR | `[error] subject=1, session=A: save failed for FilteredEMG: ...` |

---

## Error handling

scihist inherits scidb's (and scifor's) skip-and-continue philosophy:

- **Save failures** (lines 457–464): If saving a single result row fails, the error is logged but does not prevent other rows from being saved.
- **Skip hook failures**: Any exception during `_should_skip()` is not explicitly caught — if the hook fails, the combo is NOT skipped (scidb's `_pre_combo_hook` integration treats hook exceptions as "do not skip").
- **Lineage save failures** (lines 566–570): `_save_lineage_fcn_result()` logs the exception and re-raises, which is caught by the per-row error handler in `_save_with_lineage()`.

---

## A concrete end-to-end example

```python
from scihist import configure_database, for_each, Fixed, lineage_fcn
from scidb import BaseVariable

db = configure_database("experiment.duckdb", ["subject", "session"])

class RawEMG(BaseVariable):
    schema_version = 1

class Calibration(BaseVariable):
    schema_version = 1

class FilteredEMG(BaseVariable):
    schema_version = 1

@lineage_fcn
def bandpass_filter(signal, calibration, low_hz, high_hz):
    return signal * calibration  # simplified

for_each(
    bandpass_filter,
    inputs={
        "signal": RawEMG,
        "calibration": Fixed(Calibration, session="baseline"),
        "low_hz": 20,
        "high_hz": 450,
    },
    outputs=[FilteredEMG],
    subject=[1, 2],
    session=["A", "B"],
)
```

What happens:

1. **Auto-wrap**: `bandpass_filter` is already a `LineageFcn` (decorated with `@lineage_fcn`) — no wrapping needed. Log: `bandpass_filter is already a LineageFcn (hash=a1b2c3d4e5f6)`.

2. **Create plain wrapper**: `_make_plain(bandpass_filter)` creates a thin wrapper that returns `LineageFcnResult`.

3. **Build skip_computed hook**: Since `skip_computed=True` and `db` is available, `_build_skip_hook()` is called. It pre-computes:
   - `constant_hashes = {"low_hz": hash(20), "high_hz": hash(450)}`
   - `fixed_inputs = {"calibration": (Calibration, {"session": "baseline"})}`

4. **Detect generates_file**: `bandpass_filter.generates_file` is `False` → `_inject_meta = False`.

5. **Delegate to scidb.for_each**: Called with `save=False` and the skip hook as `_pre_combo_hook`.

   **Inside scidb.for_each** (steps from scidb-for-each-internals.md):
   - Loads `RawEMG` via `load_all()` → DataFrame with `__record_id`, `__branch_params`
   - Loads `Calibration` via `Fixed` → DataFrame with fixed session="baseline" (record_id stripped)
   - `low_hz=20`, `high_hz=450` pass through as constants
   - Builds combos: `[{subject:"1", session:"A"}, ..., {subject:"2", session:"B"}]`
   - Variant expansion adds `__rid_signal` to each combo
   - **For each combo, the skip hook runs** (Step 14):
     - First run: no output records exist → `[recompute] — no output record` → hook returns `False` → combo proceeds
     - Second run (same data): outputs exist, fn hash matches, rid matches, constant hashes match → `[skip]` → hook returns `True` → combo skipped
   - scifor iterates: filters DataFrames, calls `fn_plain(signal=..., calibration=..., low_hz=20, high_hz=450)`, which calls the `LineageFcn`, which:
     - Classifies `signal` (raw numpy array) as CONSTANT (not a BaseVariable — the quirk)
     - Classifies `calibration` (raw numpy array) as CONSTANT
     - Classifies `low_hz`, `high_hz` as CONSTANTs
     - Calls the wrapped `bandpass_filter` function
     - Returns `LineageFcnResult` with data + lineage
   - scifor collects `LineageFcnResult` objects into the result DataFrame

6. **Delegate to scidb.for_each with save=True**: scidb processes the for_each call:
   - Loads `RawEMG` and `Calibration` (Fixed) into DataFrames
   - Builds combos with `__rid_signal` variant expansion
   - For each combo, the skip hook checks if output exists with matching provenance
     - First run: no outputs exist → `[recompute] — no output record` → combo proceeds
     - Second run (same data): outputs exist, fn hash matches, rid matches, constant hashes match → `[skip]` → combo skipped
   - scifor iterates and calls the `LineageFcn`, collecting `LineageFcnResult` objects
   - scidb prepares to save results (Step 19), detects `LineageFcnResult`, calls back to scihist

7. **Callback to scihist.save_lineage_result()**: For each result row with a `LineageFcnResult`:
   - scidb provides pre-built metadata: `{"subject": "1", "session": "A", "__fn": "bandpass_filter", "__fn_hash": "a1b2...", "__inputs": '{"signal": "RawEMG"}', "__constants": '{"low_hz": 20, "high_hz": 450}', "__branch_params": '{"bandpass_filter.low_hz": 20, "bandpass_filter.high_hz": 450}', "__upstream": '{"__rid_signal": "sig_rid_456", "__rid_calibration": "cal_rid_123"}', "__rid_signal": "sig_rid_456", "__rid_calibration": "cal_rid_123"}`
   - scihist.save_lineage_result():
     - `extract_lineage(lineage_result)` → `LineageRecord(function_name="bandpass_filter", function_hash="a1b2...", inputs=[...], constants=[...])`
     - `_lineage_to_dict()` → flat dict
     - `_append_rid_tracking(metadata)` → adds `__rid_signal` and `__rid_calibration` entries to lineage inputs
     - `lineage_result.hash` → `lineage_hash`
     - `lineage_result.invoked.compute_lineage_hash()` → `pipeline_lineage_hash`
     - `get_raw_value(lineage_result)` → the raw numpy array result
     - `FilteredEMG(raw_data)` → variable instance
     - `db.save(instance, metadata, lineage=lineage_dict, lineage_hash=..., pipeline_lineage_hash=...)` →
       - Writes to `_record_metadata` (record_id, version_keys, branch_params, lineage_hash)
       - Writes data to `FilteredEMG_data`
       - Writes to `_lineage` (function_name, function_hash, inputs with rid_tracking, constants, lineage_hash)
     - Returns record_id to scidb
     - Log: `[save-lineage] FilteredEMG: record_id=abc123def456 function_hash=a1b2c3d4e5f6`

8. **Return**: The result DataFrame is returned to the caller.

Now if the same `for_each` is called again with identical inputs and function code:
- The skip hook finds matching output records for all combos
- Function hashes match, input record IDs match, constant hashes match
- All combos are skipped: `[skip] subject=1, session=A`, `[skip] subject=1, session=B`, ...
- No function calls, no saves — execution completes almost instantly

If you then edit `bandpass_filter`'s source code and re-run:
- The skip hook detects the function hash mismatch
- All combos are recomputed: `[recompute] subject=1, session=A — function hash changed`, ...
- New outputs are saved with the updated lineage

If you re-save `RawEMG` for subject=1, session=A with new data (different content → different record_id) and re-run:
- The skip hook detects the `__rid_signal` mismatch for subject=1, session=A
- Only that combo is recomputed; others are still skipped
- The new output's lineage records the new input record_id

---

## How scihist differs from scidb in what it writes to the database

**As of the variant tracking unification (commit 6f51da1), scihist now delegates saving to scidb, so the metadata structure is largely unified.** The main differences are:

| Aspect | scidb.for_each | scihist.for_each |
|---|---|---|
| `version_keys.__fn` | Written (from `ForEachConfig`) | Written (from `ForEachConfig` via scidb) |
| `version_keys.__fn_hash` | Written (from `ForEachConfig`) | Written (from `ForEachConfig` via scidb) |
| `version_keys.__inputs` | Written | Written (via scidb) |
| `version_keys.__constants` | Written | Written (via scidb) |
| `branch_params` | Written (accumulated upstream + namespaced constants) | Written (via scidb - same logic) |
| `_lineage` row | NOT written | Written (function_name, function_hash, inputs, constants) |
| `_lineage.inputs` entries | N/A | `rid_tracking` entries for each `__rid_*` input |
| Fixed input tracking | Strips `__record_id` from Fixed inputs | Tracks Fixed `__record_id` in lineage for skip_computed |

**Key insight:** scihist outputs now have the full scidb metadata structure (`version_keys`, `branch_params`) PLUS lineage tracking in the `_lineage` table. This unifies variant tracking while adding provenance capabilities.

The only distinguishing feature is the presence of `_lineage` rows for scihist.for_each outputs, which enable:
- skip_computed to check function hash and input record IDs
- Staleness checking via full upstream provenance graph
- Constant hash verification
