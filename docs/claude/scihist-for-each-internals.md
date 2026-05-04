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

### Step 2: Create plain callable wrapper (line 76)

scidb.for_each() expects a plain callable — it doesn't know about `LineageFcn`. The `_make_plain()` helper (lines 381–386) creates a thin wrapper:

```python
def _make_plain(lineage_fn) -> Callable:
    def wrapped(*args, **kwargs):
        return lineage_fn(*args, **kwargs)
    wrapped.__name__ = getattr(lineage_fn, "__name__", "lineage_fcn")
    return wrapped
```

This wrapper calls the `LineageFcn`, which returns a `LineageFcnResult`. scidb.for_each() collects that result into its output DataFrame — the `LineageFcnResult` objects sit in the output column cells, carrying their lineage metadata alongside the data. Since scidb is called with `save=False` (Step 5), it never tries to interpret or save these objects itself.

### Step 3: Build skip_computed hook (lines 78–98)

When `skip_computed=True` (the default), `dry_run=False`, and there are output types specified, scihist builds a pre-combo hook via `_build_skip_hook()`. This hook is a callable `(combo: dict) -> bool` that returns `True` when a combo should be skipped.

The hook is built only when a database is available (either passed as `db=` or retrieved via `get_database()`). If no database is available, skip_computed is silently disabled.

The hook construction and logic are detailed in the "The skip_computed system" section below.

### Step 4: Detect generates_file mode (line 103)

If the `LineageFcn` has `generates_file=True` (set via the `@lineage_fcn(generates_file=True)` decorator), scihist sets `_inject_combo_metadata=True`. This tells scidb.for_each() to pass the current combo's schema keys (e.g., `subject=1, session="A"`) as extra keyword arguments to the function, so it can construct output file paths from the metadata.

### Step 5: Delegate to scidb.for_each (lines 104–120)

The core delegation:

```python
result_tbl = _scidb_for_each(
    fn_plain,
    inputs,
    outputs,
    dry_run=dry_run,
    save=False,                          # <-- scihist handles saves
    as_table=as_table,
    db=db,
    distribute=distribute,
    where=where,
    _inject_combo_metadata=_inject_meta, # <-- for generates_file
    _pre_combo_hook=pre_combo_hook,      # <-- skip_computed hook
    _progress_fn=_progress_fn,
    _cancel_check=_cancel_check,
    **metadata_iterables,
)
```

Key: `save=False`. scidb.for_each() does *everything* it normally does — load inputs, build combos, expand variants, iterate via scifor — but it does NOT save results. The result DataFrame comes back with `LineageFcnResult` objects in the output columns, carrying unsaved lineage metadata.

If `dry_run=True`, scidb returns `None` and scihist returns `None` immediately (lines 122–124).

### Step 6: Classify inputs for save (lines 128–162)

Before saving, scihist classifies the original `inputs` dict to determine:

1. **Constant inputs** (`constant_inputs`): Values that are not variable types, not wrappers (Fixed, Merge, ColumnSelection), and not PathInput. These are Python scalars, strings, numpy arrays, etc. that were passed unchanged to every function call. They are added to version_keys during save for variant disambiguation.

2. **Fixed input record IDs** (`fixed_rids`): For each `Fixed` wrapper, scihist resolves the current record ID of the fixed input by calling `db.find_record_id(inner_type, fixed_metadata)`. These are stored as `__rid_{param_name}` entries in the lineage inputs for skip_computed tracking.

The classification logic (lines 134–162) walks through each input and applies these rules:

| Input type | Classification | Action |
|---|---|---|
| Variable type (class) | Skip | Not tracked here (tracked by scilineage as variable inputs) |
| Fixed wrapper | Track rid | Resolve `db.find_record_id()` → store in `fixed_rids` |
| Merge/ColumnSelection wrapper | Skip | Not tracked as constant or fixed |
| PathInput | Skip | Resolved per-combo by scidb.for_each |
| Everything else | Constant | Stored in `constant_inputs` |

### Step 7: Save with lineage (lines 166–170)

If `save=True`, outputs are specified, and the result table is non-empty, scihist calls `_save_with_lineage()`:

```python
_save_with_lineage(result_tbl, outputs, output_names, db,
                   constant_inputs=constant_inputs,
                   fixed_input_rids=fixed_rids)
```

The save logic is detailed in the "The lineage-aware save path" section below.

### Step 8: Return result DataFrame (line 172)

The result DataFrame is returned to the caller. Its output columns contain `LineageFcnResult` objects (not raw data), unless the function was not wrapped in LineageFcn. The caller can inspect these for lineage metadata if needed, or just use the DataFrame for further processing.

---

## The skip_computed system

### Overview

`skip_computed` is the mechanism that prevents re-running functions when nothing has changed. It is implemented as a pre-combo hook — a function called by scidb.for_each() before each combo is processed. If the hook returns `True`, the combo is skipped (no function call, no save).

### How the hook is built: `_build_skip_hook()` (lines 175–378)

The hook is constructed with the current function (`LineageFcn`), output types, database, and input specifications. During construction, it pre-computes:

**Constant hashes** (lines 190–215): For each constant input (non-variable, non-wrapper, non-PathInput), the canonical hash is computed via `canonical_hash()`. These hashes are compared against stored lineage constants at check time.

**Fixed input specs** (lines 194–210): For each `Fixed` wrapper, the inner variable type and fixed metadata are extracted and stored as `fixed_inputs[name] = (inner_type, fixed_metadata)`. At check time, the current record ID for each fixed input is looked up and compared against stored lineage.

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

After scidb.for_each() returns a result table with `LineageFcnResult` objects in the output columns, scihist saves each result with full lineage tracking. There are two save paths depending on whether the function has `generates_file=True`.

### `_save_with_lineage()` (lines 389–465)

This function iterates over each row of the result table and each output:

1. **Extract metadata**: Separates `__rid_*` keys (for lineage rid_tracking) from save metadata (schema keys + user metadata). Strips all `__`-prefixed keys from save metadata. Merges `fixed_input_rids` into the rid tracking dict.

2. **Add constant inputs**: Constant input values are added to save metadata for variant disambiguation (e.g., `low_hz=20` becomes a version key).

3. **Route by output type**:
   - If the output value is a `LineageFcnResult` → call `_save_lineage_fcn_result()` (the lineage-aware path)
   - Otherwise → call `output_obj.save(data, **db_kwargs, **save_metadata)` (plain save, no lineage)

4. **Log each save**: Prints timing and record ID: `[save] subject=1, session=A: FilteredEMG (lineage) -> record_id=abc123def456 in 0.042s`

5. **Error handling**: If a save fails, the error is logged but does not stop other saves.

### `_save_lineage_fcn_result()` (lines 467–570)

This is the core lineage save logic. It has two distinct paths:

#### Normal path (lines 539–565)

For functions that produce data (the common case):

1. **Extract lineage**: Call `extract_lineage(data)` to get a `LineageRecord` from the `LineageFcnResult`.

2. **Convert to dict**: `_lineage_to_dict()` (lines 622–629) converts the `LineageRecord` to a flat dict:
   ```python
   {
       "function_name": "bandpass_filter",
       "function_hash": "a1b2c3d4e5f6g7h8",
       "inputs": [...],     # list of input entries
       "constants": [...],  # list of constant entries
   }
   ```

3. **Append rid_tracking**: `_append_rid_tracking()` (lines 610–619) adds `__rid_*` entries to the lineage inputs list:
   ```python
   {"name": "__rid_signal", "source_type": "rid_tracking", "record_id": "abc123..."}
   ```
   These entries allow skip_computed to verify that the same input records were used.

4. **Compute hashes**:
   - `lineage_hash` = `data.hash` (content hash of the lineage result)
   - `pipeline_lineage_hash` = `data.invoked.compute_lineage_hash()` (hash of the full invocation — function + inputs + constants)

5. **Wrap data in variable class**: Extract the raw data via `get_raw_value(data)`, create an instance of the output variable class (`variable_class(raw_data)`).

6. **Add __fn and __fn_hash to metadata**: These are added to save metadata so that scidb's version_keys system can identify which function produced the output.

7. **Save via db.save()**: Call `active_db.save(instance, fn_metadata, lineage=lineage_dict, lineage_hash=lineage_hash, pipeline_lineage_hash=pipeline_lineage_hash)`. This writes:
   - A row to `_record_metadata` with the computed record_id, version_keys, branch_params
   - The data to the variable's data table
   - A row to `_lineage` with the function name, function hash, inputs, constants, lineage hash

#### generates_file path (lines 491–537)

For functions decorated with `@lineage_fcn(generates_file=True)` — functions that produce files on disk rather than data to store in DuckDB:

1. **Extract lineage and compute hashes** (same as normal path).

2. **Generate a synthetic record ID**: `generated_id = f"generated:{pipeline_lineage_hash[:32]}"`. There is no actual data content, so the record ID is derived from the lineage hash instead of from a content hash.

3. **Split metadata**: Call `db._split_metadata(metadata)` to separate schema keys from version keys.

4. **Write _record_metadata directly**: Call `db._save_record_metadata()` with:
   - `record_id=generated_id`
   - `content_hash=None` (no data stored)
   - `lineage_hash=pipeline_lineage_hash`
   - `version_keys` include `__fn` and `__fn_hash`

5. **Write _lineage directly**: Call `db._save_lineage()` with the lineage dict.

6. **No data table write**: The function's output is a file on disk — nothing is written to a DuckDB data table. Only metadata and lineage are persisted.

This enables skip_computed to work for file-generating functions: the lineage record tracks what inputs and function version produced the file, so a re-run can be skipped if nothing has changed.

### `save()` — public API (lines 573–607)

The module-level `save()` function is exported as `scihist.save` for standalone use outside of `for_each`:

```python
from scihist import save

result = my_lineage_fn(input_data)
save(OutputType, result, subject=1, session="A")
```

It routes `LineageFcnResult` → `_save_lineage_fcn_result()`, raw data → `variable_class.save()`.

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

6. **Classify inputs for save**:
   - `signal`: variable type → skip
   - `calibration`: Fixed wrapper → resolve `db.find_record_id(Calibration, {"session": "baseline"})` → store as `fixed_rids["__rid_calibration"] = "cal_rid_123"`
   - `low_hz`, `high_hz`: constants → `constant_inputs = {"low_hz": 20, "high_hz": 450}`

7. **Save with lineage**: For each result row:
   - Extract `__rid_signal` from result metadata, merge with `fixed_rids` → `input_rids = {"__rid_signal": "sig_rid_456", "__rid_calibration": "cal_rid_123"}`
   - Strip `__`-prefixed keys from save metadata → `save_metadata = {"subject": "1", "session": "A", "low_hz": 20, "high_hz": 450}`
   - Output value is `LineageFcnResult` → route to `_save_lineage_fcn_result()`:
     - `extract_lineage(data)` → `LineageRecord(function_name="bandpass_filter", function_hash="a1b2...", inputs=[...], constants=[...])`
     - `_lineage_to_dict()` → flat dict
     - `_append_rid_tracking()` → adds `__rid_signal` and `__rid_calibration` entries to lineage inputs
     - `data.hash` → `lineage_hash`
     - `data.invoked.compute_lineage_hash()` → `pipeline_lineage_hash`
     - `get_raw_value(data)` → the raw numpy array result
     - `FilteredEMG(raw_data)` → variable instance
     - Add `__fn="bandpass_filter"`, `__fn_hash="a1b2..."` to metadata
     - `db.save(instance, metadata, lineage=lineage_dict, lineage_hash=..., pipeline_lineage_hash=...)` →
       - Writes to `_record_metadata` (record_id, version_keys, branch_params, lineage_hash)
       - Writes data to `FilteredEMG_data`
       - Writes to `_lineage` (function_name, function_hash, inputs with rid_tracking, constants, lineage_hash)
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

Understanding the differences in what each layer writes helps debug unexpected behavior:

| Aspect | scidb.for_each | scihist.for_each |
|---|---|---|
| `version_keys.__fn` | Written | Written (via `_save_lineage_fcn_result`) |
| `version_keys.__fn_hash` | Written (from `ForEachConfig`) | Written (from `lineage_dict`) |
| `version_keys.__inputs` | Written | NOT written (scihist doesn't use `ForEachConfig` for save) |
| `version_keys.__constants` | Written | NOT written |
| `branch_params` | Written (accumulated upstream + namespaced constants) | Written `{}` (empty — scihist doesn't namespace constants into branch_params) |
| `_lineage` row | NOT written | Written (function_name, function_hash, inputs, constants) |
| `_lineage.inputs` entries | N/A | `rid_tracking` entries for each `__rid_*` input |
| Constant variant disambiguation | Via `version_keys.__constants` + `branch_params` | Via `version_keys` (constants as top-level keys) |

This means:
- `_get_output_combos()` must check BOTH `version_keys.__fn` AND `_lineage.function_name` to find all outputs
- `_get_expected_combos()` must consult BOTH `list_pipeline_variants()` AND `_get_lineage_variants()`
- scihist outputs have `branch_params={}`, so variant tracking relies on `version_keys` and `_lineage` rather than on `branch_params`
