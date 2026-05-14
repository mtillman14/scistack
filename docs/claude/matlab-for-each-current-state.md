# MATLAB `for_each` Layer — Current State Snapshot

## Why this document exists

Captures the state of the MATLAB `for_each` stack as of 2026-05-13, before the redesign described in `.claude/matlab-for-each-redesign-plan.md` begins. Pair this with the three Python internals docs (`scifor-for-each-internals.md`, `scidb-for-each-internals.md`, `scihist-for-each-internals.md`) to see the gap the redesign closes.

This is descriptive, not prescriptive. For "what should change," see the redesign plan in `.claude/`.

All MATLAB references are to files under `/workspace/sci-matlab/src/sci_matlab/matlab/`. The Python bridge is `/workspace/sci-matlab/src/sci_matlab/bridge.py`.

---

## Layer overview

MATLAB has three parallel layers that mirror Python's:

```
+scihist/for_each.m  (56 lines)   — auto-wraps fn in scidb.LineageFcn, delegates to +scidb/for_each
   ↓
+scidb/for_each.m    (2160 lines) — DB load, version keys, save, parallel branch
   ↓
+scifor/for_each.m   (1582 lines) — pure MATLAB-table iteration (also usable standalone)
```

Unlike Python (where the layers are clean delegations), the MATLAB layers have substantial duplication of work that Python now owns. This document inventories that duplication.

---

## File map

### `+scifor/`

| File | Lines | Purpose |
|---|---|---|
| `for_each.m` | 1582 | Standalone MATLAB-table iterator. Filters tables by schema-key columns, calls fn, collects results. Mirrors Python `scifor.for_each` but operates on MATLAB tables. Used both standalone AND as the inner loop of `+scidb/for_each.m`. |
| `PathInput.m` | 343 | Classdef wrapping a path template with `{key}` placeholders. Implements `.load()` (substitution + optional regex match against directory contents), `.discover()` (recursive filesystem walk with named-capture-group regex), `.placeholder_keys()`. **Duplicates Python `scifor.pathinput.PathInput`.** |
| `Fixed.m` | 68 | Wraps a MATLAB table with metadata overrides. Mirrors `scifor.Fixed` (Python). |
| `Merge.m` | 87 | Wraps multiple MATLAB tables for column-wise merging. Mirrors `scifor.Merge`. |
| `ColumnSelection.m` | 46 | Wraps a table + column subset. |
| `ColName.m` | 46 | Wraps a table; resolves to its single non-schema data column name. |
| `Col.m` | 54 | Entry point for filter expressions: `Col("speed") > 1.5`. |
| `ColFilter.m` | 58 | Single comparison filter. |
| `CompoundFilter.m` | 48 | `&` / `\|` of filters. |
| `NotFilter.m` | 34 | `~` of a filter. |
| `set_schema.m` | 24 | Sets MATLAB-side schema global. |
| `get_schema.m` | (small) | Reads MATLAB-side schema global. |
| `schema_store_.m` | 22 | Persistent module-level schema variable. **Independent from Python's scifor module global.** |

### `+scidb/`

| File | Lines | Purpose |
|---|---|---|
| `for_each.m` | 2160 | DB-backed for_each. Builds version keys, loads inputs, propagates schema, delegates the loop to `+scifor/for_each`, then saves results. Includes a 340-line `run_parallel` branch (not currently in use). |
| `BaseVariable.m` | 972 | Base class for variable types. Provides `.save()`, `.load()`, `.load_all()`, `.list_versions()`, `.provenance()`. Each method bridges to a Python equivalent. |
| `LineageFcn.m` | 190 | Wraps a MATLAB function for lineage tracking. Constructs a `MatlabLineageFcn` Python proxy. On call: builds `MatlabLineageFcnInvocation`, checks Python cache, executes in MATLAB on miss, wraps results as `scidb.LineageFcnResult`. |
| `LineageFcnResult.m` | 59 | Pairs MATLAB result data with the Python lineage shadow. |
| `PathGenerator.m` | 218 | Filesystem path generator helper. |
| `Fixed.m` | 71 | Wraps a `BaseVariable` (or other scidb wrapper) with metadata overrides. Distinct classdef from `+scifor/Fixed.m`. |
| `Merge.m` | 102 | Wraps multiple variable types for merged loading. Distinct from `+scifor/Merge`. |
| `Filter.m` | 56 | Wraps Python `scidb.filters.Filter`. |
| `ColName.m` | 42 | Wraps a `BaseVariable`; resolves to its data column name via Python query. |
| `configure_database.m` | 58 | Calls `py.scidb.configure_database`, sets schema global. |
| `Log.m` | 135 | MATLAB-side logging that mirrors `scidb.log.Log`. |
| `register_variable.m` | 32 | Registers a MATLAB type via `py.sci_matlab.bridge.register_matlab_variable`. |
| `get_database.m` | (small) | Returns the global Python `DatabaseManager`. |
| `close_database.m` | 35 | Closes the database. |
| `raw_sql.m`, `add_to_var_group.m`, `remove_from_var_group.m`, `get_var_group.m`, `list_var_groups.m`, `isabsolute.m` | misc | Various utility functions, mostly thin Python forwarders. |

### `+scidb/+internal/`

Helper functions used by `+scidb/for_each.m`, `+scidb/BaseVariable.m`, `+scidb/LineageFcn.m`.

| File | Lines | Purpose |
|---|---|---|
| `to_python.m` | 285 | Convert MATLAB values to Python (numeric, string, table, cell). Heavily optimized: cell-of-numerics packed into one numpy array; same-schema cell-of-tables concatenated to single DataFrame; string columns joined with `\x1e` separator. |
| `from_python.m` | 282 | Convert Python values back to MATLAB. Inverse of `to_python.m`. |
| `metadata_to_pydict.m` | 58 | Convert name-value pairs to a Python dict. |
| `metadata_to_pykwargs.m` | 45 | Convert name-value pairs to `pyargs`-compatible cell array. |
| `normalize_cell_column.m` | 42 | Normalize a cell column to a homogeneous MATLAB type if possible. |
| `resolve_var_names.m` | 41 | Resolve variable type names from class objects. |
| `unwrap_input.m` | 31 | Strip `BaseVariable`/`LineageFcnResult` to raw data. |
| `hash_function.m` | 31 | Compute SHA-256 of a function's source file via Python's `hashlib`. |
| `split_version_arg.m` | 30 | Parse `version_id` argument. |
| `pydict_to_struct.m` | 26 | Python dict → MATLAB struct. |
| `cartesian_product.m` | 26 | `ndgrid`-based Cartesian product over cell arrays. |
| `pylist_to_cell.m` | 24 | Python list → MATLAB cell array. |
| `to_python_input.m` | 23 | Convert a single function input for `MatlabLineageFcnInvocation`. |
| `function_name.m` | 13 | Extract function name from a function handle. |
| `ensure_registered.m` | 12 | Auto-register a MATLAB type with Python via `register_matlab_variable`. |
| `split_load_all_args.m` | 49 | Split `load_all` keyword arguments into metadata vs options. |

### `+scihist/`

| File | Lines | Purpose |
|---|---|---|
| `for_each.m` | 56 | Auto-wraps `fn` in `scidb.LineageFcn`, then delegates to **`+scidb/for_each.m`** (NOT Python's `scihist.for_each`). |
| `configure_database.m` | 38 | Configures DB. |

---

## The Python bridge surface (used by MATLAB)

`bridge.py` exposes the following entry points to MATLAB. Grouped by purpose:

**Lineage proxy classes:**
- `MatlabLineageFcn` — proxy satisfying `scilineage.LineageFcn`'s duck-typing contract.
- `MatlabLineageFcnInvocation` — proxy for `LineageFcnInvocation`. Reuses `classify_inputs`.
- `make_lineage_fcn_result(invocation, output_num, data)` — instantiates a real `LineageFcnResult`.

**Cache + lineage:**
- `check_cache(invocation)` — look up cached output by lineage hash.

**Type registration:**
- `register_matlab_variable(type_name, schema_version)` — create a Python surrogate `BaseVariable` subclass.
- `get_surrogate_class(type_name)` — retrieve an already-registered surrogate.
- `get_data_column_name(py_class, db)` — resolve `ColName` via `_variables` table query.

**Bulk load:**
- `load_and_extract(py_class, metadata_dict, version_id, db, where)` — single-call replacement for `load_all → list → wrap_batch_bridge`.
- `wrap_batch_bridge(py_vars_list)` — extract scalars/metadata/data from a list of `BaseVariable`. Caches data in `_batch_cache`; returns lightweight strings/JSON.
- `get_batch_item(batch_id, index)` — fetch one item from cache.
- `get_batch_data_item(batch_id, index)` — fetch just the data.
- `free_batch(batch_id)` — release a cached batch.

**Bulk save:**
- `save_batch_bridge(type_name, data_values, metadata_keys, metadata_columns, common_metadata, db)` — columnar save (used by `BaseVariable.save_batch`).
- `for_each_batch_save(type_name, data_list, metadata_list, db)` — list-form save (used by MATLAB `+scidb/for_each.m` standard path and parallel Phase C).
- `for_each_batch_save_dataframe(type_name, dataframe, row_counts, meta_keys, meta_columns, common_metadata, db)` — fast columnar path used by `try_fast_batch_save` for single-output DataFrame results.

**Misc:**
- `split_flat_to_lists(flat_array, lengths)` — splits one numpy array into N lists; supports `to_python`'s cell-column fast path.

---

## What `+scidb/for_each.m` does (2160 lines, summarized)

Walking through the major sections:

### 1. Argument parsing (lines 47–105)
- `split_options` extracts known options from name-value pairs.
- Builds `db_nv`, `where_nv` cells for downstream pass-through.
- Resolves function name (or accepts the `_fn_name` override that `+scihist/for_each.m` passes).

### 2. Empty-list resolution from DB (lines 107–133)
- For each `key=[]` argument, calls `py_db.distinct_schema_values(key)`.
- **Duplicates** Python `scidb.for_each` Step 2.

### 3. PathInput discovery (lines 135–212)
- If any input is a `PathInput`, calls `pi.discover()` (the MATLAB-side version in `+scifor/PathInput.m`).
- Filters discovered combos by user-supplied metadata values.
- **Duplicates** Python `scidb.for_each` Step 3.

### 4. Schema propagation (line 215, function at 1771)
- `propagate_schema(opts.db)` reads `db.dataset_schema_keys` and calls `scifor.set_schema()`.
- This sets MATLAB's schema global, which `+scifor/for_each.m` reads.
- **NOTE**: this only sets the MATLAB-side schema. Python's `scifor` schema global is set independently elsewhere.

### 5. ColName resolution (lines 217–277)
- For each `ColName(BaseVariable)` input: queries `_variables` via `py.sci_matlab.bridge.get_data_column_name`.
- For each `ColName(table)` input: resolves locally by inspecting table columns minus schema keys.
- Replaces the `ColName` wrapper with the resolved string.
- **Mostly** delegates to Python (`get_data_column_name`); the local-table fallback is MATLAB-only.

### 6. Loadable vs. constant classification (lines 279–299)
- `is_loadable()` checks for `BaseVariable`, `Fixed`, `PathInput`, `Merge`, table.
- `constant_nv` collects metadata-compatible constants for save metadata.

### 7. Version-key construction (line 302, function at 1911)
- `build_config_nv` builds `__fn`, `__inputs`, `__constants`, `__where`, `__distribute`, `__as_table` keys.
- `serialize_loadable_inputs` produces `__inputs` JSON.
- `input_spec_to_key` recursively converts each input spec to its canonical string form.
- `format_repr` formats values in Python `repr()` style.
- **Duplicates** Python `ForEachConfig.to_version_keys()`.

### 8. Schema combo prefilter (lines 313–388)
- When some keys were resolved from DB AND no PathInput, calls `db.distinct_schema_combinations(filter_keys)`.
- Builds `existing_set` (a `containers.Map`), filters `raw_combos`.
- Converts result to `_all_combos` cell of structs for scifor.
- **Duplicates** Python `scidb.for_each` Step 9.

### 9. Persist expected combos (lines 396–422)
- Builds `py_combos` list-of-dicts.
- Calls `py.scidb.foreach._persist_expected_combos(...)`.
- **Delegates** to Python — good.

### 10. Parallel branch (lines 436–453, function at 1338)
- 340 lines. Three-phase: pre-resolve inputs serially → `parfor` compute → batch save.
- Phase A pre-loads all variable inputs and builds a `containers.Map` lookup keyed by combo metadata.
- Phase B uses MATLAB `parfor` for the function calls.
- Phase C calls `for_each_batch_save` per output.
- **Not currently in use** per Phase 0 of the redesign.

### 11. Resolve DB for loading (lines 456–460)
- Either uses `opts.db` or `py.scidb.database.get_database()`.

### 12. Load all inputs (lines 462–497, function at 688)
- Iterates over `inputs`, calling `convert_input` on each loadable.
- `convert_input` (lines 688–768): handles `Merge` (recursive), `Fixed` (recursive), `PathInput` (passthrough), table (passthrough), `BaseVariable` (calls `load_and_extract`, then `lineage_results_to_table`).
- `lineage_results_to_table` (lines 771–868): converts `BaseVariable[]` from the bridge into a MATLAB table with metadata + data columns. Two assembly modes: all-tables (vertcat with replicated metadata) vs. mixed (cell-nested). **Duplicates** Python `_load_var_type_all`'s DataFrame assembly.

### 13. Build scifor metadata + options (lines 499–556)
- Converts metadata iterables to scifor-compatible name-value pairs.
- Sets `_all_combos`, `_nest_table_outputs`, `_resolve_pathinput`, `_log_fn`.

### 14. Delegate to scifor (lines 565–581)
- Calls `scifor.for_each(fn, scifor_inputs, scifor_opts{:}, scifor_meta_nv{:})` with `n_out` outputs.

### 15. Merge per-output result tables (lines 584–589)
- Joins multiple output tables into one for the return value.

### 16. Save (lines 600–614, function at 875)
- `save_results` iterates over result rows.
- For each row: either standard mode (output column present by name) or flatten mode (output is a multi-row table).
- Calls `try_fast_batch_save` first (lines 916–928, function at 1084) — vertcat all result tables, single `to_python` call, columnar metadata, `for_each_batch_save_dataframe`.
- On fast-path failure or for non-eligible rows: per-row `for_each_batch_save` accumulation.
- For `LineageFcnResult` outputs: bypasses batch and calls `py.scihist.foreach.save(out_py_class, output_value.py_obj, pyargs(...))`.
- Helper functions: `format_save_meta`, `build_row_group_keys`, `flatten_nested_table_outputs`.
- **Duplicates** Python `scidb.for_each` Step 19, but with significant differences (see Known Gaps below).

### 17. Flatten nested table outputs for return (line 619, function at 1266)
- Un-nests cell-arrays-of-tables into the final return value.

---

## What `+scihist/for_each.m` does (56 lines)

Very thin:

```matlab
% Auto-wrap in scidb.LineageFcn if not already
if isa(fn, 'scidb.LineageFcn')
    lineage_obj = fn;
else
    lineage_obj = scidb.LineageFcn(fn);
end

% Wrap LineageFcn in plain function handle for scidb.for_each
fn_plain = @(varargin) lineage_obj(varargin{:});

% Pass real function name so scidb.for_each persists expected combos correctly
real_fn_name = func2str(fn);

% Delegate to scidb.for_each (MATLAB, NOT Python scihist)
result_tbl = scidb.for_each(fn_plain, inputs, outputs, ...
    '_fn_name', real_fn_name, varargin{:});
```

**Notably absent:** no `skip_computed` parameter, no skip hook construction, no integration with Python's `scihist.foreach.save_lineage_result` callback architecture. The `LineageFcnResult` save path lives inside `+scidb/for_each.m`'s `save_results` (which directly calls `py.scihist.foreach.save`), bypassing Python's unified scihist save logic.

---

## What `+scifor/for_each.m` does (1582 lines)

Standalone iterator over MATLAB tables. Conceptually mirrors Python's `scifor.for_each`:

1. Parse options + metadata iterables.
2. Resolve `output_names`, `as_table`, `distribute_key`.
3. Resolve empty `[]` metadata via `distinct_values_from_inputs` (scans input tables for distinct column values).
4. Resolve `ColName` wrappers to data column names.
5. Classify inputs as data vs. constant.
6. Build combo list (Cartesian product, or use `_all_combos`).
7. Print banner.
8. Per-combo loop:
   - Build metadata struct.
   - Filter each data input via `prepare_input` (handles plain tables, `Fixed`, `Merge`, `ColumnSelection`).
   - Call `fn(loaded{:})`.
   - Handle `distribute` (split outputs into multiple rows).
   - Collect results.
9. Build per-output result tables.
10. Optionally apply categorical sort.

Used both standalone AND as the inner loop of `+scidb/for_each.m`. Standalone usage is real (per Phase 0 of the redesign plan).

---

## Cross-cutting concerns

### Two parallel wrapper hierarchies

There are TWO sets of `Fixed`/`Merge`/`ColumnSelection`/`ColName`:

- `+scifor/*.m` — wrap MATLAB **tables** (or other scifor wrappers).
- `+scidb/*.m` — wrap `BaseVariable` **types** (or other scidb wrappers).

`+scidb/for_each.m`'s `convert_input` translates from scidb wrappers to scifor wrappers (e.g., `scidb.Fixed(BaseVariable, sess="A")` → load → `scifor.Fixed(table, sess="A")`).

This mirrors Python's two-tier wrapper system (`scidb.Fixed` vs `scifor.Fixed`), so the structure itself is sound. The MATLAB duplication exists because each tier needs MATLAB-callable classdefs.

### Schema state in two places

- **Python:** `scifor`'s module-level `_schema` global, set via `scifor.set_schema()`.
- **MATLAB:** `+scifor/schema_store_.m` holds an independent persistent variable.

`+scidb/configure_database.m` calls both. `+scidb/for_each.m`'s `propagate_schema` reads `db.dataset_schema_keys` and calls MATLAB-side `scifor.set_schema`. Python-side schema is set elsewhere (when Python's `scidb.for_each` runs, which happens during `_persist_expected_combos` and various bridge calls).

This dual-global setup is a known fragility: a bug in the MATLAB sync would silently produce wrong filtering results.

### Function hashing

`scidb.internal.hash_function` reads the `.m` file via `fileread` and hashes via `py.hashlib.sha256`. The format is raw-source-bytes-SHA-256, no normalization.

This hash is then passed to `MatlabLineageFcn(source_hash, name, unpack_output)` in Python, which combines it with `unpack_output` and re-hashes:

```python
string_repr = f"{source_hash}{STRING_REPR_DELIMITER}{unpack_output}"
self.hash = sha256(string_repr.encode()).hexdigest()
```

Per `MEMORY.md`, the MATLAB hash recipe and the GUI's `matlab_parser` hash recipe were reconciled and verified to agree on pure-ASCII/LF files. CRLF or non-ASCII files could still diverge.

### PathInput

MATLAB has its own 343-line `PathInput.m` with full discovery + regex + load logic. Python has the equivalent in `scifor/pathinput.py`. They're independent implementations that need to agree on:

- Template syntax (`{key}` substitution)
- Regex translation rules (named capture groups, escaping)
- Recursive walk order (alphabetical via `sort`)
- Multi-occurrence placeholder handling (numbered suffix `subject_2` → `subject`)

Drift between the two is possible but not caught by automated tests (cross-language PathInput tests don't exist).

---

## Data flow: a representative `+scidb/for_each` call

User code:

```matlab
scidb.for_each(@bandpass, ...
    struct('signal', RawEMG(), 'low_hz', 20, 'high_hz', 450), ...
    {FilteredEMG()}, ...
    subject=[1 2], session=["A" "B"]);
```

What happens:

1. **Parse** name-value args. `meta_keys = ["subject", "session"]`, `meta_values = {{1,2}, {"A","B"}}`.

2. **Empty-list resolution:** none needed (both lists populated).

3. **PathInput discovery:** none.

4. **Schema propagation:** read `["subject","session"]` from Python DB → set MATLAB-side `scifor.set_schema`.

5. **ColName resolution:** none.

6. **Classify inputs:** `signal` → loadable; `low_hz`, `high_hz` → constants.

7. **Version keys:** build `{__fn: "bandpass", __inputs: '{"signal": "RawEMG"}', __constants: '{"high_hz": true, "low_hz": true}'}`. Note: MATLAB stores `__constants` as a JSON of names-only (`{"low_hz": true}`), not values.

8. **Schema prefilter:** skipped (no DB-resolved keys).

9. **Persist expected combos:** call `py.scidb.foreach._persist_expected_combos(db, "bandpass", py_combos)`.

10. **Load `signal`:** `convert_input` → `load_and_extract(RawEMG_class, {}, db=...)` → bridge returns wrapped batch → `wrap_py_vars_batch` extracts `BaseVariable[]` → `lineage_results_to_table` builds a MATLAB table with `subject`, `session`, `RawEMG` columns. Note: `__record_id` and `__branch_params` are **not surfaced** by the MATLAB bridge path — the variant tracking columns from Python's `_load_var_type_all` are stripped during `wrap_py_vars_batch` since they're internal to `_record_metadata`. **This is the root cause of MATLAB's missing `__rid_*` variant expansion.**

11. **Build scifor metadata + options:** `subject=[1 2], session=["A","B"]`, no `_all_combos`, `_nest_table_outputs=true`, `_log_fn=Log.info`.

12. **Delegate to scifor.for_each:** scifor builds combos `[(1,A), (1,B), (2,A), (2,B)]`. For each: filter the loaded `signal` table to the matching row, drop schema columns, extract scalar value, call `bandpass(signal=<value>, low_hz=20, high_hz=450)`. Collect the 4 results into one output table.

13. **Save:** for each result row, `try_fast_batch_save` checks eligibility. If output is a table → fast path: vertcat all output tables, one `to_python`, columnar metadata via `\x1e`-joined strings, single call to `for_each_batch_save_dataframe`. Otherwise: per-row `for_each_batch_save` accumulation.

   Save metadata = row metadata (subject, session) + `constant_nv` (low_hz=20, high_hz=450) + `config_nv` (`__fn`, `__inputs`, `__constants`).

   Notably **absent** from the save metadata:
   - `__branch_params` (MATLAB writes none; Python would accumulate upstream)
   - `__upstream` (MATLAB writes none)
   - `__rid_*` lookup (MATLAB never tracked them)

14. **Return:** flatten nested outputs and return the result table to the user.

---

## Known gaps vs. Python

1. **No `__rid_*` variant expansion.** When `RawEMG` has multiple variants for the same (subject, session) — e.g., from prior `for_each` calls with different parameters — MATLAB silently picks one row (or vertcats them into a multi-row table that gets flattened). Python's scidb expands the combo list to one combo per variant and processes each separately. MATLAB-driven pipelines mix variants.

2. **No `branch_params` accumulation.** Saved records carry no record of upstream pipeline choices. The `branch_params` column in `_record_metadata` is `'{}'` for MATLAB-saved records.

3. **No `__upstream` tracking.** Saved records don't reference the upstream `record_id`s they consumed.

4. **No `skip_computed`.** Every `+scihist/for_each` call recomputes everything. The Python 4-step skip check (output exists → fn hash → input rids → constant hashes) is unreachable from MATLAB.

5. **Lineage save bypasses Python's unified scidb→scihist callback model.** MATLAB's `save_results` directly calls `py.scihist.foreach.save(...)` for `LineageFcnResult` items. This works but doesn't go through `scidb.for_each`'s save path, so it doesn't get the `branch_params`/`__upstream` plumbing. (Confirmed by `MEMORY.md`'s note about `__fn`/`__inputs` config keys being stripped by scihist on this path.)

6. **MATLAB function hash format owned in MATLAB.** `+scidb/+internal/hash_function.m` is the only place that knows the MATLAB-fn hash format. The GUI's `matlab_parser` separately implements the same format. `MEMORY.md` confirms they were reconciled and currently agree, but any future change requires touching three places (MATLAB, GUI, Python proxy code).

7. **`run_parallel` is dead code.** The 340-line parallel branch is not in use (per Phase 0 of the redesign).

8. **Column-selection round-trip on Fixed inputs is custom.** `+scidb/for_each.m` `convert_input` walks `scidb.Fixed → BaseVariable → load → scifor.Fixed`, separately handling `selected_columns`. Python's `_convert_inputs` does the same translation but with full variant tracking; MATLAB's path doesn't carry the variant info forward.

---

## Performance characteristics

The MATLAB layer has been progressively optimized to reduce MATLAB↔Python crossings:

- **Bulk load:** `load_and_extract` in one call instead of `load_all → list → wrap_batch_bridge` separately.
- **Server-side data cache:** `_batch_cache` keeps loaded data in Python; MATLAB only fetches scalars/JSON across the bridge until it needs an item. `get_batch_data_item(batch_id, index)` retrieves single items lazily.
- **Scalar fast path:** `wrap_batch_bridge` packs all-scalar data into one numpy array for single-crossing transfer.
- **DataFrame fast path:** same-schema DataFrames are concatenated with `pd.concat` and a `row_counts` array; MATLAB receives one large table instead of N small ones.
- **`to_python` cell-column fast path:** cell-of-numerics packed into one numpy array + `lengths` array, then `split_flat_to_lists` reconstitutes Python lists in one Python-side call.
- **String column packing:** string columns crossed via `\x1e`-joined char arrays (record-separator-delimited) and split Python-side.
- **Columnar batch save:** `for_each_batch_save_dataframe` accepts pre-vertcat'd DataFrame + columnar metadata; replaces N per-row `to_python` + `metadata_to_pydict` crossings with one.

Despite these optimizations, the MATLAB-driven loop still costs N MATLAB↔Python crossings for save metadata construction (in the per-row fallback paths) and N `from_python` calls when consuming load results. The Python-driven design proposed in the redesign plan eliminates per-combo crossings entirely except for the user function call itself.

---

## File-of-record references

- Python redesign: `docs/claude/scifor-for-each-internals.md`, `docs/claude/scidb-for-each-internals.md`, `docs/claude/scihist-for-each-internals.md`.
- Layer friction analysis (Python only): `docs/claude/layer-friction-analysis.md`.
- Redesign plan for MATLAB: `.claude/matlab-for-each-redesign-plan.md`.
- MEMORY.md notes: matlab/GUI hash recipes agree (2026-04-19); user runs tests themselves; latest-record selection across hashes deferred.
