# MATLAB `for_each` Redesign Plan: Catch Up to Python's Layered Architecture

## Context

Python's `scifor` / `scidb` / `scihist` `for_each` was reorganized so that:

- `scifor` = pure iteration over in-memory pandas DataFrames (no DB awareness).
- `scidb` = owns DB load, version-key fingerprinting, `__rid_*` variant expansion, branch_params accumulation, `__upstream` tracking, save (with optional callback into scihist for `LineageFcnResult`).
- `scihist` = thin wrapper: auto-wrap function in `LineageFcn`, build `skip_computed` hook, delegate to `scidb`. Saves go through scidb's callback model.

The MATLAB stack has not caught up. Today it largely re-implements DB-layer concerns in MATLAB:

- `+scidb/for_each.m` is 2160 lines, doing its own version-key build, schema combo prefilter, BaseVariable→table assembly, save batching with a "fast path", PathInput discovery + filtering, and a 340-line parallel branch.
- `+scifor/for_each.m` is 1582 lines (kept — see Phase 0).
- `+scifor/PathInput.m` is 343 lines duplicating Python's `PathInput.discover/load`.
- `+scihist/for_each.m` is 56 lines but has no `skip_computed`, and delegates to MATLAB's `scidb.for_each` (not Python's `scihist.for_each`).

The MATLAB layer also does NOT do `__rid_*` variant expansion, which is a correctness regression vs. Python — MATLAB-driven pipelines silently mix upstream variants.

## Phase 0 — Locked-in decisions

- **`scifor.for_each` standalone (MATLAB tables only) IS in active use.** Keep `+scifor/for_each.m` as-is. The standalone-table iterator does not have a Python equivalent (Python scifor is for pandas DataFrames) so it stays in MATLAB. Phase 5 below removes only the parts of `+scifor/*` whose sole consumer is the redesigned scidb path.
- **`run_parallel`'s `parfor` is NOT in active use.** The 340-line parallel branch in `+scidb/for_each.m` (and its supporting `preloaded_*` plumbing) can be deleted outright in Phase 3.

## Goals

1. **Correctness:** MATLAB-driven `scidb.for_each` and `scihist.for_each` do `__rid_*` variant expansion, branch_params accumulation, `__upstream` tracking, lineage-aware save — by routing through Python instead of reimplementing.
2. **`skip_computed` for MATLAB:** scihist's 4-step skip check (output exists → fn hash matches → input rids match → constant hashes match) becomes available to MATLAB callers.
3. **No drift risk:** Function hashing, version-key serialization, schema globals, and PathInput discovery are owned by Python. MATLAB ships raw inputs across the bridge; Python returns canonical results.
4. **Faster, not slower:** Per-combo MATLAB↔Python crossings are eliminated by pre-resolving all combos in Python and shipping the batch to MATLAB once. Only the user's MATLAB function call itself happens per-combo, with no bridge crossings around it.

## Scope of deletion (~3400 lines of MATLAB removed)

| File / function | Lines | Action |
|---|---|---|
| `+scidb/for_each.m` | 2160 | Shrink to ~80 lines (delegates to new bridge entry) |
| `+scihist/for_each.m` | 56 | Rewrite, ~30 lines (delegates to Python `scihist.for_each`) |
| `+scifor/PathInput.m` | 343 | Shrink to ~30 lines (thin handle around Python `PathInput`) |
| `+scidb/+internal/hash_function.m` | 31 | Delete (move format ownership to Python `scilineage.hashing`) |
| `+scidb/+internal/cartesian_product.m` | 26 | Delete (loop body lives in Python) |
| `+scifor/schema_store_.m` + parts of `set_schema`/`get_schema` | ~25 | Delete the MATLAB-side schema global; forward to Python |
| `bridge.py` `for_each_batch_save`, `for_each_batch_save_dataframe` | ~170 | Delete (Python-driven save makes them obsolete) |
| `+scifor/for_each.m` | 1582 | **Keep** — standalone MATLAB-table use is real |
| `+scifor/Fixed.m`, `Merge.m`, `ColumnSelection.m`, `ColName.m` | ~250 | **Keep** — needed by standalone `+scifor/for_each.m` |

Estimated net change: **~3400 lines deleted from MATLAB, ~50–100 lines added in `bridge.py`**.

## Phase 1 — Foundation for delegation

Goal: stand up the Python entry points the later phases need, without changing public MATLAB behavior yet.

**1.1 Add Python bridge entry: MATLAB function hashing**

In `sci_matlab/bridge.py`, add:

```python
def compute_matlab_function_hash(source_text: str, name: str, unpack_output: bool) -> str:
    """SHA-256 hash for a MATLAB function. Owned in Python so the format
    can be tweaked centrally (e.g. to strip comments or normalize line
    endings) without divergence between MATLAB-side and Python-side
    consumers."""
```

Replace `+scidb/+internal/hash_function.m`'s body so that MATLAB only does `fileread` + sends the source string + name + unpack flag across the bridge. Delete the local SHA-256 logic.

**1.2 Surface the loaded DataFrame from Python directly**

Add bridge entry:

```python
def load_var_type_all_as_df(py_class, where=None, db=None) -> pandas.DataFrame:
    """Return the assembled DataFrame from _load_var_type_all (with
    __record_id, __branch_params, schema columns, data columns) as a
    single object that crosses the bridge once."""
```

This sets up Phase 3's deletion of MATLAB's `lineage_results_to_table`.

**1.3 Forward MATLAB schema to Python**

Rewrite `+scifor/set_schema.m` and `+scifor/get_schema.m` to call `py.scifor.set_schema(...)` / `py.scifor.get_schema()` directly. Delete `+scifor/schema_store_.m`.

This eliminates the dual-global fragility (MATLAB and Python each holding their own schema list) and makes Phase 3's `__rid_*` schema extension/restore work without MATLAB-side bookkeeping.

**Risk / mitigation:** standalone `+scifor/for_each.m` users currently set MATLAB-side schema via `scifor.set_schema(["subject","session"])` and don't have Python configured. After this change, `set_schema` will fail if Python isn't importable. Wrap the forwarding in a try/except that falls back to a small MATLAB cache when Python isn't available — but write to BOTH when Python IS available. Document the fallback.

**Phase 1 acceptance:**
- All existing MATLAB tests still pass.
- `scidb.LineageFcn` produces the same hash for the same source it did before (verify with one round-trip test).
- Standalone `scifor.for_each` still works without Python configured.

## Phase 2 — Delegate version-keys + path discovery

Goal: stop duplicating logic that already exists in Python and is purely a function of the input spec.

**2.1 Replace MATLAB version-key building**

Add bridge entry:

```python
def build_for_each_config_keys(
    fn_name: str,
    fn_hash: str,           # already computed by MATLAB (Phase 1.1)
    inputs_spec: dict,      # {param_name: type_name | Fixed-spec | Merge-spec | ColumnSelection-spec | constant-value | PathInput-spec}
    where_key: str | None,
    distribute: bool,
    as_table,
) -> dict:
    """Return the same dict ForEachConfig.to_version_keys() produces.
    MATLAB ships a JSON-friendly description of inputs; Python returns
    the canonical version_keys dict."""
```

In `+scidb/for_each.m`, replace `build_config_nv`, `serialize_loadable_inputs`, `input_spec_to_key`, `format_repr`, `build_config_nv` (~150 lines) with a single call to this entry.

**2.2 Replace MATLAB PathInput discovery in scidb.for_each**

Add bridge entry that calls Python's PathInput discovery + user-value filtering, returning the filtered combo list as a list of dicts. Delete the ~80-line block in `+scidb/for_each.m` (lines 135–211: `has_pathinput`, `find_pathinput`, the inline filter loop).

**2.3 Reduce `+scifor/PathInput.m` to a thin handle**

Rewrite the classdef so MATLAB's `scifor.PathInput(template, root_folder=...)` constructs a Python `PathInput` instance and stores it. `.load(...)` and `.discover()` become one-line bridge calls. The MATLAB-side `discover_walk`, `segment_to_regex`, the manual regex translation — all deleted.

**Phase 2 acceptance:**
- Tests in `tests/matlab/scifor/TestPathInput.m` and any scidb tests using PathInput still pass.
- A dummy call exercising version-key building produces byte-identical `__inputs` / `__constants` JSON to the previous run on the same inputs.

## Phase 3 — Delegate the loop and save (the big one)

Goal: MATLAB's `+scidb/for_each.m` shrinks to ~80–120 lines; Python owns the **prepare** (Steps 1–13) and **save** (Step 19) halves of the scidb pipeline. The MATLAB inner loop is the existing `+scifor/for_each.m`, called once with the prepared data.

### Runtime constraint: callback design is not viable

The original sketch envisioned a single Python entry (`for_each_run`) that calls back into MATLAB once per combo. **This does not work in MATLAB's embedded Python interpreter** (the in-process `py.module.func()` model used everywhere else in this codebase): when MATLAB calls into Python, Python runs synchronously inside MATLAB's call stack and has no facility to dispatch a MATLAB function and wait for the result. (`matlab.engine`, the out-of-process model, supports bidirectional flow but is a different runtime that the rest of the codebase doesn't use.)

A Python→MATLAB callback would require either out-of-process `matlab.engine` or a custom message-passing layer. Both are out of scope for this redesign.

### Two-pass design (forced alternative, same correctness, fewer crossings)

Instead, split scidb.for_each's Python work into **prepare** and **save**, with MATLAB's existing `+scifor/for_each.m` as the inner loop between them. `+scifor/for_each.m` already supports `_all_combos=...` to override its Cartesian-product step, which is the exact seam needed for variant-expanded combos coming from Python.

```
Call #1 — MATLAB → Python: for_each_prepare(...)
  Python runs scidb.for_each's pre-loop work end-to-end:
    - resolve metadata defaults (DB lookups for `key=[]`)
    - PathInput discovery (already delegated in Phase 2)
    - load each input as a DataFrame
    - __rid_* variant expansion → pre-expanded combo list
    - per-combo upstream/save-metadata bookkeeping
  Returns:
    - handle (int) — server-side cache key for the save call
    - inputs_for_scifor — dict of {name: DataFrame} the bridge converts to MATLAB tables
    - all_combos — list of combo dicts (one per expanded variant)
    - output_class_names, config_keys, etc.

MATLAB inner loop (uses the EXISTING +scifor/for_each.m unchanged):
  scifor.for_each(user_fn, scifor_inputs, ...
                  '_all_combos', all_combos, ...
                  meta_kv{:})

Call #2 — MATLAB → Python: for_each_save(handle, results, ...)
  Python looks up handle, runs scidb.for_each's save step:
  branch_params, __upstream, LineageFcnResult callback into scihist.
  Returns the result DataFrame for MATLAB to convert to a table.
```

This is the same prepare/save logic Python already runs internally inside `scidb.for_each` — just factored into two seam functions (`_for_each_prepare`, `_for_each_save_resolved`) the bridge can call separately. The existing pure-Python `scidb.for_each` becomes a thin wrapper that calls both back-to-back with a Python `for` loop in between. Zero drift risk between the two paths.

**3.1a Factor `scidb.for_each` into seam functions**

In `scidb/foreach.py`:

```python
def _for_each_prepare(fn, inputs, outputs, *, db=None, where=None,
                     distribute=False, as_table=None,
                     metadata_iterables=None,
                     dry_run=False) -> _ResolvedForEach:
    """Run scidb.for_each's pre-loop work and return a state object
    carrying everything _for_each_save_resolved needs."""

def _for_each_save_resolved(resolved, results_per_combo, *, save=True) -> pd.DataFrame:
    """Save results with branch_params/__upstream/lineage. Returns the
    result DataFrame previously returned by scidb.for_each."""
```

`scidb.for_each` becomes:

```python
def for_each(fn, inputs, outputs, **kwargs):
    resolved = _for_each_prepare(fn, inputs, outputs, **kwargs)
    results = [fn(**combo_kwargs) for combo_kwargs in resolved.combo_kwargs_iter()]
    return _for_each_save_resolved(resolved, results)
```

**3.1b Bridge entries**

```python
def for_each_prepare(fn_name, fn_hash, inputs_spec, output_class_names,
                    metadata_iterables, *, where=None, distribute=False,
                    as_table=None, db=None) -> dict:
    """Reconstruct Python wrappers from kind-tagged inputs_spec, call
    _for_each_prepare, return a MATLAB-friendly dict:
        {
          handle: int,
          inputs_for_scifor: dict[name -> DataFrame],
          all_combos: list[dict],
          ...
        }"""

def for_each_save(handle, results_per_combo) -> pd.DataFrame:
    """Look up handle, call _for_each_save_resolved, return result df."""
```

The MATLAB `fn` is passed by name+hash only (no Python callable). Python's prepare doesn't need to call it; MATLAB's scifor loop runs it.

**Crossing budget:** two bridge crossings per `scidb.for_each` call total, regardless of combo count. The original callback design would have been N+1 crossings.

**3.2 Rewrite `+scidb/for_each.m`**

Approximate target structure (~120 lines):

```matlab
function result_tbl = for_each(fn, inputs, outputs, varargin)
    [meta_args, opts] = split_options(varargin{:});

    % Phase 1.1: function name + hash
    [fn_name, fn_hash] = resolve_fn_name_and_hash(fn, opts);

    % Phase 2.1: kind-tagged inputs spec (built by describe_input_for_python)
    inputs_spec = build_inputs_spec(inputs);

    % Metadata iterables as a Python dict
    meta_dict = build_meta_dict(meta_args);

    % Output class names
    output_class_names = cellfun(@class, outputs, 'UniformOutput', false);

    % Call #1: Python prepare → loaded tables + expanded combos
    prep = py.sci_matlab.bridge.for_each_prepare( ...
        fn_name, fn_hash, inputs_spec, ...
        py.list(output_class_names), meta_dict, ...
        pyargs('where', opts.where, 'distribute', opts.distribute, ...
               'as_table', opts.as_table, 'db', opts.db));

    handle = int64(prep{'handle'});

    % Convert returned per-input DataFrames to MATLAB tables for scifor
    scifor_inputs = build_scifor_inputs_from_prep(prep);

    % Convert all_combos list[dict] → MATLAB cell-of-structs
    all_combos = py_combos_to_matlab(prep{'all_combos'});

    % MATLAB inner loop — existing +scifor/for_each.m
    n_out = max(numel(outputs), 1);
    result_tables = cell(1, n_out);
    [result_tables{1:n_out}] = scifor.for_each(fn, scifor_inputs, ...
        '_all_combos', all_combos, ...
        '_nest_table_outputs', true, ...
        meta_args{:});

    % Collect results into one list per combo (for the save call)
    results_per_combo = collect_results_for_save(result_tables, output_class_names);

    % Call #2: Python save
    py_result_df = py.sci_matlab.bridge.for_each_save(handle, results_per_combo);
    result_tbl = scidb.internal.from_python(py_result_df);
end
```

**3.3 Delete from `+scidb/for_each.m`:**
- `lineage_results_to_table` and helpers
- `flatten_nested_table_outputs`
- `try_fast_batch_save`
- `save_results`, `format_save_meta`, `build_row_group_keys`
- `strip_internal_meta`
- `propagate_schema` (Phase 1.3 made this a no-op)
- `convert_input` (Python's `_convert_inputs` now owns this)
- `run_parallel` and all its helpers (`preloaded_results`, `preloaded_maps`, `preloaded_keys`, `result_meta_key`, `combo_meta_key`, `build_meta_key`, `filter_table_for_combo_simple`, `resolve_as_table_set`)
- `build_config_nv` and helpers (already removed in Phase 2.1)
- `has_pathinput`, `find_pathinput` (already removed in Phase 2.2)

**3.4 Delete from `bridge.py`:**
- `for_each_batch_save`
- `for_each_batch_save_dataframe`

These existed only as a MATLAB-driven-save workaround; Python-driven save makes them dead code.

**3.5 Keep in `bridge.py`:**
- `MatlabLineageFcn`, `MatlabLineageFcnInvocation` (used by `scidb.LineageFcn` directly from MATLAB user code; the `for_each_save` path that handles LineageFcnResult outputs also reuses them)
- `make_lineage_fcn_result`, `check_cache` (LineageFcn cache check)
- `register_matlab_variable`, `get_surrogate_class`, `get_data_column_name`
- `wrap_batch_bridge`, `load_and_extract`, `get_batch_item`, `get_batch_data_item`, `free_batch`, `_cache_batch` (the `Type().load(...)` / `Type().load_all(...)` paths)
- `save_batch_bridge` (used by `BaseVariable.save` from MATLAB user code outside `for_each` — not a `for_each` artifact)
- `split_flat_to_lists` (used by `to_python` cell-column fast path)

**Phase 3 acceptance:**
- All existing MATLAB tests pass.
- A new test verifies that MATLAB-driven `scidb.for_each` correctly creates separate output records for each upstream variant (the `__rid_*` correctness gap from the friction analysis).
- A new test verifies that branch_params and `__upstream` are populated on outputs of MATLAB-driven runs.
- Wall-clock time on a representative test pipeline is ≤ 110% of the current implementation. (Some slowdown is acceptable in exchange for correctness; the two-pass design has only two bridge crossings per `for_each` call, so crossing overhead should be lower than today's per-row save crossings.)

## Phase 4 — scihist + skip_computed

Goal: MATLAB callers get the full scihist feature set including `skip_computed`.

**4.1 Rewrite `+scihist/for_each.m`**

```matlab
function result_tbl = for_each(fn, inputs, outputs, varargin)
    % Auto-wrap in LineageFcn (existing behavior)
    if isa(fn, 'scidb.LineageFcn')
        lineage_obj = fn;
    else
        lineage_obj = scidb.LineageFcn(fn);
    end

    % Delegate to Python scihist.for_each (NOT MATLAB scidb.for_each)
    % Python scihist auto-builds the skip_computed hook and calls
    % through to scidb's loop with the lineage callback wired in.
    result_tbl = py_scihist_for_each_bridge(lineage_obj, inputs, outputs, varargin{:});
end
```

The bridge entry parallels `for_each_prepare`/`for_each_save` but internally delegates to `scihist.for_each` (which builds the skip_computed hook from the inputs and Python `LineageFcn`/`MatlabLineageFcn`). With the two-pass design from Phase 3, `skip_computed` integrates naturally: `for_each_prepare` runs scihist's pre-combo hook for each expanded combo and attaches a `skip: bool` (plus `cached_value` when relevant) to each combo record. The MATLAB loop skips the user-fn call for combos flagged `skip=True` and passes the cached value through; `for_each_save` records lineage as if the combo had been freshly computed.

**4.2 Add bridge entry: pre-combo skip hook for MATLAB lineage fns**

scihist's `_build_skip_hook` already works for `MatlabLineageFcn` (it queries record IDs and constant hashes via the database, not via the function object). The only gap is `_check_via_lineage`'s function-hash check, which currently excludes MATLAB proxies (`scihist-for-each-internals.md` line 396). Decide:

- **Option A:** Apply the function-hash check to MATLAB proxies too. Requires confirming that MATLAB's source-hash format is stable enough to compare across save and check times. Phase 1.1 centralizes the hash format in Python, which makes this safer.
- **Option B:** Continue excluding MATLAB proxies from the hash check (matches today's behavior). MATLAB users still get input-rid and constant-hash skip checks; only the "fn source changed" check is missing.

Default to Option B in this phase; revisit Option A in a follow-up after Phase 1.1 has bedded in.

**4.3 Expose `skip_computed` parameter from MATLAB**

The `+scihist/for_each.m` rewrite accepts `skip_computed` as a name-value option (default `true` to match Python). Document it.

**Phase 4 acceptance:**
- A new test verifies that running MATLAB `scihist.for_each` twice in a row with no changes skips all combos on the second run.
- A new test verifies that editing a constant in the inputs causes the second run to recompute.
- A new test verifies that re-saving an upstream input causes downstream `skip_computed` to recompute.

## Phase 5 — `+scifor` cleanup

Goal: keep standalone `scifor.for_each` working, drop only the wrappers whose sole consumer was the old MATLAB scidb path.

**5.1 Audit usage of `+scifor/Fixed.m`, `Merge.m`, `ColumnSelection.m`, `ColName.m`**

Search the test suite and any in-tree user code for direct construction of these MATLAB classes. The standalone `+scifor/for_each.m` test files (`TestSciforForEach*.m`, `TestSciforForEachFeatures.m`, etc.) presumably exercise them.

- If standalone tests use them: **keep** all four files. They're part of the public standalone API.
- If only the old MATLAB scidb path used them (as adapters from `+scidb/Fixed` etc.): delete.

Default assumption: standalone use is real and these classes stay.

**5.2 Delete `convert_input`-style adapters**

The conversion from `scidb.Fixed` → `scifor.Fixed` (with a loaded DataFrame inside) was MATLAB-side adapter code. After Phase 3, Python owns this conversion (Python's `_convert_inputs` does it directly between Python wrappers). The MATLAB-side adapters (the `convert_input` function in the old `+scidb/for_each.m`, plus any helper that wraps a loaded table in `scifor.Fixed`) are deleted.

**5.3 Document the boundary**

Update `+scifor/for_each.m`'s help text: "for standalone use on MATLAB tables; for DB-backed iteration, use `scidb.for_each` or `scihist.for_each`."

**Phase 5 acceptance:**
- All `tests/matlab/scifor/*` tests still pass.
- Public standalone API of `+scifor` is unchanged.

## Phase ordering / dependencies

```
Phase 1 (foundation)
   ├── 1.1 MATLAB fn hashing in Python
   ├── 1.2 DataFrame surfacing bridge entry
   └── 1.3 Schema forwarding
   ↓
Phase 2 (version-keys + path discovery)  ← needs 1.1
   ├── 2.1 Version-key bridge entry
   ├── 2.2 PathInput discovery delegation
   └── 2.3 Thin PathInput classdef
   ↓
Phase 3 (loop + save delegation)         ← needs 1.2, 1.3, 2.1
   ├── 3.1 for_each_run bridge entry
   ├── 3.2 Rewrite +scidb/for_each.m
   ├── 3.3 Delete MATLAB save/conv helpers
   └── 3.4 Delete bridge save entries
   ↓
Phase 4 (scihist + skip_computed)        ← needs 3.1
   ├── 4.1 Rewrite +scihist/for_each.m
   ├── 4.2 Skip hook bridge entry
   └── 4.3 Expose skip_computed
   ↓
Phase 5 (+scifor cleanup)                ← needs 3.x complete
```

Each phase is mergeable independently. Phases 1–2 are non-breaking. Phase 3 is the highest-risk change but also the one that delivers correctness fixes (`__rid_*`, branch_params, lineage callback). Phase 4 unlocks `skip_computed`.

## Diagnostics to add along the way

Per CLAUDE.md, when something goes wrong we should be able to observe internals:

- Phase 1.1: log MATLAB fn hash + name on every `LineageFcn` construction (already partially done).
- Phase 3.1: log every callback crossing (combo metadata + duration). This makes it easy to see if per-combo crossings are dominating wall time.
- Phase 3.1: log the round-trip data shape (input kwargs sizes, output size) on the first crossing per `for_each` call, to catch unintentionally large objects.
- Phase 4.1: log every `skip_computed` decision with the reason (`[skip]` or `[recompute] — <reason>`), matching Python's existing format.

## Tests to add

The redesign is a behavior-preserving rewrite for the most part, but it also FIXES correctness bugs. Each fix needs a regression test:

1. **`__rid_*` variant separation (Phase 3):** save FilteredEMG with `low_hz=20`, then with `low_hz=50`, then run a downstream MATLAB `for_each` consuming FilteredEMG. Assert two separate downstream output records exist (one per upstream variant), with distinct `record_id`s and the correct `__rid_signal` and `branch_params` values.

2. **branch_params accumulation (Phase 3):** run a 3-step MATLAB pipeline. Assert the leaf records' `branch_params` contains entries from all three upstream functions, namespaced by function name.

3. **`__upstream` tracking (Phase 3):** run a MATLAB `for_each` and inspect the saved record's `version_keys.__upstream`. Assert it contains the upstream record IDs.

4. **`skip_computed` first run (Phase 4):** run, assert all combos computed.

5. **`skip_computed` second run no-change (Phase 4):** re-run, assert all combos skipped.

6. **`skip_computed` constant change (Phase 4):** change a constant, re-run, assert all combos recomputed.

7. **`skip_computed` upstream re-save (Phase 4):** re-save one upstream record, re-run, assert only the affected combo recomputes.

8. **Cross-language pipeline (sanity):** a Python `for_each` produces FilteredEMG, a MATLAB `for_each` consumes it, asserts variant tracking still works across the language boundary.

## Open questions to resolve before starting

1. **3.1 callback crossing budget:** is one Python-side closure call per combo acceptable, or do we need to batch combo data in one crossing? Decide by measuring on a representative pipeline at the end of Phase 3 — only redesign if measurements show >1.5× slowdown.
2. **4.2 MATLAB fn-hash skip check:** Option A vs B (apply hash check to MATLAB proxies or not). Default Option B; revisit later.
3. **Phase 5.1 audit result:** which of `scifor.Fixed/Merge/ColumnSelection/ColName` are exercised by standalone tests? If all four are, all four stay.

## Out of scope

- The Python-side friction points still flagged as DEFERRED in `docs/claude/layer-friction-analysis.md` (triple constant storage, input classification quirk, global schema state in Python). Those are Python-only concerns; this plan does not address them.
- GUI integration changes. The GUI's pipeline-status checks rely on `_for_each_expected` and `check_node_state`, both of which are already populated by Python and continue to be populated correctly in this redesign.
- Performance optimizations beyond preserving today's behavior. Once the redesign settles, separate work can profile and optimize the most expensive crossings if needed.
