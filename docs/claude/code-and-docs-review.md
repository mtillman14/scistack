# Code and Documentation Review

This file documents bugs, incomplete features, and conceptual issues found during a comprehensive review of the SciStack codebase (February 2026). Issues are organized by severity.

---

## Critical: Debug Output in Production Code

### 1. `database.py` — Debug `print()` statements in `_is_tabular_dict()`

**File:** `src/scidb/database.py`, lines 56–78

The `_is_tabular_dict()` function contains `print()` statements that fire every time data is classified during a save operation. These will appear in every user's console output with no way to suppress them.

```python
# LINES 56-78 — all these should be removed
print(f"    [_is_tabular_dict] FAIL: not dict or empty ...")
print(f"    [_is_tabular_dict] FAIL: key={k!r} is ...")
print(f"    [_is_tabular_dict] OK: {len(data)} keys, ...")
print(f"    [_is_tabular_dict] FAIL: unequal lengths=...")
```

**Fix:** Remove all `print()` calls from `_is_tabular_dict()`.

### 2. `BaseVariable.m` — Debug `fprintf` statements in `save()`

**File:** `sci-matlab/src/sci_matlab/matlab/+scidb/BaseVariable.m`, lines 102–114

The MATLAB `save()` method prints detailed debug info for every save call:

```matlab
fprintf('  [save] py_data class=%s, py_kwargs size=[%s]\n', ...)
fprintf('  [save]   kwarg %s = %s (class=%s)\n', ...)
fprintf('  [save] calling save_variable...\n');
```

This generates significant noise for any user calling `save()` and cannot be disabled.

**Fix:** Remove or comment out these `fprintf` calls.

### 3. `for_each.m` — Debug `fprintf` during preload phase

**File:** `sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m`, line 299

```matlab
fprintf('Bulk preloading variable %s\n', type_name);
```

This prints for every variable type being preloaded, using an inconsistent format (no `[...]` prefix). May be intentional progress output, but the format is inconsistent with `[run]`/`[save]`/`[skip]` messages elsewhere.

**Fix:** Either remove, or change to `fprintf('[preload] %s\n', type_name)` for consistency.

---

## High: Incomplete Implementation

### 4. `BaseVariable.m` — `provenance()` version support is unfinished

**File:** `sci-matlab/src/sci_matlab/matlab/+scidb/BaseVariable.m`, line 451

```matlab
if version ~= "latest"
    disp('what to do with syntax py_kwargs{:}?')
    % py_result = py_db.get_provenance(py_class, version=char(version), pyargs(py_kwargs{:}));
else
    py_result = py_db.get_provenance(py_class, pyargs(py_kwargs{:}));
end
```

The `version` argument to `provenance()` is silently ignored for non-`"latest"` versions. Calling `MyVar().provenance(subject=1, version="abc123...")` will return the latest version's provenance, not the specified version's.

**Fix:** Implement the non-latest branch or raise an explicit `Not implemented` error so users are not silently misled.

### 5. `for_each` caching (`skip_computed`) is not implemented

**File:** `docs/claude/for-each-caching.md`

The design document for `skip_computed=True` exists in `docs/claude/for-each-caching.md`, but the feature has not been implemented in either Python (`scirun-lib/src/scirun/foreach.py`) or MATLAB (`sci-matlab/.../for_each.m`). Users re-running `for_each` always re-execute all iterations, even if outputs already exist in the database.

**Impact:** This is a significant usability gap for large datasets. Without it, users must implement their own existence checks or tolerate redundant computation.

**Fix:** Implement per the design doc, or update the doc to explicitly state the feature is planned but not yet available, and remove it from any user-facing API descriptions until it ships.

---

## Medium: API/Behavior Inconsistencies

### 6. `table_name()` vs `view_name()` — documentation mismatch

**File:** `src/scidb/variable.py`, lines 141–164

The existing `api.md` documentation states that `table_name()` "returns the class name (e.g., `StepLength`)". However, the implementation returns `cls.__name__ + "_data"` (e.g., `"StepLength_data"`). The human-readable SQL view (named exactly `StepLength`) is returned by `view_name()`.

**Impact:** Users inspecting the database directly (DBeaver, DuckDB CLI) will see both `StepLength` (the view) and `StepLength_data` (the underlying table). The distinction between these is never explained in user-facing documentation.

**Fix:** Update the API docs to correctly state that `table_name()` returns `ClassName_data` (the raw storage table) and `view_name()` returns `ClassName` (the human-readable SQL view). Add a note in the browsing guide explaining the view vs. table distinction.

### 7. `load()` return type is inconsistent

**Files:** `src/scidb/variable.py`, `sci-matlab/.../BaseVariable.m`

`load()` returns a single variable when one record matches and a **list** of variables when multiple records match. This means the return type is determined at runtime, which can cause `AttributeError` in user code:

```python
var = StepLength.load(subject=1)  # Returns list if multiple sessions exist!
var.data  # AttributeError: list has no attribute 'data'
```

Users frequently hit this when they expect one record but have multiple sessions/conditions they forgot to specify.

**Impact:** Common runtime error that is confusing to debug.

**Fix (short-term):** Document clearly in API docs. Consider adding a note in the error message.

**Fix (long-term):** Consider adding a `load_one()` method that always returns a single variable and raises `MultipleResultsError` if more than one matches.

### 8. Cross-language thunk caching is not shared

**Files:** `sci-matlab/README.md` (documented), but missing from user-facing docs.

Python and MATLAB thunks do not share cache entries, even for the same function applied to the same data. This is expected behavior (Python uses bytecode hash; MATLAB uses source file hash), but it is never explained in the user-facing documentation.

**Impact:** Users who mix languages in a pipeline will be surprised that switching from Python to MATLAB (or vice versa) for the same processing step causes a full re-computation.

**Fix:** Add a note to the lineage guide and the MATLAB setup page.

---

## Low: Minor Issues

### 9. Typo in `save_from_dataframe()` docstring

**File:** `src/scidb/variable.py`, line ~495

```python
Returns:
    List of record_ides for each saved record  # should be "record_ids"
```

### 10. `Merge` cannot be wrapped in `Fixed` — not documented for users

**Files:** `scirun-lib/src/scirun/foreach.py` (documented in code), but not in user-facing docs.

`Fixed(Merge(...), ...)` raises a `TypeError` with a helpful message, but users have no way to know this limitation from the documentation. The workaround (`Merge(Fixed(...), ...)`) is correct and works fine.

**Fix:** Add a note in the `Merge` and `Fixed` API documentation.

### 11. `MATLAB parallel=True` restrictions not documented

**Files:** `sci-matlab/.../for_each.m` (documented in code), but not in user-facing docs.

`parallel=true` cannot be used with `scidb.Thunk` functions or `PathInput`. The error is raised at runtime with a clear message, but users have no way to know this limitation before running.

**Fix:** Add to the `for_each` API parameter table (now added in the new docs).

### 12. Empty list `[]` for metadata iterables is undocumented in user-facing docs

**Files:** `scirun-lib/src/scirun/foreach.py` (documented in docstring), but not in the user guide or quickstart.

Passing `subject=[]` to `for_each` automatically uses all distinct subject values from the database. This is a very useful feature that is easy to miss.

**Fix:** Add an example to the `for_each` guide (now added).

---

## Documentation Gaps

### 13. The difference between schema keys and version keys is underexplained

The concept that `dataset_schema_keys` defines "location in the dataset" and all other metadata keys are "version keys" (computational variants) is central to how SciStack works. It affects how `load()` returns results, how `list_versions()` groups entries, and how queries work. The existing quickstart mentions it briefly but the implication for `load()` behavior is not spelled out.

**Fix:** Expand the Database guide's schema key section with a worked example showing the same data saved with different version keys, and how `load()` returns the latest version at the schema location.

### 14. No explanation of how caching interacts with `for_each`

`for_each` with `@thunk` functions does cache — but the cache is the thunk's own lineage cache, not a `for_each`-level cache. This means:

- If you use a `@thunk` function with `for_each`, cache hits still print `[run]` and `[save]` lines (the thunk internally returns cached results without executing, but `for_each` doesn't know this).
- The planned `skip_computed` feature (see issue #5) would provide `for_each`-level skipping for non-thunked functions.

This distinction is not explained anywhere.

### 15. The `browsing.md` guide mentions a view column `value` that is only present for native types

**File:** `docs/guide/browsing.md`, line 12

The table shows a `value` column as the stored data. This is accurate for native types (numpy arrays, scalars, etc.), but variables with custom `to_db()` serialization store data in whatever columns `to_db()` returns — there is no `value` column.

**Fix:** Clarify that the table schema depends on whether native serialization or custom `to_db()` is used.
