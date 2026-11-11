# Plan: PathInput discovery for standalone `scifor.for_each`

## Status: IMPLEMENTED (Python apply_discovery unit tests pass)


## Bug

```
Error using scifor.for_each>distinct_values_from_inputs
Empty list [] was passed for 'subject', but no input DataFrame has that column.
```

User calls `scifor.for_each(@loadGaitRiteOneFile, struct('xlsx_file_path', gaitritePath, ...),
subject=[], session=[], speed=[], distribute=true)` where `gaitritePath` is a `scifor.PathInput`
with template `{subject}/Gaitrite/{session}/{subject}_{session}_GR_{speed}.xlsx`.

## Root cause

- `subject=[]` / `session=[]` / `speed=[]` mean "discover all values". In standalone mode
  (`scifor.for_each` called directly, no DB), `scifor/for_each.m` resolves empty `[]` lists via
  `distinct_values_from_inputs` (line 1371), which scans **table inputs only** (`get_raw_table`
  returns `[]` for a `PathInput`). With no table carrying those columns, it raises.
- The values should come from **filesystem discovery** against the PathInput template placeholders.
- PathInput discovery already exists and is integrated only in the **scidb** layer:
  Python `scidb/foreach.py` Step 3 (`foreach.py:885-955`) runs `PathInput.discover()` and decides
  Case A / Case B. MATLAB `scidb.for_each` inherits this because it gets combos from Python `prepare`.
- `scifor` standalone never got this. MATLAB `scifor.for_each` *does* resolve PathInput paths
  (`resolve_pathinput` defaults true), so the only missing piece is **value discovery**.

## De-duplication strategy (per user guidance)

The Case A/B discovery-orchestration decision currently lives inline in `scidb/foreach.py` Step 3.
Rather than copy it into scifor, **make `PathInput` itself own it** (scifor is the lowest layer and
the natural home for PathInput logic), and have scidb reuse it:

- The filesystem walk (`PathInput.discover` / `placeholder_keys`) is already canonical Python,
  already called from MATLAB via `py_obj`. Keep.
- Add **`PathInput.apply_discovery(metadata_iterables, user_explicit_keys, log=None)`** to
  `scifor/pathinput.py` — extract scidb Step 3's decision verbatim. Returns
  `(metadata_iterables, discovered_combos | None)`.
- scidb Step 3 calls `pi.apply_discovery(...)` instead of its inline block (keeps its own
  Fixed-aware `_find_pathinput`/`_has_pathinput`).
- MATLAB `scifor.for_each` standalone calls the same method through a thin `PathInput.m` wrapper.
  No new copy of the decision logic in MATLAB beyond marshalling.

Net: one implementation of the decision (Python, in scifor.pathinput), reused by scidb Python and
MATLAB scifor. Input-finding (different `Fixed` types per layer) stays per-layer.

## Files

1. **`scifor/src/scifor/pathinput.py`** — add `apply_discovery(self, metadata_iterables,
   user_explicit_keys, *, log=None) -> (dict, list[dict] | None)`. Logic = scidb Step 3:
   - empty `discover()` → return `(metadata_iterables, None)`.
   - Case A (no metadata_iterables): adopt every discovered key/value; `discovered_combos = combos`.
   - Case B: fill empty template keys from disk; if any *user-explicit* template key present,
     `discovered_combos = None` (Cartesian drives it); else `discovered_combos = combos`.
   - `log(msg)` callback for parity with existing scidb logging.

2. **`scidb/src/scidb/foreach.py`** — replace inline Step 3 decision (885-955) with
   `metadata_iterables, _discovered_combos = pi.apply_discovery(metadata_iterables,
   user_explicit_keys, log=Log.info)`. Keep `_has_pathinput`/`_find_pathinput`.

3. **`scimatlab/.../+scifor/PathInput.m`** — add `apply_discovery(obj, metadata_iterables_struct,
   user_explicit_keys)` MATLAB method: marshal struct→py dict / keys→py set, call
   `obj.py_obj.apply_discovery`, convert `(iterables, combos)` back (combos → cell of structs like
   `discover`).

4. **`scimatlab/.../+scifor/for_each.m`** — standalone empty-resolution block (134-145):
   - compute `user_explicit_keys` (keys with non-empty value at call site);
   - first pass: table resolution, **non-raising** (return `{}` instead of error);
   - find a PathInput input (local helper, unwrap `scifor.Fixed`);
   - if present, call `pi.apply_discovery(...)` → fill remaining empty `meta_values`; if it returns
     combos, set `opts.all_combos` to them (projected to `meta_keys`);
   - raise the informative "no values" error only if a key is still empty after both;
   - log via `opts.log_fn` / `fprintf`.

5. **Tests / logging**
   - `scifor/tests/test_pathinput_discover.py` — unit tests for `apply_discovery` (Case A,
     B-all-empty, B-explicit) against a tmp dir.
   - MATLAB `scimatlab/tests/matlab/scifor/` — `scifor.for_each` standalone with a PathInput +
     empty lists discovers combos and runs (tmp dir of files); assert no error + correct rows.
   - Logging added in `apply_discovery` and the scifor standalone path.
   - Existing scidb Step 3 tests (TestForEachSchemaFiltering, test_pathinput_discover) guard the
     refactor.

## Open question for user

After implementing, offer to write `docs/claude/` note on the scifor-vs-scidb PathInput
resolution split (Python scifor pure vs MATLAB scifor resolving; discovery ownership).
