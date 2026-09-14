# Bridge `EachOf` to MATLAB so a `PathInput` can span two on-disk locations

## Context

Same pipeline, two projects, GAITRite data. Project 1's single `PathInput`
discovers everything under one root. Project 2 splits GAITRite data across
two locations (assessment days vs. training days), and the shared pipeline
internals can't change — project 1 must keep working untouched.

My first pass at this plan recommended `scifor.EachOf(PathInput(...),
PathInput(...))` as the input value for the GAITRite parameter — `EachOf`
already implements exactly the "treat as separate calls, concatenate
results" semantics you described, in pure Python (`scifor/src/scifor/each_of.py`,
expansion logic at the top of both `scifor/src/scifor/foreach.py` and
`scidb/src/scidb/foreach.py`). That part of the analysis holds.

**What that pass missed:** your pipeline runs through MATLAB, and `EachOf`
was never bridged there. You confirmed this directly — `MATLAB is unable to
resolve the name 'scifor.EachOf'`. There is no `+scifor/EachOf.m` (or
`+scidb/EachOf.m`), and neither `+scifor/for_each.m` nor `+scidb/for_each.m`
knows how to expand one. This is a real, currently-missing feature in the
MATLAB layer, not a usage question — confirmed by checking every other
modifier class (`Fixed`, `Merge`, `ColumnSelection`, `ColName`, `PathInput`,
`PathOutput`, `Variant`, `AcrossVariants`) which all *do* have MATLAB
classdefs under `+scifor`/`+scidb`, and by grepping
`scimatlab/tests/matlab` for any `EachOf` coverage (none).

## Design: mirror Python's existing dual-layer pattern in MATLAB

Per `docs/claude/each-of-variant-expansion.md`, Python already has **two**
independent `EachOf`-expansion implementations, by necessity:

- `scifor/src/scifor/foreach.py` — simple: expand, recurse, `pd.concat`. No DB
  semantics.
- `scidb/src/scidb/foreach.py` — its own expansion (can't delegate to scifor's)
  because each branch needs independent `save`/`skip_computed`/lineage
  handling, which scifor's pure loop doesn't have.

MATLAB should get the same two, for the same reason, reusing existing
per-layer helpers rather than inventing new ones:

- `+scifor/EachOf.m` — a plain builder (mirrors `+scifor/Fixed.m`'s shape:
  `properties (SetAccess = private) alternatives`, constructor validates
  non-empty). Lives only in `+scifor` (not duplicated into `+scidb`) —
  matching the Python precedent where `scidb.EachOf` is just a re-export, and
  matching how `PathInput`/`PathOutput`/`ColumnSelection` already work
  cross-namespace (`+scidb/for_each.m`'s `describe_input_for_python` already
  does `isa(val, 'scifor.PathInput')` checks on scifor-namespaced classes).

- **`+scidb/for_each.m` recursion** (primary — this is what your GAITRite
  pipeline actually calls): insert a new Step 0, *before* `describe_input_for_python`
  is called on anything (mirrors Python: "must be first, before any other
  logic"), around line 119 in `scimatlab/src/scimatlab/matlab/+scidb/for_each.m`:
  - Scan `fieldnames(inputs)` for any field where `isa(inputs.(name), 'scifor.EachOf')`.
  - Build the cartesian product of alternative-axes with the existing
    `scidb.internal.cartesian_product` helper (already used for metadata
    combos at `+scifor/for_each.m:459` — reuse it instead of writing a second
    combinatorics routine).
  - For each combo: copy `inputs`, substitute the concrete alternative into
    each varying field, recursively call `scidb.for_each(fn, concrete_inputs,
    outputs, varargin{:})` (same varargin, unchanged — exactly like Python
    passes `**metadata_iterables` through unchanged).
  - `vertcat` each branch's `result_tbl` into the final result; return early,
    skipping the rest of the function (same shape as the Python `if
    each_of_axes: ... return`).
  - Log via `scidb.Log.debug`, mirroring the existing Python log lines
    ("EachOf expansion detected - N axes...") for parity and diagnosability.

- **`+scifor/for_each.m` recursion** (secondary, for standalone parity —
  cheap to add alongside the above and keeps the two layers in sync the way
  Python's do): same idea near the top of `scimatlab/src/scimatlab/matlab/+scifor/for_each.m`,
  but concatenating `varargout` per output index across branches (scifor
  returns multiple tables via `varargout`, unlike scidb's single `result_tbl`).

## Hard constraint this introduces: matching columns across branches

Python's `pd.concat` unions mismatched columns with NaN. MATLAB's `vertcat`
on tables has no such leniency — it **errors** if the two tables don't have
identical variable names. Practical consequence for your two GAITRite
templates: **both must resolve to the same schema-key placeholder names**
(e.g. both use `{subject}`/`{session}`, even though the literal folders and
`root_folder` differ) — not just a style recommendation as I originally
framed it for the Python case, but a hard requirement here. The plan includes
a clear error message (not a cryptic MATLAB `vertcat` error) when branch
tables disagree on columns, and a test locking that message in.

Also still worth confirming on your end (unchanged from the original
analysis): the two locations shouldn't be able to produce the *same*
`(subject, session)` combo, or `scidb.for_each`'s save step will write two
records under one identity.

## Work items

1. **`scimatlab/src/scimatlab/matlab/+scifor/EachOf.m`** — new classdef,
   mirrors `+scifor/Fixed.m`.
2. **`+scidb/for_each.m`** — Step 0 recursion as described above (primary;
   this unblocks your GAITRite pipeline).
3. **`+scifor/for_each.m`** — Step 0 recursion, `varargout`-aware (secondary,
   same PR for consistency with Python's dual implementation).
4. **Column-mismatch guard + clear error** in the `vertcat` step of both.
5. **Tests** — new `TestEachOf.m` in both `scimatlab/tests/matlab/scifor/`
   and `scimatlab/tests/matlab/scidb/` (naming matches existing
   `TestPathInput.m` / `TestForEach*.m`). Cover:
   - Two `PathInput`s over two real temp directories (MATLAB `tempname`/`mkdir`),
     same placeholder keys, discovery running per-branch, concatenated result
     containing rows from both.
   - Mismatched placeholder keys across the two `PathInput`s → the new clear
     error, not a raw `vertcat` failure.
   - `scidb.for_each` end-to-end with `save=true`, confirming distinct
     records land for each location under distinct schema-key combos
     (mirrors the Python-side `scidb/tests/test_each_of.py` pattern).
6. **Docs** — extend `docs/claude/each-of-variant-expansion.md`'s
   "Implementation" section with the new MATLAB files, closing the
   Python/MATLAB asymmetry this investigation surfaced (same spirit as
   `docs/claude/pathinput-resolution-split... ` style asymmetry notes already
   in that folder).
7. **Your pipeline config** — in project 2's MATLAB config only, the GAITRite
   input becomes:
   ```matlab
   scidb.for_each(@my_analysis, struct('filepath', ...
       scifor.EachOf( ...
           scifor.PathInput(template, root_folder="root/assessment"), ...
           scifor.PathInput(template, root_folder="root/training") ...
       )), {Output()}, subject=[], session=[]);
   ```
   Project 1's config and the shared analysis function are untouched.

## Verification

MATLAB isn't runnable on my end, so once the code above is written, run:

```matlab
runtests('scimatlab/tests/matlab/scifor/TestSciforEachOf.m')
runtests('scimatlab/tests/matlab/scidb/TestEachOf.m')
runtests('scimatlab/tests/matlab/scidb/TestForEach.m')   % regression check, unrelated paths
```

Then a manual dry run against your real GAITRite folders before wiring it
into the actual analysis function:

```matlab
scidb.for_each(@(filepath) filepath, struct('filepath', ...
    scifor.EachOf( ...
        scifor.PathInput(template, root_folder=assessment_root), ...
        scifor.PathInput(template, root_folder=training_root) ...
    )), {}, dry_run=true, subject=[], session=[]);
```

confirming both locations show up in the dry-run preview with the same
metadata columns before flipping `dry_run` off.

## After this lands

Worth a `docs/claude` note (separate from the work above, can write once this
is confirmed working) capturing that `EachOf` existed in Python for a while
before MATLAB got it, and why — useful the next time a Python-only feature
gap surfaces in the MATLAB bridge.
