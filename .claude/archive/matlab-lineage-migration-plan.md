# Bring MATLAB (`scimatlab`) in line with the lineage-simplification migration

## Context

The Python side completed the lineage-simplification migration (branch `dev-hist`):
`@lineage_fcn` / `LineageFcnResult` / the per-call rerun cache / the `_lineage`
table were removed in favor of `@pipeline` + a single bipartite provenance graph.
Lineage is now tracked **automatically** by `scidb.for_each`'s batch save path —
there is no per-call lineage wrapper anymore. `_record_metadata` was slimmed and
renamed to `_record_save` (no `version_keys`/`branch_params`/`lineage_hash`
columns; `excluded` moved to `_record`), and the `lineage_hash` attribute is gone
from the Python `BaseVariable`.

The **Python bridge (`scimatlab/src/scimatlab/bridge.py`) is already migrated** —
it removed `MatlabLineageFcnInvocation` / `make_lineage_fcn_result` and keeps only
`MatlabLineageFcn` (a lightweight identity proxy used solely for `check_node_state`
node coloring). But the **MATLAB `.m` side was never updated** and is now broken at
runtime: it calls removed bridge functions (`MatlabLineageFcnInvocation`,
`make_lineage_fcn_result`, `py.scilineage.configure_backend`), reads a removed
attribute (`py_var.lineage_hash`) and a removed bulk key (`lineage_hashes`), and
DELETEs from the renamed `_record_metadata` table.

Guiding principle (per user + CLAUDE.md): **delegate to existing Python machinery;
do not re-implement behavior in MATLAB.** The `for_each` batch path already records
the graph correctly through the bridge; the MATLAB work is to *delete the dead
per-call lineage layer* and *fix the stale table/attribute references*, not to add
new logic.

### Decisions (confirmed with user)
1. **Delete** `scidb.LineageFcn` / `scidb.LineageFcnResult` entirely (parity with
   Python's outright deletion). Lineage flows only through `scidb.for_each`.
2. **`+scihist` MATLAB package = thin shim** (parity with the Python `scihist`
   deprecated shim): `for_each` → `scidb.for_each`, `configure_database` →
   `scidb.configure_database`.
3. **Tests**: delete the 3 pure-LineageFcn test files; migrate the
   provenance/end-to-end graph coverage to `scidb.for_each`-driven tests asserting
   on the provenance graph (not `lineage_hash`).

---

## Changes

### A. Delete the dead per-call lineage classes
- **Delete** `src/scimatlab/matlab/+scidb/LineageFcn.m`
- **Delete** `src/scimatlab/matlab/+scidb/LineageFcnResult.m`

### B. `+scidb/BaseVariable.m` — drop `lineage_hash` + `LineageFcnResult` routing, fix table name
- Remove the `lineage_hash` property (line ~31) and its doc (line ~22).
- `clear()` (line ~115): `DELETE FROM _record_metadata` → `DELETE FROM _record_save`.
  Also fix the comment (line ~97) `_record_metadata` → `_record_save`.
  > Note: `_record_save` is keyed by `record_id` only (no `variable_name` column).
  > The current DELETE filters `WHERE variable_name = '<Type>_data'`, which no
  > longer exists on `_record_save`. Correct approach: delete `_record_save` rows
  > whose `record_id` belongs to this type, e.g. via a subquery against `_record`
  > (`DELETE FROM _record_save WHERE record_id IN (SELECT record_id FROM _record
  > WHERE type = '<Type>')`), and also clean `_record` (and graph rows) for the
  > type. Confirm exact cleanup scope against `scidb`'s own teardown (reuse a
  > Python DB helper if one exists rather than hand-rolling SQL — check
  > `database.py` for a `drop`/`clear`-type method to delegate to).
- `save()` (lines ~167–173): remove the `isa(data,'scidb.LineageFcnResult')` branch
  that routes to `py.scihist.foreach.save`. Update the DATA doc (line ~127) to drop
  the `LineageFcnResult` mention.
- `wrap_py_var()` (lines ~911–914): remove the `py_var.lineage_hash` read.
- `wrap_py_vars_batch()` (lines ~953, ~1124–1126): remove the
  `bulk{'lineage_hashes'}` read and the per-record `lineage_hash` assignment.
  (Bridge `wrap_batch_bridge` no longer returns `lineage_hashes`.)

### C. `+scidb/for_each.m` — drop LineageFcn / LineageFcnResult handling
- Remove the `elseif isa(fn,'scidb.LineageFcn')` branch (lines ~57–59); `fn` is
  always a plain function handle now.
- Remove the LineageFcnResult→`.py_obj` swap loop (lines ~302–319) — outputs are
  plain values; the batch save path records the graph.
- Fix comments referencing LineageFcnResult routing (lines ~15, ~284–285).

### D. `+scifor/for_each.m` — drop LineageFcnResult collapse
- Remove the `isa(res,'scidb.LineageFcnResult')` collapse (lines ~933–939). For
  `for_columns`, `res` is now always a raw value.

### E. `+scidb/+internal/` helpers — drop LineageFcnResult cases
- `to_python_input.m`: remove the `LineageFcnResult` branch; keep the
  `BaseVariable` → `.py_obj` branch. Update header/comment (drop `classify_inputs`
  reference).
- `unwrap_input.m`: remove `LineageFcnResult` from the `isa(...)` guard and doc;
  keep `BaseVariable` handling.
- `to_python.m`: comment-only — drop the `LineageFcnResult` example (lines ~10–11).
  The generic `py.object` passthrough stays.
- `+internal/hash_function.m`: comment-only — the error message names
  `scidb.LineageFcn`; reword to reference `scidb.for_each` (anonymous functions
  still unsupported for hashing).

### F. `+scihist/` — reduce to thin shims (parity with Python `scihist`)
- `configure_database.m`: delete the `py.scilineage.configure_backend(db)` call
  (function removed). Body becomes a straight delegate to
  `scidb.configure_database`. Update doc (drop "lineage backend"/cache wording).
- `for_each.m`: remove the `scidb.LineageFcn` auto-wrap. Delegate to
  `scidb.for_each` directly (lineage is tracked there automatically). Preserve the
  real function name/hash forwarding via the existing `_fn_name` / `_fn_hash`
  options that `scidb.for_each` already accepts. Mirror the Python shim's
  `skip_computed=true` default if `scidb.for_each` exposes that option (confirm the
  option name during implementation; if MATLAB `scidb.for_each` has no
  `skip_computed`, just delegate and note it).

### G. `+scidb/raw_sql.m` — doc fix
- Comment says the SQL fragment is joined with `_record_metadata`; update to
  `_record_save` / the data view (the actual join in `scidb.filters.raw_sql` is
  `_record_save` JOIN `_record` — see `scidb/src/scidb/filters.py:160`). Code is
  unchanged (delegates to `py.scidb.filters.raw_sql`).

### H. Tests
- **Delete** `tests/matlab/scihist/TestLineageFcn.m`,
  `TestForEachWithLineageFcn.m`, `TestSaveLoadWithLineageFcn.m` (pure per-call
  LineageFcn coverage; equivalent for_each lineage is already covered under
  `tests/matlab/scidb/`).
- **Rewrite** `tests/matlab/scihist/TestProvenance.m` and `TestEndToEnd.m`:
  produce data via `scidb.for_each` (or the `scihist.for_each` shim) and assert on
  the provenance graph via `Type().provenance(...)` (`function_name`,
  `function_hash`, `inputs`, `constants` — all graph-derived through
  `get_provenance`). Drop every `lineage_hash` assertion and every
  `verifyClass(...,'scidb.LineageFcnResult')`. Keep the multi-output / chained-step
  scenarios but express them as `for_each` pipelines.
- `tests/matlab/scidb/TestSaveLoad.m`: delete
  `test_raw_data_has_empty_lineage_hash` (line ~215); the property is gone.
- `tests/matlab/scidb/TestForEachWhere.m` (lines ~837–956): the three checks query
  `SELECT version_keys FROM _record_metadata` and assert `__where` appears in
  `version_keys`. Both the table and the concept are gone — `where=` is **no longer
  part of identity** (it's display-only on `_run.where_clause`). Rewrite these
  assertions to verify the *behavior* `where=` now guarantees: that the filter
  selected the expected variants/rows (assert on loaded results), and/or that the
  issued filter is recorded on `_run.where_clause` (query `_run` via
  `scidb.raw_sql`/the DB handle, or via the execution-audit read path). Remove the
  "`__where` in version_keys" expectations outright.

### I. Docs
- `scimatlab/README.md` Architecture section is stale (describes
  `MatlabLineageFcn`/`MatlabLineageFcnInvocation`/`make_lineage_fcn_result`,
  `find_by_lineage`, `classify_inputs`, `compute_lineage_hash`, `extract_lineage`,
  the LineageFcn quick-start, and the `LineageFcn`/`LineageFcnResult` API rows).
  Update to describe: lineage via `scidb.for_each` → bridge `for_each_prepare`/
  `for_each_save` → bipartite graph; `provenance()` reads the graph;
  `MatlabLineageFcn` is only a node-coloring proxy. Remove the LineageFcn rows.

---

## Out of scope
- No changes to `bridge.py` or any Python package (already migrated).
- scidb-net and scistack-gui (separate TODO clusters in `database-model.md` §11).

## Verification
No Python/MATLAB runtime in this environment — the **user runs the suites**. After
edits:
1. **Static sweep** (assistant can run): re-grep the `.m` tree for
   `LineageFcn|LineageFcnResult|lineage_hash|lineage_hashes|_record_metadata|
   make_lineage_fcn_result|configure_backend|MatlabLineageFcnInvocation` and
   confirm zero matches except intended historical comments.
2. **MATLAB tests** (user): run `tests/matlab/run_tests.m`, focusing on
   `scidb/` (save/load, for_each, for_each_where, to_csv, merge) and the rewritten
   `scihist/` (provenance, end-to-end). Expect green.
3. **Cross-language smoke** (user): a MATLAB `scidb.for_each` run followed by
   `Type().provenance(...)` should return a populated `function_name`/`inputs`/
   `constants` struct sourced from the bipartite graph; `Type().load(...)` records
   should have no `lineage_hash` field.
4. Offer to write a `docs/claude/` note capturing the MATLAB-layer lineage model
   post-migration (per CLAUDE.md convention).
