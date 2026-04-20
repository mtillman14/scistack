# Plan — fix multi-output MATLAB run issues

Run context: `aim2.duckdb`, manual load_csv (MATLAB, n_outputs=3) + Time/Force_Left/Force_Right variables, 15/16 combos succeeded (subject=01/trial=06 failed with "Assertion failed."). After the run: handles reordered, handles relabeled to Variable class names, all combos marked red, and no MATLAB-side db-close log entry.

## Root causes (from scidb.log + source)

### Issue 3 — all successful combos show as "stale: function hash changed (lineage)"

`scistack_gui/api/pipeline.py::_build_matlab_fn_proxy`:
```python
info = matlab_registry.get_matlab_function(fn_name)
unpack = info.n_outputs >= 2
proxy = MatlabLineageFcn(info.source_hash, fn_name, unpack_output=unpack)
```

MATLAB side (`sci-matlab/.../+scihist/for_each.m`) wraps the fn in `scidb.LineageFcn(fn)` with `unpack_output=false` default — so save-time hash uses `unpack_output=false`. Check-time hash in the GUI uses `True` when `n_outputs>=2`. `MatlabLineageFcn.hash = sha256(f"{source_hash}-{unpack_output}")` → different hashes → `scihist.state._check_via_lineage` reports every combo as stale.

Single-output Windows runs worked because there `n_outputs==1` and `unpack_output=False` on both sides.

### Issues 1 & 2 — handle order shuffled, labels become Variable class names

`scistack_gui/domain/graph_builder.py::build_function_nodes` for MATLAB fns:
```python
actual_outputs = fn_outputs.get(fn, set())            # {'Time','Force_Left','Force_Right'}
declared      = matlab_output_order.get(fn, [])        # ['time','force_left','force_right']
ordered = [t for t in declared if t in actual_outputs] # [] — case/underscore mismatch
extras  = sorted(t for t in actual_outputs if t not in declared)  # alphabetical
out_types = ordered + extras                           # ['Force_Left','Force_Right','Time']
```

Exact-string match fails (lowercase param names vs Pascal-case class names) → all class names fall through to the alphabetically-sorted `extras` bucket. `build_edges` then uses those class names for `sourceHandle=out__{class}`. `pipeline_store.graduate_manual_node` rewrites edge `source`/`target` but NOT `source_handle`/`target_handle`, so some stored edges still reference `out__time`.

Pre-existing working normalizer: `matlab_command_service._sort_inferred_by_params_order` already uses `normalize = s.lower().replace("_","")`.

### Issue 4 — DuckDB connection "not closed"

Not a real leak. Log timeline:
- 22:41:14.166 last MATLAB save
- 22:41:16.342 Python `get_pipeline` acquires lock fresh `(reopen)` — would fail if MATLAB still held it

MATLAB's generated command ends with `try ... db.close(); catch ... db.close(); rethrow(...); end` (scistack_gui/api/matlab_command.py:107-111, 200-203). The close runs, but the MATLAB side only emits an "ACQUIRED" log line (`configure_database.m:56`) — no matching RELEASED log. So the user sees "connection not closed" only in the log.

## Proposed fixes

All fixes live in the appropriate scistack layer (per CLAUDE.md "solutions … live in the corresponding scistack layer"). Each fix adds a log line so future drift is visible.

### Fix A — unpack_output semantics + hash coupling

Re-reading `sci-matlab/.../+scidb/LineageFcn.m` (lines 107-178): `unpack_output` does **not** mean "function has multiple outputs." It means:

- `unpack_output=true` → fn returns a single **cell array** whose elements are the outputs (e.g. `function c = fn(); c = {time, force_left, force_right}; end`). `feval(fn, ...)` is called with single-output; cell is iterated.
- `unpack_output=false` + `nargout>1` → fn uses MATLAB's native multi-output via nargout (e.g. `function [time, force_left, force_right] = fn(...)`). Captured by `[results{1:n_out}] = feval(fn, ...)`.

load_csv uses the native multi-output pattern, so `unpack_output=false` IS correct — lineage unpacking still happens via the `n_out>1` branch in `execute_and_wrap`. Flipping MATLAB's default to `true` would make `feval` try to iterate a non-cell-array return and raise `scidb:UnpackError`.

So: **both sides stay at `unpack_output=False`**, and we drop the faulty `n_outputs >= 2` heuristic on the Python side.

`scistack-gui/scistack_gui/api/pipeline.py::_build_matlab_fn_proxy`:
```python
# unpack_output MUST match sci-matlab/.../+scihist/for_each.m's default.
# MATLAB multi-output via nargout (e.g. [a,b,c] = fn(...)) uses unpack_output=False
# and the LineageFcn `n_out>1` branch. unpack_output=True is only for the rarer
# single-cell-array-return pattern, which load_csv etc. do NOT use.
proxy = MatlabLineageFcn(info.source_hash, fn_name, unpack_output=False)
logger.debug(
    "[pipeline] matlab proxy fn=%s source_hash=%s unpack=False hash=%s",
    fn_name, info.source_hash[:12], proxy.hash[:12],
)
```

(No MATLAB-side change; its default is already `false`.)

### Fix B — explicit param↔class mapping from edges (and persisted on variants)

You're right — name normalization is a coincidence here and fails for `output1 → Result`. The mapping must be **explicit from user-created edges** (and, after graduation, from persisted variant rows).

**Source of truth for the mapping, in priority order:**

1. **Persisted manual edges** (`_pipeline_edges` rows). An edge from a manual function node carries both the param name (`source_handle='out__time'`) and the class name (via `target='var__Time__...'` → label `'Time'`). `scistack_gui/domain/edge_resolver.py::infer_manual_fn_output_types` already walks these edges to collect class names — extend it to also return the `source_handle → target_label` pairing.

2. **Persisted variant rows**. Since there are no pre-existing databases, we can add an `output_param_name` column to the variant table (wherever scidb persists `(fn_name, output_type, ...)` variants). Each time MATLAB saves an output, it records position `i` within `nargout` — use that to look up `info.output_names[i]` and persist. Then the param↔class mapping is intrinsic to each variant row and never depends on the edge history.

**Use in graph_builder:**

1. `build_function_nodes` — for MATLAB fns, set `output_types = info.output_names` (signature order, from registry). These are the handle labels the user sees and the handle IDs (`out__{param_name}`) — restoring what the user originally saw when they placed the node. Drop the `ordered + extras` alphabetical fallback entirely.
2. `build_edges` — for each DB variant `(fn='load_csv', output_type='Time', output_param_name='time')`, emit an edge with `source='fn__load_csv'`, `source_handle='out__time'`, `target='var__Time'`. No inference from names.
3. Before building, log the full mapping for each MATLAB fn so future bugs are visible:
   ```python
   logger.debug("[graph_builder] matlab fn=%s param→class mapping=%s (source=%s)",
                fn, mapping, source)  # source in {"persisted-edges", "variant-rows"}
   ```

`FunctionNode.tsx` already renders handles from `output_types` unchanged, so no frontend edits.

**Where the param name enters the system** (persistence hookpoint, `sci-matlab` side):
- `scidb.LineageFcn.make_lineage_fcn_result(py_inv, i-1, py_data)` already carries position `i-1`. We need the consumer of that result (whatever writes the variant row) to also receive the calling `scihist.for_each`'s `outputs{i}` class name and the MATLAB fn's `info.output_names[i]`, and persist both to the variant row. This is a scidb-layer (not GUI) change per CLAUDE.md.

### Fix C — pipeline_store.py: no change needed

With Fix B, both manual and auto-inferred edges use `out__{param_name}` from the start, so `graduate_manual_node` only needs to keep doing what it already does (update `source`/`target`). No migration required — project is still in early development with no pre-existing databases.

### Fix D — sci-matlab: log DuckDB lock RELEASED after close actually succeeds

Right — log should fire post-close, and must handle the case where `db.close()` itself errors (otherwise the log claims a release that didn't happen).

Why no MATLAB-side RELEASED log today: `scidb.Log` writes to the shared `scidb.log` file. Python's `[sciduck] DuckDB lock RELEASED` comes from the GUI-side Python process; MATLAB's embedded Python process (which is where `db.close()` actually runs) uses its own logging handlers that don't share the file. So we need a MATLAB-emitted log line for MATLAB-triggered closes.

Add `sci-matlab/src/sci_matlab/matlab/+scidb/close_database.m`:
```matlab
function close_database(db)
%SCIDB.CLOSE_DATABASE  Close the DuckDB connection with release/error logging.
    db_path = char(db.path);  % capture before close (object may be unusable after)
    try
        db.close();
    catch close_err__
        scidb.Log.error('MATLAB: db.close FAILED for %s: %s', db_path, close_err__.message);
        rethrow(close_err__);
    end
    scidb.Log.info('MATLAB: db.close — DuckDB lock RELEASED: %s', db_path);
end
```

Update `scistack-gui/scistack_gui/api/matlab_command.py` to call `scidb.close_database(db)` in both try and catch branches instead of `db.close()` directly. In the catch, log the for_each error *before* close so the log reads:
```
ERROR MATLAB: for_each FAILED: Assertion failed.
INFO  MATLAB: db.close — DuckDB lock RELEASED: …
```
If close itself errors during cleanup, the close-error log fires and the original for_each error is still rethrown.

Pure observability fix — no behavioral change beyond error visibility.

## Verification

After fixes, re-run the same 16-combo pipeline (or a smaller 2-combo reproducer) and check `scidb.log`:

- `[pipeline] matlab proxy fn=load_csv … hash=<X>` at run time.
- Same hash `<X>` when checking state post-run (i.e. no "function hash changed (lineage)" lines for the 15 successful combos).
- `[graph_builder] matlab fn=load_csv param→var map={'time': 'Time', 'force_left': 'Force_Left', 'force_right': 'Force_Right'}`.
- 15 combos → grey (up_to_date=15, missing=1), variables green.
- `MATLAB: db.close — DuckDB lock RELEASED: …` before the post-run `get_pipeline` acquire.

## Tests to add

- `scistack-gui/tests/test_matlab_fn_proxy.py`: assert `_build_matlab_fn_proxy(...).hash` equals the hash produced by `scidb.LineageFcn(fn).hash` in MATLAB (reproduced in Python as `sha256(f"{source_hash}-False")`).
- `scistack-gui/tests/test_graph_builder_matlab.py`: build a graph for a MATLAB fn whose param names differ only in case/underscores from Variable class names; assert `output_types == info.output_names` and each edge's `sourceHandle` uses the param name.
- `scistack-gui/tests/test_graduate_manual_node.py`: assert edges created from Fix B's builder survive graduation with `source_handle='out__time'` unchanged.

## Out of scope

- The subject=01/trial=06 assertion failure — that's an upstream data issue unrelated to these four bugs.
- DuckDB lock coordination between MATLAB and Python processes — current evidence shows it works, only the logging is incomplete.

## Docs follow-up

After approval + implementation, write `docs/claude/matlab-lineage-hash-coupling.md` covering:
- The `unpack_output` hash-coupling invariant between `sci-matlab/+scihist/for_each.m` and `scistack_gui/api/pipeline.py::_build_matlab_fn_proxy`.
- How MATLAB param names vs Variable class names flow through `output_types` / handle IDs / edges, and where `graduate_manual_node` fits in.
