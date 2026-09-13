# MATLAB run timing + phantom Cartesian combos

**Date:** 2026-09-13
**Trigger:** `loadDelsysEMGOneFile` run took 10.7 min (`[timing] matlab_script: TOTAL=640.792s`)
for 419 saved records, and `scidb.log` carried ~11,200 lines of "file not found" for
combos that were never expected to exist.
**Source logs:** `/workspace/scidb.log`, `/workspace/output.txt` (VS Code Output tab),
run_id `bt46bt9r` (15:06:23 -> 15:17:04). The earlier `01h85hka` at 15:04:04 was
cancelled by the user ~43s in; it is not a failure.

---

## 1. Measured budget (what we actually know)

| Phase | Time | % | Evidence in `scidb.log` |
|---|---|---|---|
| MATLAB preamble | 8.3s | 1% | `matlab_preamble: TOTAL=8.296s (pyenv_preamble=3.479s, addpath=4.665s)` |
| `for_each_prepare` (Python) | 1.4s | <1% | `for_each_prepare returned in 1.381s` |
| prep -> MATLAB marshalling | ~0.8s | <1% | 15:06:33.180 -> 15:06:33.947 |
| iteration loop, 952 combos | 89.4s | 14% | `done in 89.4s: completed=419, failed=533, no_data=0, total=952` |
| MATLAB -> py DataFrames | ~2.6s | <1% | 15:08:03.384 -> 15:08:05.998 |
| `for_each_save` (Python) | 147.2s | 23% | `for_each_save returned in 147.173s` |
| -- `save_batch(RawEMG)` 419 rows | 77.3s | 12% | `save_batch(RawEMG): 419 items ... 77.325s` (5.4 rec/s) |
| -- `record_run` provenance | 67.1s | 10% | `record_run(fn=loadDelsysEMGOneFile): 419 record(s), 1 invocation(s), ... 67.091s` |
| **result marshalling back to MATLAB** | **~385s** | **60%** | **15:10:33.172 -> 15:16:57.945, ZERO log lines** |
| `db.close` | 6.3s | 1% | 15:16:57.945 -> `db.close -- DuckDB lock RELEASED` 15:17:04.256 |

Sums to ~640s, matching `matlab_script: TOTAL=640.792s`.

### The 385s hole

Between `for_each_save returned in 147.173s` and `===== for_each(...) done =====` MATLAB
emits nothing. Only the GUI logs, and only DB-lock retries. Per
`scimatlab/src/scimatlab/matlab/+scidb/for_each.m:628-731` the only unbounded work in
that window is:

```matlab
result_tbl = scidb.internal.from_python(py_result_df);   % line 637
```

`for_each_save` returns the merged DataFrame **still carrying the payload column**
(`[bridge] for_each_save: ... result_tbl shape=(419, 5), columns=['subject','session','speed','trial','RawEMG']`,
each cell a `dict, 10 keys` per the `[save]` lines). `from_python`
(`+scidb/+internal/from_python.m`) recurses element-by-element; every numpy array goes
`ascontiguousarray` -> `tolist()` -> `cell()` -> `cellfun(@double, ...)`. That is roughly
419 x 10 signal arrays crossing the boundary one element at a time.

The generated script discards the value: `scistack_run:143` is
`scihist.for_each(@loadDelsysEMGOneFile, ...)` with no output assignment (visible in
the failure-report stack traces). So ~60% of the run converts data nothing reads.

### Ruled out

GUI lock contention is NOT stealing time from the save. 27 reopen attempts across the
148s save window, ~1 per 5.5s. Noise, not a cause.

---

## 2. Root cause of the phantom "file not found" (533 of 952)

Discovery was correct. 419 completed == the number of files on disk.

```
15:06:33.174 PathInput discovery filled 'session'=7, 'speed'=2, 'subject'=17, 'trial'=4 from disk
15:06:33.180 for_each_prepare returning: full_combos=952        # 17 x 7 x 2 x 4
```

`PathInput.apply_discovery` (`scifor/src/scifor/pathinput.py:672-678`) already has the
shortcut that returns the real disk tuples "to avoid inventing non-existent Cartesian
combos". It is thrown away here:

```python
# scifor/src/scifor/foreach.py:1921
if discovered_combos is not None and not set(metadata_iterables.keys()) <= placeholder_keys:
    discovered_combos = None
```

Schema is `[subject, session, speed, trial, cycle]`. The template has no `{cycle}`, so
`placeholder_keys` is the four; but `cycle` is **still present** in
`metadata_iterables` as `[]` at that instant, so the subset test fails and the
Cartesian product wins.

`cycle` is deleted one step later, in scidb Step 3b
(`scidb/src/scidb/foreach.py:1619`, the `no values found for 'cycle' ... dropping it
from the iteration` INFO line at 15:06:33.174) -- **after** the decision that needed it
gone. Step 3 runs the discovery call at `scidb/src/scidb/foreach.py:1555`.

Cost: only ~16s of loop time (failures are ~30ms each), but ~11,200 log lines -- each
failure emits one WARN plus a 20-line MATLAB stack report, and every line is a separate
`py.scistacklog.Log.warn` boundary crossing (`+scidb/Log.m:156`).

---

## 3. Stages

Layer rule (CLAUDE.md NOTE 3): stages land in scistack layers (scifor / scidb /
scimatlab). The GUI is touched only to stop emitting the deprecated `scihist` shim.

### Stage 0 -- PREREQUISITE for Stage 2: make `nargout` truthful

`nargout` inside `+scidb/for_each.m` reports what its *immediate* caller requested. The
generator emits `scihist.for_each(...)` (`matlab_command.py:724, 776, 883`), and the
shim body is `result_tbl = scidb.for_each(...)` -- so `nargout == 1` at the scidb layer
even for a bare-statement user call. Without this stage, Stage 2's gate never fires on
GUI-generated runs.

Two fixes, do both:
- Generator emits `scidb.for_each` instead of `scihist.for_each`. Clean break per the
  beta no-deprecation rule; no alias.
- `+scihist/for_each.m` forwards arity for anyone still calling the shim directly:
  ```matlab
  if nargout > 0
      result_tbl = scidb.for_each(fn, inputs, outputs, varargin{:});
  else
      scidb.for_each(fn, inputs, outputs, varargin{:});
  end
  ```

**Test:** MATLAB test asserting `nargout` observed inside `scidb.for_each` is 0 for a
statement call through both the direct path and the shim.

### Stage 1 -- Turn on the instrumentation that already exists (do this FIRST)

Diagnostics before repair (CLAUDE.md NOTE 2 / NOTE 4). 60% of the runtime is currently
attributed by source reading, not measurement, and the two DB phases report one opaque
number each.

- **`+scidb/for_each.m`**: named `tic/toc` around each post-save step, emitted as one
  `[timing] for_each_postsave: TOTAL=..s (from_python=.., flatten=..,
  type_restore=.., introspect_parse=..)` line, matching the existing `matlab_preamble`
  / `matlab_script` format. Named operations, no "Step N" prefixes.
- **`scidb/src/scidb/database.py:1702`**: `save_batch` already builds an ~18-phase
  `timings` dict (`setup`, `split_metadata`, `schema_resolution`, `per_row_hashing`,
  `canonical_hash`, `record_id`, `storage_row`, `meta_row`, `dedup_check`,
  `data_df_create`, `data_insert`, `record_save_insert`, `record_entities_insert`,
  `variables_upsert`, `commit`, `batch_inserts`, `total`) behind `profile=False`, and
  then `print()`s it. Route it through `Log` and fold the top phases into the existing
  INFO summary line.
- **`scidb/src/scidb/provenance_save.py:625`**: promote `record_run`'s per-phase
  timings (`1_meta_fetch`, `2_assemble`, `3_commit`) from DEBUG into the INFO summary.
- **`+scidb/+internal/from_python.m`**: element/byte counters so a slow conversion says
  how much it converted, not just how long it took.

**Test:** extend `scimatlab/tests/matlab/scidb/TestForEachTimingInstrumentation.m` to
assert the post-save timing line is emitted and its TOTAL >= the sum of its named parts.

**Exit criterion:** a re-run attributes both the 385s and the 147s to named phases. Do
not start Stage 2 or Stage 5 until this is measured.

### Stage 2 -- Gate result marshalling on `nargout` (~385s, 60%)

Keep the payloads; just don't convert them when the caller discarded the result.

In `+scidb/for_each.m`, wrap the whole post-save block (`from_python`, the nested-table
flatten, the metadata type restore, the introspect JSON parse -- lines 633-729):

```matlab
if nargout > 0
    result_tbl = scidb.internal.from_python(py_result_df);
    ... flatten / type-restore / introspect ...
else
    result_tbl = table();   % satisfy the declared output; convert nothing
end
```

Assign the empty table explicitly rather than leaving the declared output unassigned --
do not rely on MATLAB tolerating an unassigned output on a statement call.

**Python side: no equivalent gate, and none needed.** Python has no `nargout`. Caller
bytecode inspection (`sys._getframe(1)` + checking for `POP_TOP` after the call) does
technically work but is rejected here: version-specific bytecode (3.11 `CACHE` entries,
3.12/3.13 `CALL` changes), breaks through decorators/wrappers, breaks in the REPL, and
breaks entirely when the caller is MATLAB (no Python caller frame). More to the point
there is nothing to save: `_for_each_save_resolved` builds `result_tbl` regardless and
returning an in-process DataFrame is a pointer copy. The whole cost is the
MATLAB<->Python boundary, so the gate correctly lives in scimatlab -- the *cost* is
MATLAB-specific, so this is not the kind of duplication NOTE 3 forbids. If API symmetry
is ever wanted, add an explicit `return_results=False` keyword, never introspection.

**Tests:**
- MATLAB `TestForEachReturnValue.m`: an assigned call still returns a full usable table
  with payloads intact; a statement call saves the identical records (same record_ids)
  while performing no payload conversion.
- Assert the statement-call path is materially faster on a payload-heavy fixture, so the
  gate cannot silently regress.

### Stage 3 -- Drop unsupplyable keys before the discovery decision (952 -> 419)

Move the Step 3b "no values found for '<key>' -- dropping it" removal
(`scidb/src/scidb/foreach.py:1619`) to run **before** the
`_scifor_resolve_pathinput_discovery` call at `scidb/src/scidb/foreach.py:1555`, so the
subset test at `scifor/src/scifor/foreach.py:1921` sees the final key set.

Ordering constraint to respect: the current Step 3b message distinguishes three
outcomes (discovery filled it / discovery dropped it as unsupplyable / nothing could
fill it) and needs discovery's result to say which. Splitting is required -- decide
"this key can never be supplied by the DB *or* this template" up front (the template's
`placeholder_keys()` is known without walking the disk), and keep the
filled-vs-still-empty *reporting* after discovery. Do not just reorder the whole block.

**Tests:**
- `scidb` Python test: schema `[a, b, c]`, PathInput template with `{a}/{b}` only, files
  present for a strict subset of `a x b`. Assert `full_combos == len(files)`, not
  `len(a) * len(b)`, and that `c` is still reported as dropped.
- MATLAB: extend `scimatlab/tests/matlab/scidb/TestForEachSchemaFiltering.m` (already
  the named regression home for Cartesian invention) with the extra-schema-key case.
- Assert zero `iteration failed` WARNs in that scenario -- the log volume IS the symptom.

### Stage 4 -- Log-volume cleanup

- Promote the discovery decision to INFO in `PathInput.apply_discovery`: today
  "no user-explicit template keys; using N disk combos directly" vs "explicit user
  values ...; Cartesian product of iterables will drive combos" are both DEBUG
  (`scifor/src/scifor/pathinput.py:757-770`), which is why this diagnosis needed source
  reading instead of log reading. O(1) per run and changes what executes.
- Cap the per-failure MATLAB stack report: first occurrence keeps the full 20-line
  report, subsequent identical identifiers log the WARN only. The end-of-run
  `failed: N x "<identifier>: <msg>"` summary already carries the detail.

### Stage 5 -- Speed up `for_each_save` (147.2s), AFTER Stage 1's numbers

**Hashing is out of scope by decision (2026-09-13).** `canonical_hash` is not to be
touched -- not reframed, not fed incrementally, and not parallelized across records.
See §4. Everything below is confined to the DB-write phases.

**Known ceiling this imposes:** `canonical_hash` is O(payload bytes) per record, called
419 times over dicts of 10 arrays each, and is the single largest suspect inside
`save_batch`'s 77.3s. If Stage 1 shows it dominates, then `save_batch` is effectively
closed and most of that 77.3s stays. Stage 1 should therefore report the
`canonical_hash` phase number even though we will not act on it -- it tells us how much
of `for_each_save` is reachable at all, and stops us hunting for wins that are not there.

**5a. `record_run`'s 67.1s (hypothesis only).** Anomalous against this repo's own note
at `provenance_save.py:401` -- "22.9s for 14253 records -> 1 invocation" -- i.e. our 419
records with 1 invocation are ~100x worse per record than the documented optimized
case. The `inv_cache` memo means `compute_invocation_id` runs once, `_fetch_record_meta`
is a single query, and there are only 419 output edges and 1 input edge; nothing in the
assemble loop explains it. Leading hypothesis is `3_commit` (DuckDB checkpoint / WAL
churn behind large payloads). Stage 1's DEBUG->INFO promotion settles it in one run. Do
not optimize on this hypothesis. This is the most promising remaining target because it
contains no hashing.

**5b. Remaining `save_batch` phases.** Fair game once Stage 1 sizes them, in whatever
order the numbers justify: `split_metadata`, `schema_resolution`, `dedup_check`,
`data_df_create`, `data_insert`, `record_save_insert`, `record_entities_insert`,
`variables_upsert`, `commit`. Note `.claude/speedup-meta-insert.md` already covers prior
work on the meta-insert path -- read it before re-optimizing that phase.

**Dropped:** the earlier 5c ("share one serialization buffer between `canonical_hash`
and the Arrow insert path") is removed -- the fix would necessarily change what is fed
to the hasher.

## 4. Explicitly out of scope

- **Content hashing (`canonical_hash`), by user decision 2026-09-13.** No change
  of any kind to `scicanonicalhash` or to how `save_batch` calls it: not reframed,
  not fed incrementally, not parallelized across records. Rationale: every stored
  `content_hash` is a content-identity key, and the blast radius of getting the byte
  framing wrong is DB-wide. Stage 1 still REPORTS the `canonical_hash` phase timing
  (it bounds what is reachable in `save_batch`); nothing acts on it. If a future
  session proposes hashing work, it needs an explicit new decision, not this plan.

- **GUI DB-lock starvation.** `get_pipeline DB LOCKED (21359.0ms)`,
  `list_hypotheses DB LOCKED (20000.0ms)` etc. throughout the run. Expected under the
  sidecar-only ownership model; the panel is simply dead for the run's duration. Real,
  but separate work. Measured as NOT a contributor to the save time (see 1, Ruled out).
- **`[provenance] source NOT captured for fn=loadDelsysEMGOneFile: derived hash
  4999059dff63 != stored 8ed6c96933f4`.** Known traceability-only mismatch; the
  equality check stays out.
- **`matlab_param_to_class: output_num=N is out of range`**, ~700 lines at 15:17:07.
  Noisy post-run graph rebuild, unrelated to run time.

## 5. Verification (user runs these -- one package per invocation)

```
pytest scifor/tests
pytest scidb/tests
pytest scimatlab/tests
```

MATLAB-side: `TestForEachTimingInstrumentation`, `TestForEachReturnValue`,
`TestForEachSchemaFiltering`.

Final check is a real re-run of `loadDelsysEMGOneFile`: expect 419 combos (not 952),
zero `iteration failed` WARNs, a `matlab_script: TOTAL` with every phase named, and the
post-save conversion absent from a statement call.

---

## 6. Implementation status (2026-09-13)

Stages 0-4 implemented, uncommitted. Stage 5 is measurement-gated and NOT started.

### Changed

| Stage | File | Change |
|---|---|---|
| 0 | `scistack-gui/scistack_gui/api/matlab_command.py` | emits `scidb.for_each`, not `scihist.for_each` (3 sites + module docstring) |
| 0 | `+scihist/for_each.m` | forwards `nargout` instead of assigning unconditionally |
| 1 | `scistacklog/__init__.py` | new `Log.timings(name, phases, extra=, total=, top=, total_key=)` |
| 1 | `scidb/database.py` | `save_batch` phases via `Log.timings(..., top=6)` |
| 1 | `scidb/provenance_save.py` | `record_run` phases via `Log.timings` |
| 1+2 | `+scidb/for_each.m` | post-save block gated on `nargout`, four named `tic/toc` phases, `[timing] for_each_postsave` line |
| 3 | `scifor/foreach.py` | `resolve_pathinput_discovery(..., unsupplyable_keys=None)`; subtracted before the placeholder subset test |
| 3 | `scidb/foreach.py` | computes `_unsupplyable` from Step 2's result + `pi.placeholder_keys()`, passes it, logs the decision at INFO |
| 4 | `+scifor/for_each.m` | full error report attached to the first reason per IDENTIFIER, not per reason |

### Tests added

- `scidb/tests/test_pathinput_extra_schema_key_combos.py` — 5 tests: disk combos not
  Cartesian; unsupplyable key still dropped; control case with no extra key; explicit
  user values still win, and its auto-filled contrast case. NOTE: `schema_keys=` and
  explicit `**metadata_iterables` are mutually exclusive (`scifor.expand_schema_keys`
  refuses both — it IS the sugar for `key: []`), so the explicit-values test spells
  `cycle=[]` out. Both explicit/auto tests assert on the DECISION log line, not on
  row counts: the two paths agree on rows and disagree on what they ATTEMPT.
- `scistacklog/tests/test_log.py` — 5 tests for `Log.timings` (shape parity with
  `timer`, total defaulting, `top` trimming + `+N more`, DEBUG table, pairs input).
- `scimatlab/tests/matlab/scidb/TestForEachReturnValue.m` — 4 tests: statement call
  still saves; statement and assigned produce the same record; assigned call still
  returns payloads; `scihist` shim forwards arity.
- `scimatlab/tests/matlab/scidb/TestForEachTimingInstrumentation.m` — 2 tests: the
  postsave line exists with all named parts and TOTAL >= sum of parts; a statement call
  reports `from_python=0.000s` (that zero IS the optimization).
- `scistack-gui/tests/test_matlab.py` — new `test_never_emits_the_scihist_for_each_shim`;
  3 existing assertions updated from `scihist.for_each` to `scidb.for_each`.

### Deviations from §3 as written

1. **Stage 3 does not move or split the Step 3b block.** §3 called for hoisting the
   "can this key ever be supplied" decision above discovery and leaving the reporting
   below. Implemented instead as a new `unsupplyable_keys` argument: nothing is deleted
   early, and Step 3b's three-outcome reporting and its `_any_resolved` guard are
   untouched. Reason: the pre-discovery drop could not reproduce Step 3b's
   `_any_resolved` semantics. On a fresh DB *no* key resolves from the database, so a
   hoisted `_any_resolved` check would have seen "nothing resolved" and kept `cycle`
   with a false "0 iterations" warning — the exact class of bug the Step 2/3b split was
   built to fix. Passing the set in keeps one decision point and zero behaviour change
   to the warn/drop path.
2. **Stage 1's from_python counter reports result BYTES, not element counts.** §3 asked
   for element/byte counters inside `from_python`. That function is recursive and
   reentrant; threading a counter through it risks the conversion path for a diagnostic.
   `whos('result_tbl').bytes` after conversion (in try/catch, best-effort, 0 on failure)
   answers the same question — how much was converted — without touching the recursion.
3. **Stage 4's discovery decision is logged at the scidb call site, not inside
   `apply_discovery`.** `apply_discovery` has no level of its own; it writes through a
   `log` callback that scidb passes as `Log.debug`. Promoting inside it would have meant
   changing that callback contract for every caller. scidb now logs the outcome at INFO
   after the call, and `resolve_pathinput_discovery` explains a dropped shortcut through
   the existing callback.

### Not done

- **Stage 5** — blocked on Stage 1 numbers from a real run, by design. Hashing stays out
  per §4 regardless.
- **`scihist.configure_database`** is still emitted by the generator. It is a different
  function from the `for_each` shim, nothing in this plan depends on it, and widening
  the change was not warranted.

### Verification not yet run

Nothing here has been executed: the user runs pytest, and MATLAB is not available in
this environment. Every claim above is "implemented", never "verified". The MATLAB tests
in particular are unrun, and `TestForEachTimingInstrumentation`'s two new methods depend
on `DummyMixed`/`dummy_return_one` accepting a single-combo save, which is a narrower
use than the existing 7000-record method exercises.
