# P6 — Behaviour first: what the post-tidy review changes for a user

*Source: `docs/design-review-post/09-synthesis.md` (and 02, 03, 05, 07, 04 for
detail). Scope: only findings whose fix changes what a user sees: results,
errors, logs, timings. Structure-only items (F2–F7, F11, F16, F18, F19) are out;
the critic's pass already moved them to "only when touching the code".*

## Why this order

This project's recorded bugs are almost all **divergence bugs**: one rule,
two owners, drifting apart. The review found four places where **the same
call gives a different answer in Python and MATLAB right now**. Those come
first. Next are the "mirrored but still agreeing" rules, which get pinned
before they drift. Then the save path, the error messages and the timings.

| Stage | Finding | What the user sees today | After |
|---|---|---|---|
| 0 | F1 + F3b (pin) | nothing yet: this stage records current behaviour as cases | the divergences show up as failing/xfail cases, one per row |
| 1 | **F1 `Merge`** | 3 meanings: Python refuses a shared column; MATLAB **inner-joins on any shared column and silently drops rows that don't match**; `to_csv` inner-joins on schema keys | one rule, same rows in both languages and in the CSV |
| 2 | **F3b row matching** | Python `col == value` (an int column never matches `"1"`, so the combo gets 0 rows); MATLAB parses text for numeric columns | one `key_match` rule; a 0-row-by-type-mismatch combo is logged by name |
| 3 | **F3b no-schema** | no schema: Python hands **every combo the whole table**; MATLAB filters by the iterated keys | one rule |
| 4 | F9 rule twins | missing-value ("no data" → NoOutput), blame ("your code" vs "scistack") and PathInput key-type errors are separate copies in MATLAB; they agree today as far as anyone knows | case files pin them; any disagreement found is fixed in this stage |
| 5 | F10 stringify round trip | schema-key columns are made text in prepare, then cast back in MATLAB from the user's classes; a cast it can't do warns and leaves text | test first; delete both halves only if MATLAB functions get identical values |
| 6 | F14 raw `.con` inserts | bulk saves (`write_batch_rows`, `provenance.save`, 10 sites) skip the contention log and autocommit-failure recovery | a contended or failed bulk save is logged and recovered like every other statement |
| 7 | Observability: `[seam_fallback]` + MATLAB timing | "probable" errors in the GUI with no count of how often; the P5-stage-3 MATLAB timing comparison never ran | a CLI count of hedged errors by stage; a before/after timing table for a real MATLAB pipeline |
| 8 | F13 | (none: dead `Log.journal_json`) | deleted; tagged along with stage 7 because it is 5 minutes |
| 9 | **F8 pipelines** | `db.pipeline(name)` silently makes every later `for_each` register instead of run, until something pops it | registration only inside `with db.pipeline(...) as p:` or via `p.for_each(...)`; `db.pipeline()` alone no longer activates |

Deferred: **F17** (GUI start-up budget). See "Decisions" at the end.

---

## Stage 0 — Pin current behaviour as cases (F1 + F3b, test-only)

Same pattern as `docs/claude/location-filter-cases.json`: one JSON file, read
by a Python test and a MATLAB test, so neither language can change the rule
without a visible diff.

- `docs/claude/merge-cases.json`: per case, constituents (columns + rows),
  combo, schema keys, expected merged rows **or** expected error id. Cases:
  1. disjoint columns, equal heights
  2. disjoint, one constituent 1 row (broadcast)
  3. disjoint, heights 2 vs 3 (error)
  4. shared **finer schema key** (e.g. both have `trial` inside a `session` combo), same trials
  5. shared finer schema key, **different trials** (MATLAB drops rows today)
  6. shared **data** column (`force` in both)
  7. one constituent empty
  8. the `to_csv` case: two variables across subjects joined on schema keys
- `docs/claude/key-match-cases.json`: column class (int, float, text,
  zero-padded text `"01"`, bool, datetime) × combo value (int, `"1"`, `"01"`,
  `1.0`) → match mask. Plus the no-schema cases from
  `TestSciforForEachNoSchema.m`.
- Readers: `scifor/tests/test_merge_cases.py`, `scifor/tests/test_key_match_cases.py`,
  `scimatlab/tests/matlab/scifor/TestMergeCases.m`, `TestKeyMatchCases.m`.
  Each case carries an `expect` per language; where they disagree today the
  case lists both and is marked `"diverged": true`, so stage 0 is green and
  the divergences are a readable list.
- Logging: none new; this stage only observes.

## Stage 1 — One `Merge` rule (F1)

**Owner:** new `scifor/src/scifor/merge.py`, a pure function over frames:
`merge_frames(parts, *, join_keys) -> DataFrame`. Consumers:
`foreach.merge_parts` (becomes a call), `scifor/csv_export.py` +
`scidb/csv_export.py::build_flat_table`, and MATLAB through the case file
(MATLAB's join runs on MATLAB tables, so it stays a MATLAB function but is
held to the same cases; `merge_two_tables` in `+scifor/for_each.m:1833`).

**Recommended rule** (needs your yes — Decision 1):
- join on the **schema-key columns** the constituents share (that is what
  `to_csv` does, and it is the only join with a meaning);
- a shared **non-key** column is an error naming the column and both
  constituents, with the remedy (`ColumnSelection` or rename) — never a join
  on data values;
- rows that exist in one constituent but not the other are **not dropped
  silently**: error listing the unmatched key values (memory: never drop data
  silently). An `on_unmatched=` option can come later if anyone needs inner-join;
- no shared key: column-wise concat, 1-row broadcast, height mismatch is an error (today's rule, unchanged).

**Behaviour change:** MATLAB case 5 and 6 go from "fewer rows, no message"
to an actionable error. Python case 4 goes from "column conflict" error to a
join. CSV unchanged except case 5 now errors instead of dropping rows.

**Logging:** `Log.debug("[merge] keys=… heights=… joined=…")` per merge;
`Log.warn` never needed since every lossy path is now an error.
**Tests:** stage-0 cases flip to one `expect`; `diverged` flags removed;
`TestMerge.m`/`TestSciforForEach.m` merge tests updated where they relied
on data-column joins.

## Stage 2 — One row-matching rule (F3b)

**Owner:** `scifor/src/scifor/keys.py::key_mask(column, value)`. Consumers:
`foreach._filter_df_for_combo` (`foreach.py:2484`) and `matlab_loop._key_mask`
(`matlab_loop.py:686`).

**Recommended rule** (Decision 2): MATLAB's — a numeric column compares
numbers, parsing a text value (`"1"`, `"01"` → 1); a text column compares
text exactly (`"01"` ≠ `"1"`, zero padding kept, per the zero-padded-keys
memory); everything else compares by value.

**Behaviour change:** Python combos whose value type differs from the
column's now get their rows instead of an empty frame.
**Logging:** when a combo matches 0 rows **and** the column's class differs
from the value's, `Log.warn("[key_match] combo=… column=… (int) value='1' (str): 0 rows")`.
That message is the diagnostic for every "my function was skipped" report of this kind.

## Stage 3 — One "no schema" rule (F3b)

**Recommended** (Decision 3): MATLAB's — with no schema, the iterated keys
filter. Python's "every combo gets the whole table" is almost never what a
caller who iterates `subject=[1, 2]` means.
**Owner:** `scifor.foreach.plan_run`'s filter-key choice (one function,
`filter_keys_for(schema_keys, iter_keys)`), already called by `matlab_loop`.
**Logging:** `Log.info("[plan] no schema: filtering by iterated keys […]")` once per run.

## Stage 4 — Pin the MATLAB rule twins (F9)

Three case files, same pattern; each read by the Python owner's test and the MATLAB twin's test:

| File | Python owner | MATLAB twin |
|---|---|---|
| `missing-value-cases.json` | `scifor.missing` | `all_missing` / `group_has_no_data` |
| `blame-cases.json` | `scifor.failures.classify_blame` | local fn in `failure_from_mexception.m` |
| `pathinput-key-type-cases.json` | `scidb.run.foreach._load_pathinput_checked` | `+internal/load_pathinput_checked.m` |

The blame cases need error inputs both sides can build: a stack (file
paths + function names) and an error class/identifier, not live exceptions.
Any disagreement found is a behaviour bug and is fixed **in this stage**
(owner wins; the twin changes).

## Stage 5 — Stringify / un-stringify (F10), test first

1. Diagnostic test (MATLAB, `TestSchemaKeyTypes.m`): for each key class
   (double, int, string `"01"`, categorical, datetime), save → `scidb.for_each`
   → record `class()` and value seen inside the function. Run it with the
   stringify on (today) and with an env switch `SCISTACK_NO_KEY_STRINGIFY=1`
   that skips both halves.
2. Identical → delete stringify in `for_each_prepare` and
   `coerce_meta_columns` / `restore_schema_column_types`, and the switch.
   Different → keep both, write down why in `docs/claude/` and delete the switch.
**User-visible gain if deleted:** no more "could not cast schema column back" warnings; fewer per-run conversions.

## Stage 6 — Bulk inserts through the connection (F14)

`DuckConnection.insert_frame(table, frame, conflict=None)` in sciduckdb:
register, insert, unregister under the lock, through the same contention
log (`_log_contended`) and autocommit recovery as `execute`. Migrate the 10
`duck.con.register` / `duck.con.execute` sites; drop the `query` alias.
Boundary test: no `.con.` attribute access outside sciduckdb.
**Logging:** the existing contention log now covers these; add
`Log.debug("[insert_frame] table=… rows=… ms=…")`.
**Tests:** a contended `insert_frame` logs; a failed one inside autocommit recovers (unit tests in sciduckdb).

## Stage 7 — Measure what users feel

- **`[seam_fallback]` counts.** `scidb log seams [--since]` in the
  observability CLI: groups `[seam_fallback]` lines in `scistack.log` by
  `stage` and exception type, most frequent first. Each top entry becomes a
  precise `diagnose` check + regression test (follow-up, one per entry).
- **MATLAB timing (P5 stage 3, still open).** You run one real MATLAB
  pipeline on the current main and on the pre-stage-3 commit; I compare the
  phase timings in the two `scistack.log`s (prepare, plan, per-combo, assemble,
  save). If a phase is missing a timing line, stage 7 adds it first.
- F13: delete `Log.journal_json` and its test.

## Stage 9 — Pipelines register only inside a `with` block (F8, redesigned)

**Today** (`scidb/run/pipeline.py`): `db.pipeline(name)` and
`pipe.activate()` push onto a module-level `_active_stack`. Every later
`for_each` registers as a deferred step until `run_all()` / `run_until()` /
`deactivate()` pops it. Forgetting to pop leaks into every later call,
including later notebook cells. The only guard is the atexit
`pipeline_never_run` warning.

**After** (decided 2026-11-02):

```python
with db.pipeline("gait") as p:          # registration only inside the block
    for_each(filter_emg, ...)
    for_each(compute_peaks, ...)
p.run_all()                              # running stays explicit

p.for_each(filter_emg, ...)              # explicit form, no ambient state
```

- `Pipeline.__enter__` activates and returns `self`; `__exit__` deactivates,
  also on an exception. It never runs anything. Nested blocks use the
  existing stack.
- **`db.pipeline(name)` stops activating** (beta: no deprecation). Outside a
  `with`, it returns an inactive pipeline and `for_each` runs eagerly.
- `Pipeline.activate()` / `deactivate()` leave the public Python API. They
  become `_activate` / `_deactivate`, used by `__enter__` / `__exit__` and
  the MATLAB bridge (`scimatlab/bridge/pipeline.py:59`). One way in, one way out.
- New `Pipeline.for_each(...)`: calls `scidb.for_each(..., pipeline=self)`,
  so the existing `_target_pipeline` (`run/foreach.py:550`) owns the routing.
  There's no second registration path.
- **MATLAB keeps today's form.** The `scidb.Pipeline("x")` constructor
  activates, and `run_all` / `deactivate` end it; MATLAB has no `with`, and
  `onCleanup` is unreliable in scripts. Add `pipe.for_each(...)` to
  `+scidb/Pipeline.m` as the leak-free MATLAB form, routed through the same
  `pipeline=` argument.
- **GUI source discovery** (`scistack-gui/project/pipeline_discovery.py`):
  the documented user-file convention changes from `pipe = Pipeline(...);
  pipe.activate()` to `with Pipeline("gait_analysis") as pipe:`. Discovery
  still works at import time: the steps are registered inside the block and
  the pipeline stays in `_all_pipelines`. Check
  `execution_service.build_backend_pipeline` (compiled pipelines,
  `discard()`) for any call to `activate`.
- Callers to migrate: `examples/aim2/src/CGAM/run_CGAM_pipeline.py`, the
  docstrings in `run/pipeline.py` and `store/database.py:5399`, the
  `run/foreach.py:420` doc, README / docs pages, and the tests
  (`scidb/tests/test_pipeline_registry.py`, `test_inspect_pipeline.py`,
  `test_on_error_stop.py`, `test_provenance_read.py`;
  `scistack-gui/tests/test_pipeline_discovery.py`, `test_matlab_pipeline_execution.py`,
  `test_graph_edge_endpoints.py`, `test_matlab.py`).

**Logging:**
- `Log.debug("[pipeline] enter 'gait' (stack depth N)")` and
  `"[pipeline] exit 'gait': K steps registered"`.
- `Log.warn("[pipeline] 'gait' was created but never entered or used:
  db.pipeline() no longer activates; use `with db.pipeline(...) as p:` or
  p.for_each(...)")` at atexit, for a pipeline with zero steps that was never
  entered or targeted. This is the remedy for scripts written the old way,
  whose `for_each` calls now run eagerly. The existing
  `pipeline_never_run` warning stays for pipelines that registered steps
  and never ran.

**Tests (new):**
- registration happens only inside the block;
- `for_each` after the block runs eagerly;
- an exception inside the block still deactivates;
- nested blocks register to the inner pipeline;
- `p.for_each` registers with no block;
- a bare `db.pipeline()` registers nothing and triggers the never-entered warning;
- discovery of a `with`-style source file seeds the same GUI state as before;
- MATLAB: `pipe.for_each` registers and a replay runs (`TestPipeline.m`).

---

## Decisions

1. **Merge** — DECIDED (2026-11-02): join on shared schema keys only; error on
   shared data columns and on unmatched rows.
2. **Row matching** — DECIDED: MATLAB's rule in both languages.
3. **No schema** — DECIDED: filter by iterated keys in both.
4. **F8** — DECIDED: `with` block (Python) + explicit `p.for_each` (both
   languages); `db.pipeline()` stops activating; MATLAB constructor keeps
   activating. Stage 9.
5. **F17** (GUI start-up budget) — deferred; revisit only if start-up feels slow.

## Process notes

- You run the tests (one package per pytest run). Each stage ends with the
  commands to run, e.g. `pytest scifor/tests/test_merge_cases.py -q`, then
  `pytest scidb/tests -q`, then the MATLAB suite.
- Beta: no deprecation shims; rule changes go straight to the new behaviour.
- One commit per stage, pushed to main.

---

## Status (2026-11-02): all stages written; pytest passes (user); MATLAB suite UNRUN

| Stage | Owner / files | Tests (new or changed) |
|---|---|---|
| 0+1 Merge | `scifor/merge.py` (`drop_constant_keys`, `merge_frames`); `+scifor/merge_tables.m`; callers `foreach._prepare_merge`, both `csv_export`s, `scidb run.foreach._resolve_per_combo_merge`; MATLAB locals removed | `docs/claude/merge-cases.json` ← `scifor/tests/test_merge_cases.py`, `TestMergeCases.m`; CSV "drops non-matching rows" tests flipped to raise |
| 2 Row matching | `scifor/keys.py`; `_filter_df_for_combo`, CSV metadata filter, `matlab_loop._key_mask` call it | `key-match-cases.json` ← `test_key_match_cases.py`, `TestKeyMatchCases.m` |
| 3 No schema | `RunPlan.filter_keys` (Python per-combo + MATLAB wire) | `test_no_schema_filtering.py` |
| 4 Twins | `+scifor/has_no_data.m`, `+scifor/classify_blame.m` (now Python's order: setup before frames); PathInput policy → `scidb.run.foreach.pathinput_numeric_eligible` / `pathinput_key_type_offender`, MATLAB calls them | `missing-value-cases.json`, `blame-cases.json` + readers; `scidb/tests/test_pathinput_key_policy.py` |
| 5 F10 | temporary `SCISTACK_NO_KEY_COERCE=1` switch in `+scidb/for_each.m` | `TestSchemaKeyRoundTrip.m` prints `F10 … SAME/DIFFERENT` |
| 6 F14 | `DuckConnection.insert_frame`; `bulk_insert` delegates; `query` alias gone | `sciduckdb/tests/test_insert_frame.py`, `scidb/tests/test_no_raw_duckdb_handle.py` |
| 7 Seams | `scifor.failures.parse_seam_fallback`; `scidb seams [--since]` | `test_seam_fallback_parse.py`, `scidb/tests/test_cli_seams.py` |
| 8 F13 | `Log.journal_json` + test deleted | — |
| 9 Pipelines | `Pipeline.__enter__/__exit__/for_each`, `_activate/_deactivate`, never-used warning; `db.pipeline` no longer activates; MATLAB `pipe.for_each` | `TestWithBlockScope` in `test_pipeline_registry.py` (others use `_activated(...)`) |

Deviations from the plan text:
- Stage 0 wrote the case files with the DECIDED rule plus a `today` note per
  case (what each language did before), not `diverged` flags: the decisions
  were already made, so pinning the old behaviour would have been
  thrown away one stage later.
- Stage 4's PathInput twin was removed rather than pinned: MATLAB already
  crossed to Python per combo, so it now calls the Python policy.
- Stage 5: Python's Step 5 stringify also defines scidb identity for Python
  runs, so only the MATLAB casts are switchable; deleting the Python half is
  not on the table.
- Known remaining divergence: MATLAB's merge adds the dropped schema columns
  back to the merged table; Python's does not.
