# Plan: record granularity + log fidelity fixes (2026-08-25)

> **STATUS: all stages implemented 2026-08-25, not yet run.** Tests were
> written but NOT executed (the user runs them). Per the user's instruction
> this is a **clean break: no migration code anywhere** — in particular
> Stage 3b changes `wiring_id` for PathInput-fed nodes and their saved canvas
> positions are simply orphaned. Commands to run are at the bottom.
>
> Deviations from the plan as approved, all deliberate:
> * 3b was implemented BEFORE 3a so the log line reads from the unified
>   bindings rather than being written twice.
> * The position migration described under Stage 3b was dropped (clean break).
> * Stage 1's rule needs no `distribute` clause — see below.
> * Two bugs were found mid-implementation and fixed: binding attachment
>   ordering in `derive_fn_targets`, and placement-suffixed ids in the Stage 4
>   dedup. Both are described in their stages.

Four defects found by reading `examples/vo2max/scidb.log` (a GUI session that
wired `test_pi -> pandas.read_csv -> cpet_data_raw` and ran it once).

| # | Defect | Layer | Risk |
|---|--------|-------|------|
| 1 | 4 CSVs saved as 1364 records | scifor (+ scidb, MATLAB) | high — semantics change |
| 2 | False `no values found ... 0 iterations` WARN | scidb | low |
| 3 | `inputs={}` logged for a run that had inputs | GUI | low |
| 4 | Duplicate edges (4 edges for 2 connections) | GUI | low |

Recommended landing order: **2, 3a, 4 first** (isolated, no design risk, each is
its own commit), then **1** (semantics + test rewrites + MATLAB parity), then
**3b** if we take it.

Stage 3b was added after tracing where `input_types` flows: the three-way split
behind defect 3 also makes `compute_call_id` and `wiring_id` blind to
PathInputs, which is two latent identity bugs, not a logging wart. It is
optional and carries a position migration, so it is scheduled separately.

---

## Stage 1 — Record granularity: schema keys decide, `distribute` overrides

### The rule

Rows spread **unless spreading would silently multiply records at one
address**. Concretely, rows spread when either:

1. the DataFrame carries a **schema-key column the combo does not already
   pin** — each row then lands at its own location, which is what the spread
   is genuinely for; or
2. **there is at most one row** — spreading cannot multiply anything, so the
   wide row just becomes the result table's columns.

Everything else — a MULTI-row table with nothing to say where its rows go —
is one record per combo, stored whole.

**Condition 2 was missing from the first implementation and broke
`for_columns`** (see "Corrections after the first test run"). It is what keeps
`for_columns`' reassembled `1 x N` row and every `distribute` piece
(`df.iloc[[i]]`) behaving exactly as before — both produce one record per
combo either way, so the old spread was never harmful there.

`distribute=True` needs no clause in this rule. It splits *upstream* of result
collection (`scifor/src/scifor/foreach.py:904-914`), stamping each piece with
`{distribute_key: i + 1}` — so pieces arrive already carrying distinct
addresses, that key is pinned, and the rule leaves them alone. One record per
piece, exactly as today.

### Why this rule and not a per-variable declaration

`scidb/src/scidb/foreach.py:4547` already builds each record's save metadata
from every non-`__` column of the result row, schema keys included — so a
spread row carrying a `session` column *already* saves at its own
`(subject, session)` location. The machinery has always implied the rule; it
was simply never checked. Today's trigger is `all(isinstance(v, pd.DataFrame))`
(`scifor/src/scifor/foreach.py:2003-2010`) — a type test, not a semantic one.
When rows carry no schema discriminator, N records land on one combo,
distinguished only by their data values in `branch_params`. That is the
vo2max explosion: 322 + 372 + 304 + 366 = 1364.

### What this removes, stated plainly

Same-location fan-out — N records at one address, distinguishable only by their
data — is reachable ONLY through today's implicit flatten, and this rule removes
it with no replacement. `distribute=True` is **not** a substitute: it files
pieces one schema level *below* the deepest iterated key
(`scifor/src/scifor/foreach.py:376`), and in the vo2max shape
(schema `[subject, session]`, both iterated) it raises
`"'session' is the deepest schema key. There is no lower level to distribute to."`
(line 370).

That removal is the point — same-location fan-out IS the variant explosion. A
caller who genuinely wants per-row records needs a finer address: add a schema
level, or return a schema-key column. Both remaining spread mechanisms then work
the same way — by supplying a finer address, positionally (`distribute`) or from
the data (schema-key column).

### Where it lives

`scifor/src/scifor/foreach.py::_results_to_output_dataframe` (line 1992).
scifor owns result shaping, knows the schema (`set_schema`/`get_schema`, fed by
scidb's `_propagate_schema` at `scidb/src/scidb/foreach.py:1421`), and holds each
row's combo metadata — everything the rule needs. Fixing it here also fixes
standalone scifor use, and mirrors where the MATLAB port will need it.

No new plumbing between layers: the rule reads only `collected_rows` (which
already carry each combo's metadata) and the schema, both of which scifor has.

### Implementation

1. In `_results_to_output_dataframe`, replace the `all_dataframes` branch
   condition with a decision helper: `_spread_decision(collected_rows, schema_keys)`
   returning `(spread: bool, reason: str, discriminating_keys: list[str])`.
   - pinned keys = keys present in a row's combo `metadata`
   - discriminating keys = schema keys appearing as **columns of the returned
     DataFrame** and not pinned
2. Decision is **per run** (one result table, one shape). If combos disagree —
   some outputs carry the key, some do not — spread and `Log.warn` naming the
   inconsistent combos; that is a genuine authoring bug worth surfacing.
3. Duplicate-column hazard: if the returned DataFrame carries a schema key that
   the combo **does** pin, `pd.concat([meta_df, combined_data], axis=1)`
   (line 2019) produces two same-named columns and `to_dict("records")` keeps
   the last silently. Detect and `Log.warn` — pre-existing, cheap to catch here.

### Logging (NOTE 2)

One line per run stating the decision *and its reason* — the line whose absence
made this a 20-minute investigation:

```
INFO [scifor] output 'cpet_data_raw': 322-row DataFrame, no unpinned schema-key
  columns -> saving as ONE record per combo (pass distribute=True to fan the
  rows out into separate records)
INFO [scifor] output 'SessionSummary': 4-row DataFrame discriminated by unpinned
  schema key(s) ['session'] -> spreading into 4 records per combo
```

### Tests

scifor (`scifor/tests/test_foreach_standalone.py`):
- **rewrite** `test_flatten_mode_dataframe_outputs:1168` — currently asserts
  `len(result) == 6` for `lambda: pd.DataFrame({"val": [10.,20.,30.]})` over
  `subject=[1,2]` with no schema column. New expectation: 2 rows, each holding
  the DataFrame. Rename to `test_dataframe_output_without_schema_columns_stays_whole`.
- **new** `test_dataframe_output_with_unpinned_schema_column_spreads` — fn
  returns rows carrying `session`; assert one result row per session.
- **new** `test_distribute_composes_with_whole_table_rule` — `distribute=True`
  over `subject` (schema `[subject, session]`) still yields one row per piece
  at `session=1..N`. The piece now travels in the named output column rather
  than spread across data columns; the saved data must be equivalent.
- **new** `test_distribute_at_deepest_key_still_raises` — pins the line 370
  ValueError, so nobody "fixes" the missing escape hatch by weakening it.
- **new** `test_pinned_schema_column_in_output_warns` — duplicate-column case.

scifor (`scifor/tests/test_schema_dtype_roundtrip.py`):
- **rewrite** `test_flatten_mode_restores_dtype:124` (asserts `len(result) == 4`
  under the old unconditional spread). Keep the dtype assertion; move it onto a
  spreading case so coverage of dtype restoration in spread mode is preserved.

scidb (`scidb/tests/`):
- **new** `test_table_return_saves_one_record_per_combo` — the vo2max shape: a
  322-row DataFrame over 4 combos saves 4 records, each round-tripping to the
  full table.
- **new** `test_output_with_unpinned_schema_key_saves_per_key_records`.
- **new** `test_distribute_true_still_fans_out`.
- **re-run** `test_variant_pinning.py::test_variant_wraps_column_selection:299`
  — expected to still pass (sums to 120 either way; it already handles an
  ndarray value), but it is the canary for the semantics change.
- **re-run** the `for_columns` suite: a 1xN output now takes the normal save
  path, so its column values no longer land in `branch_params` and its
  `record_id`s change. `docs/claude/for-columns-iteration.md:286-301` calls that
  behavior load-bearing and must be updated to match the new rule.

GUI (`scistack-gui/tests/`):
- **new** end-to-end: library fn returning a DataFrame over N combos produces
  exactly N records (the regression this whole stage exists for).

### Migration impact — much smaller than first thought

The first draft of this section said every DataFrame-output record would
change id, because moving from the flatten save path to the normal one drops
the data columns that the flatten path feeds into `branch_params`
(`scidb/src/scidb/foreach.py:4524-4544`), and `record_id` hashes that metadata.

With condition 2 in the rule, that is **only true for multi-row outputs with
no unpinned schema key** — i.e. exactly the records this change calls a bug.
Single-row outputs (`for_columns`, every `distribute` piece) still take the
spread path and keep their ids, so no cache invalidation and no recompute for
them.

Consequence: on first run after this change, only the affected multi-row
functions re-run. Old records remain (nothing is deleted).

### Follow-ups (tracked, not in this stage)

- **MATLAB parity**: `+scifor/for_each.m` has its own flatten path
  (`docs/claude/archive/matlab-for-each-current-state.md:212`). Until ported, Python and
  MATLAB disagree on record granularity. Should land before any MATLAB pipeline
  relies on it.
- `examples/vo2max/vo2max.duckdb` holds the 1364 bad records. Per project ethos
  we do not delete data; this is a throwaway example DB created fresh this
  session (`scidb.log:2`), so recreating it is the clean move — user's call.

---

## Stage 2 — False "no values found in database" WARN

`scidb/src/scidb/foreach.py:1380` warns at Step 2 (resolve `[]` from the DB)
while PathInput filesystem discovery, which actually supplies the values, does
not run until Step 3 (line 1406). On a fresh DB every PathInput-fed run emits
two WARNs claiming "0 iterations" and is then contradicted 8 ms later:

```
WARN [scidb] no values found for 'subject' in database, 0 iterations
INFO [scifor] for_each(pandas.read_csv) - 4 iterations: subject=3 values [...]
```

**Fix:** collect unresolved keys at Step 2 into `_unresolved_from_db` and log at
`Log.debug`. After Step 3, warn only for keys still empty — i.e. neither the DB
nor discovery could fill them, which is the case that really does mean zero
iterations.

**Logging:** state what discovery contributed:
`[scidb] PathInput discovery filled 'subject'=3, 'session'=2 from disk (DB had none)`.

**Adjacent, folded in (small, same code path):** `scifor:309`'s
`4 iterations: subject=3 values, session=2 values` reads as 6. Discovery pruned
`SS02/02` and `SS03/02` because no file exists, and nothing says so. Add the
pruned-combo count to that line.

**Tests:**
- fresh DB + PathInput that discovers combos -> assert **no** WARN (`caplog`) and
  the expected iteration count.
- empty DB, no PathInput -> WARN still emitted (guards against silencing the
  real signal).
- PathInput whose template matches nothing on disk -> WARN, 0 iterations.

---

## Stage 3a — `inputs={}` logged for a run that had inputs

`scistack-gui/scistack_gui/api/run.py:465` logs `v["input_types"]`, which holds
only *variable*-typed inputs. PathInput and Parameter bindings live in
`target["path_input_params"]` and `target["parameter_params"]`
(`services/execution_service.py:864,884`) and are invisible to it. So a healthy
PathInput run logs `inputs={}`.

This is not cosmetic: `api/run.py:689` explicitly treats an `inputs={}` line as
*the* diagnostic for "nothing was wired", so the log currently trains the reader
to misdiagnose a working run.

**Fix:** log all three binding kinds, tagged by origin:

```
[run_thread] Target 1/1 -> pandas.read_csv,
  inputs={filepath_or_buffer: PathInput('test_pi')}, constants={}, output=cpet_data_raw
```

Update the zero-combo warning at `run.py:689` so its wording no longer points at
`inputs={}` as the tell.

**Tests:** `caplog` assertion that a PathInput-wired target names the PathInput
binding in the target line; a Parameter-wired target names the Parameter.

---

## Stage 3b — Unify the three binding dicts (optional, higher risk)

Stage 3a papers over a split that should not exist. A target currently carries
its bindings in three places — `input_types` (variables), `path_input_params`,
`parameter_params` — plus `constants`. Every consumer must remember all of
them, and the ones that forget are already wrong:

1. **`compute_call_id` is wrong for PathInput functions.**
   `domain/variant_resolver.py:315-322` builds `__inputs` from `input_types`
   only. scidb's `ForEachConfig._serialize_inputs`
   (`scidb/src/scidb/foreach_config.py:198-226`) puts variables **and**
   PathInputs in `__inputs`, using `spec.to_key()`, explicitly so two templates
   cannot collapse into one version-key group. So the GUI predicts
   `__inputs: {}` where scidb writes
   `__inputs: {filepath_or_buffer: <template+root_folder>}`.
   The docstring's promise — "a combo hidden before it's ever run lands on the
   same id as the real record it eventually produces" — does not hold for any
   PathInput-fed function. Related to the known gap in
   `derive_fn_targets` combo hiding.

2. **`wiring_id` has the same blind spot.** Called with `input_types` only
   (`domain/variant_resolver.py:521`), so two call sites of one function fed by
   *different* PathInputs, producing the same output type, hash to the same
   canvas node.

3. **`_db_path_input_params`** (`services/execution_service.py:132`) exists
   solely to re-attach from the DB what a unified binding dict would have
   carried from the start.

### Shape

One ordered dict on the target, keyed by function parameter (the same key all
three already use), with a tagged value:

```python
bindings = {
    "filepath_or_buffer": {"kind": "pathinput", "ref": "test_pi"},
    "signal":             {"kind": "variable",  "ref": ["RawEMG"]},
    "low_hz":             {"kind": "parameter", "ref": "test"},
    "cutoff":             {"kind": "constant",  "value": 20},
}
```

`ResolvedEdges` (`domain/edge_resolver.py:22-39`) already resolves all four
kinds in one pass off `targetHandle`; it would return this directly instead of
splitting into parallel dicts. `build_run_inputs` collapses to a single loop.
The Stage 3a log line falls out for free.

### Risk — this is why it is a separate stage

`input_types` is **identity-bearing in two hashes**, and correcting them changes
ids:

- **`compute_call_id`**: including PathInputs makes predicted ids *match scidb*
  — a bug fix, but predicted ids for existing PathInput functions change.
  Reuses `spec.to_key()` verbatim (`_path_input_version_key`), never a
  GUI-local spelling, or the two sides drift again. Returns None when the
  declaration is gone, matching the existing fail-safe convention.
- **`wiring_id`**: node ids key **saved positions and scope membership**
  (`graph_builder.py:204-208`). Changing the recipe orphans every saved position
  for a PathInput-fed function node. **Clean break, no migration** (user
  instruction): those nodes reappear at default positions and must be
  re-placed once. Limited to PathInput-fed nodes — the term is omitted from
  the payload when empty, mirroring scidb's `to_version_keys` dropping
  `__inputs` rather than emitting `{}`.

### Two bugs found while implementing

1. **Binding attachment ordering.** `_attach_db_path_inputs` ran on the way
   OUT of `derive_fn_targets`, but `filter_disconnected_targets` and
   `filter_hidden_constant_value_targets` run before that and now read
   `bindings` — so every DB-history target would have been judged on an empty
   wiring. Bindings are now attached where `fn_variants` is built, in both
   `derive_fn_targets` and `derive_target_for_node`.
2. **Stale `input_types` view.** `filter_disconnected_targets`' manual-reconnect
   path rewrote `bindings` and left `input_types` pointing at the old variable
   — reintroducing exactly the drift this stage removes. It now recomputes the
   view (scalar shape, as `code_export_service` expects).

### Tests

- `compute_call_id` for a PathInput target equals the `call_id` scidb actually
  writes after running it (the assertion the current docstring implies but
  nothing checks — run the function, read the record's call_id back, compare).
- two PathInputs, same fn, same output type -> two distinct `wiring_id`s.
- position migration: node keeps its canvas position across the id change.
- `build_run_inputs` binds all four kinds from the unified dict.

### Recommendation

Worth doing, but **not** bundled with the log fix. Stage 3a is a 10-line change
that makes the log honest; 3b is an identity refactor with a position
migration. If 3b is deferred, leave a comment at
`variant_resolver.py:303` recording that the docstring's guarantee excludes
PathInput functions, so the next reader is not misled by it.

---

## Stage 4 — Duplicate edges after graduation

`domain/graph_builder.py:1373-1376` merges manual edges by **edge id only**
(`any(e["id"] == me["id"] ...)`), while DB-derived edges dedup on
`seen_edges` keyed by endpoints. `manual__u0cfjq` never collides with
`e__test_pi__filepath_or_buffer__pandas.read_csv__47x53y`, and graduation then
rewrites the manual edge onto the same endpoints (`api/pipeline.py:899-911`,
`pipeline_store.rename_edge_endpoints`). Result: 4 edges for 2 connections
(`scidb.log:384,393`), permanently, and deleting one leaves its twin.

**Fix:** build each manual edge's endpoint key in the same shapes `build_edges`
already uses, and skip rendering when `seen_edges` covers it:

- source is `pathInput__*` with `targetHandle=in__{param}` -> `(source, target, param)`
- otherwise -> `(source, target)`

**Do not delete the `_pipeline_edges` row** — project ethos is hide/exclude,
never delete. The manual edge stays in the DB and simply is not rendered while a
DB-derived edge covers the same connection; if the DB-derived edge later
disappears, the manual edge renders again.

Ordering is already correct for hidden edges: every DB branch adds its key to
`seen_edges` *before* the `hidden_edge_ids` check (lines 1253-1258, 1284-1290,
1316-1321, 1348-1353), so a hidden DB edge suppresses its manual twin rather
than being resurrected by it.

**Logging:** count suppressed duplicates in the completion line —
`build_edges complete: 2 total edges (2 DB-derived, 0 manual, 0 hidden, 2 manual duplicates suppressed)`.

**Tests** (`scistack-gui/tests/test_edge_resolver.py` or a new
`test_graph_builder_edges.py`):
- manual edge + matching DB-derived edge -> exactly 1 rendered edge, no
  `data.manual` flag.
- pathInput manual edge matching on `(source, target, param)` -> deduped;
  differing param -> both kept.
- DB-derived edge hidden + manual twin present -> 0 rendered (no resurrection).
- manual edge with no DB counterpart -> still rendered.
- post-graduation rebuild -> edge count stable across two consecutive builds.

---

## Verification

Nothing here has been executed. Per project convention the user runs tests.

```
# syntax first — nothing below is meaningful if this fails
python -m compileall -q scifor/src/scifor/foreach.py scidb/src/scidb/foreach.py \
  scistack-gui/scistack_gui/domain scistack-gui/scistack_gui/services \
  scistack-gui/scistack_gui/api

# new tests, per stage
pytest scidb/tests/test_pathinput_discovery_warning.py -v     # stage 2
pytest scistack-gui/tests/test_binding_identity.py -v         # stage 3b
pytest scistack-gui/tests/test_graph_builder.py -v -k BuildEdges   # stage 4
pytest scidb/tests/test_record_granularity.py -v              # stage 1

# rewritten / at-risk existing suites
pytest scifor/tests/ -v
pytest scidb/tests/test_variant_pinning.py scidb/tests/test_for_columns.py -v
pytest scistack-gui/tests/ -v
```

Manual check: delete `examples/vo2max/vo2max.duckdb`, redo the wiring in the
GUI, run, and read `scidb.log` — expect no false WARN, a target line naming the
PathInput binding, 2 edges not 4, and 4 records not 1364.

### Corrections after the first test run (2026-08-25)

Three clusters of failure, one a real bug:

1. **The rule was wrong for single-row returns (REAL BUG).** 20 scifor
   `test_iterate_*` failures plus 3 in `scidb/tests/test_for_columns.py`:
   `for_columns` reassembles a `1 x N` row per combo and DEPENDS on the spread
   to turn those columns into the result table's columns
   (`docs/claude/for-columns-iteration.md:234-238` says so outright). Without
   it the result table became `['subject', 'output']` with a DataFrame in the
   cell. Fixed by condition 2 (`max_rows <= 1`), which is sound because a
   single row cannot multiply records — and which also removes most of the
   migration impact above.
2. **View shape (REAL BUG, would have hit production).**
   `bindings_of_kind(..., VARIABLE)` returns `["RawEMG"]`, but `wiring_id`
   hashes the value as written and DB history spells single types bare
   (`"RawEMG"`) — so `filter_disconnected_targets` computed wiring ids that
   matched nothing and no hidden-edge lookup would ever have fired. Fixed by
   `edge_resolver.variable_types_view` (bare when single, list only for real
   EachOf), used wherever a target-level view feeds `wiring_id`.
3. **Test-shape only.** `TestComputeCallId`'s local `_target` still built the
   old three-dict shape, and my two new scidb tests called `.load()` as if it
   returned raw data instead of the BaseVariable wrapper (`.data`).

### Highest-risk assumptions (unverified — what to look at if something fails)

1. ~~`pd.DataFrame(rows)` with a DataFrame in a cell~~ — CONFIRMED working by
   the first run (the whole-table tests reached their assertions).
2. ~~scidb's normal save path with a whole DataFrame~~ — CONFIRMED: a 3-row
   table round-tripped as one record's `.data`.
3. ~~for_columns record_ids change~~ — MOOT under condition 2: single-row
   outputs still spread, so `for_columns` is untouched.
   `docs/claude/for-columns-iteration.md:286-301` stays accurate as written.
4. **~45 mechanically-updated `wiring_id(...)` test call sites** (sed, then
   hand-fixed multi-line ones). Verified by grep, not by running.
