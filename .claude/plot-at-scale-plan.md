# Plotting at scale: load what the plot needs, not the whole variable

**Date:** 2026-09-13
**Symptom:** banner "Request plot_resolve timed out" in Plot Studio, and the SAME
failure from right-click a DAG node -> "Schema Key Locations". 419 RawEMG records; not
even ONE schema location can be plotted or listed.
**Source:** `/workspace/scidb.log` tail (15:49 run), plus the code paths below.

---

## 1. What the log proves, and what it does not

The 15:49 request is 13 lines and ends mid-flight:

```
15:49:57.320 [db] plot_describe:  held the DuckDB connection for 0.062s
15:49:57.628 [db] plot_resolve:   held the DuckDB connection for 0.016s
15:49:57.656 [db] plot_capabilities: held the DuckDB connection for 0.047s
                                     <- log ends; client gave up ~15:50:27
```

**Proven:** `plot_resolve` got through `_load` (the entire DuckDB-touching phase) in
**16 ms**, and the `[plot] resolved %s: figure %d of %d rendered` INFO line
(`plot_service.resolve_figures`) never appears. So for THAT request the time went
somewhere after the load and before the render finished.

**Not proven:** which step. Everything between those two points logs at DEBUG only
(`reduce.py`'s `resolve: measure=... rows=...`, `ylimits`' `y limits over ...`), and
`scidb.log` contains **zero** DEBUG lines. `location_states`' own six-phase `timings`
dict is likewise DEBUG-only (`scidb/locations.py:237`) — the same hand-rolled
collect-then-hide pattern that `Log.timings` was just introduced for.

The 16 ms hold is itself suspicious and unexplained: with a cold `BaseSource` table cache
a full `load_variable` cannot finish that fast. Either the table was already cached, or
the payload columns are smaller than assumed. **Stage 1 must settle this before any
optimization is chosen.** Do not skip it on the strength of §2.

---

## 2. The architectural finding (from code, independent of the log)

### 2a. There is no way to load part of a variable

`scistackplotdb/load.py:182` — `load_variable(db, variable, *, with_variants=True)`:

```sql
SELECT t.record_id, t."<every data column>", s."<every schema key>"
FROM "<variable table>" t
JOIN _record r ON t.record_id = r.record_id
LEFT JOIN _schema s ON r.schema_id = s.schema_id
WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE
```

No schema filter. No column projection. No limit. The signature takes no filter
argument, so **no caller can ask for less.** Then
`_build_plan` (`scistackplot/reduce.py:336`) applies the user's filter — including "just
this one schema location" — in pandas, *after* all of it:

```python
frame = table.frame
frame = apply_filters(frame, spec)
```

That is the direct answer to "I can't plot even a single schema location of 419": the
cost of plotting one location is the cost of loading all of them.

### 2b. The location tree materializes the whole variable to read metadata

`plot_service._location_tree:378`:

```python
source = get_source(db)
if selection is None:
    selection = default_selection(source.get_table([variable]))   # full table build
```

`selection is None` is exactly the canvas path — the docstring says so ("`selection=None`
on the **canvas** path means 'no spec is open'"), which is the right-click the user
reported. And `default_selection` (`scistackplot/variants.py:145`) reads only
`table.default_pin`, `table.latest_column`, and `table.variant_factors[...].levels` —
precomputed metadata — plus `frame[factor.name]` for code axes. **It never touches a
payload column.** The entire variable is loaded to answer a question about variant
columns.

`ScidbSource.describe` already proves the cheap path exists: "Every plottable variable
plus the schema keys, **without loading data**", using one sampled value per variable.

### 2c. Arrays cross as Python lists of Python floats

`sciduckdb.py:918` — `self.con.execute(sql, params).fetchall()`. A DuckDB `DOUBLE[]` cell
becomes a Python `list[float]`: ~24 B per value plus an 8 B pointer, against 8 B in a
numpy array. Then `ylimits._cell_extents` converts each cell back with
`np.asarray(value, dtype="float64")` in a Python loop — so the arrays are built as Python
objects and immediately re-converted.

### 2d. Every field loads and melts even when one is plotted

`data_columns_for` returns all columns; `_melt_fields` melts every plottable one. A
10-field RawEMG record becomes 10 rows, each still holding a full signal, so the frame is
419 x 10 = 4190 array cells before any filter.

### 2e. The transport has a fixed 30 s timeout and no cancel

`frontend/src/api.ts:127`. This exact banner was fought on 2026-09-10/11 by holding back
superseded requests (`PlotStudio.tsx:585`, which documents resolve times climbing
3.3 -> 7.6 -> 15.2 -> 27.7 s as they piled up). That fix removed *pile-up*; it did
nothing about the cost of a single resolve, which is what now exceeds 30 s on its own.

---

## 3. Stages

### Stage 1 — Measure (MANDATORY FIRST; ~nothing here is optional)

- Promote `location_states`' six phases (`present`, `expected`, `superseded`,
  `excluded`, `versions`, `tree`) from DEBUG to INFO via `Log.timings` — the helper added
  in the timing plan; this is precisely its second caller.
- Wrap `BaseSource.get_table` / `_build_table` in `Log.timer` with phases for
  `load_variable`, `attach_variants`, `melt`, `stack`, `join`, and report **rows, cells,
  and total samples** — not just seconds. A row count alone cannot distinguish 4190 short
  arrays from 4190 quarter-million-sample arrays, and that distinction decides Stage 2
  vs Stage 3.
- Log a cache hit/miss for `get_table` and for `ScidbSource._frames`. The unexplained
  16 ms hold is probably one of these, and right now nothing says.
- `reduce._build_plan`: promote the existing `resolve: measure=... rows=...` DEBUG line
  to INFO, and time `apply_filters`, `limits_by_scope`, `_ordered_groups`, and the
  per-figure explode.

**Exit criterion:** one reproduction of both failures attributes the >30 s to named
phases, and states the actual payload size (samples per record, bytes per frame).

### Stage 2 — Answer metadata questions without loading payloads

Fixes "Schema Key Locations" outright, and is worth doing even if Stage 1 shows the
payloads are small, because it removes a whole-variable load from a metadata path.

- Add a source method that returns just the variant metadata a selection needs
  (`default_pin`, `latest_column`, `variant_factors` with levels) built from `_schema` /
  provenance queries — the way `describe` already does it — with no data columns.
- `_location_tree` uses that instead of `source.get_table([variable])`.
- Test: `plot_location_tree` on a variable with large payloads issues no query that
  selects a data column. Assert on the SQL, not on wall time.

### Stage 3 — Push the filter into SQL

The real scaling fix.

- `load_variable(db, variable, *, locations=None, fields=None, sample_stride=None,
  with_variants=True)` — `locations` becomes a `WHERE` on the schema keys, `fields` a
  column projection.
- Thread the spec's schema filter and chosen field(s) from `PlotSpec` down through
  `get_table` into that call, and add them to the `get_table` memo key.
- Opening Plot Studio on ONE location must issue a query that returns ONE location.
- Tests: a spec filtered to one location loads one location (assert row count and the
  emitted SQL); the memo key distinguishes two different location filters; an unfiltered
  spec still behaves as today.

**Ordering note:** Stage 3 subsumes much of the benefit of Stage 4/5, so do it before
either. It is also the only stage that changes what SQL runs, so land it alone.

### Stage 4 — Stop building Python lists for array columns

- Fetch via Arrow (`.arrow()` / `.fetch_arrow_table()`) or `fetchnumpy()` so a `DOUBLE[]`
  arrives as an Arrow list array and each cell lands as a numpy array.
- `ylimits._cell_extents`' per-cell `np.asarray` then becomes a no-op rather than a
  conversion.
- Constraint: cells must stay numpy arrays through melt/stack/explode. `_as_array`
  already accepts `np.ndarray`, so the reader side is ready.
- Test: a loaded 1-D measure's cells are `np.ndarray`, not `list`.

### Stage 5 — Decimate before Python, not after

`MAX_TRANSPORT_POINTS` already caps what reaches the browser, but the reduction happens
after the full-resolution frame exists in memory. For a 1-D measure with no aggregation,
push a stride/decimation into SQL so the full-resolution signal is never materialized for
an interactive view. The export path must keep full resolution (`export_code` deliberately
never reduces) — so this is a resolve-path-only option, not a change to the stored data.

### Stage 6 — Transport, last

Only after 1-5. Make the 30 s timeout configurable and add real cancellation so a
superseded resolve stops costing. Raising the timeout alone is not a fix and must not
substitute for Stages 2-5.

---

## 4. Out of scope

- **Hashing** — unchanged, per the run-timing plan's §4.
- The GUI DB-lock contention and the provenance hash mismatch — separate, already
  recorded there.

## 5. Verification

```
pytest scistackplot/tests
pytest scistackplotdb/tests
pytest scidb/tests
pytest scistack-gui/tests
```

Then the real check: open Plot Studio on RawEMG (419 records) and right-click ->
Schema Key Locations. Both must return well inside 30 s, and the log must attribute the
time to named phases.

---

## 6. Stage 1 implementation status (2026-09-13)

**Stage 1 only.** Stages 2-6 not started — they are gated on the numbers this
produces.

### Changed

| File | Change |
|---|---|
| `scistackplot/framesize.py` | **new** — `cell_samples`, `frame_extent`, `format_extent`; one definition of "how big is this frame" (rows / cells / samples / est_bytes / boxed) shared by every layer that reports it |
| `scistackplot/sources/base.py` | `get_table`: cache HIT/MISS at INFO, `Log.timer` with `build_table` + `measure_extent` phases, and a `built rows=… cells=… samples=… ~NGB boxed` line |
| `scistackplot/reduce.py` | `_build_plan` split into a timed wrapper + `_build_plan_timed`; phases `variant_sets`, `level_groups`, `roles`, `apply_filters`, `group_fanout`, `y_limits`; the `resolve: measure=…` line promoted DEBUG -> INFO and given the post-filter extent |
| `scistackplotdb/load.py` | `load_variable` phases `column_metadata`, `fetch`, `dataframe`, `stringify_keys`, `attach_variants`, `measure_extent`; the `loaded …` line now carries the extent; the no-filter/no-projection SQL is called out in a comment |
| `scistackplotdb/source.py` | `_variable_frame`: `_frames` cache HIT/MISS at INFO; `_melt_fields` timed, and its INFO line carries the post-melt extent |
| `scidb/locations.py` | `location_states`' six phases promoted DEBUG -> INFO via `Log.timings`, with a true wall-clock TOTAL |

### Tests added

- `scistackplot/tests/test_framesize.py` — 16 tests, including
  `test_row_count_alone_cannot_distinguish_these` (the reason the module exists)
  and the `boxed` flag that says whether an Arrow fetch path would pay.
- `scistackplot/tests/test_resolve_instrumentation.py` — 4 tests: every
  `build_plan` phase present, TOTAL >= sum of phases, the post-filter extent at
  INFO, and that the extent tracks the FILTERED frame rather than the loaded one.
- `scistackplot/tests/test_get_table_instrumentation.py` — 4 tests: miss reports
  what it built, hit builds nothing (asserted by the ABSENCE of a build timing
  line), distinct keys stay distinct, extent scans only the requested measures.

### Deviations from §3 as written

1. **No single threaded timer through `ScidbSource._build_table`.** §3 asked for
   phases `load_variable / attach_variants / melt / stack / join` under one timer.
   That function branches four ways (single measure, melted fields, x-joined
   pair, stacked) and threading one timer through all of them touches every
   branch of the hottest path in the package for a diagnostic. Each leaf
   operation got its own `Log.timer` instead: same breakdown, independently
   greppable, and `get_table`'s outer timer still gives the total. `attach_variants`
   is timed as a phase of `load_variable`, where it is actually called.
2. **The per-figure explode needed nothing.** §3 listed it; it is already a timed
   phase (`timing.phase("explode", …)`) logging at INFO from the 2026-09-11
   narration work. Left alone.
3. **`est_bytes` is an estimate, not a measurement.** 32 B/sample for a boxed
   Python `list[float]`, 8 B for float64 ndarray. Summing real `nbytes` would
   mean walking into every value; the point is to separate "big" from
   "hopeless" at a glance, which an estimate does.

### Not verified

Nothing has been run — no Python in this environment, and the failure needs the
user's 419-record database to reproduce. In particular `test_build_plan_total_covers_its_phases`
parses the summary line with a regex and drops the largest match to exclude
TOTAL; if a phase ever exceeds TOTAL through a rounding artifact that heuristic
is what breaks.

### What to capture on the next reproduction

Both failures, with the file sink at INFO (the default). The four lines that
decide Stages 2-6:

1. `variable frame cache HIT/MISS: RawEMG` — was anything actually loaded?
2. `loaded RawEMG: N record(s), rows=… cells=… samples=… ~NGB boxed` — **the
   sample count is the number that has been missing all along.**
3. `[timing] load_variable: … (fetch=…, dataframe=…, attach_variants=…)` — if
   `fetch` dominates, Stage 3 (filter pushdown) and Stage 4 (Arrow) are the fix;
   if `attach_variants` dominates, it is an N+1 and neither is.
4. `[timing] build_plan: … (apply_filters=…, y_limits=…, group_fanout=…)` — if
   `y_limits` dominates, `_raw_extents` walking every cell in Python is the
   hotspot and the fix is narrower than the plan assumes.

---

## 7. MEASURED (2026-09-13 16:44 run, Stage 1 instrumentation live)

### The data

```
419 record(s), rows=419 cells=4190 samples=1.74e+08 ~5.2GB boxed
```

10 fields x 419 records = 4190 cells, **174 million samples**, ~**5.2 GB** as boxed
Python floats — roughly **41,500 samples per cell**. §2's guesses about volume were
right; the guesses about WHERE the time goes were partly wrong (see §7.3).

### The two builds

| | start | TOTAL | fetch | attach_variants | melt |
|---|---|---|---|---|---|
| `plot_describe` | 16:44:43.495 | **188.8s** | 84.8s | 104.0s | 0.05s |
| `plot_location_tree` | 16:46:08.226 | **104.1s** | 96.7s | 7.1s | 0.04s |

Connection holds: `plot_describe` **193.6s**, `plot_location_tree` **114.3s**. Both
finished at 16:47:52 — ~3.5 minutes after the first started, against a 30 s client
timeout.

### 7.1 NEW FINDING — cache stampede (was not in this plan at all)

Both requests logged `get_table(RawEMG): table cache MISS — building`, and both logged
`variable frame cache MISS: RawEMG — loading`. **The same 5.2 GB table was built twice,
concurrently.**

`BaseSource.get_table` writes the memo only *after* `_build_table` returns, so a second
request arriving during the first build sees an empty memo and repeats the whole thing.
The `_frames` cache in `ScidbSource` has the identical shape. The 2026-09-11 memoization
work fixed *sequential* repeat cost; it never addressed concurrent arrival, and the panel
fires `plot_describe`, `plot_resolve`, `plot_capabilities` and `plot_location_tree`
together.

This is the cheapest large win available and it is not filter pushdown: an in-flight
map of key -> Future (or a per-key lock) makes the second caller wait for the first
build instead of repeating it. On this run that is ~90 s of the ~190 s.

### 7.2 attach_variants' 104s is contention, not work

104.0s in one build against 7.1s in the other, for the same 419 records, with
`variants=none` — it produced no columns at all. `attach_variants` calls
`variant_identity_batch` (one batched query, `scidb.provenance_query`), so this is **not**
an N+1. DuckDB access serializes on `_duck._lock`, so the 104s figure is one build's batch
query waiting behind the other build's 96.7s fetch. Real cost ≈ 7s. **Fixing §7.1 removes
this number rather than optimizing it.**

### 7.3 Corrections to §2 of this plan

- **§2d (melt) was wrong as a cost claim.** `melt_fields` is **0.05s**. It reshapes
  references; 4190 cells before, 4190 after. It still loads all 10 fields when one is
  plotted, which is a Stage 3 concern, but it is not a time sink and should not be
  presented as one.
- **`y_limits` / `_raw_extents` is NOT the hotspot.** `_build_plan` never ran — the
  resolve died in `get_table`. The "48 rows" assumption in `ylimits` may still be wrong at
  scale, but it is not what is failing today, and the previous message's suggestion that
  it might be the narrower real fix is withdrawn.
- **`describe` is not cheap in practice.** `ScidbSource.describe()` honestly avoids
  loading data, but the GUI's `_describe(variable)` path calls
  `source.get_table([variable])` — so `plot_describe` held the connection for 193.6s. It
  is a THIRD caller doing a full payload load for metadata, alongside `_location_tree`
  and `plot_capabilities`.
- **`location_states` is healthy.** 9.5s for 419 locations
  (present=3.1s, expected=5.3s, superseded=0.3s, versions=0.8s, tree=0.03s). The
  "Schema Key Locations" timeout is **entirely** the `default_selection(get_table(...))`
  call in `_location_tree`. Stage 2 alone fixes that view.

### 7.4 Revised stage order

1. **Stage 2a (NEW) — in-flight dedup** of `get_table` and `ScidbSource._frames`. Halves
   this run. Smallest diff of anything here.
2. **Stage 2 — metadata without payloads.** Three proven callers: `_location_tree`,
   `_describe`, `plot_capabilities`. Fixes Schema Key Locations outright.
3. **Stage 4 — PROMOTED above Stage 3.** `boxed` is measured True and `fetch` is 85-97s,
   the dominant phase of a single build. 5.2 GB of boxed floats is ~1.3 GB as float64.
4. **Stage 3 — filter pushdown.** Still the only thing that makes "plot ONE of 419
   locations" cost one location.
5. **Stage 5 — decimate in SQL.** 174M samples for a plot a few thousand pixels wide.
6. **Stage 6 — transport/cancel.** Unchanged, still last.

---

## 8. Stage 2a + Stage 2 implementation status (2026-09-13)

Implemented, uncommitted, unrun. Stages 3-6 not started.

### Stage 2a — in-flight dedup (the stampede)

| File | Change |
|---|---|
| `scistackplot/dedup.py` | **new** — `SingleFlight`: keyed in-flight map of `Future`s; first caller builds, concurrent callers wait on the same result; failure propagates to owner AND waiters; key released either way; different keys still build in parallel |
| `scistackplot/sources/base.py` | `get_table` routes its build through `SingleFlight`; the memo is written INSIDE the slot (before release) and re-checked inside it; `_lazy_attr` helper with a module lock so the per-source `SingleFlight` itself cannot be created twice under load |
| `scistackplotdb/source.py` | `_variable_frame` deduped the same way, one layer down — two different table keys over one variable both land here |

Waiters log `build already in flight — waiting for it` at INFO, so a waiter is
distinguishable from a cache hit in the log. Those mean different things when reading a
slow request.

### Stage 2 — metadata without payloads

| File | Change |
|---|---|
| `scistackplotdb/load.py` | `load_variable(..., include_data=True)`; `False` selects record ids, schema keys and variants only — no data columns in the SELECT; reports `data_columns=[]` |
| `scistackplotdb/source.py` | new `ScidbSource.variant_table(variable)`: a `LongTable` with `measures=[]` carrying the same variant factors, levels, `default_pin` and `latest_column` a full build produces; cached under its own key `("__variants__", name)`; deduped |
| `scistack-gui/services/plot_service.py` | `_location_tree` uses `variant_table` for `default_selection` when the source has it; CSV/DataFrame sources fall back to `get_table` (in-memory anyway) |

The selection POLICY stays in `scistackplot.default_selection`, untouched. Only the
load moved. `test_default_selection_agrees_with_the_full_table` is the contract: the
cheap path must give the identical answer to the expensive one.

### Tests added

- `scistackplot/tests/test_dedup.py` — 9 tests. `SingleFlight` under real threads:
  concurrent callers share one build and one object; different keys parallelise
  (barrier-based, deadlocks if they serialise); `on_wait` fires for waiters only;
  failure reaches owner and waiters; key released after success and after failure.
  Then `get_table` itself: two concurrent calls, ONE `_build_table`, one `MISS` line and
  one `waiting for it` line — the literal regression from the 16:44 log.
- `scistackplotdb/tests/test_variant_table.py` — 12 tests. No measures; no muscle
  column in the frame; **no muscle column in the SQL** (spied on `_fetchall`, asserted
  on the query text — a timing assertion would pass on a small fixture whatever the
  query did); schema keys survive; `default_selection` agrees with the full table for a
  dict variable and a scalar one; variant columns/levels/pin/latest match; cache hit on
  the second call; no key collision with the plottable table; `include_data` is opt-in.

### Deliberately NOT done: `_describe` and `plot_capabilities`

§7.3 identified three callers doing a full payload load for metadata. Only
`_location_tree` is fixed here. `_describe` needs `shape_of(variable)`, `default_spec`,
`stackable_report`, `groupable_with` — measure-level facts that today come only from
values. Answering those without a payload is a real design (sample-one-record, the way
`ScidbSource.describe` already does for shapes), not a substitution, and it should not
ride on this pair. **Consequence: `plot_describe` still loads the full table.** With
Stage 2a it now does so ONCE and the resolve waits on it rather than repeating it, so the
panel-open cost should roughly halve on this dataset — but it is not yet under 30 s, and
the Schema Key Locations view is the only surface this pair fully fixes.

### Expected log on the next reproduction

Open Plot Studio, then right-click -> Schema Key Locations:

```
get_table(RawEMG): table cache MISS — building          <- once
get_table(RawEMG): build already in flight — waiting    <- the other panel requests
variant_table(RawEMG): table cache MISS — building (no data columns)
[timing] load_variable: RawEMG (metadata only), TOTAL=<seconds, not minutes>
[timing] location_states(RawEMG): ... TOTAL=~9.5s
plot_location_tree: held the DuckDB connection for <~10-15s>
```

If `variant_table` still reports minutes, `attach_variants` is the remaining cost on
that path and it needs looking at on its own — it ran 7.1s uncontended on the 16:44 run,
which is fine, but it is the only non-trivial work left in a metadata-only load.

---

## 9. 17:00 run — Stage 2 works; the re-open hangs in the DISK WALK

### The good news, proven

```
17:00:09 RPC >> plot_location_tree(variable=RawEMG, selection=None)
17:00:09 get_table(RawEMG): table cache HIT
17:00:11 [timing] location_states(RawEMG): 419 location(s), TOTAL=1.942s
17:00:11 plot_location_tree: held the DuckDB connection for 2.000s
17:00:11 RPC << plot_location_tree OK (2000.0ms)
```

2.0 s, down from 114.3 s. (This run still says `get_table` HIT rather than
`variant_table` — the table was already warm from the earlier panel open, so the
substitution was not exercised; it is the stampede fix + a warm cache that produced this
number. `variant_table` will show on a cold open.)

Plot Studio also recovered: `plot_describe` 0.078 s, `plot_resolve` cleared its DB hold
in 0.031 s and reached `resolve: measure=RawEMG ... post-filter rows=4190
samples=1.74e+08 ~5.2GB` — i.e. into `_build_plan`, which it had never reached before.

### The re-open (17:01:18) — a different hang, precisely located

Same request, same thread (55212), **no concurrency at all**. It ran the identical query
sequence as the 17:00 success up to and including:

```
17:01:21 SELECT ii.param_name, ii.input_record_id, r.type, c.value_repr FROM _invocation_input ...
```

(`provenance_query.invocation_inputs`, called via `pathinput_configs` inside
`locations._discovery_expected`). In the successful run the NEXT query,
`SELECT "subject","session","speed","trial",...` from `check_pathinput_node_state`,
fired 1 s later. In the hung run it never fires. The log simply stops at 17:01:21.

What sits between those two queries is **not a database call**: it is
`pi.discover()` — `scifor.pathinput.PathInput._walk`, which does `os.listdir` on
`\\fs2.smpp.local\RTO\Spinal Stim_Stroke R01\AIM 2\Subject Data\...` recursively
(`state.py:768`, `pathinput.py:855`). That is a UNC path on a network share, walked
once per subject / session directory.

So the 17:01 hang is **the network filesystem, not the database and not the plot
pipeline**. The 17:00 success took 1.84 s in the `expected` phase because Windows had the
directory listings hot; ~70 s later on the re-open, they were not (or the SMB session
had to renegotiate), and `listdir` on a UNC path can block for minutes with no error.
The plot layer cannot see this because there is no log line and no timeout anywhere in
`_walk`.

Two things make this worse:
1. `PathInput` declares `self._dir_cache` (`pathinput.py:187`) "per-directory listings
   validated by mtime" — but `_walk` does not use it. It calls `os.listdir` directly
   every time. Every `location_states` call re-walks the whole share.
2. The re-open at 17:02:12 (a third `plot_location_tree`) arrived while the 17:01 one
   was still stuck; `acquire_db_connection` reports `refcount=2`, so the second request
   piled onto the same stuck state.

### What this means for the plan

- **Stage 2a + 2 are validated on this surface** — 2.0 s when the walk is warm.
- The remaining Schema-Key-Locations failure is a NEW item, out of scope of every
  stage here: `_discovery_expected` has no guard against a slow network walk. Needs (a)
  a `[timing]` line around `pi.discover()` inside `check_pathinput_node_state` so a slow
  share is visible as such, (b) wiring the already-declared `_dir_cache` into `_walk`
  so a re-open does not re-walk a share that has not changed, and probably (c) a
  memo of the discovered combos on the source, invalidated the same way tables are.
  This is scifor/scidb work, not plotting work.

---

## 10. Discovery-walk item implemented (2026-09-13) + a reorder for the plot itself

### What changed

| File | Change |
|---|---|
| `scifor/pathinput.py` | `_walk` now lists directories through `_list_dir` (the mtime-validated cache that already existed for the numeric-fallback path) instead of a bare `os.listdir`; `discover()` is wrapped in `Log.timer("pathinput_discover")` naming the root, plus an INFO line with **directories read from disk vs served from the listing cache** (new `_dir_cache_reads` / `_dir_cache_hits` counters) |
| `scidb/locations.py` | `_discovery_expected` caches its combos in **`scidb.state._discovery_cache`** — the existing TTL dict the canvas badge uses — under an `("expected", db, fn, grid)` key; cache hits log `discovery served from cache … no filesystem walk` |
| `scistack-gui/plot_service.py` | `invalidate` deliberately does NOT clear the discovery cache (comment explains why) |

### Two design corrections made mid-implementation

1. **No new memo.** I first wrote a separate, event-invalidated `_discovery_memo` in
   `locations.py` with an `invalidate_discovery()` hook wired into the GUI's post-run
   `invalidate`. Then found `scidb.state._discovery_cache` already exists for this exact
   walk, with a written-down decision: **TTL (5 s), not event invalidation**, because "the
   filesystem changes behind our back by definition, so any invalidation hook would be a
   guess about when". A second cache of the same filesystem with the opposite staleness
   policy would have been a real bug in waiting. Reverted to reuse the existing dict,
   inheriting its TTL and its `clear_discovery_cache()`. The two callers want different
   SHAPES from the walk (the badge wants a diff, `location_states` wants the full set), so
   they get different keys in one cache rather than one cache each.
2. **The listing cache is not free on a share.** `_list_dir` validates by `stat()`, which
   is itself one round-trip per directory. So wiring it into `_walk` makes a warm re-walk
   *cheaper*, not free — the TTL cache above it is what makes the re-open free. Both are
   needed; the docstring on `_list_dir` now says so.

### Tests

- `scifor/tests/test_pathinput_discover_cache.py` — 5 tests: a second `discover()`
  calls `os.listdir` zero more times (spied); a changed directory IS re-read (mtime
  still works through the walk); the cache is per instance; the timing line names the
  root and the walk phase; the read/hit counters go `(N, 0)` cold then `(0, N)` warm.
- `scidb/tests/test_locations.py` — new `TestLocationStatesReusesTheDiscoveryWalk`,
  4 tests, same `discover`-counting pattern as the existing canvas-badge test: second
  call within TTL does not walk and gives the same tree; the shared
  `clear_discovery_cache()` clears it; the hit is logged as a hit; TTL expiry (clock
  jumped via monkeypatch, not slept) walks again.

### Answer to "does this also explain the slow plotting?" — NO. Different problem.

The plot path never calls `discover()` (`resolve_figures → _load → get_table` has no
filesystem walk). The one `plot_resolve` in `output.txt` that completed, at 15:55, gave
the first real phase breakdown of a plot on this data:

```
resolve_one: band, TOTAL=357.5s  (plan=194.4s)
  y limits over the whole dataset: 1 group(s)        <- lands at ~195s
build_figure: TOTAL=153.6s  (explode=152.9s, everything else < 1s)
exploded 1-D measure 'RawEMG': 4190 row(s) -> 174,158,480 sample(s)
downsampled 174,158,480 row(s) to 20,003 for transport (stride=8707)
```

Two costs, each ~150-195 s, both pure Python over 174 M samples:

- **`plan` = 194 s is `y_limits`.** `_raw_extents` walks all 4190 cells in a Python
  loop calling `np.nanmin`/`np.nanmax` per cell. The "48 rows" assumption in its own
  comment is confirmed as the hotspot. (Earlier in this plan I withdrew this suspicion
  because `_build_plan` had not run yet — it has now, and the suspicion was right.)
- **`explode` = 153 s.** `pandas.explode` materialises a 174 M-row frame of boxed
  floats, then `downsample` immediately keeps 20,003 of them. 99.99% of the exploded
  rows are built to be thrown away.

**Plan reorder:** Stage 5 (decimate BEFORE explode, in the resolve path only) moves to
the top for the plot surface, with `y_limits` vectorisation alongside it. Stage 3 (filter
pushdown) remains right for "plot ONE of 419 locations" but is not what the 15:55 run
was waiting on. Stage 4 (Arrow) still helps `fetch` (85-97 s) but does not touch either
of these two.
