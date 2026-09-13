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
