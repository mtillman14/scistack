# Minimal-load plotting: four worked examples, today vs target

Written 2026-09-13 to check, BEFORE building, that the plan ("metadata-only table +
every per-sample operation in DuckDB + location/field pushdown") puts the speedup where
the time is actually spent. Each example traces one plot request from the GUI call to
the 20,003-row transport frame: what SQL runs, what leaves DuckDB, by which fetch
method, and what pandas then does with it.

Numbers are from the real data (`scidb.log`, 17:58–18:10 run):

```
RawEMG   DOUBLE[] x 10 fields   419 records   ~41,565 samples/cell
         174,158,480 samples = 1.4 GB float64 = 5.2 GB boxed Python floats
one location = 1 record = 10 cells = 415,650 samples = 3.3 MB float64
```

The scalar examples use a hypothetical `StepLength DOUBLE`, one per trial, same 419
locations.

## 0. What the 18:02 run proved (the finding that reorders everything)

| phase | measured | method |
|---|---|---|
| `get_table(RawEMG)` | **92.4 s** | `load_variable(include_data=True)` → `_fetchall` → 174 M boxed floats |
| `build_plan.y_limits` | **375.1 s** | pandas fallback (`ylimits._aggregated_extents`) |
| `build_figure.explode` | **110.5 s** | pandas fallback (`_explode_1d`) |
| `panel_frames` (summarize 174 M rows) | 0.8 s* | pandas groupby — *after downsample to 20 k, so cheap here |
| **total to first pixel** | **~580 s** | against a 30 s transport timeout |

*Both "pandas fallback" lines are preceded by `_fetchone FAILED … Connection already
closed!`* — `resolve` runs after `plot_service._load` has released the per-request
DuckDB hold, the reducer's first lookup raises, `_address_rows` swallows it
(`except Exception: return None`) and reports "not addressable". **The DuckDB reducer
committed in a556ee65 has never executed on this database.**

Two consequences for the plan:

1. **Lock model first.** Nothing below happens until `resolve` runs under the hold (or
   the reducer takes its own). And `_address_rows` must fall back only on genuine
   non-addressability (no `record_id`, already-exploded table); a `ConnectionException`
   is a bug and must raise. A fallback that hides a closed connection cost 8 minutes
   and produced a correct plot, which is the worst kind of failure to have.
2. **Even the reducer as written is not enough for example D** (see D.2): `explode_series`
   reproduces the full 174 M-row exploded frame in pandas by design, and `_summarize`
   then groups it in pandas. That is where band/bar must change.

## 1. The target pipeline, once, so the examples can refer to it

```
get_table(var)                      metadata frame only: record_id, schema keys, variant
                                    columns, ColName if melted. NO payload. 0.1 s measured.
                                    + WHERE on schema keys when the spec carries a
                                      location_filter (Stage 3 pushdown) [optional: the
                                      frame is 419 rows either way; the payload is what
                                      matters, and it is no longer here]
apply_filters(frame)                419-row pandas mask, ~0 s
reducer.<op>(frame, …)              ONE query per (table, column, op), joined on
                                    record_id to the filtered frame (registered view),
                                    returns ONLY what is drawn, via .df()/.fetchnumpy()
                                    (numpy buffers; never fetchall on a data column)
downsample / render                 pandas over ≤ ~10^5 rows
```

Reducer operations the four examples need (existing = built in a556ee65; NEW = not):

| op | shape | returns | status |
|---|---|---|---|
| `values(frame, measure)` | scalar | the measure column for the frame's record_ids | NEW (trivial; replaces the payload column the metadata frame no longer has) |
| `y_extents` raw | any | (low, high) per scope group | existing |
| `y_extents` aggregated | any | (low, high) of centre±spread per scope group | existing |
| `explode_series` | 1-D | one row per sample, record-major | existing — right for LINE, wrong tool for BAND/BAR (see D) |
| `summarize_series(frame, spec, groups)` | 1-D | centre, low, high, count per (groups…, position) | NEW — the band/bar path |
| in-query stride | 1-D band/bar | `WHERE pos % stride = 0` | NEW, optional, proven pixel-identical for band/bar only (plan §12) |

## Example A — scalar, ONE location, kind = scatter/strip

Spec: `StepLength`, `location_filter = {subject:03, session:2, speed:fast, trial:1}`,
no aggregation.

### A.1 Today

| step | SQL / method | leaves DuckDB | pandas work |
|---|---|---|---|
| `get_table` | `SELECT record_id, value, s.* FROM StepLength_data … WHERE type=?` — no location filter | 419 doubles + keys, `fetchall` | 419-row frame |
| `attach_variants` | `variant_identity_batch(419 ids)` | provenance rows for 419 records | join |
| `apply_filters` | — | — | mask to 1 row |
| `y_extents` | (reducer, if it ran) `MIN/MAX … GROUP BY` over 1 row | 1 row | — |
| `_panel_frame` | — | — | 1 value |

Cost is not the payload (419 floats). It is `attach_variants` over 419 records —
**7.1 s** best case, 104 s under contention (plan §7) — and the connection hold around
it. **This example is already "fast enough" and the plan must not regress it.**

### A.2 Target

| step | SQL / method | leaves DuckDB | pandas |
|---|---|---|---|
| `get_table` (metadata) | `SELECT t.record_id, s.* FROM StepLength_data t JOIN _record r … LEFT JOIN _schema s … WHERE r.type=? [AND s.subject=? AND s.session=? …]` | 1 row (with pushdown) / 419 rows (without) | 1/419-row frame |
| `attach_variants` | over 1 id (with pushdown) | 1 record's provenance | — |
| `values` | `SELECT g.__row, t.value FROM StepLength_data t JOIN _plot_groups g USING (record_id)` via `.df()` | 1 double | assign column |
| `y_extents` raw | existing SQL | 1 row | — |

**Where the win is:** `attach_variants` 419 → 1 record. That only happens with the
location pushdown (Stage 3) — a metadata-only load without pushdown still attaches
variants for all 419. So for the scalar-one-location case, **pushdown is the fix and
"metadata-only" is neutral.** Minimal data accessed: 1 record's metadata + 1 double. ✓

**Regression guard:** the `values` op must be a `.df()` fetch of the filtered rows only,
and A must still produce the identical panel frame from a metadata table as from a
payload table (parity test on `CsvSource` vs `ScidbSource` for the same fixture).

## Example B — scalar, MANY locations, kind = bar (mean ± SD per subject)

Spec: `StepLength`, no location filter, roles `{subject: X, session/speed/trial: FREE}`,
`aggregate = mean ± SD`. One bar per subject over its ~20 trials.

### B.1 Today

| step | leaves DuckDB | pandas |
|---|---|---|
| `get_table` | 419 doubles, `fetchall` (boxing 419 floats is nothing) | 419 rows |
| `attach_variants` | 419 records' provenance, 7 s+ | join |
| `y_extents` aggregated (reducer) | N_subj rows `(low, high)` | — |
| `_panel_frame` → `_summarize` | — | groupby(subject) over 419 rows, ms |

Again the payload is irrelevant; `attach_variants` and the hold dominate.

### B.2 Target

Identical to B.1 except `get_table` is the metadata query and `values` fetches the 419
doubles by `.df()`. `_summarize` stays in pandas — pushing a groupby over 419 numbers
into SQL buys nothing and adds a code path.

**Verdict for scalars (A, B): the plan is correct but the speedup there is small and
comes from pushdown + not re-attaching variants for records the plot filtered away.
Neither scalar case is what times out.** The plan's cost is one new op (`values`) and a
parity test; the benefit is that the scalar and 1-D paths share one table shape.

Minimal data accessed: 419 doubles, once. ✓ (Cannot be less: every value is drawn.)

## Example C — 1-D, ONE location, kind = line (10 fields → 10 facet panels)

Spec: `RawEMG`, `location_filter = {subject:03, session:2, speed:fast, trial:1}`,
roles `{ColName: FACET}`, no aggregation. Draws 10 traces of ~41.5 k samples.

### C.1 Today

| step | leaves DuckDB | method | pandas | measured |
|---|---|---|---|---|
| `get_table` | **174 M samples** (all 419 records × 10 fields) | `fetchall` → boxed | melt to 4190 rows | **92 s** |
| `apply_filters` | — | — | 4190 → 10 rows | 0 s |
| `y_extents` | (fallback) | — | `nanmin/nanmax` over 10 cells | ms once filtered |
| `explode` | (fallback) | — | `pandas.explode` 10 cells → 415 k rows | ~0.3 s |
| `downsample` | — | — | stride 20 → 20 k rows | ms |

**99.8 % of the time is loading 418 records the filter then discards.** Everything
after the filter is already sub-second in pandas.

### C.2 Target

| step | SQL | leaves DuckDB | method | pandas |
|---|---|---|---|---|
| `get_table` (metadata) | schema-key `WHERE` from `location_filter` | 1 record → 10 melted rows (ColName) | `fetchall` on keys only (strings, fine) | 10-row frame |
| `attach_variants` | 1 id | — | — | — |
| `y_extents` raw | `SELECT MIN(list_min(list_filter(v.val,…))), MAX(…) FROM RawEMG_data t JOIN _plot_groups g USING(record_id), LATERAL (SELECT t."<field>" AS val) v` ×10 fields (one per melted column) | 10 × (low, high) | `fetchall` on 2 doubles | dict |
| `explode_series` | `SELECT g.__row, v.pos-1, v.val FROM RawEMG_data t JOIN _plot_groups g …, UNNEST(t."<field>") WITH ORDINALITY v WHERE NOT isnan(v.val)` ×10 | **415,650 rows** (3 int/double cols) | **`.df()` → numpy** | `take` carries 10 rows' labels → 415 k rows |
| `downsample` | — | — | — | `iloc[::20]` → 20 k |

DuckDB reads: the `record_id` column of `RawEMG_data` (419 values, to find 1) and the
list payload of **1 row × 10 columns**. DuckDB does not read the other 418 rows' lists —
list children are a separate column store, fetched per matched row.

Expected wall time: metadata 0.1 s + 10 small queries ~0.1 s + explode `.df()` of 415 k
rows ~50 ms + pandas ~0.2 s ≈ **< 1 s** (from 92 s). Minimal data accessed: exactly the
10 cells drawn. ✓

**One refinement worth doing here:** 10 queries (one per melted field column) is the
existing `_reduce_by_group` "one per (table, column) pair" shape. For a one-record plot
it is 10 round-trips of ~5 ms — fine. For D it is 10 queries each scanning 419 records'
lists; still fine (DuckDB scans one column per query, 140 MB each). Not worth a UNION.

**In-query stride is NOT applied for LINE** (plan §12: row-major stride across records
≠ per-record `pos % stride`). With one record they coincide, but the rule is per kind,
not per row count. Leave the stride in pandas; 415 k rows is cheap.

## Example D — 1-D, ALL 419 locations, kind = band (mean ± SD per position)

Spec: `RawEMG`, no location filter, roles `{ColName: FACET, subject: COLOR,
session/speed/trial: FREE}`. This is the 18:02 run. Drawn output: 10 panels ×
N_subj bands × 41.5 k positions, then strided to 20,003 rows total.

### D.1 Today (18:02 run, all pandas)

| step | leaves DuckDB | pandas | measured |
|---|---|---|---|
| `get_table` | 174 M boxed | 4190 rows of lists | **92 s** |
| `y_extents` aggregated (fallback) | — | explode 174 M, groupby (ColName, subject, pos), centre±SD, extremes | **375 s** |
| `explode` (fallback) | — | `pandas.explode` → 174 M rows | **110 s** |
| `_summarize` | — | groupby (X=pos, COLOR=subject) over 174 M rows … | *hidden*: `downsample` ran BEFORE `_panel_frame` in this kind? No — see note |
| `downsample` | — | 174 M → 20 k | ms |

Note: `_build_figure` order is explode → `_collapse_aggregates` (no-op: no AGGREGATE
roles) → **downsample** → `_panel_frame`/`_summarize`. So today's band is summarised
over a **strided subsample of the raw samples**, not over all replicates per position —
`panel_frames=0.783s` confirms it ran on 20 k rows. (That is a separate fidelity issue:
a mean±SD band over 1/8707th of the samples. Recorded here because the target below
changes it, deliberately, to the statistically correct thing; the parity test for D must
compare against a full-resolution pandas reference, not against today's output.)

### D.2 With the committed reducer, once the lock is fixed (NOT sufficient)

| step | SQL | leaves DuckDB | pandas | estimate |
|---|---|---|---|---|
| `get_table` (payload) | unchanged | 174 M boxed | | **92 s** |
| `y_extents` aggregated | `UNNEST … GROUP BY ColName, subject, pos` → centre±SD → `MIN(low), MAX(high) GROUP BY scope` | N_scope rows | — | **~3–8 s** (DuckDB, 174 M doubles, vectorised) |
| `explode_series` | `UNNEST …` ×10, `ORDER BY __row, pos` | **174 M rows × 3 cols** via `.df()` (4.2 GB) | `take` carries ~8 label columns → 174 M × 8 object pointers ≈ **11 GB**; sort | **60 s+, likely OOM-adjacent** |
| `downsample` → `_summarize` | — | — | 20 k rows | ms |

Fixing the lock alone turns 375 s into seconds for `y_limits` but leaves 92 s of
payload load and an explode that materialises 174 M rows in pandas to draw 20 k. Still
minutes. **This is why `explode_series` is the wrong op for band/bar**, and why the plan
must add `summarize_series`.

### D.3 Target

| step | SQL | leaves DuckDB | method | pandas |
|---|---|---|---|---|
| `get_table` (metadata) | keys + variants only | 4190 rows (419 × 10 ColName) | `fetchall` on strings | 4190-row frame, 0.1 s |
| `attach_variants` | 419 ids, batched | provenance | | 7 s today — **now the largest remaining cost; see §3** |
| **`summarize_series`** (NEW) | per field: `SELECT g."subject", v.pos-1 AS pos, avg(v.val) AS centre, stddev_samp(v.val) AS sd, count(v.val) AS n FROM RawEMG_data t JOIN _plot_groups g USING(record_id), UNNEST(t."<field>") WITH ORDINALITY v WHERE NOT isnan(v.val) GROUP BY g."subject", pos` | **41.5 k × N_subj rows per field** (N_subj=20 → 830 k rows/field, 8.3 M total, ~330 MB) | `.df()` | low/high = centre ∓ f(sd, n) — vectorised, one line |
| `y_extents` aggregated | **not a second query**: `min(low)`, `max(high)` over the frame above, per scope | — | — | ms |
| `downsample` | — | — | — | `iloc[::stride]` over 8.3 M → 20 k |
| `_panel_frame` | — | — | — | already summarised; passthrough |

DuckDB reads: every list once (174 M doubles, ~1.4 GB columnar, one pass per field
column). It returns 8.3 M summary rows, not 174 M samples. Estimate: **~5–10 s** total,
dominated by the GROUP BY over 174 M and the 330 MB `.df()`.

### D.4 Target + in-query stride (optional, band/bar only)

Add `AND (v.pos-1) % :stride = 0` to `summarize_series`, with
`stride = ceil(positions_per_record × N_subj × 10 / 20 000)` computed from
`SELECT max(len(t."<field>"))` (ms). Plan §12 proved this is pixel-identical for
band/bar (per-position statistic, then stride ≡ stride, then per-position statistic,
because each kept position's group is the same records either way). Output: **20 k rows
straight from DuckDB**, `.df()` of ~1 MB. Estimate: **~2–4 s**, all of it the GROUP BY
scan. Not applicable to LINE (C) or box/violin.

Minimal data accessed: every sample is a member of some drawn mean, so the 174 M-sample
scan is the floor; what changes is that **nothing larger than the drawn figure ever
leaves DuckDB.** ✓

## 2. Summary table — does the plan put the speedup where the time is?

| example | today | after lock fix only | after metadata-only + pushdown | + `summarize_series` | + in-query stride |
|---|---|---|---|---|---|
| A scalar, 1 loc | ~7 s (variants) | same | **< 0.5 s** | n/a | n/a |
| B scalar, bar | ~7 s (variants) | same | ~7 s (variants; irreducible without §3) | n/a | n/a |
| C 1-D, 1 loc, line | **92 s** | 92 s | **< 1 s** | n/a | n/a (must not) |
| D 1-D, 419 loc, band | **~580 s** | ~160 s+ (92 load + explode) | ~70 s (explode `.df()` 174 M) | **~5–10 s** | **~2–4 s** |

Reading the table:

- The lock fix is necessary for everything and sufficient for nothing.
- Metadata-only `get_table` is what fixes C and is the precondition for D.
- Location pushdown is what fixes A (variants) and is what makes C read one record
  rather than filter 419 in pandas — cheap either way for the metadata frame, but it is
  the only thing that reduces `attach_variants`.
- D needs its own op. The committed `explode_series` should stay for LINE/scatter (C);
  band/bar must not go through it.

## 3. The cost the plan does not address: `attach_variants`

`variant_identity_batch(419 ids)` = **7.1 s** on an idle connection (plan §7, table).
After D.4 it would be ~70 % of the request. Not a plotting-layer problem (scidb
provenance), and it only matters when the plot spans many records — but it should be
on the table as the next bottleneck, with its own `[timing]` phases inside
`variant_identity_batch` before anyone guesses at it. Out of scope here; noted so that
"D is still 7 s" is not a surprise.

## 4. Tests that pin "minimal data, bulk method" per example

All four as fixtures in `scistackplotdb/tests` (the parity fixture already has NaN,
ragged, n=1 cases):

1. **Boxing door shut:** spy `_fetchall`; assert no SQL it receives names a data column
   of the plotted variable, for every example. (Extends the Stage 3 test.)
2. **Row counts out of DuckDB:** spy `_fetchdf_with_frame`; assert returned row counts:
   A = 1, B = 419, C = 415,650 (or the fixture's equivalent), D = positions × N_subj ×
   fields (D.3) or ≤ max_points (D.4).
3. **Location pushdown:** the emitted metadata SQL for A and C contains one `WHERE`
   predicate per schema key in `location_filter`, and the frame has 1 record's rows.
4. **Full-resolution parity for D:** `summarize_series` equals a pandas reference that
   explodes ALL samples and runs `_summarize` — not today's strided-then-summarised
   output (see D.1 note). The reference is the existing `PandasReducer`, so this is one
   `assert_frame_equal` per statistic row of plan §4.
5. **Stride identity for D.4:** `summarize_series(stride=s)` ≡ `summarize_series().iloc[::s]`
   per (facet, color) group. And a test that `stride` is refused for LINE and box/violin.
6. **Closed connection raises:** `_address_rows` with a closed `_duck` raises
   `ConnectionException`; only a frame without `record_id` / an exploded table returns
   `None`. Plus a GUI test that `resolve_figures` holds `db_connection` across
   `resolve` (assert the reducer's SQL runs inside the hold — log line
   `plot_resolve: held the DuckDB connection` must come AFTER `[timing] resolve`).
7. **Standalone guarantee:** `CsvSource` / `DataFrameSource` resolve A–D with no database
   and match the pandas reference (the reduction plan's Stage 5 test, still unwritten).

## 5. Stage order that falls out

1. Lock model: `resolve` under the hold; `_address_rows` no longer swallows
   `ConnectionException`; WARN (not INFO) on any real fallback. **Then re-run D once** —
   this alone tells us whether the y_extents SQL over 174 M is 3 s or 30 s, which sizes
   everything after.
2. Metadata-only `get_table` + `values` op + standalone parity (fixes C).
3. Location + field pushdown in `load_variable` (fixes A; reduces `attach_variants`).
4. `summarize_series` for band/bar; `y_extents` aggregated derived from it, not
   re-queried (fixes D to ~5–10 s).
5. In-query stride for band/bar (D to ~2–4 s), gated on the measurement from step 1/4.
6. Re-measure A–D on the real database; record in this file.

## 6. Stage 1 implemented (2026-09-13) — lock model, uncommitted, unrun

| File | Change |
|---|---|
| `scistackplotdb/reducer.py` | `_address_rows` no longer wraps `table_name_for` in `except Exception: return None`. Neither lookup raises for an unknown variable (`table_name_for` defaults to `<name>_data`, `data_columns_for` returns `[]` for a missing table), so any exception there is the database being unreachable and now propagates. The genuine structural fallback (no data table) logs at WARN naming the variable |
| `scistack-gui/services/plot_service.py` | `_load` → `_loaded`, a context manager holding `db_connection` for its whole block; `_load` remains for callers that only load (`capabilities_for`, `export_code`). `resolve_figures` resolves INSIDE the hold, renders outside. `save_figure` enters the hold via `ExitStack` across its `load` and `resolve` phases and releases before `render_and_write` — the "file is free for MATLAB while matplotlib draws" promise is kept; the docstring on `start_save_job` now says exactly that |
| `scistack-gui/db.py` | `db_connection` docstring: the hold covers load AND reduce |

### Tests added

- `scistackplotdb/tests/test_reducer_parity.py::TestAnUnreachableDatabaseRaises` —
  `y_extents` and `explode_series` on a closed `_duck` raise `duckdb.ConnectionException`
  and log no "pandas fallback"; a variable with no data table still falls back, at WARN,
  naming the variable.
- `scistack-gui/tests/test_plot_service.py` (new `per_request_policy` fixture — the suite
  runs under `persistent`, where `db_connection` is a no-op, which is why nothing caught
  this): `resolve` and `resolve_one` see `_db_open is True`; the save sees it True during
  `resolve` and False during `render_matplotlib`; an invalid spec's early return still
  releases; no "Connection already closed" / "pandas fallback" in the log.

### Not verified

Nothing run (no Python here). The per-request fixture's one assumption: releasing closes
`_db._duck` and the next acquire reopens it through `DatabaseManager.reopen()` — the same
path production takes, and the fixture's `db` IS `_gui_db._db`, so the teardown's second
`close()` is a no-op on duckdb.

### What to look for on the next real run (D, the band over 419)

```
plot_resolve: held the DuckDB connection for <N>s      <- N now INCLUDES the reduction
[timing] y_extents(duckdb): Series … aggregated=…      <- must appear; no "pandas fallback"
[timing] explode_series(duckdb) …
[timing] build_plan: … y_limits=<seconds, not 375>
```

`get_table` will still be ~92 s (payload load) and `explode` will still materialise 174 M
rows in pandas via `.df()` — those are stages 2 and 4 of §5. This stage exists to get the
first real number for the y-extents SQL over 174 M samples, which sizes both.
