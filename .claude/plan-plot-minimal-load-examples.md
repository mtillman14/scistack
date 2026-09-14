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

## 7. MEASURED on the real database (2026-09-13, `.claude/measure_plot_queries.py` part 1)

RawEMG_data, column RHAM, 419 records, 17.4 M samples per column (first record 79,534
samples — cells are ragged, 41.5 k is the mean). GUI closed, read-only connection.

| query | one column | ×10 columns |
|---|---|---|
| metadata only (record_id + schema keys) | 0.006 s | — |
| `len()` per record | 0.163 s | — |
| **`fetchall` (today's `load_variable`)** | **4.83 s** | 48 s (86 s in-GUI, one query for all 10) |
| **`.arrow()`** | **0.19 s** | ~2 s |
| `.df()` | 0.31 s | ~3 s |
| `.fetchnumpy()` | 0.27 s | ~3 s |
| one record by record_id | 0.11 s | — |
| one record, UNNEST → `.df()` (79 k rows) | 0.12 s | — |
| y extents `MIN(list_min(list_filter…))` | 0.70 s | 7 s |
| UNNEST all → `.df()` (committed `explode_series`) | 4.9 s | 49 s |
| `GROUP BY pos` avg/stddev/count → `.df()` | 2.2 s | 22 s |
| `GROUP BY subject, pos` (proposed `summarize_series`) | 4.3 s | **43 s** |
| same with `pos % 200 = 0` | 0.8 s | 8 s |

### What this changes

1. **`fetch` is 95 % Python boxing, not DuckDB.** `.arrow()` reads the same lists 25×
   faster. Switching `load_variable` from `_fetchall` to `.df()`/`.arrow()` takes
   `plot_describe` from 91 s to ~6 s (3 s fetch + 3 s `attach_variants`) with NO change
   to `get_table`'s shape. **This is the first fix, not the last** — it is what makes the
   panel open on this data at all (the 18:29 run: describe timed out in the fetch).
2. **§1's "returns ONLY what is drawn via SQL" is the wrong tool for per-sample
   reductions on this data.** D.3 was estimated at 5–10 s and measures 43 s; the committed
   SQL `y_extents` is 7 s where `np.nanmin` over the same 1.4 GB is a fraction of a
   second. DuckDB is the right place for SELECTING (rows, columns — both measured free)
   and the wrong place for REDUCING 1-D series. The reducer seam stays; the DuckDB
   implementation of `y_extents`/`explode_series` should give way to numpy over
   Arrow-fetched cells, and `summarize_series` should never be written in SQL.
3. **`pandas.explode` (110 s) goes regardless**; band/bar need a padded 2-D array per
   group and per-position `nanmean`/`nanstd`, never a 174 M-row long frame.

Part 2 of the script (numpy over the Arrow cells: split, extents, explode, summarize
all, summarize per subject) is the measurement that decides (2). Pending.

### Revised order

1. ~~Lock model~~ — done (§6), still needed so nothing silently falls back.
2. **Arrow/`.df()` fetch in `load_variable`** (cells become `np.ndarray`). Fixes describe.
3. Metadata-only `get_table` for describe/variant_graph/location_tree (0.006 s — for
   the 3 s of `attach_variants` and the memory, not the fetch any more).
4. numpy reductions in the reducer: extents, explode-by-concatenate, padded summarize
   for band/bar. Sized by part 2.
5. Location + field pushdown (stage 3 as before) — measured free, keeps plots of one
   location at one record.

## 8. MEASURED part 2 (2026-09-13) — numpy over Arrow cells, one column, all 419

`.arrow()` on this DuckDB returns a lazy `RecordBatchReader`; part 1's 0.18 s was the
reader's construction. Materialized (`fetch_arrow_table`, deprecated in favour of
`to_arrow_table`): **0.33 s**. `.df()` **0.26 s** is the honest number for the stack.

| reduction | pandas (18:02 run ÷ 10) | DuckDB SQL | numpy on Arrow cells |
|---|---|---|---|
| fetch | 4.7 s boxed | — | **0.26–0.33 s** |
| split ListArray into per-record views | — | — | 0.001 s (zero copy) |
| y extents | ~37 s | 0.63 s | **0.015 s** |
| explode → LINE frame (row, pos, val) | ~11 s | 4.6 s | **0.39 s** |
| summarize per position, ALL records, padded | ~37 s | 2.1 s | 2.4 s (pad is 419 × 325,855) |
| summarize per position PER SUBJECT (band) | ~37 s | 4.2 s | **0.75 s** |

Cells are ragged from **8,316 to 325,855** samples (mean 41.5 k). Padding to the
group's max is what costs the all-records case; `np.bincount(pos, weights=val)` /
`bincount(pos)` / `bincount(pos, weights=val²)` is O(N) with no pad and should bring
summarize to ~0.2 s per column. Implementation choice, to be measured in the parity
suite, not here.

**Decision: DuckDB selects, numpy reduces.** The committed `DuckDBReducer.y_extents`
(40× slower than numpy) and `explode_series` (12×) are replaced, not extended.
`summarize_series` is never written in SQL. The `Reducer` seam and the parity suite
stay — the pandas reference is still the oracle; only the fast implementation changes
from SQL to numpy.

Projected band plot over the whole variable (×10 columns): fetch 3 s + `attach_variants`
3 s + summarize 2–7.5 s ≈ **8–13 s** from ~580 s; a re-resolve on the cached frame
2–8 s. A one-location LINE: ~1 s.

### Final stage order

| # | stage | fixes | size |
|---|---|---|---|
| 1 | lock model | reducer runs at all | done §6 |
| 2 | `load_variable` fetch via `.df()`; cells are `np.ndarray` | **`plot_describe` 91 s → ~6 s** — the panel opens | small; parity suite must pass |
| 3 | numpy reducer: extents (nanmin/nanmax), explode by concatenate, band/bar summarize by bincount | resolve 500 s → seconds | medium; replaces SQL in `DuckDBReducer` |
| 4 | metadata-only `get_table` for describe/variant_graph/location_tree | the 3 s `attach_variants` + 1.4 GB on open | medium |
| 5 | location + field pushdown | one-location plot reads one record | small |

## 9. Stage 2 implemented (2026-09-13) — DataFrame fetch, uncommitted, unrun

| File | Change |
|---|---|
| `scistackplotdb/load.py` | `load_variable` fetches through `_fetchdf` (already the sanctioned idiom in sciduckdb), never `_fetchall`. Schema keys are `CAST(... AS VARCHAR)` in the query so a numeric key with a NULL cannot arrive as float64 and stringify as `"1.0"`; `_key_text` maps None/NaN → None. `_normalize_cell` runs PER CELL (4190 calls, not 174 M): leaves 1-D ndarrays alone, stacks a 2-D cell's object-array-of-rows into one `(rows, cols)` float array (so `classify_value` sees ndim 2), turns ragged rows into a list of row arrays (still classifies MATRIX_2D), and converts a Python-list cell once if a DuckDB build still boxes on the pandas path — the "loaded" line then says `boxed`, which is the tell |
| `scistackplotdb/tests/test_load_fetch.py` | NEW: 1-D cell is float64 ndarray; dict fields each ndarray; 2-D cell is `(3, 4)` ndarray; scalar column numeric; "loaded" line says `ndarray` not `boxed`; no payload column through `_fetchall`; the load query goes through `_fetchdf` exactly once; zero-padded keys survive as str; subject-level `Mass` has None (not "nan") for session/trial and `levels == ["subject"]`; the VARCHAR cast is in the SQL; `_normalize_cell` unit cases |
| `scistackplotdb/tests/test_variant_table.py` | the "no data column is even queried" spy now watches `_fetchdf` as well as `_fetchall` |

### Not verified

Nothing run. The one thing only a real DuckDB can answer is what `.df()` hands over for
a `DOUBLE[]` cell on the installed version — `test_a_1d_cell_is_a_float_ndarray` is
the question, and `_normalize_cell` is the fallback if the answer is "a list". The
measured 0.26 s says it is not boxing on the user's build.

Expected on the next real run: `[timing] load_variable: RawEMG … fetch=~3s` and
`loaded RawEMG: … ~1.4GB ndarray`; `plot_describe` held for ~6 s; the panel opens.
`plot_resolve` will still be slow (stage 3: the SQL reducer's UNNEST at 49 s and the
pandas explode fallback are still the paths) — that is the next stage, not a regression.

### Stage 2 follow-ups found by the parity suite (2026-09-13)

1. **`Series.map` / dtype inference** — building the cell column through `map` let
   pandas reinterpret a column of same-length arrays. Cells and keys are now built via
   `_object_column` (preallocated object array, one assignment per cell).
2. **`np.float64` objects after `explode`** — pandas' `to_numeric` calls `len()` on numpy
   scalars (`len() of unsized object`). New `scistackplot/numeric.py::coerce_numeric`
   replaces `pd.to_numeric` at every site downstream of an explode or over a cell
   column (`reduce._explode_1d`, `_panel_frame`, both `ylimits` sites). Tested in
   `scistackplot/tests/test_numeric.py`.
3. **NaN is stored TWO ways in the same column.** `probe_list_cells.py` on duckdb 1.5.5:
   the bulk Arrow write keeps NaN as a real NaN double; scidb's single-record
   `INSERT … VALUES (?, ?)` parameter binding stores it as a **NULL element**
   (`v[2] IS NULL`). `fetchall` spelled both as `nan`/`None` and nobody noticed. `.df()`
   spells the NULL-element cell as a `np.ma.MaskedArray`, and `np.asarray()` on that
   drops the mask and exposes `0.0` — a NaN sample silently became a number. Fixed on
   the read side (`_normalize_cell` → `_float_row` fills masks with NaN; tests in
   `test_load_fetch.py::TestNaNSamplesSurviveTheFetch`). The reducer SQL already guards
   both spellings (`IS NOT NULL AND NOT isnan`). **Open, scidb-level:** whether the
   single-record path should bind NaN as NaN so the column has one representation —
   a storage decision, and existing databases hold NULLs either way, so the reader
   keeps handling both.
4. `TestAggregateOnA2DMeasure`'s strict xfail XPASSed — ndarray cells make
   `groupby.mean()` average matrices elementwise. Now a real assertion on the mean.

**Status 2026-09-13:** stages 1 and 2 built; scistackplot, scistackplotdb and
scistack-gui plot tests all pass (user-run). Uncommitted. Not yet run against the real
database since stage 2 — the `plot_describe` ~6 s expectation in §9 is still a prediction.

## 10. Stage 3 implemented (2026-09-13) — numpy reducer, uncommitted, unrun

| File | Change |
|---|---|
| `scistackplot/series_stats.py` | NEW. `cell_arrays`, `pad`, `explode(arrays, max_points)` (record-major, NaN gap preserved, stride folded in so only kept rows' labels are gathered), `position_mean`, `position_stats(statistic, error)` with pandas' semantics (nanmean/nanmedian, `nanstd(ddof=1)` → 0.0 for n=1, linear percentiles, count = non-NaN) |
| `scistackplot/reducer.py` | Protocol grows `collapse_series` and `summarize_series`; `explode_series` takes `max_points` and returns `(frame, index, total)`. `PandasReducer` implements the new methods by delegation (`_explode_1d` + `_collapse_aggregates` + new `reduce._summarize_exploded`). NEW `NumpyReducer(PandasReducer)`: aggregated `y_extents` (raw already was per-cell numpy in the reference — deferred), `explode_series`, `collapse_series`, `summarize_series`; scalars/2-D/pre-exploded defer to the reference |
| `scistackplot/reduce.py` | `_build_figure`: a nested 1-D measure takes ONE of three routes — BAND/BAR → `summarize_series` per panel from the cells, stride applied to the SUMMARY afterwards; AGGREGATE role → `collapse_series` (exploded, small) then the usual stride; else → `explode_series(max_points=…)`. `_summarize_exploded` added (the reference's band tail) |
| `scistackplotdb/source.py` | `ScidbSource(fast=True)` attaches `NumpyReducer`; `pushdown` is gone |
| `scistackplotdb/reducer.py` | DELETED (the DuckDB-SQL reducer; lost every measurement in §8) |
| `scistack-gui/plot_service.py` | the hold returns to the LOAD only (`_load`; `_loaded`/`ExitStack` gone): `resolve` is pure memory again. Docstring carries the two flips of 2026-09-13 |
| tests | parity suite's fast side re-pointed to numpy (`numpy_source`); new `TestNumpyCollapseEqualsPandas`, `TestNumpySummarizeEqualsPandas` (8 spec shapes), `TestNumpySummarizeEdges` (n=1 → zero spread, all-NaN cell not a replicate), `TestBandThroughResolveIsSummarisedBeforeStriding`, `TestReductionNeedsNoDatabase` (every kind resolves with the connection CLOSED — the reason the GUI hold can end at the load); `TestAnUnreachableDatabaseRaises` removed with the SQL reducer; GUI lock tests flipped back to release-before-reduce; `test_narration` phase list updated |

### Behaviour change, deliberate: BAND/BAR are summarised before striding

The old figure order was explode → stride → summarise, so at scale a band was the
mean ± SD of one sample in 8,707 (§D.1). Now every sample contributes and the transport
stride thins the summary rows. Both reducers follow the new order (it lives in
`_build_figure`), so parity holds; the drawn band at a budget is *different from before*
and *correct*. Only BAND/BAR: LINE/box/violin keep explode → stride.

### Not verified

Nothing run. The parity suite is the check; `assert_frame_equal`'s default tolerance
absorbs summation-order differences between `nanmean` and pandas' mean.

### Follow-ups noted, not done

- `position_stats` pads to the group's longest cell; a `bincount` form avoids the pad
  for very ragged groups (measured 2.4 s vs 0.75 s for the whole-variable band).
- Box/violin on 1-D still stride before computing quantiles (pre-existing).
- `y_extents` (plan) and `summarize_series` (panels) compute the same statistics twice
  for a band; a memo keyed on the plan would halve it.
- `DEFAULT_REDUCER` stays the pandas reference; `CsvSource`/`DataFrameSource` could
  carry `NumpyReducer` once their cells are known to be arrays.
