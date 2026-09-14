> **SUPERSEDED 2026-09-13** by `.claude/plan-plot-minimal-load-examples.md` §8: the DuckDB-SQL reducer this plan built was measured 12–40× slower than numpy over the loaded cells and removed. Kept for the parity fixture design (§4) and the measurements.

# DuckDB-native reduction for plotting (design A)

**Date:** 2026-09-13
**Supersedes** the `y_limits` + explode fixes in `plot-at-scale-plan.md` §10, and Stage 5
there. Stage 3 (filter pushdown) is absorbed: the filter and the reduction become the
same query.
**Decision taken:** (A) — targeted source methods behind the existing `DataSource`
protocol seam, pandas fallback for in-memory sources. Not (B), the whole-reduce-in-source.
**User constraint:** must work for every plot kind, over DOUBLE, DOUBLE[], and DOUBLE[][].

---

## 1. First principles

The arrays are stored as DuckDB `DOUBLE[]` (`database.py:1060`) — DuckDB's native `LIST`
type, held in columnar storage, with a full vectorised C++ vocabulary over it
(`list_min`, `list_max`, `UNNEST`, `list_select`, `list_aggregate`). **Not one of those
functions is called anywhere in scidb, scistackplotdb, or sciduckdb.** Every consumer
does `fetchall()`, which boxes each element into a Python float (32 B vs 8 B), and then
reduces the result by hand in pandas/numpy.

That is the whole finding. The engine chosen for predicate pushdown and columnar reduction
is used as a blob store, and the reduction it exists to do is re-implemented one layer up
in the slowest way available.

## 2. What the 15:55 run actually spent (corrected)

```
resolve_one: band, TOTAL=357.5s  (plan=194.4s)
build_figure: TOTAL=153.6s  (explode=152.9s)
exploded 'RawEMG': 4190 rows -> 174,158,480 samples ; downsampled to 20,003 (stride=8707)
```

- **`plan` = 194 s is `_aggregated_extents`, not `_raw_extents`.** The run was
  `kind=band` with an error band, so `_needs_reduction` is True and the limits path
  calls `_explode_for_limits` (`ylimits.py:302`) then a pandas `groupby` over 174 M rows
  to get mean±SD per sample position. **The slow plot exploded 174 M rows TWICE**: once
  for the limits, once for the figure. §10's attribution to `_raw_extents` was wrong.
- **`explode` = 153 s** materialises 174 M rows that `_downsample` (`reduce.py:1123`)
  cuts to 20,003 by `iloc[::stride]`. 99.99% built to be discarded.
- **`fetch` = 85-97 s** (from §7) is the boxing of 174 M floats; it goes away when the
  query returns 20 k values instead.

## 3. The reduction contract, per shape and per kind

Every plot kind reduces to one of four operations. This is the table the design is
built from — if a kind is not here it is not covered, and a reviewer should check it.

| Kind(s) | DOUBLE (scalar) | DOUBLE[] (1-D series) | DOUBLE[][] (2-D matrix) |
|---|---|---|---|
| line / scatter / strip | raw values, strided | unnest + stride | n/a (not a 2-D kind) |
| band / bar (mean±err) | groupby stat | **stride first**, then per-position stat | n/a |
| box / violin | groupby quantiles | unnest (no stride: quantiles need all samples) | n/a |
| heatmap | n/a | n/a | elementwise mean of matrices |
| **y-limits (raw)** | `MIN/MAX` | `MIN(list_min)/MAX(list_max)` | `MIN/MAX` of flattened |
| **y-limits (aggregated)** | centre±spread over groupby | centre±spread per position | n/a (heatmap has a colour scale, not y limits) |

Two things fall out of the table:

1. **Stride-before-aggregate is a semantic choice, not just an optimisation.** For a band,
   the figure today explodes, aggregates every position, then strides the *aggregates*.
   Striding the raw samples first and aggregating only the kept positions gives the
   identical drawn value at every kept position (each position's mean is over the same
   records either way) — the positions in between were never going to be drawn. So the
   result is pixel-identical, and this is what makes `UNNEST … WHERE idx % stride = 0`
   valid for band/bar as well as line.
2. **Box/violin cannot stride.** A quantile over a strided subsample is a different
   number. For those kinds the 1-D path must unnest fully — but it can still push the
   *filter* and the *quantile* into SQL (`quantile_cont`), so it returns 4 numbers per
   group rather than 41 k samples per record. Slower than line, still no Python loop.

## 4. Exact statistic parity (the correctness anchor)

`_spread` and `_summarize` are "kept deliberately parallel" so limits and drawing agree.
The SQL must join that parallel, with DuckDB's function chosen to match pandas EXACTLY:

| pandas (today) | DuckDB | note |
|---|---|---|
| `mean()` | `avg(x)` | |
| `median()` | `median(x)` = `quantile_cont(x, 0.5)` | pandas median is linear-interp; `quantile_cont` matches, `quantile_disc` does not |
| `std(ddof=1)` | `stddev_samp(x)` | NOT `stddev_pop`; `fillna(0.0)` → `COALESCE(…, 0.0)` for n=1 |
| `count()` | `count(x)` | non-null count, same as pandas |
| `quantile(0.25/0.75)` | `quantile_cont(x, 0.25/0.75)` | linear interpolation, same as pandas default |
| SEM | `stddev_samp / sqrt(GREATEST(count,1))` | `count.where(count>0,1)` |
| CI95 | `1.96 * stddev_samp / sqrt(GREATEST(count,1))` | same constant, not t-dist |

A parity test per row of this table, over a fixture with NaNs and n=1 groups, is
non-negotiable — a band clipped by its own axis is the failure mode.

## 5. Source protocol additions (the seam)

Added to `DataSource` as **optional** methods; `BaseSource` provides pandas
implementations so CSV/DataFrame sources keep working unchanged and stay standalone.
`ScidbSource` overrides with SQL.

```python
def y_extents(measure, *, filters, scope, mode) -> dict[tuple, (low, high)]
    # mode="raw"        : MIN/MAX of the measure, grouped by scope
    # mode=AggregateSpec: centre±spread per (scope, x, color, position), extremes per scope

def sampled_series(measure, *, filters, stride, group_by) -> DataFrame
    # 1-D: one row per (record, kept position) with the schema/variant/field columns
    # scalar: the filtered rows, strided
    # returns ALREADY-EXPLODED rows -> reduce's explode phase becomes a no-op

def matrix_mean(measure, *, filters, group_by) -> DataFrame
    # 2-D: one row per group holding the elementwise mean matrix (see §6)
```

`filters` is the spec's schema/variant filter, rendered to a `WHERE` in `ScidbSource`.
This is Stage 3 of the old plan, arriving for free: plotting one location reads one
record.

**Lock model change, stated not hidden.** `_load`'s docstring says the DuckDB hold is
"the whole DuckDB-touching phase" and everything after runs in memory. That rule
protected the sidecar from a 350 s Python compute; now the compute IS the query and takes
well under a second. The reduction moves under the hold, the docstring is rewritten to
say so, and `plot_resolve: held the DuckDB connection for …` remains the line that proves
the hold stayed short.

## 6. DOUBLE[][] — the case DuckDB does NOT do natively

A heatmap is an elementwise mean over N matrices of identical shape (`_matrix_frame`,
`reduce.py:1062`). There is no `list_mean` over a list-of-lists in DuckDB; the honest
options:

- **(chosen) double-UNNEST with ordinality, then `avg` grouped by (row, col)**:
  ```sql
  SELECT r.i, c.j, avg(c.v)
  FROM t, UNNEST(y) WITH ORDINALITY AS r(row, i), UNNEST(r.row) WITH ORDINALITY AS c(v, j)
  WHERE <filter> GROUP BY r.i, c.j ORDER BY r.i, c.j
  ```
  Returns rows×cols numbers, reshaped in numpy. Handles the *averaging* natively and
  returns a small result. Shape-mismatch detection (`_matrix_frame` warns and takes the
  first) becomes a `GROUP BY len(y), len(y[1])` pre-check — one tiny query.
- Fallback for a source without it: today's numpy path, unchanged.

2-D is the smallest data of the three in practice (a matrix per record, not a signal per
field per record), so this path is about consistency of the contract, not about speed.
It must exist so "all plot types" is true, and it must be tested, but it is the last
thing to optimise.

## 7. Stages

### Stage 1 — Parity fixtures and the pandas reference (no SQL yet)

Build the fixture that every later stage is measured against: a small scidb DB with
scalar, 1-D (ragged lengths, NaNs, an n=1 group) and 2-D (two shapes, to hit the
mismatch warning) variables. Write `y_extents` / `sampled_series` / `matrix_mean` on
`BaseSource` as pure-pandas extractions of the CURRENT logic — `_raw_extents`,
`_aggregated_extents`, `_explode_1d`+`_downsample`, `_matrix_frame` — so the reference
answers are today's answers, byte-for-byte. Tests: `_build_plan` and `_build_figure`
called through the new methods equal the old inline path on this fixture. **Exit:**
the refactor is behaviour-preserving before any SQL exists.

### Stage 2 — `y_extents` in SQL on `ScidbSource`

Raw mode: `MIN(list_min(y)), MAX(list_max(y))` grouped by scope (scalar: plain
`MIN/MAX`; 2-D: `list_min(list_min…)` via flatten). Aggregated mode: the §4 table, per
`(scope, x, color, position)` via `UNNEST WITH ORDINALITY`, extremes per scope in the
same query. **This alone removes the 194 s.** Test: equals Stage 1's pandas answer on
the fixture for every `(kind, error)` combination.

### Stage 3 — `sampled_series` in SQL (1-D and scalar)

`UNNEST(y) WITH ORDINALITY … WHERE (idx-1) % :stride = 0` with the filter pushed down.
The stride is computed the way `_downsample` computes it today (`total_samples //
max_points`), which needs `SUM(len(y))` first — one cheap query. `_build_figure` skips
its explode phase when the source returned exploded rows (the `exploded` flag on
`MeasureInfo` already exists for this). Box/violin pass `stride=1` and instead push
`quantile_cont` for the box statistics (Stage 3b, same method, different reducer).
**This removes the 153 s and the 85-97 s fetch.**

### Stage 4 — `matrix_mean` in SQL (2-D)

§6. Test: equals `_matrix_frame` on the fixture, including the mismatch case.

### Stage 5 — Lock model + docstrings + the standalone guarantee

Rewrite `_load`/`resolve_figures`' contract comment. Add a test that `CsvSource` and
`DataFrameSource` still resolve every kind with NO database and produce the same figures
as before (the pandas fallbacks are what keep `scistackplot` standalone; this pins it).

### Stage 6 — Measure on the real data

Same two surfaces, same log. Expected: `plot_resolve` hold of a few seconds, no
`exploded … 174,158,480` line, `[timing] build_plan` with `y_limits` sub-second.

## 8. Out of scope

- Hashing (still).
- `_describe`/`plot_capabilities` loading full payloads (§8 of the previous plan) — they
  benefit from Stage 3 indirectly but are not restructured here.
- Arrow fetch (old Stage 4) — mostly moot once the queries return kilobytes.

## 9. Verification

```
pytest scistackplot/tests
pytest scistackplotdb/tests
pytest scistack-gui/tests
```
plus the real-data reproduction in Stage 6.

---

## 10. Stage 1 implementation status (2026-09-13)

Implemented, uncommitted, unrun. Behaviour-preserving by construction; Stages 2-6 not
started.

### Changed

| File | Change |
|---|---|
| `scistackplot/reducer.py` | **new** — `Reducer` protocol (`y_extents`, `explode_series`, `downsample`, `matrix_mean`), `PandasReducer` reference that DELEGATES to the existing functions, `DEFAULT_REDUCER`, `reducer_for(table)` |
| `scistackplot/table.py` | `LongTable.reducer: Any = None` — a source-supplied fact, same family as `default_pin` / `latest_column` |
| `scistackplot/reduce.py` | the four call sites route through `reducer_for(table)`: `_build_plan`'s `y_limits` phase, `_build_figure`'s `explode` and `downsample` phases, `_panel_frame`'s 2-D branch. `limits_by_scope` import dropped (now reached via the reducer). `_explode_1d` / `_downsample` / `_matrix_frame` stay where they are — nothing moved |

### Why the reference delegates instead of moving code

`PandasReducer` imports `_explode_1d` et al. LAZILY inside each method. Two reasons,
both load-bearing: (1) `reduce` imports `reducer`, so a top-level import back would be
a cycle; (2) `test_resolve_caching.py` monkeypatches `reduce_mod._explode_1d` and
expects the spy to see calls — a name resolved at call time on the module honours that,
a name bound at import time would silently bypass it. Do not "tidy" these into top-level
imports.

### Tests added

`scistackplotdb/tests/test_reducer_parity.py` — the fixture and the oracle:

- **Fixture** `parity_db`: `Scalar` (DOUBLE, with an n=1 subject), `Series` (DOUBLE[],
  ragged 6-15, a mid-array NaN, an ALL-NaN cell), `Matrix` (DOUBLE[][], two 3x4 and
  one 2x2 — the mismatch case). `TestFixtureHitsTheEdges` pins the fixture itself, so a
  later edit cannot quietly stop exercising an edge.
- **Routing** (`TestEveryKindRoutesThroughTheReducer`, 12 parametrised kinds x 3): a
  `_Spy` reducer proves each kind actually CALLS the contract. Value-parity alone would
  pass for a kind that reduced inline and bypassed the seam — which is the one way
  Stage 2 could do nothing. Covers line, scatter, band (SD/SEM/CI95/IQR+median), bar,
  box (scalar + 1-D), violin, strip, heatmap.
- **Oracle** (`TestReferenceEqualsTheOriginals`): `PandasReducer` == the underlying
  functions on the same frames — raw extents, aggregated extents for all four error
  bands, scalar extents, explode (`assert_frame_equal`), downsample, matrix mean, and
  the shape-mismatch "take the first" rule.
- Autouse `_fresh_plan_cache`: the plan cache is keyed on `id(table)`, module-global;
  cleared per test so a spy always observes a real plan build.

### Found while building the fixture

There was **no DOUBLE[][] variable in any scistackplotdb test** before this. The 2-D
path had zero database-backed coverage. The user's "all three shapes" constraint is
what surfaced it.

### Not verified

Nothing has been run. The most likely fixture breakages: `Series.save` of an all-NaN
ndarray (typing traced through `_infer_data_columns` — an all-NaN `np.ndarray` still
passes the `isinstance`/`issubdtype` check, so it should store as DOUBLE[]), and whether
`HEATMAP` with `trial: AGGREGATE` resolves on a 2-record group without a facet. If the
heatmap case fails on roles rather than on the reducer, adjust the roles in `KIND_CASES`,
not the contract.

### Stage 1 first run: 4 failures, 2 causes, both mine — and one real bug found

`box-1d` and `heatmap-2d` failed (x2 each: the routing test and the shape test).
Everything else passed, including all four error bands in the oracle and the explode
/ downsample / matrix parity.

1. **`box-1d` — my roles were invalid.** `roles.py:265` refuses a factor on X for a 1-D
   measure, because its x axis IS the sample index. Correct refusal. The case now uses
   `subject: COLOR` (distribution across samples, per colour).
2. **`heatmap-2d` — a pre-existing bug, not a test mistake.** `trial: AGGREGATE` on a
   DOUBLE[][] measure routes the 2-D cells through `_collapse_aggregates`, whose pandas
   `groupby(...).mean()` cannot average object cells and raises `TypeError`. The
   elementwise mean lives in `_matrix_frame`, reached only when NO factor is AGGREGATE.
   Every existing heatmap test uses `FREE`, which is why it was never hit. The routing
   case now uses `FREE`; the AGGREGATE form is pinned as a **strict `xfail`**
   (`TestAggregateOnA2DMeasure`) so Stage 4 has a red test to turn green, and an
   unexpected pass fails loudly rather than leaving a stale marker.

**Stage 4 scope addition:** `matrix_mean` must be reachable under an AGGREGATE role.
The cleanest form is for `_collapse_aggregates` to hand 2-D measures to the reducer
instead of to pandas — which is also where the SQL version naturally attaches.

---

## 11. Stage 2 implementation status (2026-09-13)

Implemented, uncommitted, unrun. `y_extents` in SQL; the other three operations still
fall through to the pandas reference.

### Changed

| File | Change |
|---|---|
| `sciduckdb/sciduckdb.py` | new `_fetchall_with_frame(sql, frame, view=, params=)` — the read-side twin of `_bulk_insert`'s register/unregister idiom: a DataFrame is visible to one query as a view, under the lock, unregistered in `finally` |
| `scistackplotdb/reducer.py` | **new** — `DuckDBReducer(PandasReducer)`: overrides `y_extents` only; raw mode (`MIN(list_min)/MAX(list_max)`, per shape) and aggregated mode (per-point `UNNEST WITH ORDINALITY` + the §4 statistics, extremes per scope); falls back to `super()` with an INFO line when a row is not addressable |
| `scistackplotdb/source.py` | `ScidbSource(db, pushdown=True)`; one `DuckDBReducer` per source, attached to every plottable table at both return sites (single-measure and stacked) |

### The design point that shaped the SQL

**The frame decides WHICH rows and groups; DuckDB reduces the VALUES.** They join on
`record_id` (+ `ColName` for a melted dict variable). This is forced, not chosen: a
scope / X / COLOR factor may be a frame column with NO DuckDB column behind it (a
synthetic `Variant`, a level group, `ColName`). So the group labels are registered as a
view from the frame and DuckDB does the one thing the frame cannot do cheaply — touch
every sample.

### Found while writing the SQL

**NaN inside a DOUBLE[] is stored as a real NaN, not NULL.** scidb writes arrays via
`pa.array(..., pa.list_(pa.float64()))` (`database.py:1605`), which preserves NaN. DuckDB
orders NaN ABOVE every number, so a bare `list_max` over a list holding one NaN returns
NaN where pandas' `nanmax` skips it. Every reduction therefore `list_filter`s NaN first
(`WHERE NOT isnan(v.val)` on the unnested path); an all-NaN list becomes empty →
`list_min` NULL → skipped by the outer `MIN`, which is exactly "an all-NaN cell
contributes nothing". The parity fixture has both a mid-array NaN and an all-NaN cell
specifically to catch this; it would have failed without the filter.

### Two corrections made on re-read, before any test ran

1. `_raw_extents` grouped by scope but never put the scope labels in the view — the
   `GROUP BY g."subject"` would have raised. Threaded the frame through.
2. The aggregated outer query took `LEAST(low, centre)` everywhere; pandas does that
   only in its UNGROUPED branch (`_summary_bounds`). Same number in every reachable
   case (`low ≤ centre` for every band), but made structurally identical rather than
   argued identical: `MIN(low)` when grouped, `MIN(LEAST(low, centre))` when not. Also
   removed a `"` inside an f-string expression — a syntax error below Python 3.12, and
   the floor is 3.10.

### Tests added (`test_reducer_parity.py`, Stage 2 section)

- `pandas_source` / `duckdb_source`: the SAME database opened twice, `pushdown=False`
  and `True`. Every parity test is A/B on one fixture.
- `test_duckdb_y_extents_equal_pandas` — 15 parametrised cases: raw × {1-D, scalar,
  2-D} × {global, by-subject}; aggregated × {SD, SEM, CI95, IQR+median} × {global,
  scoped}; bar over scalar including the n=1 subject. Keys AND values, `rel=1e-9`.
- `TestDuckDBExtentsActuallyRanInDuckDB` — parity would also pass if the pushdown
  silently fell back. Asserts the `[timing] y_extents(duckdb)` line with the `raw=` /
  `aggregated=` phase, no `pandas fallback` line, and — spied on `_fetchall` — that NO
  query re-selected the payload column during `y_extents`.
- `TestDuckDBExtentsFallBackHonestly` — a pre-exploded table and a frame without
  `record_id` both fall back, log it, and still give the pandas answer.
- `TestDuckDBExtentsThroughResolve` — end to end over all 12 `KIND_CASES`: the
  panels' `y_limits` match between the two sources.

### Not verified

Nothing run. Beyond the usual, the specific risks: DuckDB's `quantile_cont` vs pandas
linear interpolation on an even-count group (should match — both are type-7); and
whether `LATERAL (SELECT ... AS val) v` is accepted by the pinned DuckDB floor
(`>=0.9.0`) — if not, inline `t."col"` directly and drop the lateral alias.

---

## 12. Stage 3 implementation status (2026-09-13) — with a correction to §3

Implemented, uncommitted, unrun. `explode_series` in SQL, returned columnar.

### §3's stride-first claim was HALF wrong, and the plan as written would have changed line plots

§3 said striding before aggregating is "pixel-identical" and proposed
`UNNEST … WHERE pos % stride = 0`. Re-reading `_build_figure` and `_downsample` before
writing SQL:

- `_downsample` strides the exploded frame **by row** — `frame.iloc[::stride]` — and
  the exploded frame is **record-major** (record 1's samples, then record 2's, …).
  For a **line** plot (no collapse) the stride walks ACROSS records: it keeps position 0
  of record 1, position 8707 of record 1, … then falls into record 2 at some unrelated
  position. Per-record `pos % stride` is a different set of points. **Not identical.**
- For a **band/bar**, `_collapse_aggregates` groups by position FIRST, producing one
  row per position; only then does `_downsample` stride — over positions. Here
  stride-first IS identical. §3's argument was correct for exactly the case that was
  slow, and wrong for the other.

So Stage 3 does NOT stride in SQL. `explode_series` reproduces `_explode_1d`'s frame
**exactly** — same rows, same record-major-then-position order, same NaN gaps, same
dtypes — and leaves `_collapse_aggregates` and `_downsample` untouched downstream.
Parity for every kind then holds by construction, and the win is the one that was
actually being paid for: 174 M samples arrive as ONE numpy buffer through DuckDB's
`.df()` instead of 174 M boxed Python floats through `pandas.explode`.

In-query striding for band/bar (where it is provably identical) is a real further win
and belongs in Stage 5, gated on measurement — not folded in here.

### Changed

| File | Change |
|---|---|
| `sciduckdb/sciduckdb.py` | new `_fetchdf_with_frame` — same register/unregister idiom as `_fetchall_with_frame`, returns `.df()`. **The difference is the point**: `.df()` goes Arrow → numpy, never a Python float per value |
| `scistackplot/reducer.py` | `explode_series(frame, measure, index_column, table)` — `table` added to the protocol and reference, because a pushed-down reducer must locate rows in storage, exactly as `y_extents` already did. One caller, updated |
| `scistackplotdb/reducer.py` | `DuckDBReducer.explode_series`: `UNNEST WITH ORDINALITY` per `(table, column)`, `pos - 1` (0-based like pandas), `WHERE NOT isnan` (the `dropna`), `ORDER BY __row, pos` (record-major), carried columns gathered by a vectorised `take` on the source row; falls back to the reference when not addressable or when the index column pre-exists (the reference raises there, so the same error surfaces) |

A stub I briefly introduced (`_table_for_explode`, a fake `LongTable` so `_address_rows`
could run without the table) was removed in favour of passing the real table through
the protocol. A fake table to satisfy a helper is the kind of thing that works until a
stacked or melted frame arrives.

### Tests added (`test_reducer_parity.py`, Stage 3 section)

- `TestDuckDBExplodeEqualsPandas` — `assert_frame_equal` on the whole exploded frame
  (the contract, since two downstream steps are order-sensitive); the NaN at s1/t1
  position 4 leaves positions `0,1,2,3,5,…` (a gap, NOT renumbered); the all-NaN cell
  s2/t3 contributes zero rows; ragged per-record lengths preserved; record-major then
  position order pinned explicitly; `[timing] explode_series(duckdb)` present and no
  fallback; **no `UNNEST` reaches `_fetchall`** (spied — the boxing door stays shut);
  a pre-existing index column raises `ValueError` like the reference.
- `TestDuckDBExplodeThroughResolve` — every 1-D `KIND_CASE` at `max_points=None` AND
  `=25`, panel frames `assert_frame_equal`. The `25` case is what exercises the
  record-major-order argument: the stride lands on the same rows only if the order is
  identical.

### Not verified

Nothing run. Beyond DuckDB syntax at the `>=0.9.0` floor (`UNNEST … WITH ORDINALITY`
is 0.8+), the risk is dtype residue in `assert_frame_equal` — carried columns pass
through `take` unchanged, but if pandas' `explode` upcasts a carried column the
`check_dtype=False` in the tests is what absorbs it.
