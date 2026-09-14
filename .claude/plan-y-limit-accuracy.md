# Y-limit accuracy

Review of `scistackplot` y limits, 2026-09-14. User report: "data is often cut
off vertically … as I get into more specific combinations of the checkboxes".
Context: `docs/claude/y-limit-scoping.md`, `.claude/plan-plot-save-and-ylimits.md`
Stage 5.

## Root causes found

1. **Statistic computed at scope granularity, drawn at panel granularity.**
   `ylimits._aggregated_extents` and `NumpyReducer.y_extents` group by
   `scope + X + COLOR + index`. A panel draws `centre ± spread` over its OWN
   rows (every ITERATE ∪ FACET factor). Unticking a panel factor pools its
   levels into one mean ± SEM, whose range collapses around the grand mean;
   every panel's band lands outside it. The scope must decide how panel
   extents are COMBINED (min/max), never how the statistic is computed.
   Same defect, same fix: X/COLOR read from `spec.roles` not completed roles;
   AGGREGATE collapse skipped; MEAN+IQR grouped path drops the centre.
2. **Plan cache ignores `kind`, but `_Plan.y_limits` depends on it.**
   `_PLAN_IRRELEVANT_FIELDS = ("kind", "facet", "style")`. LINE → BAND with the
   default MEAN ± SD is a cache HIT: band drawn with raw limits (clipped for
   small n); BAND → LINE draws raw traces inside the band's range.
   `style.log_y` is also out of the key.
3. **Plotly log-y `range` is in data units.** Plotly takes log10 values on a
   `type: "log"` axis. Linear 5 % padding can also push `lo ≤ 0`.

Smaller: GUI checkbox list reads `spec.roles`, so promoted ITERATE ancestors
and defaulted FACET factors have no checkbox; BAR extents ignore 0; NaN level
keys miss in `limits_for` (numpy keys NaN as None, groupby keeps NaN).

## Invariant to enforce (the regression test)

For every scope ⊆ panel factors, every kind, every error band, every
statistic: for every panel of every figure,
`panel.y_limits[0] <= min(drawn) and max(drawn) <= panel.y_limits[1]`,
where `drawn` = Y (and Y_LOW/Y_HIGH when present) of `panel.frame`.
Fixture must make pooled-SEM narrower than any panel's band (three panels at
distinct levels, small within-panel spread) so the test fails today.

## Stages

### Stage 1 — extents at panel granularity (scistackplot/ylimits.py, reducer.py)

- `y_extents(frame, spec, table, scope, roles)` takes the COMPLETED roles
  (from `_build_plan_timed`). Protocol + both reducers updated.
- Aggregated path: group by **every panel factor** (`ITERATE ∪ FACET` present
  in the frame), and fold per-panel extents into the scope afterwards.
  Numpy reducer: call `position_stats` per (panel, colour) group DIRECTLY —
  the same `_stats` the drawing calls — with NO per-panel DataFrame (that
  would add ~1 ms x panels). Pandas reference (scalars/pre-exploded): the
  existing groupby with the panel factors as keys. Same samples, same
  complexity as today; with the default scope the group set is identical.
  Extent per panel = `min(Y, Y_LOW)`,
  `max(Y, Y_HIGH)`; BAR also includes 0.
- Fold panel extents into scope groups by projecting the panel key onto the
  scope (min/max), plus `GLOBAL_KEY`. Delete `_spread`, `_summary_bounds`,
  `_explode_for_limits` and the numpy grouping copy.
- `_needs_reduction(spec, roles)` also true when any AGGREGATE role exists
  (drawn = collapsed mean; raw extents are merely loose, but one path).
- NaN keys: normalise both sides through one `_hashable`-style key fn.
- Logging: one DEBUG line per scope group `(key, low, high, n_panels)`; keep
  the existing summary line.

### Stage 2 — limits memoised per extent mode, plan key unchanged (reduce.py)

- `kind` stays OUT of the plan key (a kind switch must not rebuild
  variants+filters+fan-out, ~0.13-0.22 s in scidb.log 2026-09-14).
- `_Plan.y_limits` becomes a per-mode memo: side cache keyed by
  `(plan key, extent mode, log_y)` -> limits, filled on first use under the
  plan lock; `_with_presentation` attaches the right entry the way it already
  restores `kind`. A LINE->BAND switch costs only the `y_limits` phase (25 ms
  raw on this data), never a plan rebuild.
- Log a MISS/HIT line for the limits memo naming the mode, and add
  `groups=N panels=M` to the `y_extents(numpy)` timing line so the band path
  at scale (never measured: zero band/bar runs in the log) becomes readable.
- Amend `test_changing_only_the_plot_kind_reuses_the_plan`: still one plan;
  add an assertion that the BAND after a LINE hit gets band limits.

### Stage 3 — log-y (ylimits.py, render/plotly_.py, render/mpl.py)

- When `style.log_y`: pad geometrically in log10 space; a non-positive low
  falls back to the smallest positive drawn value (log the drop).
- plotly: emit `range = [log10(lo), log10(hi)]` on a log axis. mpl unchanged
  (data units). Test both renderers on a log-y spec.

### Stage 4 — GUI lists the effective panel factors (resolved.py, PlotStudio.tsx)

- `ResolvedPlot.to_dict` reports `panel_factors` (completed ITERATE ∪ FACET,
  in `fanout_keys` order then facets) — the backend owns eligibility.
- `yScopeFactors` reads that when a figure is present, falling back to
  `spec.roles` before the first resolve. Rebuild both vite targets
  (see project_frontend_bundle_rebuild).

### Stage 5 — tests + doc

- `tests/test_ylimits.py`: the invariant test parametrised over
  scope × kind × error × statistic, on a new three-level fixture; MEAN+IQR
  centre; AGGREGATE + LINE; NaN level; cache-mode tests.
- Update `docs/claude/y-limit-scoping.md`: "computed per panel, folded by
  scope" replaces "kept deliberately parallel"; cache-key note; log-y note.

## Not done here

- BAR-from-zero is included only as an extent rule; no renderer change.
- Codegen `_y_limit_plan` literal path unchanged (it bakes the same numbers).

## BUILT — 2026-09-14 (uncommitted, tests not yet run)

- **Stage 1** `ylimits.py` rewritten: `ExtentMode`, `panel_factors`,
  `finish_extents`, `pair_extent`, `merge_extent`, `hashable`/`scope_key`,
  `spread_bounds` (now also called by `reduce._summarize`). `_reduced_extents`
  groups by panel factors + X/COLOR + index from COMPLETED roles, collapses
  AGGREGATE first, includes the centre. `NumpyReducer.y_extents` groups by
  panel factors and reuses `_panel_series` + `_series_stats` (split out of
  `summarize_series`). `y_extents(..., roles)` on the protocol, both reducers,
  codegen, parity tests.
- **Stage 2** `_Plan.frame` + `_Plan.y_limits_by_mode`; `reduce._with_y_limits`
  on both `_plan` exits; build seeds its own mode. Kind stays out of the key.
- **Stage 3** geometric padding + positive floor in `ylimits`;
  `render.base.drawable_limits` / `axis_range`; plotly emits log10, mpl guards.
- **Stage 4** `ResolvedPlot.panel_factors` (+ plotly `layout.meta.panel_factors`,
  `y_scope`); `PlotStudio.tsx` `yScopeFactors` reads meta, falls back to
  `spec.roles`; `appliedYLimits` un-logs a log axis. Both vite targets rebuilt;
  `tsc --noEmit` clean.
- **Stage 5** `tests/test_ylimits.py`: `three_level_table`, the invariant test
  (4 scopes × 5 kinds × 5 bands), union/never-narrows, MEAN+IQR centre, bar
  zero, aggregated line, kind-switch memo (2 tests), log axis (3 tests), NaN
  key, panel_factors report. Parity tests pass `roles`.
  `docs/claude/y-limit-scoping.md` revised.

Run (one package at a time):
    cd /workspace/scistackplot && pytest tests/test_ylimits.py tests/test_resolve_caching.py tests/test_render.py tests/test_codegen.py -q
    cd /workspace/scistackplot && pytest -q
    cd /workspace/scistackplotdb && pytest tests/test_reducer_parity.py -q
    cd /workspace/scistack-gui && pytest tests/test_plot_service.py -q
