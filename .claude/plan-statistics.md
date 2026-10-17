# Plan: Statistics — scistackstats + StatStudio

Design: `docs/claude/statistics-design.md` (S1–S21; StatStudio decisions
S10–S21 added 2026-10-17). Engine contract (target, far off):
`docs/claude/csv-stats-v1-requirements.md`.

Status: **rewritten 2026-10-17** to cover StatStudio as its own studio.
Stage 1 is built (tests not yet run). The rest is a draft for approval.

The 2026-09-27 draft assumed csv-stats v1 and put stats inside Plot Studio.
Both assumptions are gone. **csv-stats v1 is a long way off (S21).** So every
stage below runs against the engine seam. csv-stats still does all the test
math; it gets improved as gaps show up.

---

## 0. Shape of the solution

```
 Plot Studio  ──(StatStudio button: seed S12)──►  StatStudio
   PlotSpec     ◄──── Sync to PlotStudio ────      StatsSpec
                ──── Sync to StatStudio ────►        .design  (a plot-layer PlotDesign)
                                                     .dvs     (variables + ColName columns)
                                                     .descriptives / .comparisons / .format
          │                                               │
 scistackplot  design.py: DESIGN_FIELDS, design_of, design_diff, apply_design
               spread.py: CI95 (done), export.plot_data / sample_curves (rows)
          │                                               │
 scistackstats  seed · design(derive) · catalog · effects · describe · run ·
                engine (ONLY csvstats importer) · format(caption) · export · overlays · codegen
          │
 scistackstatsdb  saved stats (VersionedStore) · presets · linked_stats_for(plot) ·
                  loading through the plot source · stat_ endpoint · sweep
          │
 scistack-gui   api/stats.py + StatStudio webview tab (Descriptives | Comparisons)
                Plot Studio: StatStudio button + divergence banner + Sync button
```

**Dependency rule (S8):** stats → plot, never plot → stats. The plot layer
gains only generic pieces: the design field table, `sample_curves`, derived
bars and span shades. None of them knows statistics exist. The Plot Studio
banner is computed in the GUI service, which can see both.

### Owner table (new rows)

| Concept | Owner | Consumers |
|---|---|---|
| CI95 half-width (t-based), every descriptive formula | `csvstats.descriptive` (plot adapter: `scistackplot.spread`) ✅ | ylimits, series_stats, codegen, stats Descriptives |
| Which PlotSpec fields are "design" | `scistackplot.design.DESIGN_FIELDS` | sync both ways, banner diff, StatsSpec.design |
| Divergence between plot and stats | `scistackplot.design.design_diff` | Plot Studio banner, StatStudio banner, logs |
| Plot → table settings | `scistackstats.seed.seed_from_plot` | StatStudio button, code users |
| Rows a test sees | `scistackplot.export.plot_data` / `sample_curves` | describe, run, codegen |
| DV list (variables, ColName columns) | `StatsSpec.dvs`, resolved by `scistackstatsdb.dvs` | loader, GUI picker |
| Design (factors, within/between, unit, strata) | `scistackstats.design.derive_design` | catalog, run, caption, GUI readout |
| Primary vs. secondary comparison | `scistackstats.design.comparison_tiers` | run, families, overlays, GUI |
| "Can test T run here, and why not?" | `scistackstats.catalog.why_unavailable` | GUI menu, run, endpoint |
| Effect-size variants (names, formulas, defaults) | `scistackstats.effects` | catalog, format, caption |
| Difference + its spread definition | `scistackstats.effects.difference_spec` | run, format, caption |
| Summary-statistic catalog (formulas: `csvstats.descriptive`) | `scistackstats.describe.STATISTICS` | Descriptives, GUI picker |
| Calling csv-stats | `scistackstats.engine` | run, describe, codegen |
| Number / p / caption text | `scistackstats.format` | exporters, GUI (strings from backend), bracket labels |
| Saved stats + presets | `scistackstatsdb.saved` on `scistackplotdb._versioned.VersionedStore` | GUI |
| Plot ↔ stats link | `StatsSpec.linked_plot` only (no back-reference) | `linked_stats_for(saved_plot_id)` |

---

## Decisions this plan makes (flag any you disagree with)

- **P1. Stats run on `plot_data` rows built from the stats spec's own
  design.** The same `_plan` → `_sample_frame` code the figure runs, applied
  to `StatsSpec.design`. When the two designs match, the stats see exactly
  what the figure draws. When they don't, the banner says so (S11).
- **P2. Strata = separate figures × panels; factors = the grouping layers**
  (colour included); **subject = `roles.sample_key`**. Pooled or
  nothing-collapsed → no subject, so within-subject tests are unavailable
  and say why.
- **P3. Mixed models may go below the sample** via a `depth` cut, labelled.
  Every other test runs at the sample.
- **P4. Within/between is derived, with an override.** Mixed membership is
  refused until it's overridden.
- **P5. Family scopes:** `none`, `context` (one combination of the outer
  layers), `stratum` (one DV × one figure/panel; **the default**, per S13),
  `dv`, `table`, and later `variants` under a sweep. Omnibus terms are not
  adjusted. The family sentence comes from the backend
  (`run.family_statement`).
- **P6. Derived bars are intent:** StatsSpec stores the rule. Bars are
  computed at resolve with `origin="test"`; hidden, never deleted; blocked
  while `spec.comparison` is active (S9).
- **P7. The pipeline endpoint does not iterate.** One `stat_` call over the
  whole fan-out, so table-wide families can be corrected.
- **P8. Long runs are off the RPC clock.** A job plus progress
  notifications, as for plot save.
- **P9. Engine seam.** `engine.py` is the only `csvstats` importer.
  `FakeEngine` serves tests. `CsvStatsEngine` adapts 0.1.x output to the S5
  core, and refuses with `not yet in csv-stats: <what>` where it can't. Never
  a scipy/pingouin fallback.
- **P10. Sync copies `DESIGN_FIELDS` one way, wholesale.** No merging. The
  banner's diff lists fields with human labels. Sync is undoable (the
  existing per-surface undo stacks).
- **P11. Unsaved plot → unlinked seed** (S11 default). Saving the plot
  later offers "link this stats spec".
- **P12. csv-stats owns every descriptive formula (S18, revised
  2026-10-17).** scistackplot decides *which rows*; `csvstats.descriptive`
  decides *what each statistic is*. The plot's error bars, StatStudio's
  Descriptives and csv-stats' test results all call it. No overlap test is
  needed, because there is no second implementation.

---

## Stages

Each stage lists its files, the logging it adds (NOTE 2) and its tests.
**The user runs the tests, one package per pytest invocation.**

### Stage 1 — CI95 is t-based everywhere ✅ built 2026-10-17, tests pass, committed d4fe6b35

- **csv-stats owns every descriptive formula** (user decision, 2026-10-17).
  The new `csvstats.descriptive` module (numpy, pandas and scipy only) has
  `t_critical`, `sem`, `ci_half_width`, `ci_interval`, `summarize`,
  `SD_DDOF` and `QUANTILE_METHOD`. csv-stats' `calculate_summary_statistics`
  is rebuilt on it, which fixes a bug: n counted NaN. csv-stats is now 0.2.0,
  its heavy dependencies moved to a `[full]` extra, and it's cloned at
  `/workspace/csv-stats` (gitignored, editable via `dev-install.sh`; CI
  installs it from GitHub main).
- `scistackplot/spread.py` is the plot's adapter: `ci95_spread` /
  `sem_spread` (NaN → zero-width bar) and the re-exported conventions.
  `ylimits.spread_bounds`, `ylimits._summary_bounds` and
  `series_stats.position_stats` call it. `codegen` binds `_ci95 =
  csvstats.descriptive.ci_interval` (a median-centred lambda for median
  bars) instead of `("ci", 95)`. The docstring says "(t-based)".
- `csv-stats>=0.2` added to scistackplot's dependencies.
- GUI: the Spread option reads "95% CI (t-based)", with a formula tooltip.
  Both bundles rebuilt. Manual-testing doc §0zzzzf.
- Log: DEBUG `spread ci95: t-based (csvstats.descriptive), n a..b`.
- Tests: `csv-stats/tests/test_descriptive.py` (t tables, NaN handling,
  reference skewness and kurtosis, the summary-stats n regression, a
  light-import guard). `scistackplot/tests/test_ci95.py` (half-widths,
  zero-width below 2, conventions, a guard that no formula is restated in
  scistackplot, the numpy twin, preview bars, the export source, preview ==
  export at n = 2, 5, 30). `test_series_stats.py` reference updated.

### Stage 2 — Packages + engine seam ✅ built 2026-10-17, tests unrun

As built: no static `engine.GAPS`. A csv-stats gap is `core.NotSupported`,
turned into `EngineRefusal(kind="gap")` and logged `csv-stats gap: …`.
csv-stats gained `csvstats.core` (C3 records) instead of an adapter over the
old API. `scistackstatsdb` is only `load_stats_tables` so far. `publish.yml`
is NOT updated yet: add both packages there when scistack-gui first depends
on them (stage 12), after PyPI publishing is set up for the new names.


- `scistackstats/` and `scistackstatsdb/` scaffolding, registered in every
  package list: `dev-install.sh`, `tools/audit/run.sh`, scistacklog layer
  names, `test-shards.sh` (CI sharding guard), the fetch-locking AST guard,
  and scistack-gui's pyproject. Coversion hook pins.
- `engine.py`: the `Engine` protocol (`run`, `describe`, `correct`,
  `versions`), `CsvStatsEngine` (adapter over 0.1.10: what maps, what
  refuses), `FakeEngine`, `set_engine()`.
- A gap register: `engine.GAPS` is a dict of test id → what csv-stats lacks.
  Each entry is mirrored into `csv-stats-v1-requirements.md`.
- Log: INFO once, `engine csvstats 0.1.10; N tests mapped, M refused (gaps)`.
- Tests: AST guards (nothing under `scistackplot*` imports `scistackstats*`;
  only `engine.py` imports `csvstats`); adapter golden tests for what 0.1.10
  does map.

### Stage 3 — The design owner in scistackplot + sync primitives ✅ built 2026-10-17, tests unrun

As built: the stats spec carries a whole `PlotSpec` as its design (no
separate `PlotDesign` type). `design_of` / `design_fingerprint` /
`design_diff` / `copy_design` read `DESIGN_FIELDS`.


- `scistackplot/design.py`: `DESIGN_FIELDS` (every PlotSpec field classed as
  `design` or `display`; an exhaustiveness test like the presets one),
  `PlotDesign` (the design slice, `to_dict`/salvage via `restore`),
  `design_of(spec)`, `apply_design(spec, design) -> (spec, notes)` (the
  reconcile path, like presets), and `design_diff(a, b) -> list[DiffItem(field,
  label, a_text, b_text)]`.
- Log: INFO `design sync plot->stats: fields […]`; DEBUG for the diff.
- Tests: exhaustiveness; diff labels; round trip; apply with a stale role →
  a note.

### Stage 4 — StatsSpec, DVs (including ColName), seeding ✅ built 2026-10-17, tests unrun

As built: DV loading and the rows owner live in `scistackstats.tables`
(any `DataSource`), not in scistackstatsdb. Rows are read at
`ANALYSIS_KIND` = bar, and the unit id composes the sample key with the
schema keys above it.


- `scistackstats/spec.py`: `StatsSpec(format=1, design: PlotDesign, dvs:
  list[DV(variable, columns: list[str] | None)], linked_plot: str | None,
  descriptives: DescriptiveSpec, comparisons: list[Analysis], format:
  FormatSpec, variant_mode)`, with strict `from_dict` and salvage for opens.
- `scistackstats/seed.py`: `seed_from_plot(plot_spec) -> (StatsSpec,
  notes)`, the S12 table. It logs each rule it applied.
- `scistackstatsdb/dvs.py`: resolves DVs to tables. A `ColName`
  multi-select becomes one DV per column. The plot's multi-variable
  selection is read through its existing owner, not copied.
- Tests: one test per S12 row; ColName expansion; compare-to-reference
  refusal.

### Stage 5 — Descriptives ✅ built 2026-10-17, tests unrun


- `scistackstats/describe.py`: `STATISTICS` catalog (id, label, owner
  plot|engine, needs n ≥ k). `describe_table(spec, tables) ->
  DescriptivesTable`: rows = DV × statistic, columns = grouping layers
  (nested headers, outermost first), sections = strata, n per cell.
- Every statistic comes from `csvstats.descriptive` (P12). Grouping and
  collapsing come from scistackplot's reductions.
- Log: `Log.timer("describe")`; INFO per DV `cells N, n range a..b`.
- Tests: Better Popcorn shape (3 brands × people) gives mean (SD) per brand
  across people; the nested two-layer header; a CI95 parity test against
  the plot preview bars.

### Stage 6 — Design derivation, tiers, catalog, effects ✅ built 2026-10-17, tests unrun

As built: the catalog checks statistical requirements only. What csv-stats
does not support yet comes back from the engine, so the support matrix
has one owner.


- `design.py`: `derive_design` (within/between with evidence; P4) and
  `comparison_tiers(design) -> {primary: simple effects of the innermost
  layer within each outer combination, secondary: main effects +
  interactions}` (S13).
- `catalog.py`: `TESTS`, `why_unavailable` (also returns engine gaps as
  reasons), `suggest`.
- `effects.py`: the S15 catalog with design-based defaults, plus
  `difference_spec(test) -> DifferenceSpec(estimate, spread, label)` (S14).
- Log: INFO `design [stratum]: dv, within, between, subject, n`; INFO
  `difference: <label>` per analysis.
- Tests: aim2 fixtures, the unbalanced case, mixed membership, tiers for 1,
  2 and 3 layers, and effect defaults per design.

### Stage 7 — Runner + families ✅ built 2026-10-17, tests unrun

As built: tested against FakeEngine and against the real csv-stats end to
end. Primary = pairwise two-sample tests per outer-level context (EMM
post-hoc tests are a csv-stats gap).


- `run.py`: DVs × strata × analyses → engine → families (`dv` default) →
  `StatsReport` (strata, the primary table with the interaction p beside it,
  the secondary table, refusals carried, families, `family_statement`,
  versions, `design_digest`).
- Log: `Log.timer("stats_run")` phases; INFO per analysis.
- Tests: family membership per scope, refusals not raised, determinism, and
  primary comparisons present when the interaction p > α.

### Stage 8 — Format, caption, export

- `format.py`: significant figures, APA p, the statistic strings,
  effect-size names, and **`caption(report)`** (S17). Aliases and level order
  come through scistackplot's owners.
- `export.py`: csv / md / latex (booktabs) / json / **html** (the rich
  clipboard table for Word) / txt. Every format carries the caption and the
  family statement.
- `apa_sentence(comparison)`.
- Tests: golden strings; LaTeX and HTML escaping; the caption names the unit
  and the collapse.

### Stage 9 — Persistence: saved stats + presets + link

- `scistackstatsdb/saved.py` on `VersionedStore`, in its own table.
  `linked_stats_for(saved_plot_id)`. Stats presets (settings minus DVs)
  share the same store.
- Salvage on open. Opening writes nothing.
- Tests: a round trip with every field non-default; an unknown test id is
  kept inert with a note; the link query.

### Stage 10 — Overlays (closing the cycle, part 1)

- scistackplot generic hooks: `DifferenceBar.origin`, `resolve(...,
  derived_bars=)` (manual wins on a duplicate pair, with a WARN), and
  `SpanShade` (SPM, later).
- `scistackstats.overlays.bars_for(report)`: primary comparisons →
  `DifferenceBar`s. The S9 guard comes with its test.
- Tests: preview/export parity for derived bars; the guard; the import
  direction is still clean.

### Stage 11 — Endpoint + code export (closing the cycle, part 2)

- `codegen.generate_stats_function`, reusing scistackplot's `_preamble`
  (made public). Parity: running the generated code gives JSON equal to
  `run(...)`.
- `scistackstatsdb.endpoint`: `stat_<name>` with no iteration keys (P7).
  Lineage and staleness come for free.

### Stage 12 — GUI

Backend (`scistack-gui`): `api/stats.py` + `services/stats_service.py`
- handlers: `stats_open` (seed or saved), `stats_catalog`, `stats_describe`,
  `stats_run_start` + progress, `stats_export`, `stats_copy_html`,
  `stats_save`/`stats_presets_*`, `stats_sync` (direction), and
  `design_divergence` (for both studios)
- every handler logs counts and timing

Frontend: a new `StatStudio/` webview tab (one per spec)
- Rail: DVs (inherited list + DAG picker + ColName multi-select), the
  design (roles; within/between chips + override), Descriptives statistics,
  Comparisons (add test, with greyed reasons, the suggested test marked,
  effect sizes by exact name, difference spread), and Format.
- Tabs: **Descriptives** | **Comparisons**. Caption under each table.
  Copy-rich-table, Export, and Add to pipeline.
- Banner (S11) + **Sync to PlotStudio**.
- Plot Studio: a **StatStudio** button in the figure toolbar, the divergence
  banner, and **Sync to StatStudio**. Derived bars are listed with "from
  test" and a hide ✕.
- Pure logic in `statsSpec.ts` + tests. Colours via `var(--ps-*)` (or a
  shared token set). Rebuild **both** vite targets. Manual-testing doc
  entries per section.

### Stage 13 — Sweep, SPM (after the core works)

- The variant sweep (S3/S7) over `scistackplotdb.variants`, and
  `sample_curves` + SPM through csv-stats (S4, O8). Both are mostly blocked
  on csv-stats, so they come last.

### Stage 14 — Docs

- `docs/claude/statistics.md` (owners, data flow, traps) and
  `stat-studio-controls.md` (a control map like plot-studio-controls).
- ADR entries in `decisions.md`. Update difference-bars.md and saved-plots.md.

---

## Order

1 ✅ → 2 → 3 → 4 → 5 (**checkpoint: Descriptives scriptable end to end**) →
6 → 7 → 8 (**checkpoint: Comparisons scriptable against FakeEngine**) → 9
→ 12 (GUI can start after 5 with Descriptives only, and grow) → 10 → 11 →
13 → 14.

## Out of scope (v1)

Power analysis; Bayesian tests; a MATLAB-side stats UI (bridge only); nicer
`scidb report` rendering; bars between panels; the deferred publication
touches listed in S19.
