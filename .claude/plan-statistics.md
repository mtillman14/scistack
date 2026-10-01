# Plan: Statistics (scistackstats + Stats in Plot Studio)

Design: `docs/claude/statistics-design.md` (S1–S8, O4, O8 all decided).
Engine contract: `docs/claude/csv-stats-v1-requirements.md`.
**Assumption for this plan: csv-stats v1 exists and honours that contract.**
In particular: C3 core record, C4 keyword design signature, C9
`correct_pvalues`, `describe`, and the SPM1d wrapper with array-cell DVs.

Status: **draft for approval** (2026-09-27).

---

## 0. Shape of the solution

```
            Plot Studio spec (roles, groups, collapse, filters, variants)
                                 │
          scistackplot ──────────┤ one reduction owner (unchanged semantics)
          plot_data() / sample_curves()  →  sample rows (+ figure/panel keys)
                                 │
          scistackstats ─────────┤
            design.py    derive Design from spec + rows  (within/between, unit)
            catalog.py   which tests apply, and why not  (one refusal owner)
            run.py       strata × analyses → engine → families → StatsReport
            engine.py    THE only module that imports csvstats
            overlays.py  StatsReport → DifferenceBar / SpanShade (plot's shapes)
            format.py    p / number / APA / stars policy (one owner)
            export.py    txt / json / toml / csv / latex / md + methods text
            codegen.py   standalone script = plot preamble + csvstats calls
                                 │
          scistackstatsdb ───────┤ saved-plot attachment, stat_ endpoint, sweep loading
                                 │
          scistack-gui ──────────┘ RPC + Statistics > Tests + Results pane
```

**The dependency rule:** stats → plot, never plot → stats. `scistackplot`
gains only generic hooks: derived bars, span shading, 1-D sample curves, and
opaque saved-plot attachments. None of these know that statistics exist.

### Owner table (new rows only)

| Concept | Owner | Consumers |
|---|---|---|
| Rows a test sees (scalar) | `scistackplot.export.plot_data` (existing) | `scistackstats.run`, stats codegen |
| Rows a test sees (1-D curves) | `scistackplot.export.sample_curves` (**new**) | SPM analyses |
| Descriptive spread (SD/SEM/CI95/IQR) definition | `scistackplot` spread function (**one**, stage 1) | plot preview, plot codegen, ylimits, stats Descriptives |
| Panel identity text | `scistackplot.panels.panel_key_text` (existing) | stats strata keys, derived bars `match` |
| Design (DV, factors, within/between, unit, strata) | `scistackstats.design.derive_design` | catalog, run, GUI readout, codegen |
| "Can test T run on this design? why not?" | `scistackstats.catalog.why_unavailable` | GUI menu, run (refuses), endpoint |
| Calling csv-stats | `scistackstats.engine` | run, codegen (emits the same calls) |
| Correction families | `scistackstats.run.families` | run, GUI statement, export, methods text |
| Number / p / stars formatting | `scistackstats.format` | exporters, bar labels, GUI (displays strings from backend) |
| StatsSpec wire format + salvage | `scistackstats.spec` (+ `scistackplot.restore` generic walker) | scistackstatsdb, GUI |
| Stats spec storage | opaque `attachments["stats"]` in the saved-plot envelope (`scistackplotdb.saved`, stores verbatim) | scistackstatsdb |
| Variant sweep enumeration | `scistackstatsdb.sweep` over existing `scistackplotdb.variants` | GUI |

---

## Decisions this plan makes (flag any you disagree with)

- **P1. Stats run on `plot_data` rows.** They use the same `_plan` →
  `_sample_frame` call the figure makes, so "stats match the plot" holds by
  construction. A test on data the figure doesn't show needs a different
  plot spec.
- **P2. Strata = figure × panel.** A stratum is one ITERATE combination × one
  FACET combination, spelled like `DifferenceBar.match`. Analyses run per
  stratum. **Factors = the grouping layers** (the coloured layer included:
  colour is paint, still a factor). **Subject = `roles.sample_key`.** With
  Weight by N (pooled), or with nothing collapsed, there is no subject, so
  within-subject tests are unavailable and say so.
- **P3. Mixed models may go below the sample.** An `lmm` analysis carries a
  `depth` (a `roles.chain_cut` key, the same picker as Save data). At a depth
  below the sample, the rows keep, for example, trials, and `subject`
  becomes the random intercept. Every other test runs at the sample depth
  only. This is the one sanctioned way to use trial-level rows, and it is
  labelled.
- **P4. Within/between is derived, with an override.** A factor is
  **within** when at least one subject appears at 2 or more of its levels
  inside a stratum, and **between** when each subject sits at exactly one
  level. A factor that is both (some subjects at one level, some at several)
  is `mixed_membership`: refused until the user sets an override. The
  override is stored in the analysis.
- **P5. Family scopes:** `comparison` (post-hoc within one effect; csv-stats
  already does this), `panel`, `figure`, `all` (the whole fan-out), and
  under a sweep, `variants`. The default is `panel`. Per S7 the active
  scopes and methods are always displayed as one sentence the backend
  writes.
- **P6. Derived bars are intent, not fact** (difference-bars.md "Next" §1).
  `StatsSpec` stores the request (analysis, pairs policy, label rule). Bars
  are computed at resolve and handed to `scistackplot.resolve(...,
  derived_bars=...)` with `origin="test"`. A user can hide a derived bar
  (stored as a hidden pair key); it is never deleted. Manual bars in
  `PlotSpec.difference_bars` are untouched.
- **P7. The pipeline endpoint does not iterate.** "Add to pipeline" emits
  one `stat_` call over the whole fan-out, which stores one record per
  analysis holding every stratum. Reason: `figure`/`all`/`variants` family
  corrections need every stratum in one call. A per-combo `for_each` could
  only correct within a panel.
- **P8. Long runs are off the RPC clock.** `stats_run` starts a job and
  streams progress, the same pattern as plot save
  (`project_plot_save_and_ylimits`). Permutation SPM and LMM can take tens
  of seconds; the RPC timeout (owned by `rpcPending.ts`) is never raised to
  cover them.
- **P9. Engine seam for tests.** `scistackstats.engine` is the only importer
  of `csvstats`. Unit tests inject a fake engine that returns C3-shaped
  dicts. A small `golden` test set runs against the real csv-stats and is
  skipped (with a printed reason) when the installed version is older than
  v1.

---

## Stages

Each stage lists files, the logging it adds (NOTE 2), and its tests. Tests
are for the user to run: **one package per pytest invocation**.

### Stage 1 — Fix the CI95 divergence (scistackplot, bug fix first)

Found while planning: the preview's CI95 is `1.96·SD/√n` (`ylimits.py:711`,
`series_stats.py:201`), but exported code emits seaborn `("ci", 95)`
(`codegen.py:74`), which is a **bootstrap** interval. So preview and export
draw different bars today, and neither matches csv-stats `describe` (t-based).

- One spread function (in `ylimits.py`, which already claims to be "the place
  that says what ± SEM means"), with CI95 = **t-based**
  (`t.ppf(0.975, n-1)·SEM`, needs scipy; add it as a scistackplot
  dependency, or use a small t-quantile table if you'd rather not). `series_stats`
  (numpy twin) calls it or shares its constant source.
- codegen emits the same function (literal `errorbar=` callable over the
  values) instead of `("ci", 95)`.
- Log: DEBUG `spread ci95: t-based, n=… t=…` once per figure.
- Tests: preview == export == reference for n = 2, 5, 30; parity test in
  `test_codegen.py`. **Numbers change** for every saved CI95 plot: note it
  in the changelog (no migration).

### Stage 2 — Package scaffolding + engine seam

- `scistackstats/` (`src/scistackstats`, pyproject: pandas, numpy, scipy,
  csvstats>=1.0, scistackplot, scistacklog) and `scistackstatsdb/`
  (scistackstats, scistackplotdb, scistack-db).
- Register everywhere a package list exists: `dev-install.sh` (layers 0.5 /
  3), `tools/audit/run.sh`, `scistacklog` layer names,
  `sciduckdb/tests/test_fetch_locking.py` (scistackstatsdb touches DuckDB),
  `scistack-gui/pyproject.toml`.
- `engine.py`: `Engine` protocol (`run(test_id, data, design, options) ->
  dict`, `describe(...)`, `correct(pvals, method)`, `versions()`),
  `CsvStatsEngine`, and `set_engine()` for tests. Checks the csv-stats
  version on first use and logs `engine csvstats x.y.z` (INFO once).
- Tests: import-direction guard (AST): no module under `scistackplot*`
  imports `scistackstats*`; nothing but `engine.py` imports `csvstats`.

### Stage 3 — Design derivation + test catalog (pure)

- `design.py`: `Design(dv, factors: list[Factor(name, levels, kind:
  within|between|mixed_membership, override)], subject, covariates,
  strata_keys, depth, n_per_cell)`.
  `derive_design(plot_spec, rows, overrides) -> Design`, reading the grouping
  layers via `roles.grouping_layers`, the subject via `roles.sample_key`, and
  strata via `fanout_keys` + FACET roles.
- `catalog.py`: `TESTS` (the csv-stats ids from requirements §3 → label,
  family: parametric/nonparametric/spm/…, requirement predicate) and
  `why_unavailable(test_id, design) -> str | None`, **the one refusal
  owner**, like `roles.kind_requirement`. `available_tests(design)` returns
  every test with its reason, and `suggest(design)` returns the recommended
  test plus its nonparametric twin (never applied automatically).
- Guards as refusals or warnings: no subject + within test; one level; 1-D
  measure + non-SPM test (unless a scalar cell statistic is chosen); SPM on a
  scalar; `mixed_membership`; more than 3 factors for ANOVA.
- Log: INFO `design [panel=…]: dv=…, within=[…], between=[…], subject=…,
  n=…`; DEBUG per factor for the membership evidence (subjects seen at
  several levels).
- Tests: design fixtures from the aim2 example dataset shape (4-level gait).
  Include the unbalanced example from grouping-and-collapse.md and a
  mixed-membership case.

### Stage 4 — Input rows, including 1-D curves (scistackplot addition)

- `scistackplot.export.sample_curves(spec, table, depth=None) ->
  DataFrame`: the same `_plan` + collapse chain as the figure, but it keeps
  1-D cells, per-position means through the **numpy reducer**
  (`reducer._chain` / `series_stats`). One row per sample and one array
  cell. It is not offered as a CSV (`data_unavailable` is unchanged).
- Parity test: the band a line/band figure draws == the per-position
  mean/SD computed over `sample_curves` rows.
- Stats call `plot_data(fields_as_columns=False)` for scalars (a struct's
  `ColName` becomes a stratum or a factor, following its role).
- Log: INFO `[sample-curves] figure …: N curves, lengths {101: N}`. When
  lengths differ, WARN, naming the rows. csv-stats refuses them, and the
  WARN means the log already says why.

### Stage 5 — Runner, families, report

- `spec.py`: `StatsSpec(format=1, analyses: list[Analysis], variant_mode:
  pinned|sweep, sweep_family: none|<method>)`;
  `Analysis(id, test, options{alpha, ci_level, tails, equal_var,
  sphericity, post_hoc, correction}, family_scope, depth, overrides, bars:
  BarRule{show, pairs: significant|all|adjacent, label: stars|p|p_exact},
  hidden_bars: list[pair key])`. `to_dict` / strict `from_dict` for saves;
  salvage for opens (stage 8).
- `run.py`: `run(stats_spec, plot_spec, table) -> StatsReport`.
  For each analysis, stratum and (sweep) variant: derive the design, then
  `why_unavailable` (a refusal is recorded as a stratum result with a reason,
  never an exception), then `engine.run`. Then apply the families
  (`engine.correct`) and write `p_adjusted` + `family` into each effect or
  comparison.
- `StatsReport`: `strata: [{key (panel_key_text), variant, design, result
  (C3 core), refused}]`, `families: [{scope, method, members}]`,
  `family_statement` (the S7 sentence, written by `format`), `engine`
  versions, `plot_spec_digest` (so a GUI or report can tell a result is
  stale against the figure).
- Log: `Log.timer("stats_run")` with phases (plan, design, engine per
  analysis, families). INFO per analysis: `N strata, M refused (reasons),
  K warnings`.
- Tests with the fake engine: stratum keys, family membership per scope,
  refusal carried not raised, determinism (same input → identical
  `to_dict`).

### Stage 6 — Formatting + export + methods text

- `format.py`: significant-figure policy, `p` (`p < .001`, no leading zero,
  APA), stars rule (`*` .05, `**` .01, `***` .001; configurable), statistic
  strings per `statistic_name`/`df` shape (`t(37.6) = −2.14`, `F(2, 38) =
  …`, `U = …`, SPM "cluster 34.7–58.2 %, p = .003"), effect-size strings.
- `export.py`: `export(report, fmt, *, detail="core"|"full")` for fmt in
  `txt, md, json, toml, csv, latex`.
  - csv: **one row per effect** and **one row per comparison**, as two
    tables (two files, or a `table=` arg).
  - latex: booktabs, `siunitx`-free by default.
  - toml: needs `tomli-w` (writer), added as a dependency.
  - json: `StatsReport.to_dict()` (full precision).
  - Every format carries `family_statement` and the engine versions.
- `methods_text(report)`: a paragraph naming the tests, corrections, alpha,
  sphericity handling, software and versions, plus a citations list.
- Tests: golden strings per format from a fixed report; a LaTeX-escaping
  case (`_`, `%`, `&` in level names).

### Stage 7 — Overlays (scistackplot generic hooks + stats conversion)

- **Derived difference bars.** `DifferenceBar.origin` (`manual|test`,
  default manual). `resolve(..., derived_bars: list[DifferenceBar] = ())` is
  merged in `bars_for_panel`, **manual wins on a duplicate pair** (WARN).
  `figure_has_bars` sees both, so D7 range sync works unchanged. `meta`
  reports the origin so the GUI can show "from test: <analysis>" and a
  hide ✕.
- **Span shading** (for SPM clusters): new `SpanShade(match, series |
  None, start, end, label)` in a 1-D x unit (sample index). It is drawn by
  mpl and plotly (gid/shape names `span-shade:*`, dark-mode inked like
  `difference-*`) and by codegen (baked, like D8). It is also a generic hook:
  `resolve(..., spans=...)`.
- `scistackstats.overlays`: `bars_for(report, analysis)` turns the
  pairwise comparisons into `DifferenceBar`s (ends from comparison `a`/`b` →
  `{x layer: level}` via the design's factor ↔ layer map; label from
  `format`; hidden pairs removed). `spans_for(report)` turns SPM clusters
  into `SpanShade`s.
- **Pairs policy** (difference-bars "Next" §6): default `significant`;
  `adjacent` and `all` available. More than 10 bars in one panel → WARN +
  GUI note.
- **Within-subject error bars** (Cousineau–Morey with the Morey
  correction): `ErrorBand.CI95_WITHIN` in scistackplot's spread function
  (stage 1's owner). It is only available when the sample key is a subject
  that recurs across the grouping (the `line_recurrence` rule). This is
  descriptive and belongs to plot.
- Tests: preview/export parity for derived bars and spans (the existing
  `plot_geometry.py` readers); import-direction guard still green.

### Stage 8 — Persistence: attached to the saved plot

- `scistackplotdb.saved`: the envelope gains `attachments: dict[str,
  Any]`, stored and returned **verbatim** (plotdb never parses it).
  `save_plot(..., attachments=)`. Plot and stats are versioned together
  because they are one row.
- `scistackstatsdb.saved`: `stats_spec_of(saved_plot) -> (StatsSpec,
  notes)` salvages through a **generic** `scistackplot.restore` walker.
  Stage 8 exposes `restore_dataclass(cls, raw)` from `restore.py` (today's
  `restore_spec` becomes a caller), so there is no second salvage
  implementation. Notes appear in the Saved plots rail like plot notes.
- Reconcile: an analysis whose factors or subject no longer exist → note,
  kept (inert), same policy as stale roles.
- Tests: round-trip with every StatsSpec field non-default (the same guard
  as `full_spec()`); salvage of an unknown test id → note + analysis kept
  inert; opening writes nothing.

### Stage 9 — Variant sweep

- `scistackstatsdb.sweep`: given the plot's variant set, enumerate every
  variant of the swept axes (existing `scistackplotdb.variants` /
  `variant_graph`; no second resolver). It loads them with `Variant` as a
  stratum column. Default mode stays `pinned` (the Plot Studio default
  selection, S3).
- Sweep results: one stratum per variant; the `variants` family is opt-in
  (S7: no privileged default, statement always shown).
  `format.specification_curve(report, analysis, effect)` returns rows
  (variant, estimate, CI, p, p_adjusted), which the GUI draws.
- Log: INFO `sweep: N variants over axes […]`; WARN when a variant has no
  rows in some stratum.
- Tests: a two-branch-param fixture (the `test_plot_sweep.py` integration
  dataset); variant identity through `bindings.variant_signature`.

### Stage 10 — Code export + pipeline endpoint

- `scistackstats.codegen.generate_stats_function(stats_spec, plot_spec,
  table)`: a standalone function that reuses
  **`scistackplot.codegen._preamble`** (promote it to a public name) to
  rebuild the sample rows. It then emits the same `csvstats` calls
  `engine.py` makes, the family corrections, and returns the report dict.
  **Parity test (feedback_export_matches_preview):** executing the generated
  code gives JSON equal to `run(...)`, for every catalog family.
- `scistackstatsdb.endpoint.generate_stats_endpoint`: a `stat_<name>`
  function (the body above) + a `for_each` call with **no iteration keys**
  (P7), reusing `scistackplotdb.endpoint`'s input/variant expressions
  (`variant_expression`, joined-factor inputs), not copies. The output
  variable is declared through `required_declarations`.
  `normalize_stat_payload` stores it. `scidb report` shows it (its generic
  JSON rendering; nicer rendering deferred).
- Tests: the fan-out/variant input expressions are identical to the plot
  endpoint's for the same spec; the generated endpoint runs in a temp
  project and stores one record.

### Stage 11 — GUI

Backend (`scistack-gui`):
- `api/stats.py` + `services/stats_service.py` (reusing the plot panel's
  cached source), handlers: `stats_catalog` (design readout + every test
  with its reason + suggestion), `stats_run_start` / progress notification
  (P8), `stats_export` (fmt → file or clipboard text), `stats_methods_text`,
  `stats_add_to_pipeline`. `plot_saved_save` / `plot_saved_open` carry
  `attachments.stats`. Sweep runs over the same job path.
- Every handler logs counts and timing (startup/RPC count convention).

Frontend (`PlotStudio/`):
- **Statistics > Tests** section (the Statistics group also shows when only
  this is available):
  - design readout (DV, factors with a within/between chip + override
    dropdown, unit, n per cell);
  - "+ Add test" menu from `stats_catalog` (unavailable tests greyed out
    with the backend's reason, the suggested test marked);
  - per-analysis options, family scope, bar rule, and depth (LMM only).
  - Pure logic in `statsSpec.ts` + tests.
- **Variants**: Pinned | Sweep toggle beside the variant rows; the family
  statement is always visible under it (S7).
- **Results pane**: a collapsible drawer under the figure, outside
  `canvasRef` so it doesn't resize the preview. It holds per-analysis
  tables (effects, comparisons, assumptions ✓/⚠, warnings), a stale badge
  when `plot_spec_digest` ≠ current, the sweep specification curve (plotly,
  from backend rows), and an SPM statistic curve with its threshold.
- **Figure toolbar**: Export stats (format dropdown), Copy methods text, Add
  stats to pipeline.
- Derived bars appear in Difference bars with "from test" + a hide ✕.
- Colours via `var(--ps-*)`; rebuild **both** vite targets; add
  `docs/gui-manual-testing-todo.md` §0zzj (Tests section), §0zzk (Results
  pane + export), §0zzl (sweep), §0zzm (SPM shading).

### Stage 12 — Docs

- `docs/claude/statistics.md`: owners, data flow, traps (the user-facing
  counterpart of the design doc).
- Update `difference-bars.md` ("Next" → built), `grouping-and-collapse.md`
  (sample key = subject in stats), `plot-studio-controls.md` (new controls),
  `saved-plots.md` (attachments), and `decisions.md` (ADR entries for P1–P9).

---

## Order and dependencies

1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12.
Stages 6 and 7 can swap. Stage 11 can start its backend after stage 5 and
build up section by section (tables first, overlays after 7, sweep after 9).
Useful checkpoints: after **5** (scriptable stats end to end), after **7**
(stats on figures), and after **11** (GUI).

## Out of scope (v1)

Standalone stats specs (without a plot); power analysis; Bayesian tests;
MATLAB-side stats UI (MATLAB reaches `scistackstats` through the bridge
only); nicer `scidb report` rendering of core records; bars between marks in
different panels.
