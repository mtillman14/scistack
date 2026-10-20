# Statistics — how it works (owners, data flow, traps)

The user-facing counterpart of [statistics-design.md](statistics-design.md)
(the decisions S1–S21) and `.claude/plan-statistics.md` (the stages). Read
this before changing anything in `scistackstats`, `scistackstatsdb`,
`csvstats.descriptive`/`csvstats.core`, `scistackplot.design` or StatStudio.

## Packages

| Package | What it is |
|---|---|
| `csv-stats` (`csvstats`, own repo, cloned at `/workspace/csv-stats`) | ALL math. `descriptive` = every descriptive formula (light: numpy/pandas/scipy). `core` = tests returning the C3 core record (`ttest`, `rank_test`, `anova`, `kruskal`, `friedman`, `spm_ttest`, `correct_pvalues`, `assumption_checks`). `lmm` = `lmm` and `lmm_pairwise` (own REML fit `lmm_fit.MixedModel`, any lme4-style random effects, Satterthwaite df, 0.4.0; testing against R: lmm-r-oracle-testing.md). The old `ttest_ind`/`anova1way` API is untouched and never called by scistack. |
| `scistackplot` | Rows and design. `design.py` (which fields sync), `spread.py` (error bars call csv-stats), `export.plot_data` / `export.sample_curves` (the rows), `diffbars.with_derived_bars` (derived brackets join a resolve). |
| `scistackstats` (pure) | StatStudio's engine: spec, seed, rows, design derivation, tests, families, formatting, export, overlays, codegen. |
| `scistackstatsdb` | Loading through `ScidbSource`, saved stats tables + presets (`VersionedStore`), the `stat_` endpoint, the bundle section. |
| `scistack-gui` | `api/stats.py` + `services/stats_service.py`; `frontend/src/components/StatStudio/`; `PlotStudio/StatsLink.tsx`; the `stat` view in `extension/src/plotPanel.ts`. |

Dependency direction: stats → plot, never plot → stats (AST guard
`scistackstats/tests/test_guards.py`). Only `scistackstats/engine.py`
imports `csvstats` (same guard). scistackplot imports `csvstats.descriptive`
for error bars.

## Owners (one per concept)

| Concept | Owner |
|---|---|
| Every descriptive formula (mean, SD n−1, SEM, t-based CI, quartiles, skew G1, kurtosis G2, …) | `csvstats.descriptive` |
| How a plot DRAWS "no interval" (n < 2 → zero width) | `scistackplot.spread` |
| Which PlotSpec fields are "design" (sync) vs "display" | `scistackplot.design.DESIGN_FIELDS` |
| How two designs differ / copying one onto another | `scistackplot.design.design_diff` / `copy_design` |
| Plot → table settings | `scistackstats.seed.seed_from_plot` |
| The rows a statistic sees (scalar and 1-D) | `scistackstats.tables.analysis_frame` (via `plot_data` / `sample_curves`) |
| Experimental unit id (sample key + schema keys above it) | `tables.analysis_frame` (`UNIT` column) |
| Within / between / mixed_membership (data, or the schema for a key below the unit) | `scistackstats.design.derive_design` |
| Which units are incomplete (missing a within cell) | `scistackstats.design.incomplete_units` |
| The recommended tests + their evidence | `scistackstats.catalog.recommend` (assumption math: `csvstats.core.assumption_checks`) |
| The decision the user must make (banner) | `scistackstats.catalog.needed_action` |
| Missing-data policy (`use_all` / `complete_cases`) and its labels | `scistackstats.spec` (`MISSING_POLICIES`, `MISSING_LABELS`) |
| What a test left out, and its wording | `scistackstats.exclusions` |
| The missing-data caption sentence | `scistackstats.format.missing_sentence` |
| Primary vs secondary comparisons | `scistackstats.design.comparison_tiers` |
| Which test, and why not | `scistackstats.catalog` (`select_tests`, `why_unavailable`) |
| What csv-stats can't do yet | csv-stats (`core.NotSupported`) → `EngineRefusal(kind="gap")` |
| Effect-size names/formulas/defaults; difference definitions | `scistackstats.effects` |
| Correction families + the S7 sentence | `scistackstats.run` (`apply_families`, `family_statement`) |
| Every number's text, every table, every caption | `scistackstats.format` (`Grid`) |
| Text formats | `scistackstats.export` (renders `Grid`s only) |
| Stats preset = which settings | `scistackstats.restore.STATS_SETTINGS` |
| Plot ↔ stats link | `StatsSpec.linked_plot` only (no back-reference) |

## Data flow

```
PlotSpec ──seed_from_plot──► StatsSpec(design=PlotSpec, dvs, descriptives, comparisons, formatting, overlay)
                                  │
   load_tables(source, spec) ─────┤ one LongTable per DV variable (Plot Studio's cached source in the GUI)
                                  │
   analysis_frame(spec, dv, table, curves=False|True)
      analysis_spec: design → measures=[dv], kind=BAR (scalar) or BAND (curves), display reset,
                     ColName columns → filter + ITERATE, sweep → variant axes ITERATE
      rows = plot_data(...)  or  sample_curves(...)        ◄── the figure's own reduction
      factors (inner first), strata (ITERATE+FACET), unit, UNIT ids, levels, averaged
                                  │
   describe_table ── csvstats.descriptive.summarize per DV × stratum × column ─► DescriptivesTable
   run ── per analysis × DV × stratum: derive_design → select_tests → omnibus + primary pairs
          → engine.run (csv-stats) → apply_families ─► StatsReport (JSON, full precision)
                                  │
   format.*_grid ─► Grid (strings, caption, notes) ─► export.render (txt md csv latex html json) / GUI
   overlays.bars_for ─► DifferenceBar(origin="test") ─► with_derived_bars ─► plot resolve/export/save
   codegen.generate_stats_function ─► stat_ fn (embeds the spec, calls scistackstats) ─► scistackstatsdb endpoint
```

## Traps

- **Rows are read at a bar (or a band for SPM).** The design carrier's
  `kind` is display. Never read rows at the figure's kind: a spaghetti adds a
  `final` step, and a box needs a sample.
- **The unit is composite.** Subject "01" at two sites is two units. Use
  the `UNIT` column, never the raw sample key, as `subject=` for csv-stats.
- **Pooled / nothing collapsed ⇒ no unit.** Every factor is between, with a
  `no_unit` warning (possible pseudoreplication).
- **Data is never dropped silently (user rule, 2026-10-19).** Every unit or
  row a test leaves out is named in the table notes, shown as "n = 15 of 17",
  and logged at WARN (`exclusions`). `Analysis.missing` is the user's
  recorded choice: `use_all` (default; the recommendation picks a mixed
  model when within units are incomplete) or `complete_cases` (incomplete
  units are left out of EVERY test of the analysis, named). Every caption
  states the policy (`format.missing_sentence`).
- **Missing data never makes a test unavailable.** `why_unavailable` is
  about the data's SHAPE only. A complete-cases test (paired t, RM-ANOVA,
  Friedman, `catalog.COMPLETE_CASES_TESTS`) still runs under `use_all` and
  names whom it left out.
- **family "auto" is the default.** `catalog.recommend` runs csv-stats'
  `assumption_checks` (Shapiro–Wilk on residuals, Levene across between
  cells) and recommends; the evidence travels in `selection.recommended`
  and the GUI shows it. Any override (`pairwise_test`/`omnibus_test`) wins
  and is labelled "chosen". Pre-testing normality to choose a test is
  debated in the statistics literature: it is a recommendation with its
  evidence, never a silent switch.
- **mixed_membership**: a schema key nested below the unit (session under
  subject) is within by construction (`Factor.basis == "schema"`; Stage E).
  Any other mixed factor is refused until the user decides; the GUI shows
  `catalog.needed_action` as a banner with the two choices.
- **LMM df are Satterthwaite** (csv-stats 0.4, matched to lmerTest / emmeans on
  75 R reference datasets, `csv-stats/tests/test_lmm_reference.py`):
  - coefficients: t(df);
  - terms: type-III F(q, ddf). The Fai–Cornelius ddf depend on the BASIS of L,
    so `lmm.type3_hypotheses` builds the type-III basis lmerTest reports,
    written from Goodnight 1980 and the SAS definition, covariates included.
    A factor crossed with a covariate is tested at covariate = 0
    (`type3_at_covariate_zero` warning);
  - `lmm_pairwise`: FULL-model contrasts within each combination of the other
    factors (emmeans-style), ONE engine call per stratum
    (`run._pairwise_full_model`), covariates at their mean.
  - **Model formula** (`Analysis.formula`, right side only; `Analysis.covariates`;
    plan-statstudio-model-formula): owner `scistackstats.model` (what each
    name means here); grammar csv-stats `lmm_formula` (via `engine.parse_formula`,
    the only csvstats import). The DESIGN owns the factors: a stored formula's
    fixed part is rebased onto it at run time (`check(rebase=True)`); the
    popup's Apply writes factor changes to the design (`apply_to_design`: add
    → Group, drop → Collapse), so the divergence banner shows them. A formula
    is stored only when it says more than the design (covariates or a random
    part other than `(1 | unit)`), and then forces lmm / lmm_pairwise
    (`catalog.MIXED_FORMULA_REASON` greys the rest). Covariates are joined
    per ANALYSIS ROW after the figure's reduction (`tables.join_covariates`),
    loaded by scistackplotdb's one loader `_keyed_values` (shared with
    grouping variables); one recorded below the rows' level is refused.
    The unit's name in a formula (`subject`) maps to the `UNIT` column.
  The fit is csv-stats' own REML (`lmm_fit.MixedModel`), not statsmodels:
  statsmodels' SEs and boundary handling did not match lme4. A df that
  can't be computed falls back to Wald (`df_method: "wald"`, a
  `satterthwaite_unavailable` warning), and the caption says so.
- **csv-stats >= 0.4.0** is needed for mixed models (0.3.0 for `assumption_checks`). On an
  older install the engine refuses the mixed model as a "gap" and the
  recommendation falls back to parametric with a WARN (never a crash).
- **csv-stats gaps are refusals, never fallbacks.** Don't compute a
  statistic in scistackstats. Add it to `csvstats.core`.
- **pingouin 0.6 renamed columns:** `p_unc`, `p_GG_corr` (old `p-unc`,
  `p-GG-corr`). `core._p_unc` / `_col` read both. One-way `pg.anova` calls
  the residual row "Within", N-way calls it "Residual".
- **Greenhouse–Geisser** applies to a term with a within factor of 3+
  levels; the corrected df are shown (fractional).
- **Default correction family = one DV × one stratum** (S13). Omnibus
  terms are not adjusted. `variants` is opt-in (S7).
- **Primary comparisons always run**, with every interaction p that
  contains the primary factor beside them.
- **Derived brackets are never stored.** The GUI sends `derived_bars` on
  resolve, export, save and add-to-pipeline; `api/plot._with_derived` merges
  them. Manual bars win a duplicate pair.
- **Brackets need equal designs** (`design_fingerprint`) and no active
  compare-to-reference (S9).
- **Codegen refuses** ColName DVs, joined factor variables, variant rows or
  axes, and an x variable, with the reason (`why_not_exportable`).
  Rebuilding those tables from `for_each` inputs would duplicate the
  source's logic.
- **The `stat_` endpoint has no iteration keywords** (P7): one record over
  the whole fan-out.
- **SPM needs spm1d** (`pip install spm1d`); without it the test is a "not
  yet in csv-stats" refusal and the spm tests skip. Curves must be equal
  length (time-normalised). The figure does not shade clusters yet.
- **perl and template strings:** scripted edits of `.ts` files ate `${…}`
  (twice). Use the Edit tool for TypeScript template literals.

## Where to look

- Decisions: [statistics-design.md](statistics-design.md); ADRs in
  [decisions.md](decisions.md) (D-2026-10-17-*).
- csv-stats status and gaps: [csv-stats-v1-requirements.md](csv-stats-v1-requirements.md).
- GUI controls: [stat-studio-controls.md](stat-studio-controls.md).
- Brackets: [difference-bars.md](difference-bars.md) ("Derived bars").
