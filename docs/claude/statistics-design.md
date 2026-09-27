# Statistics — Design

Status: **design decided** (2026-09-27). S1–S8 locked; all open questions
resolved. The component inventory below is the scope checklist. No code yet.
Blocked on csv-stats v1 ([csv-stats-v1-requirements.md](csv-stats-v1-requirements.md)).
Next step: a `.claude/plan-*.md` per scistack stage.

## Where this sits

Processing and visualization are built; statistics completes the data
lifecycle. Two surfaces already exist and are **not** replaced:

- **`stat_` leaf functions** ([endpoints-viz-and-stats-design.md](endpoints-viz-and-stats-design.md)
  D5, [plotting-leaf-nodes.md](plotting-leaf-nodes.md)) — the user writes the
  test; results are JSON records with lineage, `finalized` draft/record, MATLAB
  parity via `normalize_stat_payload`.
- **`scidb report`** — stats tables + `stats.csv` for finalized records.

What is new is the **interactive, no-code layer** — the stats equivalent of
Plot Studio: choose a design from the same roles, preview numbers and plot
overlays, export in many formats, save the spec.

## Decisions

### S1. Python is the only stats engine — **DECIDED** (2026-09-27)

MATLAB users reach stats through the bridge; nothing is reimplemented in
MATLAB. Removes the cross-engine disagreement problem (Statistics Toolbox vs
statsmodels differ on mixed-model df, sphericity corrections, post-hoc
defaults). Available in `.venv` today: scipy 1.17, statsmodels 0.14,
pingouin 0.6, csvstats. **Not installed: `spm1d`** (see S4).

### S2. The design comes from Plot Studio roles — **DECIDED** (2026-09-27)

No second vocabulary for "what is the design." GROUP / FACET / ITERATE /
COLLAPSE ([grouping-and-collapse.md](grouping-and-collapse.md)) map to:

| Plot role | Statistical meaning |
|---|---|
| GROUP (each layer) | a factor in the model (the fixed effects) |
| FACET / ITERATE | run a separate analysis per level (stratify) |
| COLLAPSE chain | aggregation down to the unit of analysis |
| `sample_key` (outermost collapsed key) | the **experimental unit**: the subject in a repeated-measures design; the random effect in a mixed model |
| Plotted variable | the dependent variable |

Consequences:
- Within- vs between-subject is **derivable**: a GROUP factor is within-unit
  when every sample appears at more than one of its levels, between-unit when
  each sample sits at exactly one. Derive it from the data and show it; let the
  user override. One owner for this classification.
- The pseudoreplication guard comes for free: if nothing is collapsed down to a
  sample, the test runs on trials. Warn prominently, or require a mixed model.
- Collapse "pooled" (weight by N) versus the nested unweighted chain changes
  what the test sees. The stats layer must consume the **same** reduced table
  the plot draws. "Export matches preview" extends to "stats match the plot."

### S3. Variant selection: latest by default, sweep on request — **DECIDED** (2026-09-27)

Default = the Plot Studio default variant selection: one fully-pinned variant,
per-location latest code (`{CodeIsLatest: True}`), first natural-sorted level
for branch params ([plot-variant-rows.md](plot-variant-rows.md),
[variant-axis-node-binding.md](variant-axis-node-binding.md)). Reuse that
owner; do not write a second resolver.

**Sweep** = run the same analysis once per variant and present the results
side by side (a results table + a specification-curve plot of effect ± CI per
variant). Every result carries its variant identity via
`bindings.variant_signature`.

### S4. SPM1d is in the first release — **DECIDED** (2026-09-27)

Statistical parametric mapping on 1-D continua (gait cycles). Uses the `spm1d`
package (pure Python; numpy/scipy/matplotlib). It covers t-tests (one-sample,
paired, two-sample), regression, 1-/2-/3-way ANOVA including repeated
measures, Hotelling/CCA for vector fields, and permutation (nonparametric)
versions. Output: the test-statistic curve, the critical threshold, and
supra-threshold clusters, each with a p-value and start/end nodes.

Requirements this puts on the data layer:
- **Equal-length samples**: every curve fed to one test has the same number of
  nodes (typically time-normalized to 101 points). Validate before calling,
  and name the offending records; never resample silently.
- Rows = observations at the unit of analysis. The collapse chain runs on
  whole curves (the per-node mean), which the 1-D plot kinds already do for
  bands.
- Overlay: shade significant clusters on the 1-D mean ± band plot, with the
  cluster p-values annotated. Plus the SPM{t}/SPM{F} curve with its threshold
  as its own panel.

### S5. A core result record + per-test `details` — **DECIDED** (2026-09-27, was O1)

Every result has a required common core: test, statistic, df, p, effect size,
CI, n, warnings, engine + versions, and variant identity. The per-test JSON
from D5 is kept under `details`. This **extends** D5; it does not reverse it.
Overlays, the formatting owner, exporters and the sweep table read only the
core.

### S6. csv-stats owns the math — **DECIDED** (2026-09-27, was O6)

All test implementations, including mixed models and their df method, live in
the user's **csv-stats** package (PyPI). scistack never computes a test
statistic itself; it builds the design, calls csv-stats, and normalizes the
result into the S5 core. Gaps in test coverage are filled **in csv-stats**,
not worked around in scistack.

### S7. Variant sweep: clarity over a default — **DECIDED** (2026-09-27, was O3)

Neither "one correction family" nor "uncorrected multiverse" is privileged.
The requirement is that the selection is always **stated where the numbers
are shown**: which variants, which family, which correction (or "none —
multiverse"). The same statement goes into every export and into the
generated methods text.

### S8. A `scistackstats` package — **DECIDED** (2026-09-27, was O2/O5)

Mirror the plotting split:

| Package | Depends on | Owns |
|---|---|---|
| `scistackstats` (pure) | pandas, numpy, csvstats, `scistackplot` (core only) | analysis spec, design derivation (within/between, unit), test dispatch to csv-stats, the S5 core record, the formatting policy, exporters, code export, overlay *conversion* |
| `scistackstatsdb` | `scistackstats`, `scistackplotdb` | loading through the plot source, saved analysis specs, the `stat_` write path |

Why it works: `scistackplot`'s core needs only pandas/numpy/scistacklog;
plotly is isolated in `render/`. Depending on it does not pull in a plotting
stack.

**The dependency is one-way: stats → plot, never plot → stats.** That
decides three ownership questions:
- **Roles / reduction / variant selection** stay in `scistackplot`, and stats
  consume them (S2, S3).
- **Descriptive reductions** (mean, median, SD, SEM, CI95, IQR, which
  `spec.ErrorBand` / `series_stats` already compute) stay owned by
  `scistackplot`. The stats Descriptives table calls the same functions, so a
  plotted CI95 and a tabulated CI95 can never disagree (e.g. t- vs z-based).
- **Overlays**: `scistackplot` owns the annotation shapes it can draw
  (`diffbars.DifferenceBar`, later a cluster-shading shape for SPM).
  `scistackstats` converts S5 results *into* those shapes. The plot layer
  never sees a stats result type.

**O5 folded in**: the `stat_` leaf stays the only write path. A Studio-built
analysis runs as a `stat_` function calling `scistackstats`, so `finalized`,
lineage and `scidb report` apply unchanged.

## Component inventory

✅ = covered by a decision above; the rest are to design.

**Specification**
- ✅ Factors, stratification, unit of analysis (S2)
- Within/between derivation owner + override (S2 consequence)
- Covariates (ANCOVA) — no plot role carries these today
- Interactions: default full factorial vs. main effects only
- Contrast coding, reference level, planned contrasts
- Test recommendation from the design (suggest the test + its nonparametric
  alternative; never switch silently)

**Validity**
- Assumption checks in the result itself: normality, homogeneity of variance,
  sphericity (+ GG/HF correction), residual diagnostics
- Model health: convergence, singular fits
- Guardrails: n < 3 per cell, empty cells, unbalanced designs, trials treated
  as independent

**Beyond p**
- Effect sizes + their CIs (d/g, partial η²/ω², r, Cramér's V)
- Post-hoc tests and corrections (Tukey, Holm, Bonferroni, FDR)
- Estimated marginal means
- Power: a priori sample size, sensitivity analysis
- Model comparison (AIC/BIC, LRT)

**Data handling (all recorded, never hand-edited)**
- Missing data policy; outlier rules as declared, hidden-not-deleted
  exclusions
- N per cell + exclusion flow in every result
- Transforms (log, rank, centering) as recorded steps

**Test families for v1 candidates**
- ✅ SPM1d (S4)
- t-tests, one-/two-/three-way (RM/mixed) ANOVA, nonparametric equivalents
  (Mann–Whitney, Wilcoxon, Kruskal–Wallis, Friedman)
- Linear mixed models (see O6)
- Correlation/regression (Pearson/Spearman, simple/multiple)
- Reliability/agreement: ICC (form chosen explicitly), SEM, MDC, Bland–Altman
- Categorical: χ², Fisher's exact test
- Descriptive "Table 1" (mean ± SD / median [IQR] by group)
- Summary statistics: mean/median/mode, SD/SE, 95% CI, quartiles, min/max, n

**Output**
- One formatting owner: rounding, significant figures, `p < .001`
- APA-style sentences; generated methods paragraph (tests, corrections,
  software versions, citations)
- Export: plaintext, JSON, TOML, CSV, LaTeX (booktabs), Markdown
- Code export: a standalone Python script that reproduces the result
  (the stats sibling of plot codegen)
- Tidy data export for R / JASP / jamovi / SPSS users

**Plot overlays (consume results, never recompute)**
- Significance brackets/asterisks on top of `diffbars.py`
- Error bars matching the test (within-subject CI, Cousineau–Morey, for RM
  designs)
- Regression line + CI band, EMM plots, QQ/residual diagnostics
- SPM cluster shading (S4)

**Provenance**
- Results are lineage records and go stale when their inputs change
- Bootstrap/permutation seeds are part of the result's identity
- Engine + package versions stored with each result
- Validation suite against reference values (R output, textbook datasets)

**Confirmatory vs. exploratory**
- Build on the Hypothesis tabs + `finalized`: declare the analysis plan first,
  tag each result confirmatory or exploratory, and filter the report on it

## Open questions

All resolved (2026-09-27). O1, O3, O6 and O7 became S5, S7 and S6; O2 and O5
are in S8. The rest:

- **O4 → decided:** analysis specs are **attached to a saved plot** for now,
  sharing its roles and variant selection. Standalone specs are deferred.
- **O8 → decided:** **csv-stats wraps `spm1d`.** scistack never imports
  `spm1d`.
- **O9 → decided:** csv-stats' coverage is extended for v1. The full
  requirements handoff, written to be used in the csv-stats repo, is
  [csv-stats-v1-requirements.md](csv-stats-v1-requirements.md): contract,
  core record, test catalog, SPM1d output shape, bug list, validation
  references.

## Related docs

- [endpoints-viz-and-stats-design.md](endpoints-viz-and-stats-design.md) —
  `stat_` leaves, D5 result shape, report CLI
- [grouping-and-collapse.md](grouping-and-collapse.md) — the roles S2 reuses
- [plot-variant-rows.md](plot-variant-rows.md),
  [variant-space.md](variant-space.md) — S3's selection owner
- [difference-bars.md](difference-bars.md) — significance-bracket base
- [saved-plots.md](saved-plots.md) — O4
