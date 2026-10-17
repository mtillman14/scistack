# Statistics — Design

Status: **design decided** (2026-09-27; StatStudio decisions S10–S21 added
2026-10-17). All open questions resolved. The component inventory below is the scope checklist. No code yet.
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
- **Descriptive formulas** (mean, SD, SEM, CI95, IQR, …) are owned by
  **`csvstats.descriptive`** (revised 2026-10-17, S18). scistackplot's
  `spread.py` calls it for error bars, and the Descriptives table calls it
  too, so a plotted CI95 and a tabulated CI95 can never disagree.
  scistackplot therefore depends on csv-stats' light core (numpy, pandas,
  scipy).
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

### S9. Compared figures do not feed difference bars — **DEFERRED** (2026-09-27)

Plot Studio can now draw a measure relative to a reference level
(`PlotSpec.comparison`, `scistackplot.compare`,
[compare-to-reference.md](compare-to-reference.md)). A test on change scores
is a different test from a repeated-measures test on the raw values. Until
that is designed, **the S8 overlay conversion (results → `DifferenceBar`)
must not run while `spec.comparison` is active.** The check is
`capabilities.comparison.state` (`set` and `active` with no `inert`). Manual
difference bars (clicking two ticks) stay allowed on a compared figure. The
user decided this on 2026-09-27. No code enforces it yet because no feed
exists. The first stats → overlay code adds the guard and a test for it.

## StatStudio — decisions of 2026-10-17

The user decided these on 2026-10-17. They extend S1–S9 and replace O4
(below). The plan is `.claude/plan-statistics.md` (rewritten the same day).

### S10. StatStudio is its own studio

StatStudio is a separate surface. It is **not** a section inside Plot Studio.
It has its own webview tab (one tab per open stats spec, like plot tabs), its
own saved specs and its own presets. It can open three ways: from a
**StatStudio** button in Plot Studio (seeded from that figure, S12), from a
variable, or from a saved stats spec. Everything it does is doable in code
(`scistackstats` / `scistackstatsdb`). The GUI is a thin wrapper, as Plot
Studio is.

Plan stage 11's "Statistics > Tests section + results drawer inside Plot
Studio" is withdrawn.

### S11. Plot and stats may differ; banner + two-way "Sync to…"

A stats spec and its linked plot may hold **different** designs, so the user
can explore either way. Both studios show a banner when they diverge. Each
banner lists what differs (for example "Grouping: Brand ≠ Brand × Session")
and has a one-way copy button: **Sync to StatStudio** in Plot Studio, and
**Sync to PlotStudio** in StatStudio.

- **What syncs is the data design, and only that:** variables (with chosen
  `ColName` columns), grouping / facet / iterate / collapse roles, filters and
  exclusions, variant selection, level order and level groups. Display
  settings (kind, colours, sizes, text) and stats-only settings (statistics
  chosen, tests, effect sizes, formatting) never sync.
- **One owner for "which fields are design":** a field table in
  `scistackplot` (`design.py`, built like `presets.FIELD_CLASSES`). Every
  `PlotSpec` field must be classified, and a test fails on an unclassified
  field. Both studios read divergence from one `design_diff(a, b)` there.
  Plot never imports stats: the stats spec *holds* a plot-layer design
  object, and the GUI service compares the two.
- **The link lives on the stats side only:** `StatsSpec.linked_plot` (a
  saved-plot id) is optional. Plot Studio finds its linked stats specs by
  query (`scistackstatsdb`). It never stores a back-reference, so the link
  has one owner.
- **Unsaved plot (default; revisit if it feels wrong):** the StatStudio
  button seeds an unlinked spec. Its banner says "not linked — save the plot
  to link it." Linking happens when the plot is saved.

### S12. Plot → table seeding rules (one owner)

`scistackstats.seed.seed_from_plot(plot_spec) -> StatsSpec` is the only
place that turns figure settings into table settings. It logs every mapping
and its reason at INFO. First rules:

| Figure | Seeded table |
|---|---|
| bar/band + SD | mean (SD) |
| bar/band + SEM | mean (SEM), with a GUI note that journals usually report SD |
| bar/band + CI95 | mean [95% CI] (t-based, the same owner as the plot: `scistackplot.spread`) |
| box / violin | median [IQR], with the nonparametric test suggested |
| spaghetti (a sample that recurs across x) | within-unit comparisons (paired / RM) suggested |
| scatter / strip | mean (SD) |
| compare-to-reference active | refused with a reason until S9 is designed |

Grouping layers become table columns (or nested column headers), outermost
first. Separate figures and panels become table sections. The collapse chain
sets the unit of analysis. Settle ambiguous cases with the user as they come
up, then add a row here.

### S13. Primary vs. secondary comparisons come from nesting

- **Primary:** comparisons between the **innermost** grouping layer's levels,
  inside each combination of the outer layers (simple effects). They form the
  default correction family (per DV × stratum). They are also what becomes
  plot brackets (S20).
- **Secondary:** main effects and interactions of every layer from the
  omnibus model.
- **Primary comparisons are always reported**, even when the interaction is
  not significant. The interaction's p sits beside them. Nothing is hidden
  automatically.

### S14. The spread of a difference belongs to the difference

A comparison reports the difference (B − A, with its direction stated) and a
spread of **that difference**, never the SD of each group. Default: the 95%
CI of the difference, computed the way the chosen test sees the data.

| Test | Difference | Default spread |
|---|---|---|
| paired t / RM | mean of within-unit differences | t-CI of those differences |
| Welch t | difference of means | Welch t-CI (Welch–Satterthwaite df) |
| Student t | difference of means | pooled-variance t-CI |
| ANOVA post-hoc | difference of EMMs | CI adjusted with the post-hoc method |
| Wilcoxon / Mann–Whitney | Hodges–Lehmann estimate | its distribution-free CI |
| LMM | fixed-effect contrast | CI from the model (df method stated) |

Other choices: SE of the difference; SD of the differences (paired only). IQR
is offered only for the rank tests. The GUI shows the definition in the
column header and the caption, for example "Δ = B − A, mean [95% CI of the
paired differences]". The runner logs it at INFO. Any row csv-stats cannot
produce yet is a csv-stats gap (S21).

### S15. Effect sizes are named exactly

There is no bare "Cohen's d" or "η²". One catalog
(`scistackstats.effects`: id, display name, formula, citation, which designs
it applies to) names the variant. The header, caption and exports carry that
exact name. The math is csv-stats'. First catalog:

- **Two means:** d_s (between-subjects, pooled SD), Hedges' g_s, d_z (paired,
  SD of differences), d_av (paired, mean of the two SDs), d_rm (paired,
  corrected for the correlation), Glass's Δ.
- **ANOVA:** η²p, η²G (the generalized eta squared, Olejnik & Algina 2003;
  the default for RM and mixed designs, Bakeman 2005), ω²p.
- **Rank tests:** rank-biserial r; Kendall's W (Friedman).
- **Correlation:** r, with its CI.

Defaults per design: two independent groups → g_s; paired → d_z, with d_av
offered; between-only ANOVA → η²p; any within factor → η²G. The defaults are
shown and never chosen silently.

### S16. Many variables per table, including `ColName` columns

A StatStudio table holds **several dependent variables, one row block
each**. Ways to add them:

1. inherited from Plot Studio's multi-variable selection (the same owner,
   not a copy);
2. StatStudio's own DAG-canvas picker (the same component Plot Studio uses);
3. a **`ColName` multi-select**: pick several columns of one Variable (one
   outcome per column, typical of CSV imports). Each column becomes its own
   DV.

Every DV is analysed separately under the one shared design. A DV whose data
does not fit the design (a missing factor, a 1-D measure in a scalar test)
is refused **for that DV only**, with the reason in its row. Default
correction family: within one DV (S7: the caption states it). Correcting
across DVs is an option, not the default.

### S17. Every table has a caption, written by the backend

`scistackstats.format.caption(report)` writes it. The GUI only displays it.
The caption says:

- what a cell is, e.g. "mean (SD) of 12 per-participant means; trials
  averaged within participant first"
- the unit of analysis and n per cell, plus anything missing or excluded
- the tests used, the correction family and method, and the effect-size and
  difference definitions (S14, S15)
- the variant selection (S7)

The caption goes into every export.

### S18. Two tabs: Descriptives and Comparisons

- **Comparisons** owns the test results (the S5 core records).
- **Descriptives** owns the summary statistics. It can *display* an optional
  p column, and later compact-letter superscripts, read from the Comparisons
  results. It never recomputes them.

This is a first pass. Expect the frontend and backend split to be tuned
after use.

Which statistics are available: the "large built-in set" (n, mean, median,
mode, SD, SEM, CI95, IQR, quartiles, min, max, range, CV, skewness, kurtosis,
percentiles) is chosen per variable. **Ownership (decided 2026-10-17, revises S8):** `csvstats.descriptive`
is the one owner of every descriptive formula, including the ones a figure
draws. scistackplot decides only *which rows* (grouping, collapse) and how
to draw "no interval" (zero width). Plot error bars, the Descriptives table
and csv-stats' own test results all call the same functions, so they cannot
disagree. An earlier draft split ownership by "can a figure draw it"; it was
dropped as two owners of one concept (csv-stats' tests report means and CIs
too).

### S19. Publication touches: first pass vs. later

**First pass:**
- the significant-figure / decimal policy and APA `p` formatting
  (`format.py`)
- project and plot `[aliases]` and the live schema level order (reuse
  `aliases.display_text` and `legend_entry_labels`; no copies)
- units in headers where the variable has them
- n per cell in the header or footnote, and the S17 caption
- a p column in Descriptives
- copy as a **rich table** (HTML on the clipboard, pastes into Word with
  borders), plus CSV / Markdown / LaTeX (booktabs) export
- a one-sentence APA result per comparison

**Deferred (too large for now):**
- compact letter display (a/b/ab; needs an insert-absorb algorithm and
  tests)
- `.docx` export with journal table styles
- journal style presets (APA / AMA / Vancouver)
- a methods paragraph with citations
- drag-to-reorder rows and columns
- merged headers deeper than two levels
- per-cell footnote markers
- the specification-curve plot for sweeps (S3)

### S20. Closing the cycle (first pass)

1. **Brackets back on the figure:** primary comparisons (S13) become derived
   difference bars (`origin="test"`) on the linked plot. They can be hidden,
   never deleted. They are blocked while `spec.comparison` is active (S9).
2. **Saved stats + stats presets** reuse `VersionedStore` (shared with saved
   plots and plot presets). Saved stats get their own table in
   `scistackstatsdb`. This replaces O4.
3. **Results are lineage records:** "Add to pipeline" writes a `stat_`
   endpoint (O5), so results go stale when their inputs change and
   `scidb report` sees them.

### S21. csv-stats v1 is far off: build against the engine seam

csv-stats still does all test math (S6). csv-stats v1 is a long way off, so
everything else is built now:

- **Descriptives** use scistackplot plus whatever csv-stats `describe`
  already offers.
- **Comparisons** use the engine seam (plan P9): `scistackstats.engine` is
  the only importer of `csvstats`. Tests inject a fake engine that returns
  S5-shaped records. The real engine maps csv-stats 0.1.x output into that
  shape where it can. Where it can't, it refuses the test with "not yet in
  csv-stats" (a structured refusal, never a fallback to scipy or pingouin).
- **Each gap found** becomes an entry in
  [csv-stats-v1-requirements.md](csv-stats-v1-requirements.md), and csv-stats
  is improved as the work goes.

## Open questions

All resolved (2026-09-27). O1, O3, O6 and O7 became S5, S7 and S6; O2 and O5
are in S8. The rest:

- **O4 → superseded 2026-10-17 by S10, S11 and S20:** stats specs are
  standalone, saved in their own `scistackstatsdb` table, with an optional
  link to a saved plot. (Before: attached to a saved plot.)
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
