# Plan: a seeded LMM scenario generator, with R as the oracle (2026-10-19)

## Goal
Build a test bed for the next step of csv-stats' mixed models: random slopes,
crossed and nested groupings, and a general Z/Λ(θ) fit in place of the
one-parameter `_RandomIntercept`. Python is checked against what R actually
computes, on enough scenarios that agreement means something. lme4's own test
suite is not the oracle: it mostly tests R behaviour, and its expected
numbers come from earlier lme4 runs (docs/claude/lmm-r-oracle-testing.md).

This plan builds the generator and the Python harness ONLY. The general fit
is a separate plan, written once this suite exists and shows what fails.

The existing curated set (`tests/reference/make_lmm_reference.R`, 77 named
scenarios) stays as it is: it is the readable regression set.

## What is new compared with the curated set
1. **A random-effects catalogue**, not just `(1 | subject)`.
2. **Sampled scenarios, not a hand-written grid.** A fixed master seed draws
   each scenario's dimensions, stratified so the hard cases are always
   present.
3. **Objective parity, separate from optimizer parity.** R records lme4's
   deviance function (`devFunOnly = TRUE`) evaluated at a set of probe θ.
   Python evaluates its own criterion at the SAME θ. That test involves no
   optimizer, so it can be tight (relative 1e-9). An objective bug cannot
   hide inside a loose optimizer tolerance.
4. **Tolerances measured, not guessed.** R refits every scenario with
   several optimizers (`allFit`: bobyqa, Nelder_Mead, nloptwrap). The spread
   between them is recorded. Python's tolerance on parameters is
   `max(base, 10 × spread)` for that scenario.
5. **A manifest with coverage quotas.** A regeneration can't silently lose
   the singular or boundary cases.

## Random-effects catalogue (structure ids)
| id | lme4 formula | what it stresses |
|---|---|---|
| RI | `(1 \| subject)` | baseline; csv-stats fits this today |
| RIS | `(1 + x \| subject)` | 2×2 covariance, correlation boundary |
| RIS_UNCORR | `(1 + x \|\| subject)` | diagonal Λ; numeric x only (with a factor `\|\|` does not uncorrelate) |
| RIF | `(1 + cond \| subject)`, cond a 2-level factor, reps ≥ 2 | factor slopes under sum coding |
| RM | `(1 \| subject) + (1 \| subject:time)` | equals the classical RM ANOVA when balanced (an R-free check) |
| CROSSED | `(1 \| subject) + (1 \| item)` | crossed grouping, non-block-diagonal Z'Z |
| NESTED | `(1 \| site/subject)` | nested grouping; recorded EXPANDED as `(1\|site) + (1\|site:subject)` |
| CROSSED_SLOPES | `(1 + cond \| subject) + (1 \| item)` | the "maximal" psycholinguistics model |

Each results JSON describes the structure in a language-neutral form. Python
reads this, never the R formula string:

```json
"random": [
  {"group": ["subject"], "terms": ["(Intercept)", "x"], "correlated": true},
  {"group": ["site", "subject"], "terms": ["(Intercept)"], "correlated": true}
]
```

`group` is a list: more than one name means their interaction.

## Sampled dimensions (per scenario)
- **Groups**: 6–60 subjects, log-uniform. Items (CROSSED*): 4–30. Sites
  (NESTED): 2–6.
- **Replicates** per subject-cell: 1–5, where the structure is identifiable.
  RIF, RM and CROSSED_SLOPES need ≥ 2.
- **Fixed design**: one of {intercept only, B2, W3 factor, continuous x,
  B2 × x, W2 × W3}. Sum coding, as now.
- **Missingness**: complete | MCAR p ∈ [0.05, 0.4] | monotone dropout |
  unequal group sizes (1:3) | singletons.
- **Variance components**, drawn on the log scale. A forced stratum gets one
  component at EXACTLY 0, using the existing within-subject centring trick.
- **Correlation** (correlated structures): uniform in (−0.9, 0.9), plus a
  forced stratum at |ρ| ≥ 0.97.
- **Scale**: y × {1e-3, 1, 1e4 with offset}, as in the curated set.
- **Covariate x**: centred, OR offset (e.g. age 40–80). The offset pushes
  the intercept–slope correlation towards ±1, the classic convergence
  stress.
- **Collinear column** (about 5%): an exact linear combination of other
  columns. lme4 drops it ("fixed-effect model matrix is rank deficient").
  The dropped columns are recorded, and Python must drop the same ones.

### Stratification
25 scenarios per structure, 8 structures, so 200 scenarios. Within each
structure, the sampler guarantees at least:
- 3 complete and balanced
- 4 with a zero-variance boundary
- 3 with a correlation boundary (correlated structures only)
- 3 with n ≤ 8
- 3 with MCAR ≥ 25% or monotone dropout
- 2 at a non-unit scale

Seeds: `master_seed` + structure id + index, so adding a structure never
changes another structure's data. Ids are `<STRUCT>_<nnn>` (e.g. `RIS_007`).
The full spec is in each JSON and in the manifest; the id does not encode it.

## What each results JSON records
Everything the curated JSON has (fixed effects with Satterthwaite df, the
type-III anova, varcorr, REML criterion, logLik, singular, convergence
messages, notes, versions). Plus:
- `random`: the structure, as above. `formula`: the R formula, for humans only.
- `theta` and `theta_lower`: `getME(fit, "theta")` and `getME(fit, "lower")`,
  lme4's Cholesky parameterisation.
- `fixed_columns` (R's column names) and `dropped_columns`
  (`attr(getME(fit, "X"), "col.dropped")`).
- `vcov_beta`: the full covariance matrix of β̂.
- `probes`: about 8 θ vectors with REML and ML deviance at each:
  - θ̂
  - θ̂ with each component × 0.9 and × 1.1
  - every diagonal element set to 0 (the boundary)
  - 3 random θ within the bounds
- `optimizers`: per optimizer, the criterion, θ, β and whether it converged;
  plus `spread` (max − min per quantity, over the converged ones).
- `contrasts_satterthwaite`: emmeans, only when the fixed design has factors.
- `anova_kenward_roger` when pbkrtest is available, as now.

## Files
- `tests/reference/lmm_reference_common.R` (new): ONE owner for the R code
  both scripts share: `script_dir`, `finite_or_na`, `pair_method`,
  `contrasts_for`, the fixed-effect extraction, `versions`, JSON writing.
  `make_lmm_reference.R` sources it in place of its own copies.
- `tests/reference/make_lmm_scenarios.R` (new): catalogue, sampler,
  generator, fit + record, manifest.
- `tests/reference/scenarios/{datasets,results}/` plus
  `scenarios/manifest.csv`: one row per scenario with every sampled
  dimension and R's verdict (ok / singular / warnings / failed).
- `tests/reference/README.md`: a section on the scenario set.
- `tests/test_lmm_scenarios.py` (new): the R-oracle harness.
- `tests/test_lmm_invariants.py` (new): checks that need no R.

## Python harness (`test_lmm_scenarios.py`)
- **One adapter owns the mapping** from a scenario spec to a csv-stats call:
  `_csvstats_call(spec) -> callable | None`, where None means "not
  expressible yet". Every test goes through it, and a skip names the
  reason. Today only RI with a factor-only fixed design maps to `lmm()`.
- **Coverage quotas**: from the manifest, each structure has at least its
  stratified minimums, and fewer than 5% of scenarios failed in R.
- **Objective parity** at every probe θ, tight. Today this runs for RI with
  ANY fixed design, through `_RandomIntercept.deviance` on a patsy X, with
  the column names asserted equal to `fixed_columns`. RI's θ is a scalar,
  the same parameterisation as lme4.
- **Optimum parity**: Python's criterion ≤ R's + 1e-6. If Python's is
  lower, lme4 stopped short. This reuses the existing `lme4_stopped_short`
  rule, moved into a shared test helper (one owner, not a copy).
- **Parameter parity** (β, SE, variance components, θ): per-scenario
  tolerance from `optimizers.spread`; loosened on singular fits, as now.
- **Satterthwaite parity**: coefficients, the anova and contrasts, with the
  existing tolerances.
- **Singular flag**: agrees with lme4 in both directions.
- **Parity table** (`-s`): the worst error per scenario, plus a header line
  "N of 200 testable today; skipped by reason: …". This shows how much of
  the suite the current fit covers.

## R-free invariants (`test_lmm_invariants.py`)
These run on a few generated datasets, through the public `lmm()`.
- Permuting the rows changes nothing.
- Renaming the subjects changes nothing.
- `y → a + b·y`:
  - β and SE scale with b (the intercept also shifts by a).
  - t, df, F and p are unchanged.
  - The REML logLik shifts by `−(n − p) log|b|`.
- Reordering a factor's levels leaves every term's F, df and p unchanged.
- A balanced, complete W3 design with θ̂ > 0: the term F and its df equal
  csv-stats' own repeated-measures `anova`. For the time term the df are
  `(k − 1, (n − 1)(k − 1))`. This cross-checks two independent csv-stats
  code paths.

## Stages
1. **Shared R helpers.** Extract `lmm_reference_common.R`;
   `make_lmm_reference.R` sources it. Check: the user reruns it and
   `git status tests/reference/results` shows NO changes (same R and
   package versions).
2. **Scenario script.** Write `make_lmm_scenarios.R`. The user runs it once
   (expected: a few minutes, since allFit is the slow part) and commits the
   outputs. I read the manifest: coverage, failures, notes.
3. **Harness.** Write `test_lmm_scenarios.py`. Expected today: RI passes in
   full, and every other structure skips with a reason. If an RI scenario
   fails, that is a real finding about the current fit. Log it and fix it
   before going on.
4. **Invariants.** Write `test_lmm_invariants.py`.
5. **Docs**: update the reference README, docs/claude/lmm-r-oracle-testing.md
   and the memory entry.
6. **Next plan (not this one)**: the general Z/Λ(θ) fit (sparse or dense
   Cholesky, a bounded derivative-free optimizer), developed against
   stages 3–4.

## Logging / diagnostics
- The R script prints one line per scenario (ok / noted / FAILED, plus
  singular, the optimizer spread and dropped columns), and a summary per
  structure at the end.
- The parity table is the main diagnostic: worst error per scenario per
  quantity, so drift shows before it fails.
- Each Python skip carries its reason, and the table counts them.

## Decisions (defaults chosen; say if you want otherwise)
- **A separate script and folder**, not an extension of the curated script.
  The curated set stays readable and stable.
- **200 scenarios.** Expected size about 3–5 MB of CSV and JSON. If repo
  size matters, the datasets can be gzip-compressed (pandas and R both read
  .csv.gz).
- **No GLMMs.** The catalogue stops at Gaussian LMMs.
- **Licence**: R is a black-box oracle only. No lme4 or lmerTest code is
  translated into csv-stats (MIT); see the docs note.

## Status (2026-10-19)
- Plan approved 2026-10-19; stages 1-5 written the same day (R not run here, Python tests not run: the user runs both).
- Stage 1: `tests/reference/lmm_reference_common.R`. `make_lmm_reference.R`
  sources it; its `fit_and_describe` keeps the JSON keys in the same order.
  CHECKED 2026-10-19 (user rerun): every number is identical. The 77 diffs are
  only `factors`/`between`/`within` turning into arrays: the committed
  fixtures predated the committed I() fix in the script, and were never regenerated.
  - One behaviour change, which only shows when something errors: a failed
    lmerTest anova is now an "unavailable: ..." string. Before, it failed
    the whole scenario. The curated set had 0 failures, so its output is
    unaffected.
- Stage 2: `tests/reference/make_lmm_scenarios.R` written; not run.
  - `generate()` continues `draw_spec`'s random stream.
  - Probes: the estimate, ±10% per component, the boundary, 3 random points.
  - Optimizer refits use lme4::lmer. "Converged" means no warning.
- Stage 3:
  - `tests/_lmm_oracle.py`: shared tolerances and comparisons.
    `test_lmm_reference.py` was refactored onto it, with the same test names
    and thresholds.
  - `tests/test_lmm_scenarios.py`, with `route()` as the single gate.
  - Not planned originally: a theta parity test, plus an ML probe check
    next to the REML one.
- Stage 4: `tests/test_lmm_invariants.py`.
  - Checks: row order, subject labels, affine y (4 cases), level order, and
    balanced W3 / B2W3 against csv-stats' own `anova`.
  - The level-order test compares F and NumDF only: the Fai–Cornelius DenDF
    depends on the contrast basis.
- Stage 5: reference README section, docs/claude/lmm-r-oracle-testing.md
  (ownership section), memory.
- All tests pass (user run, 2026-10-19). Committed csv-stats 38de92c.
- First pytest run (2026-10-19), two test-side bugs fixed:
  - invariants: the intercept's t is not shift-invariant; it is now checked
    as estimate / SE.
  - NESTED small_n drew 3-6 sites with >= 2 subjects each, so n could not
    reach <= 8; small_n now draws 3-4 sites. This needs a rerun of
    make_lmm_scenarios.R; only the NESTED small_n data change.
