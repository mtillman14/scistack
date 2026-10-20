# Plan: Satterthwaite degrees of freedom for csv-stats' mixed models (2026-10-19)

## Goal
`csvstats.lmm` and `lmm_pairwise` report Wald tests today (`df_method: "wald"`:
χ² per term, z per contrast). Wald tests treat the variance estimates as
exact, so with about 17 subjects their p-values are too small. A Wald
p = .05 is about .07 with the correct t(16). Replace Wald with Satterthwaite
t and F tests, matching R's lmerTest, the reference reviewers expect.

User decision: implement in pure Python on top of the existing statsmodels
fit (option 2). R is used ONCE, to produce reference values that become test
fixtures. R is never a runtime dependency.

## The method (what csv-stats will compute)
For a fitted REML model with variance parameters φ (subject variance,
residual variance) and fixed effects β:

- **One contrast** `c'β` (a coefficient, or a pairwise difference):
  `v(φ) = c' (X' V(φ)⁻¹ X)⁻¹ c`, the gradient `g = ∂v/∂φ` (numerical), and
  `A` = the asymptotic covariance of φ = 2 · inverse Hessian of the REML
  deviance (numerical). `df = 2 v² / (g' A g)`; `t = c'β̂ / √v`, compared
  with t(df).
- **A term with q > 1 columns** (an omnibus F; Fai & Cornelius 1996, as in
  lmerTest): eigendecompose `L C Lᵀ`, take a single-df Satterthwaite ν_m for
  each eigenvector, `E = Σ ν_m / (ν_m − 2)` over ν_m > 2,
  `ddf = 2E / (E − q)`; `F = (Lβ̂)ᵀ (L C Lᵀ)⁻¹ (Lβ̂) / q`, compared with
  F(q, ddf).
- The df are invariant to how φ is parameterised. The deviance, its Hessian
  and the gradients are evaluated at the statsmodels REML estimates. Step 2
  checks those estimates against lme4 first: Satterthwaite can only match if
  the fit matches.
- Numerical derivatives: central differences with step sizes scaled to each
  parameter (relative 1e-4). Richardson extrapolation if the fixtures demand it.
- **Failure handling** (a singular Hessian, a variance on the boundary, a
  non-finite df): fall back to Wald for that effect. Record it as
  `df_method: "wald"` plus a `satterthwaite_unavailable` warning that says
  why. Never a crash, and never a silent switch.

## Open decision for the user (blocks stage 5)
**How `lmm_pairwise` gets its contrasts.**
- Today it refits a model on just the two levels' rows (and that context).
- The standard approach (R emmeans) estimates every pairwise contrast from
  the FULL model. The residual and subject variances then come from all the
  data, and the df reflect that.

Recommendation: full-model contrasts. The reference script produces both, so
either choice is validated.

## Reference datasets (the R script)
`csv-stats/tests/reference/make_lmm_reference.R`. The R script GENERATES
every dataset itself (fixed `set.seed` per dataset) and writes, per dataset:
- `datasets/<id>.csv`: the long data (one row per observation).
- `results/<id>.json`: lme4/lmerTest results (below), plus the R and package
  versions.
Python reads both, so there is never a cross-language random-number mismatch.
The CSV and JSON files are committed. Regenerating them is a deliberate act,
noted in the commit.

"Every possible scenario" can't literally be enumerated, so the datasets
cover each dimension that changes the df computation, crossed where the
dimensions interact. Each dataset id names its cell, e.g.
`w3_monotone15_n17_icc05_eff`.

### Dimensions
1. **Design shape**
   - W2: one within factor, 2 levels (pre/post)
   - W3: one within factor, 3 levels
   - W7: one within factor, 7 levels (like Aim 2's sessions)
   - W2xW3: two within factors, crossed (session x speed)
   - B2W3: one between factor (2 groups) + one within (3 levels): mixed design
   - B3W2: one between factor (3 groups) + one within (2 levels)
   - B2B2W2: two between factors + one within
   - B2 with repeats: a between-only design where each subject has several
     observations (a random intercept is still identifiable)
2. **Missing data / balance**
   - complete: every subject at every cell
   - MCAR cells: 5%, 15%, 30% of subject-cells removed at random
   - monotone dropout: later levels missing (longitudinal, the Aim 2
     shape), at 15% and 40%
   - a whole level missing for some subjects, in one between group only
   - subjects with a single observation (they inform the fixed effects but
     not the subject variance)
   - unequal between-group sizes (for example 5 vs 15)
   - unequal replicates per subject-cell (1 to 4 observations; csv-stats
     is general even though StatStudio collapses to one per cell)
   - one cell left with very few observations (n = 2)
3. **Sample size**: 6, 12, 17, 40 subjects. 6 is where Wald vs
   Satterthwaite differ most; 40 is where they nearly agree.
4. **Variance structure**: ICC ≈ 0.8, 0.5 and 0.1, and exactly 0 (subject
   means identical: a singular fit on the boundary). The 0 case checks the
   fallback.
5. **Effects**: null (no fixed effect) and non-null, so p-values span the
   range rather than all being near 0.
6. **Scale and labels**: a dataset with values around 1e-3 and one around
   1e4 (numerical stability of the derivative steps). Factor levels with
   spaces and symbols, and level names that sort differently than their given
   order (checks that level order is passed through).
7. **Random slopes** (future scope, so the fixtures exist when it comes):
   W3 with `(1 + level | subject)`, complete and with 15% MCAR. Marked
   `"future": true`; skipped until csv-stats supports slopes.

### Crossing (about 70 datasets)
- Every design shape × {complete, MCAR 15%, monotone 15%} × n = 17 ×
  ICC 0.5 × non-null.
- W2, W3, B2W3 × every missingness pattern × n = 17.
- W3 and B2W3 × every sample size × {complete, monotone 15%}.
- W3 × every ICC level × {complete, MCAR 15%}.
- W2xW3 and B2W3 × {null, non-null} × {complete, monotone 40%}.
- The scale/label datasets and the random-slope datasets.

### What each results JSON records
All with `options(contrasts = c("contr.sum", "contr.poly"))`, so the coding
matches csv-stats' sum-to-zero:
- `lmer(..., REML = TRUE)`: fixed-effect estimates, SEs, Satterthwaite df,
  t and p (`summary()`); the variance components; the REML log-likelihood;
  convergence and singular-fit flags (`isSingular`).
- `anova(fit, type = 3, ddf = "Satterthwaite")`: per term, F, NumDF, DenDF, p.
- `anova(fit, type = 3, ddf = "Kenward-Roger")`, for a later Kenward–Roger
  stage.
- `emmeans(fit, pairwise ~ factor | other, lmer.df = "satterthwaite")`: every
  pairwise contrast from the FULL model (estimate, SE, df, t, p, unadjusted).
  Also the same contrasts from a model refitted on each pair's rows, to
  validate either answer to the open decision.
- Wald z p-values for the same contrasts, so the size of the Wald error is
  documented per scenario.
- `sessionInfo()` versions: R, lme4, lmerTest, emmeans, pbkrtest.

### Running it (once, by the user)
```
install.packages(c("lme4", "lmerTest", "emmeans", "pbkrtest", "jsonlite"))
Rscript csv-stats/tests/reference/make_lmm_reference.R
```
Expected run time: well under a minute. The script prints one line per
dataset and a summary of any fits that warned.

## Python tests
`csv-stats/tests/test_lmm_reference.py`, parametrized over every
results/*.json (the "future" ones skipped):
- **Fit parity first**: REML estimates of β (rel 1e-6), SEs (rel 1e-5), the
  variance components (rel 1e-4) and the log-likelihood (abs 1e-4) match
  lme4. If the fit doesn't match, the df comparison is meaningless, so it
  fails first with a clear message.
- **Satterthwaite**: coefficient df (rel 1e-3), t (rel 1e-5), p (abs 1e-4).
  Term F (rel 1e-5), DenDF (rel 1e-3), p (abs 1e-4).
- **Contrasts**: estimate, SE, df, p against emmeans, for the chosen
  approach.
- **Singular / boundary**: csv-stats flags `singular_fit`. When lmerTest
  still reports a df, csv-stats either matches it or falls back with
  `satterthwaite_unavailable`. A silent mismatch is never allowed.
- A summary table (scenario × max relative df error) printed under `-v`,
  so the accuracy is visible, not just pass/fail.

## Stages
1. **R script + fixtures.** I write the script; the user runs it and commits
   the outputs (or sends them to me).
2. **REML parity.** `test_lmm_reference.py` fit checks only. Fix any
   statsmodels/lme4 differences (optimizer, tolerances) before any df work.
3. **Satterthwaite for single-df contrasts and coefficients**, with tests.
4. **Fai–Cornelius F for multi-df terms**, with tests.
5. **`lmm_pairwise`** per the user's decision (full-model contrasts
   recommended).
6. **scistackstats**: effects carry `df_method: "satterthwaite"`.
   - `format` shows t(df) / F(df1, df2).
   - `missing_sentence` drops the "Wald" wording, unless any effect fell back,
     in which case the caption names it.
   - The FakeEngine is updated; tests adjusted.
7. **csv-stats 0.4.0**: README, requirements §3.5 status and the C5 codes
   (`satterthwaite_unavailable`), plus statistics.md traps. Release by the user.
8. **Later, optional**: Kenward–Roger from the same fixtures (they already
   contain the KR results).

Logging: each fit logs at DEBUG the df method per effect, and any fallback
at INFO with its reason. scistackstats logs fallbacks at WARN, because the
caption changes.

## Status (2026-10-19)
- User decision: FULL-MODEL contrasts for lmm_pairwise (emmeans-style). The
  R script therefore records full-model contrasts only. The per-pair refit
  reference was dropped as no longer needed.
- Stage 1: script written at `csv-stats/tests/reference/make_lmm_reference.R`
  (+ README). About 75 scenarios: crossings 1–7 as above, with B2rep using
  {complete, unequal_reps} and a LABELS design (odd labels and order) in
  place of separate label datasets. Not run yet: the user runs it.
  Guards in the generator keep every subject at >= 1 row and every design
  cell at >= 2 subjects, so each scenario fits the specified model.
- Stage 1 DONE (2026-10-19): the user ran the script. 77 scenarios: 72 ok,
  5 singular, 0 failed. Outputs are in csv-stats/tests/reference/{datasets,results}
  (77 + 77 files, ~1 MB), uncommitted.
  - Singular fits:
    - 4 intercept-only: B2W3 n6; W3 mcar15 ICC 0.1; W3 ICC 0 (complete and
      mcar15).
    - W3rep complete slopes: the intercept–slope correlation was estimated at
      +1.000, a correlation boundary (a different singular type from a zero
      variance). Kept as a fixture: csv-stats must flag it too.
  - Slope fixtures were fixed twice:
    - The generator now gives subjects a true linear slope (W3rep, slope_sd 0.4).
    - The random slope uses the numeric covariate `time_num`, recorded as
      `random_slope` in each JSON. `(1 + time | subject)` with a 3-level factor
      was singular by construction.
  - A slopes scenario shares its intercept-only twin's seed.
- Fixtures committed: csv-stats 8300482 (pushed).
- Stage 2 (in progress, pytest unrun): `csv-stats/tests/test_lmm_reference.py`
  covers fixed effects + SEs, variance components, the singular flag in BOTH
  directions, the REML log-likelihood, and a parity table (-s).
  `lmm` coefficient details now carry `name` (lme4 contr.sum style,
  "group1:time2") and `label` ("group[ctl] : time[t2]").
  Fixture quirk: jsonlite auto_unbox wrote one-element factor lists as
  strings and empty ones as null. The Python reader normalises this; the R
  script now wraps those fields in I().
- Stage 2, first run (2026-10-19): estimates matched to 1e-6 or better,
  variances to 1e-5, and the REML log-likelihood to 1e-8 (same model and
  constants). Two findings:
  1. statsmodels SEs were off by 0.1-4% on UNBALANCED data only; balanced
     data matched to 1e-6. Its `cov_params()` comes from the joint (beta,
     variance) information. lme4 uses sigma^2 (X'V^-1X)^-1, and Satterthwaite
     needs exactly that.
  2. statsmodels stopped short of the boundary: a positive subject variance
     with a worse likelihood (-59.65 vs lme4's -58.94), so it missed singular
     fits.
  Fix: csv-stats' own REML for random-intercept models (`lmm._RandomIntercept`).
  It is a 1-D profiled criterion in theta = sigma_s/sigma_e >= 0 (lme4's
  parameterisation): a grid, then bounded Brent, plus an explicit theta = 0.
  Everything is closed-form per-subject sums. cov = sigma^2 A^-1. Singular =
  theta < 1e-4 (lme4's isSingular). statsmodels is no longer used by lmm;
  patsy builds the design and is declared in [full]. This is also the
  machinery stage 3 needs: A(theta) and the deviance as functions of the
  variance parameters.
- Stage 2 DONE (second run, 2026-10-19): all 300 parity checks pass (the 8
  skipped tests are the 2 slope scenarios). Agreement is typically 1e-8 to
  1e-15. Every lme4 singular fit is flagged, with no false flags. The worst
  case is W3rep_mcar15 (estimates 3e-7, SE 4e-6, loglik 1e-8), which looks
  like lme4's own optimizer tolerance: our criterion is minimised to 1e-12.
  Uncommitted.
- Stage 2 committed: csv-stats ed9c14f (pushed).
- Stage 3 (in progress, pytest unrun):
  - `_RandomIntercept.deviance_at((theta, sigma))` is the unprofiled REML
    deviance; `vcov_at` is sigma^2 A(theta)^-1.
  - `_hessian` / `_gradient`: central differences with one Richardson step;
    steps 1e-3 * max(|x|, 0.1).
  - `_varpar_cov` = 2 H^-1 (cached per fit; None if H is not positive
    definite). `satterthwaite_df(fit, l)` = 2 v^2 / (g' A g).
  - Coefficients carry df, t, p (Satterthwaite) plus z and p_wald. On
    fallback: df_method "wald" and a `satterthwaite_unavailable` warning.
  - All 331 reference coefficients have an R df, including singular fits, so
    fallback counts as a failure there.
  - Terms (omnibus) are still Wald until stage 4.
- Stage 3 first run: coefficient df matched lmerTest to about 1e-8 (singular
  fits too). One failure: W3rep_mcar15, intercept t off by 2.4e-5, the same
  dataset where lme4's own fit was less precise in stage 2. Fixes:
  - The tests relax tolerances (x10) only for datasets where csv-stats' REML
    loglik is HIGHER than lme4's (`lme4_stopped_short`), guarded by
    `test_lme4_stopping_short_is_rare` (<= 3 datasets).
  - The step for sigma is now purely relative. A shared 0.1 floor made it 14%
    of sigma for the 1e-3-scale data (df off by 1.5e-4). theta keeps its floor.
- Stage 3 DONE (second run, 2026-10-19): all 388 tests pass. Coefficient df
  match lmerTest to about 1e-8. The small-scale datasets' df went from 1.5e-4
  to 3e-8 to 7e-8 after the sigma-step fix. W3rep_mcar15 passes under the
  relaxation, which confirms csv-stats' likelihood is higher there (lme4
  stopped short). The largest remaining df error is
  B2B2W2_monotone15 at 5.7e-5 (its variances already differ by 1.6e-6).
  Uncommitted.
- Stage 3 committed: csv-stats 95853f3 (pushed).
- Stage 4 (in progress, pytest unrun):
  - `lmm._term_test` = lmerTest's contestMD: eigendecompose L C L', give
    each eigen-contrast its df via `satterthwaite_df`, then
    `fai_cornelius_ddf` (exactly get_Fstat_ddf: q = 1 gives nu; all equal
    gives the mean; any <= 2 gives 2; otherwise 2E/(E-q)).
  - Effects are F with df [q, ddf], df_method "satterthwaite" and
    `eigen_df`; the Wald chi2 is kept under "wald". On fallback: Wald with a
    `satterthwaite_unavailable` warning.
  - Record label "Linear mixed model (Satterthwaite)". test_lmm updated.
  - Tests: `test_term_f_tests_match_lmertest_anova` (F, NumDF, DenDF, p);
    `test_wald_chi2_alongside_matches_r`; a ddf column in the table.
- Stage 4 first run: F, NumDF and the Wald chi2 all matched. DenDF matched
  (~1e-8) for 1-df terms and on balanced data, but multi-df terms on
  UNBALANCED data were off by up to 1%. Cause: Fai–Cornelius ddf depend on
  the basis of L (F does not). We selected sum-coded coefficients; lmerTest
  builds L its own way. Fix: a faithful port of lmerTest's contrast
  construction (source fetched from github runehaubo/lmerTestR master):
  `lmm.type3_contrasts`, which covers get_contrasts_type3 /
  extract_contrasts_type3 / get_rdX / containment, and with ONE term
  get_contrasts_type1 via `_doolittle_L`. Also `_zapsmall` (R digits = 7) and
  `_lm_resid`. Unit tests for zapsmall/doolittle are in test_lmm.py.
- Stage 4 DONE (second run, 2026-10-19): all 542 tests pass. Term DenDF match
  lmerTest to about 1e-8 everywhere, including unbalanced multi-df terms (were
  up to 1e-2) and single-term Doolittle models. Uncommitted.
- Stage 4 committed: csv-stats 81acb5c (pushed).
- Stage 5 (+ the scistackstats parts of stage 6), pytest unrun:
  - csv-stats `lmm_pairwise(factor=)`: ONE full fit, then every pair of
    `factor` within each combination of the other factors.
    l = x(a, ctx) - x(b, ctx) via patsy.build_design_matrices on the fit's
    design_info. Each contrast gets t(df) from `satterthwaite_df`, keeps its
    Wald z/p under "wald", and carries a "context". Tests:
    test_pairwise_contrasts_match_emmeans and
    test_pairwise_wald_alongside_matches_emmeans_asymptotic (464 contrasts).
  - scistackstats `run._pairwise_full_model`: one engine call per stratum with
    options.factor, results laid out as Comparison. FakeEngine mirrors the
    shape. `Comparison.df_method` and omnibus effect `df_method` are recorded.
  - `format.missing_sentence` now says "Satterthwaite degrees of freedom" and
    "some tests fell back to Wald" only when one did.
    `effects.difference_spec` has an lmm_pairwise label. Catalog labels are
    "(Satterthwaite t/F)".
  - csv-stats 0.4.0; scistackstats pins >=0.4.0. The engine refuses mixed
    models on csv-stats < 0.4 as a "gap" (`_version_tuple`), with a test.
  - Docs: statistics.md trap, csv-stats README, requirements status, GUI
    checklist 0zzzzj.
- Stage 5 run (2026-10-19): csv-stats reference suite 696 pass, including all
  464 full-model contrasts against emmeans (Satterthwaite and Wald). One
  scistackstats failure: `_version_tuple("0.3.0rc1")` gave (0, 3, 1), because
  it joined all digits of "0rc1". Fixed to use leading digits only.
- Stage 5 DONE + committed (2026-10-19): csv-stats 6a4e36b (0.4.0), scistack
  1d4e9a47. All tests pass. Remaining:
  - stage 6 check by eye (GUI 0zzzzj) with real csv-stats 0.4;
  - stage 7: the user releases csv-stats 0.4.0 to PyPI;
  - stage 8 (optional): Kenward-Roger (the fixtures already hold KR).
