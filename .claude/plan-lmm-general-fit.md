# Plan: a general random-effects fit for csv-stats' mixed models (2026-10-19)

## Goal
Replace `_RandomIntercept`, which fits one random intercept per subject (one
θ), with a fit for ANY set of lme4-style random-effect terms. This covers
random slopes (correlated or `||`), several terms per group, crossed and
nested groupings, and interaction groupings (`subject:time`). It is built
against the sampled R-oracle set (`plan-lmm-scenario-generator.md`). The
test side changes only in `route()`, plus a general variance-component
comparison.

Implemented from the published method, NOT from lme4's source: lme4 is GPL
and csv-stats is MIT (docs/claude/lmm-r-oracle-testing.md). The source is
Bates, Mächler, Bolker & Walker (2015), JSS 67(1), §3: the profiled
deviance via penalized least squares.

## The model (one owner: `csvstats/lmm_fit.py`)
y = Xβ + Zb + ε, with b = Λθ u, u ~ N(0, σ² I) and ε ~ N(0, σ² I).

- **Terms → blocks.** Each `(terms | group)` is one block.
  - `||` splits a block into one block per column, as lme4 expands it.
    Numeric columns only: lme4's `||` with a factor is a known quirk, so it
    is refused with a clear error.
  - Block columns: `(Intercept)`, a numeric column, or a model factor coded
    contr.sum. Columns are named as R names them (`cond1`).
  - Group levels are the observed combinations of the group columns.
- **Block order** follows lme4's documented convention: blocks are reordered
  by decreasing number of levels, but only when some later block has more
  levels than an earlier one. θ is compared with R through `theta_names`
  where the names are unique, so the order is cross-checked as well.
- **θ per block**: the lower triangle of a p×p template T, column-major.
  lower = 0 on the diagonal, −∞ off it. Names follow lme4: `g.col` on the
  diagonal, `g.row.col` off it. Λ = blockdiag(I_levels ⊗ T).
- **Criterion** (dense: q ≤ a few hundred here). Precompute Z'Z, Z'X, Z'y,
  X'X, X'y and y'y once, with y centred when X has an intercept.
  - Λ'Z'ZΛ is computed per block by reshape-multiply: O(q² Σp), not O(q³).
  - L Lᵀ = Λ'Z'ZΛ + I.
  - c_u = L⁻¹ Λ'Z'y and R_ZX = L⁻¹ Λ'Z'X.
  - A = X'X − R_ZXᵀ R_ZX, with β from A β = X'y − R_ZXᵀ c_u.
  - r² = y'y − ‖c_u‖² − (X'y − R_ZXᵀc_u)ᵀβ.
  - ML: log|L|² + n(1 + log(2πr²/n)).
  - REML: log|L|² + log|A| + (n−p)(1 + log(2πr²/(n−p))).
  - For one random intercept this is exactly the old closed form.
- **Unprofiled `deviance_at(θ, σ)` and `vcov_at(θ, σ)`** are what the
  Satterthwaite code uses; σ² A⁻¹ is Cov(β̂).
- **Optimizer.**
  - One θ: the old grid + bounded Brent + explicit θ = 0 candidate. This keeps
    the curated parity exactly.
  - Several θ: L-BFGS-B with central-difference gradients, then a bounded
    Nelder–Mead polish. Then boundary faces: every diagonal θ that ends
    below 0.05 is fixed at 0 and the rest re-optimized (revised: every
    diagonal face, in whitened θ; see the 2026-10-20 run below). All-zero θ is also
    a candidate. The best criterion wins.
  - The start is lme4's default: 1 on the diagonal, 0 elsewhere.
- **Singular**: any diagonal θ < 1e-4 (lme4's documented isSingular rule).
  This covers a zero variance AND a ±1 correlation.
- **Variance components** in R's `as.data.frame(VarCorr)` shape: one row per
  variance, one per covariance (`sdcor` = the correlation), then Residual.

## Changes in `lmm.py`
- `_RandomIntercept` is removed (beta: a clean break). The fit is
  `MixedModel`; `_Fit` and `SINGULAR_TOL` move to `lmm_fit.py`.
- `lmm()` and `lmm_pairwise()` get `random=` (a list of `{group, terms,
  correlated}`, the JSON fixtures' shape; default `(1 | subject)`).
  `lmm()` also gets `covariates=` (numeric fixed effects, crossed full
  factorial with the factors, as `group * x`).
  - `lmm()` now allows zero factors (intercept only, or covariates only);
    there are then no term effects.
  - One shared helper prepares, validates and fits for both functions.
- `_Prepared` (core.py) gets `extra=`: covariate, group and slope columns
  are carried along, and a row missing one is dropped like any other row
  with a missing value.
- **Satterthwaite**: varpar = (θ…, σ). Steps are per-θ, as before.
  Cov(varpar) = 2 × the pseudo-inverse of the Hessian over its positive
  eigenvalues: lmerTest's documented handling of a not-quite-PD Hessian
  at a boundary. It is None only when no eigenvalue is positive. Every
  non-positive eigenvalue is logged at INFO.
- **Term F tests with covariates** select the term's own coefficients. The F
  is a true Wald F, but it has not been checked against lmerTest's type-III
  construction for covariates, so `details["term_tests"]` says which
  construction was used. Designs with only factors keep the ported
  lmerTest construction.
- **details**:
  - `formula` is the lme4 spelling.
  - `variance_components` is new: the general list.
  - `theta` is a list.
  - The RI-only keys (`subject_intercept_variance`, `icc`) stay when the
    structure is one random intercept. scistackstats reads none of them.

## Tests
- `test_lmm_scenarios.py`:
  - `route()` gives every R-fitted scenario the objective layer.
  - Factors-only and covariate designs, and intercept-only designs, get
    `lmm()`. Collinear scenarios stay objective-only, because `lmm()` does
    not drop columns.
  - Term F tests and contrasts run for factors-only designs only.
  - Variance components are compared row by row, matched on
    (group, var1, var2). lme4's `.1` suffix on a repeated group name is
    stripped, and floors come from the optimizer spread.
  - θ is compared through the names.
- `test_lmm_fit.py` (new, no R):
  - One random intercept: the general criterion equals the old closed form,
    re-derived inline in the test.
  - Block layout, θ names and lower bounds for every structure.
  - Two equivalent formulations agree: one (0 + x | g) block versus
    `(1 + x || g)` without its intercept part.
  - Errors: `||` on a factor; a factor slope that is not a model factor.
- The curated suite must still pass unchanged: one θ takes the same path as
  before.

## Logging
- DEBUG per fit: the structure, n θ, the optimizer path, the criterion, the
  boundary candidates tried and the winner.
- INFO when the Hessian has non-positive eigenvalues, and when a boundary
  face beats the interior optimum.

## Stages
1. `lmm_fit.py`: blocks, criterion, optimizer, variance components.
2. Wire it into `lmm.py`: shared prepare/fit, covariates, `random=`,
   Satterthwaite over a θ vector, details.
3. `_Prepared(extra=)`.
4. Tests: the `route()` widening, general variance and θ comparison,
   `test_lmm_fit.py`.
5. Docs: lmm.py docstring, csv-stats README LMM section, the docs/claude
   note, memory. A version bump only at release time (the user's call).

## Status (2026-10-19)
- Drafted, and implementation started the same day (user: "draft the plan,
  then implement it; I'll test it after").
- All 5 stages written 2026-10-19; pytest not run (the user runs it).
  - `src/csvstats/lmm_fit.py` (new): RandomTerm/parse_random/random_formula,
    build_blocks (|| expansion, contr.sum factor slopes), lme4_order,
    MixedModel (criterion, deviance_at, vcov_at, optimizers,
    variance_components).
  - `lmm.py`: `_RandomIntercept` removed. The shared `_setup()` serves
    lmm/lmm_pairwise. New: covariates=, random=, a rank-deficiency
    refusal, Satterthwaite over a theta vector with the
    positive-eigenvalue pseudo-inverse, details
    (variance_components / theta list / theta_names / random / optimizer /
    term_tests).
  - `core._Prepared(extra=)`.
  - Tests: test_lmm_scenarios.py route widened; test_lmm_fit.py (new:
    dense-likelihood identity, RI closed form, || equivalence, layout,
    ordering, refusals, recovery); _lmm_oracle.assert_variance_rows.
  - Docs: lmm.py docstring, csv-stats README, the docs/claude note,
    statistics.md.
- Behaviour changes to watch in the user's run:
  - `details["theta"]` is now a list (it was a float). Nothing in scistack
    reads it.
  - A not-PD varpar Hessian now gives a pseudo-inverse df instead of a
    Wald fallback.
  - A rank-deficient fixed design raises ValueError. Before, the fit
    produced numbers from a singular matrix.
- Uncertain from memory, verified only by the tests:
  - lme4's block-order rule (test_theta_names_and_order_match_lme4)
  - lme4's || theta naming (to_ours falls back to position)
  - lmerTest's positive-eigenvalue handling (the coefficient-df tests on
    singular fits)

## First test run (2026-10-20) and fixes
- **"covariate 'x' must be numeric"** (every RIS case, and 2 in
  test_lmm_fit). x was both a covariate and a slope, so it was listed twice
  in `extra`, and `data[needed]` made `frame["x"]` a DataFrame. Fixed: keys
  deduplicated in `_setup` and in `_Prepared`.
- **The optimizer stopped short** (RIS_001 2.1 units, RIS_025, RIS_UNCORR_006;
  θ off in RIS_006/019). x is on [40, 80], uncentred, so raw θ is
  ill-conditioned. Fixed:
  - optimize θ for centred and scaled block columns (`Block.whitening`),
    mapped exactly back to lme4's θ via ΛΛᵀ (`_recast`, QR)
  - two starts (whitened identity, lme4's raw start)
  - every diagonal face tried, not only θ < 0.05
  - new no-R tests: the mapping keeps Σ, singular stays singular, and a
    shift x+60 leaves the optimum unchanged
- **Singular flag** (CROSSED_SLOPES_013, RIF_020): R's own optimizers at the
  same optimum disagree on isSingular. The test now skips those
  (`_r_singularity_ambiguous`: 8 of 200 scenarios).
- **t / F / contrast-t** (CROSSED_SLOPES_013, NESTED_006, RIF_005/013/014/020,
  RM_024): est, SE and df passed, while t/F were held to 1e-5 relative.
  That is tighter than R's own optimizers agree. Fixed: inference loosens
  by 10 × the t spread over R's optimizers at the optimum
  (`_inference_loosen`; median ×19, max ×722).
  - RIF_014 does not loosen beyond its singular ×10; if it still fails,
    it is a real difference.

## Second test run (2026-10-20)
- The whitening fixed the stalls; 183 of 198 criteria now agree with lme4 to 1e-6.
- 9 RIS scenarios had csv-stats' criterion BELOW lme4's (RIS_006 by 0.58).
  `check_lmm_optimum.R` confirmed that lme4 stopped short: its own devFun
  at our θ equals our criterion, and it stays there when restarted from
  it. Fix, in the R generator: `whitened_start()`, with the reference
  being lme4's better fit, and the optimizer spread started from the same
  point. **Needs an R rerun of make_lmm_scenarios.R** (only RIS and
  RIS_UNCORR results can change).
- NESTED_006 F, RIF_013 F, RIF_014 contrast t: statistics near 0
  (|F| = 0.003), so a relative tolerance is meaningless. t and F now have
  an absolute floor equal to their relative tolerance.
- Assertion messages now show both values (`_vs`).

## Done (2026-10-20)
- Fixtures regenerated: 15 RIS/RIS_UNCORR results changed (all
  `start: "whitened"`); the other 184 only gained the new fields.
- Full scenario suite passes (user run). csv-stats committed f3f077a and pushed.
- Not done: a version bump or release (the user's call).
