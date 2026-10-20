# Testing csv-stats' mixed models against R (lme4 / lmerTest)

How csv-stats shows its linear mixed models are correct, and why it does it
this way. Read this before changing `csvstats.lmm` or its reference fixtures.

## Ownership
- **csv-stats** (`/workspace/csv-stats`, MIT) owns the LMM math:
  `csvstats/lmm.py` (tests, Satterthwaite) and `csvstats/lmm_fit.py`
  (`MixedModel`: the general random-effects fit, any lme4-style structure;
  .claude/plan-lmm-general-fit.md). Its own REML, not statsmodels.
- **R is an oracle only.** It runs once, offline, and writes fixtures. It is
  never a runtime or test-time dependency: the Python tests read the
  committed CSV and JSON files.
- **The fixtures are ground truth.** Regenerating them is a deliberate act,
  and the commit says why.

## Why not port lme4's own test suite
A port tested only against lme4's tests would be weakly validated:
- Most of lme4's `tests/` covers R behaviour that has no Python counterpart:
  formula parsing, S4 methods, `update()`, printing, exact warning text.
- Many expected numbers were pasted in from earlier lme4 runs. They are
  regression checks, not independent truth.
- Tolerances are loose (often 1e-3 to 1e-4) because results vary across
  platforms.
- A handful of classic datasets (sleepstudy, Penicillin, …) leave the hard
  cases thin: singular fits, correlation boundaries, badly unbalanced
  crossed designs.

Their few numeric checks are worth porting as extra fixtures, but they are
not the main evidence.

## Licence constraint
lme4 and lmerTest are GPL (≥ 2); csv-stats is MIT. Translating their source
would make csv-stats a derivative work. So implement from the published
method, and use R only to produce outputs to compare against:
- Bates, Mächler, Bolker & Walker 2015, JSS 67(1): PLS and the profiled
  deviance.
- Kuznetsova, Brockhoff & Christensen 2017, JSS 82(13): lmerTest.
- Fai & Cornelius 1996: multi-df F ddf.

This applies to every future feature too: Kenward–Roger, general random
structures, GLMMs.

## The two fixture sets
| set | script | scenarios | role |
|---|---|---|---|
| curated | `tests/reference/make_lmm_reference.R` → `tests/reference/{datasets,results}` | 77 named (`B2W3_mcar15_n17_icc50_effect_unit_intercept`) | readable regression grid over the Satterthwaite dimensions; tests `test_lmm_reference.py` |
| sampled | `tests/reference/make_lmm_scenarios.R` → `tests/reference/scenarios/` | 225 stratified draws over 9 random-effect structures (RIS_FACTOR, a 3-level factor × x, added for covariate type III) | development bed for general random effects; tests `test_lmm_scenarios.py` (built 2026-10-19; fixtures need one R run) |

Both scripts generate their own data with fixed seeds and write the CSV
alongside the JSON. Python never has to reproduce R's random numbers.

## What "agrees with R" means (layered)
1. **Objective parity** (sampled set): lme4's deviance function is evaluated
   at fixed probe θ and compared with Python's criterion at the same θ. No
   optimizer is involved, so the tolerance is tight. A wrong objective
   cannot hide inside an optimizer tolerance.
2. **Optimum parity**: Python's criterion must be at least as good as lme4's.
   If Python's is better, lme4 stopped short (`lme4_stopped_short` in
   test_lmm_reference.py). Tolerances relax for that dataset only, and
   `test_lme4_stopping_short_is_rare` keeps it an exception.
3. **Parameter parity**: β, SE (σ²(X'V⁻¹X)⁻¹, as lme4, not the joint-information
   SE statsmodels uses), variance components and θ. Tolerances are scaled by
   the data's SD. Singular fits are loosened (`SINGULAR_LOOSEN`). In the
   sampled set, tolerances also scale with the spread between R's own
   optimizers (`allFit`).
4. **Inference parity**: Satterthwaite df, t, F (Fai–Cornelius) and p,
   against lmerTest's `summary` and `anova(type = 3)` and emmeans contrasts.
5. **Invariants with no R** (`test_lmm_invariants.py`): row
   permutation, subject relabelling, affine y, level reordering. Plus: a
   balanced RI design must reproduce csv-stats' own RM ANOVA F and df.

## Traps already hit
- **One observation per cell + a factor random slope is unidentifiable.**
  `(1 + time | subject)` with a 3-level factor and reps = 1 is singular by
  construction. Use replicates and a numeric covariate (`time_num`).
- **A slope scenario needs a true slope in the generator** (`slope_sd`).
  Otherwise the slope variance is estimated at 0.
- **jsonlite `auto_unbox`** writes a one-element vector as a bare string and
  an empty list as null. Wrap vectors in `I()`; the Python reader tolerates
  both (`_as_list`).
- **The boundary must be a candidate in its own right.** statsmodels stopped
  at a positive subject variance with a worse likelihood where lme4 found
  exactly 0. `MixedModel` compares θ = 0 explicitly (one θ), and tries the
  boundary face θ_j = 0 of every diagonal θ (several θ).
- **Sum coding everywhere.** R sets `options(contrasts = c("contr.sum",
  "contr.poly"))`, so coefficient names and type-III tests match patsy's
  `Sum` coding.
- **A singular fit can also be a correlation boundary (ρ = ±1),** not only a
  zero variance (W3rep complete slopes). The singular flag must cover both
  once slopes are fitted.
- **An uncentred covariate wrecks raw-θ optimization.** With x on [40, 80],
  the slope θ is ~0.01 against an intercept θ of ~1, and the two are
  strongly confounded. L-BFGS-B on raw θ stalled up to 2 criterion units
  short of lme4 (RIS scenarios). `MixedModel` therefore optimizes θ for
  the block's columns centred and scaled (`Block.whitening`), and maps back
  exactly through ΛΛᵀ (`MixedModel._recast`, QR, so singular Σ maps too).
- **lme4 stops short on the same data.** On 9 RIS scenarios csv-stats'
  criterion was lower than lme4's, by up to 0.58 (RIS_006). R confirmed it
  (`tests/reference/check_lmm_optimum.R`, fed by `write_ours_check.py`):
  lme4's own devFun at csv-stats' θ equals csv-stats' criterion, and lme4
  restarted there stays there. All 4 lme4 optimizers had agreed on the
  worse point, so the optimizer spread cannot reveal this. The generator
  now also starts lme4 from the whitened fit's θ (`whitened_start`) and
  records the better fit (`start`: "default" | "whitened"). Use the check
  script whenever csv-stats beats lme4 by more than LOGLIK_ATOL.
- **Every key is listed once.** A covariate that is also a random slope sat
  twice in `_Prepared(extra=)`, so `frame["x"]` was a two-column DataFrame
  ("covariate 'x' must be numeric").
- **R's own optimizers can disagree on isSingular at one optimum** (RIF_020:
  θ = 3.5e-5 vs 1.7e-4, with criteria 2e-7 apart). The singular-flag test
  skips such scenarios (`_r_singularity_ambiguous`).
- **t and F cannot be pinned tighter than the est and SE they come from.**
  On several-θ fits, R's optimizers at the same optimum still disagree by
  ~1e-4 in θ. Inference tolerances widen to 10 × the t spread across the
  optimizers AT the optimum (`_at_optimum`). One that stopped at a worse
  criterion would widen them to nothing.

## One owner per concern
- **`tests/reference/lmm_reference_common.R`** turns a fit into JSON for
  both R scripts. If it changes, the curated results must still regenerate
  byte-identical.
- **`tests/_lmm_oracle.py`** holds the tolerances, JSON quirks and comparison
  helpers for both Python suites. A suite never defines its own tolerance
  for a quantity the other suite also checks.
- **`route(ref)` in `test_lmm_scenarios.py`** decides what csv-stats can
  check for a scenario today:
  - objective (`MixedModel`): every scenario R fitted
  - the public `lmm()`: every scenario except the collinear ones (`lmm()`
    refuses a rank-deficient design)
  - term F tests: every design with a fixed term. Type III with
    covariates is in `lmm.type3_hypotheses` (.claude/plan-lmm-covariate-type3.md).
  - emmeans contrasts: every design with a factor. A covariate sits at
    emmeans' reference-grid mean; the fixture's `covariates_at` records
    emmeans' value and `test_covariate_grid_value_matches_emmeans` checks
    ours (.claude/plan-lmm-covariate-contrasts.md).
  - otherwise skipped, with the reason
- **Per-scenario tolerance:** 10 × the spread between R's own optimizers
  (`optimizers.spread`), never below the curated set's base tolerances.
  Inference (df, t, F, p) uses the t spread over the optimizers at the
  optimum instead (`_inference_loosen`).
- **Objective tolerance:** 1e-8 relative at the probe θ. A failure there is
  a bug in the criterion, never an optimizer difference.

## Where to look
- Method and stages of the Satterthwaite work: `.claude/plan-lmm-satterthwaite.md`.
- Scenario generator plan: `.claude/plan-lmm-scenario-generator.md`.
- How scistack consumes csv-stats' LMM: `docs/claude/statistics.md`.
