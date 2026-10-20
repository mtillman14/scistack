# Testing csv-stats' mixed models against R (lme4 / lmerTest)

How csv-stats shows its linear mixed models are correct, and why it does it
this way. Read this before changing `csvstats.lmm` or its reference fixtures.

## Ownership
- **csv-stats** (`/workspace/csv-stats`, MIT) owns the LMM math:
  `csvstats/lmm.py`. The fit is its own REML (`_RandomIntercept`), not
  statsmodels. (statistics.md's package table still says statsmodels; that
  line is stale.)
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
| sampled | `tests/reference/make_lmm_scenarios.R` → `tests/reference/scenarios/` | 200 stratified draws over 8 random-effect structures | development bed for general random effects; tests `test_lmm_scenarios.py` (built 2026-10-19; fixtures need one R run) |

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
  exactly 0. `_RandomIntercept.fit` always compares θ = 0 explicitly.
- **Sum coding everywhere.** R sets `options(contrasts = c("contr.sum",
  "contr.poly"))`, so coefficient names and type-III tests match patsy's
  `Sum` coding.
- **A singular fit can also be a correlation boundary (ρ = ±1),** not only a
  zero variance (W3rep complete slopes). The singular flag must cover both
  once slopes are fitted.

## One owner per concern
- **`tests/reference/lmm_reference_common.R`** turns a fit into JSON for
  both R scripts. If it changes, the curated results must still regenerate
  byte-identical.
- **`tests/_lmm_oracle.py`** holds the tolerances, JSON quirks and comparison
  helpers for both Python suites. A suite never defines its own tolerance
  for a quantity the other suite also checks.
- **`route(ref)` in `test_lmm_scenarios.py`** decides what csv-stats can
  check for a scenario today:
  - objective only: `_RandomIntercept`, for one random intercept per
    subject with any fixed design
  - the full public `lmm()`: also factors only
  - otherwise skipped, with the reason
  When the general random-effects fit lands, widen `route` and nothing else.
- **Per-scenario tolerance:** 10 × the spread between R's own optimizers
  (`optimizers.spread`), never below the curated set's base tolerances.
- **Objective tolerance:** 1e-8 relative at the probe θ. A failure there is
  a bug in the criterion, never an optimizer difference.

## Where to look
- Method and stages of the Satterthwaite work: `.claude/plan-lmm-satterthwaite.md`.
- Scenario generator plan: `.claude/plan-lmm-scenario-generator.md`.
- How scistack consumes csv-stats' LMM: `docs/claude/statistics.md`.
