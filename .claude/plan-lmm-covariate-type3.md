# Plan: type-III term tests for LMMs with covariates (csv-stats, 2026-10-20)

## Goal
Make `lmm()`'s term F tests match lmerTest's `anova(type = 3)` when the
fixed part has numeric covariates, and test that against R. Today
covariate designs use a different construction (`term_tests =
"coefficient_selection"`), and the scenario suite skips their term tests
(70 scenarios).

## What is true today
- Factors only: `type3_contrasts()` builds lmerTest-style type-III bases
  (treatment-coded X, the complete design rdX, containment
  orthogonalization, mapped back to sum coding). The F statistic doesn't
  depend on the basis; the Fai–Cornelius ddf does (up to 1% on unbalanced
  data, 2026-10-19). That is why the construction matters.
- Covariates: each term's own coefficients are selected (`np.eye`).
- The fixtures already hold lmerTest's type-III anova for every covariate
  design, so no R rerun is needed for the existing set.
- Every covariate design in the set has only 1-df terms (2-level `group`,
  numeric `x`). A 1-row basis doesn't change the ddf, so the current
  selection may already match. The cases that matter (a 3+-level factor
  crossed with a covariate) are missing from the set.

## The construction (from the published definition)
Type-III hypotheses by containment: Goodnight 1980 ("Tests of hypotheses
in fixed effects linear models") and SAS's type-III estimable functions,
as lmerTest documents them (Kuznetsova et al. 2017 §4). For a term T:
- A term's **variables** are its factors and its covariates; `group:x` has
  {group, x}. T is **contained** in U if vars(T) ⊂ vars(U).
- Treatment-coded X and the complete design rdX are generalized: a term's
  column is the product of its factors' level indicators and its
  covariates' values. A covariate contributes one "level", so `x` is one
  column, and `group:x` is one column per group level in rdX (one per
  non-reference level in X_trt).
- L = (X_trt⁺ rdX)ᵀ. A term's columns are orthogonalized against the
  columns of every term that contains it, highest order first. Then L is
  mapped to the fit's sum coding, as now.
- **Consequence to document:** for a factor crossed with a covariate, the
  factor's main effect is tested at covariate = 0. That is lmerTest's (and
  SAS's) type III. On an uncentred x (the RIS data: 40–80) it is an
  extrapolation.

## Changes
1. `type3_contrasts()` takes covariates. It stays the one owner of type-III
   bases: the `coefficient_selection` branch is removed (beta, clean
   break), and `details["term_tests"]` becomes `"type3"` for every design.
   The DEBUG log gets each term's basis shape and its containing terms.
2. **Note to the user (Q2):** when a factor interacts with a covariate, a
   `type3_at_covariate_zero` note says the factor's main effect is tested
   at covariate = 0, and that centring the covariate tests it at the mean.
3. **Coverage, R generator:** a 9th structure `RIS_FACTOR`:
   `(1 + x | subject)` with fixed designs `W3_x` (time × x) and `B2W3_x`
   (group × time × x), 25 scenarios. It is a NEW structure, so the
   existing 200 scenarios' seeds and fixtures are untouched. Needs one R
   run (adds 25 datasets/results and manifest rows).
4. **Tests:**
   - `route()`: covariate designs get the term F tests. Contrasts stay
     skipped there, with the reason "covariate design: no emmeans
     contrasts generated" (out of scope, below).
   - R-free (`test_lmm_fit.py` or `test_lmm.py`):
     - With a 2-level factor, the type-III basis is proportional to
       selecting the coefficient.
     - The highest-order term's F and ddf don't change when x is shifted;
       a contained factor's F does (pins the at-x = 0 semantics).
     - Containment with covariates: `x` and `group` are both inside
       `group:x`.
   - The scenario quotas and structure lists cover 9 structures.
5. **Docs:**
   - lmm.py docstring and the csv-stats README: type III with covariates,
     and the x = 0 note.
   - `docs/claude/lmm-r-oracle-testing.md`: the route change and the new
     structure.
   - `docs/claude/statistics.md`, if the StatStudio UI mentions term-test
     methods.

## Stages
1. Measure first: enable term tests for covariate designs in `route()`
   with the CURRENT code, to see whether the 1-df designs already match.
   This needs no source change; it tells us whether stage 2 has to fix
   anything beyond the basis.
2. Generalize `type3_contrasts()` and remove the selection branch; add the
   R-free tests.
3. The `RIS_FACTOR` structure in `make_lmm_scenarios.R`. The user runs R.
4. The user note (if Q2 = yes) and the docs.

## Out of scope
Pairwise contrasts with covariates. emmeans evaluates them on a reference
grid with each covariate at its mean. `lmm_pairwise` has no covariates
today, and the generator writes no contrasts for covariate designs. That
is a separate plan: an emmeans reference grid plus generator support.

## Open questions for the user
- **Q1, licensing of the existing code.** The comment above
  `type3_contrasts()` (lmm.py, from the 2026-10-19 Satterthwaite work)
  says it *ports* lmerTest's R source (anova_contrasts.R, contrast_utils.R),
  which is GPL. csv-stats is MIT (docs/claude/lmm-r-oracle-testing.md,
  "Licence constraint"). Recommendation: since stage 2 rewrites this
  function anyway, write it from the published type-III definition above,
  and replace the comment with the citations. The same R fixtures verify
  it.
- **Q2:** add the `type3_at_covariate_zero` note? (Recommended: yes.)
- **Q3:** OK to add the 9th structure, one R run?

## Status
- 2026-10-20: drafted; Q1–Q3 approved (all yes).
- 2026-10-20: all stages written; pytest and R not run yet (the user runs them).
  - `lmm.py`:
    - `type3_hypotheses()` replaces `type3_contrasts()` and its helpers,
      rewritten from the definition (Q1). The single-term case is the
      unit-diagonal Cholesky (LDL') rows. Round-off is zeroed relative to
      1e-10 instead of R's zapsmall.
    - New `covariate_zero_terms()`, the `type3_at_covariate_zero` warning,
      and `term_columns()`, now the one owner of term -> columns.
    - `details["term_tests"]` is always `"type3"`.
  - Tests:
    - `test_lmm_type3.py` (new, no R).
    - `route()` gains `contrasts` (separate from `terms`).
    - 9 structures; the zapsmall/doolittle tests removed.
  - `make_lmm_scenarios.R`: W3_x, B2W3_x, and RIS_FACTOR (appended last),
    plus a time × x true effect that uses no random draws.
  - Docs: lmm.py docstrings, both READMEs, the oracle note, statistics.md.
- Q1 caveat, told to the user: the rewrite was done after reading the
  old ported code, so it is not a strict clean room. The algorithm is
  fixed by needing lmerTest's basis.
- Stage 1's separate measurement was folded into one run: the route
  widening and the new construction land together. Read the old/new
  difference off the failures, if any.
- 2026-10-20: user ran R (25 RIS_FACTOR scenarios; existing 200 unchanged)
  and pytest. Everything passes: curated 687 passed / 18 skipped; scenario
  term tests now cover 195 scenarios, and the 94 remaining covariate skips
  are contrasts only. csv-stats 995d6be pushed.
- Next (not planned yet): pairwise contrasts with covariates (emmeans grid
  at the covariate mean).
