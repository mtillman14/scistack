# Plan: pairwise LMM contrasts with covariates (csv-stats, 2026-10-20)

## Goal
`lmm_pairwise()` accepts numeric `covariates=`, like `lmm()`. Its pairwise
differences are checked against emmeans on the scenario set. Today
`lmm_pairwise` has no covariates, the R generator writes no contrasts for
covariate designs, and 94 scenario items skip with "covariate design: no
emmeans contrasts generated".

Follows `plan-lmm-covariate-type3.md` (term F tests with covariates, done).

## The convention: emmeans' reference grid
emmeans builds a reference grid. Each factor takes all of its levels, and
each numeric covariate is held at ONE value, by default its mean over the
rows the model was fitted on (`cov.reduce = mean`). A pairwise difference
is the difference of two grid cells' predictions, within each combination
of the other factors (`by`). So with `group * x`, "ctl − trt" is the group
difference at x = mean(x).

- csv-stats already does exactly this for factors (`row()` in
  `lmm_pairwise`, l = x(a, context) − x(b, context)). Covariates only add
  their value to every grid row. The SE and the Satterthwaite df use the
  same machinery (`satterthwaite_df(result, l)`).
- The mean is over `prep.frame` AFTER rows with missing values are
  dropped: the fitted rows, as emmeans uses the model frame.
- **Inconsistency to document** (it is R's too): `lmm()`'s type-III F for
  `group` tests the group difference at x = 0, while `lmm_pairwise`
  compares groups at x = mean. On an uncentred covariate the two can
  disagree sharply. Both follow R (lmerTest's anova vs emmeans).
  - Both records say which value they use: lmm's
    `type3_at_covariate_zero` warning, and lmm_pairwise's
    `details["covariates_at"]`.
  - The docstrings and the README explain it.

## Changes
1. **`lmm_pairwise(..., covariates=(), covariates_at=None)`** (Q1):
   - `covariates` goes through `_setup` (already shared with `lmm`), so
     validation, missing-row handling and the fixed formula are
     identical.
   - Grid rows set each covariate to `covariates_at[c]` when given, else
     the mean over the fitted rows.
   - `details["covariates_at"] = {"x": 61.27}`, and whether each value is
     the `mean` or `given`.
   - The `contrasts` text says "at x = 61.27 (mean)".
   - `details["formula"]` includes the covariates (`_formula_text`
     already supports them).
   - DEBUG log of the grid values.
   - Still needs at least one factor; a covariate-only design has no
     pairs.
2. **R generator** (`lmm_reference_common.R` / `make_lmm_scenarios.R`):
   write emmeans contrasts for every design with a factor, covariates
   included (today `!fd$covariate` blocks them). Record the grid value
   emmeans used per covariate (`covariates_at`, from `ref_grid(fit)@grid`),
   so the test can check our mean equals emmeans', not just the
   contrasts.
   - **Needs one R run, and it changes existing fixtures:** RI/RIS/
     RIS_UNCORR with `B2_x`, and every RIS_FACTOR scenario, GAIN
     `contrasts_*` and `covariates_at`. No other number may change; the
     commit says so, and the check is a diff limited to those keys.
3. **Tests:**
   - `route()`: `contrasts` = the design has a factor (covariates
     allowed). The reason for covariate-only designs becomes "no factor:
     no pairwise contrasts".
   - The `pairwise()` helper passes covariates through.
   - New: `covariates_at` equals emmeans' grid value.
   - R-free (`test_lmm_type3.py` or a new `test_lmm_pairwise_covariates.py`):
     - With `group * x` (2 levels, sum coding), the contrast equals
       2(β_group1 + β_group1:x · x̄): the linear identity.
     - Shifting x by a constant leaves every contrast unchanged (the mean
       shifts with it). This is the opposite of `lmm()`'s at-zero group
       F, and the test pins both side by side.
     - `covariates_at={"x": x̄}` equals the default; another value moves
       the estimate by exactly 2 β_group1:x · Δ.
     - A covariate-only design is refused with a clear error.
4. **Docs:**
   - lmm_pairwise docstring and the csv-stats README: the reference grid,
     and the at-0 vs at-mean note.
   - `docs/claude/lmm-r-oracle-testing.md`: the route and the new
     fixture keys.
   - `statistics.md`: one line. scistack doesn't pass covariates yet, so
     there is nothing to wire.

## Stages
1. `lmm_pairwise` covariates + `covariates_at`, plus the R-free tests.
2. The R generator change. The user runs R; check the fixture diff.
3. The route widening, the `covariates_at` comparison, docs, memory.

## Out of scope
- Covariates in scistack's StatStudio. Its design model has factors only
  (no `covariates`/`random` anywhere in scistackstats); that is a GUI and
  catalog feature of its own.
- Several grid values per covariate (emmeans `at = list(x = c(40, 60, 80))`,
  contrasts per value). `covariates_at` takes one value per covariate;
  a list could come later.
- Slopes as contrasts (emtrends: the group difference in the x slope).

## Open questions for the user
- **Q1:** offer `covariates_at=` (a value per covariate, like emmeans'
  `at=`)? Recommended: yes. It is cheap, it is how to compare groups at a
  clinically meaningful age instead of the sample mean, and the default
  stays emmeans'. Only the default is checked against R; a given value is
  covered by the exact linear identity.
- **Q2:** OK to regenerate fixtures in a way that ADDS contrasts to
  existing covariate scenarios (no other number changes, verified by
  diff)?

## Status
- 2026-10-20: drafted; Q1 and Q2 approved (yes).
- 2026-10-20: all stages written; R and pytest not run (the user runs them).
  - `lmm_pairwise(covariates=, covariates_at=)`: grid values recorded in
    `details["covariates_at"]` ({value, source}); refusals for a design
    with covariates but no factor and for unknown `covariates_at` keys;
    DEBUG log of the grid.
  - `test_lmm_pairwise_covariates.py` (new, no R).
  - `make_lmm_scenarios.R`: contrasts for every non-collinear design with a
    factor; `covariates_at` from `ref_grid(fit)@grid`.
  - `test_lmm_scenarios.py`: the contrasts route needs a factor; the
    `pairwise()` helper passes covariates; new
    `test_covariate_grid_value_matches_emmeans`.
  - Docs: the lmm.py docstrings, the README, the oracle note,
    statistics.md.
- 2026-10-20: R regenerated. The diff was verified: 52 results gained
  contrasts and 94 gained covariates_at; nothing else changed. pytest
  passes:
  - Every covariate contrast matches emmeans (estimate, SE, df, t, p).
  - The grid value matches emmeans' ref_grid in 52 scenarios.
  - One fix after the first run: as_list now accepts unboxed numbers.
  - csv-stats 60ed083 pushed.
