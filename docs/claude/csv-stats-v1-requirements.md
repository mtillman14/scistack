# csv-stats v1 — Requirements from scistack

Status: **requirements handoff** (2026-09-27). Written from scistack's
statistics design ([statistics-design.md](statistics-design.md)) so the
work can be carried out in the csv-stats repository
(github.com/mtillman14/csv-stats). This file is self-contained: it explains
the scistack context it needs, and every requirement names the csv-stats
code it touches.

Baseline: the csv-stats installed in scistack's `.venv` (dist 0.1.10;
`csvstats/__init__.py` says `0.1.0`). Line references below are to that
copy. **Check each item against csv-stats HEAD before starting.** Some may
already be fixed.

---

## 1. Context: what scistack will do with csv-stats

scistack is adding an interactive statistics layer (a "Stats Studio"
attached to its Plot Studio). The decisions that shape csv-stats:

- **csv-stats owns all the math.** scistack never computes a test statistic,
  df, p-value, effect size or CI. If a test is missing, it is added to
  csv-stats, not worked around in scistack. This includes mixed-model
  degrees of freedom and SPM1d.
- **Python is the only engine.** MATLAB users reach csv-stats through
  scistack's bridge, so there are no MATLAB ports.
- **Input** is always a long-format `pandas.DataFrame`: one row per
  observation, with categorical columns for the factors, a subject column
  when measures repeat, and a numeric DV column (or, for SPM1d, a column of
  equal-length 1-D arrays). scistack has already aggregated the data down to
  the unit of analysis before calling.
- **Output** is stored as JSON in a database with lineage, shown in a GUI,
  overlaid on plots (significance brackets, SPM cluster shading), exported
  (CSV/JSON/TOML/LaTeX/plaintext), and swept across analysis variants. So
  results must be **deterministic, full-precision, JSON-serializable, and
  share a common core** that all those consumers read without special-casing
  each test.
- **Boundary.** csv-stats knows nothing about scistack (schema keys,
  variants, plots). scistack owns presentation: rounding for display, APA
  strings, LaTeX, choosing which tests form a correction family. csv-stats
  returns numbers. Its own PDF report remains a csv-stats feature, but only
  on request.

---

## 2. Cross-cutting contract (applies to every test)

### C1. Pure by default
- `filename` defaults to **`None`**. No file is written unless one is asked
  for. Today every public function defaults to a `*.pdf` name and writes to
  the current working directory.
- **No `print`.** Use the `logging` module (logger `csvstats`) for
  diagnostics, and put user-relevant problems in the result's `warnings`
  list (C5). Today there are prints in `test_assumptions.py`,
  `anova.py:46`, and `save_stats.py:92`.
- **Never mutate the caller's DataFrame.** Today `ttest_ind` adds
  `one_sample`, `ttest_dep` adds `one_sample`, and
  `test_variance_homogeneity_assumption` adds `combined_group`. Work on a
  copy.
- **Deterministic.** Identical input gives byte-identical JSON. Remove
  `result["date"]` from the result dict (a rendered PDF may still print a
  date). Any resampling (permutation tests, bootstrap CIs, nonparametric
  SPM) takes a `seed` argument and records it in the result.

### C2. Full precision
Return unrounded floats. Today `p` and `t_statistic` are `round(..., 4)`, so
p = 2e-7 is stored as `0.0` and cannot be displayed as `p < .001` or used
in a correction. Round only when rendering the PDF.

### C3. Common core result
Every test function returns a dict with this core, plus test-specific extras
under `details`:

```python
{
  "schema_version": 1,
  "test": "ttest_ind_welch",          # stable machine id (catalog in §3)
  "test_label": "Welch's independent-samples t-test",
  "design": {
    "dv": "stride_length",
    "between": ["Group"],              # [] if none
    "within": [],                      # [] if none
    "subject": None,                   # str when any factor is within
    "covariates": [],
  },
  "alpha": 0.05, "ci_level": 0.95, "tails": "two-sided",
  "n": {
    "total": 40,
    "per_cell": [{"cell": {"Group": "A"}, "n": 20}, ...],
    "excluded": 2,                     # rows/subjects dropped, and why
    "excluded_detail": [{"subject": "S07", "reason": "missing level B"}],
  },
  "effects": [                         # one entry per tested term
    {
      "term": "Group",                 # "Group", "Group:Time", "(Intercept)", ...
      "statistic_name": "t",           # t | F | U | W | H | chi2 | z | r | ...
      "statistic": -2.143,
      "df": [37.6],                    # list: [df] or [df1, df2]; [] if n/a
      "df_method": "welch",            # welch | pooled | residual | satterthwaite
                                       # | kenward_roger | wald | n/a
      "p": 0.0386,
      "sphericity_correction": None,   # or {"method": "greenhouse_geisser", "epsilon": 0.71}
      "estimate": {"name": "mean_difference", "value": -1.2,
                   "ci": [-2.33, -0.07]},        # optional; direction per C7
      "effect_size": {"name": "hedges_g", "value": -0.68,
                      "ci": [-1.31, -0.04]},
    },
  ],
  "assumptions": [
    {"name": "normality", "test": "shapiro_wilk", "scope": "residuals",
     "statistic": 0.97, "p": 0.41, "violated": False},
    {"name": "homogeneity_of_variance", "test": "levene", ...},
    {"name": "sphericity", "test": "mauchly", ...},
  ],
  "post_hoc": None | {
    "correction": "holm",
    "comparisons": [
      {"term": "Group", "a": "A", "b": "B",
       "estimate": -1.2, "ci": [...], "statistic": -2.1, "df": [18],
       "p": 0.049, "p_adjusted": 0.098,
       "effect_size": {"name": "hedges_g", "value": ..., "ci": [...]}},
    ],
  },
  "warnings": [{"code": "small_cell_n", "message": "cell Group=B has n=2"}],
  "engine": {"csvstats": "1.0.0", "scipy": "...", "pingouin": "...",
             "statsmodels": "...", "spm1d": "..."},
  "seed": None,
  "details": { ... test-specific fields ... },
}
```

Rules:
- `effects` is always a list, even for a single term. Multi-way ANOVA puts
  each main effect and interaction in it; mixed models put each
  fixed-effect coefficient or term in it. scistack reads effects uniformly
  and never branches on `test`.
- `violated` in `assumptions` is decided at `alpha`. scistack shows the
  statistic and the p-value alongside it.
- Every value is a JSON primitive. numpy types are converted in the result
  itself, not only inside `save_handler`'s `convert_types`.
- Descriptive statistics (`describe`, §3.1) return their own shape; they are
  not a test.

### C4. One way to describe the design
Replace the positional `group_column1/2/3` + `repeated_measures_column`
style with one keyword signature shared by every test:

```python
fn(data, dv, *, between=(), within=(), subject=None, covariates=(),
   alpha=0.05, ci_level=0.95, tails="two-sided", levels=None,
   seed=None, filename=None, render_plot=False)
```

- `levels={"Group": ["Control", "Patient"]}` fixes factor-level order (and
  contrast direction, C7). The default is sorted order.
- The factorial model is generated from `between` + `within`. Mixed designs
  (`between` and `within` both non-empty) must work.
- Keep the `dv="_"` all-columns loop if you want it, but replace the bare
  `except:` in `run_all_columns.py`.

Decide in csv-stats whether this is a clean break (0.x → 1.0) or whether the
old names stay as thin wrappers. scistack only calls the new signature.

### C5. Structured warnings, not side effects
Every problem that might change how a result is read is added to
`warnings` with a stable `code`. At least:

`small_cell_n`, `unbalanced_design`, `empty_cell`, `rows_dropped_missing`,
`subjects_dropped_incomplete`, `assumption_violated:<name>`,
`convergence_failure`, `singular_fit`, `ties_present` (rank tests),
`exact_p_unavailable`, `unequal_curve_length` (SPM).

Invalid input **raises** a `ValueError` with a message naming the offending
column or row. Today `anova1way` returns `None` when
`data_column == group_column`.

### C6. Missing data is reported, never silent
Row-wise or subject-wise dropping is recorded in `n.excluded` /
`n.excluded_detail`. Today `ttest_dep` calls `pivot_data.dropna()` and
silently drops incomplete subjects.

### C7. Comparison direction is explicit
Every estimate and signed effect size is `a − b`, with `a` and `b` named in
the result and ordered by `levels`. Today `ttest_dep` computes
`groups[0] − groups[1]` from `unique()` order, so the sign depends on the row
order of the input.

### C8. Effect sizes with CIs everywhere
Every effect gets an effect size **and its CI** (§3 gives the measure per
test). This is the main gap after test coverage.

### C9. Standalone p-value correction
`correct_pvalues(pvals, method) -> list[float]` with `bonferroni`, `holm`,
`fdr_bh`, `fdr_by`, `none`. scistack uses it for families it builds itself
(for example one test repeated across analysis variants). csv-stats' post-hoc
code should use the same function.

---

## 3. Test catalog for v1

`test` ids are the stable machine ids for C3.

### 3.1 Descriptives — `describe(data, dv, by=())`
Per cell and overall: n, n_missing, mean, SD, SEM, variance, 95% CI of the
mean (**t-based, stated in the output**), median, IQR (Q1/Q3, **quantile
method stated**; pandas' default is linear), min, max, mode (for discrete
data). Existing `calculate_summary_statistics` covers most of this. Add
SEM, median/IQR as named fields, the CI method and the quantile method.
scistack's plot error bars must match these numbers, so the method names are
part of the contract.

### 3.2 t-tests
| id | design | effect size (+CI) |
|---|---|---|
| `ttest_one_sample` | one group vs `popmean` | Cohen's d / Hedges' g |
| `ttest_ind_student`, `ttest_ind_welch` | 2 between levels | Hedges' g |
| `ttest_paired` | 2 within levels + subject | d_z (and d_av in `details`) |

**Bug to fix:** `ttest_ind` with two groups currently returns
`anova1way`'s result (F, not t; no df for t; no Welch; no effect size),
`ttest.py:57-61`. Welch should be the default for independent samples, with
`equal_var=True` for Student's t. Report the mean difference with its CI in
`estimate`.

### 3.3 ANOVA family
| id | design | effect size |
|---|---|---|
| `anova_between` | 1–3 between factors | partial η² (+CI), ω² |
| `anova_within` | 1–3 within factors + subject | partial η², generalized η² |
| `anova_mixed` | ≥1 between + ≥1 within | partial η², generalized η² — **new** |
| `ancova` | between factor(s) + covariate(s) | partial η² — **new** |

- **Apply** sphericity corrections, not just test them. When a within term
  has more than 2 levels, report Mauchly's test and both GG and HF epsilon.
  Apply GG to df and p by default, and record it in
  `effects[].sphericity_correction`.
- Post-hoc tests on significant terms with the chosen correction (default
  Holm; the existing `_perform_posthoc_tests`). Make the 0.05 threshold in
  `significant_pairs` equal to `alpha`, and report estimates and CIs for each
  comparison.
- Keep type III sums of squares for unbalanced between designs, and state
  the type in `details`.

### 3.4 Nonparametric
| id | parametric twin | effect size (+CI) | post-hoc |
|---|---|---|---|
| `mann_whitney` | independent t | rank-biserial r | — |
| `wilcoxon_signed_rank` | paired t | matched-pairs rank-biserial r | — |
| `kruskal_wallis` | 1-way between | ε² | Dunn (with correction) |
| `friedman` | 1-way within | Kendall's W | pairwise Wilcoxon (with correction) |

Report the median difference or the Hodges–Lehmann estimate in `estimate`.
Record `exact` vs. asymptotic p in `details`, and flag ties (C5).

### 3.5 Linear mixed models — `lmm`
- Design comes from the C4 signature: fixed effects = `between` + `within` +
  `covariates` (full factorial by default), random intercept for `subject`.
  Optional `random_slopes=("Time",)`. Also accept an explicit `formula=` for
  power users.
- REML by default (`reml=True`); ML available for model comparison.
- `effects`: one entry per fixed-effect **term** (omnibus F with df) and,
  in `details`, the coefficient table (estimate, SE, CI, t, df, p). Random
  effects (variance components, ICC of the random intercept), AIC/BIC,
  log-likelihood and convergence status also go in `details`.
- **Degrees of freedom are the key requirement.** `df_method` must be
  explicit on every effect. Reviewers expect **Satterthwaite** (lmerTest's
  default); Kenward–Roger is a plus. statsmodels `MixedLM` provides only Wald
  z/χ² tests. Target: implement Satterthwaite in csv-stats and validate it
  against `lmerTest` (§5). If v1 ships before that, emit `df_method: "wald"`
  (z statistics, `df: []`) and never pretend to have t/F df.
- Convergence failures and singular fits become `warnings`, never
  exceptions.
- `model_compare(results...)`: likelihood-ratio test between nested ML fits,
  plus AIC/BIC. Optional for v1.

### 3.6 Correlation and regression
| id | notes | effect / CI |
|---|---|---|
| `pearson`, `spearman`, `kendall` | two numeric columns | r (+CI, Fisher z for Pearson) |
| `rm_corr` | within-subject correlation (subject column) | r_rm (+CI) |
| `ols` | DV ~ predictors (numeric or categorical) | coefficient table, R², adj. R², F |

`rm_corr` matters for repeated-measures lab data, where pooled correlation
across subjects is pseudoreplication.

### 3.7 Reliability and agreement
| id | notes |
|---|---|
| `icc` | all six Shrout–Fleiss/McGraw–Wong forms. The **form is a required argument** (no default: the choice is substantive). ICC + 95% CI + F test |
| `sem_mdc` | SEM (from ICC and SD) and MDC95; formula stated in `details` |
| `bland_altman` | bias, SD of differences, 95% limits of agreement **with CIs**, proportional-bias regression; paired columns or long format + method column |

### 3.8 Categorical
| id | effect size |
|---|---|
| `chi2_independence` | Cramér's V (+CI); expected-count warning |
| `chi2_goodness_of_fit` | Cohen's w |
| `fisher_exact` (2×2) | odds ratio (+CI) |
| `mcnemar` (paired 2×2) | odds ratio (+CI) |

### 3.9 SPM1d — csv-stats wraps `spm1d`
csv-stats wraps the `spm1d` package so that 1-D continuum tests return the
same core record (C3) as scalar tests. `spm1d` becomes a required
dependency (it is pure Python). Pin its version and record it in `engine`;
check the API of the pinned version (0.4 vs. 0.5 differ) at implementation
time.

**Input**: the same long DataFrame and C4 design arguments, except that `dv`
names a column whose cells are **1-D arrays of equal length** (usually 101
nodes of a time-normalized cycle). csv-stats stacks them into the
`(J, Q)` array spm1d expects, ordered by subject and level so that paired
and RM designs line up.

**Validation** (raise with named rows, never resample): unequal lengths
(list each distinct length and the rows that have it), NaN inside a curve,
a subject missing a within level (or drop it with C6 reporting, whichever
the user chooses).

| id | spm1d call |
|---|---|
| `spm_ttest_one_sample` | `stats.ttest` |
| `spm_ttest_paired` | `stats.ttest_paired` |
| `spm_ttest_ind` | `stats.ttest2` (equal_var flag) |
| `spm_regress` | `stats.regress` |
| `spm_anova_between` | `stats.anova1` / `anova2` / `anova3` |
| `spm_anova_within` | `stats.anova1rm` / `anova2rm` / `anova3rm` |
| `spm_anova_mixed` | `stats.anova2onerm` / `anova3onerm` / `anova3tworm` |
| `*_nonparam` variants | `stats.nonparam.*` with `iterations` + `seed` |
| post-hoc | pairwise SPM t-tests with Šidák-corrected alpha (the spm1d convention) |

**Output** — core record fields plus per effect:
```python
"effects": [{
  "term": "Group", "statistic_name": "t" | "F",
  "df": [...], "df_method": "spm_rft" | "spm_nonparam",
  "p": <min cluster p or None>,           # omnibus: any supra-threshold cluster
  "critical_threshold": 3.21,             # zstar at alpha
  "curve": [...],                         # SPM{t}/SPM{F} per node, full precision
  "clusters": [
    {"start": 34.7, "end": 58.2,          # interpolated node positions (floats)
     "extent": 23.5, "p": 0.003, "sign": 1 | -1 | None,
     "centroid": [..], "max_statistic": 5.1},
  ],
}]
```
Plus in `details`: `n_nodes`, `smoothness` (FWHM, parametric only),
`two_tailed`, `iterations` (nonparametric). The mean and SD curves per cell go
in `details.descriptive_curves` so a result can be drawn without a second
computation. scistack shades `clusters[].start..end` on its 1-D plots;
interpolated float boundaries are needed for exact shading.

---

## 4. Known bugs to fix (with regression tests)

| # | Where | Bug |
|---|---|---|
| B1 | `utils/save_stats.py:118-124` `dict_to_json` | `json.dump(f, result)` has its arguments swapped (crashes for `.json` filenames); `str_result` is computed and unused |
| B2 | every public fn | `filename` defaults to a PDF; writes to the current working directory (C1) |
| B3 | `anova.py:57` etc. | `result["date"]` makes results nondeterministic (C1) |
| B4 | `ttest.py:57-61` | 2-group `ttest_ind` returns a one-way ANOVA result (§3.2) |
| B5 | `ttest.py`, `anova.py` | p and t are rounded to 4 decimal places (C2) |
| B6 | `ttest.py`, `test_assumptions.py:47` | the caller's DataFrame is mutated (C1) |
| B7 | `ttest.py` `ttest_dep` | sign depends on `unique()` order (C7); `dropna` silently drops subjects (C6); `data[delta_group_column]` is dead code |
| B8 | `anova.py:44-48` | invalid arguments return `None` instead of raising (C5) |
| B9 | `anova.py` post-hoc, `test_assumptions.py` | hard-coded `0.05` instead of `alpha` |
| B10 | `utils/run_all_columns.py` | bare `except:` around `filename.format` |
| B11 | `__init__.py` | `__version__ = "0.1.0"` does not match the dist version; derive it from package metadata |
| B12 | README | import path shown as `csv_stats`; the real one is `csvstats` |
| B13 | `ttest.py` docstring | `ttest_ind` documents a `repeated_measures_column` it does not take |

---

## 5. Validation (reference values)

Every test in §3 gets at least one **golden test** against an external
reference, stored as fixtures (input CSV + expected JSON), with tolerance
around 1e-6 relative:

- R: `t.test`, `wilcox.test`, `afex::aov_ez` (between/within/mixed with GG),
  `emmeans` (post-hoc), `lmerTest::lmer` + `anova()` (Satterthwaite), `irr` /
  `psych::ICC`, `BlandAltmanLeh`, `chisq.test` / `fisher.test`,
  `rmcorr::rmcorr`.
- spm1d: its bundled example datasets and published results (zstar, cluster
  endpoints, p) for each design in §3.9.
- Textbook datasets where R isn't convenient.

Also test the contract: determinism (run twice, compare JSON bytes),
no-mutation (hash the input before and after), no files written when
`filename=None`, and that results survive a `json.dumps` round trip.

---

## 6. Suggested order

1. **Contract + bug fixes**: C1–C9, B1–B13, the core record, `describe`,
   `correct_pvalues`. Everything else builds on this.
2. **t-tests + ANOVA** on the core record: Welch default, mixed ANOVA,
   ANCOVA, applied sphericity corrections, effect-size CIs.
3. **Nonparametric** (§3.4).
4. **SPM1d wrapper** (§3.9). scistack's first release needs it.
5. **Correlation / regression / categorical** (§3.6, §3.8).
6. **Reliability** (§3.7).
7. **Linear mixed models** (§3.5), with Satterthwaite df as the largest
   single item. Ship with `df_method: "wald"` first if needed.

Deferred beyond v1: power / sample-size analysis, Bayesian alternatives,
bootstrap CIs as a general option.
