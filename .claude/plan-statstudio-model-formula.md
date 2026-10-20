# Plan: StatStudio model formula, covariates and random effects (2026-10-20)

## Goal
Write a StatStudio mixed model as an R-style formula
(`step_length ~ session * speed * age + (1 + session | subject)`; the fixed part is ONE crossing, see below). The
formula is edited in a popup opened by a button, because covariates and
custom random effects are used rarely. Today the model is fixed to
`dv ~ all plot factors crossed + (1 | unit)`. Nothing in scistackstats,
the GUI or csv-stats parses a formula, and the engine passes no
`covariates=` or `random=`.

## User decisions (2026-10-20)
1. The formula may change the fixed factors. That makes the stats design
   differ from the plot, which raises the existing divergence banner and
   its "Sync to PlotStudio" button.
2. Covariates are picked on the DAG canvas, like Variants and grouping
   variables (variable, optional column, variant pin), and are joined per
   row.
3. Random-effect rules: my call (below).
4. Both a text editor and helpers. The TEXT is the single source of
   truth; helpers only edit the text.

## My calls (say if any is wrong)
- **The left side is fixed.** The popup shows `<dv> ~` read-only and
  stores only the right side. One analysis runs on every DV, so the left
  side is "each measure".
- **The design stays the one owner of the factors.** `StatsSpec.design`
  (the plot-shaped design) is shared by Descriptives, every analysis and
  the plot link. Applying a formula whose fixed factors differ writes the
  design:
  - a new factor gets role Group
  - a factor left out gets role Collapse: the rows average over it, as the
    figure would
  - this is what makes `design_diff` show the banner (decision 1)

  When the design changes elsewhere (the rail, a sync from the plot),
  `model.rebase` rewrites the formula's fixed part from the design and
  keeps its covariates and random terms. A random term that names a
  factor no longer in the model makes the formula invalid: shown, never
  dropped silently.
- **The fixed part is a full crossing:** names joined by `*`, plus
  covariates. csv-stats' `lmm()` only fits the full factorial
  (`group * time * x`). `group + time` (main effects only) and a bare
  `group:time` are refused with that reason. Main-effects-only models are
  a separate csv-stats feature.
- **Covariates and random terms imply a mixed model.** A formula with
  either sets the analysis' omnibus and pairwise tests to lmm and
  lmm_pairwise, labelled "chosen: model formula". The catalog greys the
  other tests with the reason. A formula that only changes fixed factors
  keeps the recommendation flow as it is.
- **Random-effect rules** (decision 3):
  - A **group** must be a column of the analysis rows: the unit (named by
    its sample key, e.g. `subject`), a schema key above the unit (`site`),
    a schema key in the rows that crosses the unit (`item`), or a factor
    used as an interaction group (`subject:time`).
  - A **slope** is a covariate or a factor of the model.
  - Before any fit, it refuses what lme4 refuses: observations ≤ random
    effects for a term (RIS_014, 2026-10-20). Rows are the figure's
    reduction (one per unit × cell), so a within-factor slope with one
    row per unit × level is caught here with a plain reason, not a
    singular fit.
  - Interaction groups (`a:b`) and `||` are supported as in csv-stats.
    `||` with a factor is refused, as csv-stats does.
- **A covariate is numeric and constant within an analysis row.** The
  join (decision 2) uses the same per-schema-key merge as grouping
  variables, so a covariate must sit at or above the data's level (the
  existing prefix rule). If it varies within one analysis row (a
  per-trial value while the figure averages trials), it is refused with
  that reason. Averaging it is a later option.
- **Covariates are not a plot concept.** They live on the analysis, not in
  the design, so they never raise the divergence banner.
- **Codegen:** a formula with covariates is refused (`why_not_exportable`),
  as joined factor variables already are. A formula with only random terms
  on columns already in the rows exports.

## One owner per concept
| Concept | Owner |
|---|---|
| Formula grammar: parse the right side to terms, render terms to text, errors with positions | csv-stats `csvstats.lmm_formula` (new). `lmm_fit.random_formula` and `lmm._formula_text` delegate to it. |
| lme4's identifiability refusal (observations ≤ random effects) | csv-stats `lmm_fit.build_blocks` |
| Default formula from the design; check a formula against design + bindings + rows; rebase on a design change; formula → engine kwargs | scistackstats `model.py` (new) |
| Joining a variable onto rows per schema key, variant pinned | scistackplotdb `source._attach_factor_variables`, generalized to a `kind` ("factor" → levels; "covariate" → numeric, no level conversion). Not a second joiner. |
| Which tests a formula forces / greys | `catalog` (`select_tests`, `why_unavailable`) |
| The model sentence in captions | `format` |

## Data model (`scistackstats.spec`)
- `Analysis.formula: str | None`: the right side. None = the default
  rendered from the design (never stored, so it follows the design).
- `Analysis.covariates: list[CovariateBinding]`, where
  `CovariateBinding(name, variable, column=None, variant=None)` binds a
  formula name to its DAG source. Applying keeps only the bindings the
  text references, because the text is the truth. A formula name with no
  binding that is not a factor or row key is an error: "pick it with
  Add covariate…".
- `restore.STATS_SETTINGS` gains both, so presets and saved tables carry
  them. Beta rule: no migration; old saved specs simply have none.

## Stages
1. **csv-stats** (`lmm_formula`, identifiability check):
   - The grammar: fixed `name ('*' name)*` plus covariates; random terms
     `(lhs | g)` and `(lhs || g)`, where lhs is `1`, `0` or names joined
     by `+`, and g is `name (':' name)*`; backticks quote names with
     spaces.
   - Tests:
     - Round trip render(parse(s)) == s.
     - **Every fixture's own formula parses into the `random` blocks and
       fixed terms its JSON records:** 225 R-written formulas, free.
     - Errors carry positions.
     - The observations ≤ random effects refusal (RIS_014's shape).
   - A csv-stats release before scistack depends on it (version: the
     user's call).
2. **scistackstats** (pure):
   - `model.py`: default, check (terms classified as within or between
     factor, covariate, random group or slope, with errors and warnings),
     rebase, kwargs.
   - The spec fields; the engine passes `covariates=` and `random=` to
     lmm and lmm_pairwise.
   - catalog forcing and greying; the caption states the formula.
   - Exclusions name rows csv-stats dropped for a missing covariate (the
     never-drop-silently rule).
   - Tests, including formula → design write → `design_diff`, and rebase
     after a rail change.
3. **Covariate rows:**
   - The generalized joiner (`kind="covariate"`): numeric check, the
     prefix rule, a variant pin, and missing values left NaN with a WARN.
   - `table_request`/`load_tables` request covariate variables;
     `analysis_frame` carries them through the reduction (`carry=`) and
     refuses one that varies within a row.
   - Offers: numeric variables and columns. This is the mirror of
     `groupable_variables`, which offers categorical ones only, and reuses
     `plot_variant_graph` for the pin.
   - Tests in scistackplotdb (`test_factor_variables.py` style) and
     scistackstats.
4. **GUI backend** (`api/stats.py`, `stats_service.py`):
   - `stats_formula_check` (live, on each edit): parse + check against the
     current rows, returning terms, errors with spans, and warnings.
   - `stats_covariate_offer`; `stats_formula_apply` (writes the analysis
     and, when factors change, the design). RPC count logs per the
     startup-log convention.
5. **Frontend** (StatStudio):
   - In the Comparisons rail: a one-line read-only formula and a
     **Model formula…** button.
   - The popup:
     - the `<dv> ~` prefix and a text field
     - errors underlined at their span, plus a resolved-terms list
     - **Add covariate…** (the DAG picker, `PickerDialog` with its own
       provider, as the grouping picker does) inserts `+ name` and the
       binding
     - **Add random term…** (group from the offered keys,
       intercept/slope checkboxes, a correlated toggle) inserts text
     - **Reset to default**, Apply / Cancel
   - Undo through the existing state-diff stacks. Rebuild both vite
     targets.
6. **Docs:**
   - `stat-studio-controls.md` (rail line, popup, RPCs)
   - `statistics.md` (owners, traps)
   - `docs/gui-manual-testing-todo.md` (a new section)
   - csv-stats README (`lmm_formula`)
   - memory

## Out of scope
- Main-effects-only or partial fixed models (needs csv-stats fixed-term
  selection).
- `lmm(formula=)` in csv-stats' public API for library users. The parser
  would make it small; it is not needed for the GUI.
- A covariate averaged within a row; several grid values per covariate;
  emtrends.
- GLMMs; random effects on a non-mixed test.

## Status
- 2026-10-20: drafted after the user's 4 decisions. Not started. Waiting
  for approval of the "my calls" section.

## Status (2026-10-20, all stages written; pytest NOT run — the user runs it)
- Correction to the goal's example: `session * speed + age` is not
  fittable. `lmm()` crosses covariates with the factors, so the grammar
  refuses a second fixed term and suggests `session * speed * age`.
- **Stage 1 (csv-stats):**
  - `lmm_formula.py` (parse/render, `FormulaError` with a span).
  - `lmm_fit.quote_name`; `random_formula` spells `0 + x` and backticks.
  - `lmm._formula_text` delegates to the renderer.
  - `build_blocks` refuses observations ≤ random effects (lme4's check).
  - Tests: `tests/test_lmm_formula.py`, including every scenario fixture's
    R formula → its recorded structure.
- **Stage 2 (scistackstats):**
  - `Analysis.formula` / `.covariates` (`FactorVariable`).
  - `engine.parse_formula` / `render_formula` / `FormulaSyntaxError`; lmm
    options `covariates` / `random`.
  - `model.py` (check, rebase, `apply_to_design`, `for_frame`,
    `bindings_in`).
  - `tables.CovariateValues` / `load_covariates` / `join_covariates`.
  - catalog `mixed_formula`; `run(…, covariates)` with
    `StratumResult.model`.
  - Caption "Model: …"; codegen refuses covariates.
  - Tests: `tests/test_model.py`.
- **Stage 3 (scistackplotdb):**
  - `_keyed_values` extracted from `_attach_factor_variables` (one loader).
  - `covariate_values`, `covariate_variables`, `covariate_columns`.
  - Tests: `tests/test_covariates.py`.
- **Stage 4 (GUI backend):**
  - `stats_formula_check` / `stats_formula_apply`; `_load` (tables +
    covariates); readout `model`.
  - `purpose` on `plot_grouping_graph` / `plot_grouping_columns`.
  - Tests: appended to `tests/test_stats_service.py`.
- **Stage 5 (frontend):**
  - `ModelFormulaDialog.tsx`; `modelFormulaText.ts` (+ test).
  - `GroupingDagPopup` `purpose` prop; StatStudio rail line + button;
    `api.ts` routes.
  - tsc clean; 617 node tests pass; both vite bundles rebuilt.
  - GroupingDagPopup.tsx had two raw NUL bytes as key separators (committed
    in c948e346); rewritten as `\u0000` (the same string) so the file is
    text.
- **Stage 6:** stat-studio-controls.md, statistics.md,
  statistics-design.md, gui-manual-testing-todo.md §0zzzzk, csv-stats
  README, memory.
- Not done:
  - A csv-stats release. scistack needs `csvstats.lmm_formula`; without it,
    the formula is a "gap" refusal.
  - Undo: Apply is an ordinary spec edit, like every StatStudio edit (no
    stats handler is undoable today).
