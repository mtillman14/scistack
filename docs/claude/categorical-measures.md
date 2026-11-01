# Categorical (text) measures in Plot Studio

*Written 2026-11-01 as the before-state of `.claude/plan-categorical-measures.md`,
rewritten the same day for the finished state. Companion to
`measure-shape-and-collapse.md` (the measure axis of the control surface) and
`synthetic-factors.md` (derived tables).*

## Vocabulary

- **Categorical measure**: a measure whose cells hold text or booleans.
  `shape.classify_value` returns `Shape.CATEGORICAL` for `str`, `bool` and
  `np.bool_` (bool first: it subclasses int).
- **Categorical factor**: a text column used to *group*. Unchanged.
- **Mode** (`spec.CategoricalStat`): what the text is drawn as — `COUNT`,
  `PROPORTION` or `VALUE` (the categories as axis positions).
- **`Level`** (`categorical.LEVEL_FACTOR`): the synthetic factor naming the
  categories in COUNT / PROPORTION mode. Displayed as the measure's name.

## Decisions (user, 2026-11-01)

1. A mixed wide variable is two measures: `Gait` and `Gait (text)`.
2. A text measure opens on a **Count bar**: one bar per category, height =
   records.
3. Booleans are categories (True / False).
4. **Collapse means pool** for every text plot. The last collapsed key is
   still the sample.
5. **Two bar types**: Count (no sample, no "Show sample", no error bar) and
   Proportion (mean of per-unit %s ± band, "Show sample" draws the units).
   In the spec both are `BAR`/`HBAR` + `CategoricalOptions.stat`; the kind
   grid shows them as two tiles.
6. A Proportion bar with nothing collapsed is unavailable ("needs a sample"),
   like box / violin — no binomial CI over pooled trials.

## Owners (one each — CLAUDE.md NOTE 4)

| Concept | Owner |
|---|---|
| Which shapes are plottable | `shape.is_plottable` / `PLOTTABLE_SHAPES` (guard: `tests/test_plottable_owner.py`) |
| Text → numbers (one-hot / positions) | `categorical.apply_categorical` |
| Which mode a kind draws | `categorical.mode_for_kind` / `categorical_mode` |
| How a collapsed key pools | `categorical.pooled_reduction`, read by `reduce._collapse_levels` |
| Which keys pool / which is the sample | `roles.collapse_steps` (COUNT: pre=all; VALUE: sample=all) |
| What a text measure cannot do | `categorical.categorical_problem` (validate + capability) |
| Free text threshold | `categorical.MAX_CATEGORY_LEVELS` (= `scistackplotdb.MAX_GROUP_LEVELS`) |
| Category text of a cell | `table.category_text` / `category_levels` |
| The same rewrite as code | `categorical.categorical_code` (codegen) |
| Default roles per tile | `roles.categorical_assignment` / `categorical_roles_for` |
| Kind tiles | `capability.CATEGORICAL_TILES` / `categorical_kind_entries` |
| `Var (text)` name | `scistackplotdb.text_fields` |
| Field partition | `ScidbSource._melt_fields` (by value) / `load.field_shapes` (catalog hint) |
| Stacked bars | `stacking.stacks` / `stack_bases` / `stacked_bounds` |
| Grid heatmap layers | `roles.grid_layers`; matrix `reduce._grid_frame` |

## The pipeline

`reduce._build_plan_timed`: variant sets → level groups → cell collapse →
**`apply_categorical`** → temporal → validate / roles → filters. It returns
the SPEC too: include / exclude filters on the text measure are applied to
the records there and consumed; a range filter on it stays for `validate` to
refuse. It is idempotent on a rewritten table (codegen re-resolves one).

- **COUNT / PROPORTION** — one row per (record, category of its field): 1 (or
  100) where the record is that category, else 0. Per FIELD: `Outcome`'s
  records never get rows for `Side`'s categories. The (field, category) pairs
  are `_observed_pairs`, which codegen states as a literal so every figure of
  a fan-out offers the preview's categories.
  - Count: `collapse_steps` pre = every collapsed key, pooled and SUMMED.
  - Proportion: pre = all but the sample, pooled and AVERAGED (= the unit's %),
    then the bar's statistic ± band over the units.
  - `CategoricalOptions.level` keeps one category; `Level` is then dropped.
- **VALUE** — each value becomes its index in `MeasureInfo.levels`;
  `ResolvedPlot.y_categories` labels the y ticks; limits are `(-0.5, n-0.5)`.
  A strip draws every pooled record; a spaghetti refuses any collapsed key.

The plan cache key carries the mode (the kind is presentation otherwise),
and a kind a text measure cannot take keys its own plan so `validate` sees it.

## Refusals

| Case | Where | Reason text starts |
|---|---|---|
| > 50 distinct values | every tile, the panel's eligibility | "… free text or an identifier" |
| kind outside bar/hbar/heatmap/strip/spaghetti | validate | "… holds text: it is drawn as" |
| range filter on the measure | validate | "… filtered by choosing its categories" |
| Proportion with nothing collapsed | tile + validate | "A % per unit needs a sample" |
| Proportion with `Level` collapsed | validate / role menu | "… would make every % 100" |
| spaghetti of categories with a collapsed key | validate | "… text cannot be averaged" |
| text measure with an x measure | validate | "… no values to pair with an x measure" |
| Show sample on a count / strip / stacked bar | overlay_unavailable | "A count has no sample" … |

## Database side

- `ScidbSource.describe` lists `Var (text)` (`text_of: Var`) for a mixed
  variable, classified per field from one sampled value
  (`load.field_shapes`, cached in `_field_shapes`, container types refused
  from the type alone, row lists by element).
- `_melt_fields(text=True)` melts the text fields; the default melt takes the
  rest and logs INFO naming the text measure (it used to WARN "dropped").
  An all-text variable melts its text fields by default.
- Every public source method maps a measure name through
  `text_fields.base_variable`; a text measure stacks with nothing and joins
  no x. `generate_endpoint` reads the base variable.

## GUI

- Kind grid tiles come from the report (`id`, `categorical`, `selected`);
  `kindTiles.ts` applies a click (`applyTile`), tested in `kindTiles.test.ts`.
- **Chart > Categories**: "Show: Every category / Only X".
- **Chart > Bars**: "Stack the colour levels" (any bar kind).
- **Data**: a link between `Var` and `Var (text)` (the panel's `variable` is
  now state, seeded from the prop).

## Known limits / extensions

- Mode ("most common") as a statistic for categories-on-an-axis plots.
- Diverging stacked bars for Likert scales; mosaic; pie.
- The grid heatmap export lays out the FIRST facet only; the preview facets
  fully.
- StatStudio tests for categorical outcomes.
- A datetime partition (`Var (time)`) by the same split rule.
