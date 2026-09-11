# Plot Studio — four features (todos_26.09.09.md)

Plan drafted 2026-09-09, revised after the user's second round of answers.
Extends `.claude/plan-scistackplot.md`, `.claude/plan-facet-layout.md`,
`.claude/plan-plot-variant-rows.md`.

Decisions taken with the user before drafting:

| Question | Answer |
|---|---|
| Where does stim/sham come from? | A variable in the database, **and** already a schema key (needs filter + relabel) |
| Do overlaid variables share a y axis? | No — **a secondary y axis is needed** |
| What do the arrows step? | The ITERATE figures |
| Where do filters land on export? | **Pushed down into `for_each`** (`subject=[…]` / `where=…`), falling back to the function body when not expressible |
| How are several variables × several variants expressed? | **One linear Variants list, exactly as today, except a row may draw from ANY variable node** — not one variable per plot |
| `trial`=Separate figures with schema `[subject, trial]`? | **Auto-iterate the ancestors** — one figure per (subject, trial) |
| Do the nested group layers apply to 1-D timeseries? | **No — categorical x only** |

---

## 0. What already exists (do not rebuild)

- `PlotSpec.filters: list[Filter]` — applied in `reduce._apply_filters`, emitted
  by `codegen._preamble`. **No GUI surface whatsoever.**
- The **variant-row machinery, end to end**: named rows → a synthetic `Variant`
  factor (`variants.apply_variant_sets` derives a new `LongTable` *before*
  `validate`/`complete_roles` run) → one `for_each` input per row
  (`endpoint._foreach_call`) → concatenated with a label column
  (`codegen._variant_preamble`). Stage 3 below is almost entirely a matter of
  letting each row name its own variable.
- `reduce._ordered_groups` already orders a fan-out **odometer-style**: first
  column most significant, each factor in its *declared* level order (so
  `"01","02",…,"10"` sorts correctly, not `1,10,2`).
- `scidb.schema_key("subject").isin([...])` exists and is exported
  (`scidb/src/scidb/filters.py:875`), composable with `& | ~`.
- Two synthetic-factor precedents to imitate rather than reinvent: `ColName`
  (a struct's fields melted into a factor, with a **matching `df.melt` in
  codegen** — reshaping done only on the interactive path breaks preview/export
  equality) and `Variant`.

---

## Stage 1 — Schema-key picker + Filters section (todo 3, half of todo 2) — ✅ BUILT 2026-09-09

Tests passing (user-run), uncommitted. `reduce.apply_filters` made public and
shared with `capability.factor_summary`, so the "3 of 12 selected" readout is
measured with the rule the figure uses. Level membership compares as TEXT
(matching `variant_set_mask`) — a selection crosses JSON as strings while the
column may hold `01` or `1`. All-levels is stored as NO filter, never a list of
every level, which would freeze today's levels into the spec.

Smallest stage; lights up backend plumbing that is already written.

**scistackplot**
- `table.FactorInfo` gains `is_schema_key: bool` — the GUI must not identify
  schema keys by intersecting two lists (CLAUDE.md NOTE 3).
- `capability.capabilities()` reports per factor the levels present *after*
  variant selection plus `selected` (levels surviving `spec.filters`) — the same
  shape `variant_summary` already uses, so the picker and the variant popup read
  alike.
- Logging: keep the existing `filters kept N of M row(s)` at INFO; add a
  per-column breakdown at DEBUG so an empty figure names the culprit filter.

**GUI** (`PlotStudio.tsx`) — `Spec` gains `filters`. A **"Schema keys"** section
above Factors, one row per key showing `3 of 12 selected` plus a level
multi-select, writing `Filter(column=key, include=[…])`. Clearing a row removes
the entry entirely — never store an all-levels include, it would rot as data
arrives. A separate **"Filters"** section covers non-schema factors and numeric
ranges on scalar measures.

**Tests** — `scistackplot/tests/test_filters.py` (new): include/exclude/range;
an unknown column warns and is ignored (pin current behaviour); a filter that
empties the frame yields a zero-row figure rather than raising.

---

## Stage 2 — Figure navigator with roll-over (todo 4) — ✅ BUILT 2026-09-09

Tests passing (user-run), uncommitted. Two bugs surfaced and were fixed outside
this file's scope:

* **scidb** — `_normalize_variable_inputs` ignored `as_table`, so an input asking
  for a frame arrived as a scalar. Only reachable in "full iteration mode" (every
  schema key iterated), which is exactly what ancestor promotion creates.
  Regression: `scidb/tests/test_as_table_full_iteration.py`.
* **codegen** — `_x_expression` fell back to `table.factor_names[0]` where
  `reduce._panel_frame` uses a single constant position: a preview/export
  divergence for any scalar spec with no x factor, and a hard failure once that
  first factor became an iteration key.

`scistackplotdb/tests/test_fanout_parity.py::_run_generated` now captures
`for_each`'s `failure_reasons` (via `_progress_fn`) and every assertion reports
them — `for_each` skips failing combos and carries on, so the naive form of that
test could only ever say "0 != 12".

**Two ordering fixes first — both are latent bugs today.**

1. **ITERATE order must be schema order, not dict order.** `resolve()` builds its
   iterate list from `roles.items()`, i.e. whatever order the GUI wrote the dict
   in. Assigning `trial`→Separate figures *before* `subject` produces a
   trial-major fan-out, and the roll-over runs the wrong way. Sort ITERATE
   factors into the table's factor order (which is schema order) in `resolve`,
   and use the same order for `endpoint.iterate_keys` so the exported
   `PathOutput` template matches.
2. **A nested iterated key implies its ancestors.** `trial`→Separate figures with
   schema `[subject, trial]` iterates `[subject, trial]`, giving one figure per
   (subject, trial) — the fan-out the roll-over walks. This must happen in
   `scistackplot`/`scistackplotdb`, never in the GUI, so the exported `for_each`
   gains `subject=[]` and its `PathOutput` gains `{subject}` from the same
   decision. The GUI shows which ancestors were added (the `layout_notes`
   channel — the panel never silently does something the user did not ask for).

With those, `_ordered_groups` already yields subject-1-trial-1 … subject-1-trial-N,
subject-2-trial-1: the arrows are a flat walk over that list, and the roll-over
is free.

**scistack-gui backend** (`services/plot_service.py::resolve_figures`)
- New `figure_index: int | None`. Response becomes `{ok, error, figures: [one],
  figure_labels: [all N], figure_count: N, figure_index}`.
- When given, **only that figure is rendered and serialized**. `resolve()` still
  reduces all of them (it must, to know the fan-out), but `render_plotly` +
  `_frame_records` — the expensive half for 1-D data across 30 subjects — run
  once. Log `[plot] resolved <kind>: figure i+1/N, R row(s)`.

**GUI** — `figureIndex` state; header `◀  subject=02, trial=01  (14 of 60)  ▶`;
arrows disabled at the ends (no wrap past the end of the whole fan-out); ←/→
bound while the canvas has focus. Reset to 0 **only when the ITERATE factor set
or its levels change**, or nudging a colour throws you back to figure 1. Clamp
when the count shrinks. "Save image" still writes every figure; say so in the
button's title.

**Tests** — `scistack-gui/tests/test_plot_service.py`: `figure_index=2` returns
one figure whose label equals `figure_labels[2]`; out-of-range clamps;
`figure_count` matches an unindexed call. `scistackplot/tests/test_resolve.py`:
a spec assigning ITERATE in reverse schema order still fans out subject-major;
iterating a nested key alone adds its ancestors and says so in the notes.

---

## Stage 3 — Variant rows may draw from any variable (todo 1, part A) — ✅ BUILT 2026-09-09

Tests passing (user-run), uncommitted. Built as planned, plus:

* `variants.row_mask` — the ONE mask `apply_variant_sets` (the figure) and
  `capability.variant_summary` (the row counts on screen) both use, so a
  reported count cannot contradict the figure.
* Stacking **refuses** mismatched shape or schema level rather than
  broadcasting. Broadcasting is right for an x axis (one x per y) and a silent
  inflation of n when the rows *are* the data; the error names `x_measure` for
  the relational case.
* `BaseSource._stacked` gives the CSV path the same feature (a melt), so the
  DataSource protocol stays honest rather than scidb-shaped.
* `test_an_unselected_variant_column_stays_a_factor` was asserting a rule
  `_answered` had superseded (it predated "every code axis is answered").
  Rewritten to assert what actually holds: the column leaves, and
  `spanned_code_axes` reports the pooling on the ROW.

**This is the whole multi-variable feature, and it is mostly already built.** The
Variants section stays a linear list of named rows; a row gains a **source
variable**. Rows named `Raw` (RawEMG) and `Filtered` (FilteredEMG) give a
two-level `Variant` factor that takes colour — that *is* "plot Raw vs Filtered
together". `Var1 v1 / Var1 v2 / Var2 v1 / Var2 v2` is four rows.

```python
@dataclass(frozen=True)
class VariantSet:
    name: str | None = None
    selection: dict[str, Any] = field(default_factory=dict)
    variable: str | None = None      # NEW — None means the plot's primary measure
```

**Why this is cheap.** Every downstream piece already treats rows as independent
series: `variant_params` makes one function parameter per row,
`_variant_preamble` concatenates them with a label column, and
`_foreach_call` emits one `for_each` input per row. The only change on the export
side is that `variant_expression` wraps **the row's variable** instead of the
plot's single input variable — the generated `for_each` then has
`"raw": RawEMG, "filtered": Variant(FilteredEMG, fn='bandpass', low_hz=20)`,
which is exactly what a scientist would have typed.

**Table construction** (`ScidbSource.get_table`): load the union of the rows'
variables and **stack them long**, keeping each variable's own variant columns
(NaN where a variable has no such axis) plus a `Variable` column. Then
`apply_variant_sets` masks each row *within its own variable's rows*:
`mask = (Variable == row.variable) & variant_set_mask(...)`.

That last detail resolves an ambiguity that would otherwise be a silent-wrong-
figure bug: `variants.resolve_selection` **drops** selection keys naming a column
the frame lacks (right for a stale spec — "must never silently empty a figure").
With two variables in one frame and no `Variable` qualifier, a row pinning
`Code:filterEMG=v1` names nothing about `Force`, so Force would match *every*
row and be drawn once per variant, identically. Qualifying by variable makes
"not applicable to this variable" and "stale key" distinguishable again.

Constraints, each with its own message: rows' variables must share a **shape**
(scalar cannot stack with 1-D); differing **schema depth** broadcasts the
shallower down the hierarchy (`hierarchy.join_frames` already knows how); a
multi-column struct variable cannot be a row alongside others.

**Also in this stage: retire the `measures[1]`-means-x rule.**
`PlotSpec.x_measure: str | None` becomes an explicit field. It is a genuinely
different operation from a variant row — a relational scatter needs a **wide
join** (one x value per row), whereas overlaid series **stack long** — and
conflating them in a positional list is what made that non-obvious. Note the GUI
has *no* way to build a relational scatter today (`joinable_with` is returned by
the backend and never read); this closes that gap too.

**GUI** — each Variants row gains a variable dropdown (defaulting to the plot's
primary measure) listing variables that can stack with it. `variant_summary.sets`
gains `variable` and keeps its per-row `row_count`, which is what catches a row
that matched nothing.

**Tests** — `scistackplotdb/tests/test_variant_rows_multivariable.py` (new):
two rows over two variables produce a two-level `Variant` factor; a row pinning a
code axis that only one variable has does not claim the other's rows; mixed
shapes refuse with the named message; a shallower variable broadcasts.
`scistackplot/tests/test_codegen.py`: generated `for_each` names each row's own
variable.

---

## Stage 4 — Secondary y axis (todo 1, part B) — ⏸ DEFERRED (user, 2026-09-09)

Postponed deliberately, not dropped: seaborn cannot express a twin axis, so this
is the one stage needing a **second code-generation path** held in parity with
the preview forever. Everything below stands as the design when it is picked up;
nothing else depends on it.

Keyed by **variant row**, since rows are now what a series is.

- `PlotSpec.axes: dict[str, int]` — row name → `0` (left, default) or `1`
  (right).
- `resolved.Y_AXIS = "__yaxis"` canonical column, `Encoding.y_axis`, `Labels.y2`.
  Populated in `reduce` only when some row sits on axis 1.
- **plotly**: traces carry `yaxis: "y2"`; each subplot cell gains a
  `yaxis<N>2: {overlaying: …, side: "right"}`. Care with the existing
  `_cell`/`_gaps` domain machinery (see `plan-facet-layout.md` — the fixed
  `Y_GAP` bug that inverted panels at 9+ rows).
- **matplotlib**: `ax.twinx()` per panel.
- **Refused combinations, stated not swallowed**: a second axis is meaningless
  when `Variant` holds FACET or ITERATE (each row already owns its panel), and it
  forces `share_y=False`. Both go through `GridPlan.notes` → `layout_notes` → the
  yellow line in the Layout section.
- **codegen**: seaborn cannot express a twin axis, so this case emits explicit
  matplotlib — `fig, ax = plt.subplots(...)`, `ax2 = ax.twinx()`, one
  `sns.lineplot(..., ax=…)` per axis. A *second* generator path, not a docstring
  note like `_seaborn_can_express_layout`; this is the stage's real risk.

**Tests** — `test_render.py`: axis-1 traces carry `yaxis: "y2"` and the layout
declares the overlay; mpl builds two Axes. `test_codegen.py`: the generated
twin-axis source **executes** and produces two Axes with the expected series
counts — the preview/export parity guard for this stage.

---

## Stage 5 — Group factors (todo 2, part A) — ✅ BUILT 2026-09-09

Tests passing (user-run), uncommitted. Both halves built as designed, plus:

* `groups.apply_level_groups` is a derived table wired into all three call
  sites; the **source factor survives** (unlike a variant selection — session on
  x and Phase in colour is a real figure, and neither is a variant factor so no
  pooling guard applies).
* A new bucket set is prefilled with each level's own name, so adding a group
  cannot silently redraw the figure — it partitions nothing until two rows share
  a label.
* Grouping variables ride in as their own `for_each` input (`group_condition`),
  merged inside the function: `as_table` gives a function schema keys and data
  columns only, so a subject-level value cannot travel on a trial-level frame.
* A grouping variable's own variant columns are **dropped** — which code version
  produced a group label is not what the figure compares, and carrying them in
  would put an unassigned variant factor on screen.
* Fixture gained `Condition` with an UNEVEN split (2 stim, 1 sham): a merge that
  dropped or duplicated rows still looks plausible on symmetric counts.

Two sources of a group, both requested:

**5a. A categorical variable in the database.** `Condition`/`Group` variables
classify `Shape.CATEGORICAL` and are rejected as unplottable — right for a
*measure*, wrong for a *factor*.
- `PlotSpec.factor_variables: list[str]` (so the spec round-trips through the
  docstring).
- `get_table` joins each by the same hierarchy broadcast and registers its column
  as a **factor**, level-ordered by the usual natural sort.
- `describe()` gains `groupable`: single-column joinable variables, categorical
  first. Numeric group codes are offered but not auto-classified —
  `sources/csv.py` already documents that bare numeric IDs read as measures.
- Export: the group variable becomes an extra `for_each` input (`as_table`), and
  the generated preamble merges it on the shared schema-key prefix — the same
  broadcast `hierarchy.join_frames` performs interactively.

**5b. Relabelling a schema key's levels**, for `session ∈ {pre, post1, post2}` →
`Phase ∈ {baseline, post}`:

```python
@dataclass(frozen=True)
class LevelGroup:
    name: str                  # new factor, e.g. "Phase"
    source: str                # existing factor column, e.g. "session"
    mapping: dict[str, str]    # level -> group label
    unmatched: str | None      # None = drop those rows, else a bucket name
```

Applied in `reduce` right after filters as a `.map`; emitted by `codegen` as the
same `.map`. A schema key whose levels *are already* the group names needs none
of this — it is a factor with a role today.

**GUI** — a "Groups" section: add a group variable from `groupable`, or define a
`LevelGroup` by picking a source factor and sorting its levels into named
buckets.

**Tests** — `scistackplotdb/tests/test_source.py`: a subject-level categorical
variable broadcasts one value per trial row.
`scistackplot/tests/test_groups.py`: unmatched-drop vs unmatched-bucket; a
`LevelGroup` whose source column was filtered away degrades with a warning.

---

## Stage 6 — Ordered nested grouping on the x axis (up to 3 layers) — ✅ BUILT 2026-09-09

Tests passing (user-run), uncommitted. Built as designed (6a+6b together, no
split needed). Worth knowing:

* `Role.X` left `SINGLE_ASSIGNMENT_ROLES`; COLOR is now the only member. Two
  tests encoded the retired rule and were rewritten — the colour half of
  single-assignment kept its own test so that machinery stays covered.
* Gaps scale with boundary DEPTH (an outer change closes more layers, so opens
  a wider gap) — that is what makes 3 levels readable rather than a picket fence.
* Renderers: plotly draws brackets in PAPER coords from the cell domain (data
  coords clip and move on zoom); mpl uses a BLENDED transform, x in data space
  and y in axes space, so brackets sit a fixed distance below the axis whatever
  the measure's range.
* Export limit, stated in the docstring: seaborn has no empty category, so
  generated code separates groups by ORDER alone — the interactive gaps are the
  one thing it cannot reproduce.

`stim/sham` first, `session` within each — clustered groups along a categorical
x axis with hierarchical labels beneath it:

```
   ▉ ▉ ▉    ▉ ▉ ▉        ▉ ▉ ▉    ▉ ▉ ▉
   BL P1 P2 BL P1 P2      BL P1 P2 BL P1 P2     ← layer 2: session
   └── male ──┘└─ female ┘└── male ──┘└─ female ┘  ← layer 3
   └──────── stim ───────┘└──────── sham ───────┘  ← layer 1: group
```

Applies to **categorical x only** (box/violin/bar/scatter/strip). A 1-D
timeseries keeps the sample index on x; its grouping stays colour + facets.

**Spec.** `Role.X` leaves `SINGLE_ASSIGNMENT_ROLES` — several factors may hold X
— and `PlotSpec.x_layers: list[str]` records the **order**, outermost first, max
3. One membership source (roles), one order source (`x_layers`), reconciled in
one place: `spec.ordered_x_layers(table)` appends X-holders missing from the list
in factor order and drops stale names, so the GUI can assign a role without ever
producing an invalid spec. `validate` refuses more than 3 layers and refuses any
x layer at all for a 1-D or 2-D measure (the existing "the x axis is its
within-observation index" error).

**reduce.** Compose the x positions once, above the renderer split — same
principle as `plan_layout`: pure, label-only, replayable by codegen and testable
without frames.
- `__x` becomes the leaf category (the ordered tuple of layer values), and
  `x_order` the flat leaf sequence in nested declared-level order.
- `ResolvedPlot.x_groups: list[XGroup]` with `(label, depth, start, end)` — spans
  in leaf-position coordinates — so renderers draw brackets and headers without
  re-deriving anything.
- Separation between higher-level groups comes from **spacer categories**
  inserted into `x_order` (unique zero-width labels). Pragmatic on purpose: it
  keeps a categorical axis, so every trace type (box, violin, bar, strip) and
  both renderers behave identically. Numeric positions with manual offsets would
  give finer control but change how each trace type positions itself.

**Renderers.** Leaf labels are tick text; each higher layer is a centred label
plus a bracket under its span, computed from `x_groups` — plotly annotations +
shapes, mpl `ax.text` + `ax.plot` in axes coordinates. Both read the same spans,
so they cannot disagree.

**codegen.** Emit the combined leaf-key column and an explicit `order=[…]`
reproducing `x_order` exactly, plus the annotation loop from `x_groups`. Emitting
the *resolved* order rather than re-deriving it in generated code is what keeps
export equal to preview.

This is the largest stage: it touches `spec`, `roles`, `reduce`, both renderers,
`codegen` and the GUI. Worth splitting into 6a (spec + reduce + `x_groups`, with
golden-`ResolvedPlot` tests and no rendering) and 6b (the two renderers + codegen)
if it runs long.

**Tests** — `scistackplot/tests/test_x_nesting.py` (new): three layers produce
the expected leaf order and spans; a layer whose factor was filtered to one level
collapses without leaving an empty bracket; >3 layers refuses; a 1-D measure
refuses. Renderer tests assert bracket/annotation counts; a codegen test executes
the generated source and compares its tick labels to `x_order`.

---

## Stage 7 — `where=` push-down on export (todo 2, part B)

New `scistackplotdb/filters.py`: `PlotSpec.filters` → scidb source text.

| Filter target | Emitted as |
|---|---|
| schema key that is *also* an ITERATE key | `subject=['01','02']` (the iteration kwarg already restricts, and reads better) |
| any other schema key | `where=schema_key('subject').isin(['01','02'])` |
| a joined group/measure variable's value | `where=(Speed > 1.0)` / `Variable.isin([…])` |
| `ColName`, `Variant`, a `LevelGroup` | **not expressible** — stays in the function body as today's `df[df[…].isin(…)]`, and the docstring says which filter stayed behind |

Clauses combine with `&`. The emitted snippet grows `from scidb import schema_key`
when it needs one.

**The test that matters** — `scistackplotdb/tests/test_filter_pushdown_parity.py`,
modelled on `test_fanout_parity.py`: per filter kind, resolve interactively
(pandas) and load through the translated `where=` against the same database, then
compare the figure sets. A push-down selecting a *different* record set than the
preview produces a wrong figure that looks fine, and it would be invisible in the
generated code.

---

## Ordering and independence

```
Stage 1 (keys + filters GUI) ─┐
Stage 2 (navigator + iterate order/ancestors) ─┤ independent, ship either first
Stage 3 (variant rows over any variable) ──► Stage 4 (secondary y axis)
Stage 5 (group factors) ──► Stage 6 (nested x layers)
Stages 1 + 5 ──► Stage 7 (where= push-down)
```

## Test commands (one package per invocation)

```
pytest scistackplot/tests
pytest scistackplotdb/tests
pytest scistack-gui/tests/test_plot_service.py
```

Frontend, after any `.tsx` change — **both** vite targets, or the fix is dead
code (`project_frontend_bundle_rebuild`):

```
cd scistack-gui/frontend && npm run build && npx tsc --noEmit
```
