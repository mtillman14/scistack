# Plan: nested grouped bars, declared level order, and a DAG picker for groupings

*Drafted 2026-09-15 after `1376524e Use Variables as factors`; revised the same
day against `scidb.log` (11:16–12:33), which turned two guesses into measurements
and removed one guess entirely.*

Project: schema `[subject, session, speed, trial, cycle]`, measure
`GAITRiteLoaded`, grouping `Demographics.InterventionGroup` (16 subject-level
records, 18 columns).

---

## What the log settles

1. **The nested plan is being built correctly and the failure is at the plotly
   edge.** 12:33:10 shows
   `x_layers: ["session"] -> ["session", "InterventionGroup"]` and
   `roles={… 'session': 'x', 'InterventionGroup': 'x' …}`, then
   `resolved bar … 2 panel(s), 38 row(s)` and `render_plotly: TOTAL=0.001s`.
   So `plan_x_axis` ran, spacers and brackets were composed, and the figure that
   came out did not look nested. That eliminates "the user never assigned the
   role" as the cause and confirms the renderer defects below.
2. **The layer order is the transpose of what was asked.** `["session",
   "InterventionGroup"]` = session outer, InterventionGroup inner — because
   `setRole(name,'x')` never touches `x_layers` and unknown holders are appended
   last.
3. **`groupable_report` costs ~8 s of every panel open, and the demographics
   sheet is not why.** 12:08:46 and 12:31:23:

   | variable | `groupable_columns` |
   |---|---|
   | `Demographics` (18 cols, the one that matters) | **0.078 s** |
   | `FilteredDelsys` | **4.204 s** |
   | `RawEMG` | **5.437 s** |

   Those two offer *nothing* — their columns are muscles holding signals, and
   `_groupable_columns` discards them silently. It discards them **after**
   `sample_column_value(db, variable, column)`, i.e. after pulling a whole EMG
   trace out of DuckDB per muscle to classify it and throw it away. So the
   scalability problem is not checkbox count (12 offered, 6 refused — a
   tolerable list); it is **one full-signal value query per column of every wide
   variable, on every panel open**.

That reframes Part 2: moving the work behind a click is not sufficient on its
own, because clicking `RawEMG` in the new picker would pay the 5.4 s to be told
there is nothing there. The classification itself has to get cheap.

---

## Part 1 — the bars do not nest

### Defects

| # | Defect | Where | Symptom |
|---|---|---|---|
| **1z** | **`_summarize` drops the nested axis's LAYER columns.** It groups the panel frame to centre + error and `reset_index`es only the keys back, so for BAR (and BAND) the layer values are gone by the time `_plan_nested_x` runs. It requires every layer to be present, skips every panel, and composes an EMPTY plan — `x_order == []`, no ticks, no brackets, and `is_categorical_x` false so even 1a's fix does not apply. | `reduce._panel_frame` -> `_summarize` | **The nested-x feature was absent for exactly one kind: bar.** Every existing nested test used box, which does not summarize. Found 2026-09-15 by the Stage 2 tests failing. FIXED with a `carry=` argument; the layer columns are functionally determined by the composed key, so grouping by them splits nothing. |
| **1a** | The plotly renderer never states the category order — it sets `tickmode/tickvals/ticktext` from `x_plan` but no `categoryorder`/`categoryarray`. Plotly falls back to *order of first appearance in the traces*, and the trace order is `_summarize(..., sort=False)` = DB row order. | `render/plotly_.py:_add_axes` | **The `[schema_keys]` declared level order is discarded in the panel — for a flat single-factor x axis too, not only nested ones.** The model half is correct: `ScidbSource._ordered` → `scidb.schema_order.order_levels` → `FactorInfo.levels` → `plan_x_axis(layer_orders=…)`. Only the renderer drops it. |
| **1b** | Spacer categories hold no data by construction, so they appear in no trace and plotly never learns they exist. | same | No gap between clusters: the nested axis draws as one undifferentiated row of bars. |
| **1c** | `_add_x_groups` places brackets in paper coordinates as `group.start / len(plan.order)` — arithmetic that assumes the spacers occupy slots. | `render/plotly_.py` | Every bracket is offset from the bars it names by the number of spacers to its left. |
| **1d** | A newly checked grouping gets no role; `complete_roles` defaults it to FREE and `_summarize` pools it into the means. `roles.validate` refuses pooling only for *variant* factors. | `PlotStudio.tsx:toggleFactorVariable` | Checking a grouping changes the figure not at all, silently. (Not what the user hit — the log shows they found the role themselves — but still a trap.) |
| **1e** | `setRole(name,'x')` does not write `spec.x_layers`; the GUI's `xLayers` memo appends unknown holders last. | `PlotStudio.tsx:1167` | A new layer lands **innermost** → the transpose of the request, fixable only by finding the ↑ arrow. |

The matplotlib (save) path is correct throughout, because
`render.base.x_positions` maps each value to its index in `resolved.x_order`.
So the panel and the saved PNG currently disagree, which is its own bug.

### Decision on nesting vs dodging (user: "pick whichever is most robust")

**Keep the nested axis.** It is already built, already tested
(`test_x_nesting.py`), already correct in the model and in matplotlib, and is
what `x_layers` *means*; Stage 1 is a one-property fix to the renderer, not a new
layout mode. The dodged alternative (`x = InterventionGroup`, `color = session`,
plotly's own bar grouping) also already works today via the role dropdowns and
needs no code — so both readings remain reachable and neither is being built
from scratch.

### Stage 1 — state the category order (`scistackplot/render/plotly_.py`)

When `base.is_categorical_x(resolved)`, emit `categoryorder: "array"` and
`categoryarray: resolved.x_order` on each panel's x axis. `x_order` is already
`x_plan.order` for a nested axis and the declared level order for a flat one, so
**one change fixes 1a, 1b and 1c together**: spacers get real slots,
`len(plan.order)` becomes true again, and the brackets land. Also state
`barmode: "group"` plus `bargap`/`bargroupgap` instead of inheriting plotly's
defaults — the house rule already applied to `orientation: "v"` beside it.

**Tests** (`scistackplot/tests/test_render.py`, `test_x_nesting.py`):
- a categorical panel's `categoryarray` equals `resolved.x_order` **including
  spacers**, and the key is **absent** on a numeric axis (1-D index, x-measure);
- a declared order (`["BL","POST","FU"]`) reaches `categoryarray` even when the
  frame's row order disagrees — the direct regression test for 1a, built on a
  frame deliberately shuffled;
- a flat single-factor categorical x also gets `categoryarray` (1a is not
  nested-only);
- brackets: for a 2-layer plan, each annotation's paper `x` falls inside the
  span of the leaves it covers, computed against `len(plan.order)`;
- **parity**: for one `ResolvedPlot`, the mpl tick-label order and the plotly
  `categoryarray` (spacers dropped) are the same list — the two paths disagreeing
  silently is precisely how this shipped;
- `barmode`/`bargap` present on a bar figure and absent on a line figure.

### Stage 2 — x layers default to the data's own nesting (`scistackplot`)

`FactorInfo` gains `depth: int | None` — a schema key's index in
`table.schema_levels`, a factor variable's `len(variable.levels)`, `None` for
variants and derived buckets. `PlotSpec.ordered_x_layers` orders layers the user
has **not** explicitly reordered by ascending depth, so a subject-level
`InterventionGroup` is drawn *outside* `session` with no arrow pressed. An
explicit `x_layers` still wins; ↑/↓ still write it.

`capability`'s `grouping` report gains `layers` (the resolved order) and
`PlotStudio`'s local `xLayers` memo is **deleted** in favour of reading it — the
rule then lives once, in Python, where it has a test (same reasoning as
`axis_node_bindings`, `variant-axis-node-binding.md`).

**Tests** (`scistackplot/tests/test_x_nesting.py`, `test_spec.py`):
- two factors at different depths order outer→inner by depth regardless of the
  order the roles dict holds them in;
- an explicit `x_layers` overrides depth entirely;
- a depth-less factor (a variant, a `LevelGroup` bucket) sorts after the
  depth-bearing ones, stably;
- `capability.grouping["layers"]` equals `spec.ordered_x_layers(...)` for the
  same spec — the anti-drift test for the deleted memo.

### Stage 3 — an applied grouping gets a role, and says which

Applying a grouping in the new picker assigns: **X** if the measure's x axis is
groupable and `len(x_layers) < MAX_X_LAYERS` (placed by depth per Stage 2); else
**COLOR** if nothing holds it; else **FREE**, with the row saying "pooled into
the means — give it a role in Factors". Checking a grouping is a statement about
the data (unlike clicking "+" on a variant, which is not), so acting on it is
right; silently pooling is the worse failure. The chosen placement is shown on
the row ("x axis, outside `session`") so nothing is a surprise.

**Tests** (`scistack-gui/tests/test_plot_service.py` for the rule, in Python):
the rule is a function in `scistackplot.roles` (`role_for_new_grouping(spec,
table)`), not TSX — x when there is room, colour when x is full or unavailable
(1-D measure), free when both are taken, and never overwriting an existing role.

---

## Part 2 — grouping selection: cost and UI

### Stage 4 — classify columns by DuckDB type, not by sampling a value

The 7.9 s. `data_columns_for` already reads `information_schema.columns`; widen
it (or add `data_column_types_for`) to return `(name, data_type)`.
`_groupable_columns` then rejects any array/struct/blob type — `DOUBLE[]`,
`DOUBLE[][]`, `VARCHAR[]`, per `docs/claude/duckdb-column-types.md`'s
"DuckDB column type = cell value type" rule — **before** any value query.
`sample_column_value` survives only for scalar-typed columns, where it is cheap
and still needed to separate numeric from categorical.

This is a scistack-layer fix (CLAUDE.md NOTE 3) and it is worth having
independently of the UI work: it makes `RawEMG`/`FilteredDelsys` cost one
schema read instead of 13 signal reads, whoever asks.

**Tests** (`scistackplotdb/tests/test_factor_variables.py`):
- a variable with `DOUBLE[]` columns is refused **without** `sample_column_value`
  being called (spy/monkeypatch asserting zero calls) — the regression test for
  the 5.4 s;
- a scalar-typed wide table still offers/refuses exactly what it does today
  (the existing assertions must not move);
- a `VARCHAR[]` column is refused the same way as `DOUBLE[]`.

### Stage 5 — split the report (`scistackplotdb/source.py`)

- `groupable_variables(measure)` — candidates by schema prefix + column types +
  the cached `_shape_of` (already paid by `describe()`'s catalog). Returns per
  variable: `whole` (single-column variables offerable as they are, with levels),
  `column_count`, `has_groupable_columns`, or a refusal reason. **No value query
  and no DISTINCT.**
- `groupable_columns(measure, variable)` — today's `_groupable_columns` for one
  variable, still under its `Log.timer`.
- `groupable_report` stays as the composition of the two: `groupable_with` is the
  library API, and the CSV source and existing tests use it unchanged.

**Tests**: `groupable_variables` issues zero `column_levels` **and** zero
`sample_column_value` calls; its per-variable offers/refusals agree with
`groupable_report`'s; `groupable_columns` reproduces all three refusal reasons
verbatim; composing the two equals `groupable_report` (the anti-drift test).

### Stage 6 — pin the grouping variable to a variant (user request)

Today `_attach_factor_variables` merges
`right[[*on, factor]].drop_duplicates(subset=on)` over **all** variants of the
grouping variable: first row wins, silently.

- `FactorVariable` gains `variant` — a selection keyed by frame column, exactly
  like `VariantSet.selection`, so it stays plain data for JSON-RPC and codegen
  (`plot-variant-rows.md` § "Selection keys are frame columns"). `get_table` is
  memoized on `tuple(factor_variables)` (`sources/base.py:158`), so it is stored
  as a tuple of pairs (list values → tuples) in `__post_init__` with a
  `selection` property returning the dict.
- **Default is latest, spelled the way the Variants section spells it**:
  `variants.default_selection` against the *grouping variable's* own variant
  table — which pins the boolean `CodeIsLatest` flag, never the string
  `"latest"`. That is not a stylistic choice: a bool is not a latest-axis, so it
  survives any other pin placed beside it, where the string silently becomes
  "highest ordinal globally" and drops every location never re-run
  (`plot-variant-rows.md` §2 and its
  `test_pinning_a_param_does_not_turn_latest_global`).
- Applied by running the grouping variable's frame through
  `scistackplot.variants.variant_set_mask` — the same one definition the measure
  uses — before the merge.
- **The residual ambiguity becomes loud**: after pinning, if any location still
  holds more than one distinct value for the factor column, `Log.warn` names the
  variable, the count and that the first was taken. A pin need not resolve to
  exactly one record, and this is the diagnostic CLAUDE.md NOTE 2 asks for.

**Tests** (`scistackplotdb/tests/test_factor_variables.py`):
- two variants of a grouping variable disagreeing on `InterventionGroup`: the
  pin decides which reaches the figure, and the two pins give different figures;
- the default pin selects the latest and is the **flag**, not the string
  (assert the selection's key/type directly — the trap is invisible otherwise);
- a pin beside another pin does not turn the latest flag global (the
  `test_pinning_a_param_does_not_turn_latest_global` analogue for groupings);
- an unpinnable/ambiguous leftover emits the warning and still attaches;
- `FactorVariable` is hashable with a pin and `get_table` memoizes on it (two
  identical pins = one query; two different pins = two);
- spec round-trip with the pin through `to_dict`/`from_dict`, and the bare-string
  back-compat form still parses.

### Stage 7 — GUI backend (`scistack-gui`)

- `describe` calls `groupable_variables`, ships `groupable_variables` /
  `groupable_refused`, and **stops shipping a per-column list**. Checked entries
  need no offer list to render: they are already factors in the resolved table,
  so labels and level counts come from `capabilities`.
- New read-only handlers beside `plot_variant_graph`:
  `plot_grouping_graph(variable)` (which variable nodes may group this measure,
  and why the rest may not) and `plot_grouping_columns(variable,
  group_variable)`. Registered in `server.py` + `api/plot.py`, under
  `db_connection`.
- The **variant** half of the picker needs no new RPC: `plot_variant_graph`
  already answers for any variable and already returns `default_selection`.
- `_table_for` builds `FactorVariable`s with their pins.
- INFO logs: candidate count on describe **and that no column query ran**; one
  line per `plot_grouping_columns` naming the variable and column count.

**Tests** (`scistack-gui/tests/test_plot_service.py`):
- `describe` on a project with a signal-bearing wide variable performs **zero**
  `column_levels`/`sample_column_value` calls — the end-to-end pin on the 8 s;
- `plot_grouping_columns` returns the same offers/refusals as
  `groupable_report`'s column half for that variable;
- `plot_grouping_graph` marks a too-deep variable refused with its reason.

### Stage 8 — frontend (`scistack-gui/frontend`)

The popup is the Variants picker's **two-step flow**, which is what the request
describes:

1. **"Which variable?"** — click a Variable node. Refused ones are drawn with
   their reason, never omitted.
2. **"Which columns, and which variant?"** — the canvas becomes the *variant*
   canvas for the chosen grouping variable (parameter checkboxes, function
   version dropdowns, defaulting to latest), and a **dedicated sidebar section**
   lists that variable's columns as checkboxes with level counts, refusals
   beneath them, and a running readout of everything ticked.

Checks accumulate across variables in one visit; Apply writes
`spec.factor_variables` (with pins) and assigns roles per Stage 3.

Mechanics: extract the modal + canvas shell out of `VariantDagPopup.tsx` into
`DagPicker.tsx` (pipeline+layout fetch, dagre, Escape, ReactFlow with the inert
node overrides, footer). `GroupingDagPopup.tsx` is written on it and reuses
`VariantSelectionProvider` wholesale for step 2. A copy of the shell would drift;
extracting it before the second caller exists is cheaper than after.

`PlotStudio`'s Grouping section: the auto-populated checkbox list is **replaced**
by a button (`Group by: InterventionGroup` / `Group by…`), plus one row per
current grouping showing its variant pin, its assigned role and an ✕. Buckets
(`LevelGroupEditor`, `BucketAdder`) and `XGrouping` stay.

**Tests**: the existing "variant-mode components never reference `callBackend`"
test is **not** extended to this popup — it legitimately fetches columns. The
narrower property that test was really protecting is asserted instead: the
grouping popup calls only read RPCs and never execution-mutating ones
(`hide_constant_value` and friends). Plus a `locationSelection.test.ts`-style
unit test for the accumulate-across-variables selection reducer.

### Stage 9 — export parity

`scidb.Variant` explicitly accepts a `ColumnSelection` (`variant.py` arg docs),
so a pinned column grouping exports as
`Variant(Demographics["InterventionGroup"], fn=…, code_version="latest")`.
`endpoint.variant_expression` already builds the `Variant(...)` call; it wraps a
`ColumnSelection` expression instead of a bare type name. `codegen.group_param`
and the `_preamble` merge are unchanged apart from the pin.

**Tests**: a case in the `test_fanout_parity` family — a pinned categorical
column factor on COLOR, rendered through both the interactive and the generated
path, figures compared; plus a codegen golden for the pinned expression and for
the unpinned one (which must not grow a `Variant(...)` wrapper).

---

## Not in scope

- Numeric columns as groupings (binning `Age`) — still refused with its reason.
- Any change to `for_each` / processing-side `where=` grouping.
- Date-valued columns being offered as groupings (`Demographics.DateOfBaseline`
  and friends pass the ≤50-levels test today). Noise, not a defect; left alone.
