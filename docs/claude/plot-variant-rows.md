# Named Variant Rows, and the Variant-Selection DAG

> **Roles changed 2026-09-17.** `X / COLOR / AGGREGATE / FREE` are gone; the model is now a Grouping list (innermost first, one coloured layer) plus `FACET / ITERATE / COLLAPSE`, with a nested collapse chain. Read `docs/claude/grouping-and-collapse.md` first; role names below are historical.

> Status: **built 2026-09-08; substantially revised 2026-09-11 (stages 1-8 of
> `.claude/plan-default-variant-selection.md` and
> `.claude/plan-plot-studio-variant-axis-fixes.md`), tests passing, uncommitted.**
>
> Three rules here were **reversed** on 2026-09-11 and the old reasoning is
> recorded beside each so nobody restores it by accident: what a table opens on
> (§1), whether per-location "latest" is reported as pooling (§3), and how an
> axis finds its node (§5). Also: the section is called **Variants** — it was
> briefly renamed "Series" in the 26.09.09 multi-variable work and renamed back,
> because one row is one variant.
> Plan: `.claude/plan-plot-variant-rows.md`. Prerequisite reading:
> `variant-selection.md` (what a variant *is*, and why `Code:<fn>` columns
> exist), `plotting-library-design.md` (roles, the `PlotSpec`). Companion:
> `variant-axis-node-binding.md` (which canvas node supplies an axis — read it
> before touching anything in §5).
>
> This is the GUI half of `variant-selection.md` §5, plus the one thing that
> document left open: how a variant figure is written **as code**. Four things
> here are non-obvious enough to be worth reading before touching any of it.

## 1. What a variant row is

The Plot Studio's Variants section holds **named rows**. One row is one
`scistackplot.VariantSet` — a label and a selection:

```python
VariantSet(name="baseline", selection={"Code:bandpass": "v1"})
VariantSet(name=None,       selection={"bandpass.low_hz": ["20", "50"]})
```

- **One row is a pin.** The figure shows that variant and nothing else. This is
  what a table opens on (`default_spec` seeds one row named `current`), which is
  why the section always has something to edit rather than starting empty.

  **A plot opens on exactly ONE variant, with every axis pinned** — not just the
  code ones. `variants.default_selection` owns the rule: code axes to the latest
  body (via the per-location flag), branch-param axes to the first level in
  declared order. Until 2026-09-11 the opening row carried only the source's
  `default_pin` — the `CodeIsLatest` flag — which left a swept parameter
  unanswered, so `default_roles` found a multi-level variant factor with no role
  and put it on COLOUR. A variable produced at five filter cutoffs opened as
  **five overlaid series** before the user had said anything. Plotting one thing
  is the common case; comparing is what a second row is for.

  The pin is applied **blindly**: real data is ragged, so (latest code) x (first
  value) may be a combination nobody ran, and the figure comes out empty. That
  is deliberate — a rule that quietly picks a different value to avoid an empty
  figure is no longer a rule anyone can predict — and it is paid for by the
  empty state explaining itself (§3).

  `default_pin` still exists and still means what it always did: the selection
  the SOURCE recommends, i.e. which rows are *current*, which only scidb can
  know. `default_selection` seeds from it and finishes the job.
- **Two or more rows are a comparison.** They collapse into a synthetic
  `Variant` factor whose levels are the names, and it takes a role — colour,
  facet, separate figures — like any other factor. That is the whole feature:
  "v1 against v3" becomes one figure instead of two.
- **`name=None` means "call me whatever my selection says"** (`auto_label`:
  `bandpass v1 · low_hz=20`). The GUI keeps it None until the user types over
  it, so the label stays true while the selection is still being edited.

`VariantPolicy.PIN` and `PlotSpec.pinned_variant` **no longer exist**. Pinning
is what a one-row `variant_sets` *is*; a policy meaning "obey the rows" beside
rows that already say what to keep was two switches for one decision, and the
state where the policy said `facet` while a row said `v1` had no defensible
meaning. `POOL` survives, as a checkbox, answering the only question left: what
happens to variant factors *nothing* selected.

### Selection keys are frame columns, not scidb objects

`selection` is keyed by column (`"Code:bandpass"`, `"bandpass.low_hz"`), never
by a `scidb.Variant`. Two reasons, both hard constraints:

- a spec round-trips through JSON-RPC **and** through a generated docstring
  (`codegen.extract_spec`), so it has to be plain data;
- `scistackplot` must keep working with **no scidb installed** — the CSV source
  depends on that.

Translating a `scidb.Variant` into these keys is therefore
`scistackplotdb.variant_set`, in the layer that knows both vocabularies.

## 2. The two resolutions of "latest" — the subtlest thing here

A code axis may be selected as `"latest"` rather than a named ordinal, and it
resolves **against the data**, in one of two ways
(`scistackplot.variants.resolve_selection`):

| situation | resolves to | why |
|---|---|---|
| every code axis in the row says `latest` | the per-row `CodeIsLatest` flag | Per **schema location**. A subject nobody re-ran keeps contributing its own newest record instead of vanishing from the figure. |
| the row also pins a named ordinal somewhere | the **highest ordinal present** on the remaining axes | The flag is unusable once something is pinned to old code — a `v1` record is by definition not the latest. |

The second case *does* drop locations that never ran that ordinal. That is
acceptable only because the user already asked for it by naming a version, and
it is never silent: the GUI shows the substitution as `latest (v3)`.

Getting the first case wrong is how subjects disappear from a figure with no
error anywhere. `"latest"` is **not** a synonym for "the highest ordinal", here
or in `scidb.Variant(code_version="latest")` — same rule, same reason.

### The trap this sets for anything that pins alongside a code axis

Read the table above again with one question in mind: *what makes a selection
"the first case"?* `resolve_selection` decides with

```python
pinned_elsewhere = any(k not in latest_axes for k in present)
```

— **any** other key at all, not just another code axis. So adding a branch-param
pin beside `Code:f = "latest"` silently moves the selection into the second row
of that table, and the code axis stops resolving through the per-location flag
and becomes the global highest ordinal. Every schema location never re-run under
the newest code drops out of the figure, with no error.

This is exactly why `default_selection` pins the **boolean flag**
(`{CodeIsLatest: True}`) and never the string `"latest"`, even though the string
reads better: a bool is not a latest-axis, so `latest_axes` stays empty, the
selection is returned unchanged, and the per-location meaning survives whatever
else is pinned beside it. `test_pinning_a_param_does_not_turn_latest_global`
exists to catch a well-meaning edit that "tidies" this into the string form.

## 3. What leaves the factor list, and what an unfilled row does

Once "baseline" *means* `Code:bandpass == v1`, `apply_variant_sets` removes
`Code:bandpass` from the factor list. This is not tidying:

- keeping it states the same thing twice, and
- with two rows the leftover column has two levels and no role, so
  `roles.validate` refuses the figure — **rejecting exactly the comparison the
  user just asked for.**

`_answered` decides this, and the rule differs by axis kind:

- **Code axes belong to the Variants section, entirely.** Once *any* variant is
  defined, every `Code:<fn>` column is answered — whether the variant named a
  version, asked for `latest`, or selected the chain-wide `CodeIsLatest` flag.
  "Which version of the code" is the question the rows exist to answer, and
  offering it again in Factors asks the user to decide the same thing twice in
  two places with no way to know which wins.
- **Branch params are answered only when *every* variant answers them** — an
  intersection. Nothing about "current code" decides which filter cutoff to
  plot, so a variant that leaves `low_hz` open still owes the user a decision.

Because code axes leave unconditionally, a variant whose rows were built by two
versions is pooling code silently. `spanned_code_axes` catches that and reports
it — the fix is on the row (pin it, or split it), not in Factors.

### The latest flag is no longer exempt (reversed 2026-09-11)

**What this document used to say**, and what the code used to do:

> A selection resolving through the latest flag is never counted: spanning
> ordinals across locations is what per-location "latest" *means*, and warning
> about it would cry wolf on the most ordinary state there is.

That argument is right about **frequency** and wrong about **consequence**. The
state it stayed silent about is a figure whose points were computed by different
bodies of the same function — which a reader cannot see and must not have to
assume away. The user's call: if the body actually used differs between schema
locations, say so, prominently.

What makes that tolerable rather than noisy is that the report names **which
locations hold which version**. `spans` is no longer `{column: count}` but
`{column: {function, versions, locations, schema_levels, truncated}}`, and
`describe_span` turns it into one sentence shared by the `Log.warn`, the row tag
and the figure banner — "v1 (02, 03); v2 (01)" is something you can act on or
dismiss at a glance, where "pools 2 versions" on the commonest state in the
system is not.

**When does this actually fire?** Only across schema locations —
`is_latest` is resolved per location (`provenance_query.py`, `for bucket in
peers.values()`), so within one location exactly one chain wins and a single
subject can never contribute two versions. Two versions under "current" means
either a **partial re-run** after a body edit, or **staggered processing**:
subjects 1-3 processed, the function improved, subjects 4-6 processed. Nobody
failed to re-run anything in that second case — the dataset simply accumulated
across a code change, which is the normal path in a study collecting data over
months, and it is the case that looks exactly like a correct figure.

In a uniformly re-run project every location is on one version, `nunique() == 1`,
and nothing fires.

**It is not a duplicate of the canvas turning red.** Usually the producing node
does go red — `check_node_state` derives its expected set from the current
function hash — but not always, and the banner deliberately does not point at
the canvas. A partially re-run `PathInput`-only loader reads **green** while
producing precisely this state
(`.claude/plan-pathinput-loader-staleness-gap.md`), so the banner is the only
signal in the one case where it matters most.

### The empty figure explains itself

The counterpart to pinning blindly (§1). `variant_summary` reports, for a
defined row that matched nothing, `resolved` (what the selection actually became
— `latest` resolves to the flag, so it is not the same as `selection`) and
`available` (combinations that DO have records, scoped to the row's own
variable). The panel draws that **in place of the figure**, each combination a
button that adopts it.

Computed only when `row_count == 0`: on the common path it is pure cost, and
"what else is there" only matters when the answer to "what did I get" is
nothing.

### An unfilled row is inert

A row with an **empty selection** is skipped everywhere: `defined_sets` filters
it out of `apply_variant_sets`, `_answered`, and `codegen`. It claims no rows,
contributes no level, and decides nothing.

> **Narrowed 2026-09-11.** This used to be the state "+ Add variant" created on
> every click. It no longer is: "+" opens a two-step picker (§5) and the row is
> built on Apply, already pinned to one variant. The rule survives as the net
> for a **cancelled or half-finished** selection, not as the normal opening
> state of a new row — so the reasoning below is about why an empty selection
> must stay inert *if one occurs*, not about what clicking "+" does.

This is load-bearing, not politeness. Treating an empty selection as "all
variants" (which is what it means once applied) meant clicking "+" changed the
figure before the user had said anything, *and* un-answered the code axis for
every other row — dropping `Code:<fn>` back into Factors with a pooling error
attached. Clicking "+" is not a statement about the data.

The row still appears in the sidebar, labelled `(not set)` with a grey tag
(grey, not amber: "not yet said" is a state, not a problem — the amber "no data"
tag means a *defined* selection matched nothing, which is a real warning). If
every row is unfilled, that is the same as having none: the code axis returns to
Factors and the pooling guard fires, because now nothing has been selected at
all.

Rows matching no selection are dropped; a row matching several goes to the
**first** (overlapping selections are legal, and duplicating a row would
double-count it in every mean).

### Stale roles

Answering a column can strand a role that named it — `default_roles` puts a
multi-level `Code:<fn>` on colour, and the opening "current" variant then
answers it; or the user facets by a code axis and then pins it. `validate` would
call that an *unknown factor* and refuse to draw anything. Two defences:
`default_spec` derives its roles from the **resolved** table so the bad role is
never created, and `strip_answered_roles` drops one arriving from a saved spec.
Only names that *were* real factors are dropped — a typo still errors.

One related default: a multi-level `Variant` factor with no explicit role gets
**COLOR**, not FREE (`complete_roles`). FREE is not the conservative choice
there — it overplots two variants the user has just gone to the trouble of
naming, and `validate` would reject it a moment later anyway, so the honest
alternatives are "colour it" or "show an error instead of the figure".

## 4. Writing the same figure as code

Everything the GUI does must be writable by hand, so a variant figure has to
survive export. Three verified facts set its shape:

- **`as_table` frames carry schema keys and data columns only**
  (`scifor/foreach.py:1507`). Branch-param and `Code:<fn>` columns are
  `scistackplotdb.attach_variants`' doing — a *plotting-layer* construct that
  never reaches an endpoint. So an endpoint **cannot** receive one `df` and sort
  the variants out of it.
- **An empty `as_table` frame is valid and does not skip the combo**
  (`scifor/foreach.py:1591`), so a location that only ran `v1` still renders,
  with the absent variant contributing zero rows — the same behaviour as the
  panel.
- **List-valued branch params already mean membership**
  (`scidb/database.py:133-146`), so the popup's multi-checkbox selection is
  `low_hz=["20", "50"]` with no new scidb work.

Hence **one input per variant**, labelled and stacked inside the generated
function:

```python
def plot_step_length(baseline, new_filter, filename):
    df = pd.concat([
        baseline.assign(**{"Variant": "baseline"}),
        new_filter.assign(**{"Variant": "new filter"}),
    ], ignore_index=True)
    ...

for_each(
    plot_step_length,
    inputs={
        "baseline":   Variant(StepLength, fn="bandpass", code_version="v1"),
        "new_filter": Variant(StepLength, fn="bandpass", low_hz=["20", "50"]),
        "filename":   PathOutput("plots/step_length_{subject}.png"),
    },
    outputs=[StepLengthFigure],
    as_table=["baseline", "new_filter"],
    finalized=True,
    subject=[],
)
```

The rule tying the interactive and pipeline halves together:

> **A list of variants inside one plot is one figure with a `Variant` factor;
> `EachOf` of them is one figure each.**

Same `Variant` object either way — written once, meaning the same thing in a run
and in a figure. There is deliberately no second control for "one figure per
variant": that is the `Variant` factor in the `iterate` role.

Mechanics worth knowing:

- `codegen.variant_params` makes identifier-safe parameter names from labels
  (`"20 Hz + latest"` → `v_20_hz_latest`) and keeps the display label for the
  `assign`.
- `endpoint.variant_expression` is the inverse of `variants.selection_for`. A
  selection spanning two producing functions **nests** —
  `Variant(Variant(X, fn="loadEMG", code_version="v1"), fn="bandpass", low_hz="20")`
  — because one `Variant(fn=…)` cannot cover two functions, and
  `**{"__code__.loadEMG": "v1"}` would leak a reserved namespace into code the
  user is meant to edit.
- The `CodeIsLatest` selection key becomes `code_version="latest"`: scidb spells
  the same per-location rule the same way.
- `generate_script` (the standalone CSV path) splits the one frame with literal
  pandas instead, and says in a comment when a `"latest"` selection cannot be
  honoured — a flat table carries no provenance to resolve it against.

## 5. The popup: same widgets, opposite meaning

`VariantDagPopup` draws the pipeline canvas in a modal. Parameter nodes offer
their recorded levels as checkboxes; function nodes offer their recorded
versions as a dropdown defaulting to `latest`.

**The trap it is built around.** On the canvas, a `ParameterNode` checkbox is
*execution* state — unchecking a value excludes it from future `for_each`
fan-outs (`pipeline_store.hide_constant_value`). In the popup the identical
widget is *display* state. Binding one to the other would make looking at a plot
quietly rewrite the run configuration.

Three things keep them apart, and the third is stronger than the design asked
for:

1. `VariantSelectionContext` carries the mode. Present → selection; absent (the
   canvas, always) → nothing about existing behaviour changes.
2. A test asserts the variant-mode components never reference `callBackend`
   (`scistack-gui/tests/test_plot_service.py`).
3. **The branch is at the component boundary, not inside a component body** —
   `FunctionNode` returns either `VariantFunctionNode` or
   `PipelineFunctionNode`. This was forced by a crash, not chosen for elegance:
   `PlotRoot` mounts the panel with **no providers**, so `useScope`,
   `useRunLog` and `usePlanRun` throw the instant the popup draws a function or
   pipeline node, and hooks cannot be skipped inside a component. The upshot is
   that the popup mounts *only* selection state and the canvas *only* execution
   state.

`PipelineNode` is replaced outright by an inert stand-in for the same reason;
its insides are a different scope anyway. So are `VariableNode` and
`PathInputNode` — see "what is inert and what is not" below.

### An axis finds its node by PORT, never by name (fixed 2026-09-11)

Full write-up: **`docs/claude/variant-axis-node-binding.md`**. The short version,
because it caused two user-visible bugs at once:

`VariantAxis.param` is the producing function's **argument** name (`config`, from
scidb's `fn.param` branch-param key). A `ParameterNode`'s label is the
**Parameter entity's** name (`delsys_config`). The popup used to match those two
strings. They agree only until someone renames a Parameter or feeds a function
port from a glue node — after which the axis vanished from the dialog, its node
dimmed to "not a variant here", *and* it was listed under "defined in a nested
pipeline", which was simply a wrong guess at the cause.

The binding is now `plot_service.axis_node_bindings`, keyed on the edge's
`targetHandle` (`param__<argument name>` — exactly `VariantAxis.param`, on both
the DB-derived and the manual edge paths). It lives in Python, not the webview,
because it is a rule about what scidb's namespacing means and because a rule in
TSX has no test. The popup receives `node_bindings` on the variant graph and
reads it back.

Binding by port means **the node type stops mattering**: whatever is wired into
`filterDelsys`'s `config` argument supplies that axis, Parameter or glue node or
anything wired there later.

### What is inert, and what is not

Before this, every node that *could* hold an axis was dimmed (the name match
failed) while `VariableNode`, `PathInputNode` and `GlueNode` rendered as their
canvas selves — undimmed and looking interactive. That is backwards: a dimmed
node is saying "I do not distinguish these records", and a Variable node is
exactly that.

| node | in the popup |
|---|---|
| Parameter, Glue | the axis bound to its port, or inert with "not a variant here" |
| Function | its recorded versions, or inert when it has only one |
| Variable, PathInput | **always inert**, with the reason |
| Pipeline | inert stand-in; its insides are another scope |

Variable/PathInput/Glue are overridden in the popup's `nodeTypes` rather than
given a branch inside the shared component. Same boundary rule, and it keeps
variant-only rendering in the popup file instead of spreading through the canvas
components.

### Adding a row: two questions, on the canvas

"+ Add variant" does not append a row. It opens the canvas on **"Which
variable?"** — any plottable variable, not only the one being plotted, which is
what makes overlaying `RawEMG` on `FilteredEMG` reachable at all. A variable that
cannot stack is **drawn with its reason** rather than omitted
(`ScidbSource.stackable_report`, whose refusals were computed from the beginning
and only ever logged). Then the variant step, on the same canvas. The row is
created on Apply, already pinned by `default_selection` — so a new row is one
variant, the same way the panel opened on one.

### Why the whole pipeline, not the relevant subgraph

`variant-selection.md` §5 proposed the induced subgraph (ancestors contributing
more than one level). The whole canvas won because it is a graph the user
already knows, and because it makes the *absence* of a control informative: a
dimmed node is saying "I do not distinguish these records". Nodes with no axis
render inert with a tooltip rather than offering controls that would do nothing.

### The default selection contradicts an explicit version

A row opens on `{CodeIsLatest: true}`. Adding `Code:f == v1` on top asks for rows
that are simultaneously the newest and the old version — an empty figure, from a
control that looks broken. The popup drops the flag on the first explicit choice
(`withoutLatestFlag`); an explicit choice supersedes the shortcut, which is also
what the user means by making it.

### Axes with no node on this canvas

An axis that binds to no node is listed as plain checkbox rows beneath the graph
— an axis is never unreachable just because of where the popup opened.

The heading says "No node on this canvas", not "defined in a nested pipeline".
Nesting is one reason (the popup opens at the root scope) but it was never the
only one, and while the binding was name-based it was usually the *wrong* one:
every axis whose Parameter had been renamed landed there too, under a heading
asserting a cause that had nothing to do with it. A list that says what it knows
beats one that guesses.

## 6. Where each piece lives

| concern | home | why there |
|---|---|---|
| every recorded version of a function | `scidb.provenance_query.function_versions` | Provenance. `code_version_ordinals` is now expressed in terms of it, so the two cannot disagree about what `v2` means. |
| which axes a variable has, and their origin | `scistackplotdb.load.attach_variants` → `VariableFrame.variant_axes` → `FactorInfo.origin` | Both halves (`Code:<fn>`, `fn.param`) are in hand there. A GUI splitting those strings would re-implement scidb's namespacing one layer away, and break first when it changes. |
| axes + versions for the popup | `ScidbSource.variant_graph` | Reuses the source's frame cache; opening a dialog must not re-read the variable. |
| `scidb.Variant` → selection | `scistackplotdb.variant_set` | Needs both vocabularies; scistackplot must stay scidb-free. |
| what a selection keeps | `scistackplot.variants.variant_set_mask` | One definition, so the GUI's "4 of 24" and the renderer's rows cannot disagree. |
| the rows' data model + counts | `scistackplot.capability.variant_summary` | Adds `sets` (label, auto label, **row_count**) beside the axes. `row_count == 0` is the number that catches real mistakes, and is what gates `available`. |
| what a table OPENS on | `scistackplot.variants.default_selection` | One rule for the panel's first figure and for a row added later, or the two disagree about what "one variant" means. Not in the GUI: a library caller must open on the same figure (CLAUDE.md NOTE 3). |
| which node supplies an axis | `scistack_gui.services.plot_service.axis_node_bindings` | Needs the canvas graph, so it cannot live in scistackplot — but it is still a rule about scidb's namespacing, and a rule in TSX has no test. See `variant-axis-node-binding.md`. |
| what can be plotted alongside, **and why not** | `ScidbSource.stackable_report` | The refusals were always computed and only logged. The picker draws every variable node, so one it cannot offer has to say why in place. |
| one span as a sentence | `scistackplot.variants.describe_span` (+ its TS mirror) | The log, the row tag and the figure banner must not word the same span three ways. |

## 7. Known limits

- **The popup is root-scope only** (§5).
- ~~**A partially re-run `PathInput`-only loader reads green on the canvas**~~ —
  fixed by `state._discovery_gate`, so the canvas is no longer silent here and
  the §3 banner is no longer the only signal. The banner still earns its place:
  it answers a different question (is the data in THIS figure heterogeneous,
  and which location holds which version), and the *staggered processing* case
  — subjects 1-3 loaded, body improved, subjects 4-6 loaded — leaves no files
  unloaded at all, so the gate sees nothing to report while the figure really
  does span two versions.
- **Nothing caches the exploded frame.** A 1-D struct measure re-explodes on
  every resolve (measured: 24 rows → 8.5 M samples, ×356 650). Cancellation and
  building only the visible figure (`reduce.resolve_one`) removed the pile-up
  that turned that into a 30 s transport timeout, but the per-resolve cost is
  unchanged. Caching it properly means memoising on the *derived* table — the
  explode runs after filters and variant selection — which is why it was not
  done blind.
- **`FactorInfo.levels` are computed on the unfiltered frame** and are not
  recomputed after a selection, so a legend may carry a level with no rows.
  Pre-existing behaviour, kept deliberately: it makes level order stable as
  boxes are toggled.
- **MATLAB source is still not captured** (`variant-selection.md` §3), so
  MATLAB versions remain selectable but not inspectable.
- **Re-running an old version is still not possible** — Stage 4b of
  `.claude/plan-variant-selection.md`. Everything here selects among records
  that already exist.
