# Schema Location Picker — plan

> Status: **decisions locked 2026-09-13. ALL SIX STAGES BUILT AND COVERED.**
> Stages 1a-3 have passing Python tests. Stage 6 added 24 frontend unit tests
> (passing) and 3 source-level D5 assertions (written this session, unrun —
> no Python in the agent's environment). `tsc --noEmit` clean for frontend and
> extension; 37/37 extension tests pass; **all three bundles rebuilt**
> (standalone, webview, extension host).
>
> **Never visually checked.** That is the one outstanding gate.
>
> **Stage 1b's storage shape changed during Stage 3** — see "The defect Stage 3
> found". Its tests were updated with it.
>
> Prerequisite reading: `docs/claude/schema-location-status.md` (written first,
> for this: what green means and why four states are as reliable as two),
> `docs/claude/schema-hierarchy-contiguity.md`, `docs/claude/plot-variant-rows.md`
> §1–§3 (variant vocabulary, and the two-controls-one-question trap).

## The feature

Right-click a Variable on the DAG canvas → "View Schema Locations"; the same
popup from the Plot Studio. Scoped to **one Variant of one Variable**. Header:
`green / total` and a single green-or-red verdict. Body: searchable, scrollable,
collapsible tree, collapsed to the top schema level, a status dot and rolled-up
counts per node. Clicking a row plots that location; a checkbox per row adds or
removes that location from what is drawn.

## Decisions (locked)

| | decision | note |
|---|---|---|
| **D1** | Denominator is the **expected set** | `expected_invocations_for_function`; `check_pathinput_node_state`'s should-run set for inputless loaders. See the status doc. |
| **D2** | **Four states** — green / amber / red / grey | Only because amber is redefined as a *pure graph relation* (an input was re-saved). Function-body edits are NOT amber; the per-row version tag carries that instead. Had amber kept `check_combo_state`'s hash comparison, this would have been two. |
| **D3** | **Single select** | Clicking a row selects it exclusively. |
| **D4** | Keys below the selection get **`FREE`**, not `AGGREGATE` | Every lower location's own values are drawn as replicates. No mean ± error unless the user assigns it in Factors. |
| **D5** | **Replaces** Plot Studio's "Schema keys" section | That section becomes one button opening this popup, and holds nothing else. |
| **D6** | One variant at a time | Comparison is already the Variants section's job — `Variant` becomes a factor with a role. |

## The one thing D3 and D5 force

D3 (single select) and D5 (a checkbox per row, "add or remove that location's
plot from the list being shown") are two different interactions, and together
they are **strictly more expressive than `spec.filters` can represent.**

`Filter` is per-column include lists (`spec.py:177`), so it can only express a
**Cartesian product**: `subject ∈ {01,02} × trial ∈ {3,7}`. A tree of
checkboxes implies ragged sets — all of S01, plus only trials 1–3 of S02 — which
no combination of per-column filters can express. Worse, in a *hierarchical* UI
the per-column model is actively misleading: unchecking `S01 / trial 3` would
silently drop trial 3 from every other subject too.

So the checkbox needs a new spec field. Proposed:

```python
@dataclass(frozen=True)
class LocationFilter:
    keys: list[str]                 # schema keys, in schema order
    include: list[tuple[str, ...]]  # PREFIX tuples; ("01",) = all of subject 01
```

Row mask is prefix membership; the checked set is stored as its minimal covering
set of prefixes (all trials of S01 checked → `("01",)`, so adding a trial later
does not silently drop out). Plain data, JSON round-trips, and the CSV source
implements it with the same pandas mask — no scidb dependency.

**Interaction split, which makes D3 and D5 consistent rather than contradictory:**
clicking a row label = "only this" (checks it, clears the rest — the single
select of D3); the checkboxes then let you add locations back to the same view.
Parent checkboxes are tri-state over their descendants.

**Consequence for codegen, worth accepting explicitly.** `for_each(subject=[…],
trial=[…])` is Cartesian, so a ragged location set cannot be expressed in the
generated pipeline's `for_each` call. It *can* be expressed exactly in the
generated plot function's body as a pandas mask over the `as_table` frame —
which is what the standalone CSV path already does for variant splitting
(`codegen.generate_script`). Recommend that, with a comment naming the
selection. The alternative (emit the Cartesian hull and warn) silently plots
data the user deselected, which is worse.

> **Approved 2026-09-13**: add `LocationFilter` to `scistackplot`.

## Stages

**Stage 1a — `scidb/locations.py`. DONE (unrun).**
`location_states(variable, *, variant, db, fn_registry, **grid) -> LocationTree`,
computed as the four set operations + one propagation in
`docs/claude/schema-location-status.md` §"Batching is the whole
implementation". Timing log per sub-step at DEBUG, one INFO summary line.

Shipped with it, in `provenance_query.py`, because each one was an N+1 on this
path:

| new | replaces, per record | notes |
|---|---|---|
| `variant_keys_batch` | `_producing_variant_key` | 3 queries total, byte-identical key |
| `current_records_by_schema_batch` | `_current_records_by_schema` | the per-record version now delegates, so node state got faster too |
| `latest_at_location_batch` | `get_latest_record_id_for_variant` + `state._get_latest_record_at_location` | the two differ in exactly one NULL rule; both are reproduced |
| `superseded_batch` | `state._has_superseded_ancestor` | one closure build, dirty-input marking, memoised reachability |

`locations.pathinput_configs` was lifted out of `Inspector.pathinput_state`
(which now calls it) rather than copied — it is the PathInput reconstruction the
loader denominator needs.

**`check_node_state` was left alone at this stage, deliberately** — the canvas
badge for a partially-run loader still read green until Stage 1c brought the
same rule there. See "The loader gap, split in two".

**Stage 1b — `LocationFilter` in `scistackplot`. DONE (unrun).**

| where | what |
|---|---|
| `spec.py` | `LocationFilter(keys, include)` — prefixes as `list[list[str]]` so JSON *and* TOML round-trip with no conversion; `prefixes()` is the tuple view. `PlotSpec.location_filter`, in `to_dict`/`from_dict`. |
| `reduce.py` | `_location_mask` + applied inside `apply_filters`, so the figure and the panel's "3 of 12" readout share one rule (the reason `apply_filters` is public). |
| `codegen.py` | `_location_lines` emits the mask into the function body; `_levels_after_location` narrows the colour-legend count the same way `spec.filters` already did. |
| `capability.py` | the no-filter fast path in `factor_summary` now also checks the location filter — otherwise it reports "all 3 selected" beside a figure drawing 1. |
| `__init__.py` | `LocationFilter` exported. |

Two rules worth not re-deriving later:

- **A prefix constrains only the keys it names *that the frame actually has*.**
  A key the frame lacks goes unconstrained — the graceful answer for a shallower
  variable (subject-level Mass against a trial-level selection contributes its
  one value, matching `hierarchy`'s broadcast) and the correct generalisation to
  non-contiguous schemas, where the absent key can be in the middle.
- **An empty `include` is inert**, the same way an unfilled variant row is
  (`plot-variant-rows.md` §3). Opening a picker is not a statement about the
  data.

No GUI or `scistackplotdb` change was needed: the RPC path parses specs through
`PlotSpec.from_dict` (`plot_service._spec_from_payload`), so the new field
already travels.

**Codegen carries ragged selections in the body, by necessity.**
`for_each(subject=[…], trial=[…])` cross-products its keys, so a ragged
selection cannot be expressed in the generated `for_each` call at all. It is
emitted as a pandas mask inside the generated function instead — exact, and
`test_generated_code_matches_the_mask` execs the emitted lines and compares them
against `reduce._location_mask` on the same frame, because two implementations
of one rule drift silently otherwise.

**Stage 2 — surfaces. DONE (unrun).**

```
scidb locations <Type> [subject=S01 …] [--variant fn.param=v] [--problems] [--depth N] [--json]
```

| where | what |
|---|---|
| `locations.py` | `LocationTree.to_dict` / `LocationNode.to_dict`; `prune_to_problems` |
| `inspect/api.py` | `Inspector.locations(variable, *, variant, problems_only, fn_registry, **grid)` — resolves the type, delegates, optionally prunes. Computes nothing. |
| `inspect/render.py` | `render_location_tree` + four `loc_mark_*` style fields, `color_amber`, ASCII presets |
| `inspect/cli.py` | the `locations` command; `_emit_json` now prefers a result's own `to_dict` |
| `inspect/__init__.py` | `LocationTree` / `LocationNode` / `LocationState` re-exported |

Three decisions inside it:

- **`to_dict`, not `dataclasses.asdict`.** `total`/`green`/`verdict` are
  properties so there is one definition of "how many count" rather than fields
  that drift from the tree they summarise — but `asdict` drops properties
  silently, which would have shipped a `--json` payload with no headline
  numbers. `_emit_json` now prefers a result's own `to_dict`, which also makes
  the Stage 3 RPC free.
- **`--problems` leaves counts intact.** A pruned node still reports `1/8`: the
  denominator is what makes the numerator mean anything, and hiding the healthy
  rows must not also hide how many there were.
- **Counts render on parents only.** At a leaf they are always `1/1` or `0/1`,
  which the dot already says — the same rule the GUI pane will follow, and the
  user's own instruction about the deepest level.

**Stage 3 — RPC + component. DONE (Python tests unrun; never rendered).**

| where | what |
|---|---|
| `scistackplotdb/variants.py` | `branch_params_for` — `selection_for` run backwards. The picker holds a column-keyed selection; scidb takes a `branch_params_filter`. |
| `plot_service.py` | `location_tree(db, variable, *, selection, problems_only, csv_path)`; `selection=None` falls back to `variants.default_selection` |
| `api/plot.py` | `POST /api/plot/locations` |
| `server.py` | `plot_location_tree` RPC + listed in `SELF_MANAGED_DB_METHODS` (it holds its own connection, like `plot_variant_graph`) |
| `frontend/api.ts` | route entry |
| `SchemaLocationPicker.tsx` | the component |

Decisions inside it:

- **The service takes the plotting layer's vocabulary, not scidb's.** Both
  callers already hold a column-keyed selection (Plot Studio has the open row;
  the canvas has `default_selection`), and translating in the GUI would
  re-implement scidb's namespacing one layer away. `branch_params_for` lives in
  `scistackplotdb`, the layer that knows both — the same home as its inverse.
- **`selection=None` means the canvas path**, and resolves to the rule a panel
  opens on. Two entry points showing different variants of one variable would
  be indefensible.
- **A CSV returns an empty tree with a note**, not an error: the popup opens and
  explains that a flat file carries no provenance to verify.
- **Checkboxes render only when `onChange` is given.** From the canvas no spec
  is open, so a control that cannot change anything is not drawn as though it
  could.
- **The minimal covering set is re-derived by a coverage walk**, not maintained
  incrementally — so "tick every trial" and "tick the subject" produce the same
  stored selection, and unticking one trial inside a ticked subject explodes the
  covering prefix into its siblings.
- No cache yet. `location_states` is batched and the popup is on-demand; adding
  one before a measurement would be guessing.

## The defect Stage 3 found (Stage 1b, now fixed)

`LocationFilter.include` held **positional** value-tuples read against
`LocationFilter.keys`. Writing the picker made the hole obvious: a node's path
is `[["subject","01"],["speed","SSV"]]` when `timepoint` is NULL — a supported,
documented shape (`schema-hierarchy-contiguity.md`), and the exact reason
`LocationNode` carries `key` beside `value`. Read positionally, that second
value would be matched against `timepoint` and select **nothing, in silence**.

`include` entries are now lists of `[key, value]` pairs. `keys` survives as
display order only. Three consequences, all improvements:

- the mask and the generated code zip nothing — they read the key off the pair;
- `_levels_after_location` becomes a membership test rather than depth
  arithmetic;
- the frontend sends a node's `path` back **verbatim**, with no positional
  translation step to get wrong.

`TestNonContiguousLocations` in `test_location_filter.py` is the regression.

**Stage 4 — canvas entry. DONE.** "🗂 View Schema Locations" beside "📈 Plot" in
the `variableNode` context menu. No spec is open there, so the picker arrives
**without `onChange`** — no checkboxes, because there is no figure yet to add a
location to — and the variant defaults server-side to `default_selection`.

Clicking a row opens the Plot panel **already showing that location**, which
needed one field threaded down the existing single funnel:

```
PipelineDAG.openPlot(variable, location)
  → open_plot_panel RPC  (dagPanel forwards params wholesale — no change)
  → PlotTarget.location  (plotPanel.ts: opaque, the host never reads it)
  → __SCISTACK_VIEW__.location / open_plot_studio params
  → PlotRoot → PlotStudio.initialLocation
```

The first attempt dropped the location at this boundary and told the user to
re-pick it inside the panel. That is the requirement ("clicking one schema
location should open a plot of that Variable Variant at that location"), not a
detail, and the plumbing turned out to be one optional field on a chain that
already existed.

**Stage 5 — Plot Studio entry. DONE.** The "Schema keys" section is now one
button and nothing else; the per-key `LevelPicker`s are **removed, not hidden**
(D5). The button's label is the selection — one location reads as the location
itself, because that is the common case.

- checkbox → `setLocationInclude` (the picker's own minimal-covering-set
  arithmetic, written straight into `spec.location_filter`);
- row click → `applyPickedLocation`, which is **module-level and pure** because
  the canvas hand-off needs the same rule applied to the *opening* spec. Seeding
  it there rather than editing after the fact means the first resolve already
  draws that location, instead of drawing the whole dataset once and then
  narrowing.

**D4, refined during the build.** The plan said "keys below the selection get
`FREE`". Applied bluntly that stamps over a chosen x axis because someone
clicked a location. The rule shipped is narrower and keeps the intent: a schema
key the selection does not name is moved off `AGGREGATE` onto `FREE`, and one
with **no** role gets `FREE` — but `X`, `COLOR` and `FACET` are left alone,
because they already draw their levels individually, which is what "as granular
as possible" asked for.

**Stage 1c — the canvas half of the loader gap. DONE.** `state._discovery_gate`,
called from `check_node_state` after the invocation-membership answer. The four
risks and how each was answered are recorded at the end of
`.claude/plan-pathinput-loader-staleness-gap.md`, which is now CLOSED. The
short version: a TTL cache for the cost, a **credibility guard** for paths that
cannot resolve (discovery finding nothing while outputs exist means the walk is
broken, not that the study vanished), exclusions as the single escape hatch, and
the discipline that the gate may only ever ADD red.

The badge and the picker's denominator now use one rule, which is the outcome
worth having: a red node and a red row are the same fact rather than two
estimates of it.

## The loader gap, split in two — BOTH HALVES NOW DONE

`.claude/plan-pathinput-loader-staleness-gap.md` was one symptom with two fixes
of very different cost. Splitting them is what let the cheap half ship first
rather than being blocked by the expensive one's risks.

**(A) The denominator in this view — done in Stage 1a.** When the variable is
produced by a PathInput-only loader, `location_states` derives its expected set
from `check_pathinput_node_state`'s should-run set (`PathInput.discover()` ∩
grid − exclusions) instead of from what the loader has already produced. A file
on disk that was never loaded now reads **red** in the picker. This is safe
because discovery runs **when the popup opens**, not on every canvas refresh —
which is the whole reason the gap doc listed "discovery cost on every graph
build" as the top risk.

**(B) The canvas badge — done in Stage 1c.** `state._discovery_gate` asks the
same question inside `check_node_state`, behind the three things the popup did
not need: a TTL cache for the per-refresh cost, a credibility guard for paths
that cannot resolve, and the discipline that it may only ever ADD red. Full
risk-by-risk accounting at the end of the gap doc.

**The one deviation from the gap doc's own advice.** It asked for diagnostics
before behaviour — measure the delta on a real project, then decide. That was
folded into the fix instead: the gate logs the on-disk count, the never-run
count and the walk timing every time it runs, so the measurement is available,
but it is emitted *by* the working change rather than ahead of it. The
credibility guard is what made that safe in one step; without a guaranteed floor
on the failure mode (a broken path reddening an entire study), measuring first
would have been mandatory.

`test_the_graph_alone_still_cannot_see_the_shortfall` replaced the old boundary
test: the gap is in the graph, that has not changed, and the gate is a second
question asked beside it rather than a repair of the first.

**Stage 6 — tests.** `scidb/tests/test_locations.py` covers the Stage 1a half
(written, unrun): per-location status vs `check_combo_state` location for
location on a fixture holding one of each state (the correctness anchor);
`variant_keys_batch` and `superseded_batch` vs their per-record originals;
roll-up arithmetic incl. the zero-non-grey-descendant → grey rule; excluded
locations absent from both numerator and denominator; the partially-run loader
reading `2/3` here while `check_node_state` still reads green; variant scoping.

Stage 2 tests live at the end of `scidb/tests/test_locations.py` (written,
unrun): the facade shapes rather than recomputes (`facade.to_dict() ==
direct.to_dict()`); unknown type raises; `--problems` prunes while counts stay
whole; `to_dict` carries what `asdict` drops and the whole tree survives a JSON
round trip; the renderer's header, parent-only counts, excluded label, ASCII
purity, `--depth` cap and notes; the CLI's JSON payload, `--problems`, and
`--variant` literal-eval.

`scistackplot/tests/test_location_filter.py` covers Stage 1b (written, unrun):
the ragged-vs-Cartesian difference stated as an assertion; prefix/subtree/union
semantics; inert and degrading states; text-comparison of zero-padded values;
JSON + TOML + `extract_spec` round trips; generated-code parity against the
runtime mask over five selections; the `factor_summary` fast path; the
legend-level arithmetic.

Stage 3 tests (written, unrun): `scistack-gui/tests/test_plot_service.py` — the
payload's four states and counts, the canvas default matching
`default_selection`, a column-keyed selection reaching through, `problems_only`,
JSON-serializability across the webview boundary, the CSV note, and both
transports reaching one function. `scistackplotdb/tests/test_variant_chain.py` —
`branch_params_for`'s four mappings, the dropped `CodeIsLatest: False`, and the
round trip (which *resolves* a bare `code_version` rather than echoing it).

**Stages 4-5 coverage — DONE, and it forced a refactor worth keeping.** The
rules inside the picker had no way to be executed in a test, so they were
extracted into `frontend/src/components/PlotStudio/locationSelection.ts` — no
React, no DOM, no fetch. `SchemaLocationPicker.tsx` and `PlotStudio.tsx` now
import from it (`applyPickedLocation` is a two-line wrapper over
`rolesAfterPick`; the button label is `describeSelection`).

`npm test` in `scistack-gui/frontend` runs them under node's own runner —
`tsc -p tsconfig.test.json && node --test dist/test`, the same split
`extension/tsconfig.test.json` makes for its vscode-free modules, and for the
same stated reason. **24 tests, all passing.** Two mechanics worth knowing: the
test imports `'./locationSelection.js'` (the package is `type: module`, so node
needs a real specifier and TS maps it back to the `.ts`), and `@types/node` was
added as a frontend devDependency because the main `tsc --noEmit` typechecks the
test file too. `frontend/dist/` is gitignored.

The tests that matter most:

- ticking every trial stores the *same* selection as ticking the subject — if
  those differed, a trial added tomorrow would be inside one and outside the
  other;
- unticking one trial from the **all** state explodes from the virtual root, and
  re-ticking it collapses back to one entry;
- a path with the same value under a different key is not covered (the
  non-contiguous rule, in the algebra as well as the mask);
- a chosen X/COLOR/FACET role is never stamped over by a pick.

**D5 asserted, in `test_plot_service.py`** (source-level, following the existing
"variant-mode nodes never call the backend" precedent): the Schema keys section
contains no `<LevelPicker` and no `setLevelFilter` — while `setLevelFilter`
itself survives, because the Filters section below still needs it. Two more
invariants pinned there: the picker reaches **exactly one** backend method
(inspecting integrity must not be able to write execution state — the same trap
the variant popup is built around, one control further on), and it never
*assigns* a state name, only compares against one.

## Out of scope (v1)

- Per-location sparkline/thumbnail previews — the only check that catches a
  flat-lined trial every hash calls green. Size the rows to hold one later.
- The subject × trial matrix view (systematic gaps read as stripes there and are
  invisible in a collapsed tree).
- Keyboard traversal + "next non-green" with the figure following the cursor.
- Any write action from the popup (exclude-from-here is tempting and is exactly
  the Phase 5 bright line in `observability-api-design.md`).
