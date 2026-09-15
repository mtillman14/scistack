# Plan: per-key schema omission (synced picker pane) + declared level order

Drafted 2026-09-14. Two independent features, planned together because both
answer "which schema key levels, in which order".

## Decisions locked with the user (2026-09-14)

| Question | Answer |
|---|---|
| Layout | **Two panes in one dialog, left (by key) + right (locations), SYNCED.** Left entry is green / amber(mixed) / red, derived from the same selection the tree draws. Clicking an amber key or level resolves it to all-on or all-off. |
| Omission scope | **Local to the plot / node.** Nothing written to the database; no project-wide exclusion. |
| New data later | **The left pane stores a RULE, not a snapshot.** Deselect `session=BL` and a subject added tomorrow appears with its BL dropped, not missing entirely. |
| Levels listed | **Only levels present for this variable/variant** (derived from the same tree the right pane draws) — no extra backend call. |
| Processing tab | **Both panes.** Right-pane tree is the **inner join** (intersection) of the input variables' schema locations. |
| Ragged runs | **Add ragged combo filtering to scifor** so the processing tab's right pane is really editable, not decorative. |
| Order reach | **Every constructed DataFrame** — plot tables, scidb row order, GUI level lists. |

## Feature A — the two-pane picker

### A1. The storage shape (why the panes can't disagree)

`LocationFilter` grows one field:

```python
include: list[list[list[str]]]          # existing: minimal covering prefixes (ragged)
exclude_levels: dict[str, list[str]]    # NEW: standing per-key rule
```

A location is drawn iff **covered by `include`** AND **no step `(key, value)`
has `value in exclude_levels[key]`**. Both empty = everything, inert, which is
what an untouched picker means (unchanged rule).

Why two structures rather than one:

- A ragged tick (`trials 1-3 of subject 02`) is only expressible as prefixes.
- A standing rule (`BL is out, everywhere, forever`) is only expressible as a
  per-key list — exploding it into prefixes freezes today's subjects and drops
  tomorrow's entirely. That is the drift `normalize()` already exists to avoid.

The tri-state on BOTH panes is **derived** from the pair, never stored beside
it — the same discipline `coverageOf` already follows, and the reason the panes
cannot fall out of sync.

### A2. Pure rules (scistack-gui/frontend/src/components/PlotStudio/locationSelection.ts)

All new functions pure and unit-tested (node test runner, existing
`locationSelection.test.ts`):

- `coverageOf(node, include, excludeLevels)` — existing walk, now also `none`
  when a step is level-excluded. Every existing caller updated.
- `levelCoverage(roots, include, excludeLevels, key, value)` → full/partial/none:
  the roll-up over every location whose path contains `(key, value)`.
- `keyCoverage(...)` → roll-up over that key's levels.
- `withoutLevel(roots, include, excludeLevels, key, value)` → adds to
  `exclude_levels[key]`.
- `withLevel(...)` → removes the exclusion AND re-ticks that level's
  prefix-excluded locations (amber → green, per the user's rule).
- `levelsByKey(roots)` → `{key: [values…]}` in tree order (feature B orders it).
- `describeSelection(include, excludeLevels)` — the button label gains
  "− 1 session" style suffixes so an omission is visible without opening.

### A3. Components

- **New `SchemaKeyLevels.tsx`** — generic, plotting-agnostic. Props:
  `keys`, `levelsByKey`, `stateOf(key) / stateOf(key, value)`, `onToggleKey`,
  `onToggleLevel`, `disabled?`. Knows nothing about specs, variables or RPCs;
  the two call sites pass the state functions. Two levels of nesting only:
  key → its levels, collapsible, per the user's description.
- **`SchemaLocationPicker.tsx`** — grows the left column, keeps the tree at
  right. `value`/`onChange` carry `{include, exclude_levels}`. The canvas entry
  (no `onChange`) shows both panes read-only, as today.
- **`PlotStudio.tsx`** — the "Schema keys" section's one button now summarises
  both halves of the selection.

### A4. Python: applying `exclude_levels`

- `scistackplot/spec.py` — field, `to_dict`/`from_dict`, `is_empty()`.
- `scistackplot/reduce.py::_location_mask` (~743) — mask gains the level test.
- `scistackplot/codegen.py` (~542, ~875) — exported plain-pandas must reproduce
  it, and `_narrowed_levels` must drop excluded levels from an axis's order.
- `scistackplot/capability.py:528` — the "nothing is filtered" shortcut.

### A5. scifor: ragged combo filtering (the new lowest-layer concept)

`scifor` owns the predicate because `scifor` owns combo expansion. New module
`scifor/src/scifor/locations.py`:

```python
@dataclass(frozen=True)
class LocationFilter:
    include: list[list[tuple[str, str]]] = ()
    exclude_levels: dict[str, list[str]] = {}
    def matches(self, combo: dict) -> bool: ...
    def is_empty(self) -> bool: ...
```

- `for_each(..., locations=LocationFilter | dict | None)`.
- Applied in `foreach.py` at the combo-list seam (~556-576) — **both** branches:
  the Cartesian product AND the `_all_combos` list handed in by scidb, or the
  DB-driven path silently ignores the filter.
- Values compared **as text**, matching every other selection that crosses JSON
  (and the picker's own contract).
- Logging (NOTE 2): `Log.info("locations: %d of %d combos kept (%d by prefix,
  %d by level rule)")`, and a WARNING when the filter removes **every** combo —
  that is the silent-empty-run failure this feature could otherwise introduce.

`scistackplot` cannot import `scifor` (deps: pandas/numpy/scistacklog only), so
the rule genuinely exists twice — as a dict predicate and as a pandas mask.
Mitigation: one semantics doc (`docs/claude/location-filter-semantics.md`) and a
**parity test** in each package built from the same table of cases, so a change
to one that is not mirrored in the other fails.

### A6. scidb: threading + the intersected tree

- `scidb.for_each` passes `locations=` through to `scifor.for_each` (the
  `_all_combos` path included).
- `scidb/locations.py` — `location_states(variables=[...])` (or
  `intersect_trees`) returning the **inner join**: a location survives iff every
  named variable has it. States roll up worst-of, so a location green in one
  input and red in another reads red. Timing logged per variable — `location_states`
  measured 9.5 s on the big dataset, so N inputs is the cost to watch; log
  `TOTAL` and per-variable splits.

### A7. GUI wiring for the processing tab

- New RPC `node_location_tree` → service that resolves a function node's input
  variables and calls the intersected `location_states`.
- `FunctionSettingsPanel` — the "Schema Filter" checkbox block is replaced by
  one button opening the shared popup. Node data field `schemaFilter` is
  **replaced** by `schemaSelection = {include, exclude_levels}` (clean break, no
  shim — the beta rule), `pipeline_store` and `put_node_config` updated.
- `api/run.py` — derives the per-key iterables from `exclude_levels` (so the
  Cartesian half still narrows discovery up front) and passes `locations=` for
  the ragged half. Both logged in the existing run-start summary line.
- scimatlab parity: `locations=` accepted by the MATLAB `for_each` wrapper.

## Feature B — declared level order

### B1. Config surface

```toml
# scistack.toml
[schema_keys]
session = ["BL", "POST", "FU"]
speed   = ["SSV", "FAST"]

# pyproject.toml
[tool.scistack.schema_keys]
session = ["BL", "POST", "FU"]
```

Values compared **as strings** after the existing canonicalization, so `"01"`
stays `"01"` (the zero-padding rule) while a key declared `numeric` still
canonicalizes first and then matches. Undeclared keys and undeclared levels fall
back to today's method — declared first, remainder appended in the existing
order (`["a","c"]` + observed `b` → `["a","c","b"]`).

### B2. One owner

`scidb/src/scidb/schema_order.py` (scidb owns schema keys; the config reader
already exists in `scifor.discovery.read_scistack_section`, which is a pure
reader available to every layer — same shape as `resolve_entities_path`):

- `declared_level_order(project_root=None) -> dict[str, list[str]]`, cached.
- `order_levels(key, values, *, fallback)` → declared prefix + `fallback(rest)`.
- `level_sort_key(key, fallback_key)` → for row sorting.
- `DatabaseManager.dataset_schema_key_order` property.

Each caller keeps ITS OWN fallback comparator, so undeclared keys behave exactly
as they do today and this cannot regress existing orderings.

### B3. Consumers

| Where | What changes |
|---|---|
| `scidb/database.py` `_sort_by_schema` (~1880) | declared → sort position; undeclared → today's numeric/string test |
| `scistackplotdb/source.py::_ordered` (~174) | declared first; then the existing `numeric` / `natural_sort_key` branch |
| GUI `get_schema` values | checkbox/level display order |
| picker left pane | inherits the order from the levels it is handed |

### B4. The config-writer trap

`scistack_gui/config.py::_render_scistack_toml` rewrites the whole file from the
fields it knows, so a hand-authored `[schema_keys]` table would be **silently
deleted** by the Paths popup — exactly the failure its own docstring says must
not happen. It must round-trip the table, with a regression test that adds a
path and asserts the ordering survives.

### B5. Logging & validation (NOTE 2)

- INFO at load: which keys declared an order, and how many levels each.
- WARNING: a declared key that is not a dataset schema key (typo).
- DEBUG per construction: levels appended beyond the declaration.

## Stages

1. `scifor.locations` + `for_each(locations=)` + logging + tests.
2. `scidb`: pass-through, multi-variable intersected `location_states`, timing.
3. `scistackplot`/`scistackplotdb`: `exclude_levels` (spec, reduce, codegen,
   capability) + parity test.
4. Frontend rules: `locationSelection.ts` + unit tests (no UI yet).
5. Frontend UI: `SchemaKeyLevels.tsx`, the two-pane picker, PlotStudio wiring.
6. Processing tab: RPC, panel replacement, node config, run path, MATLAB arg.
7. Feature B: `scidb.schema_order`, the four consumers, config round-trip.
8. Docs (`docs/claude/location-filter-semantics.md`, an update to
   `schema-location-status.md`) + rebuild BOTH vite targets before any visual
   check.

## Risks

- **Two implementations of one predicate** (A5) — held together by the parity
  test only; if that test is skipped the features drift silently.
- **N× `location_states`** for the processing tree on a large dataset.
  Stage 2 must measure, not assume.
- **`schemaFilter` → `schemaSelection`** is a saved-pipeline format change; old
  node configs need a read-time translation at minimum, or a stated break.

---

## Status

**Stage 1 (scifor) — built 2026-09-14.** `scifor.locations`, `for_each(locations=)`
applied to both combo branches, reporting, tests. Two fixes fell out of it: a
`for_each` whose every iteration fails returns an empty frame that reads exactly
like an empty schema, so that now WARNs (`foreach.py`, tests in
`test_logging.py`); and `test_case_a_adopts_template_keys_in_placeholder_order`
(added in 496d65e6, never green) passed a mismatched parameter name to
`fn(**kwargs)`.

**Stage 3 (scistackplot) — built 2026-09-14.** `exclude_levels` in `spec`,
`reduce`, `codegen`, `capability`; the shared case file; parity suites.

### Decisions taken during Stage 3

- **The rule is FOUR implementations, not two.** `codegen` emits its own copy
  of the mask for the export path, and the GUI has the coverage walk. An
  exported script that filters differently from the figure it came from is the
  worst version of the drift, because the picture was approved and the data
  behind it silently is not the same data.
- **Cases live in `docs/claude/location-filter-cases.json`**, loaded by each
  suite, never transcribed. A case added to the spec now fails every
  implementation that has not adopted it. Each case carries `applies_to`
  (`combo` / `row` / `tree`) because a combo may be coarser than a location and
  a row never is.
- **`value_spellings`: an integral number matches both `1` and `1.0`.** A
  schema key that round-tripped through DuckDB as a float reaches the plotting
  frame as `1.0` while the picker — whose tree comes from scidb — sends `1`;
  raw text comparison selected nothing across that seam, in silence. A
  zero-padded string is NOT a number here, so `"01"` still only matches `"01"`.
  Implemented once per package (`scifor.locations`, `scistackplot.spec`), pinned
  by shared cases 16-18; `codegen` BAKES the spellings into generated source
  rather than shipping a third copy of the function.
- **NULL at a key is not a level**: matches no prefix, dropped by no rule.
  Without it the first level rule anyone writes deletes every cross-cutting
  record (`None` vs `NaN` do not even share a text form).
- **An empty prefix (`include=[[]]`) is dropped at construction** in scifor
  too, so `is_empty()` agrees with scistackplot's `prefixes()`. Both already
  selected everything; now they say so identically.

### Still open

- Stage 4 must make `locationSelection.test.ts` read the same JSON (`tree`
  cases), or the fourth implementation stays unpinned.
- Stage 2's intersected `location_states` is unbuilt; the processing tab's
  right pane depends on it.

**Stage 4 (frontend rules) — built 2026-09-14, 62/62 passing under `npm test`.**
`locationSelection.ts` now holds the pair (`LocationSelection`), the by-key
roll-ups (`levelsByKey`, `levelCoverage`, `keyCoverage`, `toggleLevel`,
`toggleKey`, `withLevel`, `withoutLevel`), `valueSpellings`, and
`visibleSelection`. The test file LOADS the shared cases (walking up from
`import.meta.url`, because `npm test` runs the compiled copy out of
`dist/test`). No UI yet — that is Stage 5.

### Three rule changes Stage 4 forced, each pinned by a test

- **`covers` matches by KEY in order, not by position.** A prefix may skip a
  key (`[subject=02, trial=3]` against a path carrying a `session`), which the
  Python sides always allowed and this did not: read positionally, `trial=3`
  was compared against the path's `session` step and selected nothing, in
  silence. Latent today because clicked prefixes are contiguous; shared case 7
  is what caught it.
- **An empty selection now covers EVERYTHING** (`coverageOf` returns `full`).
  It previously returned `none`, so an untouched picker drew every location
  with every box unticked. That was survivable while the tree was the only
  pane; with a by-key pane it is incoherent — there would be no green state to
  click away from. **Visible change in the existing UI before Stage 5 lands:
  boxes now start ticked.**
- **`withPath` on an inert selection is a no-op.** Everything is already
  selected, so adding narrows nothing. Without the guard, `withLevel` — which
  re-ticks a level's locations one at a time — could collapse to inert midway
  and then narrow the whole figure to whichever location it added next.

### Still open

- Stage 5 (the two-pane UI) and Stage 6 (processing tab) are unbuilt; Stage 2
  (intersected `location_states`) still gates Stage 6.
- Nothing has been visually checked, and both vite targets need rebuilding
  before it can be.

**Stage 5 (the two-pane UI) — built 2026-09-14.** `SchemaKeyLevels.tsx` (the
generic left pane) + the two-column `SchemaLocationPicker` + PlotStudio hint.
tsc clean, 62/62 rule tests still pass, and BOTH vite bundles rebuilt and
verified to contain the new pane (the dead-bundle trap in
`project_frontend_bundle_rebuild`).

- `SchemaKeyLevels` takes `keys`, `levels` and two coverage FUNCTIONS, and
  reports clicks. No spec, no RPC, no plotting vocabulary — so Stage 6 can hand
  it a function node's schema filter unchanged. Read-only when the handlers are
  omitted, drawn disabled rather than hidden.
- Levels come from the TREE (`levelsByKey`), so only levels this variable and
  variant actually have are listed — the decision from the first round of
  questions, now enforced by where the data comes from rather than by a rule.
- **Deviation to confirm:** the user described one click on a key doing both
  "toggle all its levels" and "open/close its level list". Built as the
  standard idiom instead — the CHECKBOX toggles (and auto-expands, so a click
  that changes twenty levels shows what it changed), the twisty/label expands
  without touching the selection. Doing both on one click makes it impossible
  to look at a key's levels without also toggling them. One-line change back if
  that is wanted.

### Still open

- **Never visually checked.** Both bundles are current; nobody has opened the
  panel.
- Stage 2 (intersected `location_states`) still gates Stage 6, and Stage 7
  (the `[schema_keys]` level order) is untouched.

**Stage 2 (scidb) — built 2026-09-14, tests unrun.**

- `for_each(locations=...)` threaded through all three scifor call sites: the
  real delegation, the DRY-RUN path (a preview that describes a different run
  than the one that follows is worse than no preview), and the hand-written
  EachOf recursion.
- scidb does NOT pre-filter combos of its own. The filter is a pure combo
  predicate that owns no database concepts, so it is applied once, in scifor,
  after this layer has built the full combo list including rid variants.
  Loading is bulk per variable rather than per combo, so an earlier pass would
  save nothing and double the reporting.
- `scidb.locations.intersect_location_states(variables, …)` — the inner join,
  worst-state-wins, grey preserved, `record_id`/`code_version` dropped,
  per-variable timings logged. One variable delegates; none returns an empty
  tree that says so. Written up in `docs/claude/schema-location-status.md`
  §"The fifth question".
- Tests: `scidb/tests/test_foreach_locations.py` (new) and a
  `TestIntersectedLocations` class in `scidb/tests/test_locations.py`.

### Still open

- **Nothing here has been run** — hand over the two pytest commands.
- The N× cost of the intersection is logged but has not been MEASURED against
  a real dataset; that was the Stage 2 risk and it stays open until someone
  opens the pane on the big study.
- Stage 6 (processing tab) and Stage 7 (`[schema_keys]` order) remain.

**Stage 6 (processing tab) — built 2026-09-14, Python tests unrun.**

- **RPC `node_location_tree`** + `services/node_location_service.py`: resolves
  a node's input variables through `derive_target_for_node` (by the NODE, never
  by function name — one name can have several wirings) and hands them to
  `intersect_location_states`. Registered as self-managing its DB connection,
  like `plot_location_tree`: it can spend seconds there, one `location_states`
  per input.
- **The picker serves both tabs.** `nodeId` switches its fetch; the payload
  shape is identical on purpose, so there is one renderer.
- **`schemaFilter` → `schemaSelection`, clean break**, end to end: node data,
  `_SAVED_CONFIG_KEYS`, `put_node_config`, the `start_run` RPC, `RunRequest`,
  `_run_in_thread` (now passing `locations=`), and the GUI tests that carried
  the old key.
- **`domain/schema_selection.py`** — the lossy projection onto what a generated
  MATLAB command can spell (one value list per key), returning WHAT WAS LOST
  beside the filter. Every caller logs it.
- **scimatlab bridge**: `for_each_prepare(locations=)` filters `full_combos`
  before they cross, so MATLAB honours the ragged half with no `.m` change.

### The MATLAB gap that remains

The bridge accepts `locations`; `+scidb/for_each.m` does not yet PASS it, so a
generated MATLAB command still carries only the Cartesian projection. That is
why the projection warns rather than being quietly correct. Closing it is a
`.m` signature change plus the generator emitting the struct.

### Still open

- Python tests unrun: `scistack-gui/tests/test_schema_selection.py` is new, and
  the `schemaFilter` rename touches `test_graph_builder`, `test_portability`,
  `test_pipeline_scopes`.
- Still never visually checked, though both bundles are current.
- Stage 7 (`[schema_keys]` level order) is the last one.

**Stage 7 (declared level order) — built 2026-09-14, tests unrun. Feature complete.**

- `scidb/schema_order.py` — one reader for `[schema_keys]` (and
  `[tool.scistack.schema_keys]`), cached on the config's mtime like
  `entities.load_for_project`, values kept as TEXT so `"01"` survives.
  `order_levels(key, values, declared=, fallback=)` takes the caller's OWN sort
  as `fallback`, so an undeclared key behaves exactly as it did before this
  module existed.
- `DatabaseManager.dataset_schema_key_order` reads it at open and VALIDATES it
  against this dataset's keys — a typo is otherwise completely silent.
- Four consumers: `_sort_by_schema_keys` (row order, via a single integer rank
  column — a `(group, position)` tuple would make every undeclared level tie),
  `scistackplotdb.source._ordered` (factor levels), the GUI's `get_schema`
  (level lists), and `locations._build_tree`'s sibling sort (the picker's tree,
  which must not read BL, FU, POST beside an axis reading BL, POST, FU).
- The GUI config writer round-trips the table and emits it LAST, since a TOML
  table swallows every key after it. Two tests pin that.
- Documented in `docs/claude/config-file-formats.md`.

### Still open across the whole plan

- **Every Python test since Stage 2 is unrun**, and nothing has been visually
  checked.
- MATLAB gets the Cartesian projection only: the scimatlab bridge accepts
  `locations`, `+scidb/for_each.m` does not yet pass it.
- The N× cost of `intersect_location_states` is logged but never measured
  against the big study.
