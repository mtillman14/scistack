# Plan: expected locations, missing data, and the CLI queries that share them

Drafted 2026-11-07. Approved 2026-11-07 (Stage 7 after Stages 1-3; names as proposed).

**Status 2026-11-07:** ALL stages written. 1-3 and 7 pytest pass (user). 4, 5, 6 written: pyright clean,
frontend tsc + 743 node tests pass, both bundles rebuilt; pytest NOT yet run; GUI not checked by eye
(docs/gui-manual-testing-todo.md, top entry). Stage 4: `scidb/inspect/cells.py`, `scidb nans`, preview
`cells`. Stage 5: `services/expectation_service.py`, `api/schema.py` handlers, `components/Expectation/`,
`context/ExpectationBadgesContext.tsx`, Coverage header button. Stage 6: `locations._expectation_locations`;
grey is now "not counted" (CLI + GUI). Remaining: the plot layer's collapsed-1-D partial-NaN gap;
MATLAB run outcomes.

**Status:** Stages 1-3 written 2026-11-07 (pyright clean; pytest NOT yet run by the
user): `scidb/provenance/expectation.py`, `scidb/provenance/coverage.py`, tables in
`store/tables.py`, `exclusions.most_specific_match` + `override_history`,
`query.variable_type_edges` / `related_variable_types` / `variable_types_in_pipeline_order`,
Inspector + Mutator + CLI (`expected`, `keys`, `set-key`, `mark`, `unmark`, `marks`,
`coverage`, `follow`, `exclusions --history`, `vars` producers). Tests:
`scidb/tests/test_expectation.py`, `scidb/tests/test_coverage.py`. Doc:
`docs/claude/expected-locations.md`. Stage 7 written 2026-11-07 (pyright clean, pytest NOT yet run): `scidb/provenance/outcomes.py`,
`_run_outcome`, scifor combo_skip payload gains blame/probable, foreach collects + writes,
expectation rows gain `last_run`. Tests `scidb/tests/test_outcomes.py`. Python runs only
(MATLAB not yet). Next: 4, 5, 6.

Builds on
`docs/claude/data-inventory.md`, `docs/claude/schema-location-status.md`,
`docs/claude/observability-api-design.md`.

## Why

A NaN-filled / missing stretch of data went unnoticed until the end of an
analysis. Today nothing says, for one variable, which schema locations
*should* exist, which are missing *as expected*, which are missing
*unexpectedly*, and which are *unexpectedly present* (a typo'd `S7` beside
`S07`, a trial 6 when there are five, data for a location marked as never
recorded). The Inventory answers a database-wide question (no
per-variable mode), and the location picker's "expected" is derived from what a
function should have run, so a downstream variable inherits its loader's blind
spot (`12/12` when eight subjects were never loaded).

## Rule: one read API, three consumers

Every question below is answered by an `Inspector` method (the shared read
API, `scidb/inspect/api.py`). The `scidb` CLI renders it, the GUI calls the
same method in-process (never a subprocess: a second DuckDB handle brings back
the write-lock contention). Writes go through `scidb/inspect/mutate.py`
(`Mutator`), like `exclude`/`include`. Rules live in a scidb owner module,
never in the facade, the CLI or the GUI.

## Decisions already made (user, 2026-11-07)

- **D1 Words.** "Exclude"/"Include" keep meaning *leave out of / put back in
  the analysis* (`__scidb_schema_overrides`, affects runs). The new marks are
  **Expected** and **Not expected** (revised 2026-11-07 from "Should exist" /
  "Missing as expected" so that unexpected presence fits). They never change
  what runs.
- **D1b Expectation × fact.** Every combination has an expectation (generated
  by the D2/D3 rule, overridden by a mark) and a fact (present or absent).
  The 2×2 gives the four statuses below; an analysis exclusion sits outside it.
- **D2 Per-key question:** "Does every parent share the same values for this
  key?" (`shared` vs `per_parent`). Default: the top key is shared; any key
  left of the last shared key is shared (the user's heuristic). Optional
  declared values per key (`trial = 1..5`) catch a value missing everywhere.
- **D3 Level cut.** Keys deeper than the variable's own level are never
  crossed.
- **D4 Marks are per variable and inherited downstream** through the pipeline
  graph by variables at the same or deeper level, so one missing file is
  marked once.
- **D5 Storage.** Per-key settings are project-wide dataset intent (bundle
  `BUNDLE_INTENT`, like `_schema_key_type`). Marks are dataset intent too.
- **D6 One owner of "should exist":** the new owner. The location picker reads
  it (Stage 6) so the canvas, the picker, the popup and the CLI cannot disagree.
- **D8 Two levels on top, full detail underneath** (user, 2026-11-07). The
  first look is a dichotomy: **green** = the fact matches the expectation
  (`present`, `missing_expected`), **red** = it does not
  (`missing_unexpected`, `present_unexpected`); grey = excluded. The exact
  status is never hidden:
  - every cell/row has a hover naming its quadrant and its *why*: "Missing
    unexpectedly: trial is declared 1–5 and this file was not found", "Missing
    as expected (marked in RawEMG by …, reason: sensor off)", with the mark's
    date and whether it is inherited;
  - the same text is a visible column in the popup grid, the CSV export and
    `scidb expected` (hover alone is not enough: not copyable, not on touch);
  - a shape or icon alongside the colour for each of the four statuses, so
    the two reds and the two greens are told apart without hovering and
    without relying on colour;
  - filters by the four statuses, not just red/green.
  Owner of the words: `expectation.py` (status label + one-sentence
  explanation per row), like `OmissionReport` owns "Not plotted" wording; the
  CLI and GUI only display it.
- **D7 Badge** on the variable node in two parts: unexpected count (missing
  unexpectedly + unexpectedly present) and marked count, each opening the
  popup filtered. Cached.

## The owner: `scidb.provenance.expectation` (new)

Statuses for one variable at one combination:

| | present | absent |
|---|---|---|
| **expected** | `present` | `missing_unexpected` |
| **not expected** | `present_unexpected` | `missing_expected` |

plus `excluded` (analysis exclusion wins, as in Inventory). An Expected mark
adds a combination the product would not have generated (or accepts an
unexpected one, e.g. a pilot subject); a Not-expected mark removes one.

Where `present_unexpected` comes from:
- a value outside a key's **declared** values (`trial=6` when `1..5`; `S7`
  when subjects are declared). Without declared values a shared key's universe
  is every value seen, so a stray value cannot be unexpected by itself; it
  instead shows up as `missing_unexpected` rows for every *other* variable.
  The popup should point at such a value ("`S7` exists only in RawEMG").
- a **Not-expected mark on a location that now has data** (the mark is stale,
  or the wrong file was loaded).
- a record at a **level deeper than the variable's level** (a subject-level
  variable with a trial-level row).

Universe: cross the variable's keys at or above its level (D3). `shared`
keys use declared values if any, else every value the key has anywhere in the
database (Inventory's `distinct_schema_values`, so a subject present for any
variable is expected for this one). `per_parent` keys contribute only the
values that exist under each parent (Inventory's *Within* stratum rule). Values
are the stored strings (`"01"` stays `"01"`).

Each status carries *why*: which mark (own or inherited from which variable),
which exclusion (`governing_overrides`), which rule generated it.

## Tables (registered in `scidb/store/tables.py`; the registry guard test)

- `_schema_key_expectation(key, sharing, declared_values VARCHAR[], declared_at, source)`
  — append-only; latest row per key wins (history kept).
- `_expectation_mark(variable, <schema columns>, expected BOOLEAN,
  active BOOLEAN, reason TEXT NOT NULL, changed_at)` — append-only; never
  deleted, an "unmark" appends `active = false`.

## CLI and Inspector surface

| Question | Inspector | CLI |
|---|---|---|
| Which function produces / consumes X? | `variable(X)` gains `producers`, `consumers` | `scidb vars X` |
| How is each key expected to vary? | `key_expectations()` | `scidb keys` / `scidb set-key KEY --shared/--per-parent [--values 1-5]` |
| Is X missing anything, or holding anything it shouldn't? | `expectation(X, problems_only=, by=)` | `scidb expected X [--problems] [--by subject]` |
| Mark a combination | `Mutator.mark_expectation(X, expected, reason, **keys)` / `unmark` | `scidb mark X key=val --expected\|--not-expected --reason "…"`, `scidb unmark …` |
| Which variables have which locations? (coverage matrix / attrition funnel) | `coverage(by=, variables=)` | `scidb coverage --by subject [--funnel]` |
| Where in the pipeline did this location drop out? | `follow(**keys)` | `scidb follow subject=S07 trial=2` |
| Are there NaNs / constant values inside cells? | `cell_profile(X, **keys)` | `scidb nans X [--by subject]`; `show --types` gains NaN-element counts |
| When was this combination excluded, and why? | `exclusions(history=True)` | `scidb exclusions --history` |

`follow` walks the pipeline graph in order (`inspect/graph.py`) and gives, per
variable, the status above plus the first variable where the location is
absent. Without Stage 7 it can say *absent*, not *why* (no data, failed, never
run): per-combination run outcomes are not stored today (they live only in the
`run_done` event and the logs).

Renderers take presentation from `RenderStyle` (`inspect/render.py`), never
literals. Every command supports the existing `--json`.

## Stages

1. **Owner + tables.** `expectation.py`, the two tables, unit tests for every
   status (all four cells of the 2×2, each `present_unexpected` source),
   D2's default rule, D3, D4 inheritance (same/deeper level only),
   exclusion-wins, zero-padded values. DEBUG logging of the universe size per
   key and of every inherited mark's source.
2. **Inspector + CLI: `vars` producers, `keys`/`set-key`, `expected`,
   `mark`/`unmark`, `exclusions --history`.** CLI tests via the existing CLI
   test harness.
3. **`coverage` and `follow`.** Batched (the N+1 rule): one projection query
   per variable, never per combination.
4. **`cell_profile` / `nans`** in scidb next to `inspect/preview.py`: NaN
   elements, all-NaN, constant, length per cell. Preview reports partial NaNs.
5. **GUI.** Right-click Variable → *Expected locations…* (popup: per-key
   settings strip + combination grid with row actions Expected / Not
   expected + reason), the two-part badge, Coverage view. All data via RPCs
   that call the Stage 2–4 Inspector methods. Manual-test section in
   `docs/gui-manual-testing-todo.md`.
6. **Picker reads the owner (D6).** `location_states` consults
   `expectation` for the denominator; amber/grey rules unchanged. Parity test:
   picker red == `missing_unexpected`. Open: how the picker shows
   `present_unexpected` (it has no such state today).
7. **Optional: persist per-combination run outcomes** (`_run_outcome`,
   history bundle, failures and no-data skips only) so `follow` says why.

Docs: `docs/claude/expected-locations.md` (rules, written with Stage 1),
observability-api-design.md rows added per stage.

## Open questions

- Command names (`follow`, `coverage`, `nans`, `mark`).
- Stage 7 in or out.
- Inheritance (D4) to a *coarser* downstream variable (a subject mean built
  from 9 of 12 trials): out of scope here; that is the pooling check.
