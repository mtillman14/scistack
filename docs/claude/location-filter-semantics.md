# Location filters: one meaning, four implementations

> Written 2026-09-14, ahead of the two-pane schema key picker
> (`.claude/plan-schema-key-picker-and-level-order.md`).
>
> Prerequisite reading: `schema-location-status.md` (what a location *is* and
> what green/amber/red/grey mean), `schema-hierarchy-contiguity.md` (why a
> location need not fill the schema).

A "location filter" answers one question — **which schema locations are in
play** — and it is asked in four places that cannot share code:

| Asker | Mechanics | Home |
|---|---|---|
| Which combos should `for_each` run? | a predicate over a `dict` combo | `scifor.locations` |
| Which rows should the figure draw? | a vectorised pandas mask | `scistackplot.reduce` |
| Which rows should the EXPORTED script draw? | that mask, emitted as source | `scistackplot.codegen` |
| Which boxes are ticked, and in what state? | a coverage walk over a tree | `locationSelection.ts` |

`scistackplot` depends on pandas, numpy and scistacklog only — **not** on
scifor — so the rule genuinely exists four times. This document is the single
statement of what it means, and the cases every implementation is tested
against are `location-filter-cases.json` beside it (see the last section). A
change made in one place and not the others is a silent wrong-data bug: the
figure, the export and the run would disagree about which subjects exist.

The third row is the one people forget. An exported script that filters
differently from the figure it came from is the worst version of this: the
picture was approved, and the data behind it quietly is not the same data.

## The two structures, and why it is not one

A selection is a pair:

```python
include: list[list[tuple[str, str]]]   # minimal covering set of PREFIXES
exclude_levels: dict[str, list[str]]   # a standing per-key RULE
```

### `include` — ragged, positional, a set of places

A prefix is the outermost steps of a location, each naming its key:
`[("subject", "01")]` is all of subject 01; `[("subject", "02"), ("trial", "3")]`
is one trial. Stored as the **minimal covering set**, so a fully-ticked subject
collapses to its own one-element prefix and a trial added to that subject
tomorrow lands *inside* the selection rather than silently outside it.

This is the only form that can express something ragged — all of subject 01,
plus trials 1-3 of subject 02 — which is what a tree of checkboxes means, and
what real datasets look like (subject 01 ran eight trials, subject 02 ran five).

### `exclude_levels` — a rule that outlives the data it was written against

`{"session": ["BL"]}` means *BL is out, everywhere, including in data that does
not exist yet*. Exploding that into prefixes would enumerate today's subjects
and freeze them: a subject added next month is not covered by any stored prefix,
so it would vanish **entirely** — not merely lose its BL. That drift is the
exact failure the minimal covering set exists to prevent, so the rule is stored
as a rule.

The two are not redundant. `include` cannot express a rule; `exclude_levels`
cannot express raggedness (it is a per-key list, so unticking *one* trial of
*one* subject would drop that trial from every subject). Every selection needs
both, and the pair is the whole selection.

## The rule

A location **L** (an ordered list of `(key, value)` steps) is IN iff:

1. `include` is empty, **or** some prefix in `include` covers L; **and**
2. no step `(k, v)` of L has `v` among `exclude_levels.get(k, [])`.

"Covers" and "among" both mean *matches under `value_spellings`*, and a null
`v` matches nothing at all — the two bullets below.

An **empty pair is inert**: it constrains nothing. That is deliberate and
load-bearing — "everything is selected" and "the picker was never opened" must
produce the *same* stored spec, or two identical figures would differ by
whether someone clicked into a dialog.

Five consequences worth stating, because each has a plausible wrong answer:

- **Exclusion wins over inclusion.** `include=[[("subject","01")]]` with
  `exclude_levels={"session": ["BL"]}` draws subject 01 minus its BL sessions.
  The rule is applied *after* coverage, never merged into it.
- **Values compare as text, through `value_spellings`.** Every selection here
  crosses JSON (and TOML, for a saved spec), so `1` and `"1"` are one value.
  One exception, and it is deliberate: **an integral number matches both of its
  spellings**, `1` and `1.0`. A schema key that round-tripped through DuckDB as
  a float reaches the plotting frame as `1.0` while the picker — whose tree
  comes from scidb — sends `1`, and raw text comparison selects nothing across
  that seam, in silence. A **zero-padded** string is not a number here: `"01"`
  matches only `"01"`, because `"01"` and `"1"` can be two genuinely distinct
  trials and which one is identity is scidb's decision
  (`schema-key-types.md`), never a shortcut taken in a comparison.
- **NULL at a key is not a level.** A null value matches no prefix step and is
  dropped by no rule. The two sides spell null differently — `None` in a combo,
  `NaN` in a DataFrame column, whose texts are not even equal — so this is
  stated rather than left to `str()`. Cross-cutting records make it routine:
  without it, the first level rule anyone writes would delete every one of them.
- **A step whose key is absent from a location is not a match.** A
  cross-cutting record saved at `subject` + `speed` with `timepoint` NULL has
  no `timepoint` step, so `exclude_levels={"timepoint": ["T1"]}` does not touch
  it. Reading prefixes *positionally* against the schema would match that value
  against the wrong key and select nothing, in silence — which is why a prefix
  names its keys.
- **Excluding every level of a key selects nothing**, and that is a legitimate
  thing to have typed. It is reported (see below), not corrected.

## Deriving the two panes' tri-state

The picker draws the same selection twice, and neither pane stores its own
checkbox state — both are **derived** from the pair, so they cannot disagree:

- **A tree node** is `full` when a prefix covers it and nothing beneath it is
  level-excluded; `none` when it is level-excluded or uncovered; `partial`
  when its children disagree. (`coverageOf`.)
- **A left-pane level** `(key, value)` rolls up the state of every location
  containing that step: `full` if all are in, `none` if none are, `partial`
  (amber) otherwise. (`levelCoverage`.)
- **A left-pane key** rolls up its levels the same way. (`keyCoverage`.)

Clicking an amber left-pane entry resolves it to one state or the other, which
means a write to **both** structures: turning a level back on clears its
exclusion *and* re-ticks the locations a ragged untick had removed. Turning it
off records the exclusion; the stale prefixes beneath it are harmless because
rule 2 is applied after coverage, and `normalize()` collapses them anyway.

## Combos are coarser than locations

The scifor half filters `for_each` COMBOS, which need not name every key the
selection does: a run iterating `subject` only, against a selection naming
`trial`, produces combos that are ANCESTORS of the selected locations. Such a
combo is KEPT -- some of what it covers was selected, and dropping it would
silently run nothing at all. This is the same reason the tree shows an ancestor
as `partial` rather than unticked; it is not a third rule, it is rule 1 read
against a combo that stops early.

The pandas half never meets this case (a row always carries every key it was
saved at), which is exactly why it is written down here rather than left to be
rediscovered when the processing tab runs something unexpected.

## Reporting (NOTE 2)

A filter that quietly removes everything is the failure mode this feature
introduces, so both implementations say what they did:

- INFO, always: how many combos/rows were kept out of how many, split by which
  clause dropped them (prefix vs level rule).
- WARNING when the filter keeps **zero** of a non-empty input: names the
  selection, because "the run did nothing" otherwise looks like a bug in the
  pipeline rather than a filter the user typed three days ago.

## Parity case table

The cases are **not written here**. They live in
`docs/claude/location-filter-cases.json`, and every implementation loads that
one file:

| Suite | Reads | Runs the cases marked |
|---|---|---|
| `scifor/tests/test_locations.py` | the JSON | `combo` |
| `scistackplot/tests/test_location_filter.py` | the JSON | `row`, twice — once against `reduce._location_mask`, once against the pandas `codegen` emits |
| `locationSelection.test.ts` (Stage 4) | the JSON | `tree` |

A table written out three times is a table that drifts: the suite whose author
forgot to copy a case keeps passing, which is the exact failure the parity test
exists to catch. Loading one file inverts that — a case added to the spec
**fails** every implementation that has not adopted it.

Each case carries an `applies_to` list, because not every case is meaningful
everywhere. A combo may be **coarser** than a location (the ancestor rule
above), which a DataFrame row never is; and `tree` cases are about checkbox
state rather than row survival. A case that only one side can express says so
rather than being quietly skipped.

When you add a case, say what it pins in `why` — that string is the assertion
message, so a failure reads as the rule it broke rather than as
`assert False == True`.
