# Plot Studio: Structure > Combine, where a combined factor replaces its source

Status: implemented 2026-09-26 (all 5 stages), uncommitted; pytest unrun; GUI §0zze unchecked. Replace-only (no "Keep
alongside" mode; see Goal).

## Stages

1. **Backend** (scistackplot): `LevelGroup.active`, depth = source − 0.5,
   `combined_from` / `combined_into`, source forced to COLLAPSE in
   `complete_roles`, validate refusal, capability `slot` / `alternatives` /
   `selected`, logging, pytest.
2. **Switching**: `combine.ts switchSlot` + tests; the dropdown in the
   Grouping list and Factors rows.
3. **Combine section**: moved above Grouping, relabelled, one-line collapsed
   rows with persisted open state (mount/unmount diagnostic first).
4. **Bucket-first editor** (separable).
5. Docs, manual-testing §, rebuild both vite targets.

## Goal

"+ Group levels of…" moves out of Structure > Grouping into its own section,
**Structure > Combine**, placed above Grouping. A combined factor (e.g.
`Cohort` from `subject`) is an alternative **view of its source's slot**, not
an extra factor:

- The Grouping and Factors lists have ONE row per slot. Its name is a
  dropdown: `subject`, `Cohort`, and any other combines of `subject`.
- Whichever option is selected holds the slot: its role, its position in the
  grouping order, and the colour tag. The others do not exist in any list, in
  the table, or in the figure.
- Switching the dropdown moves the slot to the new choice unchanged, in both
  directions. Choosing the source again brings the source back with all its
  levels, including any rows the combine had dropped.
- The only trace of a replaced source is statistical. Its rows are combined
  by an implicit **Collapse** (averaged within each source level first), so
  it can be the sample the error bars are computed over, and it appears in
  Statistics > Summary / Show sample. It never appears in Grouping or Factors.

Name: "Combine". "Merge" collides with `scidb.Merge` (variable join).
"Rename" and "Relabel" collide with the cosmetic Labels/aliases section.

**Combine always replaces.** Example: stim1–4 → IS_STIM, sham → SHAM. The
IS_STIM value is the mean of the four per-condition means (nested), or of
every row when "Weight by N" is on. Keeping the source alongside a coarser
label (subjects bracketed by Cohort) is deliberately NOT a Combine mode
(decided 2026-09-26). It is "Group by…" on a recorded subject-level variable
(e.g. `Demographics.Cohort`), which can be reused across plots, endpoints
and pipelines. The Combine hint points there.

## Why the current figure shows odd x ticks (the bug this also removes)

A bucket today is an extra factor with `depth=None`. Both depth readers send
`None` innermost: Python `PlotSpec.ordered_groups.depth_rank` (`spec.py:1074`)
and TS `groups.ts deepness(null) = +Infinity`. That puts the bucket *inside*
its own source, giving one bucket tick per source bracket.
`roles.collapse_order` treats the same factor as outermost, so two readers
already disagree. Replacing the source (it never coexists with the combined
factor) removes the conflicting state. The depth fix below makes the rest
consistent.

## Data model (scistackplot, one owner: `groups.py`)

- `LevelGroup` gains `active: bool` (new combines start `True`). At most
  one active combine per source. `roles.validate` refuses two, naming both.
- `apply_level_groups` applies **active** groups only, and for each one:
  - adds the combined factor with **depth = source depth − 0.5**, which is
    just above its source. It lands in the source's place relative to every
    other key, collapses after its source, and `line_recurrence` correctly
    answers "a subject does not recur across Cohort" (today: "cannot be
    told"). When the source has no depth (variant/field), the depth is
    `None`, plus an explicit "outside its source" rule in `ordered_groups` /
    `placeGroupLayer`.
  - sets `FactorInfo.combined_from = source` on the new factor and
    `FactorInfo.combined_into = name` on the source.
- `FactorInfo.depth` widens to `int | float`. I'll audit every `.depth` /
  `factor_depths` reader with `findReferences` first; they only compare.
- `roles.complete_roles` is the one owner of "a replaced source is
  collapsed". A source with `combined_into` set always gets `COLLAPSE`,
  whatever the spec says, and is removed from `groups` / `color`. An INFO log
  fires if the spec had said otherwise.
- The capability report marks each factor's slot: `slot` (source name),
  `alternatives` (source plus every combine of it, in declaration order) and
  `selected`. The GUI renders the dropdown from this and never works it out
  itself.

## Switching (one owner for the spec edit)

A pure TS function `switchSlot(spec, source, choice)` in a new
`combine.ts`, the same kind of spec edit as `placeGroupLayer`:

- sets `active` on the chosen combine (or none, when the source is chosen)
  and clears it on the others of that source;
- moves the slot: `roles[new] = roles[old]`, then removes `roles[old]`;
  replaces the name in place in `groups`; `color` follows. The source's own
  role is never stored while it is replaced; `complete_roles` supplies
  COLLAPSE.

Python keeps the invariant (`complete_roles` + `validate`), so a saved plot
or endpoint spec that skips the GUI still gets the same figure.

On creation, the new combine becomes active immediately and takes the slot
through `switchSlot`. Every level starts in its own bucket named after
itself, so the figure's data is unchanged until you actually combine levels.

Removing a combine (✕) while it is active switches the slot back to the
source first.

## UI: Structure > Combine

- `<Section title="Combine">` above Grouping, with the ⓘ hint: "Combine a
  factor's levels into fewer, averaging each combined group, e.g. four stim
  conditions into one STIM. The result replaces the factor everywhere; switch
  back from its name in Grouping or Factors. To keep the original levels and
  group them by a label, record the label as a variable and use Group by…."
- One row per combine, then "+ Combine levels of…" (the current
  `BucketAdder`, relabelled). It lists sources, including ones currently
  replaced, so a second combine of `subject` can be added.
- Rows are **collapsed by default**, one line each:
  `▸ Cohort ← subject · 10 → 2 · ✕`. A newly added combine opens expanded.
  The open state lives in PlotStudio view state keyed by `source::name`,
  not in `useState` in a row keyed by array index, which resets on remount.
  To confirm the remount theory before relying on it: a console.debug on
  `LevelGroupEditor` mount/unmount.
- **Bucket-first editor** (replaces today's one text box per level). The
  expanded row lists the combine's buckets, e.g. `STIM`, `SHAM`, then
  `+ Add bucket`. Each bucket shows its member levels as ticks over the
  source's levels. Shift-click ticks a range; a level can belong to at most
  one bucket (ticking it in one bucket unticks it in another). Levels in no
  bucket are listed under "Unused (dropped)", unless "Keep the rest as ___"
  is on. It writes the same `LevelGroup.mapping`, so the backend doesn't
  change. This is a separable stage and can be deferred without affecting
  the rest.
- Grouping keeps "Group by…" and the grouping list. Buckets move out of it.
- Grouping list and Factors: a slot with alternatives shows its name as a
  `<select>`, with the source first and then its combines. A slot without
  alternatives stays plain text.

## Logging

- groups.py INFO: `combine 'Cohort' replaces 'subject' (depth 1 -> 0.5): 10 levels -> 2`.
- complete_roles INFO: `'subject' is replaced by 'Cohort' — collapsed`.
- validate refusal (two active combines of one source) names both.

## Tests

scistackplot (`tests/test_level_groups.py`, extended):
- only active groups apply; an inactive group changes nothing, and dropped
  rows come back when it is inactive;
- the combined factor's depth = source − 0.5; `combined_from` / `combined_into`
  are set;
- the replaced source is COLLAPSE even when the spec says GROUP/ITERATE, and
  is removed from `groups` / `color`;
- a bar of Cohort with subject replaced gives nested means; the sample is
  subject; `collapse_order` = [.., subject, (Cohort if collapsed)];
- the x plan never contains the source; `line_recurrence(subject across Cohort)`
  is False;
- validate refuses two active combines of one source;
- the capability report gives `slot` / `alternatives` / `selected`;
- codegen export matches the preview for a combined figure (existing parity rule).

Frontend:
- `combine.test.ts`: `switchSlot` round trip source → Cohort → source
  restores roles, groups order and colour exactly; switching between two
  combines; removing the active combine.
- `sidebarGroups.test.ts`: Combine is a Structure section above Grouping.
- `combine.test.ts`: bucket-editor helpers. Ticking a level in bucket B
  removes it from bucket A; a shift-range; bucket order is kept in the
  written mapping (it decides legend order); unused levels are dropped
  unless there's a catch-all.

## Docs

- `docs/claude/plot-studio-controls.md`: Combine row; Grouping row updated.
- `docs/claude/grouping-and-collapse.md`, `synthetic-factors.md`: slot model,
  depth rule, and the implicit-collapse invariant.
- `docs/gui-manual-testing-todo.md`: new § (create, combine two levels,
  switch back and forth, collapsed rows stay collapsed, x ticks).

## Rebuild

Both vite targets (extension webview + static), per the bundle-rebuild trap.
