# Default variant selection, and what the Variants section is for

Concept refined with the user 2026-09-11. Companion to
`.claude/plan-plot-studio-variant-axis-fixes.md` (the bug findings that prompted
it). Prerequisite reading: `docs/claude/plot-variant-rows.md`,
`docs/claude/variant-selection.md`.

---

## The governing idea

**A plot opens on exactly one variant.** Plotting one thing is the common case;
comparing is the exception you opt into by adding a row. Today the panel opens on
"whatever the latest code left open", which for a variable with a swept parameter
means several series overlaid before the user has said anything.

Worked example, used throughout. `Var1` has **10 variants**: 2 function-body
versions x 5 `Parameter1` values (1-5).

| | today | after |
|---|---|---|
| opening rows | one, pinned `{CodeIsLatest: true}` | one, pinned `{Code:f = <latest>, Parameter1 = <first>}` |
| `Parameter1` | unpinned -> multi-level variant factor -> `default_roles` puts it on **colour** -> **5 overlaid lines** | pinned to one value -> **1 line** |
| Factors section | holds every unanswered variant axis | holds none of them; the row answered them all |

That last row also dissolves the `filterDelsys.config` / `filterDelsys.Fs`
complaint from the bug plan: `variants._answered` removes an axis the opening row
has pinned. The single-level-axis fix (Finding 1 there) stays independent and
still lands first — an axis that distinguishes nothing should not exist to be
pinned, and should never appear in the pin's label.

## The rule

Deterministic, statable in one sentence, and applied per axis:

- **Code axes** -> the latest function body.
- **Branch-param axes** -> the first value, in natural-sorted order of the values
  actually present (`source._ordered`, so `Parameter1` -> `1` and zero-padded IDs
  sort `01, 02, ... 10` rather than `1, 10, 2`).
- **Single-level axes** -> do not exist by then (bug plan, Stage 2).

Every axis is pinned independently by the same rule; there is no cross-axis
search. `Parameter2` alongside `Parameter1` is pinned the same way.

### The pin is applied blindly, and explains itself when it misses

Real data is ragged: `(latest code) x (Parameter1 = 1)` may never have been run.
The pin is **still applied** — a rule that quietly picks a different value to
avoid an empty figure is no longer predictable, and predictability is the whole
point of having a rule. Instead the empty figure explains itself (below).

In the 10-variant example every combination exists, so this never fires. It is
the exception path, not the common one.

## What the row reports: one mechanism, two symptoms

Both of the user's requirements are the same question — *what did this pin
actually resolve to against the data?* — so they are one readout on the row, not
two unrelated warnings.

### 1. A pin that spans code versions must be prominent

**This overrules a documented decision, deliberately.**
`docs/claude/plot-variant-rows.md` §3 currently says:

> A selection resolving through the latest flag is never counted: spanning
> ordinals across locations is what per-location "latest" *means*, and warning
> about it would cry wolf on the most ordinary state there is.

The user's position: if the function body actually used **differs between schema
locations**, that must be brought to attention *very prominently*. So
`spanned_code_axes` stops exempting the latest flag, and the report must name
**which locations are on which version** — "pools 2 versions" is not enough to
act on.

Note what this implies about the "latest" rule itself: the mixed state has to be
*reachable* for the warning to mean anything, so **latest stays per-location**
(every subject keeps contributing its own newest record; nobody silently
vanishes). The loudness is what changes, not the selection.

Anyone later re-reading §3 will find a rule that contradicts the code. §3 must be
rewritten, not just appended to, with the reason recorded.

### 2. An empty figure must say why it is empty

Required regardless of the pin rule — a figure empties from filters, a narrowed
schema key, or a stale saved spec just as easily. The explanation goes **on the
figure**, in the canvas area, where the figure would have been (user, 2026-09-11)
— not in the sidebar. It is the figure that is missing, so that is where the
question gets asked. It must name:

- **what is being attempted** — the resolved pin, axis by axis;
- **what actually exists** — the variant combinations present in the data, with
  their row counts;
- ideally, one click to adopt the nearest existing combination.

`capability.variant_summary` already measures `row_count` per row through the
same mask the renderer uses, so `row_count == 0` is the trigger and the existing
combinations come from the same frame. No new source of truth.

### Where each report lives

The two land in different places because they are different states: one is a
figure that is wrong-but-drawn, the other is no figure at all.

| report | canvas | sidebar row |
|---|---|---|
| pin spans code versions | **banner above the figure**, naming the versions and which locations hold each | amber tag — the row is where the fix is (pin a version, or split the row) |
| pin matched no rows | **the empty-state panel**, in place of the figure | `row_count == 0` tag, as today |

The spanning banner is on the canvas because "very prominent" was the
requirement, and an amber tag in a sidebar section the user may have collapsed is
not that. The row keeps its tag regardless: prominence belongs where the problem
is visible, the control belongs where the problem is fixed.

## The Variants section

- **Renamed back to "Variants"** from "Series". One row is one variant; that is
  the user's model and the name should match it.
- **Each row shows its Variable name** beside the Select button. With rows able
  to name different variables, a row that does not say which one is unreadable.
- **"+ Add Variant" is a two-step pick on the canvas**: click a Variable node,
  then select one variant of it. Step one may be **any plottable Variable**, not
  only the one being plotted — that is what makes overlaying `RawEMG` on
  `FilteredEMG` possible (it is `VariantSet.variable`, already in the model since
  Stage 3 of the 26.09.09 plan). Variables that cannot stack (wrong shape or
  wrong schema level) are non-clickable with the reason on hover, from the
  existing `stackable_with` refusal reasons.
- **A new row arrives already pinned** by the same default rule, so it is a valid
  single series the moment it is created (confirmed by the user 2026-09-11 as the
  intended behaviour). This narrows — but does not remove — the "an unfilled row
  is inert" rule in `plot-variant-rows.md` §3; that stays as the safety net for a
  row whose variable was picked but whose variant selection was cancelled.

## Decisions, and what is left open

Resolved with the user 2026-09-11: latest stays **per-location** with loud
reporting; the pin is applied **blindly** and the empty figure explains itself;
the empty-state explanation goes **on the figure**; "+ Add Variant" opens on
**any plottable Variable**, and the new row arrives **already pinned**.

Decided in the absence of a stated preference, and cheap to reverse: the spanning
warning is a **banner on the figure** as well as a tag on the row (rationale in
"Where each report lives" above).

Nothing else is open. The sequence below is ready to build.

## Stages

Sequenced after the bug plan's Stage 1 (diagnostics) and Stage 2 (single-level
axes), which this depends on.

1. **`default_pin` becomes a full pin.** Move the rule into
   `scistackplotdb`/`scistackplot` so a library caller opens on the same figure
   (CLAUDE.md NOTE 3). Tests: 2 versions x 5 values opens on 1 series, not 5; the
   pinned value is the first natural-sorted level; a second axis is pinned
   independently; a pin matching no rows still resolves rather than raising.
2. **The resolution readout.** `spanned_code_axes` stops exempting the latest
   flag and reports locations per version; `variant_summary` gains the existing
   combinations for the empty case. Tests: a mixed-version pin reports both
   versions and the locations; a zero-row pin lists what does exist.
3. **GUI.** Rename to Variants; variable name on each row; empty-state panel;
   prominent spanning tag.
4. **Two-step "+ Add Variant"** on the canvas, with non-stackable variables
   refused in place.
5. **Docs.** Rewrite `plot-variant-rows.md` §3 (the reversal above) and §1 (the
   opening state is now a full pin, not just the latest flag).
