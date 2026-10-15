# Saved plots remember their code version; no silent pooling

Drafted 2026-10-15. Status: ALL 5 stages built 2026-10-15; npm test + tsc pass and both bundles were rebuilt; pytest NOT yet run (the user runs it). Deviation: a deleted frozen version gives a note and falls back (remaining frozen versions, else current) with the prompt, rather than an empty figure.

## What happened (from scidb.log, 2026-10-15)

- 12:16 and 12:21: two plots of `CosmedData` were saved while only one version
  of `load_cbrace` existed. Each stored `variant_sets: [{"selection": {}}]`,
  an empty row 0. **No version was recorded**, because there was no code axis
  to pin.
- 12:51: `load_cbrace.m` was edited and re-run. That gave 714 more records
  (35700 rows: 17850 from v1 and 17850 from v2).
- 12:54 and 12:57: the saved plots were opened again.
  - **The figure** goes through `apply_variant_sets`. There it finds no
    *defined* set, so `_apply_default_pin` applies `{CodeIsLatest: True}` and
    draws **v2 only** ("keeps 17850 of 35700").
  - **The banner** comes from `capability.py`. It treats an empty row 0 as
    *stated* (`variants.is_stated`), so its `row_mask` matches **all 35700
    rows**. `spanned_code_axes` then finds v1 + v2, and the banner reads
    "CosmedData mixes v1 (…) | v2 (…)".

  The two places give different answers to "what does an empty row 0 select?"
  (NOTE 4). The banner reports pooling that the figure does not do.
- A related leftover: `_apply_default_pin` filters the rows but keeps
  `Code:load_cbrace` in `table.factors` with levels `[v1, v2]`. As a result it
  picks up `iterate` (`complete_assignment`: unknown factor → ITERATE), and the
  figure title reads "Code:load_cbrace=v2".
- A separate frontend bug: opening a saved plot **writes its variant sets into
  the intent store** (`intent_store wrote ('CosmedData','variant_selection','#0')`
  at 12:54:15 and 12:57:57). `openedVariantSets` is reset only when `variable`
  changes, not when a saved plot is opened, so the saved rows look like an
  edit. This overwrote the variable's stored pins with the saved plot's
  empty row.

## Decisions (user, 2026-10-15)

1. **Save freezes the version.** A saved plot records exactly which code
   version(s) were on screen, by **function hash** and not by ordinal. Ordinals
   renumber after a real Variants-popup Delete, so a stored "v2" could quietly
   start meaning something else.
2. **The opposite mode is "Follow latest".** It is reached by answering Yes to
   the prompt. In this mode the saved plot never freezes and never prompts, and
   later re-saves keep the mode.
3. **Yes appends a new saved-plot version** set to Follow latest. The plot does
   not show as "modified". No answers "No": the plot stays on the frozen
   version and nothing is written.
4. **Older saved plots that recorded no version open on latest, with no
   prompt.** Nothing is back-filled ([[feedback_beta_no_deprecation]]).
5. **Pooling versions only ever happens when the user explicitly selects
   several versions in Variants.** Every other path draws one version per
   location.

## Stages

### Stage 1: one owner for "what an unstated row selects" (scistackplot)
- `variants.row_mask` (or a new `effective_selection(variant, index, table)`)
  becomes the single answer. An empty row 0 means the source's `default_pin`.
  `capability.py` and `apply_variant_sets` both use it. `_apply_default_pin`
  becomes a call into it rather than a second rule.
- When the default pin answers the code axes, they leave `table.factors`
  (same rule as `_answered`). The figure stops showing "Code:fn=v2", and the
  axis stops picking up ITERATE.
- **One variant means no variant in the title** (user, 2026-10-15). Today a
  single defined row adds a one-level `Variant` factor. It defaults to
  ITERATE, so `DisplayText.figure_title` titles the figure
  `Variant=CosmedData · load_cbrace v2` (log line 4202). The rule: a variant
  factor (`is_variant`, i.e. `Variant` or `Code:*`) with exactly one level
  never appears in the figure title. The rule lives in `figure_title`, which
  owns the title. `figure_key` is left alone, since save-all file names and
  the endpoint `for_each` keys consume it. Single-level schema keys stay in
  the title. Tests: a one-row pinned spec gives a title with no `Variant=`;
  two iterated rows still say `Variant=…`.
- Logging: one INFO line per resolve naming the effective selection for each
  row and where it came from (`stated` / `source default` / `frozen`).
- Tests (`scistackplot/tests/test_variant_sets.py`):
  - an empty row 0 over a two-version table reports `spans == {}` and the
    same `row_count` as the figure;
  - the code factor is absent from the resolved factors;
  - a guard test that the capability count and the figure's row count agree
    for every row shape (empty, latest flag, named ordinal, several rows).

### Stage 2: freezing on save (scistackplotdb.saved, the owner)
- `VariantSet` gains `follow_latest: bool = False`, which is plain data, so
  scistackplot stays scidb-free. The envelope gains
  `frozen_code: {row_index: {"Code:fn": [hashes]}}`, written by
  `scistackplotdb.saved` at save time from the rows actually drawn. Its source
  is the table's hash column next to `CodeVersion`. If that column is missing,
  `variant_identity_batch` already returns `function_hash`, and we expose it
  as a hidden column.
- A **set** of hashes, not a single one: if per-location latest spans v1/v2
  at save time, the frozen plot reproduces exactly that, as newest-per-location
  restricted to the frozen set.
- On open, `scistackplotdb.saved.open` maps hashes back to today's ordinals.
  It returns the selection plus `newer: [{column, function, frozen: [v1],
  newest: v2, locations_with_newer: n}]`. A frozen hash that no longer exists
  (deleted) produces a note and the empty-figure explanation. It never falls
  back silently to latest.
- Logging: save logs what was frozen; open logs frozen → resolved ordinals
  and what is newer.
- Tests (`scistackplotdb/tests/test_saved_plots.py`, two-version fixture):
  - save on v1, add v2, open → draws v1 rows only, `newer` names v2;
  - `follow_latest` → draws v2, `newer == []`;
  - a mixed per-location freeze reproduces its locations;
  - after deleting the frozen version, the plot opens with a note and is empty;
  - an old envelope without `frozen_code` → latest, no prompt.

### Stage 3: "Switch to latest?" (GUI service + handler)
- `plot_saved_open` passes `newer` through.
  `plot_saved_follow_latest(plot_id)` appends a version with
  `follow_latest=True` and the frozen entries removed. It reuses
  `VersionedStore`, so no new storage.
- Tests in `scistack-gui/tests/` for the round trip.

### Stage 4: frontend
- When `newer` is non-empty, show a modal prompt: "Newer data exists:
  load_cbrace v2 (N locations). Switch this saved plot to the latest version?
  [Switch to latest] [Keep v1]". Yes calls the new RPC and reloads. No just
  closes it.
- Fix the pin-overwrite bug: opening a saved plot re-baselines
  `openedVariantSets`, so it never writes intent pins. Add an npm test.
- The Variants row shows "v1 (frozen)" or "latest (following)", so the mode
  is visible.
- Rebuild both vite bundles ([[project_frontend_bundle_rebuild]]). Add a
  manual-test entry in `docs/gui-manual-testing-todo.md`.

### Stage 5: docs
- `docs/claude/saved-plots.md`: freezing, follow-latest, and the prompt.
- `docs/claude/plot-variant-rows.md` §1/§3: unstated row = source default,
  with one owner, rewritten in place.

## Diagnostics if anything still looks pooled
Every resolve logs the effective selection per row and its source (Stage 1).
A banner that disagrees with the figure would then show up in the log as two
different row counts for the same row.
