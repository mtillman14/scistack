# Plan: PathOutput variant placeholders (no more artifact clobbering)

Fixes the limitation documented in stage 3
([plotting-leaf-nodes.md](../docs/claude/plotting-leaf-nodes.md), D4 section):
`PathOutput` templates cannot reference branch_params, so multiple variant
groups at one schema location resolve to the SAME artifact path — each group's
file (and stamp) overwrites the previous one. Records stay distinct; only the
files clobber.

## Syntax (nothing new to learn)

`PathOutput` resolution is already a literal `str.replace` of `{key}` per
combo-metadata key (`scifor/pathoutput.py::resolve` — NOT `str.format`, so
dots in names are fine). Branch params become just more keys, same `{}` form
as schema keys:

```python
PathOutput("plots/{subject}_{low_hz}.png")            # bare name, suffix-matched
PathOutput("plots/{subject}_{bandpass.low_hz}.png")   # namespaced (disambiguates)
PathOutput("plots/{subject}_{variant}.png")           # whole-group shorthand
```

- **Bare name** (`{low_hz}`): suffix-matched against the combo's group
  branch_params, exactly like `Variant(X, low_hz=20)` / `branch_param()`
  loading. Ambiguous across two namespaced keys → hard error at resolve
  prep naming both candidates (same contract as `AmbiguousParamError`).
- **Namespaced** (`{bandpass.low_hz}`): exact key.
- **`{variant}`**: an 8-char hex digest of the group's full canonical
  signature — for pipelines with many swept params where enumerating them in
  a filename is noise. `{variant}` of the empty (no-variants) signature is
  still stable, so templates using it don't break on single-group runs.
- No variants / empty group: bare and namespaced placeholders for absent keys
  stay untouched (current missing-key behavior) but WARN naming the available
  keys; `{variant}` always resolves.

## Mechanics (scidb injects; scifor stays generic — NOTE 3)

scifor's resolver already substitutes every combo-metadata key; the entire
feature is scidb putting per-group values INTO the expanded combo at Step 12
(aggregation) / rid expansion (full iteration), then keeping those keys out
of results and saves:

1. **Template scan** (scidb, before Step 12): collect placeholder names from
   all `PathOutput` inputs (`re.findall(r"\{([^{}]+)\}", template)`), minus
   schema keys, metadata_iterables keys, and `ColName`. Empty → zero new work
   (fast path unchanged).
2. **Injection at combo expansion**: for each expanded combo, resolve each
   requested placeholder from the group's branch_params dict — aggregation:
   the parsed `__vsig` signature (merged across split inputs; cross-input
   conflict on the same bare name → same ambiguity error); full iteration:
   `rid_to_bp[combo's __rid_*]` merged the same way. Inject as plain combo
   keys under the EXACT placeholder text (`low_hz`, `bandpass.low_hz`,
   `variant`). Values are path-sanitized: `str(value)` with `os.sep`, `"/"`
   and null bytes replaced by `-`.
3. **Tracking + stripping**: injected key names recorded on `_ForEachState`
   (`path_extra_keys`). They ride the combo through scifor (harmless there:
   not schema keys, so `_filter_df_for_combo` ignores them; they land as
   result-table columns) and are STRIPPED before Step 19 save and in
   `_apply_introspect` — they must not become dynamic-discriminator
   branch_params (the group's real namespaced bp already inherits via
   `combo_to_rids`) and must not pollute the user-facing table.
4. **No scifor changes at all** for resolution. (Verify: pre-built
   `_all_combos` with extra non-schema keys flow through scifor's loop
   without needing `extended_metadata_iterables` registration — flag for the
   first test to confirm.)

## Collision guard (the actual safety net)

Placeholders only help users who use them; the guard catches everyone else.
After combo expansion, when any `PathOutput` is present and has no
`{ColName}` token: resolve each combo's template(s) in preview and group by
resolved path. **Two combos → one path = hard ERROR** (not a warning — this
is silent file loss, and unlike the ragged-groups case there is a one-line
fix to name):

```
PathOutput 'plots/{subject}.png' resolves identically for 2 variant groups at
subject=S01 (bandpass.low_hz=20 vs 30). Add a distinguishing placeholder,
e.g. PathOutput('plots/{subject}_{low_hz}.png') or '..._{variant}.png'.
```

The message computes the differing bp keys between the colliding groups and
suggests the minimal placeholder. **`{ColName}` templates are covered too**
(updated 2026-07-07): `_resolve_for_columns` runs before prepare, so the
concrete column axis is known at guard time — the guard preview-resolves the
cross product of combos × columns via `PathOutput.resolve(combo, column)`.
Applies to draft AND finalized runs (drafts clobber too). Same-path combos
that are genuinely identical (1 group) never trigger it, and collisions whose
combos differ only in SCHEMA keys (a template that omits `{trial}`, say) are
deliberately NOT errors — that is pre-existing, possibly intentional
overwrite behavior; the guard fires only when the colliding combos differ in
variant identity (`__vsig_*` / `__rid_*`).

## Files

| File | Change |
|---|---|
| `scidb/src/scidb/foreach.py` | template scan; placeholder resolution helper (suffix match + ambiguity error + `{variant}` digest + sanitization); injection at both expansion sites (Step 12 aggregation branch, full-iteration rid expansion); `path_extra_keys` on `_ForEachState`; strip before save + introspect; collision guard after expansion |
| `scifor/src/scifor/pathoutput.py` | docstring only: mention scidb-layer branch-param placeholders |
| `scidb/tests/test_pathoutput_variants.py` | new (below) |
| `docs/claude/plotting-leaf-nodes.md` | replace the "known limitation" with the placeholder syntax + guard |
| `docs/claude/endpoints-viz-and-stats-design.md` | close the D4 limitation note |

## Tests (`test_pathoutput_variants.py`)

1. **Bare-name placeholder, aggregation:** two `low_hz` groups + `stat_`
   with `PathOutput(".../{low_hz}.pdf")` → two distinct PDFs, each stamped
   with ITS group's record_id + inputs (this is the stage-3 test we couldn't
   write).
2. **Bare-name, full iteration:** two variants per location + `plot_` with
   `.../{subject}_{low_hz}.png` → one file per (subject × variant).
3. **Namespaced form** resolves; **ambiguous bare name** (two fns sweeping
   `threshold`) → error naming both namespaced candidates.
4. **`{variant}`:** distinct 8-char tokens per group; stable across two runs
   (same signature → same digest); works with zero variants.
5. **Collision guard:** template without a distinguishing placeholder + two
   groups → error before any file is written; message names the differing bp
   key. Single group → no error (existing pipelines unaffected).
6. **Missing-key warning:** placeholder for a bp key absent from the group →
   file still written with the literal `{...}` retained + warning.
7. **Hygiene:** injected keys absent from the returned result table, saved
   record metadata, and branch_params; introspect columns unaffected.
8. **Draft mode:** placeholders + guard behave identically (draft stamps land
   in the per-group files).
9. **Sanitization:** a bp value containing `/` produces a `-` in the filename.

Run: `pytest scidb/tests/test_pathoutput_variants.py scidb/tests/test_plotting.py scidb/tests/test_stat_leaves.py scidb/tests/test_artifact_stamp.py -q`, then full sweep (user runs).

## Risks / watch items

1. **Extra combo keys through scifor** — expected to pass through untouched
   (non-schema keys are ignored by filtering), but this is the load-bearing
   assumption; test 7 exercises it first.
2. **Guard false positives** — `distribute` / multi-output flows where two
   result rows legitimately share a path? Guard compares *combos*, not rows,
   and only fires when the colliding combos' bp groups differ, so identical
   re-renders are safe. Flag any suite failure here for a scope decision.
3. **`{ColName}` + variants** remains unguarded (resolution order); the
   collision would need both features at once — documented gap, revisit if it
   bites.
4. **MATLAB (stage 4):** combos cross the bridge with injected keys already
   present, so MATLAB-side literal replacement should work unchanged; the
   guard runs Python-side in prepare, so MATLAB inherits it. Verify when
   stage 4 lands (`+scifor` PathOutput resolution parity).
5. **Identity:** a template edit (adding a placeholder) changes `__inputs` →
   new config identity → endpoints re-render once. Correct and expected;
   mention in docs.
