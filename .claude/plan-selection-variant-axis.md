# Plan: a schema selection below the iterated level is a variant (Option B)

User decision 2026-11-05: selections become an explicit variant axis, and only
the part that changes what a call READS counts. Rules live in
`docs/claude/selection-variant-axis.md`.

## Stages

1. `scidb/run/selection_axis.py`: the owner. What a selection narrowed, from the
   rows before and after, never from how the selection was spelled.
2. `__selection__` internal record + `__selection` edge: identity, coordinate,
   downstream split, cards and pins all follow from existing owners.
3. Save, skip gate, predictor read and write it.
4. `_invocation_pooled`: a plain aggregated input's locations stop defining the
   variant, so added data updates the result.
5. MATLAB command: the per-key bound stops narrowing keys the run does not
   iterate, which hid the narrowing from the axis.
6. Tests in each layer, doc, memory.

## Decisions challenged

| Candidate | Why rejected |
|---|---|
| Reserved constant edge | `constants` has ~100 readers; GUI re-runs pass them to the function |
| `_invocation` column | Forces a database rebuild; an edge needs no schema change |
| Label from the selection text | `minute=6` as include vs exclude rule would be two variants |
| Label per call | CB04 holding only minute 6 would land outside `minute=6` |
| Run-option label | Currency rule supersedes old options; these must coexist |
| Superset rule for added data | `where=L` then no `where` looks identical, and would supersede |
| Drop consumed for every record | `where=` and `Fixed` rely on it |

## Accepted limits

- A ragged selection (not one per-key rule) gets a per-call axis, and reads red
  at locations it did not run, as any partial run does.
- Outputs saved before this have no pooled marker, so they coexist with a re-run
  once.
- Not visually checked in the GUI; the axis shows as `fn.__selection`.
