# A selection below the iterated level is a variant

*Written 2026-11-05 with `.claude/plan-selection-variant-axis.md`. Read this before
touching `scidb.run.selection_axis`, `_invocation_pooled`, or the variant key.*

## The rule

A selection on a key the run **iterates** decides which calls happen. On a key it does
**not** iterate, it decides what each call **reads**. Only the second makes a variant.

| Run | Selection | Result |
|---|---|---|
| `[subject, minute]` | minute 6, later minute 5 | different output locations, no variant |
| `[subject]` | minute 6, later minute 5 | one location, **two variants** |
| `[subject]` | minute 6 of CB01, later minute 6 of everyone | one variant; CB01 is replaced |
| `[subject]` | nothing, then a new minute is added | one variant; the result is replaced |

## The axis is read off the rows

`selection_axes_of` compares each input before and after `narrow_inputs`. It never reads
how the selection was spelled, so `include` of minute 6 and `exclude_levels` of every
other minute are one variant. Selecting everything narrows nothing, which matches "an
empty pair is inert" in `location-filter-semantics.md`.

Only calls that kept a row count, so running one subject cannot make its other sessions
look narrowed.

- **One rule fits every call** (the normal case): every call gets it, including a subject
  that only ever had minute 6. Without this, one selection would scatter across cards and
  downstream groups.
- **No single rule fits** (all of subject 1, minute 6 of subject 2): each call answers for
  itself. A call with nothing narrowed has no axis, so it is the same variant as an
  unselected run.
- **No per-key rule reproduces a call** (minute 5 of session A, minute 6 of session B):
  the axis lists the kept locations (`rows`), and its label says `(per location)`.

A label names values, and adds a `#digest` whenever it abbreviates or could misread them,
so one label is always one meaning.

## How it is stored

An input edge `__selection` to a record of type `__selection__`, with `SelectionAxis.to_json`
in `_constant`. A call that narrowed nothing has no edge, so its invocation id is unchanged.

| Reader | How it follows |
|---|---|
| `invocation_id` | the edge (`provenance_save._selection_bindings`, one reader of `__selection`) |
| branch params | `{fn}.__selection` = the label (`_build_upstream_closure`, `derived_branch_params`) |
| variant key, cards, downstream split, `Variant(...)` pins, `AcrossVariants` | read branch params |
| call id, canvas node, `constants` | never see it: not a constant, skipped by the config reader |
| skip gate | `_find_skip_gate_record(target_selection=)` |
| predictor | `cfg["selection"]`: rebuilds the edge and keeps the rows `SelectionAxis.admits` |

Pin one with `Variant(X, **{"fn.__selection": "minute=6"})`, or the bare `__selection` name
when one step in the chain has it.

## Added data: `_invocation_pooled`

The variant key used to include every consumed location, so a new minute made a second
current record. It now counts an input that pooled whatever its location held **by name and
type**, not location. Written only when aggregating with no `where=`, and never for `Fixed`:

- `where=` chose among the rows, so its locations do identify the computation.
- `Fixed` names its record, so two pins are two computations.

The marker is not identity, and an invocation without one keeps the older rule. So outputs
saved before 2026-11-05 coexist with a re-run once if their inputs changed: delete them first.
`consumed_input_schema_ids` still returns every location, because a `where=` load matches on it.

## MATLAB

The generated command's per-key bound keeps only iterated keys (`schema_selection.report(iterated=)`).
On any other key it became a `where=` that filtered while loading, before `locations=` ran, so
the run could not see what was narrowed and MATLAB would have recorded no axis where Python did.

## Limits

- A location a selection did not run reads red, as any partial run does. That includes a
  ragged selection, whose configs each predict every location.
- The Variants popup and Plot Studio show the raw name `fn.__selection`.
