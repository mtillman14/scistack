# Plan: one owner for a function node's output order

Status: DRAFT, awaiting approval (2026-10-28).
Follows `.claude/plan-for-columns-groups.md` (split mode pairs column groups
with outputs BY POSITION, which made output order carry meaning).

## Problem

"Output order" is one concept: the order of `outputs=` in the for_each call
(for a normal multi-output function, the order of its return values; for a
for_columns split, the group pairing). Today it has several independent
derivations, none of them the call's actual `outputs=`:

| Where | Order used |
|---|---|
| Python function node handles (`graph_builder.build_function_nodes`, ~1870) | alphabetical: `sorted(actual_outputs)` |
| MATLAB function node handles (same, ~1826) | MATLAB signature order (param names) |
| MATLAB canvas run (`matlab_command_service`, ~715) | signature, or edge order re-sorted by signature (`_sort_inferred_by_params_order`) |
| Python canvas run (`api/run.py`) | one output per call; a split uses node handle order (`run_output_types`) |
| Picker / output tooltips | node handle order (`plan_groups`) |
| Recorded | nothing: `_invocation_output.output_num` is NOT the `outputs=` position (slots append across runs, `provenance_save` "Fix B"; a distributed run's `output_num` is the slice index) |

Concrete bug: a code run `outputs=[TenMWT, SixMWT]` with groups `[SIX, TEN]`
saves 6MWT data into `TenMWT`. The canvas draws handles `SixMWT, TenMWT`
(alphabetical), so the tooltip and picker claim `SixMWT ← 6MWT`, and a canvas
re-run swaps them.

## Design

**The fact is recorded once, by the run, in scidb.** Every for_each save
records the call's `outputs=` order. Every reader (graph build, both canvas
run paths, picker, tooltips) reads it through ONE function; nothing else
derives it.

- **Record:** `_invocation.outputs VARCHAR[]`: the output TYPE names in
  `outputs=` order. Additive backfill, like `_invocation.for_columns` before
  it. Descriptive, NOT an identity term (the call's outputs already determine
  its records; adding them to `invocation_id` would re-identify every call).
  Written on the one save path both languages use (`_save_results` →
  `record_run`), so MATLAB records it through the bridge with no MATLAB change.
- **Owner (scidb):** `provenance_query.call_site_output_order(duck, fn,
  call_ids) -> {call_id: [types]}`: the most recent recorded order per call
  site. Old invocations without the column answer nothing (not "alphabetical").
- **Owner (GUI):** `domain/output_order.node_output_order(recorded, wired,
  declared, language)`: the order a function node draws its output handles.
  1. recorded order (filtered to outputs still wired), then
  2. wired outputs not in it, in **edge order** (the order the output edges
     were drawn; manual edges keep insertion order), and
  3. for a MATLAB node with no record: its signature order (today's rule).
  `graph_builder` calls it for every function node in both languages;
  `sorted(actual_outputs)` goes away.
- **Consumers read the node's handle order, never re-derive:**
  - Python canvas run: `run_output_types` (already reads the request's
    `output_types` = node handles).
  - MATLAB canvas run: `generate_matlab_command` uses the node's handle order
    (mapped to classes via `matlab_param_to_class`) instead of its own
    signature re-sort; `_sort_inferred_by_params_order` remains only as the
    no-record fallback inside the owner, not in the service.
  - Picker and tooltips: unchanged (already node handle order).

**Decision taken as proposed (user to confirm):** a Python node that has
never run has no declared output order, so its order is the order its output
edges were drawn; the picker's ↑↓ reorders groups against it.

## Stages

### Stage 0: diagnostics (tests that pin today's behaviour, logging)
- Test: a code run `outputs=[TenMWT, SixMWT]` → graph build draws handles
  `[TenMWT, SixMWT]` (expected to FAIL today: alphabetical).
- Find out what a canvas run of an ordinary two-output PYTHON function does
  today: one for_each per output type, and `deduplicate_variants` keeps one
  target per variant, so possibly only the first output is ever written.
  Add an INFO line in `api/run.py` naming the outputs each call writes, and a
  test that records the current behaviour. Report before changing it.
- Find out how a MATLAB split function (one output in its signature, two
  output edges) renders: `matlab_param_to_class` has one param for two
  classes, so the `orphan` warning at graph_builder ~1841 may fire and an
  edge may not render. Test + log.
- Confirm manual edges come back in insertion order (`pipeline_store`).

### Stage 1: record the order (scidb)
- Additive backfill of `_invocation.outputs`; written in `record_run` from the
  call's output type names (`state.output_names` → variable type names via
  `_output_type_label`, not result column names).
- `provenance_query.call_site_output_order`.
- Tests (scidb): a run with `outputs=[B, A]` records `["B", "A"]`; a re-run in
  the other order updates "most recent"; a pre-column invocation answers
  nothing; `invocation_id` unchanged (identity regression); MATLAB-path parity
  via `scimatlab/tests` (bridge save).

### Stage 2: one owner for node handle order (GUI)
- `domain/output_order.py: node_output_order` + `graph_builder` uses it for
  both languages (MATLAB keeps param-name handles, ordered by the same owner).
- Tests: recorded order wins; a newly wired output appends in edge order;
  never-run Python node = edge order; never-run MATLAB node = signature order;
  the Stage 0 failing test passes.

### Stage 3: consumers
- MATLAB canvas run reads node handle order (drop the service-side re-sort).
- Depending on Stage 0: a Python canvas run of an ordinary multi-output
  function writes all its outputs in one call in handle order (generalising
  `run_output_types`), if Stage 0 shows it writes only one today.
- Tests: generated MATLAB command lists outputs in handle order for a split
  and for a normal two-output MATLAB function; the TenMWT/SixMWT scenario
  end to end (code run → tooltip pairs SIX with TenMWT → canvas re-run keeps
  it).

### Stage 4: docs
- `docs/claude/for-columns-iteration.md` (replace the "Open" note),
  a short `docs/claude/output-order.md` (the owner table above, resolved),
  GUI manual-test entry.

## Testing note
User runs pytest (one package at a time: scidb, scimatlab, scistack-gui) and
MATLAB. Node/npm checks and bundle rebuilds by Claude if any frontend changes
(none expected).

## Stage 0 results (2026-10-28, user-run)
- Python handles are alphabetical (xfail pinned).
- A canvas Run of an ordinary two-output Python function makes ONE for_each
  call writing ONE output (`[['OrderTen']]`): its other output is never
  written by a canvas run. Existing bug; Stage 3 fixes it (all node outputs
  in one call, handle order).
- A MATLAB split function draws only its signature's one handle.
- Manual edges have no guaranteed read order (`intent_store.manual_edges`,
  no ORDER BY). Decided (user): never-run Python output order = edge
  statement `stated_at` order; re-drawing an edge moves it last.
