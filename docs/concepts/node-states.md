# Node Run States: Green, Grey, Red

Every function node and variable node in the pipeline GUI has a **run state** that tells you whether its outputs are current. This document defines the exact rules that determine each state.

---

## The Three States

| State     | Meaning                                                                             |
| --------- | ----------------------------------------------------------------------------------- |
| **Green** | Every expected combo has been run and all outputs are up-to-date                    |
| **Grey**  | Some combos have been run successfully, but others are missing (partial completion) |
| **Red**   | Never run, or at least one combo's output is stale (inputs or function changed)     |

---

## Function Node State

A function node's state is computed by `scihist.check_node_state`. It enumerates **actual combos** (output records that exist in the DB) and **expected combos** (combos that should have been run), then classifies each.

### Combo States

Each individual (schema_combo, branch_params) pair has one of three states:

| Combo State  | Condition                                                   |
| ------------ | ----------------------------------------------------------- |
| `up_to_date` | Output exists **and** full upstream provenance is unchanged |
| `stale`      | Output exists but something upstream has changed            |
| `missing`    | No output record exists for this combo                      |

### Staleness Check (for `up_to_date` vs `stale`)

Two methods are used, in priority order:

**1. Lineage-based** (used when the output was saved via `scihist.for_each`):

- Function hash mismatch → stale
- Any upstream input record_id has since been superseded → stale
- Otherwise → up_to_date

**2. `__fn_hash` fallback** (used when the output was saved via `scidb.for_each`, which does not write lineage records):

- `__fn_hash` in version_keys mismatches current function hash → stale
- If `__upstream` record_ids are stored: any superseded → stale
- Otherwise: if any upstream input was re-saved _after_ the output timestamp → stale
- Otherwise → up_to_date

### Expected Combos

The expected set is determined by:

1. **Variable-input functions**: For each registered pipeline variant, query the DB for all (schema_id, branch_params) combinations of the input variable types. Any input combo that exists in the DB is expected to have a corresponding output.

2. **PathInput-only functions** (no DB-variable inputs): scidb/scihist.for_each writes the full expected combo set to `_for_each_expected` before execution begins (before any skip_computed filtering). `check_node_state` falls back to this table when no variable-input combos can be found.

   > **Key implication**: `_for_each_expected` is written at the _start_ of a run, reflecting all combos that were _attempted_. If a combo raises an exception mid-run, it is still in `_for_each_expected` as expected but will be absent from the output records → detected as `missing`.

### Aggregation Rules

| Counts                                      | Node State                       |
| ------------------------------------------- | -------------------------------- |
| `stale > 0`                                 | **Red**                          |
| `missing > 0` and `up_to_date == 0`         | **Red** (never successfully run) |
| `missing > 0` and `up_to_date > 0`          | **Grey** (partial completion)    |
| `missing == 0` and `stale == 0`             | **Green**                        |
| No combos at all (never run, no input data) | **Red**                          |

---

## Variable Node State

Variable nodes inherit their state from the function that produces them via DAG propagation:

- A variable node's state equals the **effective state** of its producing function node.
- The effective state of a function is the **minimum** of its own state and all its input variable states (green > grey > red).
- Variable nodes with no known producer (i.e., not connected to any function in the current graph) default to **green** if they have records in the DB.

---

## Propagation

`check_node_state` handles two kinds of upstream change:

### Data-change propagation — **deep** (handled by scihist)

When an upstream record is re-saved, the staleness **cascades through the entire lineage graph** down to every descendant, regardless of depth or DAG shape (chain, fork, join).

Implementation: `_has_superseded_ancestor` performs a BFS across `_lineage` rows starting from the output's `record_id`, walking both `variable` and `rid_tracking` input entries. For each visited record, it checks whether the current latest variant equals the record_id referenced in the downstream's lineage. If any ancestor has been superseded, the descendant is stale.

Covered scenarios:

- `step1 → step2 → step3`: resave the ultimate input → every step turns red for the affected combo.
- `fork_left` and `fork_right` both consume the same root; re-save the root → both branches red.
- `join_sides(left, right)` → deep cascade from root reaches the join via the branches.

### Function-code-change propagation — **shallow** (GUI layer concern)

`check_node_state(fn, outputs)` can only detect a function-hash mismatch for the _queried_ function, because it receives no handle on ancestor functions.

If a user edits an ancestor function's code but hasn't re-run it:

- The ancestor's own node turns red (its stored lineage hash mismatches the current in-memory `fn.hash`).
- Descendants' **own** state stays green — scihist cannot introspect current ancestor functions from inside `check_node_state`.
- The GUI is responsible for propagating the ancestor's red down the DAG via its own graph walk.

Workaround: once the ancestor is re-run, it produces a new `record_id`. That new record_id then cascades as a **data change**, and the deep walk above turns every descendant red automatically.

> **Note on `check_node_state` scope**: it returns a function's _own_ state. A downstream function may be green (ran correctly for all available inputs) even when its upstream is grey. The GUI layer combines these to show the downstream node as grey.

---

## Constants

"Constant" means two very different things in this system. Both influence node color, but through independent mechanisms.

### 1. Runtime constants — a **variant discriminator**

Values passed via `inputs={"scale": 2.0}` or wrapped in `scidb.constant(...)`. Stored in `version_keys` on every output record. A run with `scale=2.0` and a run with `scale=3.0` produce **two independent variants** of the same function — they never shadow each other.

Consequences for state:

- Changing a constant's value at a call site does _not_ make the old combo stale. It creates a new variant; the old one stays green.
- `check_node_state` enumerates expected combos **per variant** (via `list_pipeline_variants` for scidb-saved outputs, and via `_lineage` rows for scihist-saved outputs). If `scale=2.0` has 4 of 4 combos and `scale=3.0` has 2 of 4, the node is **grey** (aggregate over all variants).
- Editing the function body that _reads_ the constant still changes `fn.hash` → the normal fn-hash stale path applies.

### 2. Pending constants — a **GUI-only grey downgrade**

A pending constant is a value the user has dragged onto a constant node in the canvas but has **not yet run**. It lives in the `_pipeline_pending_constants` table (scistack-gui only) and has no corresponding record in `_record_metadata`.

Rule (`scistack_gui/domain/run_state.py::propagate_run_states`):

> If a function consumes constant `c` AND `c` has at least one pending value AND the function's own state is `green`, downgrade to **grey**.

The downgrade cascades like any other grey through `propagate_run_states`'s DAG walk — downstream functions also turn grey.

Lifecycle:

1. User adds pending value `c=4.0` → `add_pending_constant` writes the row.
2. Graph builder downgrades every consumer (and its descendants) green → grey.
3. User runs the pipeline; the new combo is saved and `c=4.0` now appears in `const_counts`.
4. `auto_clean_pending_constants` removes the pending row on the next graph build. Node returns to green.

Scope of the downgrade:

- Only applies to `green` functions. A function that is already `grey` or `red` is left alone — the more severe state wins.
- `red` is **never** promoted or reset by pending constants.
- Pending constants do not affect scihist-level state (`check_node_state`). They are evaluated only when the GUI assembles the React Flow graph.

| Change type                      | Where handled                        | Result                                            |
| -------------------------------- | ------------------------------------ | ------------------------------------------------- |
| New variant value, not run yet   | GUI (`propagate_run_states`)         | Green → grey                                      |
| New variant value, run           | scihist (`check_node_state`)         | New variant counted as up_to_date                 |
| Old variant's constant edited    | scihist (fn-hash mismatch)           | Red (function body changed)                       |
| Pending + downstream dependency  | GUI (DAG propagation)                | Downstream also grey                              |

---

## Common Scenarios

### Full run, all combos succeed → Green

```
Expected: {sub01/trial01, sub01/trial02, sub02/trial01, sub02/trial02}
Actual:   all four, all up_to_date
→ Green
```

### Partial run (some combos error or files missing) → Grey

```
Expected: {sub01/trial01, ..., sub03/trial05}  (15 combos, written to _for_each_expected)
Actual:   14 combos up_to_date, sub03/trial05 missing (function raised during execution)
→ Grey
```

This is the key scenario for PathInput-only functions (e.g., `load_csv`). Because the expected set is written before execution, a combo that errors mid-run is correctly detected as missing.

### Re-run adds new data → Grey

```
Round 1: Seed sub01, sub02 data. Run function. state=green (10 combos).
Action:  Seed sub03 data (new subjects arrive).
state=grey (10 up_to_date, 5 missing — new sub03 inputs exist but haven't been processed)
```

### Input re-saved → Red

```
State was green. Re-save one input record with different data (same schema combo).
→ That combo becomes stale.
→ Node state = red (any stale → red).
```

### Function code changed → Red

```
State was green. Modify the function body (changes its hash).
→ All combos become stale.
→ Node state = red.
```

### Function renamed by a library action → stays Green

```
State was green. ⇪ Share as library, ⇪ publish, or ✎ Make my own copy renames
filter_emg → emg_tools.filter_emg (scidb.rename_functions).
→ Code unchanged: each result is re-recorded under the new name without running.
→ The new-name node is green; the old name's earlier records become history
  (_function_rename), so they no longer count as current.
→ The retired old-name node is hidden, and leaves the run-state cascade.
→ Code changed, or the step never ran: nothing is carried; the node is red
  until it runs.
```

The library action's report lists, per renamed step, how many results were
carried or why none were.

---

## Why Grey Is Not Red

Grey means "partially done, not broken". The outputs that exist are correct — they were computed from the inputs that were available at the time. Grey is the right state when:

- New subjects/trials have been added to the dataset since the last run
- Some combos failed due to bad input files but others succeeded
- A function was run for a subset of the full schema and more data has since arrived

Red means "something is wrong with what exists" — either nothing exists, or what exists was computed from inputs that have since changed.

---

## Edge Cases

### No input data, function never run

```
counts = {up_to_date: 0, stale: 0, missing: 0}
→ Red
```

There are no records and no expected combos, so the function has never meaningfully run.

### All combos fail

```
Expected: 15 combos (written to _for_each_expected before execution)
Actual:   0 combos (all raised exceptions)
→ counts = {up_to_date: 0, missing: 15}
→ Red  (missing > 0 and up_to_date == 0)
```

### Downstream function, upstream is grey

```
upstream (load_csv):     grey  (14/15 combos)
downstream (compute_peak): green (ran for all 14 available inputs, 0 missing from its perspective)
GUI propagates:          downstream effective state = grey
```

`check_node_state` for the downstream function returns green — it did its job correctly for everything it was given. The grey comes from DAG propagation in the GUI layer.
