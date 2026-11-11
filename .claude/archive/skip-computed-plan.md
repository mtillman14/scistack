# Plan: skip_computed in scihist.for_each

## Status
Planned — not yet implemented.

## Problem
`scihist.for_each` re-runs every (subject, session, …) combo on every call, even
when the function code, its inputs, and all upstream variables are unchanged.

## What Already Exists

`scilineage.LineageFcn.__call__` already short-circuits the **function call** on
a cache hit (`_backend.find_by_lineage(invocation)` returns a result). But:
- **All inputs are still loaded** from the DB for every combo.
- `_save_with_lineage` is still called for every combo.
- `run.py` in scistack-gui imports `from scidb import for_each`, completely
  bypassing the scihist/scilineage caching layer entirely.

## Design

### Where the logic lives: scihist (not scidb)

`scihist.for_each` is the right place. `scidb.for_each` is a generic iteration
engine with no lineage awareness. The skip logic belongs in scihist because it
depends on lineage metadata (`_lineage.inputs`, `_lineage.function_hash`,
`get_upstream_provenance`).

### API change

```python
# scihist.for_each
def for_each(
    fn,
    inputs,
    outputs,
    skip_computed: bool = True,   # NEW — default True
    ...
    **metadata_iterables,
):
```

`skip_computed=True` is the default because scihist's whole purpose is smart
re-execution. Users who want to force a full re-run pass `skip_computed=False`.

---

## Skip Logic Per Combo

For a given combo (e.g. `subject=1, session="A"`), skip iff ALL of the
following hold. No timestamps are used anywhere — all comparisons are via
content-addressed `record_id`s and lineage hashes.

### Step 1 — output exists

Every output type in `outputs` must have an existing record in the DB for this
combo. If any output is missing → compute.

### Step 2 — walk the full upstream graph

Call `db.get_upstream_provenance(existing_output_record_id)` to get every
upstream variable node (all depths). For each node (including depth=0):

**a. Function hash check**
Query `_lineage WHERE output_record_id = node.record_id` to get
`function_hash`. Compare to the current `LineageFcn.hash` (bytecode hash).
If any mismatch → function has changed → compute.

**b. Input record_id check**
The `_lineage.inputs` JSON stores the **exact `record_id`** of each input
variable that was used when that node was saved (captured by scilineage's
`classify_input` → `to_lineage_dict()` → `"record_id"` field for
`SAVED_VARIABLE` inputs).

For each input entry in `_lineage.inputs`:
- Get the `record_id` that was used: `used_rid`
- Look up the **current** record for that `(variable_name, schema_id)` using
  the existing `_find_record(..., version_id="latest")` mechanism — the same
  trusted timestamp-based partition logic used by `load()` everywhere
- Get its `record_id`: `current_rid`
- If `used_rid != current_rid` → that upstream was updated → compute

If all nodes pass both checks → **skip** (print `[skip] subject=1, session=A`).
Otherwise print the reason: `[recompute] subject=1, session=A — upstream
RollingVO2 updated` or `[recompute] … — function changed`.

### Why record_id comparison is not brittle

`record_id` is content-addressed: derived from `(class_name, schema_version,
content_hash, metadata)`. Identical data + identical metadata always produces
the same `record_id`, deterministically and regardless of when it was saved.

Consequently:
- Re-running a function that produces **identical output** → same `record_id` →
  no mismatch → correctly skips downstream.
- Re-running with **different output** → new `record_id` → mismatch →
  correctly triggers recomputation.

The "latest version" question (e.g. `low_hz=30`, then `low_hz=20`, then
`low_hz=30` again) is handled by `_find_record`'s existing
`ROW_NUMBER() OVER (PARTITION BY variable_name, schema_id, version_keys
ORDER BY timestamp DESC)` logic — the same mechanism used by every `load()`
call in scidb. The skip_computed check inherits those semantics unchanged.

---

## Role of get_upstream_provenance

`get_upstream_provenance(record_id)` provides the **graph structure** for
traversal. It returns all upstream variable nodes with their current
`record_id`s (found via branch_params subset matching). The staleness
**comparison** is then done using `_lineage.inputs` (which stores the exact
`record_id`s used at save time) — not based on what provenance finds as the
"current best match".

This separation is important: provenance gives us the set of nodes to inspect;
`_lineage.inputs` gives us the ground truth of what was used.

---

## The Cascade

"From the earliest upstream node that changed, everything downstream needs to
re-run." This cascade is handled by the **orchestration layer** (`run.py`),
not inside a single `scihist.for_each` call:

1. `run.py` calls `get_upstream_provenance` on the existing output to discover
   all upstream **function** nodes and their order.
2. It runs each function in **topological order** (leaves first), each call
   using `skip_computed=True`.
3. Each function's skip check (depth-1 input check described above) sees the
   freshly-updated intermediate outputs and correctly re-runs or skips.

Because each function only needs to check its direct inputs (which have already
been updated or skipped by the time it runs), the full cascade propagates
correctly through the pipeline without any function needing to inspect beyond
its own `_lineage.inputs`.

---

## Implementation Steps

### Step 1 — `scidb/src/scidb/foreach.py`: add `_pre_combo_hook`

Add an internal `_pre_combo_hook: Callable[[dict], bool] | None = None`
parameter. Before loading inputs for each combo, call
`_pre_combo_hook(combo_metadata)`. If it returns `True` (skip), omit that
combo from input loading, function call, and save. Prefixed with `_` — not
public API.

### Step 2 — `scidb/src/scidb/database.py`: add two helper methods

```python
def get_record_timestamp(self, record_id: str) -> str | None:
    """ISO timestamp for a record_id, or None."""

def get_function_hash_for_record(self, record_id: str) -> str | None:
    """function_hash from _lineage for a record_id, or None."""
```

No schema changes. Both are thin wrappers around existing queries.

### Step 3 — `scihist-lib/src/scihist/foreach.py`: build the skip hook

When `skip_computed=True` and a backend is configured, build a
`_pre_combo_hook` closure that captures `outputs` and `fn`:

```python
def _should_skip(combo_metadata: dict) -> bool:
    for OutputCls in outputs:
        try:
            existing = db.load(OutputCls, combo_metadata, version_id="latest")
        except NotFoundError:
            return False  # output missing → compute

    # Walk full upstream graph
    nodes = db.get_upstream_provenance(existing.record_id)
    for node in nodes:
        # a. function hash check
        stored_fn_hash = db.get_function_hash_for_record(node["record_id"])
        if stored_fn_hash and stored_fn_hash != fn.hash:
            log(f"[recompute] {combo_str} — function changed")
            return False

        # b. input record_id check via _lineage.inputs
        lineage_row = db.get_lineage_inputs(node["record_id"])
        for inp in lineage_row:
            if inp.get("source_type") != "variable":
                continue  # constants and thunks handled by hash check
            used_rid = inp["record_id"]
            current = db._find_record(inp["type"], ..., version_id="latest")
            if current.record_id != used_rid:
                log(f"[recompute] {combo_str} — upstream {inp['type']} updated")
                return False

    log(f"[skip] {combo_str}")
    return True
```

### Step 4 — `scistack-gui/scistack_gui/api/run.py`: two changes

1. Switch `from scidb import for_each` → `from scihist import for_each` to
   enable lineage tracking and skip_computed for GUI-triggered runs.
2. Use `get_upstream_provenance` to determine topological run order when the
   user clicks Run on a downstream node, so upstream functions are run (and
   potentially skipped) in the correct order before the target.

---

## What This Does NOT Cover (known limitations)

- **In-memory-only changes**: if upstream data changes without a new DB save,
  the `record_id` comparison won't detect it. In practice scidb saves are
  always append-style.
- **Non-deterministic functions**: result served from cache even if the
  function would produce different output today. Inherent to any caching.
- **Constants changes**: a constant value change (e.g. `window_seconds=30→60`)
  produces a different `branch_params` and therefore a distinct output variant —
  the skip check applies independently per variant, so this is handled correctly.

---

## Files to Modify

| File | Change |
|------|--------|
| `scidb/src/scidb/foreach.py` | Add internal `_pre_combo_hook` param; call before each combo's input load |
| `scidb/src/scidb/database.py` | Add `get_function_hash_for_record`, `get_lineage_inputs` helpers |
| `scihist-lib/src/scihist/foreach.py` | Add `skip_computed=True`; build hook closure; pass to scidb |
| `scistack-gui/scistack_gui/api/run.py` | Switch to `from scihist import for_each`; add topological run ordering |
