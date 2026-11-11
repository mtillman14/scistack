# Plan: Scihist Staleness API + Single Source of Truth

## Problem
Two sources of truth cause inconsistency:
1. DuckDB — actual data, run history, lineage
2. JSON layout file — GUI pipeline structure (manually placed nodes, edges, positions)

Consequence: manually placed nodes have no run state (invisible to the green/grey/red system).
Root cause: "declaring" a pipeline step and "executing" it are conflated — nothing exists in the
DB until a for_each() run succeeds.

## Principle
**DuckDB is the single source of truth for pipeline structure.**
The JSON file becomes purely cosmetic (x/y positions only).
"Adding a node to the GUI" = writing a DB registration record → that node immediately has a
run state (red, since it has never been run).

---

## Phase 1 — Logging in scihist (immediate, low-risk)

Replace existing `print()` calls in `scihist/foreach.py::_should_skip` with
`logging.getLogger(__name__)` calls so per-combo staleness decisions are observable
at the DEBUG level in production and during GUI operation.

**File:** `scihist-lib/src/scihist/foreach.py`
- Add `import logging` and `logger = logging.getLogger(__name__)`
- `[skip]` → `logger.debug("skip: %s", combo_str)` 
- `[recompute] ... function changed` → `logger.debug("recompute %s: function changed", combo_str)`
- `[recompute] ... upstream updated` → `logger.debug("recompute %s: upstream %s updated", combo_str, var_type)`
- Add debug log for the "output missing" early-return path (currently silent)

---

## Phase 2 — Public staleness API in scihist

Expose a clean public API for per-combo and per-node state queries. The GUI (and any other
consumer) calls this instead of implementing its own approximation.

### New public functions in `scihist-lib/src/scihist/foreach.py` (or a new `scihist/state.py`):

```python
ComboState = Literal["up_to_date", "stale", "missing"]

def check_combo_state(
    fn: LineageFcn | Callable,
    outputs: list[type],
    schema_combo: dict,
    db=None,
) -> ComboState:
    """
    Return the run state for a single (function, schema_combo) pair.
    - "up_to_date": output exists and full upstream provenance is unchanged
    - "stale":      output exists but upstream has changed (input updated or fn hash changed)
    - "missing":    no output record exists for this combo
    """

def check_node_state(
    fn: LineageFcn | Callable,
    outputs: list[type],
    db=None,
) -> dict:
    """
    Aggregate run state across all combos known to the DB for this function.

    Returns:
    {
        "state": "green" | "grey" | "red",   # overall node state
        "combos": [
            {"combo": {"subject": 1, "session": "pre"}, "state": "up_to_date"},
            ...
        ],
        "counts": {"up_to_date": N, "stale": N, "missing": N},
    }

    Combo enumeration: union of schema_ids from all output variable types.
    A combo is "missing" if it has no output record at all.
    A combo present in inputs but not outputs is also "missing".
    """
```

**Node state mapping:**
- all combos "up_to_date" → green
- all combos "missing" (never run) → red
- any combo "stale" → red (stale = effectively needs rerun = red)
- mix of up_to_date and missing/stale → grey

**Note on "stale" vs "red":** A stale node (output exists but upstream changed) is shown red,
not a separate colour, because the user needs to re-run it just like a missing one. Grey is
reserved for the "partially executed" case only.

### Combo enumeration strategy
`check_node_state` needs to know what combos exist. Strategy:
1. Query `_record_metadata` for all schema_ids across all output variable types for this function
2. Query `_record_metadata` for all schema_ids across all input variable types
3. Expected = union of (input schema_ids); actual = set from outputs
4. Combos in actual → call `check_combo_state` (up_to_date or stale)
5. Combos in expected but not actual → "missing"

---

## Phase 3 — Pipeline structure in DuckDB

### New DB tables (added to sciduck or scidb's schema init)

```sql
CREATE TABLE IF NOT EXISTS _pipeline_nodes (
    node_id     VARCHAR PRIMARY KEY,   -- e.g. "fn__bandpass_filter"
    node_type   VARCHAR NOT NULL,      -- "functionNode" | "variableNode" | "constantNode"
    label       VARCHAR NOT NULL,      -- human name
    created_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS _pipeline_edges (
    edge_id       VARCHAR PRIMARY KEY,
    source        VARCHAR NOT NULL,
    target        VARCHAR NOT NULL,
    source_handle VARCHAR,
    target_handle VARCHAR,
    manual        BOOLEAN DEFAULT TRUE
);
```

Node positions stay in the JSON (purely cosmetic, no semantic value). Everything else moves to DB.

**Migration:** On GUI startup, read existing JSON manual_nodes and manual_edges, write them to
the new DB tables, then clear those keys from the JSON. Positions remain in JSON untouched.

### Impact on _build_graph
- Manual nodes: read from `_pipeline_nodes` instead of JSON
- Manual edges: read from `_pipeline_edges` instead of JSON
- Nodes from DB data (via `list_pipeline_variants`) continue to work as today;
  when a variant is first run, its node_id is upserted into `_pipeline_nodes` automatically
  (or the GUI reads it from variants and the table is only needed for "declared but unrun" nodes)

---

## Phase 4 — GUI wiring

### Backend (`pipeline.py`)
- Replace `_compute_run_states` with calls to `scihist.check_node_state` for each registered function
- Functions in `_pipeline_nodes` with no variants get `run_state = "red"` directly (never run)
- Variable node state = upstream function's effective state (unchanged)

### Layout API
- `PUT /api/layout/:id` with `node_type` → write to `_pipeline_nodes` (DB) instead of JSON
- `PUT /api/edges/:id` → write to `_pipeline_edges` (DB) instead of JSON
- `DELETE /api/layout/:id` → delete from `_pipeline_nodes`
- `DELETE /api/edges/:id` → delete from `_pipeline_edges`
- Positions (`x`, `y` only) continue to go to JSON

---

## Phase 0 — Store function hash in scidb (prerequisite for scidb.for_each staleness)

Two small changes to `scidb` so that plain `scidb.for_each` runs carry enough information
for staleness detection without requiring scihist/scilineage.

### 0a — Write `__fn_hash` into `version_keys` in scidb.for_each

**File:** `scidb/src/scidb/foreach.py`

When building `version_keys` for each output record, add `__fn_hash`: a SHA-256 of the
function's source code (via `inspect.getsource`), truncated to 16 hex chars.  Falls back
to a hash of `fn.__name__` if source is unavailable (e.g. compiled extensions).

```python
import hashlib, inspect

def _compute_fn_hash(fn) -> str:
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        src = getattr(fn, "__name__", repr(fn))
    return hashlib.sha256(src.encode()).hexdigest()[:16]
```

`version_keys["__fn_hash"] = _compute_fn_hash(fn)` written alongside `__fn`.

### 0b — Timestamp-based input freshness as last-resort fallback in check_combo_state

**File:** `scihist-lib/src/scihist/state.py`

**Staleness check priority order (most to least authoritative):**

1. **Lineage record exists** (`_lineage` row for the output record):
   - Function staleness: compare stored `function_hash` against current `LineageFcn.hash`.
   - Input staleness: compare stored `record_id` per input against current latest record_id.
   - *No timestamps used.*

2. **No lineage, but `__fn_hash` present in `version_keys`** (scidb.for_each, new records):
   - Function staleness: compare stored `__fn_hash` against `_compute_fn_hash(fn)`.
   - Input staleness: compare output record `timestamp` against the `timestamp` of the
     latest input record at the same schema_id. If any input was saved *after* the output,
     the output is stale.
   - *Timestamps used only for input freshness — the minimum unavoidable fallback.*

3. **Neither lineage nor `__fn_hash`** (old scidb.for_each records):
   - Function staleness: cannot determine → treat as up_to_date, log a warning.
   - Input staleness: timestamp comparison as in (2).
   - *Timestamps used as last resort for input freshness only.*

**Rationale:** timestamps are an approximation (a re-save with identical data triggers a
false positive; clock skew could in theory cause false negatives).  They are used only where
exact record identity is unavailable.  All cases where lineage or a hash is present avoid
timestamps entirely.

---

## Sequencing
0. Phase 0 (scidb fn_hash + check_combo_state fallback logic) — prerequisite for accurate
   scidb.for_each staleness; implement before Phase 2 tests are finalised
1. Phase 1 (logging) — ✅ done
2. Phase 2 (API) — ✅ done (state.py); update check_combo_state with Phase 0 fallback logic
3. Phase 3 (DB tables) — add tables + migration, update layout/edge API endpoints
4. Phase 4 (GUI wiring) — replace `_compute_run_states`, wire to scihist API

Phases 3 and 4 can be done together since they're tightly coupled.

---

## Open questions
- Should `_pipeline_nodes` live in scidb/sciduck (it's DB infrastructure) or is it purely a
  GUI concern that belongs in scistack-gui?
  **Recommendation:** GUI concern — add table creation to scistack-gui's DB init, not to scidb.
