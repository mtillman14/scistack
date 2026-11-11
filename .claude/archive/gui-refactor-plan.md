# GUI Server Refactoring Plan

## Diagnosis: Why Troubleshooting is Hard

The GUI server has grown organically feature-by-feature, and several code smells have accumulated that make debugging difficult:

### Problem 1: `server.py` is a God Module (986 lines)
All 35+ RPC handlers, the startup sequence, MATLAB helpers, the dispatch table, and the main loop live in a single file. When something breaks, you have to search through ~1000 lines of unrelated code to find the relevant handler.

### Problem 2: Duplicated Edge-Inference Logic (3 places)
The pattern "scan manual edges to figure out what's wired to a function" appears in three places with slight variations:
- `api/run.py::_run_in_thread()` lines 137-300 (infer inputs/outputs for first-time runs)
- `api/pipeline.py::_build_graph()` lines 622-733 (infer connections for manual nodes)
- `server.py::_h_generate_matlab_command()` lines 617-691 (infer outputs for MATLAB command generation)

Each copy handles edge cases slightly differently, so a bug fix in one place doesn't propagate to the others.

### Problem 3: `_run_in_thread` is a 460-line Mega-Function
This single function handles: edge scanning, constant resolution, variant cross-product building, pending constant merging, schema setup, option extraction, `for_each` execution, stdout capture, cancellation, and cleanup. It's nearly impossible to test any of these pieces in isolation.

### Problem 4: `_build_graph` is a 440-line Mega-Function
Similarly combines: variant aggregation, hidden-node filtering, run-state computation, node construction (4 types), edge construction (4 types), manual node merging with graduation logic, and config overlay. A bug in edge deduplication requires understanding the entire function.

### Problem 5: Dual-Protocol Routing Inconsistency
Some JSON-RPC handlers delegate to shared functions (`_h_get_pipeline` calls `_build_graph`), while others inline their logic (`_h_create_variable` is 60 lines that partially duplicates `api/variables.py::create_variable`). This means bug fixes sometimes need to happen in two places.

### Problem 6: `_h_generate_matlab_command` is 130 Lines of Inline Logic
This handler in server.py contains its own edge-scanning, PathInput resolution, and output inference -- all of which are variations of logic that exists elsewhere.

---

## Recommended Refactoring (Ordered by Impact)

### Refactor 1: Extract Edge-Inference into a Shared Module

**New file: `scistack_gui/edge_resolver.py`**

Extract the common pattern into testable functions:

```
get_wired_outputs(function_name, manual_edges, manual_nodes) -> list[str]
get_wired_inputs(function_name, manual_edges, manual_nodes, fn_params) -> dict[str, list[str]]
get_wired_constants(function_name, manual_edges, manual_nodes) -> set[str]
get_wired_path_inputs(function_name, manual_edges, manual_nodes, saved_path_inputs) -> dict[str, dict]
```

**Why this helps most:** This logic is the #1 source of subtle bugs (positional matching, constant detection by node prefix vs manual_nodes lookup, PathInput overlay). Having it in one tested place means a fix works everywhere.

**Callers that simplify:**
- `run.py::_run_in_thread()` — the 160-line "no DB history" branch collapses to ~20 lines
- `pipeline.py::_build_graph()` — the manual-node inference block collapses similarly
- `server.py::_h_generate_matlab_command()` — the 70-line edge-scanning block goes away

### Refactor 2: Break `_run_in_thread` into Stages

Split the mega-function into a pipeline of smaller functions:

```
resolve_run_targets(function_name, db, manual_edges, manual_nodes, pending_constants) -> list[RunTarget]
merge_pending_variants(targets, pending_constants, db) -> list[RunTarget]
build_schema_kwargs(db, schema_filter, schema_level) -> dict
execute_targets(run_id, fn, targets, schema_kwargs, run_options, cancel_event) -> RunResult
```

Where `RunTarget` is a small dataclass:
```python
@dataclass
class RunTarget:
    input_types: dict[str, list[str]]
    output_type: str
    constants: dict
```

**Why:** Each stage becomes independently testable. When a run fails, you can log the exact `RunTarget` list to see what was resolved, separate from execution. The execution stage becomes a clean loop with no inference logic.

### Refactor 3: Break `_build_graph` into Composable Steps

Split into:

```
aggregate_variants(db) -> VariantSummary  # parse variants into structured dicts
filter_hidden(summary, hidden_ids) -> VariantSummary  # remove hidden nodes
build_db_nodes(summary) -> list[Node]  # variable, constant, pathInput, function nodes
build_db_edges(summary) -> list[Edge]  # all DB-derived edges
merge_manual_nodes(db_nodes, manual_nodes, manual_edges) -> list[Node]  # graduation + inference
merge_manual_edges(db_edges, manual_edges, hidden_ids) -> list[Edge]
```

**Why:** When a node shows wrong state or a missing edge, you can inspect the intermediate data structures. Currently you have to mentally trace through 440 lines with many interleaved concerns.

### Refactor 4: Split `server.py` Handlers into Domain Modules

Move handlers out of server.py into the modules that already own the business logic:

| Handler group | Move to | Lines saved from server.py |
|---|---|---|
| `_h_put_layout`, `_h_delete_layout`, `_h_put_edge`, `_h_delete_edge`, `_h_put_node_config` | `layout_handlers.py` or inline in `layout.py` | ~50 |
| `_h_create_constant`, `_h_delete_constant`, `_h_create_path_input`, etc. | Same | ~50 |
| `_h_create_variable`, `_create_matlab_variable` | `api/variables.py` (already has `create_variable`) | ~95 |
| `_h_generate_matlab_command`, `_find_sci_matlab_matlab_dir` | `api/matlab_command.py` (already exists) | ~130 |
| `_h_start_run`, `_h_cancel_run`, `_h_force_cancel_run` | `api/run.py` (already has the thread logic) | ~60 |
| `_h_get_project_code`, `_h_get_project_libraries`, `_h_refresh_project` | trivial 1-line delegations, can stay | 0 |

After this, server.py becomes ~300 lines: startup, main loop, dispatch table, and the trivial delegation handlers. Each domain module owns both its logic AND its RPC handler.

**METHODS table becomes auto-generated** from a decorator or registry pattern:

```python
# In each module:
@rpc_handler("put_layout")
def handle_put_layout(params):
    ...

# In server.py:
from scistack_gui.rpc import METHODS  # auto-collected
```

### Refactor 5: Unify the Dual-Protocol Handlers

Rather than having FastAPI routes AND JSON-RPC handlers that call the same logic, make the JSON-RPC dispatch table point directly at protocol-agnostic handler functions. The FastAPI routes become thin wrappers that call the same functions.

Currently:
```
FastAPI route → business logic function
JSON-RPC handler → (sometimes) business logic function
JSON-RPC handler → (sometimes) inline duplicate of business logic
```

After:
```
handler function (protocol-agnostic, takes dict params, returns dict)
  ↑                    ↑
FastAPI route       JSON-RPC dispatch
(thin adapter)      (thin adapter)
```

This is partially done already for `get_pipeline`, `get_variable_records`, etc. The refactor just completes the pattern for the remaining handlers.

---

## What NOT to Change

- **The dual-protocol architecture itself is sound.** JSON-RPC for VS Code, FastAPI for standalone -- this is a good design. The issue is inconsistent factoring, not the architecture.
- **The DB refcount pattern in db.py is fine.** It's simple and works.
- **The frontend's `callBackend()` abstraction is clean.** No changes needed there.
- **notify.py is fine** at 50 lines.

---

## Suggested Order of Execution

1. **Edge resolver extraction** (Refactor 1) — highest bug-fix leverage, and it's a pure extraction with no behavioral change
2. **Break up `_run_in_thread`** (Refactor 2) — second-highest complexity, and it touches the most fragile code path
3. **Break up `_build_graph`** (Refactor 3) — same pattern, lower urgency since it's read-only
4. **Split server.py handlers** (Refactor 4) — organizational, lower risk
5. **Unify dual-protocol** (Refactor 5) — optional, only if the FastAPI standalone mode is still actively used

Each refactor is independent and can be done incrementally without breaking anything.
