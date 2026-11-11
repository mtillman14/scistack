# GUI Server Layered Architecture Refactor

## Context

The scistack-gui Python backend has grown to ~5,000 lines across 22 files, with complexity concentrated in three mega-functions: `_build_graph()` (440 lines in `api/pipeline.py`), `_run_in_thread()` (460 lines in `api/run.py`), and `server.py` (986 lines with 35+ inline handlers). Edge-inference logic is duplicated in 3 places with subtle variations, making bug fixes unreliable. There's a circular import (`api/variables.py` → `server.py:_create_matlab_variable`). The goal is a principled layered architecture that separates protocol handling, orchestration, pure domain logic, and data access — while respecting that scidb/scihist/scilineage are the lower-level domain packages and must never know about the GUI.

## Architecture: Four Layers

```
Layer 4: Protocol Adapters    server.py (JSON-RPC), api/*.py (FastAPI routes)
              ↓                Thin: parse request → call service → format response
Layer 3: Services             services/*.py
              ↓                Orchestration: fetch data, call domain fns, emit notifications
Layer 2: Domain               domain/*.py
              ↓                Pure functions: data in → data out. No I/O. Testable in isolation.
Layer 1: Data Access          db.py, pipeline_store.py, layout.py, registry.py, etc.
              ↓                Read/write DB, JSON files, in-memory registries
Layer 0: External Packages    scidb, scihist, scilineage (never modified)
```

### Import Rules

| Layer | Can import from | CANNOT import from |
|-------|----------------|-------------------|
| Protocol (L4) | Services (L3) only | Domain, Data Access directly |
| Services (L3) | Domain (L2), Data Access (L1), External (L0), notify.py | Protocol (L4) |
| Domain (L2) | stdlib only | Everything else |
| Data Access (L1) | External (L0) | Everything above |

Key constraint: **Domain (L2) is pure.** Functions take plain Python data (dicts, lists, sets, strings) and return plain data. They never call `get_db()`, `pipeline_store.*`, `layout.*`, `registry.*`, `notify.*`, or `scihist.*`.

## Unifying the Manual-vs-DB Duality

The biggest source of past bugs is the two-path system: nodes/edges either come from **DB history** (`list_pipeline_variants()`) or from **manual GUI actions** (`pipeline_store`). Currently these merge in ad-hoc ways across multiple files, each with slightly different rules. The refactored architecture gives this a single, explicit model.

### Current Problem: Four Divergent Code Paths

| Path | Location | When | How connections are resolved |
|------|----------|------|------------------------------|
| **A: DB-derived nodes** | `pipeline.py:307-600` | Function has run before | Input/output types from DB variants |
| **B: Manual-only nodes** | `pipeline.py:621-733` | Function placed but never run | Edge-inferred from manual edges + positional matching |
| **C: DB + manual override** | `run.py:137-176` | Function has run, but user rewired outputs | Manual edges override DB output types, DB provides constants |
| **D: First run (no DB)** | `run.py:178-300` | Function about to run for first time | Everything from manual edges + pending constants |

Each path re-implements edge scanning with subtle differences (e.g., constant detection by `const__` prefix vs `manual_nodes` lookup, whether `targetHandle` is checked, positional matching order).

### Refactored Model: Single Resolution with Priority

In `domain/edge_resolver.py`, ONE function handles all four cases:

```python
def resolve_function_connections(
    fn_name: str,
    fn_node_ids: set[str],
    db_variants: list[dict],           # from list_pipeline_variants(), may be empty
    manual_edges: list[dict],          # from pipeline_store, may be empty
    manual_nodes: dict[str, dict],
    sig_params: list[str],
) -> ResolvedConnections:
```

**Resolution priority rules** (explicit, documented, tested):

1. **Output types**: Manual edges win if present, else DB variants
2. **Input types**: Manual edges fill in per `targetHandle`, then positional matching for unmatched edges, then DB variants fill remaining gaps
3. **Constants**: Union of DB variant constants + manually-wired constant nodes. Pending constant values are overlaid separately by `variant_resolver.py`.
4. **PathInputs**: Manual edge connections + saved path input templates overlay DB-derived PathInput values

This means:
- **Path A** (DB-derived graph display) → `resolve_function_connections(db_variants=variants, manual_edges=edges, ...)` — DB provides base, manual edges overlay
- **Path B** (manual node display) → same function, `db_variants=[]`
- **Path C** (run with rewired outputs) → same function, both populated, manual edges win for outputs
- **Path D** (first run) → same function, `db_variants=[]`

All four paths become ONE call with different data.

### Graduation: Explicit Rules in `graph_builder.py`

The `merge_manual_nodes()` function in `domain/graph_builder.py` handles the manual→DB-derived transition with clear, documented rules:

1. **Graduate**: Manual node matches a DB-derived node by `(type, label)` AND the DB node has no saved position yet → transfer position, remove manual entry. This means the function just ran for the first time and the DB node appeared.
2. **Keep both**: Manual node matches DB node but DB node already has a saved position → user intentionally placed an extra instance.
3. **Stay hidden**: If a DB-derived node ID is in `hidden_ids`, it stays hidden even after re-running. The user must explicitly un-hide it.
4. **Return side effects**: Graduation changes (position transfers, manual entry deletions) are returned as a list of `GraduationAction` objects. The service layer executes them. The domain function itself performs no I/O.

### Why This Fixes Past Bugs

Past bugs came from:
- Path C not checking `targetHandle` when Path D did → edge resolver now checks uniformly
- Graduation happening inside `_build_graph()` where it's mixed with node construction → now a separate, testable step
- `_run_in_thread` re-implementing edge scanning slightly differently than `_build_graph` → now both call the same domain function
- Constant detection differing between `const__` prefix check and `manual_nodes` type check → edge resolver does both uniformly

## External Package Interface Map

Every import from scidb, scihist, scilineage, and sci_matlab is concentrated in specific layers. The domain layer (L2) has NO external imports.

### Data Access Layer (L1) → External Packages (L0)

| Module | External Import | Purpose |
|--------|----------------|---------|
| `db.py` | `scidb.configure_database()` | One-time DB setup at startup |
| `db.py` | `scidb.database.DatabaseManager` | Type annotation for the DB handle |
| `registry.py` | `scidb.BaseVariable` | Variable class metaclass registry (`_all_subclasses`) |
| `matlab_registry.py` | `sci_matlab.bridge.register_matlab_variable()` | Create Python surrogate for MATLAB variable class |

These are all startup/discovery operations. Clean and minimal.

### Service Layer (L3) → External Packages (L0)

| Service | External API Call | Type | Purpose |
|---------|-------------------|------|---------|
| `pipeline_service.py` | `db.list_pipeline_variants()` | Query | Get all function→output mappings from DB |
| `pipeline_service.py` | `db.list_variables()` | Query | Discover variable types in DB |
| `pipeline_service.py` | `db.distinct_schema_values(key)` | Query | Get schema value lists |
| `pipeline_service.py` | `scihist.check_node_state(fn, outputs, db=db)` | Query | Per-function staleness check → "green"/"grey"/"red" |
| `pipeline_service.py` | `sci_matlab.bridge.MatlabLineageFcn` | Query | Build proxy for MATLAB function hash matching |
| `pipeline_service.py` | `scidb.BaseVariable._all_subclasses` | Query | Look up output classes for state check |
| `run_service.py` | `scidb.for_each(fn, inputs, outputs, ...)` | Mutation | Execute pipeline function |
| `run_service.py` | `scidb.EachOf` | Query | Multi-input type combinator |
| `run_service.py` | `scidb.filters.VariableFilter` | Query | WHERE filter construction |
| `run_service.py` | `registry.get_function(name)` | Query | Look up Python callable |
| `run_service.py` | `registry.get_variable_class(name)` | Query | Look up BaseVariable subclass |
| `variable_service.py` | `scidb.BaseVariable._all_subclasses` | Query | Validate name doesn't exist |
| `variable_service.py` | `sci_matlab.bridge.register_matlab_variable()` | Mutation | Register MATLAB variable surrogate |
| `matlab_command_service.py` | `db.list_pipeline_variants()` | Query | Get variants for MATLAB command |

### Domain Layer (L2) → External Packages (L0)

**None.** The domain layer works entirely with plain Python data structures:
- Variants are `list[dict]` (not `DatabaseManager` query results)
- Run states are `dict[str, str]` (not `scihist.ComboState` objects)
- Node IDs are `str` (not `BaseVariable` classes)
- Function params are `list[str]` (not `inspect.Parameter` objects)

### Why This Boundary Matters

1. **scidb/scihist never import from scistack_gui** — correct one-way dependency
2. **If scidb changes an API** (e.g., `list_pipeline_variants()` return format), only service modules need updating, not domain logic
3. **scihist.check_node_state()** is called ONLY in `pipeline_service.py` — results are passed as `dict[str, str]` to the pure domain propagation function
4. **scidb.for_each()** is called ONLY in `run_service.py` — the domain layer resolves WHAT to run, the service layer actually calls for_each
5. **Testing domain functions requires zero scidb/scihist setup** — feed in dicts, get dicts out

## Layer 2: Domain Modules (NEW)

### `domain/edge_resolver.py` (~120 lines)

Eliminates the 3-way duplication. Works purely on edge/node dicts.

```python
def resolve_function_edges(
    fn_name: str,
    fn_node_ids: set[str],          # all node IDs for this function
    manual_edges: list[dict],        # [{id, source, target, sourceHandle, targetHandle}]
    manual_nodes: dict[str, dict],   # {node_id: {type, label}}
    sig_params: list[str],           # function signature param names, in order
) -> ResolvedEdges:
    """Returns ResolvedEdges(input_types, output_types, constant_names)."""

def node_id_to_var_label(
    node_id: str,
    existing_node_labels: dict[str, str],
    manual_nodes: dict[str, dict],
) -> str | None:
```

**Replaces duplicated logic in:**
- `api/pipeline.py:89-110` (`_node_id_to_var_label`)
- `api/pipeline.py:662-692` (manual node edge scanning)
- `api/run.py:137-248` (first-run edge inference)
- `server.py:617-691` (MATLAB command edge scanning)

### `domain/graph_builder.py` (~250 lines)

Builds React Flow nodes/edges from pre-fetched data. No DB calls.

```python
def aggregate_variants(variants: list[dict], listed_var_names: set[str]) -> AggregatedData
def filter_hidden(aggregated: AggregatedData, hidden_ids: set[str]) -> AggregatedData
def build_nodes(aggregated, record_counts, run_states, pending_constants,
                saved_path_inputs, manual_nodes, fn_params_map,
                matlab_functions, saved_configs) -> list[dict]
def build_edges(aggregated, manual_edges, hidden_ids) -> list[dict]
def merge_manual_nodes(nodes, manual_nodes, saved_positions, aggregated,
                       manual_edges, fn_params_map, matlab_functions,
                       pending_constants, manual_fn_states) -> tuple[list[dict], list[GraduationAction]]
```

**Extracted from:** `api/pipeline.py:307-746` (`_build_graph`)

### `domain/run_state.py` (~80 lines)

Pure DAG propagation. The scihist query (`_own_state_for_function`) stays in the service layer.

```python
def propagate_run_states(
    fn_own_states: dict[str, str],     # pre-computed by service layer via scihist
    fn_input_params: dict[str, dict],
    fn_outputs: dict[str, set],
    fn_constants: dict[str, set] | None,
    pending_constants: dict[str, set] | None,
) -> dict[str, str]:
    """Returns {node_id: "green"|"grey"|"red"} for fn__ and var__ nodes."""
```

**Extracted from:** `api/pipeline.py:202-304` (`_compute_run_states`)

### `domain/variant_resolver.py` (~120 lines)

Pure variant resolution, dedup, and pending-constant merging.

```python
def build_inferred_variants(input_types, output_types, inferred_constants) -> list[dict]
def filter_variants(fn_variants, selected_variants) -> list[dict]
def deduplicate_variants(targets: list[dict]) -> list[dict]
def merge_pending_constants(fn_variants, pending_constants) -> list[dict]
def build_schema_kwargs(schema_level, all_schema_keys, schema_filter,
                        distinct_values: dict[str, list]) -> dict
```

**Extracted from:** `api/run.py:260-399` (variant building, dedup, pending merge, schema kwargs)

## Layer 3: Service Modules (NEW)

Each service function is the **single source of truth** for an operation. Both JSON-RPC handlers and FastAPI routes call these.

### `services/pipeline_service.py` (~200 lines)

```python
def get_pipeline_graph(db) -> dict:
    """Orchestrates: fetch data → aggregate → filter hidden → compute states → build graph."""
    # 1. Fetch variants, variables, hidden nodes, manual nodes/edges,
    #    pending constants, saved path inputs, saved positions from data access
    # 2. Call domain.aggregate_variants()
    # 3. Call domain.filter_hidden()
    # 4. Query record counts from DB
    # 5. Compute fn_own_states via scihist.check_node_state() for each function
    # 6. Call domain.propagate_run_states(fn_own_states, ...)
    # 7. Call domain.build_nodes(), build_edges(), merge_manual_nodes()
    # 8. Execute graduation side effects (layout_store.graduate_manual_node)
    # 9. Return {nodes, edges}

def get_function_params(fn_name) -> list[str]
def get_function_source(fn_name) -> dict
def get_schema(db) -> dict
def get_info() -> dict
def get_registry() -> dict
```

### `services/run_service.py` (~200 lines)

```python
def start_run_in_thread(run_id, function_name, variants, db,
                        schema_filter, schema_level, run_options, where_filters) -> dict:
    """Orchestrates: resolve targets → execute for_each → emit notifications."""
    # 1. Look up function from registry
    # 2. Fetch DB variants, manual edges/nodes, pending constants
    # 3. Call domain.resolve_function_edges() for edge inference (if needed)
    # 4. Call domain.build_inferred_variants() or use DB variants
    # 5. Call domain.merge_pending_constants()
    # 6. Call domain.filter_variants() / deduplicate_variants()
    # 7. Call domain.build_schema_kwargs()
    # 8. Execute loop: for each target, resolve classes, call for_each()
    # 9. Emit run_output/run_done/dag_updated notifications

def cancel_run(run_id) -> dict
def force_cancel_run(run_id) -> dict
```

### `services/variable_service.py` (~100 lines)

Consolidates the duplicated variable creation logic (currently in both `server.py:378-473` AND `api/variables.py:130-201`, with a circular import between them).

```python
def create_variable(name, docstring, language) -> dict
    """Validates, writes class to file (Python or MATLAB), refreshes registry."""

def get_variable_records(variable_name, db) -> dict
```

### `services/layout_service.py` (~60 lines)

Thin orchestration for layout CRUD. Keeps protocol adapters from importing data access directly.

```python
def get_layout() -> dict
def put_layout(node_id, x, y, node_type, label) -> dict
def delete_layout(node_id) -> dict
def put_edge(params) -> dict
def delete_edge(edge_id) -> dict
def create_constant(name) -> dict
def delete_constant(name) -> dict
def put_pending_constant(name, value) -> dict
def delete_pending_constant(name, value) -> dict
def create_path_input(name, template, root_folder) -> dict
def update_path_input(name, template, root_folder) -> dict
def delete_path_input(name) -> dict
def put_node_config(node_id, config) -> dict
```

### `services/matlab_command_service.py` (~80 lines)

Extracts the 130-line `_h_generate_matlab_command` orchestration from server.py.

```python
def generate_matlab_command(function_name, db, params) -> dict:
    """Orchestrates: fetch variants → resolve path inputs → infer outputs → format command."""
```

### `services/project_service.py` and `services/indexes_service.py`

These already delegate cleanly to `api/project.py` and `api/indexes.py`. They become direct re-exports or the existing logic moves into the service.

## Layer 4: Protocol Adapters (THINNED)

### `server.py` (~250 lines, down from 986)

Becomes: startup sequence + thin dispatch table + main loop. Every handler is 1-5 lines.

```python
def _h_get_pipeline(params):
    from scistack_gui.services.pipeline_service import get_pipeline_graph
    from scistack_gui.db import get_db
    return get_pipeline_graph(get_db())

def _h_create_variable(params):
    from scistack_gui.services.variable_service import create_variable
    return create_variable(params.get("name", ""), params.get("docstring"), params.get("language", "python"))

def _h_generate_matlab_command(params):
    from scistack_gui.services.matlab_command_service import generate_matlab_command
    from scistack_gui.db import get_db
    return generate_matlab_command(params["function_name"], get_db(), params)
```

### `api/*.py` route files (~200 lines total, down from ~2,700)

Each becomes a thin FastAPI wrapper calling the same service function:

```python
# api/pipeline.py
@router.get("/pipeline")
def get_pipeline(db: DatabaseManager = Depends(get_db)):
    from scistack_gui.services.pipeline_service import get_pipeline_graph
    return get_pipeline_graph(db)
```

## Layer 1: Data Access (UNCHANGED)

These files are already well-factored and stay as-is:

- `db.py` (140 lines) — connection lifecycle, refcounting
- `pipeline_store.py` (314 lines) — manual nodes/edges/constants in DuckDB
- `layout.py` (221 lines) — JSON positions + delegates to pipeline_store
- `registry.py` (289 lines) — Python function/variable discovery
- `config.py` (316 lines) — pyproject.toml parsing
- `matlab_registry.py` (142 lines) — MATLAB function discovery
- `matlab_parser.py` (139 lines) — .m file parsing
- `startup.py` (197 lines) — lockfile staleness check
- `notify.py` (50 lines) — JSON-RPC push notifications
- `api/ws.py` (114 lines) — WebSocket push (standalone mode)
- `api/matlab_command.py` (375 lines) — MATLAB string formatting (already pure)

## Final File Layout

```
scistack_gui/
  __main__.py              (155 — unchanged)
  server.py                (~250 — down from 986)

  # Data Access (L1 — unchanged)
  db.py                    (140)
  pipeline_store.py        (314)
  layout.py                (221)
  registry.py              (289)
  config.py                (316)
  matlab_registry.py       (142)
  matlab_parser.py         (139)
  startup.py               (197)
  notify.py                (50)

  # Domain (L2 — NEW, pure functions)
  domain/
    __init__.py
    edge_resolver.py       (~120)
    graph_builder.py       (~250)
    run_state.py           (~80)
    variant_resolver.py    (~120)

  # Services (L3 — NEW, orchestration)
  services/
    __init__.py
    pipeline_service.py    (~200)
    run_service.py         (~200)
    variable_service.py    (~100)
    matlab_command_service.py (~80)
    layout_service.py      (~60)
    project_service.py     (~40)
    indexes_service.py     (~40)

  # Protocol Adapters (L4 — thinned)
  api/
    app.py                 (62 — unchanged)
    pipeline.py            (~20 — down from 786)
    run.py                 (~60 — down from 713, keeps Pydantic models)
    layout.py              (~60 — down from 139)
    variables.py           (~30 — down from 201)
    ws.py                  (114 — unchanged)
    schema.py              (~15 — down from 61)
    registry.py            (~15 — unchanged)
    project.py             (~30 — down from 167)
    indexes.py             (~30 — down from 159)
    matlab_command.py      (375 — unchanged, pure formatting)
```

## Fixes the Circular Import

Currently: `api/variables.py:162` imports `from scistack_gui.server import _create_matlab_variable`.

After: Both `server.py` and `api/variables.py` call `services/variable_service.py:create_variable()`, which owns the MATLAB variable creation logic. No circular dependency.

## New Tests Enabled

| Test file | What it tests | Why it's valuable |
|-----------|--------------|-------------------|
| `test_edge_resolver.py` | Pure edge resolution with hand-crafted edge/node dicts | Currently untestable — logic is buried in 460-line functions |
| `test_graph_builder.py` | Node/edge construction from pre-aggregated data | Currently only testable through full `/api/pipeline` endpoint with real DB |
| `test_run_state.py` | DAG propagation: cycles, root variables, pending constant downgrade | Currently coupled to scihist + full DB setup |
| `test_variant_resolver.py` | Variant filtering, dedup, pending merge, cross-product | Currently coupled to run execution thread |

All domain tests run in milliseconds with no DB setup.

## Migration Path (Incremental)

Each phase leaves the system in a working state. Existing tests pass after each phase.

**Phase 1: Extract `domain/edge_resolver.py`**
- Extract `_node_id_to_var_label` and edge-scanning loops
- All 3 call sites import from the new module
- Write `test_edge_resolver.py`
- *Highest bug-fix leverage — fixes the 3-way duplication*

**Phase 2: Extract `domain/run_state.py`**
- Move propagation algorithm from `_compute_run_states`
- Keep `_own_state_for_function` in pipeline_service (calls scihist)
- Write `test_run_state.py`

**Phase 3: Extract `domain/variant_resolver.py`**
- Move variant dedup, pending constant merge, inferred variant building from `run.py`
- Write `test_variant_resolver.py`

**Phase 4: Extract `domain/graph_builder.py`**
- Move aggregate/filter/build from `_build_graph`
- Write `test_graph_builder.py`

**Phase 5: Create `services/` layer**
- Create service modules that orchestrate domain + data access
- Both server.py handlers and api/ routes call service functions

**Phase 6: Thin out protocol adapters**
- server.py handlers become 1-5 line delegates
- api/*.py routes become 1-5 line delegates
- Circular import eliminated

**Phase 7: Verification**
- Run all existing tests
- Run new domain unit tests
- Verify both JSON-RPC and FastAPI paths work

## Key Files to Modify

- `/workspace/scistack-gui/scistack_gui/api/pipeline.py` — largest extraction source (786 lines → ~20)
- `/workspace/scistack-gui/scistack_gui/api/run.py` — second largest (713 lines → ~60)
- `/workspace/scistack-gui/scistack_gui/server.py` — third largest (986 lines → ~250)
- `/workspace/scistack-gui/scistack_gui/api/variables.py` — circular import fix (201 lines → ~30)
