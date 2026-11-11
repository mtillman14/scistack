# Plan: scistack-gui Test Suite

## Files to create
```
scistack-gui/tests/__init__.py
scistack-gui/tests/conftest.py
scistack-gui/tests/test_layout.py
scistack-gui/tests/test_registry.py
scistack-gui/tests/test_api.py
```

## Fixtures (conftest.py)
- `clear_db_state` (autouse) — resets `scistack_gui.db._db/_db_path` and thread-local `_local.database` after each test
- `tmp_db_path` — `tmp_path / "test.duckdb"`
- `populated_db` — real DuckDB with `["subject", "session"]` schema, 4 `RawSignal` records, one `for_each` run producing `FilteredSignal`; sets `_gui_db._db` and `_gui_db._db_path`
- `layout_path` — `tmp_path / "test.layout.json"` with `_gui_db._db_path` patched to point at it
- `client` — FastAPI `TestClient` from `create_app()`, uses `populated_db`

## test_layout.py (layout.py module)
- `read_layout` on missing file → returns `{positions:{}, manual_nodes:{}, constants:[], manual_edges:[]}`
- `write_node_position` / `read_layout` round-trip
- `write_manual_node` → persists position + manual_nodes entry
- `get_manual_nodes` → returns manual_nodes dict
- `delete_node` → removes position and manual entry
- `read_constants` / `write_constant` / `delete_constant` — no duplicates
- `read_manual_edges` / `write_manual_edge` (upsert by id) / `delete_manual_edge`
- `graduate_manual_node` — transfers position to new_id, removes old_id from positions and manual_nodes
- Legacy flat format migration (raw `{"node_id": {"x":0,"y":0}}` → new format)

## test_registry.py (registry.py module)
- `register_module` → populates `_functions` (finds callables, skips `_private`)
- `get_function` → returns callable; raises `KeyError` for unknown name
- `get_variable_class` → returns subclass from `BaseVariable._all_subclasses`; raises `KeyError` for unknown

## test_api.py (HTTP endpoints via TestClient)
### Schema endpoints
- `GET /api/info` → `{"db_name": "test.duckdb"}`
- `GET /api/schema` → keys=`["subject","session"]`, values dict with correct distinct values

### Registry endpoint
- `GET /api/registry` → lists registered functions and variable classes

### Pipeline endpoint
- `GET /api/pipeline` → has variableNode for `RawSignal` and `FilteredSignal`, functionNode for `bandpass_filter`, correct edges

### Layout endpoints
- `GET /api/layout` → returns layout dict
- `PUT /api/layout/{node_id}` → saves position; re-`GET` confirms it
- `PUT /api/layout/{node_id}` with `node_type`+`label` → also creates manual_node entry
- `DELETE /api/layout/{node_id}` → removes it

### Constants endpoints
- `GET /api/constants` → list
- `POST /api/constants` → adds; re-`GET` confirms
- `DELETE /api/constants/{name}` → removes

### Edges endpoints
- `PUT /api/edges/{edge_id}` → saves manual edge
- `DELETE /api/edges/{edge_id}` → removes it

### Run endpoint
- `POST /api/run` with registered function → returns `{"run_id": ...}`
- `POST /api/run` with unknown function → returns run_id (error streamed over WS, not HTTP)

## Key design decisions
- `layout.py` tests patch `scistack_gui.db._db_path` directly (it's the module global)
- `_build_graph` tested indirectly via `GET /api/pipeline`
- No WebSocket tests (live streaming; too complex for unit tests without async test harness)
- `BaseVariable._all_subclasses` is global — test classes defined at module level in conftest so they're always registered
- `registry._functions` is cleared in `clear_db_state` to avoid pollution between registry tests
