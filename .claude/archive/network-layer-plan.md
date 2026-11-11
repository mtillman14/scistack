Here is Claude's plan:
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
Plan: Network Layer for SciStack (scidb-net package)

Context

SciStack currently uses local DuckDB + SQLite files accessed directly. We need a client-server layer so users can work over a network. The architecture is already well-factored: all DB access goes through
DatabaseManager, which delegates to SciDuck (DuckDB) and PipelineDB (SQLite). The plan is a single HTTP server + client, using Arrow IPC for DataFrames/arrays and JSON for metadata.

Package Structure

New submodule: scidb-net/ (module name: scidbnet)

scidb-net/
pyproject.toml # hatchling, deps: scidb, fastapi, uvicorn, httpx, pyarrow, pydantic
README.md
src/scidbnet/
**init**.py # Exports: RemoteDatabaseManager, create_app, configure_remote_database
server.py # FastAPI app wrapping a real DatabaseManager
client.py # RemoteDatabaseManager mirroring DatabaseManager's public API
serialization.py # Arrow IPC for DataFrames/arrays, JSON for scalars/dicts
\_types.py # Pydantic request/response models (shared by client & server)
exceptions.py # NetworkError, ServerError, SerializationError
tests/
**init**.py
conftest.py # FastAPI TestClient fixtures, test variable classes
test_serialization.py # Serialization round-trip unit tests
test_server.py # Server route tests
test_client.py # Client integration tests (via TestClient)
test_cache.py # find_by_lineage cache tests over network

Architecture

Client side Server side
───────────── ───────────
User code FastAPI app
│ │
▼ ▼
RemoteDatabaseManager ──HTTP──> Route handlers
│ │
├─ serialize_data() ├─ deserialize_data()
│ (Arrow IPC / JSON) │
│ ▼
│ DatabaseManager (real)
│ ├─ SciDuck (DuckDB)
│ └─ PipelineDB (SQLite)
│ │
├─ deserialize_data() <──HTTP── serialize_data()
▼
User code (BaseVariable instances)

Wire Protocol

- Binary data (DataFrames, numpy arrays): Arrow IPC bytes
- Simple data (scalars, dicts, lists): JSON
- Envelope: 4-byte header length (big-endian) | JSON header | body bytes
  - Header describes format: {"format": "dataframe"}, {"format": "numpy", "dtype": "float64", "shape": [3, 4]}, {"format": "json_scalar", "python_type": "int"}, etc.
- Save requests: 4-byte meta_len | JSON metadata | data envelope
- Multi-value responses (load_all, find_by_lineage): 4-byte count | [4-byte part_len | envelope]...

Server Endpoints (/api/v1/)
┌────────┬─────────────────────────┬────────────────────────────────────────┐
│ Method │ Path │ Purpose │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ GET │ /health │ Health check │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /register │ Register a variable type │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /save │ Save data + metadata + lineage │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /load │ Load single variable → binary response │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /load_all │ Load all matching → packed binary │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /list_versions │ List versions → JSON │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /provenance │ Get provenance → JSON │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /provenance_by_schema │ Get provenance by schema → JSON │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ GET │ /pipeline_structure │ Get pipeline structure → JSON │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /has_lineage │ Check lineage → JSON bool │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /save_ephemeral_lineage │ Save ephemeral lineage → JSON │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /export_to_csv │ Export CSV (server-side) → JSON count │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /find_by_lineage │ Cache lookup by hash → binary or null │
├────────┼─────────────────────────┼────────────────────────────────────────┤
│ POST │ /close │ Close database │
└────────┴─────────────────────────┴────────────────────────────────────────┘
Server config via env vars: SCIDB_DATASET_DB_PATH, SCIDB_DATASET_SCHEMA_KEYS (JSON), SCIDB_PIPELINE_DB_PATH, SCIDB_LINEAGE_MODE. CLI entry point: scidb-server.

Also accepts programmatic config via create_app(dataset_db_path, dataset_schema_keys, pipeline_db_path, lineage_mode).

Key Design Decisions

1.  Network boundary at DatabaseManager level — server wraps a full DatabaseManager. Client mirrors its public API. No changes to SciDuck/PipelineDB needed.
2.  ThunkOutput handled client-side — ThunkOutput contains in-memory Python object graphs that can't be serialized. The client extracts lineage + raw data locally (same logic as
    DatabaseManager.save_variable lines 403-452), then sends the unwrapped data to the server's save endpoint.
3.  Custom serialization split — to_db()/from_db() runs on the client. Server stores the resulting DataFrame. On load, server returns raw DataFrame, client calls from_db(). Server never needs user-defined
    code.
4.  Variable registration — Client sends type metadata (name, table_name, schema_version, has_custom_serialization). Server creates dynamic BaseVariable subclasses via type() with correct metadata. Server
    never needs actual class definitions.
5.  Cache lookup — Client computes lineage_hash locally (pure function of function hash + input hashes), sends just the hash string. Server queries PipelineDB, loads matching data from DuckDB, returns
    serialized values.
6.  No changes to existing code needed — configure_remote_database() sets Thunk.query and \_local.database to the RemoteDatabaseManager. All existing user code (BaseVariable.save(), .load(), thunk caching)
    works transparently.

Implementation Order

Phase 1: Placeholders (all files with signatures + docstrings, no logic)

1.  Create scidb-net/pyproject.toml
2.  Create scidb-net/src/scidbnet/exceptions.py
3.  Create scidb-net/src/scidbnet/\_types.py — all Pydantic models
4.  Create scidb-net/src/scidbnet/serialization.py — serialize_data(), deserialize_data(), encode_response(), decode_response()
5.  Create scidb-net/src/scidbnet/server.py — FastAPI app with all route stubs
6.  Create scidb-net/src/scidbnet/client.py — RemoteDatabaseManager with all method stubs
7.  Create scidb-net/src/scidbnet/**init**.py — exports + configure_remote_database()

Phase 2: Implementation (fill in all logic)

1.  serialization.py — Arrow IPC for DataFrames/numpy, JSON for scalars/dicts, envelope encoding
2.  server.py — All route handlers with real DatabaseManager delegation
3.  client.py — All methods with HTTP calls + serialization

Phase 3: Tests

1.  conftest.py — Fixtures with FastAPI TestClient, test variable classes
2.  test_serialization.py — Round-trip tests for all data types
3.  test_server.py — Direct route tests
4.  test_client.py — Client→server integration tests
5.  test_cache.py — find_by_lineage cache lookup tests

Phase 4: Documentation

1.  scidb-net/README.md — Overview, installation, quick start for both server and client usage

Key Files Referenced

- /workspace/src/scidb/database.py — DatabaseManager (the interface we mirror)
- /workspace/src/scidb/variable.py — BaseVariable (.\_all_subclasses, get_subclass_by_name, \_has_custom_serialization)
- /workspace/thunk-lib/src/thunk/core.py — Thunk.query.find_by_lineage() call at line 106
- /workspace/thunk-lib/src/thunk/lineage.py — LineageRecord.to_dict()/from_dict()
- /workspace/src/scidb/lineage.py — Re-exports: extract_lineage, find_unsaved_variables, get_raw_value

Verification

1.  cd scidb-net && python -m pytest tests/ -v — all tests pass
2.  Serialization round-trips: scalar, numpy 1D/2D, DataFrame, dict, list, bool
3.  Server save/load cycle: register type → save data → load data → verify match
4.  Cache test: save with lineage → find_by_lineage returns cached data
5.  Custom serialization: type with to_db/from_db works end-to-end
