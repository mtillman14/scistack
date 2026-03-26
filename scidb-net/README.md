# scidb-net

Network client-server layer for SciStack. Wraps `DatabaseManager` behind a FastAPI server and provides a drop-in HTTP client so existing `BaseVariable.save()`/`.load()` and thunk caching work transparently over the network.

## How It Works

```
CLIENT MACHINE                       SERVER MACHINE
==============                       ==============

Your analysis code                   scidb-server process
       |                                    |
  configure_remote_database()        create_app() / scidb-server CLI
       |                                    |
  RemoteDatabaseManager  --- HTTP -->  FastAPI app
       |                                    |
  RawSignal.save(...)                  DatabaseManager
  RawSignal.load(...)                       |
  Thunk caching                          DuckDB
                                     (data + lineage)
```

All existing user code works unchanged. The only difference is a single
`configure_remote_database()` call at the top of your script instead of
`configure_database()`.

## Prerequisites

- Python 3.10+
- An existing SciStack project with variable definitions (subclasses of `BaseVariable`)
- Network access between client and server machines

## Installation

Install on **both** the server and client machines:

```bash
pip install scidb-net
```

This pulls in `scidb`, `fastapi`, `uvicorn`, `httpx`, and `pyarrow` as
dependencies.

## Deployment Walkthrough

This walks through setting up a server on a shared lab machine and connecting
to it from a laptop.

### Step 1: Define your variables (shared code)

Both server and client need access to the same variable definitions. Put them
in a shared package (e.g. `my_experiment`):

```python
# my_experiment/variables.py
import numpy as np
from scidb.variable import BaseVariable

class RawSignal(BaseVariable):
    """Raw EEG signal, stored as a numpy array."""
    pass

class ProcessedSignal(BaseVariable):
    """Band-pass filtered signal."""
    pass
```

Install this package on both machines:

```bash
pip install -e ./my_experiment
```

### Step 2: Start the server

On the server machine (the one with storage), choose either the CLI or
programmatic approach.

**Option A: CLI (recommended for production)**

```bash
export SCIDB_DATASET_DB_PATH=/data/experiment.duckdb
export SCIDB_DATASET_SCHEMA_KEYS='["subject", "session"]'

scidb-server
```

The server starts on `0.0.0.0:8000` by default.

To customize the bind address or port:

```bash
export SCIDB_HOST=0.0.0.0
export SCIDB_PORT=9000
scidb-server
```

**Option B: Programmatic**

```python
# run_server.py
from scidbnet import create_app
import uvicorn

app = create_app(
    dataset_db_path="/data/experiment.duckdb",
    dataset_schema_keys=["subject", "session"],
)

uvicorn.run(app, host="0.0.0.0", port=8000)
```

```bash
python run_server.py
```

### Step 3: Verify the server is running

From any machine with network access:

```bash
curl http://server-hostname:8000/api/v1/health
# {"status":"ok"}
```

### Step 4: Connect from the client

On the client machine, import your variables and call
`configure_remote_database()`:

```python
# analysis.py
from my_experiment.variables import RawSignal, ProcessedSignal
from scidbnet import configure_remote_database

configure_remote_database("http://server-hostname:8000")

# Save data — goes over the network to the server's DuckDB
import numpy as np
RawSignal.save(np.random.randn(1000), subject=1, session="A")

# Load it back
signal = RawSignal.load(subject=1, session="A")
print(signal.data.shape)  # (1000,)

# Thunk caching also works remotely
from scidb import thunk

@thunk
def bandpass_filter(raw):
    # ... filtering logic ...
    return filtered

result = bandpass_filter(signal)
ProcessedSignal.save(result, subject=1, session="A")
```

That's it. Every `save()`, `load()`, `load_all()`, and thunk cache lookup
goes through the network to the shared server.

## Server Configuration Reference

All server settings are passed either as arguments to `create_app()` or as
environment variables for the CLI.

| Environment Variable | `create_app()` Argument | Required | Default | Description |
|---|---|---|---|---|
| `SCIDB_DATASET_DB_PATH` | `dataset_db_path` | Yes | — | Path to the DuckDB file for data and lineage storage |
| `SCIDB_DATASET_SCHEMA_KEYS` | `dataset_schema_keys` | Yes | — | JSON list of metadata keys (e.g. `'["subject", "session"]'`) |
| `SCIDB_LINEAGE_MODE` | `lineage_mode` | No | `"strict"` | `"strict"` or `"ephemeral"` |
| `SCIDB_HOST` | — | No | `"0.0.0.0"` | Bind address (CLI only) |
| `SCIDB_PORT` | — | No | `8000` | Port number (CLI only) |

## Client Configuration

```python
configure_remote_database(
    base_url,       # Server URL, e.g. "http://server:8000"
    timeout=30.0,   # HTTP request timeout in seconds
)
```

This function:
1. Creates a `RemoteDatabaseManager` pointing at the server.
2. Registers all known `BaseVariable` subclasses with the server.
3. Sets `Thunk.query` so thunk caching queries the remote database.
4. Returns the `RemoteDatabaseManager` instance.

## Error Handling

scidb-net raises three exception types (all under `scidbnet.exceptions`):

| Exception | When |
|---|---|
| `NetworkError` | HTTP connection failure or timeout |
| `ServerError` | Server returned an error (has a `.status_code` attribute) |
| `SerializationError` | Data encoding/decoding failure |

```python
from scidbnet.exceptions import NetworkError, ServerError

try:
    RawSignal.load(subject=99, session="Z")
except ServerError as e:
    print(f"Server error ({e.status_code}): {e}")
except NetworkError as e:
    print(f"Connection failed: {e}")
```

## API Endpoints

All endpoints live under `/api/v1/`.

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Health check |
| POST | `/register` | Register a variable type |
| POST | `/save` | Save data + metadata + lineage |
| POST | `/load` | Load a single variable |
| POST | `/load_all` | Load all matching variables |
| POST | `/list_versions` | List versions at a schema location |
| POST | `/provenance` | Get provenance of a variable |
| POST | `/provenance_by_schema` | Get all provenance by schema keys |
| GET | `/pipeline_structure` | Get pipeline DAG structure |
| POST | `/has_lineage` | Check if a variable has lineage |
| POST | `/save_ephemeral_lineage` | Save temporary lineage record |
| POST | `/export_to_csv` | Export variables to CSV (server-side) |
| POST | `/find_by_lineage` | Cache lookup by lineage hash |
| POST | `/close` | Close the server-side database |

## Wire Format

- **DataFrames / numpy arrays**: Arrow IPC
- **Scalars / dicts / lists**: JSON
- **Envelope**: `4-byte header length | JSON header | body bytes`

## Security Notes

scidb-net does **not** include built-in authentication or TLS. It assumes a
trusted network. For production deployments, consider:

- Running behind a reverse proxy (nginx, Caddy) with TLS termination
- Restricting access with a firewall or VPN
- Adding an auth middleware to the FastAPI app
