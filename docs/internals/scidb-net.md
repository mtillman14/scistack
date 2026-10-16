# scidb-net

/// info | Optional package
Most projects don't need it. `scidb-net` lets the database live on another machine, such as a shared lab server, while your `save()` / `load()` calls stay unchanged.
///

`scidb-net` runs `scidb`'s database behind a FastAPI server and provides a client that stands in for it. Your analysis code changes only at setup: call `configure_remote_database(url)` where you would call `configure_database(path, keys)`.

```text
CLIENT                                   SERVER
your analysis code                       scidb-server
configure_remote_database(url)           create_app(db_path, schema_keys)
RemoteDatabaseManager  ── HTTP ──▶       FastAPI app ─▶ the DuckDB file
```

Install with `pip install scidb-net` on both machines. Both sides also need your variable class definitions, for example in a shared package.

## Server

/// tab | CLI
```bash
export SCIDB_DATASET_DB_PATH=/data/experiment.duckdb
export SCIDB_DATASET_SCHEMA_KEYS='["subject", "session"]'
scidb-server                 # SCIDB_HOST (0.0.0.0) / SCIDB_PORT (8000) optional
```
///

/// tab | Python
```python
from scidbnet import create_app
import uvicorn

app = create_app(dataset_db_path="/data/experiment.duckdb",
                 dataset_schema_keys=["subject", "session"])
uvicorn.run(app, host="0.0.0.0", port=8000)
```
///

To check that the server is up: `curl http://server:8000/api/v1/health` returns `{"status":"ok"}`.

## Client

```python
from scidbnet import configure_remote_database

configure_remote_database("http://server:8000", timeout=30.0)
RawSignal.save(data, subject=1, session="A")     # stored on the server
RawSignal.load(subject=1, session="A")
```

`configure_remote_database` creates a `RemoteDatabaseManager`, registers every known `BaseVariable` subclass with the server, makes the client the current database, and returns the client.

## Errors

Import these from `scidbnet.exceptions`:

| Exception | When |
|---|---|
| `NetworkError` | The connection failed or timed out. |
| `ServerError` | The server returned an error. The HTTP status is in `.status_code`. |
| `SerializationError` | Data could not be encoded or decoded. |

## Security

`scidb-net` has no authentication and no TLS, so run it only on a trusted network. For anything wider, put it behind a reverse proxy that adds TLS and authentication, or restrict access with a VPN or firewall.
