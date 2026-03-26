# SciStack

Scientific Data Versioning Framework with provenance tracking.

# Architecture

Once-per-pipeline setup

```python
pipeline_db = PipelineDB("path/to/pipeline.sqlite")
sciduck_db = SciDuck("path/to/sci.duckdb", dataset_schema = ["subject", "session", "trial"])
Thunk.query = db  # DatabaseManager instance (set by configure_database)
```

```
User Code
    │
    ├─────────────────────────────────────────────────┐
    │                                                 │
    ▼                                                 ▼
BaseVariable                                      @thunk functions
(in SciStack)                                        (from thunk-lib)
    │                                                 │
    │ .save() / .load()                               │ returns ThunkOutput
    │                                                 │
    ▼                                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                        SciStack Integration Layer                  │
│  - Bridges data storage and lineage                             │
│  - Provides find_by_lineage for cache lookups                   │
└────────────────┬────────────────────────┬───────────────────────┘
                 │                        │
                 ▼                        ▼
         ┌─────────────┐          ┌─────────────┐
         │   SciDuck   │          │  PipelineDB │
         │   (DuckDB)  │◄─────────│   (SQLite)  │
         │             │ record_id│             │
         │  - Data     │ reference│  - Lineage  │
         │  - Versions │          │  - Thunks   │
         │  - Groups   │          │  - Inputs   │
         └─────────────┘          └─────────────┘
```

# Components

## Data Storage Layer: SciDuck

DuckDB-based database for data storage. Each variable is identified by a unique name. Focuses only on loading/saving variables to/from the database.

User-facing API:

```python
from sciduckdb import SciDuck

duck = SciDuck("data.duckdb", dataset_schema=["subject", "session"])
duck.save("MyVar", df, subject=1, session=1)
loaded = duck.load("MyVar", subject=1, session=1)
```

## Lineage Layer: Thunk

In-memory lineage/provenance tracking through Haskell-style Thunk objects.

User-facing API:

```python
from thunk import thunk

@thunk
def fcn1(a: int):
    return a * 2

@thunk
def fcn2(b: int):
    return b - 4

result1 = fcn1(2)
result2 = fcn2(result1)  # Tracks provenance

# Alternative
thunked_fcn1 = Thunk(fcn1)
```

## Lineage Persistence Layer: PipelineDB

SQLite-based lineage persistence layer. Stores computation lineage (provenance) separately from data, using `record_id` references to link to data in SciDuck.

User-facing API:

```python
from pipelinedb import PipelineDB

db = PipelineDB("pipeline.db")

# Save lineage for a computation
db.save_lineage(
    output_record_id="abc123",
    output_type="ProcessedData",
    lineage_hash="def456",
    function_name="process_data",
    function_hash="ghi789",
    inputs=[{"name": "arg_0", "record_id": "xyz000", "type": "RawData"}],
    constants=[],
)

# Look up by lineage hash (for cache hits)
records = db.find_by_lineage_hash("def456")
```

## Integration Layer: SciStack

Bridges SciDuck (data) and PipelineDB (lineage) with the BaseVariable abstraction. Provides:

- `BaseVariable`: Type-safe serialization with metadata addressing
- `DatabaseManager.find_by_lineage()`: Cache lookups via lineage hash
- `configure_database()`: Unified configuration

`configure_database()` sets `Thunk.query` to the `DatabaseManager` instance. That allows `Thunk.__call__()` to run queries by the lineage hash for the current Thunk. If no cached result is found, the function executes. If found, returns the previously computed data.

User-facing API:

```python
from scidb import configure_database, BaseVariable, thunk
import numpy as np
import pandas as pd

class RotationMatrix(BaseVariable):
    pass

# Setup (creates both DuckDB for data and SQLite for lineage)
db = configure_database("experiment.duckdb", ["subject", "trial"], "pipeline.db")

# Save/load
RotationMatrix.save(np.eye(3), subject=1, trial=1)
loaded = RotationMatrix.load(subject=1, trial=1)
```

## Batch Execution Layer: SciRun

Loosely coupled batch execution utilities. Runs functions over combinations of metadata, automatically loading inputs and saving outputs.

User-facing API:

```python
from scirun import for_each, Fixed

for_each(
    process_data,
    inputs={"raw": RawData, "calibration": Fixed(Calibration, session="baseline")},
    outputs=[ProcessedData],
    subject=[1, 2, 3],
    session=["A", "B", "C"],
)
```

## Network Layer: scidb-net

Client-server HTTP layer for remote database access. Wraps `DatabaseManager` behind a FastAPI server and provides a drop-in HTTP client (`RemoteDatabaseManager`) so existing code works transparently over the network.

User-facing API:

```python
# Server
from scidbnet import create_app
app = create_app("experiment.duckdb", ["subject", "session"], "pipeline.db")

# Client — one-line replacement for configure_database()
from scidbnet import configure_remote_database
configure_remote_database("http://server:8000")

# All existing code works unchanged
RawData.save(data, subject=1, session="A")
loaded = RawData.load(subject=1, session="A")
```
