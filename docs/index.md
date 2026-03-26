# SciStack

**Scientific Data Versioning Framework**

SciStack is a lightweight database framework for scientific computing that provides automatic versioning, provenance tracking, and computation caching using DuckDB for data storage and SQLite for lineage persistence.

## Key Features

- **Type-safe storage** - Define custom variable types with explicit serialization
- **Content-based versioning** - Automatic deduplication via deterministic hashing
- **Metadata addressing** - Query data by flexible key-value metadata
- **Lineage tracking** - Automatic provenance capture via `@thunk` decorator
- **External library support** - Wrap functions from scipy, sklearn, etc. with `Thunk(fn)`
- **Computation caching** - Skip redundant computations automatically
- **Portable storage** - DuckDB file for data, SQLite file for lineage

## Installation

```bash
pip install scidb
```

## Quick Example

```python
from scidb import BaseVariable, configure_database, thunk
import numpy as np

# Define a variable type (native storage - no to_db/from_db needed)
class TimeSeries(BaseVariable):
    pass

# Setup (DuckDB for data, SQLite for lineage)
db = configure_database("experiment.duckdb", ["subject", "session"], "pipeline.db")

# Save with metadata
data = np.array([1.0, 2.0, 3.0])
TimeSeries.save(data, subject=1, session="baseline")

# Load by metadata
loaded = TimeSeries.load(subject=1, session="baseline")

# Track lineage with @thunk
@thunk
def normalize(arr: np.ndarray) -> np.ndarray:
    return (arr - arr.mean()) / arr.std()

result = normalize(loaded)  # Pass the variable, not .data
TimeSeries.save(result, subject=1, session="normalized")

# Query provenance
provenance = db.get_provenance(TimeSeries, subject=1, session="normalized")
print(provenance["function_name"])  # "normalize"
```

## Why SciStack?

| Problem                                   | SciStack Solution                                |
| ----------------------------------------- | --------------------------------------------- |
| "Which version of this data did I use?"   | Content-based hashing ensures reproducibility |
| "What processing produced this result?"   | Automatic lineage tracking via `@thunk`       |
| "I already computed this, why recompute?" | Computation caching skips redundant work      |
| "How do I organize my experimental data?" | Flexible metadata addressing                  |
| "I need to share this database"           | Portable DuckDB + SQLite files                |

## Documentation

- [Quickstart](quickstart.md) - Get up and running in 5 minutes
- [VO2 Max Walkthrough](guide/walkthrough.md) - Step-by-step example with design philosophy
- [User Guide](guide/variables.md) - Detailed documentation
- [API Reference](api.md) - Complete API documentation
