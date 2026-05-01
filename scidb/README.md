# SciDB

Database operations layer for SciStack. Provides abstractions for defining typed variables, configuring the database, and saving/loading data by metadata.

```python
from scidb import configure_database, BaseVariable
import numpy as np

db = configure_database("experiment.duckdb", ["subject", "session"])

class RawSignal(BaseVariable):
    schema_version = 1

RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")
raw = RawSignal.load(subject=1, session="A")
```