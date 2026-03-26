# Quickstart

Get started with SciStack in 5 minutes.

## Installation

```bash
pip install scidb
```

## 1. Define a Variable Type

For most data types (scalars, numpy arrays, lists, dicts), no serialization methods are needed — SciDuck handles them natively:

```python
from scidb import BaseVariable
import numpy as np

class SignalData(BaseVariable):
    pass
```

For simple types, conversion to and from the database is handled automatically. For complex custom serialization, override `to_db()` and `from_db()`:

```python
import pandas as pd

class CustomSignal(BaseVariable):

    def to_db(self) -> pd.DataFrame:
        """Convert numpy array to DataFrame for storage."""
        return pd.DataFrame({
            "index": range(len(self.data)),
            "value": self.data
        })

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> np.ndarray:
        """Convert DataFrame back to numpy array."""
        return df.sort_values("index")["value"].values
```

## 2. Configure the Database

```python
from scidb import configure_database

# dataset_schema_keys defines which metadata keys identify dataset location
# (vs. computational variants at that location)
db = configure_database(
    "my_experiment.duckdb",
    dataset_schema_keys=["subject", "trial", "condition"],
)
```

## 3. Save Data with Metadata

```python
# Save data with metadata
signal = np.sin(np.linspace(0, 2*np.pi, 100))
record_id = SignalData.save(signal,
    subject=1,
    trial=1,
    condition="control"
)
print(f"Saved with hash: {record_id[:16]}...")
```

## 4. Load Data by Metadata

```python
# Load by metadata query
loaded = SignalData.load(subject=1, trial=1)
print(loaded.data)       # The numpy array
print(loaded.record_id)      # Version hash
print(loaded.metadata)   # {"subject": 1, "trial": 1, "condition": "control"}
```

## 5. Track Processing Lineage

Use `@thunk` to automatically track what processing produced each result:

```python
from scidb import thunk

@thunk
def bandpass_filter(signal: np.ndarray, low: float, high: float) -> np.ndarray:
    # Your filtering logic here
    return filtered_signal

@thunk
def compute_power(signal: np.ndarray) -> float:
    return np.mean(signal ** 2)

# Run pipeline - lineage tracked automatically
# Pass the loaded variable (not .data) to preserve lineage
raw = SignalData.load(subject=1, trial=1)
filtered = bandpass_filter(raw, low=1.0, high=40.0)
power = compute_power(filtered)

# Save result - lineage captured
class PowerValue(BaseVariable):
    pass

PowerValue.save(power, subject=1, trial=1, stage="power")

# Query what produced this result
provenance = db.get_provenance(PowerValue, subject=1, trial=1, stage="power")
print(provenance["function_name"])  # "compute_power"
```

## 6. Wrap External Functions

Leverage existing libraries with lineage tracking:

```python
from scidb import Thunk
from scipy.signal import butter, filtfilt

# Wrap external functions
# unpack_output=True splits the returned tuple into separate ThunkOutputs
thunked_butter = Thunk(butter, unpack_output=True)
thunked_filtfilt = Thunk(filtfilt)

# Use with full lineage tracking
b, a = thunked_butter(N=4, Wn=0.1, btype='low')
filtered = thunked_filtfilt(b, a, raw_data)

SignalData.save(filtered, subject=1, stage="filtered")
```

## 7. Specialized Types via Subclassing

When one variable class represents multiple data types, create subclasses:

```python
# Create specialized types - each gets its own table
class Temperature(SignalData):
    pass  # Table: Temperature

class Humidity(SignalData):
    pass  # Table: Humidity

# Data stored in separate tables (auto-registered on first save)
Temperature.save(temp_array, sensor=1, day="monday")
Humidity.save(humid_array, sensor=1, day="monday")
```

## Next Steps

- [VO2 Max Walkthrough](guide/walkthrough.md) - Full example pipeline with design philosophy explanations
- [Variables Guide](guide/variables.md) - Deep dive into variable types
- [Database Guide](guide/database.md) - All database operations
- [Lineage Guide](guide/lineage.md) - Full lineage tracking details
- [Caching Guide](guide/caching.md) - Computation caching
