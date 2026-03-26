# Thunk

**Lineage tracking for Python data pipelines.**

Thunk is a lightweight library inspired by Haskell's thunk concept, designed for building data processing pipelines with automatic provenance tracking. It captures the full computational lineage of your results, enabling reproducibility and intelligent caching.

## Features

- **Automatic Lineage Tracking**: Every computation captures its inputs and function, building a complete provenance graph
- **Input Classification**: Automatically distinguishes variable inputs from constants for accurate lineage
- **Pluggable Caching**: Set a query backend on `Thunk.query` to enable cache lookups via lineage hashes
- **Lightweight**: Core dependency is only `canonicalhash`
- **Type Safe**: Full type hints throughout

## Installation

```bash
pip install thunk
```

With optional dependencies:

```bash
pip install thunk[numpy]     # numpy support
pip install thunk[pandas]    # pandas support
pip install thunk[all]       # all optional dependencies
```

## Quick Start

### Basic Usage

```python
from thunk import thunk

@thunk
def process(data, factor):
    return data * factor

# Call returns a ThunkOutput, not the raw result
result = process([1, 2, 3], 2)

# Access the computed value
print(result.data)  # [2, 4, 6]

# Access lineage information
print(result.pipeline_thunk.inputs)  # {'arg_0': [1, 2, 3], 'arg_1': 2}
print(result.pipeline_thunk.thunk.fcn.__name__)  # 'process'
```

### Multi-Output Functions

```python
@thunk(unpack_output=True)
def split_data(data):
    mid = len(data) // 2
    return data[:mid], data[mid:]

first, second = split_data([1, 2, 3, 4])
print(first.data)   # [1, 2]
print(second.data)  # [3, 4]
```

### Chaining Computations

```python
@thunk
def normalize(data):
    max_val = max(data)
    return [x / max_val for x in data]

@thunk
def scale(data, factor):
    return [x * factor for x in data]

# Build a pipeline - lineage is automatically tracked
raw = [10, 20, 30, 40]
normalized = normalize(raw)
scaled = scale(normalized, 100)

# The full computation graph is captured
print(scaled.data)  # [25.0, 50.0, 75.0, 100.0]
```

### Extracting Lineage

```python
from thunk import extract_lineage, get_upstream_lineage

@thunk
def step1(x):
    return x + 1

@thunk
def step2(x):
    return x * 2

result = step2(step1(5))

# Get immediate lineage
lineage = extract_lineage(result)
print(lineage.function_name)  # 'step2'
print(lineage.function_hash)  # SHA-256 of function bytecode

# Get full upstream lineage chain (returns list of dicts)
chain = get_upstream_lineage(result)
for record in chain:
    print(f"{record['function_name']}: inputs={record['inputs']}, constants={record['constants']}")
```

### Caching via Query Backend

Thunk supports caching by setting a query backend on the `Thunk.query` class variable. The backend must implement a `find_by_lineage(pipeline_thunk)` method that returns cached results or `None`.

```python
from thunk import Thunk

class MyQueryBackend:
    """Custom cache backend for thunk lookups."""

    def find_by_lineage(self, pipeline_thunk):
        """Return list of cached values or None if not cached."""
        cache_key = pipeline_thunk.compute_lineage_hash()
        # Look up in your storage system...
        return None  # or [cached_value1, cached_value2, ...]

# Set the global query backend
Thunk.query = MyQueryBackend()

# Now repeated calls with same inputs will check the cache
```

## API Reference

### `@thunk(unpack_output=False, unwrap=True, generates_file=False)`

Decorator to convert a function into a Thunk.

- `unpack_output`: Whether to unpack a tuple return into separate ThunkOutputs (default: False)
- `unwrap`: If True, automatically unwrap `ThunkOutput` inputs to their raw data
- `generates_file`: If True, marks the function as producing files as side effects
  (plots, reports, exported CSVs) rather than returning data. When the result is
  saved, only lineage is stored — no data goes into DuckDB. On re-run with the
  same inputs, the function is skipped (cache hit with `data=None`).

### `ThunkOutput`

Wrapper around computed values that carries lineage.

- `.data`: The actual computed value
- `.pipeline_thunk`: The `PipelineThunk` that produced this
- `.hash`: Unique hash based on computation lineage
- `.output_num`: Index for multi-output functions
- `.is_complete`: True if the data has been computed

### `PipelineThunk`

Represents a specific function invocation with captured inputs.

- `.thunk`: The parent `Thunk` (function wrapper)
- `.inputs`: Dict of captured input values
- `.outputs`: Tuple of `ThunkOutput` results
- `.compute_lineage_hash()`: Generate lineage hash for cache key computation
- `.is_complete`: True if all inputs are concrete values

### `LineageRecord`

Structured provenance information.

- `.function_name`: Name of the function
- `.function_hash`: Hash of function bytecode
- `.inputs`: List of input descriptors (variables)
- `.constants`: List of constant values
- `.to_dict()`: Convert to dict for serialization
- `.from_dict(data)`: Create from dict

### Input Classification

- `InputKind`: Enum with values `THUNK_OUTPUT`, `SAVED_VARIABLE`, `UNSAVED_THUNK`, `RAW_DATA`, `CONSTANT`
- `ClassifiedInput`: Dataclass holding classified input info
- `classify_input(name, value)`: Classify a single input
- `is_trackable_variable(obj)`: Check if an object is a trackable variable

### Utility Functions

- `extract_lineage(thunk_output)`: Get `LineageRecord` for an output
- `get_upstream_lineage(thunk_output)`: Get full upstream lineage as list of dicts
- `find_unsaved_variables(thunk_output)`: Find unsaved variables in upstream chain
- `get_raw_value(data)`: Unwrap `ThunkOutput` or return as-is
- `canonical_hash(obj)`: Deterministic hash for any Python object (re-exported from `canonicalhash`)

## Integration with SciStack

Thunk is designed to work seamlessly with [SciStack](https://github.com/mtillman14/general-sqlite-database), a scientific data versioning framework. When used together, SciStack provides a `QueryByMetadata` backend that enables automatic caching:

```python
from scidb import BaseVariable
from thunk import Thunk, thunk

# SciStack sets up Thunk.query for cache lookups
# Thunk.query = QueryByMetadata(...)

@thunk
def process(data):
    return data * 2

result = process(loaded_data)
# Lineage is captured and can be stored alongside the data
```

## License

MIT License - see [LICENSE](LICENSE) for details.
