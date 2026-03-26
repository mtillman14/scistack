# Caching

Thunk supports pluggable caching via the `Thunk.query` class variable. When a query backend is set, thunked functions automatically check for cached results before executing.

## Configuring a Query Backend

Set a query backend on `Thunk.query`. The backend must implement a `find_by_lineage(pipeline_thunk)` method:

```python
from thunk import Thunk

class MyQueryBackend:
    def __init__(self):
        self.store = {}

    def find_by_lineage(self, pipeline_thunk):
        """
        Look up cached results by lineage hash.

        Args:
            pipeline_thunk: The PipelineThunk to look up

        Returns:
            List of (data, identifier) tuples if cached,
            None otherwise.
        """
        cache_key = pipeline_thunk.compute_lineage_hash()
        if cache_key in self.store:
            return self.store[cache_key]
        return None

    def save(self, cache_key, results):
        """Save results to cache."""
        self.store[cache_key] = results

backend = MyQueryBackend()
Thunk.query = backend
```

## Lineage Hash Computation

Lineage hashes are computed from:

1. **Function hash**: SHA-256 of bytecode + constants
2. **Input lineage**: For each input:
   - `ThunkOutput`: Uses its lineage-based hash
   - Trackable variable: Uses lineage_hash if available
   - Raw value: Uses content hash

This means:

- Same function + same inputs = same lineage hash
- Same content but different computation path = different lineage hash

```python
@thunk
def process(x):
    return x * 2

result1 = process(5)
result2 = process(5)

# Same lineage hash
key1 = result1.pipeline_thunk.compute_lineage_hash()
key2 = result2.pipeline_thunk.compute_lineage_hash()
assert key1 == key2
```

## Cache Hit Behavior

When a cache hit occurs, the function is not executed and the cached values are wrapped in `ThunkOutput` objects:

```python
from thunk import thunk, Thunk

class SimpleCache:
    def __init__(self):
        self.store = {}
        self.hits = 0

    def find_by_lineage(self, pipeline_thunk):
        cache_key = pipeline_thunk.compute_lineage_hash()
        if cache_key in self.store:
            self.hits += 1
            return self.store[cache_key]
        return None

    def save(self, cache_key, results):
        self.store[cache_key] = results

cache = SimpleCache()
Thunk.query = cache

@thunk
def expensive(x):
    print("Computing...")
    return x ** 2

# First call - computes (prints "Computing...")
r1 = expensive(5)
cache.save(r1.pipeline_thunk.compute_lineage_hash(), [(r1.data, "cached")])

# Second call - cache hit (no "Computing..." printed)
r2 = expensive(5)
print(cache.hits)  # 1
```

## Disabling Cache

```python
from thunk import Thunk

# Disable caching
Thunk.query = None
```

## Integration with SciStack

When used with SciStack, the `QueryByMetadata` class is set as the query backend:

```python
from thunk import Thunk

# SciStack sets up Thunk.query for lineage-based cache lookups
# Thunk.query = QueryByMetadata(...)

@thunk
def process(data):
    return data * 2

# Results are automatically checked against existing database records
result = process(input_data)
```

## Best Practices

1. **Cache expensive computations**: Focus on functions that take significant time
2. **Consider cache invalidation**: Function changes invalidate cache entries automatically (different bytecode = different lineage hash)
3. **Handle cache misses gracefully**: The system falls back to recomputation if `find_by_lineage` returns `None` or raises an exception
4. **Monitor cache hit rates**: Track effectiveness of your caching strategy
