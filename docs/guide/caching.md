# Computation Caching

SciDB automatically caches computation results. When you save a variable produced by a thunked function, the result is cached. Future identical computations are skipped automatically.

## How Caching Works

1. **Save populates cache** - When saving a `ThunkOutput`, lineage is stored in PipelineDB
2. **Cache key** - Lineage hash: hash of function + input hashes
3. **Automatic lookup** - Thunks check `Thunk.query` (the `DatabaseManager`) before executing

```
                    ┌─────────────────┐
                    │ @thunk function │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌─────────────────┐          ┌─────────────────┐
    │  First run:     │          │  Later run:     │
    │  Execute +      │          │  Auto cache hit │
    │  Save + Cache   │          │  Skip execution │
    └─────────────────┘          └─────────────────┘
```

## Automatic Caching

Caching is fully automatic. Just save once, and future calls with the same inputs skip execution:

```python
@thunk
def expensive_computation(data):
    print("Computing...")  # Only prints on first run
    return data * 2

# First run: executes and prints "Computing..."
result = expensive_computation(raw_data)
MyVar.save(result, subject=1, stage="computed")

# Second run: cache hit, no execution!
result2 = expensive_computation(raw_data)  # No print, returns cached
print(result2.data)        # Same result, no recomputation
```

**Requirements for automatic caching:**

- Database must be configured (`configure_database(...)`)
- Variable class must be registered (happens on first `save()` or `load()`)

### Multi-Output Functions

Multi-output functions are also cached automatically. All outputs must be saved before caching takes effect:

```python
@thunk(unpack_output=True)
def split_data(data):
    print("Splitting...")  # Only prints on first run
    return data[:len(data)//2], data[len(data)//2:]

# First run: executes
left, right = split_data(raw_data)
LeftHalf.save(left, subject=1)
RightHalf.save(right, subject=1)

# Second run: cache hit for both outputs!
left2, right2 = split_data(raw_data)  # No print
```

**Important:** If only some outputs are saved, no caching occurs:

```python
left, right = split_data(raw_data)
LeftHalf.save(left, subject=1)  # Only save one output
# right is not saved

# Next run: cache miss (partial save)
left2, right2 = split_data(raw_data)  # Executes again
```

## Cache Key Components

The cache key (lineage hash) is a SHA-256 hash of:

| Component            | Source                   |
| -------------------- | ------------------------ |
| Function hash        | Bytecode + constants     |
| Input record_ids     | For saved variables      |
| Input content hashes | For unsaved values       |
| Output thunk hashes  | For chained computations |

This ensures cache hits only when:

- Same function code
- Same input data
- Same input metadata

## When Cache Misses Occur

Cache misses happen when:

| Scenario            | Reason                    |
| ------------------- | ------------------------- |
| First run           | No previous result exists |
| Different inputs    | Input data changed        |
| Function modified   | Bytecode hash changed     |
| Different constants | e.g., `x * 2` vs `x * 3` |

## Side-Effect Functions (`generates_file`)

Some pipeline steps produce files — plots, reports, exported CSVs — rather than
returning data to store in the database. You still want cache-hit behavior so
these steps are skipped on re-runs when inputs haven't changed.

Use `@thunk(generates_file=True)`:

```python
class Figure(BaseVariable):
    schema_version = 1

@thunk(generates_file=True)
def plot_signal(data, subject, session):
    plt.plot(data)
    plt.title(f"Subject {subject}, Session {session}")
    plt.savefig(f"signal_s{subject}_{session}.png")

# Run and save lineage (no data stored in DuckDB):
data = ProcessedData.load(subject=1, session="A")
result = plot_signal(data, subject=1, session="A")
Figure.save(result, subject=1, session="A")  # Returns "generated:..." ID

# Next run — cache hit, function skipped:
data = ProcessedData.load(subject=1, session="A")
result = plot_signal(data, subject=1, session="A")
# result.data is None, result.is_complete is True
```

### How it works

1. `Figure.save()` detects `generates_file=True` on the thunk and saves **lineage only** to PipelineDB, with a `generated:` prefixed record ID. No data row is written to DuckDB.
2. On the next call with the same inputs, `Thunk.query.find_by_lineage()` finds the `generated:` record and returns a cache hit with `data=None`.
3. The function is never re-executed.

### With `for_each`

When used with `for_each`, a `generates_file` function can receive
the current metadata values by using `as_table=True`, which keeps schema key
columns in the input DataFrames:

```python
for_each(
    plot_signal,
    inputs={"data": ProcessedData},
    outputs=[Figure],
    as_table=True,
    subject=subjects,
    session=sessions,
)
```

## Best Practices

### 1. Save After Expensive Computations

```python
result = expensive_computation(data)
MyVar.save(result, subject=1)  # Populates cache
```

### 2. Use Saved Variables as Inputs

Variables with record_ids have stable cache keys:

```python
# Good: loaded variable has record_id
raw = RawData.load(subject=1)
result = process(raw)  # Pass variable, not .data

# Less stable: unsaved data uses content hash
result = process(np.array([1, 2, 3]))
```

### 3. Cache Keys Are Content-Based

If you modify a function's code, the cache key changes automatically. You don't need to manually invalidate—the next run will simply compute fresh results.
