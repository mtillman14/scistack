# Lineage Tracking

SciStack automatically tracks data provenance using the `@thunk` decorator. When you save data produced by a thunked function, the lineage is captured automatically.

## The `@thunk` Decorator

Wrap processing functions with `@thunk` to enable lineage tracking:

```python
from scidb import thunk

@thunk
def process_signal(signal: np.ndarray, factor: float) -> np.ndarray:
    return signal * factor

# Returns ThunkOutput, not raw array
result = process_signal(data, 2.5)
print(result.data)  # The actual array
```

### Multiple Outputs

```python
@thunk(unpack_output=True)
def split_data(data: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    mid = len(data) // 2
    return data[:mid], data[mid:]

first_half, second_half = split_data(data)
```

## How It Works

1. **Thunk wraps function** - Creates a `Thunk` object with function hash
2. **Call creates PipelineThunk** - Captures all inputs
3. **Execution returns ThunkOutput** - Wraps result with lineage reference
4. **Save extracts lineage** - Stores provenance in PipelineDB (SQLite)

```
@thunk decorator
    │
    ▼
┌─────────┐      ┌───────────────┐      ┌─────────────┐
│  Thunk  │──────│ PipelineThunk │──────│ ThunkOutput │
│ (func)  │      │ (inputs)      │      │ (result)    │
└─────────┘      └───────────────┘      └─────────────┘
    │                   │                      │
    │                   │                      ▼
    │                   │               ┌─────────────┐
    │                   └──────────────▶│  .save()    │
    │                                   │  captures   │
    └──────────────────────────────────▶│  lineage    │
                                        └─────────────┘
```

## Automatic Lineage Capture

When saving a `ThunkOutput`, lineage is captured automatically:

```python
@thunk
def normalize(arr):
    return (arr - arr.mean()) / arr.std()

raw = RawData.load(subject=1)
normalized = normalize(raw)  # Pass the variable, not .data

# Lineage captured on save
NormalizedData.save(normalized, subject=1, stage="normalized")
```

**Important:** Pass the `BaseVariable` instance (not `.data`) to the thunk to preserve lineage tracking.

## Two Levels of Lineage

SciStack provides two complementary views of provenance, both stored in the same
lineage table (PipelineDB/SQLite):

### Schema-blind: Pipeline Structure

Answers: **"How is the pipeline generally structured?"**

This is a derived view that groups lineage records by function and variable types,
ignoring specific data instances. It describes the abstract computation graph:

```
RawData ──[normalize()]──> NormalizedData ──[analyze()]──> FinalResult
```

No record_ids, no content hashes, no schema keys. Just the topology.

```python
structure = db.get_pipeline_structure()

for step in structure:
    print(f"{step['input_types']} --[{step['function_name']}]--> {step['output_type']}")
# ['RawData'] --[normalize]--> NormalizedData
# ['NormalizedData'] --[analyze]--> FinalResult
```

### Schema-aware: Instance Provenance

Answers: **"Where exactly did this particular data come from?"**

Each lineage record includes the schema keys and content hashes for a specific
computation, enabling queries like "what inputs at subject=1, session=1 produced
this output?"

```
RawData(subject=S01, session=1, content_hash=abc)
    ──[normalize(), hash=a1b2c3]──>
NormalizedData(subject=S01, session=1, content_hash=def)
```

```python
# Find all computations at a specific schema location
records = db.get_provenance_by_schema(subject="S01", session="1")

for r in records:
    print(f"{r['function_name']}: {r['output_type']}")
    print(f"  output content_hash: {r['output_content_hash']}")
    print(f"  inputs: {r['inputs']}")
```

### Lineage Table Structure

Each lineage record stores both levels of information:

| Column | Schema-blind | Schema-aware | Description |
|---|---|---|---|
| `function_name` | Yes | Yes | Name of the function |
| `function_hash` | Yes | Yes | SHA-256 of function bytecode |
| `output_type` | Yes | Yes | Variable class name |
| `inputs` | Types only | Full details | Input descriptors (JSON) |
| `output_record_id` | -- | Yes | Unique ID of the saved output |
| `output_content_hash` | -- | Yes | Content hash of the output data |
| `schema_keys` | -- | Yes | Schema location (JSON, e.g., `{"subject": "S01"}`) |
| `lineage_hash` | -- | Yes | Hash of the full computation (for cache lookups) |
| `constants` | -- | Yes | Constant values used |

Input descriptors for saved variables include `record_id`, `content_hash`,
`metadata`, and `type`:

## Querying Provenance

### What Produced This Variable?

```python
provenance = db.get_provenance(NormalizedData, subject=1, stage="normalized")

print(provenance["function_name"])   # "normalize"
print(provenance["function_hash"])   # SHA-256 of function bytecode
print(provenance["inputs"])          # List of input descriptors
print(provenance["constants"])       # List of constant values
```

### What Happened at a Schema Location? (Schema-aware)

```python
# All computations for subject S01
records = db.get_provenance_by_schema(subject="S01")

# Narrower: specific subject and session
records = db.get_provenance_by_schema(subject="S01", session="1")

for r in records:
    print(f"{r['function_name']} -> {r['output_type']}")
    print(f"  content_hash: {r['output_content_hash']}")
    for inp in r["inputs"]:
        if "content_hash" in inp:
            print(f"  input {inp['type']}: content_hash={inp['content_hash']}")
```

### What Does the Pipeline Look Like? (Schema-blind)

```python
structure = db.get_pipeline_structure()

for step in structure:
    print(f"{step['input_types']} --[{step['function_name']}]--> {step['output_type']}")
```

### Check Lineage Exists

```python
if db.has_lineage(record_id):
    print("This variable was produced by a thunked function")
```

## Chained Pipelines

Lineage tracks through multiple processing steps:

```python
@thunk
def step1(data):
    return data * 2

@thunk
def step2(data):
    return data + 1

@thunk
def step3(data):
    return data ** 2

# Chain of operations
result = step3(step2(step1(raw_data)))

# Lineage captures full chain
MyVar.save(result, subject=1, stage="final")
```

## Manual Lineage Extraction

For inspection without saving:

```python
from scidb.lineage import extract_lineage, get_raw_value

result = process(data)

# Extract lineage record
lineage = extract_lineage(result)
print(lineage.function_name)
print(lineage.inputs)

# Get raw value
raw_value = get_raw_value(result)
```

## Function Hashing

Functions are identified by a SHA-256 hash of:

- Bytecode (`__code__.co_code`)
- Constants (`__code__.co_consts`)

This means:

- Same function logic = same hash (reproducible)
- Different constants = different hash (e.g., `x * 2` vs `x * 3`)
- Renamed variables don't change the hash

## Wrapping External Functions

One of the main goals of scientific workflows is leveraging existing libraries. You can wrap any external function as a `Thunk` to get lineage tracking:

```python
from scidb import Thunk

# Wrap functions from any package
from scipy.signal import butter, filtfilt, welch
from sklearn.preprocessing import StandardScaler

# unpack_output=True for functions that return tuples you want to destructure
thunked_butter = Thunk(butter, unpack_output=True)   # Returns (b, a)
thunked_filtfilt = Thunk(filtfilt)
thunked_welch = Thunk(welch, unpack_output=True)     # Returns (freqs, psd)
```

### Example: Signal Processing Pipeline

```python
from scipy.signal import butter, filtfilt, welch
from scidb import Thunk, BaseVariable, configure_database
import numpy as np

# Wrap scipy functions (unpack_output=True for tuple returns)
thunked_butter = Thunk(butter, unpack_output=True)
thunked_filtfilt = Thunk(filtfilt)
thunked_welch = Thunk(welch, unpack_output=True)

# Define variable types (native storage)
class SignalData(BaseVariable):
    pass

class PSDData(BaseVariable):
    pass

# Setup
db = configure_database("experiment.duckdb", ["subject", "session"], "pipeline.db")

# Run pipeline with full lineage tracking
raw_signal = SignalData.load(subject=1, session="baseline")

b, a = thunked_butter(N=4, Wn=[1, 40], btype='band', fs=1000)
filtered = thunked_filtfilt(b, a, raw_signal)
freqs, psd = thunked_welch(filtered, fs=1000)

# Save with lineage
SignalData.save(filtered, subject=1, session="filtered")
PSDData.save((freqs, psd), subject=1, session="psd")
```

### Example: Machine Learning Pipeline

```python
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from scidb import Thunk

# Wrap sklearn - note: wrap the methods, not the class
scaler = StandardScaler()
pca = PCA(n_components=10)

thunked_fit_transform = Thunk(scaler.fit_transform)
thunked_pca_fit_transform = Thunk(pca.fit_transform)

# Pipeline with lineage
scaled = thunked_fit_transform(raw_features)
reduced = thunked_pca_fit_transform(scaled)

ReducedFeatures.save(reduced, subject=1, stage="pca")
```

### Creating a Thunk Library

For frequently used external functions, create a module of pre-wrapped thunks:

```python
# my_project/thunks.py
from scidb import Thunk
from scipy.signal import butter, filtfilt, welch, hilbert
from scipy.fft import fft, ifft

# Signal processing
thunk_butter = Thunk(butter)
thunk_filtfilt = Thunk(filtfilt)
thunk_welch = Thunk(welch)
thunk_hilbert = Thunk(hilbert)
thunk_fft = Thunk(fft)
thunk_ifft = Thunk(ifft)
```

```python
# In your pipeline
from my_project.thunks import thunk_filtfilt, thunk_welch

filtered = thunk_filtfilt(b, a, data)
freqs, psd = thunk_welch(filtered)
```

## Cross-Script Lineage

When pipelines are split across separate files/scripts, lineage is still tracked by passing the loaded variable to the thunk:

```python
# step1.py
@thunk
def preprocess(data):
    return data * 2

result = preprocess(raw_data)
Intermediate.save(result, subject=1, stage="preprocessed")
```

```python
# step2.py (separate execution)
loaded = Intermediate.load(subject=1, stage="preprocessed")

@thunk
def analyze(data):
    # Receives the raw numpy array (unwrapped from loaded)
    return data.mean()

# Pass the loaded variable - lineage links to loaded.record_id
result = analyze(loaded)
FinalResult.save(result, subject=1, stage="analyzed")

# Lineage correctly shows: FinalResult <- analyze <- Intermediate
```

The key: pass the `BaseVariable` instance, not `loaded.data`. The thunk automatically unwraps it.

## Debugging with `unwrap=False`

By default, thunks unwrap `BaseVariable` and `ThunkOutput` inputs to their raw data. Use `unwrap=False` to receive the wrapper objects for debugging:

```python
@thunk(unwrap=False)
def debug_process(var):
    # var is the BaseVariable, not raw data
    print(f"Input record_id: {var.record_id}")
    print(f"Input metadata: {var.metadata}")
    print(f"Data shape: {var.data.shape}")
    return var.data * 2

# Lineage still captured, but function can inspect metadata
result = debug_process(loaded)
```

This is useful for:

- Tracing data provenance during debugging
- Logging metadata alongside processing
- Building introspection tools

## Limitations

### Functions in Loops

Don't thunk functions called in loops that accumulate results:

```python
# DON'T do this
@thunk
def process_item(item):
    return item * 2

results = []
for item in items:
    results.append(process_item(item))  # Returns ThunkOutput
pd.concat(results)  # Error: can't concat ThunkOutputs

# DO this instead
def process_item(item):  # No @thunk
    return item * 2

@thunk
def process_all(items):
    return [process_item(item) for item in items]
```
