# Tracking Lineage

<!-- Ground truth (tests/source win over prose). Verified against:
     scilineage/src/scilineage/core.py: lineage_fcn(func=None, *, unpack_output=False,
       unwrap=True, generates_file=False) — usable as decorator OR direct wrapper
       lineage_fcn(fn) / lineage_fcn(fn, unpack_output=True);
     scilineage/tests/test_core.py, test_lineage.py (LineageFcnResult.data; extract_lineage ->
       function_name/function_hash[64 SHA-256]/inputs/constants; get_upstream_lineage; manual());
     scidb/src/scidb/variable.py:271 (BaseVariable.save does NOT record lineage for a
       LineageFcnResult — "use scihist.for_each or scihist save helpers");
     scihist/src/scihist/foreach.py:753 save(variable_class, data, db=None, **metadata)
       (records lineage); scihist.configure_database wires the cache backend;
     scidb/src/scidb/database.py: get_provenance(...) -> {function_name, function_hash, inputs,
       constants}|None; get_provenance_by_schema(**schema_keys) -> dicts with output_record_id/
       output_type/function_name/function_hash/inputs/constants; get_pipeline_structure() ->
       function_name/function_hash/output_type/input_types; has_lineage(record_id).
     NOTE: `Thunk` is NOT a real export; lineage lives in the same DuckDB (_lineage table),
     not a separate SQLite "pipeline.db"; extract_lineage/get_upstream_lineage are in scilineage. -->

This guide shows how to add provenance tracking to a pipeline and query it. For
the model behind it (how lineage forms a cache key, how it survives save/reload),
see [Lineage & Provenance](../concepts/lineage.md). Lineage is a `scihist`
feature — it re-exports the `lineage_fcn` decorator, so you import it from
`scihist`.

## Add tracking with `@lineage_fcn`

Decorate a processing function. Each call returns a `LineageFcnResult` carrying
provenance; the value is on `.data`:

```python
from scihist import lineage_fcn

@lineage_fcn
def process_signal(signal, factor):
    return signal * factor

result = process_signal(data, 2.5)
result.data   # the computed array
```

Multi-output functions opt in with `unpack_output=True`:

```python
@lineage_fcn(unpack_output=True)
def split_data(data):
    mid = len(data) // 2
    return data[:mid], data[mid:]

first, second = split_data(data)   # each is its own tracked result
```

## Chain steps and pass variables

Results flow into other tracked functions, building the provenance graph. When an
input is a stored variable, **pass the `BaseVariable` instance, not `.data`** —
the decorator unwraps it for you (default `unwrap=True`) while keeping the link to
its `record_id`:

```python
raw = RawData.load(subject=1, session="A")   # pass the variable…
normalized = normalize(raw)                  # …not raw.data
result = analyze(normalized)                 # lineage spans both steps
```

For debugging, `@lineage_fcn(unwrap=False)` hands the wrapper objects to your
function so it can read `.record_id` / `.metadata` while still recording lineage.

## Persist results with lineage

This is the key practical point: a plain `VariableClass.save(...)` stores the
*data* but **not** the lineage. To persist a tracked result *with* its provenance,
use `scihist.save` (or run the whole thing through
[`for_each`](for_each.md)):

```python
from scihist import save

norm = normalize(RawData.load(subject=1, session="A"))
save(NormalizedData, norm, subject=1, session="A")   # records lineage in _lineage
```

Because lineage is preserved across the database boundary, a pipeline split across
scripts still links up — save a result in one script, reload it in another, and
feeding it onward records the correct ancestry (and hits the
[cache](caching.md)):

```python
# later / elsewhere
loaded = NormalizedData.load(subject=1, session="A")
save(FinalResult, analyze(loaded), subject=1, session="A")
# provenance: FinalResult <- analyze <- NormalizedData
```

## Query provenance

Once results are saved with lineage, query the database handle:

```python
# What produced one record?
prov = db.get_provenance(NormalizedData, subject=1, session="A")
prov["function_name"], prov["inputs"], prov["constants"]   # or None if no lineage

# Everything computed at a schema location (schema-aware view)
for r in db.get_provenance_by_schema(subject=1):
    print(r["function_name"], "->", r["output_type"], r["output_record_id"][:12])

# The abstract pipeline shape, ignoring specific data (schema-blind view)
for step in db.get_pipeline_structure():
    print(step["input_types"], "--[", step["function_name"], "]-->", step["output_type"])

db.has_lineage(record_id)   # True if this record was produced with lineage
```

## Inspect lineage without saving

To examine provenance in memory, use the lineage engine's extractors (see
[Internals — scicanonicalhash](../internals/scicanonicalhash.md)):

```python
from scilineage import extract_lineage, get_upstream_lineage

rec = extract_lineage(result)
rec.function_name      # "analyze"
rec.function_hash      # 64-char SHA-256 of the function's bytecode
rec.inputs, rec.constants

for r in get_upstream_lineage(result, max_depth=10):   # walk the ancestry
    print(r["function_name"])
```

## Wrap existing library functions

You don't have to own a function to track it — wrap any callable by *calling*
`lineage_fcn` on it (with `unpack_output=True` for tuple returns):

```python
from scihist import lineage_fcn
from scipy.signal import butter, filtfilt

butter_l   = lineage_fcn(butter, unpack_output=True)   # returns (b, a)
filtfilt_l = lineage_fcn(filtfilt)

b, a     = butter_l(4, [1, 40], btype="band", fs=1000)
filtered = filtfilt_l(b, a, raw)                       # fully tracked
```

For frequently used externals, collect the wrapped versions in a small module and
import them where needed.

## Document manual interventions

When you must edit data outside the pipeline, re-enter with `manual()` so the
correction is recorded rather than hidden:

```python
from scidb import manual

clean = manual(edited_data, label="outlier_removal",
               reason="amplitude < 0.1 in trial 3 is a sensor artifact")
```

The result is a normal tracked result whose function name is `"manual"`, with the
`label` and `reason` captured in its lineage.

## Gotcha: don't decorate accumulation-loop helpers

A `@lineage_fcn` returns a `LineageFcnResult`, not a raw value — so don't decorate
a helper you call in a loop and then combine the results directly:

```python
# Don't: each call returns a wrapper; concatenating them fails
@lineage_fcn
def process_item(item): ...

# Do: leave the helper plain and decorate the function that returns the final value
def process_item(item): ...

@lineage_fcn
def process_all(items):
    return [process_item(i) for i in items]
```

**Next:** [Computation Caching](caching.md) ·
[Batch Processing (for_each)](for_each.md) ·
[Database & Configuration](database.md) · [API: Lineage](../api/lineage.md)
