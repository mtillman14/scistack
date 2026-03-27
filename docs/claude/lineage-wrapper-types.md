# Lineage Wrapper Types and Cross-Layer Data Flow

## The Big Picture

Every layer of the stack wraps your raw data (a numpy array, scalar, etc.)
and adds extra information around it. Understanding the two wrapper types —
`LineageFcnResult` and `BaseVariable` — and how they relate is key to
understanding the full stack.

---

## Wrapper 1: `LineageFcnResult` (scilineage)

When you call a `@lineage_fcn`-decorated function, it runs normally but
instead of returning raw data it returns a `LineageFcnResult`:

```
                    ┌─────────────────────────────────────┐
                    │       LineageFcnResult               │  (scilineage)
                    │                                     │
                    │  .data     → numpy_array            │  ← your actual result
                    │  .invoked  → LineageFcnInvocation   │  ← who called what
                    │  .hash     → "a3f9..."              │  ← lineage fingerprint
                    └─────────────────────────────────────┘
```

```python
from their_package import bandpass_filter   # decorated with @lineage_fcn

result = bandpass_filter(signal, low_hz=20)

type(result)       # LineageFcnResult  (scilineage)
type(result.data)  # numpy.ndarray     ← the actual filtered signal
```

You must reach inside via `.data` to get the raw value.

---

## Wrapper 2: `BaseVariable` (scidb)

Separately and independently, `scidb` has its own wrapper for the purpose
of saving and loading data from DuckDB. User-defined variable classes
(e.g. `FilteredEMG`) subclass `BaseVariable`:

```
                    ┌─────────────────────────────────────┐
                    │       FilteredEMG                   │  (user's code, scidb)
                    │       (subclass of BaseVariable)    │
                    │                                     │
                    │  .data      → numpy_array           │  ← your actual result
                    │  .metadata  → {subject:1,           │  ← where it lives in DB
                    │                session:"pre"}       │
                    │  .record_id → "b7c2..."             │  ← unique DB row ID
                    └─────────────────────────────────────┘
```

```python
var = FilteredEMG.load(subject=1, session="pre")

type(var)       # FilteredEMG  (scidb)
type(var.data)  # numpy.ndarray ← the actual filtered signal
```

---

## Key Point: These Two Types Are Completely Independent

Both `LineageFcnResult` and `BaseVariable` wrap raw data with a `.data`
attribute. They have **no inheritance relationship** and come from packages
that know nothing about each other:

```
    scilineage                        scidb
    ──────────                        ──────
    LineageFcnResult                  BaseVariable
      .data → raw_data                  .data → raw_data
      .hash → "a3f9..."                 .record_id → "b7c2..."
      .invoked → ...                    .metadata → {...}
```

`scilineage` does not import `scidb`. `scidb` does not import `scilineage`.

---

## Layer 3: scihist Bridges the Two

`scihist` is the only layer that knows about both. Its `for_each` function
handles the full pipeline automatically:

```
Step 1: Load from DB
────────────────────
  scidb loads RawEMG(subject=1, session="pre") from DuckDB
  → RawEMG instance (BaseVariable, scidb)
        .data = numpy_array   ← scidb unwraps this before passing to your function


Step 2: Call your function
──────────────────────────
  bandpass_filter(numpy_array, low_hz=20)
  ↑ function is wrapped in LineageFcn (scilineage) by scihist
  → LineageFcnResult (scilineage)
        .data = numpy_array   ← the filtered result
        .hash = "a3f9..."


Step 3: Save to DB (the scihist bridge)
────────────────────────────────────────
  scihist unwraps LineageFcnResult  →  gets numpy_array
  scihist creates FilteredEMG(numpy_array)  →  BaseVariable (scidb)
  scidb saves it, with the lineage dict attached
```

Full data journey:

```
DuckDB            scidb           your fn         scilineage        scidb           DuckDB
  │                 │                │                 │               │               │
  │──RawEMG row────►│                │                 │               │               │
  │             .data (numpy)        │                 │               │               │
  │                 │──numpy_array──►│                 │               │               │
  │                 │                │──LineageFcnResult──────────────►│               │
  │                 │                │                 │           .data (numpy)        │
  │                 │                │                 │               │               │
  │                 │                │                 │           FilteredEMG          │
  │                 │                │                 │               │──save+lineage─►│
```

The translation in `scihist/foreach.py::_save_lineage_fcn_result`:

```python
raw_data = get_raw_value(data)           # unwrap LineageFcnResult  (scilineage)
instance = variable_class(raw_data)      # wrap in BaseVariable     (scidb)
active_db.save(instance, metadata, ...)  # save with lineage dict
```

---

## What Happens with scifor Only

If you use only `scifor` (no DB, no lineage) and call an external function
decorated with `@lineage_fcn`:

```python
from their_package import bandpass_filter   # @lineage_fcn from scilineage

result = bandpass_filter(signal, 20)

# result      → LineageFcnResult   (scilineage)
# result.data → numpy_array        ← what you actually want
# No BaseVariable involved at all — scidb is not in the picture
```

`scifor`'s `for_each` will collect `LineageFcnResult` objects instead of
raw values, which will break any downstream code expecting a numpy array.

**Workaround** — unwrap manually before passing to `scifor`:

```python
def unwrapped_filter(signal, low_hz):
    return bandpass_filter(signal, low_hz).data   # reach in, return numpy

scifor.for_each(unwrapped_filter, ...)
```

---

## Summary Table

| Type | Module | `.data` contains | Purpose |
|---|---|---|---|
| `LineageFcnResult` | `scilineage` | raw data (numpy, scalar, …) | Carry provenance/lineage |
| `BaseVariable` subclass | `scidb` (user-defined) | raw data (numpy, scalar, …) | DB storage + addressing |
| Combined handling | `scihist` | — | Bridge between the two |
