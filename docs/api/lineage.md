# Lineage API

<!-- Ground truth (source/tests win over prose). Verified against:
     scilineage/src/scilineage/core.py: lineage_fcn(func=None, *, unpack_output=False,
       unwrap=True, generates_file=False) -> LineageFcn (usable @decorator or lineage_fcn(fn));
       LineageFcnResult(.data, .invoked, .hash, .output_num); LineageFcnInvocation(.fcn, .inputs,
       .outputs, .compute_lineage_hash()); LineageFcn(.fcn, .hash, .generates_file, .unwrap);
     scilineage/src/scilineage/lineage.py: LineageRecord(function_name, function_hash, inputs,
       constants, to_dict/from_dict); extract_lineage(result)->LineageRecord;
       get_upstream_lineage(result, max_depth=...) -> list[dict];
     scilineage/__init__: manual, configure_backend, canonical_hash, compute_function_hash;
     scihist/src/scihist/foreach.py:753 save(variable_class, data, db=None, **metadata)->str|None
       (records lineage for a LineageFcnResult, else delegates to variable_class.save).
     NOTE: there is NO `Thunk` / `ThunkOutput` export — decorator is @lineage_fcn, result is
     LineageFcnResult; lineage is persisted via scihist.save (plain VarClass.save does NOT). -->

The lineage system records what produced each value and serves as the cache key.
For task usage see [Tracking Lineage](../guide/lineage.md); for the model see
[Lineage & Provenance](../concepts/lineage.md). It is Python-only.

!!! note "Reached through `scihist`"
    The `lineage_fcn` decorator and the `LineageFcn*` types are **re-exported by
    `scihist`** — import them as `from scihist import lineage_fcn`. The
    lower-level extractors (`extract_lineage`, `get_upstream_lineage`) live in the
    internal engine, [scicanonicalhash](../internals/scicanonicalhash.md); import those
    directly only when you need them.

---

## `@lineage_fcn`

```python
lineage_fcn(func=None, *, unpack_output=False, unwrap=True, generates_file=False) -> LineageFcn
```

Wraps a function so each call returns a `LineageFcnResult` carrying provenance.
Usable as a decorator (`@lineage_fcn`) or applied directly to wrap an existing
function (`lineage_fcn(fn)`).

- **`unpack_output`** — if `True`, a tuple return is unpacked into separate tracked
  results (each with its own `.output_num`).
- **`unwrap`** — if `True` (default), `BaseVariable` / `LineageFcnResult` inputs are
  unwrapped to their raw data before the call (lineage link preserved). Set `False`
  to receive the wrapper objects.
- **`generates_file`** — mark a side-effect function that writes a file rather than
  returning data; enables cache-hit skipping. Does not affect the function hash.

```python
from scihist import lineage_fcn

@lineage_fcn
def process(signal, factor):
    return signal * factor

butter_l = lineage_fcn(butter, unpack_output=True)   # wrap an external function
```

---

## `LineageFcnResult`

The object returned by a tracked call.

| Member | Meaning |
|---|---|
| `.data` | the computed value |
| `.invoked` | the `LineageFcnInvocation` that produced it |
| `.hash` | deterministic id of this computation (cache key) |
| `.output_num` | output index for multi-output functions |

Compares equal to its raw value and `str()`s as it, but read `.data` to use the
value.

---

## `LineageFcnInvocation`

A specific call with captured inputs.

| Member | Meaning |
|---|---|
| `.fcn` | the parent `LineageFcn` |
| `.inputs` | captured inputs (positional args bound to parameter names) |
| `.outputs` | the produced `LineageFcnResult`s |
| `.compute_lineage_hash()` | the lineage/cache hash |

---

## `LineageFcn`

The wrapped function itself.

| Member | Meaning |
|---|---|
| `.fcn` | the original function |
| `.hash` | bytecode-based function hash (see [Hashing](../concepts/hashing.md)) |
| `.generates_file` | the side-effect flag |
| `.unwrap` | the unwrap setting |

---

## Persisting with lineage — `scihist.save()`

```python
scihist.save(variable_class, data, db=None, **metadata) -> str | None
```

If `data` is a `LineageFcnResult`, extracts and stores its lineage; otherwise
delegates to `variable_class.save(...)`. A plain `VariableClass.save()` does **not**
record lineage — use this (or [`for_each`](for-each.md)) to persist provenance.

```python
from scihist import save
save(NormalizedData, normalize(raw), subject=1, session="A")
```

---

## Inspecting lineage

```python
extract_lineage(result) -> LineageRecord
get_upstream_lineage(result, max_depth=...) -> list[dict]
```

- **`extract_lineage`** — a `LineageRecord` with `.function_name`,
  `.function_hash` (64-char SHA-256), `.inputs` (list of dicts), `.constants`
  (list of dicts), plus `.to_dict()` / `.from_dict()`.
- **`get_upstream_lineage`** — the ancestry as a list of dicts, walked up to
  `max_depth`.

```python
from scilineage import extract_lineage, get_upstream_lineage
rec = extract_lineage(result)
chain = get_upstream_lineage(result, max_depth=10)
```

---

## `manual()`

```python
manual(data, label=..., reason=...) -> LineageFcnResult
```

Re-enter the pipeline after an out-of-band edit, recording it as a `"manual"`
lineage step with the given `label` and `reason`.

---

## Hashing utilities

```python
canonical_hash(value) -> str            # 16-char content hash
compute_function_hash(fn) -> str        # bytecode-based function hash
configure_backend(db) -> None           # register a cache backend (scihist does this)
```

See [Versioning & Content Hashing](../concepts/hashing.md).

**See also:** [Variables](variables.md) · [Database](database.md) ·
[Batch Processing](for-each.md)
