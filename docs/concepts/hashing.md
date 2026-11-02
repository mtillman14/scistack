# Versioning & Content Hashing

<!-- Ground truth (tests/source win over prose). Verified against:
     scicanonicalhash/src/scicanonicalhash/hashing.py (canonical_hash: json-serialize ->
       sha256 hexdigest()[:16] = 16-char hex; numpy = shape+dtype+raw bytes; dicts key-order
       independent; generate_record_id(class_name, schema_version, content_hash, metadata)
       -> sha256[:16], deterministic, any field change -> new id);
     scicanonicalhash/tests/test_hashing.py (16-char format; list order matters; tuple vs
       list type matters; numpy shape/content; DataFrame by content; generate_record_id
       5-fields-all-distinguish);
     scilineage/src/scilineage/hashing.py (compute_function_hash bytecode-based, recursive
       default, truncate=16 default; _hash_bytecode_only uses __code__.co_code + co_consts);
     scilineage/tests/test_hashing.py:177 (whitespace/comments/docstrings don't change hash),
       :375-402 (recursive vs bytecode-only path);
     scilineage/tests/test_lineage.py (lineage record function_hash is full 64-char SHA-256);
     scidb/src/scidb/store/database.py:1147,2015 (canonical_hash(data) -> content_hash;
       generate_record_id -> record_id; stored in _record_metadata).
     scicanonicalhash depends on nothing; it is the leaf every other layer builds on. -->

!!! note "Under-the-hood concept"
    This page explains a mechanism SciStack uses *for* you. You normally never
    call the hashing functions directly — versioning (`scidb`) and lineage
    (`scihist`) invoke them automatically. The implementation lives in the
    internal [scicanonicalhash](../internals/scicanonicalhash.md) package; the
    code below is illustrative.

Hashing is the foundation the rest of SciStack stands on. A **hash** is a short,
deterministic fingerprint of some data or code: the same input always produces
the same fingerprint, and different inputs produce different ones. That turns
expensive questions — *"is this the same value? the same function? the same
computation?"* — into a cheap string comparison, which is what makes
[storage identity](variables.md), [lineage](lineage.md), and
[caching](caching.md) possible.

## Content hashing — `canonical_hash`

`canonical_hash(value)` fingerprints a *value*. It returns a **16-character
lowercase hex string** and is fully deterministic:

```python
from scicanonicalhash import canonical_hash

canonical_hash(42)          # same every time, 16 hex chars
canonical_hash([1, 2, 3])   # == canonical_hash([1, 2, 3])
```

What it hashes is **structural**, with rules chosen so that "meaningfully equal"
values hash equal and meaningfully different ones don't:

- **Primitives** (`int`, `float`, `str`, `bool`, `None`) hash by value.
- **Lists** hash in **order** — `[1, 2, 3]` ≠ `[3, 2, 1]`.
- **Type matters** — a tuple `(1, 2)` ≠ the list `[1, 2]`.
- **Dicts** hash **independent of key order** — `{"a": 1, "b": 2}` ==
  `{"b": 2, "a": 1}` — because what a mapping *means* doesn't depend on insertion
  order.
- **Nested** structures hash by recursing through the above.
- **numpy arrays** hash by **content + dtype + shape**: the same numbers as
  `int32` vs `int64`, or flat vs reshaped, hash differently.
- **pandas DataFrames** hash by their content.

This is the single definition of "same value" used across the whole stack.

## Record identity — `generate_record_id`

A stored value's identity is more than its content: two different variables with
the same numbers, or the same variable at two different subjects, must be
distinct records. `generate_record_id` combines the content hash with the
addressing facts:

```python
from scicanonicalhash import generate_record_id

record_id = generate_record_id(
    class_name="StepLength",   # which variable type
    schema_version=1,          # which structural version of it
    content_hash="…",          # canonical_hash of the data
    metadata={"subject": 1},   # where it lives
)
```

It too returns a deterministic 16-character hex id, and **any** of the four
inputs changing yields a different id. This is exactly the content-addressed
`record_id` behind [Variables & Storage](variables.md): re-saving identical data
at the same coordinates reproduces the same id (so it dedups), while different
data, a different type, a bumped `schema_version`, or different metadata creates a
new one.

`schema_version` is the deliberate lever here — bumping it changes the id space,
so old and new structural layouts of a variable never collide.

## Function hashing — `compute_function_hash`

To know whether a *computation* is the same, you also need to fingerprint the
**function**. Function hashing is **bytecode-based**: it hashes the function's
compiled code, not its source text (it is part of the lineage engine — see
[Internals — scicanonicalhash](../internals/scicanonicalhash.md)). Two consequences:

- **Reformatting is invisible.** Changing whitespace, comments, or the docstring
  does **not** change the hash — only changing what the function actually computes
  does.
- **It is recursive by default.** The hash also reflects the functions your
  function calls, so editing a helper changes the hash of everything that depends
  on it.

The fingerprint is a SHA-256 digest. It is stored at full width
(64 hex characters) in the lineage record, and truncated to 16 characters where
`scidb` records it as `__fn_hash` in a variable's version keys — the same hash,
sized for its use.

## How the hashes compose

The three hashes stack into two identities the rest of the system relies on:

```
canonical_hash(data) ─┐
class_name           ─┤
schema_version       ─┼─► generate_record_id ─► record_id   (identity of a stored value)
metadata             ─┘

compute_function_hash(fn) ─┐
input record_ids          ─┼─► lineage hash   (identity of a computation)
constant variant keys     ─┘
```

- The **record id** answers "is this the same stored value?" — the basis of
  versioning and deduplication.
- The **lineage hash** answers "is this the same computation?" — the basis of
  [caching](caching.md) and [provenance](lineage.md).

## Why determinism matters

Because every fingerprint depends only on its inputs — never on time, machine, or
run order — the same data and the same code produce the same identities
everywhere. That is what lets a result computed today be recognized and reused
tomorrow, or a pipeline split across runs and machines to line up exactly. The
hashes *are* the contract that makes reproducibility and caching trustworthy.

**Next:** [Variables & Storage](variables.md) · [Lineage & Provenance](lineage.md)
· [Computation Caching](caching.md) · [Glossary](glossary.md)
