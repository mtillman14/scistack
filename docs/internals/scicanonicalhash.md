# scicanonicalhash — deterministic hashing

/// info | Internal package
You don't call `scicanonicalhash` directly. `scidb` uses it to version saved
data and to decide whether a function has changed since it last ran.
///

`scicanonicalhash` produces stable, deterministic hashes of arbitrary Python
objects and of Python functions. These hashes are the basis of SciStack's
versioning and reproducibility: "has this changed?" decisions throughout the
stack reduce to a hash comparison.

## Content hashing

- `canonical_hash(value)` returns a 16-character hex string (the first 64 bits
  of SHA-256). It is *structural*: lists hash in order, dicts hash independent
  of key order, and numpy arrays hash by content **plus dtype and shape**.
- `generate_record_id(...)` derives the content-addressed IDs that `scidb` uses
  to version saved variables.

## Function hashing

`compute_function_hash(fn)` is a short, stable fingerprint of a function's
code. `scidb` stores it with every run. It is how `skip_computed=True` notices
that you edited a function and need to re-run it, and how the GUI marks a step
as out of date.

| Name | Purpose |
|---|---|
| `compute_function_hash(fn, *, truncate=16, recursive=True)` | The hash of `fn`. |
| `compute_function_hash_with_sources(fn, *, truncate=16)` → `(hash, {qualname: source})` | The same hash, plus the source text behind it. Use this whenever you store source alongside a hash, so that the two always agree. |

- **Changes it:** any edit to the function's logic, *and* any edit to a
  function it calls that is defined in your own code (helpers are hashed
  recursively).
- **Doesn't change it:** whitespace, formatting, comments and docstrings,
  which are removed before hashing. Calls into the standard library or
  installed packages are treated as opaque, so upgrading numpy does not
  invalidate your results.

MATLAB functions are hashed from their source file by
[`scimatlab`](scimatlab.md). Python and MATLAB hashes are therefore never
equal, even for equivalent code.

For how these surface, see [Versioning & Content Hashing](../concepts/hashing.md).
