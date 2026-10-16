# scilineage

/// info | Internal package
You don't call `scilineage` directly. `scidb.for_each` uses it to decide whether a function has changed since it last ran.
///

`scilineage` computes a **function hash**: a short, stable fingerprint of a function's code. `scidb` stores this hash with every run. It is how `skip_computed=True` notices that you edited a function and need to re-run it, and how the GUI marks a step as out of date.

/// note | Changed
Earlier versions of `scilineage` provided a `@lineage_fcn` decorator and a `LineageFcnResult` wrapper. Both were removed. Provenance is now recorded by `scidb.for_each` directly, and the optional `@scistack` marker lives in [`scidb`](../api/scidb.md#batch-processing).
///

## API

| Name | Purpose |
|---|---|
| `compute_function_hash(fn, *, truncate=16, recursive=True)` | The hash of `fn`. |
| `compute_function_hash_with_sources(fn, *, truncate=16)` → `(hash, {qualname: source})` | The same hash, plus the source text behind it. Use this whenever you store source alongside a hash, so that the two always agree. |
| `canonical_hash(value)` | A stable content hash of any value (arrays, DataFrames, dicts, ...). Used for constants and data. |

## What changes the hash

- **Changes it:** any edit to the function's logic, *and* any edit to a function it calls that is defined in your own code (helpers are hashed recursively).
- **Doesn't change it:** whitespace, formatting, comments and docstrings, which are removed before hashing. Calls into the standard library or installed packages are treated as opaque, so upgrading numpy does not invalidate your results.

MATLAB functions are hashed from their source file by [`scimatlab`](scimatlab.md). Python and MATLAB hashes are therefore never equal, even for equivalent code.
