# Internals

SciStack's two user-facing layers — [`scifor`](../getting-started/choosing-a-layer.md)
and `scidb` — are built on a set of **internal packages**. You normally never
import these directly; you reach their capabilities through the user-facing
layers (for example, the database lives behind `scidb`).

This section exists for contributors and the curious: it explains *what each
internal package is for* and *which user-facing layer surfaces it*, without
requiring any of it to use SciStack.

| Internal package | Provides | Reached through |
|---|---|---|
| [sciduckdb](sciduckdb.md) | DuckDB persistence (one table per variable) | `scidb` |
| [scicanonicalhash](scicanonicalhash.md) | Deterministic content and function hashing | `scidb` |
| [scimatlab](scimatlab.md) | MATLAB bridge for `scifor.*` / `scidb.*` | MATLAB usage |

For how the user-facing layers compose, see
[Architecture & Layers](../concepts/architecture.md).
