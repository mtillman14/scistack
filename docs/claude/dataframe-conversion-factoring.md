# DataFrame Conversion: Factoring Out as a Standalone Library

## Question
Would it be beneficial to factor out the data type conversion to/from `pd.DataFrame` into a standalone library?

## Where Conversion Code Lives

### `sciduck/src/sciduckdb/sciduckdb.py` (primary location)
All the substantive conversion logic lives here as module-level functions:
- `_infer_duckdb_type()` — maps Python/numpy types to DuckDB column type strings (`DOUBLE[]`, `JSON`, `LIST`, etc.)
- `_python_to_storage()` / `storage_to_python()` — serialize/deserialize individual cell values
- `infer_data_columns()` — infers column schema from a sample value; handles DataFrame "mode" (one DuckDB row per DataFrame row), dict mode (multi-column), and single-column mode
- `dataframe_to_storage_rows()` — converts all rows of a DataFrame to storage-ready lists
- `_convert_for_json()` — recursively converts ndarrays/DataFrames to JSON-serializable form
- `_restore_types()` — applies `dtype_meta` restoration to a loaded DuckDB result DataFrame

These all share a common `dtype_meta` dict format that encodes round-trip type information (e.g., `{"python_type": "ndarray", "numpy_dtype": "float64", "ndim": 2}`).

### `src/scidb/variable.py` (user-facing hook)
The `to_db()` / `from_db()` interface is the clean seam for user-defined serialization. The default implementation is trivial (~10 lines total). Users override these for custom multi-column storage.

### Other packages (independent concerns)
- `canonical-hash/` — hashes DataFrames by column/index/array structure for lineage tracking; completely unrelated to storage type inference
- `scidb-net/` — serializes DataFrames over the wire using Arrow IPC; again unrelated

## Verdict: Low Benefit

**The conversions are DuckDB-specific, not generic.** The functions produce DuckDB column type strings and a `dtype_meta` format that is an internal implementation detail of SciDuck's storage layer. There is no natural consumer of this logic outside a DuckDB context.

**The boundary already exists.** `sciduck` is already a standalone installable package with its own tests. The conversion logic is already isolated behind that package boundary. Further extraction would add a package without adding reusability.

**Other DataFrame-touching code wouldn't benefit.** Hashing and network serialization have completely different requirements and would remain independent regardless.

**The `to_db()`/`from_db()` interface is already the right abstraction.** It's small, clean, and user-overrideable.

## When It Would Make Sense

If a second storage backend were ever added (Parquet, SQLite, HDF5), a shared "Python object ↔ columnar representation" library could be worth extracting — the type inference logic could then be reused across backends. With only DuckDB today, that remains speculative.
