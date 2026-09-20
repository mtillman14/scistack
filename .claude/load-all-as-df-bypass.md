# Plan: shared `db.load_all_as_df()` engine — bulk DataFrame loading

## Context

After the prior `bulk-load-all-optimization-plan.md` round, `db.load_all` does chunked SQL fetches. But the timing-instrumentation run on a 7,000-record DummyMixed fixture surfaced the remaining hot spot:

```
[timing] load_all(DummyMixed): pre-yield setup:
    chunks_total=7.102s (sql=0.265s, deserialize=6.836s, n_chunks=14, mode=dataframe)
[timing] _load_var_type_all(DummyMixed):
    load_all+materialize = 7.410s for 7000 records
    assembly = 0.363s
```

The 6.836s deserialize comes from 7,000 mini `pd.DataFrame(...)` constructions inside a `groupby`, 378,000 individual `_storage_to_python` calls, and 7,000 throwaway BaseVariable instances. Most of that exists because `db.load_all` returns *instances* and `_load_var_type_all` immediately re-extracts their fields to assemble a wide DataFrame.

## Wider architectural finding

While planning the bypass we noticed:

1. **Two engines exist for one job.** Both `BaseVariable.load_all(as_df=True)` and `_load_var_type_all` independently call `db.load_all()` → list-materialize → loop-and-assemble. Same expensive work, two copies.
2. **`_load_var_type_all` lives in `foreach.py` only because that's where the first caller was.** Its body is "fetch all records of a type and assemble a wide DataFrame" — a database operation, not a foreach-specific one. The module location is incidental.
3. **The output shape difference (packed vs spread for dataframe mode) IS real** — packed serves notebook-style record browsing; spread serves pipeline-style flat-row processing — but it should be a *parameter* on one engine, not the reason for two parallel implementations.
4. **`BaseVariable.load_all(as_df=False)` (iterator) must stay** — its contract is "yield instances," which is the user-facing API. The fast path doesn't touch it.

So the right scope is broader than the original bypass: extract a single shared engine, route both consumers through it, delete the duplicated assembly path.

## Target architecture

```
DatabaseManager
├── load_all(cls, ...) → Iterator[BaseVariable]            # unchanged user contract
└── load_all_as_df(cls, *, layout="packed"|"spread",
                   include_rid=False, include_bp=False,
                   stringify_schema=False, ...) → DataFrame # NEW shared engine

BaseVariable
└── load_all(as_df=False|True, include_record_id=False, ...)
       ├─ as_df=False → returns db.load_all(cls, ...)                       (unchanged)
       └─ as_df=True  → returns db.load_all_as_df(cls, layout="packed",
                                                  include_rid=include_record_id)

scidb.foreach
└── _convert_inputs → calls db.load_all_as_df(var_type, layout="spread",
                                              include_rid=True,
                                              include_bp=True,
                                              stringify_schema=True)
    _load_var_type_all → DELETED
```

### What each parameter on `load_all_as_df` does

| Parameter | Purpose | Used by |
|---|---|---|
| `layout="packed"` | dataframe-mode data goes in one `data` column whose cells hold the nested DataFrame. multi_column data goes in one `view_name` column of dicts. single_column data goes in one `view_name` column of scalars. One output row per record. | `BaseVariable.load_all(as_df=True)` |
| `layout="spread"` | dataframe-mode data is flattened: each DataFrame column becomes a top-level column; multi-row records contribute multiple output rows. multi_column / single_column same as packed. | `_convert_inputs` (foreach) |
| `include_rid=True` | Add `__record_id` column. Foreach needs this for variant tracking; notebook user opt-in via `include_record_id=True` flag (renames to `record_id`). | foreach + opt-in user |
| `include_bp=True` | Add `__branch_params` (JSON string) column. Foreach-only — used for variant-tracking and lineage. Not exposed to users. | foreach |
| `stringify_schema=True` | Cast schema-key column values to strings via `_schema_str`. Needed because scifor compares against user-supplied iterables that arrive as strings. Off by default — user notebooks want typed values. | foreach |

## Dispatch (where the fast path / slow fall-back decision lives)

Single dispatcher at the top of `db.load_all_as_df`. All checks are O(1), happen once on entry, and are determined by `cls` + `dtype_meta` (no per-record decisions, no probing):

```python
def load_all_as_df(self, cls, metadata=None, *, layout="packed",
                   include_rid=False, include_bp=False,
                   stringify_schema=False, version_id="latest",
                   where=None, branch_params_filter=None) -> pd.DataFrame:
    dtype_meta = self._lookup_dtype(cls)

    # --- Class-introspection bailouts ---
    if cls.__init__ is not BaseVariable.__init__:
        return self._load_as_df_via_iterator(cls, metadata, layout=layout, ...)
    if cls.from_db is not BaseVariable.from_db:
        return self._load_as_df_via_iterator(cls, metadata, layout=layout, ...)

    # --- Storage-mode bailouts ---
    if dtype_meta.get("custom"):
        return self._load_as_df_via_iterator(cls, metadata, layout=layout, ...)
    if dtype_meta.get("nested"):  # multi_column with _unflatten_dict
        return self._load_as_df_via_iterator(cls, metadata, layout=layout, ...)

    # --- Fast path ---
    records = self._fetch_records(cls, metadata, version_id,
                                  where=where, branch_params_filter=branch_params_filter)
    if records.empty:
        return pd.DataFrame()
    chunk_df = self._fetch_data_chunks(cls, records, dtype_meta)
    return self._assemble_fast(records, chunk_df, dtype_meta,
                               layout=layout, include_rid=include_rid,
                               include_bp=include_bp,
                               stringify_schema=stringify_schema)
```

`_load_as_df_via_iterator` is the existing assembly loop (currently in `_load_var_type_all`) relocated. It honors all the same `layout`/`include_*`/`stringify_schema` parameters by reshaping after iteration. Bit-equivalence on the cases the fast path supports is structurally guaranteed: same record set (same `_find_record`), same data fetch, just different post-processing.

## Fast-path implementation per mode × layout

After the chunk SQL returns, the post-processing per mode:

- **single_column, any layout**: `_duck._restore_types(chunk_df)` once per chunk → rename data col to `view_name` (spread) or `data` (packed) → vectorized merge with records on `record_id`.
- **multi_column flat, packed**: bundle data cols into a single column of dicts via `chunk_df[data_cols].to_dict('records')` (one vectorized pandas call, no Python loop) → merge with records.
- **multi_column flat, spread**: keep data cols spread (already that shape from chunk_df) → merge with records.
- **dataframe (1-row records), packed**: group chunk_df rows by record_id (still one row per group in this case), wrap each in a 1-row DataFrame for the `data` cell → merge. *(This case is rare for notebook users; if profiling shows it matters, optimize later.)*
- **dataframe (1-row records), spread** ← *your DummyMixed case*: chunk_df IS already the wide data layout. Merge with records on `record_id`. **No groupby. No per-record DataFrame construction. This is where the ~6s saving lives.**
- **dataframe (multi-row records), packed**: groupby record_id, per group build the nested DataFrame, store in `data` cell.
- **dataframe (multi-row records), spread**: groupby record_id for the meta-replication only (left join records to chunk_df on record_id achieves this without an explicit groupby).

Vectorized `_storage_to_python` dispatch: most types are pass-through (DOUBLE no-op; DOUBLE[] already list-of-list from DuckDB). JSON columns get `pd.Series.apply(json.loads)` once per column instead of per cell. A small helper next to `_storage_to_python` does this column-wise dispatch.

## Pros / cons

### Maintainability

**Pro:**
- One engine instead of three (db.load_all, BaseVariable.load_all(as_df=True), _load_var_type_all). Storage-format changes touch one place.
- Iterator path (`db.load_all`) untouched — zero risk to user code that consumes BaseVariable instances.
- Fall-back keeps the safety net honest: anything the fast path doesn't claim explicitly routes to the slow path verbatim. No silent drift on unsupported cases.
- `_load_var_type_all` deletion removes a chunk of duplicated assembly logic from foreach.py — that module becomes shorter and more focused on the foreach loop itself.

**Con:**
- One engine with parameters is more complex than three simple functions. Mitigated by the fact that the parameters are orthogonal (`layout`, `include_rid`, `include_bp`, `stringify_schema`) and dispatch cleanly.
- Two execution paths within the engine (fast vs iterator-fallback). 5 storage modes × 2 layouts = at most 10 branches in the fast path; if any storage detail changes we touch more branches than a single iterator-based assembly does. Mitigated by sharing a "deserialize one chunk into typed columns" helper across modes.

### Code complexity

- `db.load_all_as_df` engine: ~180 lines including docstring + dispatch + fall-back + per-mode/per-layout assembly.
- Per-column vectorized `_storage_to_python` helper next to `_storage_to_python` in `sciduckdb.py`: ~40 lines.
- `BaseVariable.load_all(as_df=True)` body shrinks to ~10 lines (delegate to engine, rename column).
- `_load_var_type_all` deletion: removes ~120 lines from `foreach.py`.
- `_convert_inputs` change: ~10-line edit (call engine directly).
- Regression tests: ~250 lines covering each (mode × layout × bailout) combination.
- **Net: ~350 lines added, ~150 removed.** Single PR scope.

### Magnitude of change

- 1 new public method on `DatabaseManager`.
- 1 internal helper in `sciduckdb`.
- 1 method-body shrink on `BaseVariable.load_all`.
- 1 function deleted from `scidb.foreach`.
- 1 caller update in `_convert_inputs`.
- 0 changes to public BaseVariable subclasses (`RawSignal`, etc.).
- 0 changes to MATLAB-side code (bridge sees the same DataFrame output shape from `_load_var_type_all_as_df` that `_load_var_type_all` produced).

### Table schema changes

**None.** Same `_record_metadata`, `_variables`, `<Type>_data` tables. Same queries.

### User-facing API changes

**None breaking.**

Optional additive — not blocking:
- `BaseVariable.load_all_as_df(...)` classmethod for symmetry (would just delegate). Not strictly needed; `load_all(as_df=True)` covers it.

## Performance documentation — where each note goes

The architecture only works if engineers can find the perf characteristics without reading the source. Three places get updated:

### 1. `db.load_all_as_df` docstring (engine)

Must document, in this order:
- **What it returns** for each `layout` value.
- **Fast-path supported cases**: storage modes (single_column, multi_column-flat, dataframe-1row, dataframe-multirow) and the `cls.__init__ is BaseVariable.__init__` & `cls.from_db is BaseVariable.from_db` requirement.
- **Fall-back triggers** (custom dtype, nested multi_column, subclass overrides) — explicit list, not vague "some cases."
- **Order-of-magnitude speedup expectation** vs the slow path, with the caveat that the win depends on storage mode and record count.

### 2. `BaseVariable.load_all` docstring (wrapper)

Already documents `as_df`. Adds:
- "When `as_df=True`, internally calls `db.load_all_as_df` (bulk path)."
- "When `as_df=False`, returns the iterator that constructs one BaseVariable per record — useful when you need `.content_hash`, `.lineage_hash`, or subclass-specific instance behavior. Pay per-record construction cost."
- "Rule of thumb: if you're going to put the results in a DataFrame anyway, prefer `as_df=True`."

### 3. New doc: `docs/claude/load-all-as-df-architecture.md`

Engineer-facing internals doc covering:
- Why the two paths exist (iterator API contract vs bulk-table use case).
- The dispatch table (cls + dtype_meta → fast path / fall-back).
- Per-mode × per-layout assembly strategy.
- Vectorized `_storage_to_python` column dispatch.
- How to extend (adding a new dtype mode requires updating dispatch + assembly).
- Performance expectations (with the DummyMixed benchmark numbers as the reference data point).

This sits next to the existing `matlab-load-performance.md`, `archive/array-column-loading-optimization.md`, `dataframe-conversion-factoring.md` in `docs/claude/` — same audience, same depth.

### 4. Light touch to `docs/guide/variables.md` (user-facing)

A single short note in the `load_all` section:
- "If you want all matching records as a DataFrame, pass `as_df=True` — it uses a bulk-fetch path that's much faster than iterating and building the DataFrame yourself."

No internals leak into the public guide. Just the user-actionable takeaway.

## Validation plan

1. **Equivalence tests** (`tests/scidb/test_load_all_as_df_equivalence.py`): for each combination of (storage mode × layout × include_rid × include_bp × stringify_schema), assert `db.load_all_as_df(...)` returns a DataFrame bit-equal to the assembly produced by the iterator path. Equivalence: column names, column order, dtypes, cell values, JSON formatting of `__branch_params`.
2. **Fall-back triggers**: tests verifying that
   - `cls.__init__` overridden → routes to slow path
   - `cls.from_db` overridden → routes to slow path
   - `dtype_meta["custom"]` → routes to slow path
   - `dtype_meta["nested"]` → routes to slow path
   and that the slow-path output exactly matches what `_load_var_type_all` produces today (i.e. no behavior change for fall-back cases).
3. **`BaseVariable.load_all(as_df=True)` equivalence**: regression test that the new shape (post-engine-delegation) matches the current shape byte-for-byte on a fixture covering each storage mode.
4. **Re-run `TestForEachTimingInstrumentation`**: compare `[timing]` lines before/after on the same DummyMixed fixture. Target: dataframe-mode load drops from ~7.4s to ~1s; remaining cost should be chunk SQL (~0.3s) + vectorized JSON parse on `version_keys` (~0.05s after the small follow-up vectorization) + assembly merge (~0.1s).
5. **Full `scidb` and `sci-matlab` test runs** must pass unchanged.

## Out of scope (deliberate)

- `_find_record` latest-collapse vectorization (saves ~0.25s; separate small PR).
- Vectorizing `_deserialize_custom_subdf` for custom-mode variables.
- Lazy `.data` construction in `db.load_all` iterator (would speed up user iteration too; much bigger contract change).
- MATLAB↔Python marshalling on the bridge side (timing run showed it's not a hot spot for the load case).

## Sequencing (suggested PR shape)

One PR is fine but if reviewers want it split:

1. Extract `db.load_all_as_df` engine + fall-back. Route `_load_var_type_all` through it (still exists as a one-line shim). Tests + docs.
2. Route `BaseVariable.load_all(as_df=True)` through the engine. Tests verifying public-API shape unchanged.
3. Delete `_load_var_type_all`; inline call into `_convert_inputs`. Tests.

Each step is independently revertible and independently testable.
