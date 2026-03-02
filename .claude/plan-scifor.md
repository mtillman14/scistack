# Plan: `scifor` — Standalone `for_each` Package

## Motivation

`for_each` is currently implemented in `scirun-lib` and is tightly coupled to
SciDB's database layer. The core loop logic — iterating over metadata
combinations, dispatching inputs, collecting outputs — has value independent of
any database. This plan extracts that logic into a new standalone package
(`scifor`) that works with plain DataFrames (Python) / tables (MATLAB), file
I/O, or any custom `.load()`/`.save()` implementation. SciDB becomes a consumer
of `scifor`, adding DB-specific features on top.

**Goals:**
- Identical user-facing API with and without a database
- Minimal code duplication between DB and standalone modes
- No new concepts required for existing SciDB users (zero breaking changes)
- Gradual adoption path: use `scifor` standalone, migrate to SciDB later

---

## Core Design Decisions

### 1. Schema keys global

`scifor` maintains a module-level schema key list:

```python
# scifor/schema.py
_schema_keys: list[str] = []

def set_schema(keys: list[str]) -> None:
    global _schema_keys
    _schema_keys = list(keys)

def get_schema() -> list[str]:
    return list(_schema_keys)
```

`scidb.configure_database()` automatically calls `scifor.set_schema(dataset_schema_keys)` as a side effect. Standalone users call it once manually. `distribute=True` and DataFrame detection both read from this global.

In MATLAB, a persistent variable in `+scifor/set_schema.m` serves the same role, populated by `scidb.configure_database()`.

### 2. DataFrame / table input detection

Whether a DataFrame input is treated as a per-combo input or a constant is determined automatically:

> A DataFrame is a **per-combo input** if `set(df.columns) ∩ set(get_schema())` is non-empty.
> Otherwise it is a **constant** and is passed unchanged to the function on every iteration.

Examples with `schema = ["subject", "session"]`:

```python
# Per-combo — has schema key columns → rows filtered per iteration
raw_emg_df = pd.DataFrame({"subject": [...], "session": [...], "emg": [...]})

# Constant — no schema key columns → passed unchanged every iteration
coeffs_df = pd.DataFrame({"freq_low": [...], "freq_high": [...]})
```

This works identically in DB and non-DB modes — no special-casing.

### 3. Per-combo DataFrame filtering

For each metadata combo, a per-combo DataFrame input is filtered to rows where
the schema key columns (that exist in the DataFrame) match the current combo values.
Columns absent from the DataFrame are not filtered on — this handles coarser-resolution
inputs naturally (e.g., a subject-level DataFrame used in a trial-level iteration returns
all rows for that subject).

After filtering:
- **One row, one data column** → extract the cell value (scalar, array, etc.) and pass to `fn`
- **Multiple rows** → pass the sub-DataFrame to `fn`
- **`as_table=True`** → always pass as DataFrame, even for one row

### 4. `[]` shorthand — standalone mode

`subject=[]` means "all distinct values." In standalone mode (no DB), `for_each`
scans all per-combo DataFrame inputs for distinct values in the `subject` column
and takes their union. Error if no input DataFrame has that column.

In DB mode, behavior is unchanged: `db.distinct_schema_values(key)`.

### 5. `distribute` — uses schema global in both modes

`distribute=True` reads `scifor.get_schema()` to find the schema hierarchy. Since
`configure_database()` calls `set_schema()`, this works identically in both modes.
Standalone users who need `distribute` call `scifor.set_schema()` explicitly.

### 6. `Fixed`, `Merge`, `ColumnSelection`, `PathInput` — unchanged semantics

These move from `scirun-lib` into `scifor`. Their semantics are unchanged:

- `Fixed(df, session="baseline")` — the DataFrame is checked for schema key columns
  and filtered with `session` overridden to `"baseline"`.
- `Merge(df_a, df_b)` — each constituent filtered independently, then columns joined.
- `ColumnSelection(df, ["col"])` — filter rows, then select the specified column(s).
- `PathInput` — generates a file path from metadata, passes it to `fn`. Unchanged.

`scirun-lib` re-exports all of these for backwards compatibility.

### 7. File I/O classes — `MatFile`, `CsvFile`

Single class with both `load()` and `save()`. These can be used as either inputs
or outputs to `for_each`.

```python
class MatFile:
    def __init__(self, path_template: str):
        # e.g. "data/{subject}/{session}.mat"
        self.path_template = path_template

    def load(self, **metadata):
        import scipy.io
        path = self.path_template.format(**metadata)
        return scipy.io.loadmat(path)

    def save(self, data, **metadata):
        import scipy.io
        path = self.path_template.format(**metadata)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        scipy.io.savemat(path, {"data": data})

class CsvFile:
    def load(self, **metadata): ...
    def save(self, data, **metadata): ...
```

`PathInput` remains separate — it passes the resolved path string *to* `fn` rather
than loading data itself.

### 8. Filters — `Col` for standalone, `VariableFilter` for DB

`scifor` defines a lightweight filter hierarchy for operating on column names:

```python
Col("side") == "R"                         # ColFilter
(Col("side") == "R") & (Col("speed") > 1.2) # CompoundFilter
~(Col("side") == "R")                      # NotFilter
```

`ColFilter` is applied as a pandas boolean mask inside the DataFrame filtering logic
(not inside a `.load()` call, since DataFrame inputs are handled natively).

SciDB's `VariableFilter`, `ColumnFilter`, `InFilter`, `CompoundFilter`, `NotFilter`,
`RawFilter` are unchanged and continue resolving to schema_ids via DuckDB.

`RawFilter` / `raw_sql()` has no standalone equivalent.

In MATLAB, `scifor.Col("side") == "R"` mirrors the Python syntax.

The `&`, `|`, `~` operator syntax is identical between both filter systems.

### 9. `ForEachConfig` / version keys — DB-only, dropped in standalone

`ForEachConfig` is not part of `scifor`. In `scirun-lib`'s DB wrapper, version
keys are still computed and embedded in save metadata as today. Standalone
users do not get version keys (no lineage database).

### 10. Return value and output protocol

`for_each` returns a DataFrame of results in both modes (unchanged). For saving
outputs, any object with `.save(data, **metadata)` works. Built-in options:
`MatFile`, `CsvFile`, `BaseVariable` (DB-backed). The `outputs=` parameter
remains optional — if omitted or empty, `for_each` just returns the result
DataFrame without saving.

### 11. `db=` kwarg passthrough

`db=` continues to be passed to `.load()` and `.save()` calls on objects that
accept it (i.e., `BaseVariable`). Standalone objects (`MatFile`, `CsvFile`,
`DataFrameInput`-like logic) accept `**kwargs` and ignore `db=`. No change
to the call sites in the loop.

---

## Package Structure

### New package: `scifor`

```
scifor/
  src/scifor/
    __init__.py          # exports: for_each, set_schema, get_schema,
                         #          Fixed, Merge, ColumnSelection, PathInput,
                         #          Col, MatFile, CsvFile
    schema.py            # set_schema(), get_schema(), _schema_keys global
    foreach.py           # core for_each loop
    filters.py           # Col, ColFilter, CompoundFilter, NotFilter
    files.py             # MatFile, CsvFile
    fixed.py             # Fixed (moved from scirun-lib)
    merge.py             # Merge (moved from scirun-lib)
    column_selection.py  # ColumnSelection (moved from scirun-lib)
    pathinput.py         # PathInput (moved from scirun-lib/scidb)
  pyproject.toml
  README.md
```

No dependencies on `scidb`, `sciduck`, `pipelinedb-lib`, or `thunk-lib`.
Optional soft dependencies: `pandas`, `numpy`, `scipy` (for file I/O).

### Modified: `scirun-lib`

`scirun-lib/src/scirun/foreach.py` becomes a thin wrapper:

```python
from scifor import for_each as _scifor_for_each
from .foreach_config import ForEachConfig

def for_each(fn, inputs, outputs, db=None, where=None, **metadata_iterables):
    # DB-specific preamble:
    # 1. [] resolution via db.distinct_schema_values()
    # 2. Schema combo pre-filtering via db.distinct_schema_combinations()
    # 3. Build ForEachConfig version keys
    # Then delegate to scifor's for_each with augmented metadata/outputs
    ...
```

`scirun-lib` retains only `foreach.py` and `foreach_config.py`. `Fixed`, `Merge`,
`ColumnSelection`, and `PathInput` are removed from `scirun-lib` entirely — they
now live in `scifor` only. `scidb.__init__` imports them directly from `scifor`.

### Modified: `scidb` (`src/scidb/`)

- `database.py`: `configure_database()` calls `scifor.set_schema(dataset_schema_keys)`
- `__init__.py`: `for_each` imported from `scirun-lib`; `Fixed`, `Merge`,
  `ColumnSelection`, `PathInput` imported directly from `scifor`
- `filters.py`: unchanged

---

## `for_each` Loop: What Changes vs. What Stays

The main loop body is **mode-agnostic** and lives entirely in `scifor`. The
only DB-specific logic is in the **preamble** (before the loop) in `scirun-lib`:

| Section | Location | Notes |
|---|---|---|
| `[]` resolution | `scirun-lib` wrapper | DB: `distinct_schema_values()`; standalone: scan input DataFrames |
| Schema combo pre-filtering | `scirun-lib` wrapper | DB-only optimization, dropped in standalone |
| `ForEachConfig` version keys | `scirun-lib` wrapper | DB-only |
| `distribute` key lookup | `scifor` (uses global) | Both modes read `get_schema()` |
| DataFrame detection | `scifor` loop | Schema key column presence check |
| Input loading | `scifor` loop | DataFrames filtered natively; `.load()` objects called normally |
| Function call | `scifor` loop | Unchanged |
| Output saving | `scifor` loop | `.save()` protocol; `db=` passed through and ignored if not needed |
| Result collection | `scifor` loop | Unchanged |
| `dry_run`, `as_table`, `Fixed`, `Merge` | `scifor` loop | Unchanged |

---

## MATLAB

### Schema global

```matlab
% +scifor/set_schema.m
function set_schema(keys)
    persistent schema_keys;
    schema_keys = keys;
end

% +scifor/get_schema.m
function keys = get_schema()
    persistent schema_keys;
    if isempty(schema_keys)
        keys = string.empty;
    else
        keys = schema_keys;
    end
end
```

`scidb.configure_database()` calls `scifor.set_schema(dataset_schema_keys)`.

### Table input detection

Same rule as Python: a MATLAB table input is a per-combo input if its variable
names (column names) intersect with `scifor.get_schema()`.

### `for_each` in MATLAB

The existing `+scidb/for_each.m` is updated to:
1. Detect MATLAB table inputs using the schema key column rule
2. Filter table rows per combo (instead of calling Python bridge for `.load()`)
3. Skip the preload phase for table inputs (data already in memory — this is actually faster than DB mode by default)
4. Support `parallel=true` more broadly (table inputs have no Thunk restriction)
5. Call `scifor.set_schema()` / `scifor.get_schema()` for `distribute`

### `Col` filter for MATLAB

```matlab
scifor.Col("side") == "R"
(scifor.Col("side") == "R") & (scifor.Col("speed") > 1.2)
```

Applied as logical row indexing on MATLAB tables.

### File I/O

```matlab
% +scifor/MatFile.m  — load() and save() using MATLAB's load/save
% +scifor/CsvFile.m  — load() and save() using readtable/writetable
```

---

## Import paths after refactor

| Symbol | Old import | New import |
|---|---|---|
| `for_each` | `scidb` / `scirun` | `scidb` / `scifor` |
| `Fixed` | `scidb` / `scirun` | `scidb` / `scifor` |
| `Merge` | `scidb` / `scirun` | `scidb` / `scifor` |
| `ColumnSelection` | `scidb` / `scirun` | `scidb` / `scifor` |
| `PathInput` | `scidb` / `scirun` | `scidb` / `scifor` |
| `Col` | — | `scifor` only |
| `MatFile`, `CsvFile` | — | `scifor` only |
| `set_schema` | — | `scifor` only |

`scidb` re-exports everything a user needs — the only import path that changes is
if someone was importing directly from `scirun` (an internal package), which is
not part of the public API.

---

## Implementation Order

1. Create `scifor` package skeleton with `pyproject.toml`
2. Implement `scifor.schema` (`set_schema` / `get_schema`)
3. Move `Fixed`, `Merge`, `ColumnSelection`, `PathInput` from `scirun-lib` → `scifor`; delete from `scirun-lib`; update `scidb.__init__` to import them from `scifor`
4. Implement `scifor.filters` (`Col`, `ColFilter`, `CompoundFilter`, `NotFilter`)
5. Implement `scifor.files` (`MatFile`, `CsvFile`)
6. Implement `scifor.foreach` — core loop with DataFrame detection and filtering
7. Refactor `scirun-lib/foreach.py` into DB-specific preamble + delegation to `scifor`
8. Update `scidb.configure_database()` to call `scifor.set_schema()`
9. Update `scidb.__init__` imports if needed
10. Python tests: new `scifor` tests (no DB); verify existing `scirun-lib` and `scidb` tests still pass
11. MATLAB: implement `+scifor` package (`set_schema`, `get_schema`, `Col`, `MatFile`, `CsvFile`)
12. MATLAB: update `scidb.configure_database` to call `scifor.set_schema`
13. MATLAB: update `+scidb/for_each.m` for table input detection and filtering
14. MATLAB tests: new standalone table-based tests; verify existing tests pass

---

## Open Questions

- Should the Python `scifor` package be published to PyPI independently, or only
  distributed as part of the `scidb` install? (Probably independent — that's the
  whole point of the extraction.)
- Should `scirun-lib` be renamed or restructured now that its `for_each` is a thin
  wrapper? Or keep the name for continuity?
- `NpyFile` (numpy `.npy`/`.npz`)? `HDF5File`? These could be added later without
  architectural changes.
- For the MATLAB `+scifor` package: should it live inside `scidb-matlab` or as a
  separate repository? Given that MATLAB has no independent package manager equivalent
  to PyPI, keeping it in `scidb-matlab` is probably pragmatic.
