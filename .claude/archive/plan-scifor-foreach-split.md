# Plan: scifor.for_each as Pure Loop Orchestrator (No I/O)

## Design Philosophy

**scifor.for_each()** is a **pure nested-loop orchestrator**. It:
- Accepts **data** (MATLAB tables, constants) — not loadable objects
- Iterates over metadata combinations
- Filters/slices tables per combination
- Calls the user function with the sliced data
- Collects and returns results
- Does **NO I/O** — no `.load()`, no `.save()`, no database, no file operations, no Python bridge calls

**scidb.for_each()** is the **DB I/O layer**. It:
- Loads all input variables from the database into MATLAB tables
- Converts scidb wrappers → scifor wrappers (e.g. `scidb.Fixed(BaseVar)` → `scifor.Fixed(loaded_table)`)
- Handles DB-specific concerns (empty-list resolve, combo pre-filtering, version keys, Thunk, parallel, batch save)
- Delegates the core iteration to `scifor.for_each()`
- Saves results back to DB output variables

This **completely separates** nested-loop orchestration from data I/O.

---

## scifor.for_each() — New Interface

### Signature

```matlab
result_tbl = scifor.for_each(fn, inputs, varargin)
%   fn      - Function handle
%   inputs  - Struct mapping parameter names to:
%               MATLAB tables     → filtered per combo by metadata columns
%               scifor.Fixed()    → table filtered with overridden metadata
%               scifor.Merge()    → multiple tables merged column-wise
%               constants         → passed unchanged (numeric, string, struct, etc.)
%   varargin - Name-value pairs:
%               Metadata iterables: subject=[1 2 3], session=["A" "B"]
%               Options: dry_run, distribute, where, pass_metadata, as_table,
%                        output_names, _all_combos
```

### Key changes from current

1. **No `outputs` parameter** — scifor doesn't save anything. Functions just return values, scifor collects them.
2. **No `.load()` calls** — all inputs are already data (tables or constants).
3. **No `.save()` calls** — results are collected and returned as a table, caller saves if needed.
4. **No `preload`/`parallel`/`db`/`save` options** — these are DB concerns, moved to scidb.for_each.
5. **New `output_names` option** — optional cell array of strings to name result columns (defaults to `output_1`, `output_2`, etc.). Replaces the current approach of naming columns after output variable class names.
6. **`where=` uses scifor.Col filters** — filters table rows, not database queries. (scidb applies its own scidb.Filter during load, before delegating.)

### What stays in scifor.for_each

- Cartesian product of metadata iterables
- Table filtering per combo (`filter_table_for_combo`)
- `scifor.Fixed(table, key=val)` — filter with overridden metadata
- `scifor.Merge(table1, table2)` — column-wise table merge
- `scifor.ColumnSelection(table, cols)` — column extraction from table
- `distribute=` — split results by lowest schema level
- `where=` — scifor.Col-based table row filtering
- `dry_run=` — preview iterations without executing
- `pass_metadata=` — pass metadata as trailing NV args to fn
- `as_table=` — convert multi-row filtered results to table format
- `_all_combos` extension point — accept pre-built combo list from scidb
- Result collection → return as MATLAB table
- All standalone helpers: `cartesian_product`, `split_for_distribute`, `is_metadata_compatible`, `format_value`, etc.

### What's removed from scifor.for_each

| Removed | Lines | Moves to |
|---------|-------|----------|
| Empty-list resolve via `py.scidb.database.get_database().distinct_schema_values()` | 157-180 | scidb.for_each |
| Schema combo pre-filtering via `filter_db.distinct_schema_combinations()` | 276-341 | scidb.for_each |
| `build_config_nv()` / ForEachConfig version keys | 262-264, 2465-2529 | scidb.for_each |
| Preloading phase (`load_and_extract`, `preloaded_maps`) | 361-490 | scidb.for_each (becomes "load all data upfront") |
| Batch save accumulation + flush | 519-526, 859-900 | scidb.for_each |
| Per-iteration `.save()` calls | 855-883 | scidb.for_each |
| Per-iteration `.load()` calls on BaseVariable | 656-676 | scidb.for_each (load upfront, pass tables) |
| Parallel execution (`run_parallel` + parfor) | 493-508, 1048-1313 | scidb.for_each |
| Thunk detection + special handling | 116-117, 741-747, 753 | scidb.for_each |
| All `py.*` calls | throughout | scidb.for_each |
| All `scidb.internal.*` calls | throughout | scidb.for_each |
| `scidb.PathInput` handling | 382-384 | scidb.for_each |
| `outputs` parameter + `n_outputs` | line 1, 267 | scidb.for_each |
| `preload`/`parallel`/`db`/`save`/`where` (db Filter) options | split_options | scidb.for_each |

---

## scifor.Fixed — New Class

```matlab
classdef Fixed
    % scifor.Fixed  Specify fixed metadata overrides for a table input.
    %
    %   Wraps a MATLAB table with metadata overrides. When scifor.for_each
    %   filters this input for a combo, it uses the fixed metadata values
    %   instead of the iteration values for the specified keys.
    %
    %   Example:
    %       scifor.for_each(@compare, ...
    %           struct('baseline', scifor.Fixed(data_table, session="BL"), ...
    %                  'current',  data_table), ...
    %           subject=[1 2 3], session=["A" "B"])
    %
    properties (SetAccess = private)
        data            % MATLAB table (the actual data)
        fixed_metadata  struct  % Metadata overrides
    end
end
```

Note: the property is `data` (not `var_type` like scidb.Fixed) because it holds a table, not a variable type.

---

## scifor.Merge — New Class

```matlab
classdef Merge
    % scifor.Merge  Combine multiple tables into a single table input.
    %
    %   Wraps 2+ MATLAB tables. For each combo, scifor.for_each filters
    %   each table individually, then merges them column-wise (inner join
    %   on common schema key columns, or simple horzcat for single-row results).
    %
    %   Constituents can be:
    %   - MATLAB tables
    %   - scifor.Fixed(table, ...) wrappers
    %
    %   Example:
    %       scifor.for_each(@analyze, ...
    %           struct('data', scifor.Merge(gait_table, force_table)), ...
    %           subject=[1 2 3])
    %
    properties (SetAccess = private)
        tables  cell  % Cell array of table specs (tables or Fixed wrappers)
    end
end
```

---

## scifor.ColumnSelection — New Class (or enhancement)

```matlab
classdef ColumnSelection
    % scifor.ColumnSelection  Extract specific columns from a table input.
    %
    %   After filtering the table for the current combo, extracts only the
    %   specified columns. Single column → returns the column values (array).
    %   Multiple columns → returns a sub-table.
    %
    %   Example:
    %       scifor.for_each(@fn, ...
    %           struct('speed', scifor.ColumnSelection(data_table, "speed")), ...
    %           subject=[1 2 3])
    %
    properties (SetAccess = private)
        data     % MATLAB table
        columns  % String array of column names to extract
    end
end
```

---

## scidb.for_each() — Rewrite as DB Wrapper

### Signature (mostly unchanged from user perspective)

```matlab
result_tbl = scidb.for_each(fn, inputs, outputs, varargin)
%   fn      - Function handle or scidb.Thunk
%   inputs  - Struct mapping parameter names to BaseVariable instances,
%             scidb.Fixed wrappers, scidb.Merge wrappers, or constants
%   outputs - Cell array of BaseVariable instances for output types
%   varargin - Metadata iterables + options (preload, parallel, db, where, etc.)
```

### Implementation flow

```
scidb.for_each(@fn, inputs, outputs, subject=[1 2 3], session=["A" "B"])
│
├─ 1. Parse options (preload, parallel, db, where, save, + metadata iterables)
├─ 2. Resolve empty lists: [] → db.distinct_schema_values()
├─ 3. Set schema: scifor.set_schema(db.dataset_schema_keys)
├─ 4. Build ForEachConfig version keys
├─ 5. Pre-filter combos: db.distinct_schema_combinations() → _all_combos
│
├─ 6. PARALLEL BRANCH (if parallel=true):
│     Run 3-phase loop entirely within scidb (no delegation to scifor)
│     Return early
│
├─ 7. LOAD ALL INPUTS → TABLES:
│     For each loadable input:
│       BaseVariable    → bulk load all records → MATLAB table with metadata columns
│       Fixed(BaseVar)  → bulk load with fixed overrides → table
│       Merge(vars...)  → bulk load each constituent → tables
│       PathInput       → per-combo load → table (or handle separately)
│       table inputs    → pass through unchanged
│       constants       → pass through unchanged
│
├─ 8. CONVERT TO SCIFOR INPUTS:
│     scidb.Fixed(BaseVar, session="BL")  →  scifor.Fixed(loaded_table, session="BL")
│     scidb.Merge(VarA, VarB)             →  scifor.Merge(loaded_table_a, loaded_table_b)
│     BaseVariable                         →  loaded_table
│
├─ 9. HANDLE THUNK:
│     If fn is scidb.Thunk, wrap in a plain function that handles lineage
│
├─ 10. DELEGATE TO scifor.for_each():
│      result_tbl = scifor.for_each(fn_wrapped, scifor_inputs, ...
│          output_names=output_class_names, ...
│          _all_combos=filtered_combos, ...
│          metadata_iterables...)
│
├─ 11. SAVE RESULTS:
│      Iterate over result_tbl rows
│      For each row + each output:
│        output_var.save(data, metadata..., version_keys...)
│      Or batch save for efficiency
│
└─ 12. RETURN result_tbl
```

### Key design points

**Loading strategy**: scidb always loads ALL records for each input variable upfront (equivalent to current `preload=true`). This produces MATLAB tables with metadata columns that scifor can filter per combo. The `preload=false` option is removed (or kept as a legacy no-op with a warning). If memory is a concern, users should iterate at a finer granularity.

**Batch save is natural**: Since scifor returns all results at once in the result table, scidb can save them all in one batch — no proxy objects needed. This is simpler than the current incremental batch accumulation.

**Parallel stays in scidb**: The parfor branch doesn't delegate to scifor. It does its own per-worker loading and saving, since workers can't share pre-loaded tables across process boundaries.

**Thunk wrapping**: scidb wraps Thunk functions in a plain function handle before passing to scifor. The wrapper handles lineage tracking. scifor sees a regular function.

---

## Empty-List Resolution

Currently, `subject=[]` queries the database for all distinct values. In pure scifor:
- **scifor.for_each**: `subject=[]` means "infer from the table columns." If an input table has a `subject` column, extract all distinct values from it. If no table input has that column, error.
- **scidb.for_each**: `subject=[]` queries the database as before, then passes the resolved list to scifor.

---

## Files to Create

### `+scifor/Fixed.m` (~50 lines)
New class wrapping a MATLAB table + fixed metadata struct. Property: `data` (table), `fixed_metadata` (struct).

### `+scifor/Merge.m` (~50 lines)
New class wrapping 2+ table specs. Property: `tables` (cell array of tables or Fixed wrappers).

### `+scifor/ColumnSelection.m` (~40 lines)
New class wrapping a table + column names. Properties: `data` (table), `columns` (string array).

## Files to Modify

### `+scifor/for_each.m` — Rewrite as pure orchestrator (~800-1000 lines, down from 2612)

**Remove entirely:**
- All `py.*` calls
- All `scidb.internal.*` calls
- `outputs` parameter
- `preload`/`parallel`/`db`/`save`/`where` (scidb.Filter) options from `split_options`
- Empty-list DB resolve → replace with table-column inference
- Schema combo pre-filtering via DB
- ForEachConfig version keys
- Preloading phase
- Batch save accumulation + flush
- Per-iteration `.load()` / `.save()` calls
- Parallel `run_parallel` + parfor
- Thunk-specific handling
- `has_pathinput()` helper
- `build_config_nv()` / `serialize_loadable_inputs()` / `input_spec_to_key()` helpers
- `merge_constituents()` (current version calls `.load()` on vars) → replace with pure-table merge

**Rewrite:**
- `is_loadable()` → `is_data_input()`: detects tables, scifor.Fixed, scifor.Merge, scifor.ColumnSelection
- Input loading loop → input *filtering* loop: for each combo, filter tables by metadata columns
- `merge_constituents()` → `merge_tables()`: filter each table in a Merge, join column-wise
- `results_to_output_table()` → rebuild to use `output_names` instead of output class names
- `split_options()` → remove DB options, add `output_names`, `_all_combos`
- `format_inputs()` / `format_outputs()` / `print_dry_run_iteration()` → adapt for tables
- Empty-list resolve → scan input table columns for distinct values
- Unwrapping → no longer needed (inputs are already raw data)

**Keep as-is:**
- `cartesian_product()`
- `filter_table_for_combo()`
- `split_for_distribute()`
- `is_metadata_compatible()`
- `normalize_cell_column()`, `cartesian_indices()`, `format_value()`

### `+scidb/for_each.m` — Rewrite as full DB wrapper (~600-800 lines)

Currently a 10-line passthrough. Becomes a substantial function that:

1. Parses options (including DB-specific: `preload`, `parallel`, `db`, `where`, `save`)
2. Resolves empty metadata lists via DB
3. Propagates schema
4. Builds ForEachConfig version keys
5. Pre-filters combos via DB
6. Handles parallel branch (moved from scifor, stays self-contained)
7. Loads all input variables → MATLAB tables
8. Converts scidb wrappers → scifor wrappers
9. Delegates to scifor.for_each
10. Saves results to output variables (batch or per-row)

**Moved helpers (from scifor → scidb local functions):**
- `build_config_nv()`, `serialize_loadable_inputs()`, `input_spec_to_key()`, `format_repr()`
- `run_parallel()` (full parallel execution, ~270 lines)
- `schema_str()` (for DB combo filtering)
- `has_pathinput()` (checks `scidb.PathInput`)
- Preloading logic (adapted to produce tables instead of ThunkOutput arrays)
- Batch save logic (reads from result_tbl, saves to DB)

## Files NOT Changed

- `+scidb/Fixed.m` — stays as-is (wraps BaseVariable, used by scidb.for_each)
- `+scidb/Merge.m` — stays as-is (wraps BaseVariables, used by scidb.for_each)
- `+scifor/Col.m`, `ColFilter.m`, `CompoundFilter.m`, `NotFilter.m` — no changes
- `+scifor/set_schema.m`, `+scifor/get_schema.m`, `+scifor/schema_store_.m` — no changes

---

## Tests

### New: `TestSciforForEach.m` — standalone for_each tests (no database)
- Table input filtering by schema keys
- Constants passed through unchanged
- `scifor.Fixed(table, key=val)` filtering
- `scifor.Merge(table1, table2)` merging
- `scifor.ColumnSelection(table, col)` extraction
- Distribute with table outputs
- Dry-run mode
- Empty-list inference from table columns
- Multi-output with `output_names`
- `where=scifor.Col("speed") > 1.5` filtering
- `pass_metadata=true`
- Result table structure (metadata cols + output cols)

### Existing: `TestForEach.m` — should pass as-is
All existing DB tests call `scidb.for_each()` which is now a proper wrapper. The interface is unchanged from the caller's perspective.

### Existing: `TestScifor.m` — add tests
- `scifor.Fixed` constructor and properties
- `scifor.Merge` constructor and properties
- `scifor.ColumnSelection` constructor and properties

---

## Resolved Design Decisions

### `as_table=` semantics

`as_table=` still exists and the convention is unchanged:
- **`as_table=true`**: The function receives a table with metadata columns + data column(s). This is the mechanism to pass metadata to the called function when needed.
- **`as_table=false`** (default): The subset of data matching the current combo is unwrapped and passed directly to the function — as a table if that's the data's type, or as any other type as appropriate. Metadata is not included; data unwrapping is performed if appropriate.

Thunk-specific as_table handling is scidb's concern, not scifor's.

### Function output count

scifor needs to know the output count because MATLAB requires specifying `nargout` at call time (`[out1, out2] = fn(...)`). The `output_names` parameter serves double duty:
- **Names** the result table columns
- **Implicitly provides the count** (`numel(output_names)`)
- Defaults to `{"output"}` (single output) if not specified
- Can also accept a plain number (e.g. `output_names=3`) for auto-named outputs (`output_1`, `output_2`, `output_3`)

### Table metadata columns

Convention: loaded tables passed to scifor MUST include schema key columns alongside data columns. When scidb bulk-loads a variable, the resulting table has columns like `[subject, session, <data_col_1>, <data_col_2>, ...]`. scifor uses the schema key columns (via `scifor.get_schema()`) to filter rows per combo. After filtering, when `as_table=false`, scifor drops the schema key columns and passes only the data columns to the function.

---

## Verification

1. Run new `TestSciforForEach` tests for standalone behavior
2. Run existing MATLAB test suite (`TestForEach`, `TestScifor`, etc.) to verify no regressions
3. Run Python tests (`scifor/tests/`, `scirun-lib/tests/`) to verify Python side is unaffected
