# `where=` Filter System

## Purpose

Users frequently need to load one variable conditioned on the value of another. Example: load `StepLength` only where `Side == "L"`. The `where=` parameter provides a composable, Pythonic filter system that requires no SQL knowledge for common cases.

## Syntax

```python
# Equality filter on a whole-variable value
StepLength.load(where=Side == "L")

# Comparison filter
StepLength.load(where=Speed > 1.2)

# Compound AND / OR
StepLength.load(where=(Side == "L") & (Speed > 1.2))
StepLength.load(where=(Side == "L") | (Side == "R"))

# NOT
StepLength.load(where=~(Side == "L"))

# Column-level filter (tabular variable)
StepLength.load(where=GaitData["Side"] == "L")

# Set membership
StepLength.load(where=GaitData["Side"].isin(["L", "R"]))

# Raw SQL escape hatch (applied to the target variable's data table)
StepLength.load(where=raw_sql('"value" > 0.70'))

# Combined with metadata addressing
StepLength.load(subject=1, where=Side == "L")
```

## How It Works

Filters are resolved **before** data is fetched, at the schema_id level:

1. `load()` calls `db.load_all_as_df()` (DataFrame path) or `db.load()` (generator path).
2. `_find_record()` returns the full set of matching `_record_metadata` rows.
3. If `where` is provided, `where.resolve(db, target_class, table_name)` is called.
4. `resolve()` returns `allowed_schema_ids: set[int]`.
5. The records DataFrame is filtered: `records = records[records["schema_id"].isin(allowed_schema_ids)]`.
6. Bulk data loading proceeds as normal over the filtered records.

This is efficient — no unnecessary data is loaded from DuckDB.

## Filter Class Hierarchy

All classes are in `src/scidb/filters.py`.

| Class | Created by | Queries |
|-------|-----------|---------|
| `VariableFilter` | `Side == "L"` (metaclass) | Filter variable's `"value"` column |
| `ColumnFilter` | `GaitData["Side"] == "L"` (ColumnSelection) | Filter variable's named column |
| `InFilter` | `.isin([...])` | Filter variable's column via SQL `IN` |
| `SchemaKeyCompareFilter` | `schema_key("subject") > 2` | `_schema` table by key value |
| `SchemaKeyInFilter` | `schema_key("session").isin(["BL","POST"])` | `_schema` table via SQL `IN` |
| `CompoundFilter` | `f1 & f2` or `f1 \| f2` | Set intersection / union of schema_ids |
| `NotFilter` | `~f` | Complement: all target schema_ids minus inner set |
| `RawFilter` | `raw_sql("...")` | Raw SQL applied to the **target** variable's data table |

### resolve() contract

Every filter implements:
```python
def resolve(self, db, target_variable_class, target_table_name) -> set[int]:
    ...
```

Returns the set of `schema_id` integers that pass the filter. `CompoundFilter` combines two sets; `NotFilter` subtracts from the full target schema_id set.

### Latest-version semantics for filter variables

`VariableFilter`, `ColumnFilter`, and `InFilter` all query the filter variable using "latest version per parameter set" semantics (a `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY version_id DESC)` CTE). This mirrors exactly how `load(version="latest")` works for the target variable — no special casing.

## Metaclass: `VariableMeta`

Defined in `src/scidb/variable.py` and applied to `BaseVariable`.

Overrides `__eq__`, `__ne__`, `__lt__`, `__le__`, `__gt__`, `__ge__` at the **class** level so that `Side == "L"` (where `Side` is a class, not an instance) produces a `VariableFilter`.

Two important preservation rules:
- **Class-to-class equality is preserved**: `Side == Side` returns `True` (standard `type.__eq__` is called when `other` is a `type`).
- **Hashing is preserved**: `__hash__ = type.__hash__` is explicitly set. Without this, overriding `__eq__` on a metaclass would make classes unhashable, breaking dict/set use.

## ColumnSelection operators

`ColumnSelection` (in `scirun-lib/src/scirun/column_selection.py`) gained comparison operators and `isin()`. Each returns a `ColumnFilter` using `self.columns[0]` as the column name. Multi-column selections only use the first column for filtering (consistent with single-column typical usage).

`__hash__` was also added to `ColumnSelection` since `__eq__` was overridden.

## Schema-Level Validation

### Filter at same or coarser level — OK
- **Same level** (e.g., both at `trial`): direct schema_id set match.
- **Coarser level** (e.g., filter at `subject`, target at `trial`): the filter's matching `schema_id`s are **expanded** to all finer-level `schema_id`s in the target that share the same coarse key values. This is a natural hierarchical filter.

### Filter at finer level — Error
Raises `ValueError`:
```
Filter variable 'Side' is stored at schema level 'trial' which is finer than
target 'StepLength' at level 'subject'. Filters must be at the same or coarser
level than the target.
```

### Coverage validation
Every schema location in the target must have a corresponding filter value. If incomplete coverage is detected, raises:
```
Filter variable 'Side' is missing data at 2 schema locations that 'StepLength'
has data for. Ensure the filter variable covers all target locations.
```

### Level detection
Level is inferred by inspecting `_schema` rows for each variable's schema_ids. The deepest non-null column in `dataset_schema_keys` order is the variable's level. This is the same logic used by `_infer_schema_level()` in `DatabaseManager`.

## SchemaKey Filters

`SchemaKey` filters restrict records by schema key values (the keys defined at `configure_database`) rather than by another variable's data. They are appropriate when the filtering criterion is inherent to the dataset structure — e.g. "only BL and POST sessions" or "only subjects 3 through 8".

### Python syntax

```python
from scidb import schema_key

# Set membership
MyVar.load(where=schema_key("session").isin(["BL", "POST"]))

# Equality / inequality
MyVar.load(where=schema_key("session") == "BL")
MyVar.load(where=schema_key("session") != "FOL")

# Ordering (numeric keys stored as VARCHAR — TRY_CAST to DOUBLE is used)
MyVar.load(where=schema_key("subject") > 2)
MyVar.load(where=schema_key("subject") >= 3)
MyVar.load(where=schema_key("subject") < 5)
MyVar.load(where=schema_key("subject") <= 4)

# Compound
MyVar.load(where=(schema_key("subject") >= 2) & (schema_key("subject") <= 4))
MyVar.load(where=(schema_key("session") == "BL") | (schema_key("session") == "POST"))
MyVar.load(where=~schema_key("session").isin(["FOL"]))
```

### MATLAB syntax

```matlab
% Set membership (ismember overloaded on scidb.SchemaKey)
MyVar().load(where=ismember(scidb.schema_key("session"), ["BL", "POST"]))

% Equality / inequality
MyVar().load(where=scidb.schema_key("session") == "BL")
MyVar().load(where=scidb.schema_key("session") ~= "FOL")

% Ordering (numeric keys)
MyVar().load(where=scidb.schema_key("subject") > 2)
MyVar().load(where=scidb.schema_key("subject") >= 3)
MyVar().load(where=scidb.schema_key("subject") < 5)
MyVar().load(where=scidb.schema_key("subject") <= 4)

% Compound
MyVar().load(where=(scidb.schema_key("subject") >= 2) & (scidb.schema_key("subject") <= 4))
```

### How it resolves

Both `SchemaKeyCompareFilter` and `SchemaKeyInFilter` query the `_schema` table directly, then intersect with the target variable's schema_ids:

```
matching = {schema_id FROM _schema WHERE key condition}
           ∩
           _get_all_schema_ids_for_variable(target)
```

There is no filter-variable coverage check — schema key filters are self-contained and do not require another variable to have data at matching locations.

### Numeric ordering and VARCHAR storage

Schema key values are stored as `VARCHAR` in `_schema`. Lexicographic ordering would give wrong results for multi-digit integers (`"10" < "2"`). The ordering operators (`<`, `<=`, `>`, `>=`) always use `TRY_CAST(key AS DOUBLE)` so numeric ordering is correct.

For equality/inequality (`==`, `!=`) the comparison is done as `VARCHAR` after converting the Python value to its stored string form (matching `_schema_str`: `1.0 → "1"`, `2 → "2"`, `"BL" → "BL"`). The `isin()` filter similarly converts values to strings before comparison.

### Validation

If the key name is not in `dataset_schema_keys`, both filter types raise:
```
ValueError: Unknown schema key: 'nonexistent'. Valid keys: ['subject', 'session']
```

This is checked at `resolve()` time (when `load()` is called), not at construction time.

### Key files

| File | Role |
|------|------|
| `scidb/src/scidb/model/filters.py` | `SchemaKey`, `SchemaKeyCompareFilter`, `SchemaKeyInFilter`, `schema_key()` |
| `scidb/src/scidb/__init__.py` | Exports `schema_key` |
| `sci-matlab/src/sci_matlab/matlab/+scidb/SchemaKey.m` | MATLAB builder class with operator overloads and `ismember` |
| `sci-matlab/src/sci_matlab/matlab/+scidb/schema_key.m` | MATLAB factory function |
| `scidb/tests/test_filters.py` | Construction tests (`TestSchemaKeyConstruction`) |
| `scidb/tests/test_schema_key_filter.py` | DB integration tests |
| `sci-matlab/tests/test_bridge_where.py` | Bridge tests (`TestSchemaKeyFilterBridge`) |

## RawFilter special behavior

`RawFilter` (created by `raw_sql()`) applies the SQL fragment to the **target** variable's own data table (not to the filter variable). This is the only filter type that works this way. It uses a latest-version CTE over the target and injects the raw SQL into the WHERE clause. DuckDB errors propagate wrapped as `ValueError("Invalid where= SQL: ...")`.

## Error messages

| Scenario | Error |
|----------|-------|
| Filter variable has no saved data | `"Filter variable 'X' is not registered. Save data to it first."` |
| Filter at finer level than target | `"Filter variable 'X' is stored at schema level 'y' which is finer than target 'Z' at level 'w'. Filters must be at the same or coarser level than the target."` |
| Filter missing coverage | `"Filter variable 'X' is missing data at N schema locations that 'Z' has data for. Ensure the filter variable covers all target locations."` |
| Raw SQL syntax error | `"Invalid where= SQL: {duckdb_error}"` |

## Key Files

| File | Role |
|------|------|
| `scidb/src/scidb/model/filters.py` | All filter classes + `raw_sql()` + `schema_key()` factories |
| `scidb/src/scidb/model/variable.py` | `VariableMeta` metaclass + `where=` in `load()` |
| `scidb/src/scidb/__init__.py` | Exports `raw_sql`, `schema_key` |
| `scidb/src/scidb/store/database.py` | `where=` in `DatabaseManager.load()` and `load_all_as_df()` — calls `resolve()` and filters the records DataFrame |
| `scirun-lib/src/scirun/column_selection.py` | Comparison operators on `ColumnSelection` |
| `scidb/tests/test_filters.py` | Unit tests (no DB required), incl. `TestSchemaKeyConstruction` |
| `scidb/tests/test_where.py` | Integration tests for variable-based filters |
| `scidb/tests/test_schema_key_filter.py` | Integration tests for schema key filters |
| `sci-matlab/src/sci_matlab/matlab/+scidb/SchemaKey.m` | MATLAB builder class |
| `sci-matlab/src/sci_matlab/matlab/+scidb/schema_key.m` | MATLAB factory function |
| `sci-matlab/tests/test_bridge_where.py` | Bridge tests, incl. `TestSchemaKeyFilterBridge` |

## MATLAB Implementation

### Filter construction (scidb.BaseVariable operators)

Operator overloads on `scidb.BaseVariable` produce `scidb.Filter` objects wrapping the corresponding Python filter:

```matlab
filt = Side() == "L";           % VariableFilter
filt = Side() ~= "L";           % negated VariableFilter
filt = ScalarVar() > 1.2;       % comparison VariableFilter
filt = (Side() == "L") & (ScalarVar() > 1.0);  % CompoundFilter AND
filt = (Side() == "L") | (Side() == "R");       % CompoundFilter OR
filt = ~(Side() == "L");        % NotFilter
filt = scidb.raw_sql('"value" > 0.70');         % RawFilter
```

**Column-specific filtering** (analogous to Python's `MyVar["col"]` syntax):

```matlab
filt = MyVar("col_a") ~= 0;     % ColumnFilter on "col_a" column
filt = MyVar("speed") > 1.5;    % ColumnFilter on "speed" column
filt = MyVar(["a","b"]) == 5;   % ColumnFilter on "a" (first column only)
```

When a `BaseVariable` instance is constructed with column selection (e.g., `MyVar("col_a")`), the comparison operators detect the `selected_columns` property and create a `ColumnFilter` instead of a `VariableFilter`. This matches Python's behavior where `MyVar["col_a"] == value` creates a `ColumnFilter`.

**Implementation note**: Multiple column selection (`MyVar(["a","b"])`) uses only the first column for filtering, consistent with Python's `ColumnSelection` behavior.

`scidb.Filter` holds a `.py_filter` property containing the Python filter object. The `&`, `|`, and `~` operators on `scidb.Filter` delegate to Python via `__and__`, `__or__`, `__invert__`.

Key files:
- `sci-matlab/src/sci_matlab/matlab/+scidb/Filter.m`
- `sci-matlab/src/sci_matlab/matlab/+scidb/BaseVariable.m` (comparison operators: lines 562-627)

### where= in load()

```matlab
StepLength().load(where=Side() == "L", subject=1, session='A')
StepLength().load(where=(Side() == "L") & (ScalarVar() > 1.0), version="all", db=db)
```

The filter is forwarded to Python as `where_filter.py_filter` via the bridge. Schema-level validation and coverage checks behave identically to the Python API.

## `where=` in `scidb.for_each` (MATLAB)

> ⚠️ **Stale below this point.** The MATLAB `preload` / `parallel` / `merge_constituents()`
> / `load_and_extract` machinery described in the next few subsections has been
> **removed**. MATLAB `scidb.for_each` is now a thin two-pass shell: it ships the
> live `where_filter.py_filter` to `py.sci_matlab.bridge.for_each_prepare`, and **all**
> input loading (including `where=` application and Merge constituent loading) happens
> in Python's `_for_each_prepare` → `_convert_inputs` → `_load_input`. `preload=` and
> `parallel=` are accepted but ignored (`split_options` in `+scidb/for_each.m`). The
> behavioral summary table and the corrected Merge subsection below are current; treat
> the `preload`/`parallel` prose as historical. See [[where-provenance-and-merge]].

### How it applies — the preload path (default, `preload=true`) — *historical*

For each non-Merge, non-PathInput input variable, `for_each` bulk-preloads all values for all iteration combos in a single Python call. The `where=` filter is passed to `load_and_extract`:

```matlab
bulk = py.sci_matlab.bridge.load_and_extract( ...
    py_class, py_metadata, ...
    pyargs('version_id', 'latest', 'db', py_db, 'where', where_filter.py_filter));
```

Results are stored in a `containers.Map` keyed by metadata. Any combo whose key is absent from the map is silently skipped.

### How it applies — the per-iteration path (`preload=false` or no metadata keys)

When preloading is disabled, the filter is passed directly to the per-iteration load call:

```matlab
loaded{p} = var_inst.load(load_nv{:}, db_nv{:}, where_nv{:});
```

Behavior is identical; any iteration where the load returns nothing is skipped.

### where= with `parallel=true`

Parallel mode also uses the preload path (Phase A is serial). The `where=` filter is applied to `load_and_extract` exactly as in the serial preload. Combos missing from the preloaded map are skipped before Phase B (parfor compute).

### where= with `scidb.Fixed` inputs

`Fixed` inputs are preloaded with their overridden metadata substituted in. The `where=` filter is applied to those pinned metadata bulk-loads too.

Practical implication: if `where=Side()=="L"` is used with `Fixed(Baseline, session='BL')`, the filter checks Side at `session='BL'` (not the iteration's session). Save Side data at all sessions that any input will be queried at, or the Fixed preload may yield empty results and skip the iteration.

### where= with `scidb.Merge` (filter IS applied — propagated to constituents)

> **History:** This used to say the filter was NOT applied to Merge. That is no
> longer true. The old MATLAB `merge_constituents()` path was removed; MATLAB
> `for_each` now delegates all input loading to the Python bridge, and the Python
> Merge loader (`_load_input`) propagates `where=` to every constituent.

The `where=` filter **is** applied to each `Merge` constituent. The constituent is
loaded exactly as a direct `.load(where=…)` would be — including selecting the
correct variant by `__where` provenance when a variable has several variants on the
same schema keys. Coverage is validated **once** against the Merge inner-join result
(`_compute_merge_effective_ids`), not per constituent, so a filter gap at a row the
join would drop does not raise a false-positive error.

See [[where-provenance-and-merge]] for the full mechanism (`_PreresolvedFilter`,
`_merge_constituent_where_key`, and the two-strategy `_load_with_where`).

```matlab
% where= IS applied to the Merge constituents:
scidb.for_each(@fn, struct('d', scidb.Merge(A(), B())), {Out()}, ...
    'subject', [1 2], where=Side() == "L");
% → Each constituent is filtered by Side()=="L"; if A or B has multiple
%   for_each-computed variants, only the one matching this where= is loaded.
```

### `where=` with Column Selection

**IMPORTANT**: Column selection has two distinct uses in MATLAB:

1. **For filtering** (in `where=` parameter): Creates a `ColumnFilter` that queries a specific column
2. **For inputs** (in `for_each` inputs): Extracts columns after loading

#### Using column selection in where= filters

When a column-selected variable is used in a comparison, it creates a `ColumnFilter`:

```matlab
% Creates ColumnFilter that queries "StepLengths_GR" column where value != 0
scidb.for_each(@fn, ..., where=GAITRiteData("StepLengths_GR") ~= 0)
```

This is implemented by checking the `selected_columns` property in `BaseVariable` comparison operators (eq, ne, lt, le, gt, ge). When `selected_columns` is set, a `ColumnFilter` is created instead of a `VariableFilter`. This matches Python's behavior where `MyVar["col"] == value` creates a `ColumnFilter`.

**Technical note**: The filter queries the database during the schema_id resolution phase, before any data is loaded.

#### Using column selection in inputs

When column selection is used for inputs (not in `where=`), it extracts columns **after** loading:

```matlab
% Loads full table first, then extracts "StepLengths_GR" column as input
scidb.for_each(@fn, struct('x', GAITRiteData("StepLengths_GR")), ...)
```

The `where=` filter is applied to the preload/load of the full table first; column narrowing happens after. The combination works correctly:

```matlab
% Filter queries "speed" column, then extracts "force" column for input
scidb.for_each(@fn, struct('x', GaitData("force")), {Out()}, ...
    where=GaitData("speed") > 1.5, ...)
```

### Summary table

| Input type | where= applied? | Path |
|------------|----------------|------|
| Plain `BaseVariable` | Yes | Preload bulk query or per-iteration `load()` |
| `scidb.Fixed(BaseVariable)` | Yes | Preload bulk query using fixed metadata |
| `BaseVariable("col")` | Yes | Preload bulk query, column narrowed after |
| `scidb.PathInput` | N/A | PathInput is not a DB load |
| `scidb.Merge(...)` | **Yes** | Propagated to each constituent; coverage validated once against the join result. See [[where-provenance-and-merge]] |
| Constant (scalar/table) | N/A | Not loaded from DB |

## What is NOT implemented

- **Version-specific filter**: The filter always uses "latest version" of the filter variable. There is no mechanism to specify a particular version of the filter variable.
- **Cross-database filters**: The filter variable must be in the same database as the target.
- **SchemaKey mixed into `for_each` `where=` + provenance**: when a `for_each` `where=`
  mixes a `SchemaKey` filter with variable filters, the saved `__where` key (full
  filter) and the load-time key (variable portion only) diverge, so provenance
  matching (Strategy 1) won't fire for that variant. SchemaKey is being moved out of
  `for_each` `where=` for this reason. See [[where-provenance-and-merge]].

## Related

- [[variant-branch-param-pinning]] — `Variant` threads a `branch_params_filter` through `_load_input` the same way `where=` is threaded, and the two now coexist on the `load_all_as_df` fast path.
