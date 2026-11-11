# Plan: `introspect=True` flag for `load()` and `for_each()`

## Motivation: what questions does this answer?

Users currently have no way to interrogate the internal state of a `load()` or
`for_each()` result without querying the database directly. Here are the real
questions this feature answers:

**For `load()`**
- "Which exact database record did this result come from?" → `.record_id` / `record_id` column
- "I have two computational variants of this variable (different branch_params). Which one did I just load?" → `.branch_params` / `branch_params` column
- "Did the `where=` filter I passed actually apply?" → `.where` / `where` column (echoes what was passed in)
- "What function and constants produced this data?" → `.metadata` / `version_keys` column
- "What is the content hash of this record?" → `.content_hash` / `content_hash` column

**For `for_each()`**
- "For row (subject=1, session='A') — which specific input record was used?" → `_record_id_signal` column
- "My input has two branch_params variants. Which one fed into each output row?" → `_branch_params_signal` column
- "Was my `where=` filter active?" → `_where` column
- "What is the version/call key for this computation?" → `_call_id` column

---

## Design principles

1. **Return type is unchanged.** `load()` returns the same `BaseVariable`, list, or
   DataFrame it always did. `for_each()` returns the same DataFrame. `introspect=True`
   only enriches what's already returned — adding attributes to `BaseVariable`
   instances, or adding columns to DataFrames.

2. **Column order: schema → data → introspection.** Introspection columns are always
   appended to the right of the existing columns. The existing left-to-right order
   (schema keys first, then data/output columns) is never disturbed.

3. **Columns, not attrs, for DataFrame results.** Repeated values down a column are
   preferable to `df.attrs` because columns survive `pd.concat` (fixing the EachOf
   limitation) and are more visible and discoverable.

---

## Part 1: `BaseVariable.load(introspect=True)`

### Updated signature

```python
@classmethod
def load(
    cls,
    as_df: bool = False,
    version: str = "latest",
    where=None,
    db=None,
    introspect: bool = False,   # NEW
    **metadata,
)
```

`introspect=True` and `as_df=False` are compatible — return type stays `BaseVariable`
(or `list[BaseVariable]`).
`introspect=True` and `as_df=True` are compatible — return type stays `pd.DataFrame`,
extra columns added.

### Case A: `as_df=False, introspect=True`

`BaseVariable` instances already have `.record_id`, `.metadata`, `.branch_params`,
`.content_hash`, `.lineage_hash` populated by the loader (database.py:2826–2832).
The only fields that are **not** currently set on the instance are the call context:
which `where=` filter was applied and which `version` mode was used.

`introspect=True` adds two new attributes to each returned `BaseVariable`:
- `var.where` — the `where=` argument passed to `load()` (the filter object itself,
  or `None` if none was passed)
- `var.version_mode` — the `version=` string that was passed (e.g. `"latest"`,
  `"all"`, or a specific record_id string)

All other introspection data (`.record_id`, `.branch_params`, `.metadata`,
`.content_hash`) was already accessible on the instance regardless of `introspect=`.

**The `introspect=` flag's value for this path is discoverability:** it signals to
the user "this object has rich metadata attached" and makes the call context
(.where, .version_mode) available without the user having to track those values
themselves.

### What's on the `BaseVariable` instance after load

| Attribute | Type | Always set? | Description |
|-----------|------|-------------|-------------|
| `.data` | Any | yes | The payload |
| `.record_id` | `str \| None` | yes | e.g. `"3f2504e0-4f89-11d3..."` |
| `.metadata` | `dict \| None` | yes | Flat mix of schema keys + `__`-prefixed version keys |
| `.branch_params` | `dict` | yes | e.g. `{"cutoff_hz": 100}` or `{}` |
| `.content_hash` | `str \| None` | yes | SHA-256 of stored content |
| `.lineage_hash` | `str \| None` | yes | Lineage tracking hash |
| `.where` | Any | **only with `introspect=True`** | The `where=` filter passed to load() |
| `.version_mode` | `str` | **only with `introspect=True`** | `"latest"`, `"all"`, or specific record_id |

### Example usage (non-df)

```python
var = StepLength.load(subject=1, session="A", introspect=True)

# Already always accessible:
print(var.data)            # np.array([0.82, 0.91, ...])
print(var.record_id)       # "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
print(var.metadata)        # {"subject": 1, "session": "A", "__fn": "compute_step_length", ...}
print(var.branch_params)   # {"cutoff_hz": 100, "filter_type": "butter"}
print(var.content_hash)    # "e3b0c44298fc1c14..."

# Added by introspect=True:
print(var.where)           # None (no filter passed)
print(var.version_mode)    # "latest"

# With where= filter:
var = StepLength.load(subject=1, introspect=True,
                      where=StepLength["value"] > 0)
print(var.where)           # <ColumnFilter: StepLength['value'] > 0>
```

### Case B: `as_df=True, introspect=True`

The returned DataFrame gets additional columns beyond what `as_df=True` normally
returns:

| Column | Type | Source |
|--------|------|--------|
| `record_id` | `str` | `var.record_id` |
| `branch_params` | `dict` | `var.branch_params` |
| `content_hash` | `str \| None` | `var.content_hash` |
| `version_keys` | `str` (JSON) | The `__`-prefixed entries from `var.metadata`, serialized |
| `where` | `str` | `repr(where)` if `where=` was passed, else `None` (repeated on all rows) |
| `version_mode` | `str` | `"latest"` / `"all"` / record_id string (repeated on all rows) |

Values are repeated on all rows for the call-level fields (`where`, `version_mode`),
consistent with how `for_each(introspect=True)` works.

### Example usage (as_df)

```python
df = StepLength.load(subject=1, as_df=True, introspect=True)
print(df.columns)
# Index(['subject', 'session', 'StepLength',             ← schema, then data (unchanged)
#        'record_id', 'branch_params', 'content_hash',   ← introspection appended
#        'version_keys', 'where', 'version_mode'])

print(df[["subject", "session", "record_id", "branch_params"]])
#   subject session  record_id                          branch_params
# 0       1       A  3f2504e0...  {'cutoff_hz': 100, 'filter': 'butter'}
# 1       1       B  1a2b3c4d...  {'cutoff_hz': 100, 'filter': 'butter'}
```

---

## Part 2: `scidb.for_each(introspect=True)`

### Updated signature

```python
def for_each(
    fn,
    inputs,
    outputs,
    dry_run=False,
    save=True,
    as_table=None,
    db=None,
    distribute=False,
    where=None,
    introspect=False,   # NEW
    ...
)
```

### Return type

Unchanged — same DataFrame. When `introspect=True`, the following extra columns
are added. All values are repeated down every row.

#### Per-input columns (one pair per DB-backed input)

Added for variable types and Fixed wrappers. **Not** added for constants, PathInput,
or Merge (see limitation below).

| Column | Type | Description |
|--------|------|-------------|
| `_record_id_{param}` | `str` | record_id of the specific DB record used for this row |
| `_branch_params_{param}` | `dict` | branch_params of that record, e.g. `{"cutoff_hz": 100}` |

Source: the `__rid_{param}` columns already present in `result_tbl` after Step 17
(scifor includes them because they're in the extended schema from Step 15), renamed
and joined against `state.rid_to_bp`.

#### Call-level columns (repeated on every row)

| Column | Type | Description |
|--------|------|-------------|
| `_call_id` | `str` | 16-char hex, stable ID for this call site |
| `_config_keys` | `str` (JSON) | Full version key dict: `{"__fn": ..., "__fn_hash": ..., "__inputs": ..., "__constants": ..., "__where": ..., "__schema_overrides_hash": ...}` |
| `_where` | `str \| None` | `repr(where)` if `where=` was passed, else `None` |

Using columns (not `df.attrs`) means these survive `pd.concat`, which fixes the
EachOf limitation noted in the previous plan version.

### Example output

```python
result = scidb.for_each(
    process_signal,
    inputs={"signal": RawSignal, "cutoff_hz": 100},
    outputs=[ProcessedSignal],
    subject=[1, 2],
    session=["A", "B"],
    where=RawSignal["quality"] > 0.5,
    introspect=True,
)

print(result.columns)
# Index(['subject', 'session', 'ProcessedSignal',         ← schema, then output (unchanged)
#        '_record_id_signal', '_branch_params_signal',    ← introspection appended
#        '_call_id', '_config_keys', '_where'])

print(result[["subject", "session", "_record_id_signal", "_branch_params_signal"]])
#   subject session  _record_id_signal               _branch_params_signal
# 0     "1"     "A"  "3f2504e0-4f89..."  {"filter_type": "butter", "cutoff_hz": 100}
# 1     "1"     "B"  "1a2b3c4d-5e6f..."  {"filter_type": "butter", "cutoff_hz": 100}
# 2     "2"     "A"  "9z8y7x6w-5v4u..."  {}
# 3     "2"     "B"  "3c4d5e6f-7g8h..."  {}

print(result["_call_id"].iloc[0])   # "a1b2c3d4e5f60718" (same on every row)
print(result["_where"].iloc[0])     # "RawFilter(quality > 0.5)" (same on every row)

# With Fixed baseline:
result = scidb.for_each(
    compute_change,
    inputs={"baseline": Fixed(RawSignal, session="BL"), "value": RawSignal},
    outputs=[ChangeScore],
    subject=[1, 2], session=["A", "B"],
    introspect=True,
)
# Columns include:
#   _record_id_baseline, _branch_params_baseline  (same value on every row — one Fixed record)
#   _record_id_value,    _branch_params_value      (varies per row)
```

### Merge inputs — documented limitation

Merge inputs have their constituent `__record_id` columns stripped during bulk
loading (foreach.py:1444). `introspect=True` therefore adds **no** `_record_id_*`
or `_branch_params_*` columns for Merge-typed inputs. If per-record traceability
is needed for Merge constituents, use separate inputs instead.

### Implementation: `_apply_introspect(result_tbl, state, where)`

Called in the main `for_each()` body after `_for_each_save_resolved` returns,
only when `introspect=True` and `result_tbl` is non-None and non-empty.

1. Separate `__rid_{param}` columns from the rest of `result_tbl`. Drop them from
   their current positions; they will be re-added at the right end.
2. For each such column, build `_record_id_{param}` (renamed) and
   `_branch_params_{param}` (mapped through `state.rid_to_bp`).
3. Assemble the final column order:
   - All non-`__rid_*` columns from `result_tbl` (schema + output, order preserved)
   - `_record_id_{param}`, `_branch_params_{param}` pairs (one pair per input, in
     the order they appear in `inputs`)
   - `_call_id`, `_config_keys`, `_where`
4. Return enriched DataFrame (copy, not in-place)

The helper is private and lives in `scidb/foreach.py`.

The EachOf path threads `introspect=introspect` to each recursive call. Since
introspect enrichment happens inside each recursive call, the `_record_id_*`,
`_branch_params_*`, `_call_id`, `_config_keys`, and `_where` columns will all
survive `pd.concat` naturally.

---

## Files changed

| File | Change |
|------|--------|
| `scidb/src/scidb/variable.py` | Remove `include_record_id=`; add `introspect=False` to `BaseVariable.load()`; set `.where` and `.version_mode` on returned instances; add extra columns when `as_df=True` |
| `scidb/src/scidb/foreach.py` | Add `introspect=False` to `for_each()`, thread to EachOf path, add `_apply_introspect()` helper |
| `scidb/src/scidb/__init__.py` | No change needed (no new exported types) |
| `scidb/tests/test_introspect.py` | Integration tests (see below) |

No new files needed — no new exported types since return types are unchanged.

---

## Tests (`scidb/tests/test_introspect.py`)

### `load(introspect=True)` — non-df
1. Returns `BaseVariable` (single) or `list[BaseVariable]` unchanged
2. `.where` is `None` when no filter passed; equals filter object when passed
3. `.version_mode` is `"latest"` by default; `"all"` when passed
4. `.record_id`, `.branch_params`, `.metadata`, `.content_hash` are populated
   (they always are, but verify with introspect=True to confirm the path works)
5. Branch_params round-trip: save two variants, load each with `introspect=True`,
   verify `.branch_params` distinguishes them

### `load(introspect=True, as_df=True)`
1. Returns DataFrame with extra columns: `record_id`, `branch_params`,
   `content_hash`, `version_keys`, `where`, `version_mode`
2. `record_id` column values are non-empty strings
3. `branch_params` column contains dicts
4. `where` column is `None` when no filter; `repr(filter)` when filter passed
5. `version_mode` is repeated correctly on all rows

### `for_each(introspect=True)`
1. Returns DataFrame with `_record_id_{input}` and `_branch_params_{input}` columns
2. `_record_id_*` values are non-empty strings
3. `_branch_params_*` column contains dicts
4. `_call_id` is a 16-char hex string, same on every row
5. `_config_keys` is valid JSON string containing `__fn`, `__fn_hash`
6. `_where` is present (non-None) when `where=` was passed; `None` otherwise
7. Fixed input: `_record_id_baseline` present and same on every row
8. **Default behavior unchanged**: `introspect=False` produces no `_record_id_*`,
   `_branch_params_*`, `_call_id`, `_config_keys`, `_where` columns
9. EachOf: `_record_id_*` and `_call_id` columns survive across EachOf concat
