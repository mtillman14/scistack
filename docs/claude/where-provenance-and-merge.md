# `__where` Provenance and Merge Constituent Loading

## Why this exists

A single variable can have **multiple records at the same schema-key combination**
(same `subject`/`session`/`speed`/…) that differ only by *how they were computed*.
The most common source is `for_each(..., where=X)`: running the same function over
the same combos under different `where=` filters produces several **variants** that
all land on the same schema keys but carry different values.

Real example (the case that motivated this doc, 2026-05-30):

```matlab
% Three variants of DeltaStepLength, same (subject, session, speed) combos:
deltaSL_A  = scidb.for_each(@meanChangeFromReference, ..., where=noZerosGRFilter & UAStartFoot() == "A");
deltaSL_U  = scidb.for_each(@meanChangeFromReference, ..., where=noZerosGRFilter & UAStartFoot() == "U");
deltaSL_UA = scidb.for_each(@meanChangeFromReference, ..., where=noZerosGRFilter);
```

These cannot be told apart by schema keys alone — they *share* them. They are
distinguished by a stored **`__where` provenance key**.

## The `__where` version key

When `for_each` saves a computed variant, `ForEachConfig.to_config_keys()`
(`scidb/src/scidb/run/foreach_config.py`) writes the `where=` filter into the record's
`version_keys` JSON under the key `"__where"`:

```python
keys["__where"] = self.where.to_key()   # e.g. "(UAStartFoot == 'U') AND (GAITRiteLoadedCycle['StepLengths_GR'] != 0.0)"
```

This is provenance: it records *which filter produced this record*. It is stored
alongside the other version keys (`__fn`, `__fn_hash`, `__inputs`, `__constants`)
that together define a variant's identity.

## The two-strategy load: `_load_with_where`

`DatabaseManager._load_with_where` (`scidb/src/scidb/store/database.py`) is the single
engine that turns a `where=` filter into a set of records. It tries two strategies
**in order**:

### Strategy 1 — provenance match (fast path for for_each-computed data)

1. Split off any `SchemaKey` portion of the filter (those select *rows*, not
   variants — see below). Keep the **variable-level** portion.
2. Derive a key string from that portion via `_where_key_from_filter()` and look it
   up as `augmented["__where"]`.
3. Select records whose stored `version_keys["__where"]` **equals** that key.

If any records match, return them (after applying any `SchemaKey` portion as an
extra row selector). This is what makes `DeltaStepLength().load(where=UAStartFoot() == 'U' & ...)`
return exactly the one variant computed under that filter.

### Strategy 2 — schema-id fallback (backward compat)

If Strategy 1 matched nothing — e.g. the data was saved directly with `.save()` and
has no `__where` provenance — fall back to `where.resolve(...)`, which resolves the
filter to a set of `schema_id`s and selects records by schema id. This is the
"classic" filter behavior documented in [[where-filter-system]].

**Key insight:** Strategy 2 *cannot distinguish variants that share schema keys* —
all three `DeltaStepLength` variants resolve to the same `schema_id`s, so schema-id
filtering returns all of them. Only Strategy 1 (provenance) separates them.

## `_where_key_from_filter`: one source of truth

`_where_key_from_filter(where_for_key)` (`scidb/src/scidb/store/database.py`) is the
**single** function that maps a (variable-level) filter to its `__where` key string.
It handles `str`, `RawFilter._original_str`, anything with `.to_key()`, and falls
back to `str()`. Both the direct-load path (`_load_with_where`) and the Merge path
(`_merge_constituent_where_key` in `foreach.py`) call it, so they always agree on
the key. If you change key derivation, change it here — never duplicate the logic.

> ⚠️ **Save/load must agree.** `to_config_keys()` keys on the **full** filter; the
> load path keys on the **variable-level portion** (SchemaKey split off). For pure
> variable/column filters these are identical, so provenance matches. If a `SchemaKey`
> filter is mixed into a `for_each` `where=`, the stored and load-time keys diverge
> and Strategy 1 will *not* match (it falls through to Strategy 2). SchemaKey is being
> moved out of `for_each` `where=` for this reason (commit `81ad8ef`).

## Merge constituent loading

`scidb.Merge(A, B)` loads each constituent independently and column-joins them on
schema keys. The loading lives in `_load_input` (the `isinstance(var_spec, Merge)`
branch) in `scidb/src/scidb/run/foreach.py`.

The subtlety: each constituent must be filtered **exactly as a direct `.load(where=…)`
would be**, including provenance. The mechanism:

1. **Coverage is validated once, at the merge level** — `_check_merge_filter_coverage`
   against `_compute_merge_effective_ids` (the inner-join result), *not* per
   constituent. This avoids false-positive coverage errors when a constituent has
   "extra" rows the join would drop anyway.
2. **Provenance key computed once** — `_merge_constituent_where_key(where)` derives
   the shared `__where` key (via `_where_key_from_filter`).
3. **Each constituent is loaded** with a `_PreresolvedFilter(matching_ids, where_key=...)`:
   - `matching_ids` = `where.resolve(..., validate_coverage=False)` — the pre-resolved
     schema ids, used by Strategy 2.
   - `where_key` = the provenance key, returned by `_PreresolvedFilter.to_key()` so
     Strategy 1 can run.

### Why `_PreresolvedFilter` carries *both*

| Constituent | Has stored `__where`? | Which strategy fires |
|-------------|----------------------|----------------------|
| `DeltaStepLength` (for_each-computed) | Yes | **Strategy 1** — selects the one matching variant |
| `SubjectGrouping` (raw `.save()`) | No | **Strategy 2** — uses pre-resolved schema ids |

A single `Merge(SubjectGrouping, DeltaStepLength)` exercises both at once. Carrying
both pieces of information lets each constituent take the correct path.

### The bug this prevented (2026-05-30)

Before the fix, `_PreresolvedFilter.to_key()` returned `""`. That made
`augmented["__where"]` empty, so **Strategy 1 was skipped for every Merge
constituent** and everything fell through to schema-id filtering (Strategy 2). For a
variable with multiple variants on the same schema keys, *all* variants leaked
through — the user saw three rows per schema-key combo from `Merge`, even though a
direct `DeltaStepLength().load(where=...)` correctly returned one. The fix gives
`_PreresolvedFilter` a real `where_key` so the Merge path matches provenance just
like direct load.

## MATLAB and Python are the same code path

This logic is **Python-only**. MATLAB's `scidb.for_each` (`+scidb/for_each.m`) is a
thin two-pass shell: it ships the live Python `Filter` object
(`where_filter.py_filter`) to `py.sci_matlab.bridge.for_each_prepare`, which calls
Python `_for_each_prepare` → `_convert_inputs` → `_load_input`. MATLAB receives the
already-loaded DataFrames and only runs the inner loop. Likewise
`DeltaStepLength().load(where=…)` in MATLAB delegates to the same `_load_with_where`.

Consequence: because both APIs hand the *same* `Filter` object through the *same*
`_where_key_from_filter`, **Merge selects exactly the variant a direct `.load()`
selects, in both languages.** There is no separate MATLAB implementation to keep in
sync.

## Key files

| File | Role |
|------|------|
| `scidb/src/scidb/run/foreach_config.py` | `to_config_keys()` writes `__where` at save time |
| `scidb/src/scidb/store/database.py` | `_where_key_from_filter()` (source of truth); `_load_with_where()` Strategy 1/2 |
| `scidb/src/scidb/run/foreach.py` | `_PreresolvedFilter`, `_merge_constituent_where_key`, Merge branch of `_load_input` |
| `scidb/src/scidb/model/filters.py` | `split_schema_key_filters()`, filter `to_key()`/`resolve()` |
| `sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m` | Thin shell delegating to the Python bridge |
| `sci-matlab/src/sci_matlab/bridge.py` | `for_each_prepare` → `_for_each_prepare` |

## Tests

| File | What it covers |
|------|----------------|
| `scidb/tests/test_variable_filter_merge.py::TestMergeSelectsVariantByProvenance` | Python: two variants on shared schema keys, Merge selects one by provenance |
| `sci-matlab/tests/matlab/scidb/TestMerge.m::test_merge_selects_variant_by_where_provenance` | MATLAB (through the bridge): same scenario, both variants |
| `sci-matlab/tests/matlab/scidb/TestForEachWhere.m` | MATLAB `where=` on Merge generally (filter propagated to constituents) |

## Related

- [[where-filter-system]] — the general `where=` filter system (Strategy 2 / schema-id resolution, filter class hierarchy, SchemaKey filters).
- [[scidb-for-each-internals]] — the broader for_each prepare/loop/save pipeline.
