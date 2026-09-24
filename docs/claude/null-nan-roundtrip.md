# NULL ≡ NaN: how a missing number survives the round trip

*Written 2026-09-15, from the "grSides sees zeros, loadGaitRiteOneFile sees
NaN" investigation. Concerns `sciduckdb` (the owner), `scidb`'s two load
seams, and the MATLAB bridge. Plan:
`.claude/plan-null-list-elements-to-nan.md`.*

## The contract

**A NaN in memory is a NULL in storage, and a NULL in storage is a NaN in
memory.** One rule, both directions, for every shape:

| in memory | column type | stored as |
|---|---|---|
| `float('nan')` scalar | `DOUBLE` | `NULL` |
| `np.array([nan, 0.49])` | `DOUBLE[]` | `[NULL, 0.49]` |
| `np.array([[1, nan]])` | `DOUBLE[][]` | `[[1, NULL]]` |

Scalars always obeyed this: DuckDB's pandas scanner writes NaN as NULL, and a
fetch turns a NULL `DOUBLE` back into NaN because pandas has nowhere else to
put it. **List elements did not**, and that was the bug.

## The bug it replaced

DuckDB returns a LIST holding NULL elements as a **`numpy.ma.MaskedArray`**:
real values in `.data`, a boolean `.mask` marking the NULL slots, and an
arbitrary fill (zeros) underneath the mask.

`np.asarray(masked_array, dtype=…)` **silently drops the mask** and hands back
the fill buffer. So:

```
MATLAB writes  [NaN 0.49146 0.45202]
  → stored     [NULL, 0.49146, 0.45202]      (correct)
  → loaded     [0.0,  0.49146, 0.45202]      (WRONG — the mask was dropped)
```

That one `np.asarray` in `storage_to_python` sat under **every** load path, so
the corruption was uniform and invisible: `load()`, `load_all_as_df`, the
`for_each` spread load, and both MATLAB bridge routes all funnel through it.

Observed 2026-09-15: `loadGaitRiteOneFile` produced `GAITRiteLoaded` with NaN
for the first step length (no preceding footfall); `grSides`, one hop
downstream, received `0` and computed symmetry against it. Nothing errored.

### Why it looked like a variant-pinning problem

It didn't reproduce in the producer, only in the consumer, which is the shape
of a "wrong record was selected" bug. Three log lines ruled that out:

- `save_batch(GAITRiteLoaded): 420 items (0 new rows, …)` — a re-run wrote
  nothing new, so the consumer's bytes ARE the producer's bytes. (`record_id`
  is a content hash and `NaN` hashes differently from `0`, so this line is
  proof, not a hint.)
- `_find_record(GAITRiteLoaded, latest): … returned 420 rows` from 980, with
  no "locations retain >1 record after collapse" WARN — exactly one record per
  location was selected.
- No `x__vsig` group difference in the values themselves.

**Rule of thumb:** when a downstream function disagrees with an upstream one
about *values* at the same location, check `0 new rows` first. It separates
"wrong record" from "right record, wrong bytes" in one line.

## Where the fix lives

`sciduckdb.sciduckdb._array_from_storage` — one helper, called from every
`ndarray`/`list`-of-array branch of `storage_to_python`:

```python
if isinstance(value, np.ma.MaskedArray) and np.ma.is_masked(value):
    if dtype.kind != "f":
        dtype = np.dtype("float64")      # int/bool cannot carry NaN
    return np.ma.filled(value.astype(dtype), np.nan)
return np.asarray(value, dtype=dtype)
```

Two consequences worth knowing:

- **Integer and boolean array columns upcast to float64 when they hold a
  NULL.** There is no other dtype that can express "missing" — a `BIGINT[]`
  with a NULL element comes back `float64` with a NaN. Logged at DEBUG.
- **Genuine zeros are untouched.** Only *masked* slots are filled; an
  unmasked `0.0` stays `0.0`. Pinned by
  `test_a_real_zero_is_still_a_zero` / `test_real_zero_stays_zero`.

### Restoration must be idempotent

`SciDuck.load` restores each cell **twice** — `_restore_types` walks every
column, then the `multi_column` / `single_column` branch calls
`storage_to_python` again on the same cell. That was harmless while
restoration was a pure re-cast, and stopped being harmless the moment the
upcast existed: the second pass sees a plain `float64` array whose *declared*
dtype is still `int64`, and `np.asarray([1., nan], dtype=int64)` casts the NaN
to `INT_MIN` **silently**. The upcast was undone one line after it happened
(caught by `test_int_array_with_sql_null_upcasts`, 2026-09-15).

So `_array_from_storage` also refuses to cast a NaN-bearing float array into
an integer dtype, whatever the declared type says. That guard is worth having
on its own: any `BIGINT[]` column that acquires a NULL — by hand-written SQL,
by import, by a schema change — would otherwise deserialize as garbage rather
than as an error.

The double restoration itself is still there and is redundant work; removing
it is a separate change, and idempotence is the property that makes it safe
either way.

Storage was deliberately **not** changed: NULL stays the stored form (SQL
aggregates skip NULL, which is what a query over step lengths wants, and no
migration of existing databases is needed).

## Hash stability

The content hash is computed on the in-memory array, i.e. on NaN. Before the
fix, `save → load → save` produced a *different* record_id the second time
(NaN out, 0 back in), so an idempotent re-run silently wrote a second record
at the same location. After the fix the round trip is a fixed point. Pinned by
`test_reload_hashes_the_same` and `test_nan_roundtrip_is_hash_stable`.

## Reading it in `scidb.log`

| line | layer | meaning |
|---|---|---|
| `array column(s) contain NaN — stored as NULL, reload as NaN: {col: n}` | sciduckdb, INFO | save side: this many NaN went in |
| `load_all_as_df(T): restored N NULL list element(s) as NaN in M column(s): {…}` | scidb, INFO | bulk load (the `for_each` path) restored them |
| `load(T, <rid>): restored NULL list element(s) as NaN: {…}` | scidb, DEBUG | per-record load; DEBUG because a bulk MATLAB load would emit one per record |
| `_restore_types: restored NULL list element(s) as NaN: {…}` | sciduckdb, DEBUG | the non-DataFrame mode path |

If the save line reports NaN and no load line reports a restore, the NULLs
never came back as masked arrays — that is the regression this doc exists for.

## Inspecting storage by hand

Two traps, both hit during the investigation:

1. **`GAITRiteLoaded` is a VIEW, not the table.** `_create_variable_view`
   defines it as `GAITRiteLoaded_data LEFT JOIN _record LEFT JOIN _schema`.
   The base table (what the loader reads) is `<Type>_data`. `rowid` exists
   only on the base table; the view's joins may also reorder rows.
2. **`scidb show --values` truncates** the preview to one line, so a column
   late in the frame is never visible. Use `scidb sql` against `<Type>_data`
   for a specific column.

3. **`scidb sql` used to print a NULL as `nan`.** It fetched through
   `_fetchdf`, and pandas has no NULL for a float column, so every NULL
   `DOUBLE` arrived as NaN — which made a NULL indistinguishable from a
   stored NaN in the one command whose job is to report what is stored. It
   cost a wrong hypothesis (a save-path difference between the "normal" and
   "flatten" batch paths) that a follow-up query disproved: `n_null=212,
   n_nan=0`. `Inspector.sql` now uses `SciDuck._fetch_table`, which keeps
   NULL as `None`, and the CLI prints the word `NULL`.

**There is exactly one stored form.** A missing number is a NULL, whichever
save path wrote it; `count(*) FILTER (WHERE isnan(col[1]))` returns 0 across
the real data. If that ever stops being true, the contract at the top of this
document is what broke.

`scidb sql` also renders masked list elements as `NULL` rather than numpy's
`--` (which reads like data).

```
scidb sql "SELECT rowid, record_id, L_StepLengths_GR FROM GAITRiteLoaded_data
           WHERE record_id = '…'"
```

Row order within one record is insertion order in practice (verified:
`rowid` 0,1,2 for a 3-row record) but is **not** guaranteed — there is no
row-index column and the load SQL has no `ORDER BY`. See the plan's Stage 6.

## Tests

- `sciduckdb/tests/test_null_list_roundtrip.py` — the deserialiser itself
  (masked float/int/2-D/list-of-array), both counters, and save→load through
  DuckDB including a NULL written by raw SQL into a `BIGINT[]`.
- `scidb/tests/test_nan_array_roundtrip.py` — `load()`, the `for_each` input
  path, real-zero preservation, hash stability.
- `scimatlab/tests/matlab/scidb/TestTableRoundTrip.m` —
  `test_nan_in_ragged_vector_column_survives`, `test_real_zero_stays_zero`,
  `test_nan_roundtrip_is_hash_stable` (the MATLAB end of the same contract).
