# Plan: `scifor.Merge.as_df()`

## Goal
Convert a `scifor.Merge` to the inner-joined pandas DataFrame, after the fact:
`merged = Merge(a, b); df = merged.as_df()`.

## API decision
- `pd.DataFrame(Merge(...))`: rejected — no clean hook (would need iterable or
  `__dataframe__` interchange protocol; both hacky, latter doesn't power the bare
  constructor anyway).
- `Merge(a, b, as_df=True)`: rejected — constructor can't return a different type,
  and Merge is a wrapper spec consumed by `for_each`; conflating roles breaks that.
- **`merged.as_df(where=None, verbose=False, **metadata)`**: chosen. Explicit,
  after-the-fact, mirrors `to_csv`.

## Refactor (DRY with to_csv)
`csv_export.py` already builds the joined DataFrame inside `export_merge_csv`.
Extract that into `merge_to_dataframe(merge, where=None, _log_fn=None, **metadata)`
returning the joined DataFrame. `export_merge_csv` becomes: validate `.csv`
filename (fail fast) → `merge_to_dataframe(...)` → `df.to_csv(filename, index=False)`.

## merge.py
Add `Merge.as_df(where=None, verbose=False, **metadata)` delegating to
`merge_to_dataframe` (lazy import). Same filter semantics as `to_csv`:
per-constituent metadata filters, post-join `where`, one copy of schema cols.

## Tests
Extend `tests/test_merge_to_csv.py` (or add `test_merge_as_df.py`):
- `as_df()` returns the same frame `to_csv` would write (columns, rows, one schema
  copy).
- `where=` and metadata filters on `as_df()`.
- `as_df()` does not require a `.csv` filename (no filename at all).

## Files touched
- `scifor/src/scifor/csv_export.py` (extract `merge_to_dataframe`)
- `scifor/src/scifor/merge.py` (add `as_df`)
- `scifor/tests/test_merge_as_df.py` (new)
