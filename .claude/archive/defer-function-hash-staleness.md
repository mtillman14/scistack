# Function-hash staleness: Python enabled, MATLAB deferred

## Current State (updated)

Function-hash staleness is now **enabled for Python `LineageFcn`** and
**deferred for MATLAB `MatlabLineageFcn`**.

In `scihist-lib/src/scihist/state.py:_check_via_lineage`:

- When `fn` is a Python `scilineage.LineageFcn` and `stored_hash != fn.hash`,
  the combo is returned as `"stale"`. Python bytecode hashing is reliable
  because the same `__code__` object is used at both save-time and check-time.
- When `fn` is a MATLAB proxy (`MatlabLineageFcn`) and hashes differ, the
  mismatch is logged but **not** treated as stale. The MATLAB hashing
  pipeline can produce false mismatches (see Background below).

## Background (MATLAB false stale reports)

`scidb.log` showed 15 successfully-saved combos immediately reported as stale:

```
node load_csv: red (up_to_date=0, stale=15, missing=1)
stale: subject=XX, trial=YY — function hash changed (lineage)
```

The stale branch fires when `stored_hash != fn.hash`:

- `stored_hash` — `function_hash` column in `_lineage`, written at save time
  from `MatlabLineageFcnInvocation.fcn.hash`.
- `fn.hash` — freshly computed `MatlabLineageFcn.hash` built by the GUI at
  state-check time from `matlab_parser.parse_matlab_function`'s `source_hash`.

The user confirmed (interactively) that both hash recipes agree on the
same file: GUI raw-bytes SHA-256 == MATLAB `fileread`+utf-8 SHA-256 ==
`387621759246...`, and both the GUI proxy and a live MATLAB proxy compute
`ce634fb42246...` from it. So the two sides no longer disagree at recipe
level, yet the stale check still fires on just-saved rows.

Until we actually see what is in `_lineage.function_hash` for those 15
rows, we cannot explain the false stale.

## Out of scope (future)

- Enabling content-staleness for MATLAB functions, pending a robust way
  to ensure save-time and check-time hash recipes produce identical
  values for unchanged `.m` files.
- Tokenized-source hashing resilient to comment/whitespace edits.
- Per-Run snapshots captured at the GUI layer.
