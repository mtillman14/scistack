# Plan: `for_columns` column-wise iteration in `for_each`

## Goal

Let a user iterate `for_each` over each column of a wide-table variable, feeding one
column at a time to the function, and reassemble the per-column results into a single
output variable whose data (per schema combo) is a `1 × N` table with the **same column
names** as the source.

Replaces the inelegant pattern of one `for_each` call (and one `str2var` output variable)
per column. `str2var` is no longer involved — there is exactly one output variable.

## Confirmed decisions

- **API:** reuse `ColumnSelection` with a new `iterate` flag. Construct iterate-mode via a
  classmethod `BaseVariable.for_columns([...])`; no args (`for_columns()`) ⇒ all columns.
  No new top-level abstraction (no `EachColumn`).
- **Output shape:** `fn` returns a scalar per column; per combo the saved output is a
  `1 × N` DataFrame whose columns mirror the iterated source columns.
- **Multi-input pairing:** zip **by column name**. All `for_columns` inputs in one call
  must share the same resolved column set, else error.

## Why not `EachOf`

`EachOf` was considered (user's first instinct) but mismatches on two axes:
1. It fans **out** into N separately-saved variant records; the goal is a fan-**in** to one
   reassembled record/variable.
2. Its axes are a **cartesian product**; `for_columns` needs a single **shared/zipped**
   axis across `baseline` + `value`.

## Why wrap-the-function (chosen) instead of expand-and-remerge

`for_each` saves whatever `fn` returns as the output's `.data` per combo. If we wrap `fn`
to loop over columns and return the assembled `1 × N` row, the **existing** save path
produces exactly one output variable with one record per combo — no custom merge/save, no
`save=False` inner passes. This reuses loading, `where=`, `Fixed`, caching, and save
unchanged.

Tradeoff: per-column function calls are hidden inside the wrapper, so lineage/scihist
records one combined call per combo rather than N. This matches the "one variable" intent;
called out as an accepted consequence.

## Implementation steps

### 1. `ColumnSelection` (scidb/src/scidb/column_selection.py)
- Add `iterate: bool = False` and allow `columns: list[str] | None` (None ⇒ all columns,
  resolved at run time).
- `__name__`, `to_key()`, `__hash__` include `iterate` and the (possibly resolved) columns
  so changing the iterated set invalidates cached results.
- Iterate-mode is a *pass-as-loop* selection, distinct from today's *pass-subset-as-one-arg*.

### 2. `BaseVariable.for_columns(...)` (scidb/src/scidb/variable.py)
- `classmethod for_columns(cls, columns=None)` → `ColumnSelection(cls, columns, iterate=True)`.
- Keep `__class_getitem__` unchanged (still produces `iterate=False`).

### 3. Column resolution + validation (top of `for_each`, foreach.py)
- Scan `inputs` (including inside `Fixed`) for `ColumnSelection(iterate=True)`.
- Resolve `columns is None` ⇒ all columns, via a single lightweight representative load /
  schema peek, so version_keys are stable before the loop.
- Validate all iterate inputs resolve to the **same** ordered column set (zip-by-name);
  error with a clear message otherwise.
- Per NOTE 2 in CLAUDE.md: add `Log.info` lines (resolved column list, count, which inputs
  participate) mirroring the existing "Step 1" EachOf logging.

### 4. Load full tables for iterate inputs
- In the loading loop, when a `ColumnSelection` has `iterate=True` (bare or inside `Fixed`),
  load the **full** table (do NOT extract a single column). Touch points:
  `_is_loadable`, the `ColumnSelection` unwrap at load (~foreach.py:1678, :2060-2110),
  and the `Fixed`+`ColumnSelection` combination.

### 5. Wrap `fn` for column-wise loop
- Add a wrapping stage (near the existing PerComboLoader/lineage wrap, ~foreach.py:343).
- Per combo, for each resolved column `c`: slice every iterate input to `c` (single
  column → array, matching today's single-column return type), call the user `fn` with the
  sliced iterate inputs plus untouched non-iterate inputs, collect `result_c`.
- Assemble and return a `1 × N` DataFrame `{c: result_c}` in resolved column order.
- Ordering must compose correctly with the lineage tuple-unpacking wrapper.

### 6. Save (unchanged)
- Existing Step 19 save persists the `1 × N` DataFrame as the single output variable's data
  per combo. Verify no code assumes scalar-only fn returns on this path.

### 7. `dry_run` display
- Extend `_convert_inputs_for_display` / `_input_type_name` so iterate-mode prints e.g.
  `ColumnSelection(GAITRiteLoadedCycle, [..], iterate=True)` and dry-run reports the column
  count it would loop over.

### 8. MATLAB parity (sci-matlab/.../+scidb/BaseVariable.m, for_each.m)
- Add `for_columns(...)` returning a selection with the iterate flag; teach
  `apply_column_selection`/load path to load the full table and loop in iterate-mode,
  assembling the wide output table. Mirror Python semantics and validation.

### 9. Exports / docs
- No new public class to export (reuses `ColumnSelection`).
- New doc `docs/claude/for-columns-iteration.md` describing the feature, the wrap-fn
  mechanism, and the lineage-granularity tradeoff.

## Tests (Python; user runs them — no Python in assistant env)
- Single iterate input over an explicit subset; output column names/order preserved.
- `for_columns()` (all columns) resolution.
- Two iterate inputs (`baseline` Fixed + `value`) zipped by name; mismatched column sets ⇒
  error.
- Mixed iterate + non-iterate (constant / plain var) inputs.
- `where=` filter still applied per combo under iteration.
- Caching: changing the column set / `iterate` flag invalidates; identical re-run is a hit.
- `dry_run` reports the loop without executing/saving.

## Open risks / to verify during implementation
- Save path assuming scalar fn returns (step 6).
- Column-set drift across combos (some combos missing a column) — resolved set is fixed up
  front; decide error vs NaN-fill (default: error, surfaced clearly).
- Interaction with `as_table` and `distribute` when an iterate input is present.
