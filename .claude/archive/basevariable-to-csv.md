# Plan: `BaseVariable.to_csv()` table export

## Goal
Add a class-level `to_csv()` that loads a variable across all matching
schema_ids and writes a flat table: one row per loaded record, one column per
schema key, plus a single value column. Scalar-only; error otherwise.

## Signature
```python
@classmethod
def to_csv(cls, filename: str, **kwargs) -> None
```
- `filename` is a **required positional** first arg. Must end in `.csv` or raise `ValueError`.
- `**kwargs` are forwarded verbatim to `cls.load(as_df=True, **kwargs)`, so it
  supports everything `load()` does:
  - `where=` → `scidb` Filter objects (ColumnFilter/VariableFilter, `&`/`|`/`~`)
  - branch-param kwargs → `Variant` semantics (load splits non-schema keys into `branch_params_filter`)
  - `version=`, `db=`, and schema metadata kwargs
  - `as_df`/`introspect` are popped (to_csv owns output shape).

## Behavior
1. Validate `filename` is a str ending in `.csv`.
2. `df = cls.load(as_df=True, **kwargs)` — packed layout: schema-key columns + `data`.
   Propagates `NotFoundError` when nothing matches.
3. Validate every `data` cell is scalar (`None`, Python/numpy number, str, bytes, bool).
   Reject `np.ndarray` (DOUBLE[]/DOUBLE[][]), `list`/`tuple`/`dict`,
   `pd.DataFrame`/`pd.Series` (multi-row table) → `ValueError` naming the schema_id
   and offending type.
4. Build output: schema-key columns (in `dataset_schema_keys` order, only those
   present) + value column named `cls.__name__`.
5. Log an INFO line; write with `index=False`.

## Replaces
The existing **instance** `to_csv(self, path)` (dumps `self.to_db()`), which is
unused elsewhere in the repo. Replaced by the classmethod.

## Expansion: Merge + ColumnSelection (multi-column)
Revised constraint per user: **one row per schema_id**, but multiple *columns*
are allowed/expected. A single-row table writes one column per table column;
multi-row tables and bare vectors still error.

Shared core lives in `scidb/src/scidb/csv_export.py` (`export_csv` /
`build_flat_table` / `_resolve_constituent` / `_expand_data` / `is_scalar_value`).
Three Python entry points delegate to it: `BaseVariable.to_csv` (classmethod),
`ColumnSelection.to_csv`, `Merge.to_csv`. `_resolve_constituent` also unwraps
`Fixed` (metadata override) and `Variant` (branch-param pin) inside a Merge.
Merge constituents are loaded independently and inner-joined on shared schema
keys (coarser-level constituents broadcast).

MATLAB: `BaseVariable.to_csv` honors `obj.selected_columns` by building a Python
`ColumnSelection`; `Merge.to_csv` translates each constituent to its Python
object (`constituent_to_py`) and calls the Python `Merge.to_csv`. Shared MATLAB
helpers: `+scidb/+internal/{validate_csv_filename,split_csv_args,build_csv_kwargs}.m`.

## MATLAB side (sci-matlab/.../+scidb/BaseVariable.m)
Thin instance-method wrapper `to_csv(obj, filename, varargin)` that:
- validates `filename` ends in `.csv` (MATLAB-native `scidb:ToCsvError`; Python re-validates),
- reuses `split_load_args` to pull `version`/`where`/`db`/metadata,
- builds py kwargs (metadata + `version` + `where.py_filter` + `db`),
- calls the Python classmethod `py_class.to_csv(char(filename), pyargs(...))`.
All loading / scalar-validation / writing happens in Python — MATLAB just marshals.
Test: `sci-matlab/tests/matlab/scidb/TestToCsv.m`.

## Tests (scidb/tests/test_to_csv.py)
- scalar trial-level export → subject,trial,<Name> columns, one row per combo.
- subject-level scalar → only subject column present.
- filename not ending `.csv` → ValueError.
- array (DOUBLE[]) variable → ValueError.
- DataFrame variable → ValueError.
- `where=` filter restricts rows.
- branch-param kwarg (Variant semantics) selects one variant.
- NotFoundError when no match.
