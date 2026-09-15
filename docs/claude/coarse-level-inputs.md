# Coarse-level inputs and unpopulated schema columns

*Written 2026-09-15 while diagnosing `grSides` failing every iteration with
`MATLAB:string:MustBeConvertibleCellArray`. Concerns `scidb` (the owner) and
both `scifor` implementations (the two victims).*

## The shape

`db.load_all_as_df(layout="spread")` emits **one column per dataset schema
key**, not per schema key the variable actually uses:

```python
# database._assemble_df_from_records_and_data
for key in schema_keys:            # dataset_schema_keys — ALL of them
    if key in records.columns:
        meta_dict[key] = ...
```

So in a `subject/session/speed/trial/cycle` schema, a **subject-level**
variable such as `Demographics` loads as:

| subject | session | speed | trial | cycle | PareticSide | … |
|---------|---------|-------|-------|-------|-------------|---|
| SS01    | `None`  |`None` |`None` |`None` | L           | … |
| SS02    | `None`  |`None` |`None` |`None` | R           | … |

Four all-NULL columns that are not data and not absence-of-data.

## What an all-NULL schema column means

**The variable does not participate in that dimension, so its rows broadcast
across it.** `Demographics` has no session axis; the paretic side of SS01 is
the paretic side of SS01 at every session, speed and trial.

It emphatically does **not** mean "this row has no session". That reading is
what both filters do by default, and both are wrong:

| Layer | Code | Behaviour with the NULL column left in |
|---|---|---|
| Python | `scifor/src/scifor/foreach.py` `_filter_df_for_combo` | `col_vals == meta_val` → `None == "BL"` → `False` for every row → zero rows → `scifor:NoData` on **every** combo. Silent; looks like "no data". |
| MATLAB | `+scifor/for_each.m` `filter_table_for_combo` (~line 1585) | The column crosses the bridge as a cell array of `0×0 double`; `string(col_data)` raises `MATLAB:string:MustBeConvertibleCellArray` → **every** iteration fails with `failed to filter <param>`. |

Both end at `completed=0` and an empty result table, so the node reads as a
no-op. Neither says "your input is coarser than your iteration level".

## The fix: drop unpopulated schema columns at load

`scidb/src/scidb/foreach.py` `_drop_unpopulated_schema_columns` removes every
schema-key column that is **entirely** null, and `_load_input` applies it to
every input kind — plain variable type, `ColumnSelection`, `Fixed` (via
recursion) and `Merge` constituents. With the column gone,
`filter_table_for_combo` simply never sees that dimension and the rows
broadcast, in both languages.

Only **fully** null columns are dropped. A variable with records saved at mixed
granularity keeps its partially-populated key and still filters on it — a row
that genuinely has no session is a different thing from a variable that has no
session axis, and this rule does not conflate them.

### Why scidb owns it, not scifor

The all-NULL column is an artifact `scidb`'s spread loader creates. A pure
scifor caller passing their own DataFrame never produces one. Putting the rule
in scifor would mean writing it twice — once in Python, once in MATLAB — to
repair something a third package invented. One owner, at the point of creation.

### History: this was fixed three times, locally, before it was fixed once

The same hazard had already been patched wherever it happened to bite, which is
why it kept coming back somewhere else:

| Site | Scope | Status |
|---|---|---|
| `_load_input` Merge branch (`foreach.py`) | Merge constituents only | folded into the shared helper |
| `build_scifor_input_from_desc` → `drop_all_empty_cell_columns` (`+scidb/for_each.m`) | the `'merge'` case only — not `'dataframe'`, not `'column_selection'` | left in place, now redundant but harmless |
| `_aggregation_mode` block (`foreach.py` ~2500) | aggregation runs only, and only keys *below* the iterated level | still there; full iteration never reached it |

The gap that bit `grSides`: a coarse variable bound **directly** to a
parameter (not inside a `Merge`) under **full iteration**. None of the three
covered it.

Two downstream sites independently recompute `schema_cols_in_df` by excluding
all-NaN columns — `rid_per_combo` and `colsel_existence` (`foreach.py` ~2166,
~2209, with `scidb/tests/test_coarse_input_provenance.py` explaining why:
pandas `groupby` drops NaN-key groups, which silently severed input
provenance). Dropping at load makes those exclusions redundant rather than
wrong, so they stay as belt-and-braces.

## The second defect: rid expansion pruned the whole grid

Dropping the columns is necessary but was not sufficient. Under **full
iteration**, a plain (non-`ColumnSelection`) input also gets a `__rid_{param}`
key, and every combo is expanded by looking its record ids up per location:

```python
for rid_col, mapping in rid_per_combo.items():
    rids = mapping.get(schema_vals, [])   # exact match on the FULL key
    if not rids:
        valid = False                     # combo skipped entirely
        break
```

`rid_per_combo`'s keys are built over `_lookup_keys` with `""` in every
position the input does not populate — `("S01", "", "")` for a subject-level
variable — while `schema_vals` carries a real value in every position:
`("S01", "BL", "1")`. The two never match, so `valid` is false for **every**
combo and the entire grid is pruned. Zero calls, zero records, no error.

`ColumnSelection` inputs escaped this by accident of design: they are
registered as *pruning-only* (`colsel_params`, no rid expansion), and their
pruning check `_colsel_combo_present` has always compared on populated
positions only. That asymmetry is exactly why a coarse `ColumnSelection`
worked while a coarse plain variable did not — and why the first round of this
fix made the `grSides` shape pass while `"side": Demographics` still returned
nothing.

`_rid_probe_key` closes it: blank the positions the input does not populate
before probing, the same way `_colsel_combo_present` does. An input that
populates every lookup key probes unchanged, so the common path and all
non-existent-combo pruning (`test_column_selection_combo_pruning.py`) are
byte-identical.

Aggregation mode was never affected: it keys `per_combo_rids` by the *iterated*
keys only (`_iter_idx`), so an unpopulated key that is not iterated simply
never enters the comparison. That is why `test_coarse_input_provenance.py`
passes today and why the bug was invisible until someone iterated the full
schema.

## Diagnosing it in the field

The fix logs one INFO line per affected input:

```
[coarse-input] 'side' (ColumnSelection on Demographics): dropped unpopulated
schema column(s) ['session', 'speed', 'trial'] — stored at a coarser level, so
its rows broadcast across session/speed/trial and filter only on ['subject']
```

The parameter name in that line is why `_load_input` threads `param_name` at
all: it is logging-only and changes nothing about what loads, but the failure
it prevents names the *parameter* (`failed to filter side`), not the variable
type, so a message keyed only on `Demographics` would be hard to connect to it.

Its **absence**, combined with either symptom below, means the input reached
the filter with its NULL columns intact:

* MATLAB — `failed to filter <param>: Conversion from cell failed. Element 1
  must be convertible to a string scalar.`, `failed=N` where N is every combo
  that had data.
* Python — `scifor:NoData` on every combo, `completed=0`.

Note that the crash names the **parameter** (`side`), not the variable, and
points at `filter_table_for_combo` — neither of which mentions granularity.
That is what made it read as a ColumnSelection bug; the ColumnSelection is
incidental, since filtering happens before any column extraction. Binding bare
`Demographics` fails identically.

## Key files

| File | Role |
|------|------|
| `scidb/src/scidb/foreach.py` | `_drop_unpopulated_schema_columns` (the owner), applied in `_load_input`; `_rid_probe_key` in `_for_each_prepare` for the full-iteration lookup |
| `scidb/src/scidb/database.py` | `_assemble_df_from_records_and_data` — emits one column per dataset schema key (the source of the shape) |
| `scifor/src/scifor/foreach.py` | `_filter_df_for_combo` — the Python victim |
| `scimatlab/src/scimatlab/matlab/+scifor/for_each.m` | `filter_table_for_combo` — the MATLAB victim |
| `scidb/tests/test_coarse_input_broadcast.py` | regression: coarse input under full iteration, all four input kinds |
| `scidb/tests/test_coarse_input_provenance.py` | the earlier all-NaN casualty (provenance edges) |
