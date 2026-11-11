# Plan: Flatten Nested Struct Columns in DataFrame Save/Load

## Context
When saving a MATLAB table where columns contain nested structs (e.g., `seconds(1,1).gaitPhases.start.leftStance`), the save fails with `ValueError: Per-column arrays must each be 1-dimensional`. Leaf values can be arrays (e.g., 6x1 vectors).

## File to Modify
**`/workspace/src/scidb/database.py`**

## Implementation Steps
1. Add helper functions: `_get_leaf_paths`, `_get_nested_value`, `_set_nested_value`
2. Add `_flatten_struct_columns(df)` - detect dict columns, flatten to dot-separated names, serialize array leaves as JSON
3. Add `_unflatten_struct_columns(df, struct_info)` - inverse, rebuild nested dicts, restore arrays
4. Modify `save()` DataFrame path (~line 1553) to flatten before `_save_columnar()`
5. Modify `_save_columnar()` to accept and store struct_columns metadata
6. Modify `_load_by_record_row()` to unflatten on load when metadata present
7. Add diagnostic prints
