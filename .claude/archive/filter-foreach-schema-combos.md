# Plan: Filter `for_each` to Existing Schema Combinations

## Summary

When `for_each` is called with `[]` (meaning "all values") for schema keys, it resolves each key independently and computes the full cartesian product. Many combinations may not exist in the dataset, producing noisy `[skip]` messages. This change filters the cartesian product to only combinations that actually exist in the `_schema` table — while preserving the full product when a `PathInput` is present.

## Changes Made

### Python

| File | Change |
|------|--------|
| `sciduck/src/sciduck/sciduck.py` | Added `distinct_schema_combinations(keys)` method on SciDuck |
| `src/scidb/database.py` | Added passthrough `distinct_schema_combinations(keys)` on DatabaseManager |
| `scirun-lib/src/scirun/foreach.py` | Added `_has_pathinput()` helper; added filtering logic after cartesian product; imported `_schema_str` for type coercion |
| `scirun-lib/tests/test_foreach.py` | Added `TestForEachSchemaFiltering` test class (9 tests); updated existing `TestForEachAllLevels` MockDB to include `dataset_schema_keys` and `distinct_schema_combinations` |

### MATLAB

| File | Change |
|------|--------|
| `sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m` | Added filtering logic after cartesian product; added `has_pathinput()` and `schema_str()` local helpers; moved dry-run header after filtering so iteration count reflects filtered total |
| `sci-matlab/tests/matlab/TestForEachSchemaFiltering.m` | New test class with 9 integration tests |

## Key Design Decisions

- **PathInput bypass**: When any input is a `PathInput` (or `Fixed(PathInput)`), filtering is skipped entirely since filesystem ingestion data isn't in the DB yet
- **Non-schema keys excluded**: Only keys that are in `dataset_schema_keys` are sent to the filter query; non-schema iterable keys pass through unfiltered
- **String coercion**: Uses `_schema_str()` (Python) / `schema_str()` (MATLAB) to normalize values for comparison since `_schema` stores VARCHAR
- **Info message**: Prints `[info] filtered N non-existent schema combinations (from X to Y)` when filtering removes combos
