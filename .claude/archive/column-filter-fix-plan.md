# Plan: Fix MATLAB Column Filtering in where= Clauses

## Problem Summary
`GAITRiteLoadedCycle("StepLengths_GR") ~= 0` in a `where=` clause fails because:
- MATLAB creates a `VariableFilter` that queries the "value" column
- Tabular variables like `GAITRiteLoadedCycle` don't have a "value" column
- The correct behavior is to create a `ColumnFilter` that queries the specified column

## Solution

### 1. Update MATLAB BaseVariable Comparison Operators
**File**: `sci-matlab/src/sci_matlab/matlab/+scidb/BaseVariable.m`

Modify all comparison operators (eq, ne, lt, le, gt, ge) to:
- Check if `obj.selected_columns` is non-empty
- If yes, create a `ColumnFilter` using `py.scidb.filters.ColumnFilter(py_class, column, op, py_val)`
- If no, create a `VariableFilter` as before (current behavior)

**Changes needed**:
- Lines 562-627: Update `eq`, `ne`, `lt`, `le`, `gt`, `ge` methods

**Example implementation for `ne`**:
```matlab
function filt = ne(obj, other)
    if isa(other, 'scidb.BaseVariable')
        filt = builtin('ne', obj, other);
        return;
    end
    type_name = class(obj);
    py_class = scidb.internal.ensure_registered(type_name);
    py_val = scidb.internal.to_python(other);

    % Check if column selection is active
    if ~isempty(obj.selected_columns)
        % Use ColumnFilter for column-specific filtering
        column = char(obj.selected_columns(1));  % Use first column
        py_filter = py.scidb.filters.ColumnFilter(py_class, column, '!=', py_val);
    else
        % Use VariableFilter for whole-variable filtering
        py_filter = py.scidb.filters.VariableFilter(py_class, '!=', py_val);
    end
    filt = scidb.Filter(py_filter);
end
```

### 2. Add Regression Test
**File**: `sci-matlab/tests/matlab/scidb/TestWhereFilters.m` (create if doesn't exist)

Add test case that:
- Creates a tabular variable with data
- Uses column selection in where= clause: `Variable("column") ~= 0`
- Verifies that the filter works correctly
- Ensures error doesn't occur about missing "value" column

### 3. Update Documentation
**File**: `docs/claude/where-filter-system.md`

Add section explaining MATLAB column filtering behavior:
- How `Variable("column")` with comparison operators creates `ColumnFilter`
- Clarify that this matches Python's `Variable["column"]` behavior
- Note that multiple column selection uses only the first column (consistent with Python)

### 4. Add Logging for Diagnostics
**Consideration**: Add debug logging in the comparison operators to make it easier to diagnose similar issues in the future. This could log:
- Which filter type is being created (VariableFilter vs ColumnFilter)
- Which column is being filtered (if ColumnFilter)

## Implementation Order
1. Fix BaseVariable comparison operators (task #2)
2. Add regression test (task #3)
3. Update documentation (task #4)

## Testing Strategy
- Run the user's failing command to verify it now works
- Run existing MATLAB filter tests to ensure no regressions
- Add new test for column filtering with various operators (==, !=, <, <=, >, >=)

## Edge Cases to Consider
- Multiple column selection: Use first column only (consistent with Python)
- Empty column selection: Fall back to VariableFilter (current behavior)
- Non-tabular variables with column selection: Should still work if column exists

## Success Criteria
- User's command `where=GAITRiteLoadedCycle("StepLengths_GR") ~= 0` works without error
- All existing tests pass
- New test demonstrates column filtering works correctly
- Documentation clearly explains the behavior
