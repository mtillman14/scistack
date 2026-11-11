# for_each Performance Fix: from_python Auto-Conversion Overhead

## Problem
`scidb.for_each` with wide DOUBLE[] tables (e.g. 54-column GAITRiteLoaded) is extremely slow:
- ~142s total, of which ~54s is pure exception-handling overhead
- Actual user function: 0.374s

## Root Cause
When MATLAB extracts elements from `cell(py.list(...))`, Python floats are auto-converted
to native MATLAB doubles. These native doubles then fall through ALL `isa(py_obj, 'py.*')`
checks in `from_python`, reaching the `else` fallback which throws+catches a `PyException`
trying `py.builtins.isinstance()` on a MATLAB double. ~50K exceptions × stack trace overhead
= 54s wasted.

## Changes Made (`from_python.m`)

### Fix 1: Native MATLAB type early-exits (lines 12-23)
Added early return for `islogical`, `isnumeric`, and `isstring` values at the top of
`from_python`, before any `py.*` checks. This catches auto-converted values immediately:
- Eliminates ~50K PyException constructions
- Eliminates ~100K getStack/flipud calls
- Expected savings: ~54s (exception overhead) + ~20s (avoided isa checks)

### Fix 2: Bulk list-to-numpy conversion (lines 70-86)
For `py.list` objects, try converting the entire list to a numpy array via
`py.numpy.asarray()` before falling back to element-by-element conversion:
- When successful (homogeneous numeric lists): 1 boundary crossing instead of N
- Falls through gracefully for heterogeneous lists (mixed types, nested structures)
- Expected savings: further reduces from_python calls for DOUBLE[] columns

## Expected Impact
For the GAITRiteLoaded case (54 columns, many DOUBLE[]):
- Before: ~142s (85s from_python + 54s exceptions)
- After: estimated ~5-15s (bulk numpy conversion + minimal overhead)
