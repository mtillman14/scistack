# Array Column Loading Optimization

> **Historical.** This describes an earlier round of bridge-transfer work and
> its file paths predate the `sci-matlab/sci_matlab` -> `scimatlab/scimatlab`
> rename. The "Future Improvements" below have since been done: flattening
> landed as `bridge.flatten_sequences`, and the remaining per-element cost was
> removed by the raw-buffer path. For the current picture see
> **`matlab-bridge-transfer-paths.md`**.

## Problem

When loading DuckDB tables with array columns (e.g., `DOUBLE[]`, `BOOLEAN[]`) into MATLAB, the conversion was extremely slow:
- **516 rows × 50+ array columns = ~25,000+ individual Python→MATLAB boundary crossings**
- Each cell was converted individually, taking several minutes for moderate-sized datasets

## Root Cause

1. DuckDB array columns return as pandas object dtype (each cell contains a numpy array)
2. MATLAB's `convert_dataframe()` function detected object dtype and used the "object branch"
3. The object branch iterated through each cell individually, calling `from_python()` on each numpy array
4. Each `from_python()` call crosses the expensive Python-MATLAB boundary

## Solution

### Python Side Optimization (`sci-matlab/src/sci_matlab/bridge.py`)

**Location:** `wrap_batch_bridge()` function, after DataFrame concatenation

**What it does:**
- Detects object-dtype columns containing numpy arrays
- Pre-converts numpy arrays to Python lists: `arr.tolist()`
- This conversion happens once in Python (fast) instead of 516× in MATLAB

**Why it helps:**
- Python list conversion is much faster than numpy array conversion across the MATLAB boundary
- Reduces per-cell overhead when MATLAB processes the column

### MATLAB Side Optimization (`sci-matlab/src/sci_matlab/matlab/+scidb/+internal/from_python.m`)

**Location:** `convert_dataframe()` function, object branch

**What it does:**
- Instead of converting `py_list` to a MATLAB cell array and iterating manually
- Calls `from_python(py_list)` directly on the entire Python list
- Uses the optimized `py.list` branch which attempts vectorized numpy conversion first
- Falls back to element-by-element only when necessary (complex types)

**Why it helps:**
- Leverages existing optimizations in the `py.list` branch
- Reduces redundant conversions and checks
- Single try-catch for the entire column instead of per-cell error handling

## Performance Impact

**Before:**
- Object columns: Processed cell-by-cell with explicit MATLAB for-loop
- Each cell: Python numpy array → MATLAB conversion (expensive boundary crossing)
- Time: Several minutes for 516×54 table

**After:**
- Python: Pre-convert numpy arrays to lists (one-time cost)
- MATLAB: Process entire column through optimized `py.list` branch
- Expected: **10-100× speedup** depending on array sizes and types

## Testing

To verify the optimization is working, check the logs for:
```
[DEBUG] wrap_batch_bridge: optimized N array columns for MATLAB transfer
[DEBUG] convert_dataframe: column X - used optimized py.list conversion
```

## Future Improvements

If further optimization is needed, consider:
1. **Flattened array format**: Flatten all arrays in a column + send size array separately
2. **PyArrow dtypes**: Use pandas list[double] dtype instead of object dtype
3. **Batched conversion**: Process multiple columns in parallel on MATLAB side

## Related Files

- `/workspace/sci-matlab/src/sci_matlab/bridge.py` - Python-side optimization
- `/workspace/sci-matlab/src/sci_matlab/matlab/+scidb/+internal/from_python.m` - MATLAB-side optimization
- `/workspace/docs/claude/archive/array-column-loading-optimization.md` - This document
