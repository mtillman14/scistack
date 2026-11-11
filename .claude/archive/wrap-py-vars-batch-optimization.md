# wrap_py_vars_batch Optimization Plan

## Problem
`wrap_py_vars_batch` is a bottleneck in `scidb.for_each()` execution. MATLAB profiler for 14,400 rows shows ~1.9s spent across 5 hot lines, all inside the per-item loop.

## Profiler Data
| Line | Code | Calls | Time |
|------|------|-------|------|
| 551 | `get_batch_item(batch_id, int64(i-1))` | 14,400 | 0.19s |
| 552 | `from_python(item{1})` | 14,400 | 0.451s |
| 553 | `ThunkOutput(matlab_data, item{2})` | 14,400 | 0.735s |
| 564 | `int64(str2double(vid_strs(i)))` | 14,400 | 0.286s |
| 567 | `int64(str2double(pid_strs(i)))` | 14,400 | 0.243s |

## Optimizations

### A. Vectorize str2double (saves ~0.5s)
**File:** `BaseVariable.m` (wrap_py_vars_batch)

Move `str2double` outside the loop. One vectorized call replaces 14,400 scalar calls:
```matlab
% Before loop
vid_nums = str2double(vid_strs);
pid_nums = str2double(pid_strs);
vid_valid = ~isnan(vid_nums);
pid_valid = ~isnan(pid_nums);
% In loop
if vid_valid(i), v.version_id = int64(vid_nums(i)); end
if pid_valid(i), v.parameter_id = int64(pid_nums(i)); end
```

### B. Scalar data batch transfer (saves ~0.45s)
**Files:** `bridge.py` (wrap_batch_bridge) + `BaseVariable.m` (wrap_py_vars_batch)

When all data items are scalars (int/float), pack as a single numpy array in the bulk dict. MATLAB converts with one `double()` call instead of 14,400 `from_python` calls.

Python side:
```python
all_scalar = all(isinstance(d, (int, float)) for d in data)
if all_scalar:
    result['scalar_data'] = np.array(data, dtype=float)
```

MATLAB side:
```matlab
if bulk.keys().__contains__('scalar_data')
    all_scalar_data = double(bulk{'scalar_data'});
    scalar_path = true;
end
% In loop: matlab_data = all_scalar_data(i);
```

### C. Bulk py_vars cell conversion (saves ~0.5-0.7s)
**Files:** `bridge.py` + `BaseVariable.m`

Instead of 14,400 `get_batch_item` Python function calls, return the `py_vars` list directly in the bulk dict. MATLAB converts it to a cell array with one `cell()` call, then indexes into the cell array (pure MATLAB, no Python crossing) in the loop.

Note: Originally considered lazy py_obj loading via Dependent property getter on ThunkOutput, but the bulk cell conversion approach is simpler and avoids batch cache lifetime complexity (lazy loading would require the batch to stay alive until all py_obj references are resolved).

### D. Preallocate results array (minor savings)
**File:** `BaseVariable.m` (wrap_py_vars_batch)

Replace `results(end+1) = v` with indexed assignment into preallocated array:
```matlab
results(n) = scidb.ThunkOutput();
for i = 1:n
    ...
    results(i) = v;
end
```

## Expected Result
~1.65s savings out of ~1.9s (~87% reduction for this function).
