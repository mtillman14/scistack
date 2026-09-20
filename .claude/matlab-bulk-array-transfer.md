# Plan: MATLAB bulk array transfer

## Diagnosis

`filterDelsys` appeared to hang in MATLAB. All four runs in `scidb.log`
(2026-09-14 09:53, 09:55, 09:59, 10:02) stop at the same line and emit nothing
further:

```
for_each_prepare returned in 6.371s
struct-valued input(s) rebuilt per combo: loaded_data (10 field(s))
<silence>
```

That is `+scidb/for_each.m:383`. The next expected INFO is `+scifor/for_each.m:545`.
So MATLAB is in the conversion block between them — `build_scifor_input_from_desc`
-> `scidb.internal.from_python(DataFrame)`.

The data: `load_all_as_df(RawEMG)` -> 419 rows x 17 cols, measured by the plot
path as 4190 cells / 1.74e8 samples / ~1.3 GB. `from_python`'s ndarray branch
converted via `tolist()` + `cell(py_list)` — one Python->MATLAB crossing per
element. 1.74e8 crossings plus multi-GB transient MATLAB cell arrays reads as a
hang, not as slowness.

The DuckDB lock errors that fill the log after that point are a symptom: MATLAB
holds the database for the whole run.

## Stage 1 — Observability (done)

- `+scidb/for_each.m`: time every phase of the previously-silent conversion
  block. Per-input INFO line with duration *and* size
  (`converted input 'loaded_data' in 6.412s (table 419x17, 1.30GB)`), plus a
  `[timing] for_each_convert_inputs:` summary covering resolved_paths /
  loaded_inputs / glue / metadata_iterables / full_combos.
- `from_python.m`: per-column timing in `convert_dataframe`, reported at INFO
  only when the conversion took >= 1s (it runs on every result table too),
  naming up to the three slowest columns.

## Stage 2 — Bulk bytes transfer (done)

- `bridge.ndarray_to_buffer(arr)`: returns the array's raw bytes in **Fortran
  order** plus dtype/count, or `ok=False` with a reason. `_BUFFER_DTYPES` lists
  the supported dtypes (float32/64, int/uint 8-64, bool).
- `from_python.m` ndarray branch tries `ndarray_via_buffer` first:
  `uint8(bytes)` -> `typecast` -> `reshape`. One crossing. The old
  `tolist()`/`cell()` path stays as the fallback, so the libmwbuffer concern
  the original comment cites is still covered.
- This is the only branch that needed fixing: the `py.list` numeric fast path,
  `flatten_sequences`, and `convert_dataframe`'s default branch all funnel
  through it.

**Type contract preserved:** the element-wise path returned `double` for every
numeric dtype, and `TestDataRoundTrip.test_int32_array` / `test_single_precision`
pin that with `verifyEqual` (strict about class). The buffer path casts to
double after typecast. Only `logical` survives as itself.

## Stage 3 — Size guard + tests (done)

- WARN in `from_python.m` when the buffer path declines on an array over 1e6
  elements, naming the element count and dtype, so the slow path announces
  itself rather than going quiet.
- Refusal guard: if `typecast` yields a different element count than the
  description claims, fall back instead of returning wrong data.
- `scimatlab/tests/test_bridge_ndarray_buffer.py` — dtype mapping, Fortran byte
  order (including a C-contiguous input and a strided view), NaN/Inf, empty and
  0-d, every decline case, and the ragged-column path that actually hung.
- `TestFromPython.m` — 8 MATLAB-side tests: large 1-D, 2-D orientation, int and
  single still arriving as double, empty, NaN/Inf, object fallback, bool 2-D.

## Stage 4 — Not done

`for_each` still materializes the entire input table in MATLAB before the loop.
Since `mapping_inputs` means each combo consumes only its own row, a per-combo
lazy fetch would avoid holding ~1.3 GB in MATLAB at all. Larger change to the
prepare/loop contract; deferred.

## Docs

`docs/claude/matlab-bridge-transfer-paths.md` — all transfer routes, their
crossing costs, the two invariants, and how to read the new log lines.
`docs/claude/archive/array-column-loading-optimization.md` marked historical.
