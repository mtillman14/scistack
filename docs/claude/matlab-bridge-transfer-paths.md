# MATLAB Bridge Transfer Paths

How data gets from Python into MATLAB in `scimatlab`, which route each shape of
data takes, and what each route costs. Written after a `filterDelsys` run
appeared to hang: it was not hung, it was converting 1.74e8 samples one element
at a time.

Supersedes the transfer half of `archive/array-column-loading-optimization.md`, which
describes an earlier round of this work and still points at the pre-rename
`sci-matlab/sci_matlab` paths.

## The one cost that matters

Every value handed from Python to MATLAB crosses a boundary. The crossing has
meaningful fixed overhead, so the only question that matters for performance is
**how many crossings** a conversion makes:

| Shape of conversion | Crossings | Practical ceiling |
| --- | --- | --- |
| `cell(py_list)` over N elements | N | ~1e5–1e6 elements/sec, plus one ~112-byte MATLAB mxArray per element |
| One `py.bytes` buffer | 1 | memcpy speed, ~GB/sec |

A 419-record `RawEMG` variable in the spread layout is 419 rows x 10 array
columns x ~41,500 samples = 1.74e8 samples, ~1.3 GB. Element-wise that is
1.74e8 crossings and multi-GB of transient MATLAB cell arrays. As one buffer it
is ten crossings.

## Entry point

`+scidb/+internal/from_python.m` is the single conversion entry point. It
dispatches on the Python type:

```
from_python(py_obj)
├── native MATLAB value (bridge already converted it) -> return as-is
├── py.NoneType    -> []
├── py.numpy.ndarray  -> THE BULK PATH (below)
├── py.bool/float/int/str/datetime -> scalar conversion
├── py.list        -> four fast paths, in order (below)
├── py.dict        -> pydict_to_struct
└── pandas DataFrame -> convert_dataframe (below)
```

## The bulk path: numpy ndarray

This is the only branch that has to be fast, because **every other bulk route
funnels through it**: the `py.list` numeric fast path builds an ndarray, the
`flatten_sequences` path returns an ndarray, and `convert_dataframe`'s default
branch calls `col.to_numpy()`.

Order of attempts:

1. **`ndarray_via_buffer`** (local function in `from_python.m`) calls
   `py.scimatlab.bridge.ndarray_to_buffer`, which returns the array's raw bytes
   plus dtype and count. MATLAB does `uint8(bytes)` (one memcpy),
   `typecast(raw, class)`, then `reshape`. One crossing, independent of element
   count.
2. **Element-by-element fallback** — `tolist()` + `cell()` + `cell2mat`. Still
   present and still correct; it runs whenever the buffer path declines.

### When the buffer path declines

Declining is a normal outcome, never an error. `ndarray_to_buffer` returns
`ok=False` with a `reason` for any dtype with no fixed-width raw form:

- `object` (`O`) — the dtype of every DuckDB array column before flattening,
  and of struct/dict cells
- strings (`U`, `S`), `datetime64`/`timedelta64` (`M`, `m`), complex (`c`)
- `float16` and `float128` — MATLAB's `typecast` has no target for them

`_BUFFER_DTYPES` in `bridge.py` is the authoritative list of what *is*
supported: float32/64, int8/16/32/64, uint8/16/32/64, bool.

### Two invariants the buffer path must preserve

**Fortran order.** MATLAB is column-major, numpy is not. The Python side writes
`a.tobytes(order="F")` so MATLAB can `reshape(vec, shape)` with no permute. Get
this wrong and every matrix-valued record is silently transposed —
`TestFromPython.test_numpy_2d_orientation_is_not_transposed` and
`TestDataRoundTrip.test_matrix_element_order` are the guards.

**Everything numeric arrives as `double`.** The element-by-element path
converted every numeric dtype to double (single and all int/uint included), and
`TestDataRoundTrip.test_int32_array` / `test_single_precision` pin that with
`verifyEqual`, which is strict about class. So MATLAB casts after the typecast.
`logical` is the one class that survives as itself. This is a transport
optimization; it must not move the type contract. (For the float64 data the
optimization exists for, the cast is a no-op.)

A third guard is size: if `typecast` yields a different element count than the
description claims, MATLAB refuses the buffer and falls back rather than hand
back plausible-looking wrong data.

## `py.list`: four fast paths in order

1. `py.numpy.asarray(list)` — homogeneous numeric list becomes one ndarray, then
   the bulk path. Unicode (`U`) dtype is deliberately excluded: the ndarray
   route mis-handles string lists.
2. `bridge.flatten_sequences` — a list of variable-length numeric/bool
   sequences (the DuckDB `DOUBLE[]` column shape) becomes one concatenated
   ndarray plus a lengths vector. MATLAB converts the flat array through the
   bulk path and splits it back by length. **This is the path RawEMG takes.**
3. `bridge.convert_nested_dicts_to_json` — lists of nested dicts cross as one
   JSON string and are parsed by MATLAB's native `jsondecode`.
4. Element-by-element.

## `convert_dataframe`: per column

For each column, dispatching on pandas dtype:

- `datetime64` -> ISO strings -> MATLAB `datetime`
- `object` -> `col.tolist()`, then: concat-homogeneous-DataFrames, else
  `from_python(py_list)` (which re-enters the `py.list` fast paths above), else
  element-by-element
- everything else -> `col.to_numpy()` -> the bulk path

Object is the dtype DuckDB array columns arrive as, so array columns reach the
buffer path indirectly, via `flatten_sequences`.

## What the log tells you

Every message inside `from_python` is DEBUG, so at the default INFO level a
slow conversion used to be pure silence. Three INFO/WARN lines now cover it:

```
for_each: converted input 'loaded_data' in 6.412s (table 419x17, 1.30GB)
[timing] for_each_convert_inputs: 3 input(s), 419 combo(s), TOTAL=7.104s (resolved_paths=0.001s, loaded_inputs=6.418s, glue=0.102s, metadata_iterables=0.004s, full_combos=0.579s)
[timing] convert_dataframe: 419x17, TOTAL=6.390s (slowest: data=6.201s, __record_id=0.102s)
```

- **`for_each: converted input '<name>' in Xs (<size>)`** — one per input,
  always. Reports size next to duration so a slow conversion names its cause.
- **`[timing] for_each_convert_inputs`** — covers the whole stretch between
  `struct-valued input(s) rebuilt per combo` and the scifor loop's first line,
  which was previously an unattributed gap.
- **`[timing] convert_dataframe`** — emitted only when the conversion took at
  least 1s (this function also runs on every result table; staying quiet keeps
  ordinary runs readable). Names up to the three slowest columns.
- **`from_python: converting a <N>-element <dtype> array element-by-element`** —
  WARN, above 1e6 elements. Says outright that the slow path was taken and
  roughly why, instead of leaving you to infer it from a stalled log.

## Diagnosing a suspected hang here

1. Find the last MATLAB line. If it is `struct-valued input(s) rebuilt per
   combo` or `for_each_prepare returned`, MATLAB is in the conversion block of
   `+scidb/for_each.m`, not in user code and not deadlocked on the database.
2. Check for the `element-by-element` WARN. If present, a dtype declined the
   buffer path — the `reason` is at DEBUG, so re-run with
   `scidb.Log.set_level('DEBUG', 'file')` to see which.
3. Note that DuckDB lock errors from the GUI (`acquire_db_connection: reopen
   blocked`, `RPC << ... DB LOCKED`) during this window are a *symptom*: MATLAB
   holds the database for the whole run. They are not the cause.

## Where things live

| Concern | File |
| --- | --- |
| Dispatch, all MATLAB-side paths | `scimatlab/src/scimatlab/matlab/+scidb/+internal/from_python.m` |
| Raw buffer description | `ndarray_to_buffer` in `scimatlab/src/scimatlab/bridge.py` |
| Ragged-column flattening | `flatten_sequences` in the same file |
| Conversion timing in for_each | `scimatlab/src/scimatlab/matlab/+scidb/for_each.m` |
| Buffer contract tests (no MATLAB needed) | `scimatlab/tests/test_bridge_ndarray_buffer.py` |
| MATLAB-side conversion tests | `scimatlab/tests/matlab/scidb/TestFromPython.m` |
| Type-contract tests the buffer path must not break | `scimatlab/tests/matlab/scidb/TestDataRoundTrip.m` |

## Still open

The bulk path makes the crossing cheap, but `for_each` still materializes the
**entire** input table in MATLAB before the loop starts. Because
`mapping_inputs` means each of the 419 combos consumes only its own row, a
per-combo lazy fetch would avoid holding ~1.3 GB in MATLAB at all. That is a
larger change to the prepare/loop contract and has not been made.
