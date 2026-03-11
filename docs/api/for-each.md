# Batch Processing API — `for_each`

`for_each` runs a function over every combination of metadata values, automatically loading inputs and saving outputs. See the [Batch Processing guide](../guide/for_each.md) for conceptual explanations and worked examples. This page is the complete API reference.

---

## `for_each()`

=== "Python"

    ```python
    from scidb import for_each

    for_each(
        fn,
        inputs,
        outputs,
        dry_run=False,
        save=True,
        as_table=None,
        distribute=False,
        db=None,
        **metadata_iterables,
    )
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(fn, inputs, outputs, Name=Value, ...)
    ```

### Parameters

=== "Python"

    | Parameter | Type | Default | Description |
    |-----------|------|---------|-------------|
    | `fn` | `Callable` | — | Function to call. Works with plain functions and `Thunk`-wrapped functions. |
    | `inputs` | `dict` | — | Maps parameter names to input specs (see Input Types below). |
    | `outputs` | `list[type]` | — | Variable types for the outputs, matched positionally to the function's return values. |
    | `dry_run` | `bool` | `False` | If `True`, print what would load/save without executing. |
    | `save` | `bool` | `True` | If `False`, run the function but don't save outputs. |
    | `as_table` | `bool \| list[str] \| None` | `None` | Convert multi-result loads to DataFrame. `True` = all inputs; list of names = only those inputs; `None` / `False` = none. |
    | `distribute` | `bool` | `False` | Split each output by element/row and save each piece at the next-deeper schema level (1-based indexing). |
    | `db` | `DatabaseManager \| None` | `None` | Use a specific database instead of the global one. |
    | `**metadata_iterables` | any | — | Keyword arguments with iterables of values. Every combination is iterated (Cartesian product). Pass `[]` to use all distinct values in the database for that key. |

=== "MATLAB"

    | Parameter | Type | Default | Description |
    |-----------|------|---------|-------------|
    | `fn` | `function_handle` or `scidb.Thunk` | — | Function to call. |
    | `inputs` | `struct` | — | Maps parameter names to input specs. Field order determines argument order to `fn`. |
    | `outputs` | `cell array` | — | Cell array of `BaseVariable` instances for outputs. |
    | `dry_run` | `logical` | `false` | Preview without executing. |
    | `save` | `logical` | `true` | If `false`, run but don't save. |
    | `preload` | `logical` | `true` | Bulk-load all inputs in one query per variable type before iterating. Faster but uses more memory. Set to `false` for very large datasets. |
    | `as_table` | `logical \| string array \| []` | `[]` | Convert multi-result loads to MATLAB table. `true` = all inputs; string array = only named inputs; `[]` = none. |
    | `distribute` | `logical` | `false` | Split outputs by element/row into the next-deeper schema level. |
    | `parallel` | `logical` | `false` | 3-phase parallel execution (pure MATLAB functions only; requires Parallel Computing Toolbox for true parallelism). |
    | `db` | `DatabaseManager \| []` | `[]` | Use a specific database instead of the global one. |
    | `subject=...` etc. | numeric or string array | — | Metadata iterables. Cartesian product is computed. Pass empty array `[]` to use all distinct values in the database. |

### Reusing metadata iterables

The metadata name-value pairs (`subject=[1 2 3], trial=["A" "B"]`) can be stored in a cell array and expanded with `{:}` for reuse across multiple `for_each` calls:

=== "Python"

    ```python
    meta = dict(subject=[1, 2, 3], trial=["A", "B"])

    for_each(fn1, inputs1, outputs1, **meta)
    for_each(fn2, inputs2, outputs2, **meta)
    ```

=== "MATLAB"

    ```matlab
    meta = {'subject', [1 2 3], 'trial', ["A" "B"]};

    scifor.for_each(@fn1, inputs1, meta{:})
    scifor.for_each(@fn2, inputs2, meta{:})
    ```

    You can also build the list programmatically:

    ```matlab
    meta = {};
    meta = [meta, {'subject', [1 2 3]}];
    meta = [meta, {'trial', ["A" "B"]}];
    scifor.for_each(@fn, inputs, meta{:})
    ```

### Returns

Nothing. Outputs are saved to the database and logged to stdout.

### Console Output

Every iteration prints its status:

```
[run] subject=1, session=A: bandpass_filter(signal, low_hz)
[save] subject=1, session=A: FilteredSignal
[skip] subject=2, session=A: failed to load signal (RawEMG): No RawEMG found...
[run] subject=3, session=A: bandpass_filter(signal, low_hz)
[save] subject=3, session=A: FilteredSignal

[done] completed=2, skipped=1, total=3
```

---

## Input Types

The `inputs` dict/struct accepts several types:

### Variable type (loads from database)

=== "Python"

    ```python
    inputs={"signal": RawEMG}
    ```

=== "MATLAB"

    ```matlab
    struct('signal', RawEMG())
    ```

Loaded with the current iteration's metadata.

---

### `Fixed` — fixed metadata override

=== "Python"

    ```python
    from scidb import Fixed

    Fixed(VarType, key=value, ...)
    ```

    ```python
    # Always load session="pre", regardless of the current session
    inputs={"baseline": Fixed(StepLength, session="pre")}
    ```

=== "MATLAB"

    ```matlab
    scidb.Fixed(VarInstance, key=value, ...)
    ```

    ```matlab
    struct('baseline', scidb.Fixed(StepLength(), session="pre"))
    ```

`Fixed` can wrap a variable type or a `ColumnSelection`. It cannot wrap a `Merge`.

**`Fixed` constructor:**

| Parameter | Description |
|-----------|-------------|
| `var_type` | Variable class (Python) or instance (MATLAB) to load |
| `**fixed_metadata` | Key-value pairs that override the iteration metadata at load time |

---

### Column selection — extract specific columns

Use when your variable stores a multi-column table and you only need certain columns.

=== "Python"

    ```python
    MyVar["col_name"]             # single column → numpy array
    MyVar[["col_a", "col_b"]]    # multiple columns → DataFrame subset
    ```

    ```python
    inputs={"x": GaitData["step_length"]}
    inputs={"features": GaitData[["force", "moment"]]}
    ```

=== "MATLAB"

    ```matlab
    MyVar("col_name")             % single column → numeric vector
    MyVar(["col_a", "col_b"])     % multiple columns → subtable
    ```

    ```matlab
    struct('x', GaitData("step_length"))
    struct('features', GaitData(["force", "moment"]))
    ```

Can be combined with `Fixed`:

=== "Python"

    ```python
    Fixed(GaitData["step_length"], session="pre")
    ```

=== "MATLAB"

    ```matlab
    scidb.Fixed(GaitData("step_length"), session="pre")
    ```

---

### `Merge` — combine multiple variables into one table

=== "Python"

    ```python
    from scidb import Merge

    Merge(VarTypeA, VarTypeB, ...)
    ```

    ```python
    inputs={"combined": Merge(KinematicData, ForceData)}

    # With Fixed and column selection
    inputs={
        "data": Merge(
            GaitData["force"],
            Fixed(ParticipantInfo, session="baseline"),
        )
    }
    ```

=== "MATLAB"

    ```matlab
    scidb.Merge(VarInstanceA, VarInstanceB, ...)
    ```

    ```matlab
    struct('combined', scidb.Merge(KinematicData(), ForceData()))

    % With Fixed and column selection
    struct('data', scidb.Merge(GaitData("force"), ...
                               scidb.Fixed(ParticipantInfo(), session="baseline")))
    ```

**Rules:**

- At least 2 constituents required
- Column names must be unique across all constituents
- All multi-row constituents must have the same number of rows; single-row values are broadcast
- `Fixed(Merge(...), ...)` is not supported — use `Merge(Fixed(...), ...)` instead

---

### `PathInput` — resolve file path from metadata

=== "Python"

    ```python
    from scidb import PathInput

    PathInput(path_template, root_folder=None)
    ```

    ```python
    inputs={
        "filepath": PathInput("{subject}/session_{session}.csv", root_folder="/data")
    }
    # subject=1, session="A" → receives Path("/data/1/session_A.csv")
    ```

=== "MATLAB"

    ```matlab
    scidb.PathInput(path_template, root_folder=root)
    ```

    ```matlab
    struct('filepath', scidb.PathInput("{subject}/session_{session}.csv", ...
                                       root_folder="/data"))
    ```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `path_template` | Format string with `{key}` placeholders matching metadata keys |
| `root_folder` | Optional root directory. Path is resolved relative to it (or to `cwd` if `None`). |

The function receives the resolved `pathlib.Path` (Python) and should handle file reading itself. The path is not loaded from the database.

---

### Constant (plain value)

Any value that is not a variable type, `Fixed`, `Merge`, or `PathInput` is treated as a constant:

=== "Python"

    ```python
    inputs={"signal": RawEMG, "low_hz": 20, "high_hz": 450}
    ```

=== "MATLAB"

    ```matlab
    struct('signal', RawEMG(), 'low_hz', 20, 'high_hz', 450)
    ```

Constants are passed directly to the function as named arguments and are also stored as **version keys** in the output metadata, so you can filter by parameter value later:

```python
FilteredEMG.load(subject=1, session="A", low_hz=20, high_hz=450)
```
