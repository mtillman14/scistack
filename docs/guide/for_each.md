# Batch Processing with `for_each`

`for_each` is SciStack's batch execution engine. It runs a function over every combination of metadata values, automatically loading inputs from the database and saving outputs back — with graceful skipping when data is missing.

## The Core Idea

Without `for_each`, processing 5 subjects × 3 sessions looks like this:

```python
for subject in [1, 2, 3, 4, 5]:
    for session in ["pre", "post", "follow"]:
        raw = RawSignal.load(subject=subject, session=session)
        result = bandpass_filter(raw.data, 20, 450)
        FilteredSignal.save(result, subject=subject, session=session)
```

With `for_each`, it's:

```python
for_each(
    bandpass_filter,
    inputs={"signal": RawSignal},
    outputs=[FilteredSignal],
    subject=[1, 2, 3, 4, 5],
    session=["pre", "post", "follow"],
)
```

The two are equivalent in behavior, but `for_each` adds:

- **Graceful skipping** — if a subject's data is missing, that iteration is skipped and a `[skip]` line is printed rather than raising an exception
- **Progress logging** — every load, run, and save is logged
- **Dry-run mode** — preview what would be loaded and saved without executing
- **Constant inputs** — scalars passed to the function and recorded as version keys in the output metadata

## Basic Usage

=== "Python"

    ```python
    from scidb import for_each

    for_each(
        bandpass_filter,            # function to call
        inputs={"signal": RawEMG}, # parameter name → variable type to load
        outputs=[FilteredEMG],     # output variable types (positional)
        subject=[1, 2, 3],         # metadata iterables (Cartesian product)
        session=["A", "B"],
    )
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@bandpass_filter, ...
        struct('signal', RawEMG()), ...   % parameter name → instance
        {FilteredEMG()}, ...              % output types (cell array)
        subject=[1 2 3], ...              % metadata iterables
        session=["A" "B"]);
    ```

For each `(subject, session)` combination, `for_each`:

1. Loads `RawEMG` with `subject=s, session=ses`
2. Calls `bandpass_filter(signal=raw_data)`
3. Saves the result as `FilteredEMG` with `subject=s, session=ses`

If loading fails (e.g., data doesn't exist yet), the iteration is skipped silently.

## Constant Inputs

Pass plain values (not variable types) as constants. They are passed directly to the function and included in the saved output's metadata as **version keys** — so you can query by parameter value later.

=== "Python"

    ```python
    for_each(
        bandpass_filter,
        inputs={
            "signal": RawEMG,
            "low_hz": 20,    # constant — passed as argument, stored as version key
            "high_hz": 450,  # constant — same
        },
        outputs=[FilteredEMG],
        subject=[1, 2, 3],
        session=["A", "B"],
    )

    # Load a specific computational variant by parameter value
    filtered = FilteredEMG.load(subject=1, session="A", low_hz=20, high_hz=450)
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@bandpass_filter, ...
        struct('signal', RawEMG(), ...
               'low_hz',  20, ...
               'high_hz', 450), ...
        {FilteredEMG()}, ...
        subject=[1 2 3], session=["A" "B"]);

    % Load by parameter value
    filtered = FilteredEMG().load(subject=1, session="A", low_hz=20, high_hz=450);
    ```

## Fixed Inputs

Use `Fixed` when an input should always be loaded from a specific metadata location, regardless of the current iteration. The most common use is comparing against a fixed baseline condition.

=== "Python"

    ```python
    from scidb import Fixed

    for_each(
        compare_to_baseline,
        inputs={
            "baseline": Fixed(RawEMG, session="pre"),  # always load session="pre"
            "current": RawEMG,                          # iterates with current session
        },
        outputs=[Delta],
        subject=[1, 2, 3],
        session=["post", "follow"],   # "pre" is never iterated
    )
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@compare_to_baseline, ...
        struct('baseline', scidb.Fixed(RawEMG(), session="pre"), ...
               'current',  RawEMG()), ...
        {Delta()}, ...
        subject=[1 2 3], session=["post" "follow"]);
    ```

For each `(subject, session)` pair, `baseline` is loaded with the fixed `session="pre"` while `current` is loaded with the current `session` value.

## Column Selection

When a variable stores a multi-column table (DataFrame) but you only need certain columns, use column selection to extract just what you need before the function is called.

=== "Python"

    ```python
    # Single column — function receives a numpy array
    for_each(
        compute_mean,
        inputs={"values": GaitData["step_length"]},
        outputs=[MeanStepLength],
        subject=[1, 2, 3],
    )

    # Multiple columns — function receives a DataFrame
    for_each(
        compute_symmetry,
        inputs={"data": GaitData[["left_step", "right_step"]]},
        outputs=[SymmetryIndex],
        subject=[1, 2, 3],
    )
    ```

=== "MATLAB"

    ```matlab
    % Single column — function receives a numeric vector
    scidb.for_each(@compute_mean, ...
        struct('values', GaitData("step_length")), ...
        {MeanStepLength()}, ...
        subject=[1 2 3]);

    % Multiple columns — function receives a subtable
    scidb.for_each(@compute_symmetry, ...
        struct('data', GaitData(["left_step", "right_step"])), ...
        {SymmetryIndex()}, ...
        subject=[1 2 3]);
    ```

Column selection can be combined with `Fixed`:

=== "Python"

    ```python
    Fixed(GaitData["step_length"], session="pre")  # fixed session, single column
    ```

=== "MATLAB"

    ```matlab
    scidb.Fixed(GaitData("step_length"), session="pre")
    ```

## Merge: Combining Multiple Variables

Use `Merge` when your function needs a single DataFrame combining columns from multiple variable types.

=== "Python"

    ```python
    from scidb import Merge

    for_each(
        compute_joint_metric,
        inputs={
            "combined": Merge(KinematicData, ForceData),
        },
        outputs=[JointMetric],
        subject=[1, 2, 3],
        session=["A", "B"],
    )
    # Function receives a DataFrame with all columns from both types
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@compute_joint_metric, ...
        struct('combined', scidb.Merge(KinematicData(), ForceData())), ...
        {JointMetric()}, ...
        subject=[1 2 3], session=["A" "B"]);
    ```

`Merge` constituents can include `Fixed` and column selection:

=== "Python"

    ```python
    Merge(
        GaitData["force"],                     # column selection
        Fixed(PareticSide, session="pre"),     # fixed session
    )
    ```

=== "MATLAB"

    ```matlab
    scidb.Merge(GaitData("force"), scidb.Fixed(PareticSide(), session="pre"))
    ```

**Rules for `Merge`:**

- At least 2 constituents are required
- Column names must be unique across all constituents (use column selection to resolve conflicts)
- All multi-row constituents must have the same number of rows; scalar/single-row constituents are broadcast
- `Fixed` cannot wrap a `Merge` (use `Merge(Fixed(...), ...)` instead)

## PathInput: Loading from Files

Use `PathInput` when your function reads directly from a file rather than the database. The template is resolved with each iteration's metadata.

=== "Python"

    ```python
    from scidb import PathInput

    for_each(
        load_and_process,
        inputs={
            "filepath": PathInput("{subject}/trial_{trial}.mat", root_folder="/data"),
        },
        outputs=[ProcessedData],
        subject=[1, 2, 3],
        trial=[1, 2, 3],
    )
    # For subject=1, trial=2: receives Path("/data/1/trial_2.mat")
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@load_and_process, ...
        struct('filepath', scidb.PathInput("{subject}/trial_{trial}.mat", ...
                                           root_folder="/data")), ...
        {ProcessedData()}, ...
        subject=[1 2 3], trial=[1 2 3]);
    ```

## Loading Multiple Results as a Table

When a partial metadata query returns multiple records per iteration (e.g., loading all trials for a subject within a session loop), use `as_table` to receive a combined table rather than a list.

=== "Python"

    ```python
    for_each(
        aggregate_trials,
        inputs={"trials": StepLength},
        outputs=[TrialMean],
        as_table=True,          # load all trials per iteration as a DataFrame
        subject=[1, 2, 3],
        session=["A", "B"],     # trial is not iterated — loads all trials at once
    )
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@aggregate_trials, ...
        struct('trials', StepLength()), ...
        {TrialMean()}, ...
        as_table=true, ...
        subject=[1 2 3], session=["A" "B"]);
    ```

## Dry Run: Preview Without Executing

Before running a batch job, verify what would be loaded and saved:

=== "Python"

    ```python
    for_each(
        bandpass_filter,
        inputs={"signal": RawEMG, "low_hz": 20},
        outputs=[FilteredEMG],
        dry_run=True,           # preview only, no execution
        subject=[1, 2, 3],
        session=["A", "B"],
    )
    ```

    ```
    [dry-run] for_each(bandpass_filter)
    [dry-run] 6 iterations over: subject, session
    [dry-run] inputs: {signal: RawEMG, low_hz: 20}
    [dry-run] outputs: [FilteredEMG]

    [dry-run] subject=1, session=A:
      load signal = RawEMG.load(subject=1, session=A)
      constant low_hz = 20
      save FilteredEMG.save(..., subject=1, session=A, low_hz=20)
    ...
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@bandpass_filter, ...
        struct('signal', RawEMG(), 'low_hz', 20), ...
        {FilteredEMG()}, ...
        dry_run=true, ...
        subject=[1 2 3], session=["A" "B"]);
    ```

## Iterating Over All Existing Values

Pass an empty array `[]` for a metadata key to automatically use all distinct values already stored in the database for that key:

=== "Python"

    ```python
    for_each(
        compute_metric,
        inputs={"data": RawEMG},
        outputs=[Metric],
        subject=[],     # use all subjects in the database
        session=["A"],
    )
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@compute_metric, ...
        struct('data', RawEMG()), ...
        {Metric()}, ...
        subject=[], ...    % use all subjects in the database
        session=["A"]);
    ```

## Distribute: Splitting Outputs Across the Schema

Use `distribute=True` when a function returns a vector or table that should be split into individual records at the next-deeper schema level. For example, with schema `[subject, trial, cycle]` and iteration at the `trial` level, each element of the output vector becomes a separate `cycle` record.

=== "Python"

    ```python
    for_each(
        detect_cycles,          # returns array of one value per cycle
        inputs={"data": RawEMG},
        outputs=[CycleMetric],
        distribute=True,        # split output by element → saves as cycle=1, 2, 3, ...
        subject=[1, 2, 3],
        trial=[1, 2, 3],        # deepest iterated schema key
    )
    # Each element of the returned array is saved as CycleMetric(subject=s, trial=t, cycle=i)
    ```

=== "MATLAB"

    ```matlab
    scidb.for_each(@detect_cycles, ...
        struct('data', RawEMG()), ...
        {CycleMetric()}, ...
        distribute=true, ...
        subject=[1 2 3], trial=[1 2 3]);
    ```

!!! note
    Lineage tracking is not recorded for distributed saves.

## Parallel Execution (MATLAB only)

MATLAB's `for_each` supports a 3-phase parallel execution mode using `parfor`. This requires the Parallel Computing Toolbox (without it, `parfor` runs serially).

```matlab
scidb.for_each(@pure_matlab_fn, ...
    struct('data', RawEMG()), ...
    {Result()}, ...
    parallel=true, ...
    subject=[1 2 3], session=["A" "B"]);
```

**Phase 1 (serial):** Pre-resolve all inputs from the database.
**Phase 2 (parfor):** Compute all results in parallel.
**Phase 3 (serial):** Batch-save all results.

!!! warning
    `parallel=true` cannot be used with `scidb.Thunk` functions or `PathInput` (parfor workers cannot call Python).

## Option Reference

=== "Python"

    | Parameter | Default | Description |
    |-----------|---------|-------------|
    | `fn` | — | Function to call |
    | `inputs` | — | `dict` mapping parameter names to variable types, `Fixed`, `Merge`, `PathInput`, or constants |
    | `outputs` | — | `list` of variable types for outputs |
    | `dry_run` | `False` | Preview without executing |
    | `save` | `True` | Save outputs after each iteration |
    | `pass_metadata` | `None` | Pass iteration metadata as keyword arguments to `fn` (auto-detects from `generates_file`) |
    | `as_table` | `None` | Convert multi-result inputs to DataFrame: `True` (all), list of names, or `None` |
    | `distribute` | `False` | Split outputs by element/row into the next schema level |
    | `db` | `None` | Use a specific `DatabaseManager` instead of the global database |
    | `**metadata_iterables` | — | Keyword arguments with lists of values (Cartesian product) |

=== "MATLAB"

    | Parameter | Default | Description |
    |-----------|---------|-------------|
    | `fn` | — | Function handle or `scidb.Thunk` |
    | `inputs` | — | `struct` mapping parameter names to `BaseVariable` instances, `Fixed`, `Merge`, `PathInput`, or constants |
    | `outputs` | — | Cell array of `BaseVariable` instances for output types |
    | `dry_run` | `false` | Preview without executing |
    | `save` | `true` | Save outputs after each iteration |
    | `preload` | `true` | Bulk-load all input data in one query per variable type before iterating (faster but uses more memory) |
    | `pass_metadata` | `[]` | Pass iteration metadata as trailing name-value arguments to `fn` |
    | `as_table` | `[]` | Convert multi-result inputs to table: `true` (all), string array of names, or `[]` |
    | `distribute` | `false` | Split outputs by element/row into the next schema level |
    | `parallel` | `false` | Use 3-phase parfor execution (pure MATLAB functions only) |
    | `db` | `[]` | Use a specific `DatabaseManager` instead of the global database |
    | `subject=...` etc. | — | Metadata iterables (numeric arrays or string arrays; Cartesian product) |
