# Writing Functions for SciStack

SciStack runs **your** functions. It doesn't ask you to subclass anything or learn a new language. It does expect each function to have a particular _shape_. This page explains that shape, why it's worth it, how to convert an existing analysis script into it, and when you shouldn't bother making code general.

Everything here applies to both the code and GUI workflows: the GUI calls the same functions, connecting variables to them on the canvas instead of in a `for_each` call.

<!-- Ground truth: scifor/src/scifor/foreach.py (_extract_data: 1x1 -> scalar,
     Nx1 -> (N,1) array, multi-column -> data-only DataFrame; as_table keeps
     schema columns; tuple returns = several outputs, foreach.py ~L1019);
     scidb/src/scidb/roles.py (plot_/stat_/glue_ prefixes; stat_ defaults to
     as_table); scifor.discovery.find_top_level_side_effects + registry
     _screen_for_side_effects (files with top-level work are skipped);
     scidb variable.py (as_table data column named after the variable class);
     custom storage: scidb/tests/test_custom_serialization.py (Python),
     scimatlab +scidb/+internal/codec.m + tests/matlab/scidb/TestCodec.m
     (MATLAB to_db/from_db, scidb:CodecError, for_each inputs/outputs). -->

## The shape in one sentence

> A SciStack function takes **one location's worth of data** as named arguments, and **returns** its results. It doesn't loop over subjects, open the database, or save anything.

/// tab | Python

```python
def lowpass_envelope(signal, fs_hz, cutoff_hz):
    """Rectify and low-pass filter one EMG recording."""
    b, a = butter(4, cutoff_hz / (fs_hz / 2), "low")
    return filtfilt(b, a, np.abs(signal - np.mean(signal)))
```

///

/// tab | MATLAB

```matlab
% lowpass_envelope.m
function env = lowpass_envelope(signal, fs_hz, cutoff_hz)
    % Rectify and low-pass filter one EMG recording.
    [b, a] = butter(4, cutoff_hz / (fs_hz / 2), "low");
    env = filtfilt(b, a, abs(signal - mean(signal)));
end
```

///

SciStack supplies the loop. You tell `for_each` which arguments come from which variables, and which schema keys to iterate:

```python
for_each(
    lowpass_envelope,
    inputs={"signal": RawEMG, "fs_hz": 1000, "cutoff_hz": 20},
    outputs=[EmgEnvelope],
    subject=[], session=[],      # once per (subject, session) in the database
)
```

For each `(subject, session)`, SciStack loads that location's `RawEMG`, calls `lowpass_envelope(signal=..., fs_hz=1000, cutoff_hz=20)`, and saves the return value as that location's `EmgEnvelope`, along with a record of which function, inputs and settings produced it.

## The rules

### 1. Inputs are named arguments

Each key of `inputs` is the name of one of your function's parameters. Data, settings and file paths all arrive the same way, as arguments. In the GUI, each parameter is an input port on the function's node.

Give parameters names that say what they are (`signal`, `fs_hz`, `cutoff_hz`), not `x`, `y`, `data2`. The names are what you will see on the canvas and in a result's history.

### 2. Settings are arguments, not hard-coded numbers

A number buried in the function body (a cutoff frequency, a threshold, a window length) is hidden from the next person who reads your code: a labmate, a reviewer, or you in six months writing the methods section. To learn which settings an analysis depends on, they would have to read every line of every function, and they could easily miss one.

Pass it in as an argument instead. Then every setting a step depends on is listed in its signature, and each value is visible in one place: the `for_each` call, or the node's inputs on the GUI canvas.

It also makes the setting visible to SciStack:

- it is **recorded** with every result it produced;
- changing it creates a new **variant** next to the old results, rather than silently overwriting them;
- you can try several values in one run, with `Parameter(10, 20, 30)` or `EachOf(...)`.

Settings you will reuse across steps, or want to see by name in the GUI, can be declared once as a [`Parameter`](../api/scidb.md#batch-processing), in code or in the [entities file](project-files.md#scistack_entitiestoml).

### 3. Return results; don't save them

Return the value. `for_each` saves it as the variable named in `outputs`. To produce several outputs, return several values and list one variable per value:

/// tab | Python

```python
def gait_events(heel_marker, fs_hz):
    ...
    return heel_strikes, toe_offs        # a tuple: two outputs

for_each(gait_events, inputs={...}, outputs=[HeelStrikes, ToeOffs], ...)
```

///

/// tab | MATLAB

```matlab
function [heel_strikes, toe_offs] = gait_events(heel_marker, fs_hz)
    ...
end

scidb.for_each(@gait_events, struct(...), {HeelStrikes(), ToeOffs()}, ...);
```

///

A function can return any value. SciStack stores the usual data types natively, in readable database columns, and gives them back with the same type:

| Python                                                     | MATLAB                                              |
| ---------------------------------------------------------- | --------------------------------------------------- |
| `bool`, `int`, `float`, `str` and their NumPy scalar types | numeric and `logical` scalars and arrays            |
| `np.ndarray` of any shape                                  | `string` (and `char`, which loads back as `string`) |
| `list` (with every element the same type)                  | `cell` arrays                                       |
| `dict`                                                     | `struct`                                            |
| `pd.DataFrame`                                             | `table`                                             |
| `datetime`, `date`, `timedelta`                            | `datetime`                                          |

A table that has schema-key columns (for example a `subject` column) is saved as one record per row, at each row's own location. That's how an import function turns one CSV into many records.

A few values change on the way through: a `tuple` comes back as a `list`, a MATLAB `categorical` as `string`, and a struct array as a cell array of structs. A list mixing types (`[1, "a"]`) can't be saved; use a `dict` or a DataFrame.

#### Saving your own classes

If a function returns an instance of **your own class** (a fitted model object, a custom `Trajectory`) and you want it saved, its variable has to say how to convert it to a table and back. Override two methods on the `BaseVariable` subclass:

| Method    | Signature                                                     | Must                                                                                                                       |
| --------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `to_db`   | `def to_db(self) -> pd.DataFrame`                             | Turn `self.data` (the value your function returned) into a DataFrame. Use ordinary column types: numbers, strings, arrays. |
| `from_db` | `@classmethod`<br>`def from_db(cls, df: pd.DataFrame) -> Any` | Rebuild the original value from that DataFrame. Whatever it returns is what `load()` and later steps receive.              |

Override both or neither. They are separate from `save()` and `load()`, which you call to store and fetch records and should never override.

/// tab | Python

```python
from dataclasses import dataclass
import numpy as np
import pandas as pd
from scidb import BaseVariable


@dataclass
class Trajectory:
    t: np.ndarray        # time, seconds
    xyz: np.ndarray      # N x 3 positions
    label: str


class MarkerTrajectory(BaseVariable):
    def to_db(self) -> pd.DataFrame:
        traj = self.data
        return pd.DataFrame({
            "t": traj.t,
            "x": traj.xyz[:, 0], "y": traj.xyz[:, 1], "z": traj.xyz[:, 2],
            "label": traj.label,          # repeated on every row
        })

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> Trajectory:
        return Trajectory(
            t=df["t"].to_numpy(),
            xyz=df[["x", "y", "z"]].to_numpy(),
            label=str(df["label"].iloc[0]),
        )
```

A function can now `return Trajectory(...)` with `outputs=[MarkerTrajectory]`, and steps that take `MarkerTrajectory` as an input receive a `Trajectory` again.

///

/// tab | MATLAB

Override the same two methods on the variable's `classdef`. `to_db` is an ordinary method that reads `obj.data`; `from_db` is `Static`, because it builds the value from a table, without an object to start from.

```matlab
% Trajectory.m: your own class
classdef Trajectory
    properties
        t      % time, seconds (N x 1)
        xyz    % N x 3 positions
        label  % string
    end
end
```

```matlab
% MarkerTrajectory.m: the variable, with its own storage
classdef MarkerTrajectory < scidb.BaseVariable
    methods
        function tbl = to_db(obj)
            traj = obj.data;
            n = numel(traj.t);
            tbl = table(traj.t(:), traj.xyz(:,1), traj.xyz(:,2), traj.xyz(:,3), ...
                        repmat(string(traj.label), n, 1), ...   % repeated on every row
                        'VariableNames', {'t', 'x', 'y', 'z', 'label'});
        end
    end
    methods (Static)
        function traj = from_db(tbl)
            traj = Trajectory();
            traj.t = tbl.t;
            traj.xyz = [tbl.x, tbl.y, tbl.z];
            traj.label = string(tbl.label(1));
        end
    end
end
```

A function can now return a `Trajectory` with `{MarkerTrajectory()}` as its output, and a step that takes `MarkerTrajectory()` as an input receives a `Trajectory` again. `save`, `save_from_table` and `load` convert the same way.

- **Override both or neither.** Defining only one, a `Static` `to_db`, or a non-`Static` `from_db` is an error (`scidb:CodecError`), as is a `to_db` that doesn't return a `table`.
- **The object is rebuilt for plain inputs.** `MarkerTrajectory()` and `scidb.Fixed(MarkerTrajectory(), ...)` hand your function a `Trajectory`. A column selection (`MarkerTrajectory("t")`), `for_columns`, or a `scidb.Merge` hands it the stored table's columns, as for any table variable.
- **Python and the GUI see the stored table.** They can't build a MATLAB object, so they read a `MarkerTrajectory` record as its `t, x, y, z, label` table.
- **To raise the schema version**, use `scidb.register_variable(MarkerTrajectory(), schema_version=2)`.

///

- **Without these methods, an object of your own class can't be saved.** It isn't a type SciStack knows how to store, and the save fails. (Returning it is fine when the result is only used in memory, for example from `scifor.for_each`, which saves nothing.)
- **Changing how a variable is stored is a change of type.** If you later change `to_db`/`from_db` so that old records no longer load correctly, also raise the class's `schema_version` (it starts at `1`).

### 4. No loops over subjects, sessions or trials

If your function has `for subject in subjects:`, but each iteration could be done in parallel/independent of the other iterations, that loop belongs to SciStack. Remove it, and write the function for one subject. Then:

- a crash for one subject doesn't stop the others; failures are counted and summarised at the end;
- `skip_computed=True` re-runs only what's out of date;
- adding a new subject means running the same call again, not editing code.

Loops over things _inside_ one location's data (samples, strides within a trial, columns of a table) are fine.

### 5. Read raw files only in loader functions, through `PathInput`

Functions should get their data as arguments. The exception is the step that reads your raw files from disk. Make that a small function that takes a **path** and returns the data, and let a [`PathInput`](../api/scifor.md#input-wrappers) build the path for each location:

/// tab | Python

```python
def load_emg(path):
    return pd.read_csv(path)["emg"].to_numpy()

EMG_FILE = PathInput("{subject}/{session}_emg.csv", root_folder="D:/study/raw", name="emg_file")
for_each(load_emg, inputs={"path": EMG_FILE}, outputs=[RawEMG],
         subject=[], session=[])
```

///

/// tab | MATLAB

```matlab
% load_emg.m
function emg = load_emg(path)
    emg = readtable(path).emg;
end
```

```matlab
emg_file = scidb.PathInput("{subject}/{session}_emg.csv", ...
    root_folder="D:/study/raw", name="emg_file");
scidb.for_each(@load_emg, struct("path", emg_file), {RawEMG()}, ...
    subject=[], session=[]);
```

///

**`subject=[], session=[]` works here too, even though nothing has been loaded yet.** An empty list always means "every value there is", and `for_each` works out those values before the first call:

- **`scidb.for_each`** takes the values already in the database and, for keys still empty, runs **file discovery**: it scans `root_folder` for files matching the template and reads `subject` and `session` from the matching file names.
- **`scifor.for_each`** reads the values from the schema-key columns (`subject`, `session`) of the table inputs, and fills any key still empty by the same file discovery.

When every template key is left empty, `for_each` runs only the combinations that have a file on disk. If `S01` and `S02` both have `BL` files but only `S01` has a `POST` file, it makes three calls, not four. List values explicitly (`subject=["S01", "S02"]`) only when you want to run a subset.

No hard-coded paths inside functions: a function that builds `f"C:/Users/me/{subject}.csv"` only works on your computer, and SciStack can't record which file it read.

### 6. Same inputs, same outputs (idempotent)

A function should depend only on its arguments. Avoid reading global variables, the current date, or files that aren't passed in. If it uses random numbers, take the seed as an argument. SciStack decides whether a result is up to date from the function's code and its recorded inputs. Anything else the function depends on is invisible to that check.

### 7. Files must be safe to import

To discover your functions, SciStack imports your Python files. A file that _does work_ at the top level (runs an analysis, draws figures, writes files) would do that work every time SciStack scans your project, so SciStack skips it and reports why. Keep functions and declarations at the top level, and put script code under a guard:

```python
def lowpass_envelope(signal, fs_hz, cutoff_hz):
    ...

if __name__ == "__main__":       # runs only when you run this file directly
    for_each(lowpass_envelope, ...)
```

In MATLAB, put each function in **its own `.m` file**, named after the function. SciStack identifies a MATLAB function by its file, so it can tell when the code changes.

### 8. Name prefixes give some functions a special role

| Prefix        | Role                                      | Notes                                                                |
| ------------- | ----------------------------------------- | -------------------------------------------------------------------- |
| `plot_`       | Draws a figure                            | Writes to a `PathOutput`; the figure is stamped with its provenance. |
| `stat_`       | Produces a statistics result              | Receives its inputs as tables by default (`as_table`).               |
| `glue_`       | A small reshaping step between two others | Its result is never saved. See [Glue](../api/scidb.md#glue).         |
| anything else | An ordinary processing step               |                                                                      |

## What your function receives: `as_table`

When `for_each` loads a variable for one call, it hands your function **just the data**, in the simplest form that fits:

| What matched at this location                                                                                     | Your function receives                             |
| ----------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| One record holding one value                                                                                      | The value itself: `0.72`, a NumPy array, a string. |
| Several records with one data column (e.g. every trial of a session, when iterating only `subject` and `session`) | One NumPy column of the values (shape `(N, 1)`).   |
| Data with several columns                                                                                         | A DataFrame of **only** the data columns.          |

The schema-key columns (`subject`, `session`, ...) are removed. That's usually what you want: a filter function shouldn't care which subject its signal came from.

Sometimes a function _does_ need to know where each row came from: to pair up rows by participant, pivot a table, or compare sessions. Pass **`as_table`**:

| Setting                   | Effect                                                                                                                                                                            |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `as_table=None` (default) | Data only, as above. `stat_` functions default to tables.                                                                                                                         |
| `as_table=True`           | Every loaded input arrives as a DataFrame (MATLAB: a table) **with** its schema-key columns. A scalar variable's values are in a column named after the variable, e.g. `PeakEMG`. |
| `as_table=["baseline"]`   | Only the named inputs arrive as tables; the others arrive as plain data.                                                                                                          |

/// tab | Python

```python
def session_change(peaks):
    """Every subject's peak at every session in, one row per subject out."""
    wide = peaks.pivot(index="subject", columns="session", values="PeakEMG")
    return pd.DataFrame({"subject": wide.index, "change": wide["POST"] - wide["BL"]})

for_each(session_change, inputs={"peaks": PeakEMG}, outputs=[PeakChange],
         as_table=True)          # no schema keys: one call, all the data
```

///

/// tab | MATLAB

```matlab
% session_change.m
function out = session_change(peaks)
    % Every subject's peak at every session in, one row per subject out.
    wide = unstack(peaks(:, ["subject", "session", "PeakEMG"]), "PeakEMG", "session");
    out = table(wide.subject, wide.POST - wide.BL, VariableNames=["subject", "change"]);
end
```

```matlab
scidb.for_each(@session_change, struct("peaks", PeakEMG()), {PeakChange()}, ...
    as_table=true);
```

///

Because the returned table has a `subject` column, each row is saved as its own `PeakChange` record at that subject.

/// tip | A rule of thumb
If your function would need to _ask_ "which subject is this?", give it `as_table`. If it wouldn't, don't: a function that works on plain arrays is easier to test and reuse.
///

## Pros and cons of this shape

|                                                |                                                                                                                                                                             |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ✅ **Every result is traceable**               | Each saved value records the function, input records and settings that produced it. Months later you can answer "which cutoff did I use for that figure?"                   |
| ✅ **Re-runs are cheap and safe**              | Change one step or add one subject, and only the affected results are recomputed.                                                                                           |
| ✅ **One failure doesn't stop the run**        | A bad trial is reported, and the rest still run.                                                                                                                            |
| ✅ **Trying alternatives is built in**         | `Parameter(10, 20)` runs both and keeps both, side by side.                                                                                                                 |
| ✅ **Functions are easy to test**              | A function of its arguments can be tested on a tiny array, without the dataset.                                                                                             |
| ✅ **Functions are reusable**                  | `lowpass_envelope` works in any project, with or without SciStack.                                                                                                          |
| ⚠️ **More, smaller functions**                 | One long script becomes several functions, plus the `for_each` calls that connect them.                                                                                     |
| ⚠️ **Variables to declare**                    | Each intermediate result is a named variable (a one-line class, or one line in the entities file).                                                                          |
| ⚠️ **Data must be addressable by schema keys** | Every value has to live at a location (`subject`, `session`, ...). Deciding the [schema](whats-a-schema.md) takes some thought up front.                                    |
| ⚠️ **Less freedom inside a step**              | A step can't reach across to other subjects' data unless you ask for it, by iterating fewer keys or using `Fixed`. That's deliberate, but it can feel restrictive at first. |

## Reusable code and project-specific code

Not every function deserves the same care. Analysis code tends to fall into two kinds, and it helps to treat them differently.

**Project-independent code** does a general job: filter a signal, detect peaks, compute a range of motion, fit a curve. Any study with that kind of data could use it. Write it well:

- arguments in general terms (`signal`, `fs_hz`), never your project's variable names;
- no assumptions about your schema, file layout or units, unless they are arguments;
- a docstring, and ideally a test on a small made-up input.

Collect these in a module (or your lab's own package) and reuse them across projects. A shared package can be listed under `packages` in [`scistack.toml`](project-files.md#packages) so its functions appear in every project that uses it.

**Project-specific code** does something only this study will ever need: read your motion-capture system's export format, apply the trial-exclusion rules from your protocol, rename columns to match a collaborator's spreadsheet, pick the three middle trials because that's what your methods section says. **It doesn't need to be general.** It's fine to hard-code your column names, your trial numbers, or your file's peculiar header layout. Making this kind of code "configurable" adds work and makes it harder to read, for flexibility nobody will use.

The useful habit is to keep the two apart: a **thin project-specific function** that adapts your data, calling **general functions** that do the real work.

```python
# Project-specific: knows this lab's export format. Fine to hard-code.
def load_vicon_trial(path):
    df = pd.read_csv(path, skiprows=4)                 # Vicon puts 4 header lines first
    return df.rename(columns={"LHEE:Z": "heel_z"})

# General: knows nothing about Vicon, this study, or SciStack.
def heel_strikes(heel_z, fs_hz, min_gap_s=0.4):
    peaks, _ = find_peaks(-heel_z, distance=int(min_gap_s * fs_hz))
    return peaks / fs_hz
```

Both are ordinary SciStack steps. Only the second goes into your reusable module.

## Converting an existing script

Most analysis scripts look something like this: nested loops over conditions, a file read, some processing with numbers typed in, and results collected into a spreadsheet.

/// tab | Python

```python
# old_analysis.py
import numpy as np
import pandas as pd
from scipy.signal import butter, filtfilt

rows = []
for subject in ["S01", "S02", "S03"]:
    for session in ["BL", "POST"]:
        df = pd.read_csv(f"D:/study/raw/{subject}/{session}_emg.csv")
        emg = df["emg"].to_numpy()
        b, a = butter(4, 20 / (1000 / 2), "low")
        env = filtfilt(b, a, np.abs(emg - emg.mean()))
        rows.append({"subject": subject, "session": session, "peak": env.max()})

pd.DataFrame(rows).to_csv("peaks.csv", index=False)
```

///

/// tab | MATLAB

```matlab
% old_analysis.m
subjects = ["S01", "S02", "S03"];
sessions = ["BL", "POST"];
peaks = table();
for s = subjects
    for ses = sessions
        df = readtable("D:/study/raw/" + s + "/" + ses + "_emg.csv");
        [b, a] = butter(4, 20 / (1000 / 2), "low");
        env = filtfilt(b, a, abs(df.emg - mean(df.emg)));
        peaks = [peaks; table(s, ses, max(env), VariableNames=["subject", "session", "peak"])];
    end
end
writetable(peaks, "peaks.csv");
```

///

Convert it in six steps:

1. **The loops become the schema.** `for subject` and `for session` mean the schema is `["subject", "session"]`.
2. **The file read becomes a loader function plus a `PathInput`.** The f-string `f"D:/study/raw/{subject}/{session}_emg.csv"` is already a `PathInput` template.
3. **Each block of work inside the loop becomes a function**, written for one subject and session. Here: filtering (`lowpass_envelope`) and taking the peak (`peak_value`).
4. **Typed-in numbers become arguments**: the sampling rate `1000` and the cutoff `20`.
5. **Each intermediate result gets a variable name**: `RawEMG`, `EmgEnvelope`, `PeakEMG`.
6. **The spreadsheet becomes an export.** Results are in the database; write a CSV whenever you want one.

/// tab | Python

```python
# emg_tools.py -- general, reusable in any project
import numpy as np
from scipy.signal import butter, filtfilt

def lowpass_envelope(signal, fs_hz, cutoff_hz):
    """Rectify and low-pass filter one EMG recording."""
    b, a = butter(4, cutoff_hz / (fs_hz / 2), "low")
    return filtfilt(b, a, np.abs(signal - np.mean(signal)))

def peak_value(signal):
    return float(np.max(signal))
```

```python
# pipeline.py -- this project
import pandas as pd
import scidb
from scidb import BaseVariable, Parameter, PathInput, for_each
from emg_tools import lowpass_envelope, peak_value

class RawEMG(BaseVariable): pass
class EmgEnvelope(BaseVariable): pass
class PeakEMG(BaseVariable): pass

FS_HZ = Parameter(1000)
CUTOFF_HZ = Parameter(20)
EMG_FILE = PathInput("{subject}/{session}_emg.csv", root_folder="D:/study/raw", name="emg_file")

def load_emg(path):
    """This study's EMG export: one 'emg' column."""
    return pd.read_csv(path)["emg"].to_numpy()

if __name__ == "__main__":
    scidb.configure_database("study.duckdb", ["subject", "session"])

    for_each(load_emg, inputs={"path": EMG_FILE}, outputs=[RawEMG],
             subject=["S01", "S02", "S03"], session=["BL", "POST"])
    for_each(lowpass_envelope,
             inputs={"signal": RawEMG, "fs_hz": FS_HZ, "cutoff_hz": CUTOFF_HZ},
             outputs=[EmgEnvelope], subject=[], session=[])
    for_each(peak_value, inputs={"signal": EmgEnvelope}, outputs=[PeakEMG],
             subject=[], session=[])

    PeakEMG.to_csv("peaks.csv")
```

///

/// tab | MATLAB

```matlab
% lowpass_envelope.m -- general, reusable in any project
function env = lowpass_envelope(signal, fs_hz, cutoff_hz)
    [b, a] = butter(4, cutoff_hz / (fs_hz / 2), "low");
    env = filtfilt(b, a, abs(signal - mean(signal)));
end
```

```matlab
% peak_value.m -- general
function p = peak_value(signal)
    p = max(signal);
end
```

```matlab
% load_emg.m -- this study's EMG export: one 'emg' column
function emg = load_emg(path)
    emg = readtable(path).emg;
end
```

```matlab
% RawEMG.m (and likewise EmgEnvelope.m, PeakEMG.m)
classdef RawEMG < scidb.BaseVariable
end
```

```matlab
% pipeline.m -- this project
scidb.configure_database("study.duckdb", ["subject", "session"]);

fs_hz = scidb.Parameter(1000);
cutoff_hz = scidb.Parameter(20);
emg_file = scidb.PathInput("{subject}/{session}_emg.csv", ...
    root_folder="D:/study/raw", name="emg_file");

scidb.for_each(@load_emg, struct("path", emg_file), {RawEMG()}, ...
    subject=["S01", "S02", "S03"], session=["BL", "POST"]);
scidb.for_each(@lowpass_envelope, ...
    struct("signal", RawEMG(), "fs_hz", fs_hz, "cutoff_hz", cutoff_hz), ...
    {EmgEnvelope()}, subject=[], session=[]);
scidb.for_each(@peak_value, struct("signal", EmgEnvelope()), {PeakEMG()}, ...
    subject=[], session=[]);
```

///

The new version is longer, and that's the trade. What you get for it:

- **Try a second cutoff** with `CUTOFF_HZ = Parameter(20, 50)`. Both envelopes and both sets of peaks are kept, and each peak knows which cutoff it came from.
- **Add subject S04**: add it to the first call and run again. With `skip_computed=True`, S01–S03 are not recomputed.
- **Fix a bug in `lowpass_envelope`**: SciStack sees the code changed, so the step and everything after it show as needing a re-run. The old results are kept, labelled with the code version that produced them, until you choose otherwise.
- **Ask where a number came from**: `db.get_provenance(PeakEMG, subject="S02", session="POST")` leads back through the envelope to the exact raw file.

Constants such as `FS_HZ`, `CUTOFF_HZ` and `EMG_FILE` can also live in the project's [entities file](project-files.md#scistack_entitiestoml), which Python, MATLAB and the GUI all read. That's where the GUI puts them when you create them on the canvas.

### Then: a group-level step

The old script stopped at a spreadsheet, and the statistics happened somewhere else. In SciStack, the next step is just another function. To compare sessions, the function needs every subject at once, and it needs to know which value belongs to which subject and session, so it uses `as_table` (see [`session_change`](#what-your-function-receives-as_table) above):

```python
for_each(session_change, inputs={"peaks": PeakEMG}, outputs=[PeakChange], as_table=True)
```

With no schema keys, `for_each` makes **one** call with all of the data, and saves one `PeakChange` per subject (from the table's `subject` column).

### Common conversion snags

| In the old script                                                  | In SciStack                                                                            |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| A results list you append to, then save                            | Return the value. `for_each` collects and saves.                                       |
| `if subject == "S07": continue` (a known bad recording)            | Exclude it once with `exclude_schema(reason=..., subject="S07")`; every step skips it. |
| A `try/except` around each subject so the script keeps going       | Not needed. Failures are caught per location and summarised.                           |
| Global settings at the top of the script                           | Arguments, or `Parameter`s in the entities file.                                       |
| Two scripts that each re-read the raw files                        | One loader step; later steps read its saved variable.                                  |
| A step that needs another session's data, e.g. "subtract baseline" | `Fixed(MyVar, session="BL")` as a second input pins that input to BL.                  |
| A figure saved with `plt.savefig(f"{subject}.png")`                | A `plot_` function with a `PathOutput`.                                                |

## Next steps

- [Best Popcorn - scidb](../examples/best-popcorn-scidb.md): a complete small analysis in this style.
- [scidb API](../api/scidb.md): `for_each` options, input wrappers and variables.
- [Project Files](project-files.md): where variables, parameters and file paths can be declared.
