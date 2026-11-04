# Choosing Your Layer

<!-- Ground truth (tests win over prose). Verified against:
     scifor/README.md and scifor/tests/test_foreach_standalone.py (set_schema, for_each,
        inputs=, returns DataFrame);
     scidb/README.md, scidb/tests/test_integration.py and
        docs/examples/best-popcorn-scidb.md (configure_database, BaseVariable,
        save/load, record .data, for_each, get_provenance, MATLAB spellings);
     scidb/src/scidb/run/foreach.py (skip_computed defaults to False);
     scistack-gui/README.md (what the GUI does, one database per backend process);
     scistack/pyproject.toml (`pip install scistack` pulls in every package).
     Roles and dependencies are owned by concepts/architecture.md; link there. -->

SciStack gives you three ways to work: two code libraries, `scifor` and `scidb`,
and a GUI built on top of them. Start with the lightest one that does the job and
move up when you want more. `pip install scistack` installs all three, so this
choice doesn't affect your installation.

## Quick chooser

| You want to… | Use | Languages |
|---|---|---|
| Replace nested `for` loops, with your data already in tables and nothing saved between runs | **`scifor`** | Python, MATLAB |
| Save every result in a database, with its versions and a record of how it was made | **`scidb`** | Python, MATLAB |
| See the pipeline as a diagram, run it from there, and make figures and statistics without writing that code | **SciStack GUI** | Python and MATLAB functions |

If you're unsure, start lower than you think you need. Moving up doesn't require
rewriting your analysis functions.

## scifor: batch iteration, no database

**What it does:** runs a function you already wrote across every combination of
your experimental conditions. You say which columns are conditions and which
inputs to slice. scifor filters each table to the matching rows, calls your
function, and collects the results into one table. It doesn't create or use a
database.

**Choose this when:** your data is already in a DataFrame or MATLAB table, you
want to stop writing loops that slice, call, and collect, and you don't need to
keep anything between runs.

/// tab | Python
```python
from scifor import set_schema, for_each

set_schema(["subject", "session"])

results = for_each(
    my_analysis,
    inputs={"emg": data_table, "cutoff_hz": 20},  # cutoff_hz is a constant
    subject=[1, 2, 3],
    session=["pre", "post"],
)
# results is a DataFrame with one row per (subject, session)
```
///

/// tab | MATLAB
```matlab
scifor.set_schema(["subject", "session"]);

results = scifor.for_each(@my_analysis, ...
    struct('emg', data_table, 'cutoff_hz', 20), ...
    subject=[1, 2, 3], session=["pre", "post"]);
```
///

Your function only receives data and returns a result. It doesn't need to know
about the looping or filtering.

**Full example:** [Best Popcorn - scifor](../examples/best-popcorn-scifor.md)

## scidb: results that are saved and traceable

**What it adds:** a database of named *variables*. `scidb.for_each` uses scifor
for the looping, but loads its inputs from the database and saves its outputs
back, so you pass variable types instead of tables. Each result also records how
it was made: the function, the data it read, and the settings passed in.
`skip_computed=True` skips combinations whose results are already up to date.

**Choose this when:** you'll come back to the results later, change the analysis
and want to keep both versions, or need to show where a number came from.

/// tab | Python
```python
from scidb import BaseVariable, configure_database, for_each
import numpy as np

class RawSignal(BaseVariable):
    schema_version = 1

class ProcessedSignal(BaseVariable):
    schema_version = 1

db = configure_database("experiment.duckdb", ["subject", "session"])

RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")

for_each(
    process_signal,
    inputs={"raw": RawSignal},
    outputs=[ProcessedSignal],
    subject=[1],
    session=["A"],
)

print(ProcessedSignal.load(subject=1, session="A").data)  # the saved result
print(db.get_provenance(ProcessedSignal, subject=1, session="A"))  # how it was made
```
///

/// tab | MATLAB
MATLAB needs each variable in its own file, such as `RawSignal.m`:

```matlab
classdef RawSignal < scidb.BaseVariable
end
```

Then, with `ProcessedSignal.m` declared the same way:

```matlab
scidb.configure_database("experiment.duckdb", ["subject", "session"]);

RawSignal().save([1 2 3], subject=1, session="A");

scidb.for_each(@process_signal, ...
    struct('raw', RawSignal()), ...
    {ProcessedSignal()}, ...
    subject=[1], session=["A"]);

disp(ProcessedSignal().load(subject=1, session="A").data)  % the saved result
p = ProcessedSignal().provenance(subject=1, session="A");  % how it was made
```
///

**Full example:** [Best Popcorn - scidb](../examples/best-popcorn-scidb.md)

## SciStack GUI: see and drive the pipeline

**What it adds:** a VS Code extension, which can also run in a browser, that draws
your pipeline as a diagram. From there you can run one step or all of them, see
which results are up to date, compare versions of a result, and find missing data.
Plot Studio and Stat Studio make figures and run statistics without you writing
that code. You still write your analysis functions in Python or MATLAB.

**Choose this when:** you want to see the whole pipeline at once, or would rather
make figures and choose statistics by clicking than by coding.

**Full example:** the [Best Popcorn GUI walkthrough](https://scistack.readthedocs.io/en/latest/ui/examples/best-popcorn/)

## Moving up

- **From scifor to scidb:** keep your function as it is. Declare variables,
  configure a database, and replace the tables in `inputs=` with variable types,
  adding an `outputs=` list.
- **From scidb to the GUI:** open the same database in the GUI. Your functions and
  results stay as they are, and you can keep running code as well.

For what each level is responsible for and what it depends on, see
[Architecture](../concepts/architecture.md). To get set up, see
[Installation](installation.md), plus [MATLAB Setup](../matlab-setup.md) if you
use MATLAB.
