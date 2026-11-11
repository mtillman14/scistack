Here is Claude's plan:
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
Plan: Multi-Result DataFrame/Table Return for load() and for_each()

Context

When for_each() iterates at a schema level higher than a variable's save level (e.g. iterating over subject but the variable is saved per trial), load() returns multiple results. Currently these
come back as a list of BaseVariable objects, which is awkward for the function to work with.

The user wants a tabular return format: pd.DataFrame in Python, MATLAB table in MATLAB.

Design Decisions

- Opt-in for load(): Add as_table=True parameter to BaseVariable.load(). Default is False (no breaking change).
- Opt-in for for_each(): Add as_table parameter (list of input names) to for_each(). Only specified inputs get DataFrame/table treatment. No automatic conversion.
- Non-scalar data: Arrays/objects are allowed in the data column (object dtype).
- DataFrame columns:
  - Schema key columns: one per key (e.g. subject, trial)
  - version_id: single column (integer)
  - Parameter key columns: one per parameter kwarg (e.g. smoothing, filter_hz)
  - Data column: named after the variable's view_name() (i.e. the class name)
- Thunk interaction: Out of scope for initial implementation.

Changes by File

1.  src/scidb/variable.py — BaseVariable

Add version_id and parameter_id properties (populated during load):

def **init**(self, data):
self.data = data
self.record_id = None
self.metadata = None
self.content_hash = None
self.lineage_hash = None
self.version_id = None # NEW
self.parameter_id = None # NEW

Add as_table parameter to load():

@classmethod
def load(cls, version="latest", loc=None, iloc=None, as_table=False, \*\*metadata):

When as_table=True and multiple results:

- Build a pd.DataFrame with schema key columns + version_id column + parameter key columns + data column (named cls.view_name())
- Return the DataFrame
- When as_table=True and single result: still return single BaseVariable

# In the multi-result branch:

if as_table:
rows = []
for var in results:
row = dict(var.metadata) if var.metadata else {}
row["version_id"] = var.version_id
row[cls.view_name()] = var.data
rows.append(row)
return pd.DataFrame(rows)
else:
return results # list, as before

2.  src/scidb/database.py — DatabaseManager.\_load_by_record_row()

Populate version_id and parameter_id on the loaded instance:

instance = variable_class(data)
instance.record_id = record_id
instance.metadata = flat_metadata
instance.content_hash = content_hash
instance.lineage_hash = lineage_hash
instance.version_id = version_id # NEW (already available from row)
instance.parameter_id = parameter_id # NEW (already available from row)

3.  scirun-lib/src/scirun/foreach.py — for_each()

Add as_table parameter (list of input names):

def for_each(
fn, inputs, outputs,
dry_run=False, save=True, pass_metadata=None,
as_table=None, # NEW: list of input names to load as DataFrame
\*\*metadata_iterables,
):

In the load loop, after loading an input, check if that input name is in as_table. If so, and if the result is a list, convert to DataFrame:

as_table_set = set(as_table) if as_table else set()

# ... in the load loop:

loaded_inputs[param_name] = var_type.load(\*\*load_metadata)

if param_name in as_table_set and isinstance(loaded_inputs[param_name], list):
loaded_inputs[param_name] = \_multi_result_to_dataframe(
loaded_inputs[param_name], var_type
)

Add helper:
def \_multi_result_to_dataframe(results, var_type):
import pandas as pd
view_name = var_type.view_name() if hasattr(var_type, 'view_name') else var_type.**name**
rows = []
for var in results:
row = dict(var.metadata) if var.metadata else {}
row["version_id"] = getattr(var, "version_id", None)
row[view_name] = var.data
rows.append(row)
return pd.DataFrame(rows)

In the unwrap section, skip DataFrame inputs (they don't have .data in the BaseVariable sense, so \_unwrap() already returns them unchanged — no change needed here).

4.  scidb-matlab/src/scidb_matlab/matlab/+scidb/BaseVariable.m — load()

Add as_table option parsing from varargin:

- Extract as_table from name-value args (default false)
- When as_table is true and n > 1:
  - Construct a MATLAB table from the array of ThunkOutput objects
  - Columns: metadata fields + version_id + variable class name (data)
  - Return the table

5.  scidb-matlab/src/scidb_matlab/matlab/+scidb/for_each.m

Add as_table option to the options parser (split_options).

After loading each input, if that input name is in as_table list and the result is an array with numel > 1, convert to MATLAB table.

Add local helper:
function tbl = multi_result_to_table(results, type_name)
n = numel(results);
meta_fields = fieldnames(results(1).metadata);
tbl = table();
for f = 1:numel(meta_fields)
col_data = cell(n, 1);
for i = 1:n
col_data{i} = results(i).metadata.(meta_fields{f});
end
tbl.(meta_fields{f}) = col_data;
end
% version_id column
vid_data = zeros(n, 1);
for i = 1:n
vid_data(i) = results(i).version_id; % need to populate this
end
tbl.version_id = vid_data;
% data column
data_col = cell(n, 1);
for i = 1:n
data_col{i} = results(i).data;
end
tbl.(type_name) = data_col;
end

In the unwrap section for non-Thunk functions, skip table inputs:
if loadable_idx(p) && ~istable(loaded{p})
loaded{p} = scidb.internal.unwrap_input(loaded{p});
end

6.  MATLAB ThunkOutput — expose version_id

scidb.BaseVariable.wrap_py_var() should also populate version_id and parameter_id on the ThunkOutput:
py_vid = py_var.version_id;
if ~isa(py_vid, 'py.NoneType')
v.version_id = int64(py_vid);
end

This requires adding version_id and parameter_id properties to scidb.ThunkOutput.

7.  Test Updates

tests/test_integration.py

- Test load(as_table=True): Save a scalar per trial for multiple trials. Call load(as_table=True, subject=1). Verify DataFrame columns (schema keys + version_id + data column named after view
  name), row count, and values.
- Test with version keys: Save with smoothing=0.2. Verify smoothing appears as column.
- Test single result: Only one match → returns BaseVariable, not DataFrame.
- Test array data: Save numpy arrays per trial, verify data column contains arrays.

scirun-lib/tests/ (for_each tests)

- Test as_table parameter: Set up schema ["subject", "trial"], save per trial, for_each over subject with as_table=["values"]. Verify function receives DataFrame.
- Test without as_table: Same setup, no as_table. Function receives list (current behavior).

MATLAB tests

- Similar tests for load(as_table=true) and for_each(... as_table=["values"]).

Example Usage

Python

db = configure_database("test.duckdb", ["subject", "trial"], "pipeline.db")

class StepLength(BaseVariable): pass

# Save per trial

for t in range(1, 6):
StepLength.save(t \* 0.1, subject=1, trial=t)

# Direct load as table

df = StepLength.load(as_table=True, subject=1)

# subject trial version_id StepLength

# 0 1 1 1 0.1

# 1 1 2 1 0.2

# 2 1 3 1 0.3

# 3 1 4 1 0.4

# 4 1 5 1 0.5

# for_each with as_table

for_each(
compute_mean,
inputs={"steps": StepLength},
outputs=[MeanStepLength],
as_table=["steps"], # <-- load "steps" as DataFrame
subject=[1, 2, 3],
)

MATLAB

df = StepLength().load(as_table=true, subject=1);
% Returns MATLAB table with same structure

scidb.for_each(@compute_mean, ...
struct('steps', StepLength()), ...
{MeanStepLength()}, ...
as_table=["steps"], ...
subject=[1 2 3]);

Key Files

- src/scidb/variable.py — BaseVariable (add version_id, parameter_id, as_table)
- src/scidb/database.py — DatabaseManager.\_load_by_record_row() (populate version_id)
- scirun-lib/src/scirun/foreach.py — for_each() (add as_table param + helper)
- scidb-matlab/src/scidb_matlab/matlab/+scidb/BaseVariable.m — load() as_table
- scidb-matlab/src/scidb_matlab/matlab/+scidb/for_each.m — as_table param
- scidb-matlab/src/scidb_matlab/matlab/+scidb/ThunkOutput.m — add version_id property
- tests/test_integration.py — Python tests

Implementation Order

1.  Add version_id / parameter_id to BaseVariable.**init**() and populate in \_load_by_record_row()
2.  Add as_table parameter to BaseVariable.load() (Python)
3.  Add as_table parameter + helper to for_each() (Python)
4.  Python tests
5.  Add version_id to MATLAB ThunkOutput, update wrap_py_var
6.  Add as_table to MATLAB BaseVariable.load()
7.  Add as_table to MATLAB for_each.m
8.  MATLAB tests
