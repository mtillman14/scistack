Revised User-Facing API

The entire MATLAB surface area is the scidb namespace:

%% Setup
scidb.configure_database('experiment.duckdb', ["subject", "session"], 'pipeline.db');
scidb.register_variable('RawSignal');
scidb.register_variable('ProcessedSignal');

%% Save raw data
scidb.save('RawSignal', randn(100,3), subject=1, session='A');

%% Load, process, save with lineage
raw = scidb.load('RawSignal', subject=1, session='A');

filter_fn = scidb.Thunk(@bandpass_filter);
result = filter_fn(raw, 10, 200);

scidb.save('ProcessedSignal', result, subject=1, session='A');

%% Second run — cache hit, no computation
raw = scidb.load('RawSignal', subject=1, session='A');
result = filter_fn(raw, 10, 200); % instant

Does the user even need a classdef?

With Option B, the type name is just a string. For the common case (arrays, scalars), no classdef is needed at all. scidb.register_variable('RawSignal') creates a Python surrogate, and that's sufficient.

A classdef only becomes necessary for custom serialization:

classdef RotationMatrix < scidb.BaseVariable
methods
function df = to_db(obj)
% ... custom multi-column serialization
end
end
methods (Static)
function data = from_db(df)
% ... custom deserialization
end
end
end

But that's an advanced case. The default path — register_variable with a string — covers the vast majority of scientific computing (arrays, scalars, dicts).

Complete Function Signatures

% +scidb/configure_database.m
function configure_database(dataset_db_path, dataset_schema_keys, pipeline_db_path, options)
arguments
dataset_db_path string
dataset_schema_keys string % ["subject", "session"]
pipeline_db_path string
options.lineage_mode string = "strict"
end

% +scidb/register_variable.m
function register_variable(type_name, options)
arguments
type_name string
options.schema_version double = 1
end

% +scidb/save.m
function record_id = save(type_name, data, varargin)
% type_name: string
% data: MATLAB array, scalar, scidb.ThunkOutput, or scidb.BaseVariable
% varargin: name=value metadata pairs

% +scidb/load.m
function var = load(type_name, varargin)
% Returns scidb.BaseVariable with .data, .record_id, .metadata, .py_obj

% +scidb/load_all.m
function vars = load_all(type_name, varargin)
% Returns array of scidb.BaseVariable

% +scidb/list_versions.m
function versions = list_versions(type_name, varargin)
% Returns struct array with record_id, schema, version, created_at

% +scidb/provenance.m
function prov = provenance(type_name, varargin)
% Returns struct with function_name, function_hash, inputs, constants

What scidb.load Returns

Since there's no user-defined classdef in the common case, load returns a generic scidb.BaseVariable:

var = scidb.load('RawSignal', subject=1, session='A');
var.data % double array (100x3)
var.record_id % "a3f8c2e1b9d04710"
var.metadata % struct with subject=1, session='A'
class(var) % 'scidb.BaseVariable'

The loaded object carries its Python shadow (var.py_obj), so when passed to a Thunk, input classification works automatically.

The Thunk Call Flow (revised for Option B)

raw = scidb.load('RawSignal', subject=1, session='A');
% raw.data = 100x3 double
% raw.py_obj = Python BaseVariable (with \_record_id, \_lineage_hash, etc.)

t = scidb.Thunk(@bandpass_filter);
result = t(raw, 10, 200);
% result.data = 100x3 double (filtered)
% result.py_obj = Python ThunkOutput (with full lineage graph)

scidb.save('ProcessedSignal', result, subject=1, session='A');
% Passes result.py_obj (Python ThunkOutput) to Python's save_variable()
% Python extracts lineage, computes hashes, saves to DuckDB + SQLite

What happens inside t(raw, 10, 200):

Step 1: Build Python inputs dict
─────────────────────────────────
"arg_0" → raw.py_obj % Python BaseVariable (has \_record_id, \_lineage_hash)
"arg_1" → 10 % Python int (auto-converted by py.)
"arg_2" → 200 % Python int

Step 2: Create proxy + check cache (all in Python)
───────────────────────────────────────────────────
py_pt = MatlabPipelineThunk(py_thunk, py_inputs)

    py_pt.compute_lineage_hash():
      classify_inputs({"arg_0": <BaseVariable>, "arg_1": 10, "arg_2": 200})
        → arg_0: has _record_id + _lineage_hash → THUNK_OUTPUT ("arg_0", "output", "a3f8...")
          OR: has _record_id, no _lineage_hash → SAVED_VARIABLE ("arg_0", "lineage", "b2c9...")
        → arg_1: int → CONSTANT ("arg_1", "value", canonical_hash(10))
        → arg_2: int → CONSTANT ("arg_2", "value", canonical_hash(200))
      hash_input = f"{thunk.hash}-{input_tuples}"
      → lineage_hash = sha256(hash_input)

    Thunk.query.find_by_lineage(py_pt)
      → queries PipelineDB with lineage_hash

Step 3a: Cache HIT
──────────────────
Python returns list of cached values (numpy arrays)
MATLAB wraps each in ThunkOutput:
result.data = double(cached_numpy) % MATLAB array
result.py_obj = ThunkOutput(py_pt, 0, True, cached_numpy) % Python shadow

Step 3b: Cache MISS
───────────────────
Unwrap inputs to raw MATLAB data:
raw.data → 100x3 double
10 → 10
200 → 200

    Execute: feval(@bandpass_filter, raw_data, 10, 200)
      → result_matlab = 100x3 double

    Convert result to numpy:
      result_numpy = py.numpy.array(result_matlab)

    Create Python ThunkOutput:
      py_to = bridge.make_thunk_output(py_pt, 0, result_numpy)

    Wrap in MATLAB ThunkOutput:
      result.data = result_matlab
      result.py_obj = py_to

Chained Thunks

The shadow system composes naturally across thunk chains:

raw = scidb.load('RawSignal', subject=1, session='A');

t1 = scidb.Thunk(@bandpass_filter);
t2 = scidb.Thunk(@normalize);

filtered = t1(raw, 10, 200); % ThunkOutput with Python shadow
normed = t2(filtered); % filtered.py_obj is a Python ThunkOutput

When t2(filtered) runs:

- filtered is a MATLAB scidb.ThunkOutput
- Marshalling extracts filtered.py_obj → a real Python ThunkOutput
- Python's classify_input sees isinstance(value, ThunkOutput) → True
- Reads value.hash → the lineage hash from t1's computation
- This becomes an input tuple ("arg_0", "output", "<t1_hash>")
- The lineage hash for t2 incorporates t1's hash → full provenance chain

Then when saving:
scidb.save('NormedSignal', normed, subject=1, session='A');

- normed.py_obj is a ThunkOutput whose pipeline_thunk.inputs["arg_0"] is the ThunkOutput from t1
- extract_lineage() walks this graph to build the LineageRecord
- find_unsaved_variables() traverses up through t1's inputs to check for unsaved intermediates (strict mode)

All of this happens in existing Python code, unchanged.

Cross-Language Interop

Python saves data → MATLAB loads it → MATLAB thunk consumes it → saves result → Python loads result. It works because the lineage hashes are opaque strings stored in the database:

Python: @thunk process(raw) → save as ProcessedSignal
stored \_lineage_hash = "abc123..."

MATLAB: var = scidb.load('ProcessedSignal', subject=1)
var.py_obj.\_lineage_hash = "abc123..." (read from DB)

           t = scidb.Thunk(@further_analysis);
           result = t(var);
             → classify_input sees _lineage_hash → ("arg_0", "output", "abc123...")
             → lineage_hash for t = sha256("matlab_thunk_hash-[('arg_0', 'output', 'abc123...')]")

           scidb.save('FinalResult', result, subject=1)
             → lineage record links back to ProcessedSignal via record_id

MATLAB thunks can't cache-hit against Python thunks (different function hashes), but the lineage graph is continuous across languages. Provenance queries show the full chain.

File Layout

sci-matlab/
├── +scidb/
│ ├── configure_database.m % → py.scidb.configure_database(...)
│ ├── register_variable.m % → py.sci_matlab.bridge.register_matlab_variable(...)
│ ├── save.m % → py.scidb.database.get_database().save_variable(...)
│ ├── load.m % → py.scidb.database.get_database().load(...)
│ ├── load_all.m % → py.scidb.database.get_database().load_all(...)
│ ├── list_versions.m % → py.scidb.database.get_database().list_versions(...)
│ ├── provenance.m % → py.scidb.database.get_database().get_provenance(...)
│ ├── Thunk.m % MATLAB class (~120 lines)
│ ├── ThunkOutput.m % MATLAB class (~20 lines)
│ ├── BaseVariable.m % MATLAB class (~40 lines, for loaded data + custom serialization)
│ └── +internal/
│ ├── to_python.m % MATLAB→numpy conversion
│ ├── from_python.m % numpy→MATLAB conversion
│ ├── to_python_input.m % Extract .py_obj or convert
│ ├── unwrap_input.m % Extract .data for MATLAB execution
│ ├── hash_function.m % Read .m source + py.hashlib.sha256
│ └── parse_metadata.m % varargin name=value → Python-compatible pairs
│
├── scidb_matlab/ % Python package
│ ├── **init**.py
│ └── bridge.py % ~80 lines: MatlabThunk, MatlabPipelineThunk, helpers
│
└── tests/

Open Design Questions

1. unpack_output syntax. Python uses @thunk(unpack_output=True). In MATLAB:
   t = scidb.Thunk(@split_function, unpack_output=true);
   [first, second] = t(data);
   This works naturally with MATLAB's [a, b] = f(x) multiple-output syntax.

2. Anonymous function hashing. @(x) x * 2 has no .m file. func2str gives "@(x)x*2". Hashing that string works but is fragile — whitespace changes produce different hashes. An option is to require named functions for thunks
   and error on anonymous ones.

3. Thunk decorator equivalent. Python has @thunk to permanently wrap a function. MATLAB equivalent could be:
   % Option: wrap at definition time
   bandpass_filter = scidb.Thunk(@bandpass_filter);

% Then use directly
result = bandpass_filter(raw, 10, 200);
This shadows the original function. It's idiomatic MATLAB — function handles are reassignable.

4. py. environment setup. MATLAB needs pyenv configured to point at the right Python with scidb packages installed. This is a one-time setup cost. scidb.configure_database could verify this upfront and give a clear error
   message.

Want me to start implementing any of these components?
