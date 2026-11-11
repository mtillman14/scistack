Plan: Round-trip arbitrarily nested scalar structs

Context

MATLAB tables saved through scidb-matlab can contain columns with scalar struct values (possibly deeply nested). The current to_python.m only handles scalar structs — when a struct column is
extracted from a table, it becomes a struct array (non-scalar), which hits the unsupported type error. The reverse path (from_python.m) also needs fixes to properly reconstruct structs from Python
dicts when loading DataFrame columns.

Files to modify

1.  scidb-matlab/src/scidb_matlab/matlab/+scidb/+internal/to_python.m
2.  scidb-matlab/src/scidb_matlab/matlab/+scidb/+internal/from_python.m
3.  scidb-matlab/src/scidb_matlab/matlab/+scidb/BaseVariable.m (save_from_table metadata columns)

Changes

1.  to_python.m — struct array support

Add a case for non-scalar struct arrays, converting to a Python list of dicts. The existing scalar struct handler already recurses into to_python for each field value, so nesting is handled
automatically.

elseif isstruct(data) && ~isscalar(data)
% Struct array -> Python list of dicts
py_obj = py.list();
for i = 1:numel(data)
py_obj.append(scidb.internal.to_python(data(i)));
end

Place this after the existing isstruct(data) && isscalar(data) case (line ~86).

2.  from_python.m — object-dtype numpy arrays and DataFrame columns

a) numpy ndarray handler — detect object dtype (dtype.kind == "O") and convert element-by-element instead of calling double():

elseif dtype_kind == "O"
% Object array -> cell array, convert each element
py_list = py_c.tolist();
c = cell(py_list);
data = cell(1, numel(c));
for i = 1:numel(c)
data{i} = scidb.internal.from_python(c{i});
end

Place this inside the existing ndarray handler, after the dtype_kind == "b" (bool) check.

b) DataFrame handler — detect object-dtype columns and convert element-by-element (same pattern as the existing datetime64 column handling):

elseif dtype_str == "object"
% Object column (e.g. dicts/structs) -> cell array via from_python
py_list = col.tolist();
c = cell(py_list);
col_data = cell(numel(c), 1);
for k = 1:numel(c)
col_data{k} = scidb.internal.from_python(c{k});
end
args{i} = col_data;

Place this in the DataFrame column loop, after the startsWith(dtype_str, "datetime") check.

3.  BaseVariable.m save_from_table — struct metadata columns

In the metadata column loop (~line 170), add handling for struct columns. Unlike data columns (which become the stored value), metadata columns need to be converted to something the Python bridge
can consume. Convert struct metadata to JSON strings:

elseif isstruct(col)
% Struct metadata column -> JSON strings (one per row)
json_strs = strings(numel(col), 1);
for si = 1:numel(col)
json_strs(si) = string(jsonencode(col(si)));
end
col = json_strs;

Place after the existing isdatetime(col) check.

Round-trip summary after changes
┌──────────────────────────────────────┬───────────────────────────────────────────┬──────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────┐
│ MATLAB input │ Python storage │ MATLAB output │ Fidelity │
├──────────────────────────────────────┼───────────────────────────────────────────┼──────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
│ Scalar struct struct("a", 1) │ dict {"a": 1} → JSON │ struct s.a = 1 │ Exact │
├──────────────────────────────────────┼───────────────────────────────────────────┼──────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
│ Nested struct struct("a", │ nested dict → JSON │ nested struct s.a.b = 1 │ Exact │
│ struct("b", 1)) │ │ │ │
├──────────────────────────────────────┼───────────────────────────────────────────┼──────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
│ Struct with numeric array field │ dict with list (ndarray at top-level │ struct with double array │ Exact at top level │
│ │ restored) │ │ │
├──────────────────────────────────────┼───────────────────────────────────────────┼──────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
│ Struct with nested struct+array │ dict with nested dict+list │ struct with nested struct, arrays become │ Lossy for nested ndarrays — sciduck only restores top-level │
│ │ │ cell/list │ ndarray_keys │
├──────────────────────────────────────┼───────────────────────────────────────────┼──────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
│ Table with struct column │ DataFrame with object-dtype column of │ Table with cell column of structs │ Cell column (not struct array) │
│ │ dicts │ │ │
└──────────────────────────────────────┴───────────────────────────────────────────┴──────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────┘
Known limitation

Numpy arrays inside nested dicts are not automatically restored by sciduck (only top-level ndarray_keys are tracked). This is a pre-existing sciduck limitation, not introduced by this change.
Fixing it would require recursive ndarray tracking in \_infer_duckdb_type/\_storage_to_python — out of scope.

Verification

- No automated MATLAB tests can run in this environment
- Manual verification: save a MATLAB table with a struct column, load it back, confirm structs are intact
- Verify nested struct fields survive: struct("a", struct("b", [1,2,3], "c", "hello"))
