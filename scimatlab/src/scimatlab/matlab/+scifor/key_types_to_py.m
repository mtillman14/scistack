function py_key_types = key_types_to_py(key_types)
%KEY_TYPES_TO_PY  Schema-key types struct -> py.dict, or py.None when empty.
%
%   PY = scifor.key_types_to_py(struct('trial', 'numeric'))
%
%   The one MATLAB -> Python conversion of a schema_key_types= option, used
%   by scifor.for_each, scidb.for_each and scidb.configure_database. Values
%   are validated Python-side (scifor.schema_types.validate_key_types /
%   scidb.schema_types.validate). See docs/claude/schema-key-types.md.

    if nargin < 1 || isempty(key_types) || ~isstruct(key_types) ...
            || isempty(fieldnames(key_types))
        py_key_types = py.None;
        return;
    end
    fields = fieldnames(key_types);
    py_key_types = py.dict();
    for i = 1:numel(fields)
        py_key_types.update(pyargs(fields{i}, ...
            char(string(key_types.(fields{i})))));
    end
end
