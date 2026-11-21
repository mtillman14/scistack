function [py_keys, py_scope] = exclusion_args(fn_name, varargin)
%EXCLUSION_ARGS  Split exclude/include_schema arguments into keys and scope.
%
%   [py_keys, py_scope] = scidb.internal.exclusion_args('exclude_schema', ...
%       'subject', 1, 'trial', 2, 'variable', "EMG")
%
%   The one owner of how the MATLAB wrappers read their name-value pairs
%   (scidb.exclude_schema, scidb.include_schema):
%
%   - Every pair is a schema key, its value converted by number_to_python
%     (whole numbers -> int, the rule the rest of the wrapper uses).
%   - 'variable', V is the SCOPE (GitHub #28): only that variable is bad or
%     absent there (Python's variable=, the CLI's --variable). V is a
%     variable name or a BaseVariable instance. Omitted = everything.
%   - In a dataset with a schema key literally named 'variable', the pair
%     stays a schema key (as before #28) and no scope can be given here.
%
%   py_keys is a py.dict; py_scope is a py.str or py.None.

    if mod(numel(varargin), 2) ~= 0
        error(['scidb:' fn_name], ...
              'Schema keys must be provided as name-value pairs.');
    end

    scope_is_key = any(string(cell( ...
        py.scidb.store.database.get_database().dataset_schema_keys)) == "variable");

    py_keys = py.dict();
    py_scope = py.None;
    for i = 1:2:numel(varargin)
        key = char(string(varargin{i}));
        val = varargin{i + 1};
        if strcmp(key, 'variable') && ~scope_is_key
            names = cell(scidb.internal.resolve_var_names(val));
            if numel(names) ~= 1
                error(['scidb:' fn_name], ...
                      '''variable'' takes one variable, got %d.', numel(names));
            end
            py_scope = py.str(char(names{1}));
            continue
        end
        is_whole = false;
        if isnumeric(val) && isscalar(val)
            [py_num, is_whole] = scidb.internal.number_to_python(val);
        end
        if is_whole
            py_keys{key} = py_num;
        else
            py_keys{key} = py.str(char(string(val)));
        end
    end

    if isa(py_scope, 'py.NoneType')
        scope_text = 'everything';
    else
        scope_text = char(py_scope);
    end
    scifor.Log.debug('scidb.%s: %d key(s), scope %s', ...
        fn_name, int32(py.len(py_keys)), scope_text);
end
