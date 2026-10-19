function py_dict = metadata_to_pydict(varargin)
%METADATA_TO_PYDICT  Convert MATLAB name-value pairs to a Python dict.
%
%   py_dict = scidb.internal.metadata_to_pydict('subject', 1, 'session', 'A')
%   returns py.dict({"subject": 1, "session": "A"})

    if mod(numel(varargin), 2) ~= 0
        error('scidb:InvalidMetadata', ...
            'Metadata must be specified as name-value pairs.');
    end

    py_dict = py.dict();
    for i = 1:2:numel(varargin)
        key = varargin{i};
        val = varargin{i+1};

        if isstring(key)
            key = char(key);
        end

        % Convert MATLAB values to Python-compatible types
        if isstring(val) && isscalar(val)
            py_dict{key} = char(val);
        elseif isstring(val) && ~isscalar(val)
            % String array (non-scalar) → Python list for "match any"
            py_dict{key} = py.list(cellfun(@char, num2cell(val), 'UniformOutput', false));
        elseif isnumeric(val) && isscalar(val)
            % Whole numbers → py.int (scidb.internal.number_to_python)
            py_dict{key} = scidb.internal.number_to_python(val);
        elseif isnumeric(val) && ~isscalar(val)
            % Numeric array (non-scalar) → Python list for "match any"
            py_dict{key} = scidb.internal.number_to_python(val).tolist();
        elseif ischar(val)
            py_dict{key} = py.str(val);
        elseif isstruct(val)
            % Structs may contain arrays that aren't JSON-serializable on
            % the Python side.  Encode the whole struct as a JSON string so
            % it travels safely through json.dumps in save_batch.
            py_dict{key} = jsonencode(val);
        else
            try
                py_dict{key} = val;
            catch
                py_dict{key} = jsonencode(val);
            end
        end
    end
end
