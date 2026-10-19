function exclude_schema(reason, varargin)
%SCIDB.EXCLUDE_SCHEMA  Mark a schema-key combination as permanently excluded.
%
%   scidb.exclude_schema(REASON, 'subject', 1, 'trial', 2)
%   scidb.exclude_schema(REASON, 'subject', 3)   % wildcard — excludes all trials
%
%   Stores a persistent exclusion record in the database.  The combination
%   is automatically skipped by scidb.for_each without any per-call config.
%   Omitted schema keys act as wildcards (NULL in the table), so omitting
%   'trial' excludes every trial of the given subject.
%
%   Arguments:
%       reason   - Human-readable explanation (string)
%       varargin - Schema key/value pairs, e.g. 'subject', 1, 'trial', 2
%
%   Raises an error if:
%       - No schema keys are specified
%       - A key is not in the dataset schema
%       - The exact keyset is already excluded

    arguments
        reason (1,1) string
    end

    if mod(numel(varargin), 2) ~= 0
        error('scidb:exclude_schema', ...
              'Schema keys must be provided as name-value pairs.');
    end

    py_keys = py.dict();
    for i = 1:2:numel(varargin)
        key = char(string(varargin{i}));
        val = varargin{i + 1};
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

    py.scidb.exclusions.exclude_schema_dict( ...
        py_keys, char(reason), py.None);

end
