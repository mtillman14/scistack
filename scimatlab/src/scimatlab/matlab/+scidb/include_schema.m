function include_schema(reason, varargin)
%SCIDB.INCLUDE_SCHEMA  Re-include a previously excluded schema-key combination.
%
%   scidb.include_schema(REASON, 'subject', 1, 'trial', 2)
%   scidb.include_schema(REASON, 'subject', 3)   % re-include all trials
%
%   Stores a re-inclusion record in the database (the original exclusion row
%   is NOT deleted — the full history is preserved).  The combination will
%   no longer be skipped by scidb.for_each.
%
%   Arguments:
%       reason   - Human-readable explanation (string)
%       varargin - Schema key/value pairs, e.g. 'subject', 1, 'trial', 2
%
%   Raises an error if:
%       - The exact keyset has no exclusion record
%       - The exact keyset is already included

    arguments
        reason (1,1) string
    end

    if mod(numel(varargin), 2) ~= 0
        error('scidb:include_schema', ...
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

    py.scidb.exclusions.include_schema_dict( ...
        py_keys, char(reason), py.None);

end
