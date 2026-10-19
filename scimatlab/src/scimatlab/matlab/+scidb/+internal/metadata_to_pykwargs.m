function kwargs = metadata_to_pykwargs(varargin)
%METADATA_TO_PYKWARGS  Convert MATLAB name-value pairs to pyargs cell array.
%
%   kwargs = scidb.internal.metadata_to_pykwargs('subject', 1, 'session', 'A')
%   returns a cell array suitable for unpacking into a Python call:
%       py_func(kwargs{:})
%
%   Handles MATLAB name=value syntax (R2021a+) which arrives as
%   name-value pairs in varargin.

    if mod(numel(varargin), 2) ~= 0
        error('scidb:InvalidMetadata', ...
            'Metadata must be specified as name-value pairs.');
    end

    kwargs = cell(1,numel(varargin));
    for i = 1:2:numel(varargin)
        key = varargin{i};
        val = varargin{i+1};

        if isstring(key)
            key = char(key);
        end

        % Convert MATLAB values to Python-compatible types
        if isstring(val) && isscalar(val)
            val = char(val);
        elseif isnumeric(val) && isscalar(val)
            % Whole numbers → py.int (scidb.internal.number_to_python)
            val = scidb.internal.number_to_python(val);
        elseif isstruct(val)
            % Structs become JSON strings so they can be stored as
            % version keys (e.g. config structs).
            val = char(jsonencode(val));
        end
        
        kwargs{i} = key;
        kwargs{i+1} = val;
    end
end
