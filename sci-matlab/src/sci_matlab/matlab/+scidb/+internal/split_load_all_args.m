function [metadata_args, py_version_id, as_table, db, where, categorical_flag] = split_load_all_args(varargin)
%SPLIT_LOAD_ALL_ARGS  Separate 'version_id', 'as_table', 'db', 'where', and 'categorical' from other name-value metadata.
%
%   [metadata_args, py_version_id, as_table, db, where, categorical_flag] = scidb.internal.split_load_all_args(...)
%   extracts the 'version_id', 'as_table', 'db', 'where', and 'categorical' keys if present,
%   returning the remaining name-value pairs and the version_id as a
%   Python-compatible value. Defaults to "all", false, [], [], and false respectively.

    py_version_id = py.str('all');
    as_table = false;
    db = [];
    where = [];
    categorical_flag = false;
    metadata_args = {};

    i = 1;
    while i <= numel(varargin)
        key = varargin{i};
        if isstring(key), key = char(key); end

        if strcmpi(key, 'as_table') && i < numel(varargin)
            as_table = logical(varargin{i+1});
            i = i + 2;
        elseif strcmpi(key, 'version_id') && i < numel(varargin)
            val = varargin{i+1};
            if isstring(val) || ischar(val)
                py_version_id = py.str(char(val));
            elseif isnumeric(val) && isscalar(val)
                py_version_id = py.int(int64(val));
            elseif isnumeric(val) && ~isscalar(val)
                py_version_id = py.list(arrayfun(@(x) py.int(int64(x)), val, 'UniformOutput', false));
            end
            i = i + 2;
        elseif strcmpi(key, 'db') && i < numel(varargin)
            db = varargin{i+1};
            i = i + 2;
        elseif strcmpi(key, 'where') && i < numel(varargin)
            where = varargin{i+1};
            i = i + 2;
        elseif strcmpi(key, 'categorical') && i < numel(varargin)
            categorical_flag = logical(varargin{i+1});
            i = i + 2;
        else
            metadata_args{end+1} = varargin{i};   %#ok<AGROW>
            metadata_args{end+1} = varargin{i+1};  %#ok<AGROW>
            i = i + 2;
        end
    end
end
