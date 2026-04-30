function py_obj = to_python(data)
%TO_PYTHON  Convert MATLAB data to a Python object for database storage.
%
%   Handles: double/single/integer arrays, scalars, strings, logicals.
%   Arrays are converted to C-contiguous numpy ndarrays so that
%   canonical_hash produces consistent results.

    if isstring(data) && isscalar(data)
        py_obj = char(data);

    elseif isstring(data)
        % String array -> Python list of strings
        py_obj = py.list(cellfun(@char, num2cell(data(:)'), 'UniformOutput', false));

    elseif ischar(data)
        py_obj = py.str(data);

    elseif islogical(data) && isscalar(data)
        py_obj = py.bool(data);

    elseif islogical(data)
        % Logical array -> numpy bool array
        py_obj = py.numpy.array(data(:)', pyargs('dtype', 'bool'));
        py_shape = py.builtins.tuple(num2cell(int64(size(data))));
        py_obj = py_obj.reshape(py_shape, pyargs('order', 'C'));

    elseif isnumeric(data) && isscalar(data)
        if isfloat(data)
            py_obj = py.float(double(data));
        else
            py_obj = py.int(int64(data));
        end

    elseif isnumeric(data)
        % Multi-element array -> numpy ndarray (C-contiguous / row-major)
        %
        % MATLAB is column-major; numpy default is row-major.  We
        % transpose so that the logical element order matches, then
        % request C-contiguous layout for deterministic hashing.
        %
        % Vectors (row or column) are always mapped to 1-D numpy arrays
        % with shape (N,) so they store as DOUBLE[] not DOUBLE[][] in DuckDB.
        if isvector(data)
            flat = data;
            py_shape = py.builtins.tuple(num2cell(int64([numel(data)])));
        elseif ismatrix(data)
            % 2-D matrix: transpose so row-major matches MATLAB convention
            flat = data';
            py_shape = py.builtins.tuple(num2cell(int64(size(data))));
        else
            flat = data;
            py_shape = py.builtins.tuple(num2cell(int64(size(data))));
        end

        % Determine numpy dtype string
        dtype = matlab_dtype_to_numpy(data);

        py_flat = py.numpy.array(flat(:)', pyargs('dtype', dtype));
        py_obj = py_flat.reshape(py_shape, pyargs('order', 'C'));
        py_obj = py.numpy.ascontiguousarray(py_obj);

    elseif iscell(data)
        % Cell array -> Python list
        py_obj = py.list();
        for i = 1:numel(data)
            py_obj.append(scidb.internal.to_python(data{i}));
        end

    elseif isdatetime(data)
        % datetime -> ISO 8601 string(s)
        if isscalar(data)
            py_obj = char(string(data, 'yyyy-MM-dd''T''HH:mm:ss.SSS'));
        else
            strs = string(data(:)', 'yyyy-MM-dd''T''HH:mm:ss.SSS');
            py_obj = py.list(cellstr(strs));
        end

    elseif istable(data)
        % MATLAB table -> pandas DataFrame
        col_names = data.Properties.VariableNames;
        py_dict = py.dict();
        for i = 1:numel(col_names)
            col = data.(col_names{i});
            orig_class = class(col);
            orig_size = size(col);
            if iscategorical(col)
                col = string(col);
            elseif isdatetime(col)
                col = string(col, 'yyyy-MM-dd''T''HH:mm:ss.SSS');
            elseif isnumeric(col) && ismatrix(col) && ~isvector(col) && ~isscalar(col)
                % Multi-column numeric variable (e.g. Nx2 matrix) ->
                % cell array of row vectors.  pandas rejects 2-D ndarrays
                % as column values, so store each row as a separate 1-D array.
                tmp = cell(size(col, 1), 1);
                for k = 1:size(col, 1)
                    tmp{k} = col(k, :);
                end
                col = tmp;
            end

            try
                if iscell(col)
                    % Cell array column: try flatten+lengths fast path
                    % first to avoid per-element bridge crossings.
                    [can_flat, flat, lengths, flat_dtype] = try_flatten_cell_column(col);
                    if can_flat
                        % Fast path: 3 bridge crossings instead of N*3
                        py_flat = py.numpy.array(flat, pyargs('dtype', flat_dtype));
                        py_lengths = py.numpy.array(lengths, pyargs('dtype', 'int64'));
                        py_val = py.sci_matlab.bridge.split_flat_to_lists(py_flat, py_lengths);
                    else
                        % Fallback: convert element-by-element.
                        % Inner numpy arrays must become Python lists so that
                        % pandas creates an object column instead of trying to
                        % stack arrays into a 2-D ndarray.
                        py_val = py.list();
                        for k = 1:numel(col)
                            elem_data = col{k};
                            % Ensure cell elements that are Nx1 cells are
                            % transposed to 1xN so MATLAB's Python bridge
                            % can handle them.
                            if iscell(elem_data) && iscolumn(elem_data)
                                elem_data = elem_data';
                            end
                            elem = scidb.internal.to_python(elem_data);
                            if isa(elem, 'py.numpy.ndarray')
                                % Ravel to 1-D before tolist so Nx1 vectors
                                % produce flat lists, not nested [[v],[v],...].
                                elem = elem.ravel().tolist();
                            end
                            py_val.append(elem);
                        end
                    end
                else
                    py_val = scidb.internal.to_python(col);
                    % to_python always reshapes using size(data) which is
                    % at least 2-D in MATLAB (e.g. Nx1 -> shape (N,1)).
                    % pandas requires per-column arrays to be 1-D, so ravel.
                    if isa(py_val, 'py.numpy.ndarray')
                        py_val = py_val.ravel();
                    elseif ~isa(py_val, 'py.list')
                        % Scalar value (e.g. 1-row table) — wrap in a list
                        % so pandas gets array-like values for every column.
                        py_val = py.list({py_val});
                    end
                end
            catch ME
                % Report which column failed and what the element looks like
                detail = sprintf('class=%s size=[%s]', orig_class, num2str(orig_size));
                if iscell(col)
                    detail = sprintf('%s, cell elem %d: class=%s size=[%s]', ...
                        detail, k, class(col{k}), num2str(size(col{k})));
                end
                error('scidb:ColumnConvertError', ...
                    'to_python table col %d/%d "%s" failed (%s): %s', ...
                    i, numel(col_names), col_names{i}, detail, ME.message);
            end
            py_dict{col_names{i}} = py_val;
        end
        py_obj = py.pandas.DataFrame(py_dict);

    elseif isstruct(data) && isscalar(data)
        % Scalar struct -> Python dict
        py_obj = py.dict();
        fns = fieldnames(data);
        for i = 1:numel(fns)
            py_obj{fns{i}} = scidb.internal.to_python(data.(fns{i}));
        end

    elseif isstruct(data) && ~isscalar(data)
        % Struct array -> Python list of dicts
        py_obj = py.list();
        for i = 1:numel(data)
            py_obj.append(scidb.internal.to_python(data(i)));
        end

    else
        error('scidb:UnsupportedType', ...
            'Cannot convert MATLAB type "%s" to Python.', class(data));
    end
end


function dtype = matlab_dtype_to_numpy(data)
%MATLAB_DTYPE_TO_NUMPY  Map MATLAB numeric class to numpy dtype string.
    switch class(data)
        case 'double',  dtype = 'float64';
        case 'single',  dtype = 'float32';
        case 'int8',    dtype = 'int8';
        case 'int16',   dtype = 'int16';
        case 'int32',   dtype = 'int32';
        case 'int64',   dtype = 'int64';
        case 'uint8',   dtype = 'uint8';
        case 'uint16',  dtype = 'uint16';
        case 'uint32',  dtype = 'uint32';
        case 'uint64',  dtype = 'uint64';
        otherwise
            dtype = 'float64';
    end
end


function [can_flat, flat, lengths, flat_dtype] = try_flatten_cell_column(col)
%TRY_FLATTEN_CELL_COLUMN  Try to flatten a cell column into a single array.
%
%   For cell columns where every element is a numeric or logical vector
%   (or empty) of the same MATLAB class, concatenate all elements into
%   one flat array and record their lengths.  This enables a single
%   MATLAB->Python bridge crossing instead of one per element.
%
%   Returns:
%     can_flat   - true if flattening succeeded
%     flat       - 1xN flat array of all elements concatenated (row vector)
%     lengths    - 1xM array of element lengths (int64)
%     flat_dtype - numpy dtype string for the flat array

    can_flat = false;
    flat = [];
    lengths = [];
    flat_dtype = '';

    n = numel(col);
    if n == 0
        return;
    end

    % Determine the common class from the first non-empty element
    common_class = '';
    for i = 1:n
        elem = col{i};
        if ~isempty(elem)
            if ~(isnumeric(elem) || islogical(elem))
                return;  % Non-numeric/logical element — can't flatten
            end
            common_class = class(elem);
            break;
        end
    end

    if isempty(common_class)
        % All elements are empty — still flattenable
        can_flat = true;
        flat = double([]);
        lengths = zeros(1, n, 'int64');
        flat_dtype = 'float64';
        return;
    end

    % Verify all elements are numeric/logical vectors of the same class
    lengths = zeros(1, n, 'int64');
    total_len = int64(0);
    for i = 1:n
        elem = col{i};
        if isempty(elem)
            lengths(i) = 0;
        elseif (isnumeric(elem) || islogical(elem)) && isvector(elem) && strcmp(class(elem), common_class)
            lengths(i) = int64(numel(elem));
            total_len = total_len + lengths(i);
        else
            % Mixed type, non-vector, or non-numeric — can't flatten
            return;
        end
    end

    % Preallocate flat array of the common class
    if islogical(col{find(lengths > 0, 1)})
        flat = false(1, total_len);
        flat_dtype = 'bool';
    else
        flat = zeros(1, total_len, common_class);
        flat_dtype = matlab_dtype_to_numpy(zeros(1, 1, common_class));
    end

    % Copy elements into flat array
    pos = int64(1);
    for i = 1:n
        len_i = lengths(i);
        if len_i > 0
            flat(pos:pos + len_i - 1) = col{i}(:)';
            pos = pos + len_i;
        end
    end

    can_flat = true;
end
