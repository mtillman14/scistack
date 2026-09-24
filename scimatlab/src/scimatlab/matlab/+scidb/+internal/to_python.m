function [py_obj, col_stats] = to_python(data)
%TO_PYTHON  Convert MATLAB data to a Python object for database storage.
%
%   Handles: double/single/integer arrays, scalars, strings, logicals.
%   Arrays are converted to C-contiguous numpy ndarrays so that
%   canonical_hash produces consistent results.
%
%   [py_obj, col_stats] = to_python(tbl) also returns, for a TABLE, one
%   entry per column: name, strategy ('plain', 'table_concat', 'flat' or
%   'element_by_element'), n (elements) and seconds. for_each.m logs the
%   slowest ones, so a slow MATLAB->Python result transfer names the column
%   and the strategy that cost it (cleanup-audit F31). Empty for non-tables.

    col_stats = struct('name', {}, 'strategy', {}, 'n', {}, 'seconds', {});

    % Pass through Python objects that arrived in a MATLAB cell. The
    % MATLAB→Python bridge can route them straight back without conversion
    % (e.g. a py.pandas.DataFrame produced upstream).
    if isa(data, 'py.object') && ~isa(data, 'py.NoneType')
        py_obj = data;
        return;
    end

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
            t_col = tic;
            strategy = 'plain';
            col = data.(col_names{i});
            orig_class = class(col);
            orig_size = size(col);
            scidb.Log.debug('to_python: processing table column %d/%d "%s" (class=%s, size=[%s])', ...
                i, numel(col_names), col_names{i}, orig_class, num2str(orig_size));
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
                    % Cell array column: try multiple optimization strategies
                    % in order of preference to avoid per-element bridge crossings.

                    % Strategy 1: Homogeneous table concatenation
                    % When all cells contain tables with identical schemas,
                    % concatenate them into one table and convert once.
                    [can_concat, concat_table] = try_concat_homogeneous_tables(col);
                    concat_ok = false;
                    if can_concat
                        try
                            % Track original row count per cell element so we
                            % can split the concatenated DataFrame back into
                            % one sub-DataFrame per combo (handles both 1-row
                            % and multi-row inner tables correctly).
                            row_counts_vec = int64(cellfun(@height, col(:)'));
                            py_row_counts = py.numpy.array(row_counts_vec, ...
                                pyargs('dtype', 'int64'));

                            scidb.Log.debug('to_python: trying table concat for cell column "%s" (%d elems, %d total rows, %d cols each)', ...
                                col_names{i}, numel(col), sum(row_counts_vec), width(concat_table));

                            % Convert concatenated table once, then split
                            % Python-side by row counts (avoids iloc bridge
                            % issues and is more efficient than N calls).
                            py_concat_df = scidb.internal.to_python(concat_table);
                            py_val = py.scimatlab.bridge.split_df_to_dataframes( ...
                                py_concat_df, py_row_counts);

                            concat_ok = true;
                            strategy = 'table_concat';
                            scidb.Log.debug('to_python: table concat succeeded for column "%s"', col_names{i});
                        catch concat_err
                            scidb.Log.debug('to_python: table concat failed for column "%s", falling back: %s', ...
                                col_names{i}, concat_err.message);
                            concat_ok = false;
                        end
                    end

                    % Strategy 2: Numeric/logical array flattening
                    % (original fast path for homogeneous numeric/logical vectors)
                    fast_path_ok = false;
                    if ~concat_ok
                        [can_flat, flat, lengths, flat_dtype] = scidb.internal.try_flatten_cell_column(col);
                        if can_flat
                            try
                                scidb.Log.debug('to_python: trying fast path for cell column "%s" (dtype=%s, total_len=%d)', ...
                                    col_names{i}, flat_dtype, numel(flat));
                                % Fast path: 3 bridge crossings instead of N*3
                                py_flat = py.numpy.array(flat, pyargs('dtype', flat_dtype));
                                py_lengths = py.numpy.array(lengths, pyargs('dtype', 'int64'));
                                py_val = py.scimatlab.bridge.split_flat_to_lists(py_flat, py_lengths);
                                fast_path_ok = true;
                                strategy = 'flat';
                                scidb.Log.debug('to_python: fast path succeeded for column "%s"', col_names{i});
                            catch fast_err
                                % Fast path failed (e.g., numpy bridge error) — fall back
                                % to element-by-element conversion
                                scidb.Log.debug('to_python: fast path failed for column "%s", falling back: %s', ...
                                    col_names{i}, fast_err.message);
                                fast_path_ok = false;
                            end
                        else
                            scidb.Log.debug('to_python: cell column "%s" not flattenable, using element-by-element', ...
                                col_names{i});
                        end
                    end

                    % Strategy 3: Element-by-element fallback
                    % (most compatible but slowest)
                    if ~concat_ok && ~fast_path_ok
                        strategy = 'element_by_element';
                        % Fallback: convert element-by-element.
                        % Inner numpy arrays must become Python lists so that
                        % pandas creates an object column instead of trying to
                        % stack arrays into a 2-D ndarray.
                        scidb.Log.debug('to_python: converting column "%s" element-by-element (%d elements)', ...
                            col_names{i}, numel(col));
                        py_val = py.list();
                        for k = 1:numel(col)
                            elem_data = col{k};
                            % Ensure cell elements that are Nx1 cells are
                            % transposed to 1xN so MATLAB's Python bridge
                            % can handle them.
                            if iscell(elem_data) && iscolumn(elem_data)
                                elem_data = elem_data';
                            end

                            % Bypass numpy for simple numeric/logical arrays to avoid
                            % libmwbuffer errors. Convert directly to Python lists.
                            % Pandas can handle lists fine for DataFrame columns.
                            if (isnumeric(elem_data) || islogical(elem_data)) && ~isscalar(elem_data)
                                % Convert MATLAB array -> Python list (avoiding MATLAB numpy bridge)
                                elem = py.list(num2cell(elem_data(:)'));
                            else
                                % For other types (scalars, strings, nested cells),
                                % use the standard to_python conversion
                                elem = scidb.internal.to_python(elem_data);
                                if isa(elem, 'py.numpy.ndarray')
                                    % Ravel to 1-D before tolist so Nx1 vectors
                                    % produce flat lists, not nested [[v],[v],...].
                                    elem = elem.ravel().tolist();
                                end
                            end
                            py_val.append(elem);
                        end
                        scidb.Log.debug('to_python: element-by-element conversion complete for column "%s"', ...
                            col_names{i});
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
                if iscell(col) && exist('k', 'var')
                    detail = sprintf('%s, cell elem %d: class=%s size=[%s]', ...
                        detail, k, class(col{k}), num2str(size(col{k})));
                elseif iscell(col)
                    detail = sprintf('%s, cell array with %d elements', detail, numel(col));
                end
                error('scidb:ColumnConvertError', ...
                    'to_python table col %d/%d "%s" failed (%s): %s', ...
                    i, numel(col_names), col_names{i}, detail, ME.message);
            end
            py_dict{col_names{i}} = py_val;
            col_stats(end + 1) = struct('name', col_names{i}, ...
                'strategy', strategy, 'n', numel(col), 'seconds', toc(t_col)); %#ok<AGROW>
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


function [can_concat, concat_table] = try_concat_homogeneous_tables(col)
%TRY_CONCAT_HOMOGENEOUS_TABLES  Try to concatenate tables in a cell column.
%
%   For cell columns where every element is a table with identical
%   VariableNames (same schema), concatenate all tables into one.
%   This enables converting N 1-row tables with a single to_python call
%   instead of N calls, reducing logging verbosity and bridge crossings.
%
%   Returns:
%     can_concat    - true if concatenation succeeded
%     concat_table  - Vertically concatenated table (all rows)

    can_concat = false;
    concat_table = [];

    if ~iscell(col)
        return;
    end

    n = numel(col);
    if n == 0
        return;
    end

    % Find first non-empty element to establish the expected schema
    expected_var_names = {};
    first_table_idx = 0;
    for i = 1:n
        elem = col{i};
        if ~isempty(elem)
            if ~istable(elem)
                % Non-table element found — cannot concatenate
                return;
            end
            expected_var_names = elem.Properties.VariableNames;
            first_table_idx = i;
            break;
        end
    end

    if first_table_idx == 0
        % All elements are empty — nothing to concatenate
        return;
    end

    % Verify all non-empty elements are tables with matching schemas
    for i = 1:n
        elem = col{i};
        if isempty(elem)
            % Empty cells mixed with non-empty — cannot safely concatenate
            % (would need to handle missing rows, which complicates indexing)
            return;
        elseif ~istable(elem)
            % Mixed types — cannot concatenate
            return;
        elseif ~isequal(elem.Properties.VariableNames, expected_var_names)
            % Schema mismatch — cannot concatenate
            return;
        end
    end

    % All checks passed — attempt vertical concatenation
    try
        concat_table = vertcat(col{:});
        can_concat = true;
    catch
        % Concatenation failed (e.g., incompatible column types despite
        % matching names). Fall back to element-by-element conversion.
        can_concat = false;
        concat_table = [];
    end
end
