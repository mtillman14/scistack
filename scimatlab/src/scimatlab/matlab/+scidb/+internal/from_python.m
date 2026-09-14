function data = from_python(py_obj)
%FROM_PYTHON  Convert a Python object back to a native MATLAB type.
%
%   Handles numpy ndarrays, Python scalars (float, int, str, bool),
%   lists, and dicts.

    % MATLAB's Python bridge auto-converts certain Python types to native
    % MATLAB types before this function is called (e.g. when reading dict
    % values or extracting elements from cell(py.list(...))).  Detect these
    % early so they never fall through to the expensive py.* isa() checks
    % and the exception-throwing fallback path.
    %
    % IMPORTANT: isnumeric/isstring can return true for Python proxy objects
    % (e.g. py.int, py.str) in some MATLAB versions.  Guard with a class
    % name check to ensure we only short-circuit native MATLAB types.
    cl = class(py_obj);
    is_python = numel(cl) >= 3 && cl(1) == 'p' && cl(2) == 'y' && cl(3) == '.';
    if ~is_python
        if islogical(py_obj) || isnumeric(py_obj) || isstring(py_obj)
            data = py_obj;
            return;
        end
        % The bridge converts Python str to MATLAB char when extracting from
        % cell(py.list(...)). Convert to string so lists of strings round-trip
        % as string arrays, not cell arrays of ASCII code vectors.
        if ischar(py_obj)
            data = string(py_obj);
            return;
        end
    end

    if isa(py_obj, 'py.NoneType')
        data = [];

    elseif isa(py_obj, 'py.numpy.ndarray')
        py_c = py.numpy.ascontiguousarray(py_obj);
        dtype_kind = string(py_c.dtype.kind);
        arr_shape = int64(py_c.shape);
        arr_ndim = int64(py_c.ndim);
        scidb.Log.debug('from_python: numpy array dtype=%s, shape=[%s], ndim=%d', ...
            string(char(py.str(py_c.dtype))), strjoin(string(arr_shape), ' '), arr_ndim);

        % --- Bulk path: ONE bridge crossing for the whole array. ---
        % Everything below it costs one Python->MATLAB crossing per ELEMENT,
        % which is the entire cost at scale (see ndarray_via_buffer).  This
        % is the only branch that has to be fast: the py.list fast paths,
        % convert_dataframe's object and default branches, and
        % bridge.flatten_sequences all funnel their bulk data through here.
        [data, buf_ok] = ndarray_via_buffer(py_c, dtype_kind, arr_shape, arr_ndim);

        if ~buf_ok
            % Bypass MATLAB-numpy bridge (libmwbuffer issues) by converting
            % to Python list first, then to MATLAB
            warn_if_large_slow_conversion(arr_shape, py_c);

            % Convert to Python list to avoid libmwbuffer errors.
            try
                py_list = py_c.tolist();
                scidb.Log.debug('from_python: tolist() succeeded, converting to MATLAB...');
            catch list_err
                scidb.Log.err('from_python: tolist() failed: %s', list_err.message);
                rethrow(list_err);
            end

            if dtype_kind == "b"
                % bool array -> logical
                c = cell(py_list);
                if isempty(c)
                    data = logical([]);
                else
                    % Convert list of bools to MATLAB logical array
                    % Explicitly handle py.bool objects
                    if ~isempty(c) && isa(c{1}, 'py.bool')
                        data = cellfun(@logical, c);
                    else
                        data = cellfun(@logical, c);
                    end
                end
            elseif dtype_kind == "O"
                % Object array -> cell array, convert each element
                c = cell(py_list);
                data = cell(1, numel(c));
                for i = 1:numel(c)
                    data{i} = scidb.internal.from_python(c{i});
                end
            else
                % Numeric array -> MATLAB numeric array
                c = cell(py_list);
                if isempty(c)
                    data = [];
                else
                    % Convert list of numbers to MATLAB array
                    % Handle case where c contains py.int/py.float objects
                    if ~isempty(c) && (isa(c{1}, 'py.int') || isa(c{1}, 'py.float'))
                        % Explicitly convert Python numeric types to MATLAB doubles
                        data = cellfun(@double, c);
                    elseif ~isempty(c) && is_python_object(c{1})
                        % 2-D+ array: inner elements are Python row-lists.
                        % Recursively convert each row, then stack into a matrix.
                        scidb.Log.debug('from_python: numeric array contains Python row-lists, converting recursively');
                        data = cell(1, numel(c));
                        for idx_inner = 1:numel(c)
                            data{idx_inner} = scidb.internal.from_python(c{idx_inner});
                        end
                        data = try_stack_numeric(data);
                    else
                        data = cell2mat(c);
                    end
                end
            end
        end

        if dtype_kind ~= "O" && int64(py_c.ndim) == 1 && ~isempty(data)
            data = data(:);
        end

        scidb.Log.debug('from_python: numpy conversion complete, result class=%s size=[%s]', ...
            class(data), num2str(size(data)))

    elseif isa(py_obj, 'py.bool')
        % Must check py.bool BEFORE py.int: Python bool is a subclass of int,
        % so isa(py_obj, 'py.int') returns true for True/False values.
        data = logical(py_obj);

    elseif isa(py_obj, 'py.float')
        data = double(py_obj);

    elseif isa(py_obj, 'py.int')
        data = double(py_obj);

    elseif isa(py_obj, 'py.str')
        data = string(py_obj);

    elseif isa(py_obj, 'py.datetime.datetime')
        data = datetime(string(py_obj.isoformat()), 'InputFormat', 'yyyy-MM-dd''T''HH:mm:ss');

    elseif isa(py_obj, 'py.list')
        % Fast path 1: try converting the entire list to a numpy array in one
        % Python call.  This avoids N individual boundary crossings when the
        % list contains homogeneous numeric values (the common case for
        % DOUBLE[] columns loaded from DuckDB).
        % Exclude Unicode ('U') dtype: numpy creates a 'U' array for lists of
        % strings, but converting it back via tolist() gives py.str proxies
        % that the numeric branch misroutes through the 2D-array path,
        % producing a cell instead of a string array. Let the fallback handle
        % string lists correctly.
        try
            py_arr = py.numpy.asarray(py_obj);
            dtype_kind = string(py_arr.dtype.kind);
            if dtype_kind ~= "O" && dtype_kind ~= "U"
                % Successfully converted to a typed numpy array — use the
                % ndarray path which handles bool/numeric in bulk.
                data = scidb.internal.from_python(py_arr);
                return;
            end
        catch
            % Fall through to more specialized paths below
        end

        % Fast path 2: flatten+convert sequences (numeric or boolean)
        % When the list contains homogeneous sequences (numeric or bool) of
        % varying lengths, flatten all into one array, convert once (1 bridge
        % crossing), then split back. Much faster than N individual conversions.
        try
            py_result = py.scimatlab.bridge.flatten_sequences(py_obj);
            % Python returns tuple (flat_array, lengths) - access as cell array
            py_flat = py_result{1};
            py_lengths = py_result{2};

            if ~isa(py_flat, 'py.NoneType')
                scidb.Log.debug('from_python: using sequences fast path (flatten+split)');

                % Single bridge crossing for the flattened array
                flat_data = scidb.internal.from_python(py_flat);
                lengths = double(py_lengths);

                % Split into cell array using lengths
                n = numel(lengths);
                data = cell(1, n);
                pos = 1;
                for i = 1:n
                    len = lengths(i);
                    if len > 0
                        data{i} = flat_data(pos:pos+len-1);
                        pos = pos + len;
                    else
                        data{i} = [];
                    end
                end
                return;
            end
        catch
            % Fall through to more specialized paths below
        end

        % Fast path 3: nested dicts with numpy arrays (via JSON)
        % When the list contains dicts with nested structure and numpy arrays
        % (e.g., structs stored as JSON in DuckDB), convert to JSON string in
        % Python, then deserialize in MATLAB. Single bridge crossing instead of
        % N dict conversions + N×M numpy array conversions.
        try
            py_json = py.scimatlab.bridge.convert_nested_dicts_to_json(py_obj);
            if ~isa(py_json, 'py.NoneType')
                scidb.Log.debug('from_python: using nested dicts JSON fast path');

                % Single bridge crossing for the JSON string
                json_str = char(py_json);

                % Single MATLAB operation to deserialize entire structure
                data = jsondecode(json_str);

                % jsondecode returns struct array for list of dicts
                % Convert to cell array of structs to match expected format
                if isstruct(data) && ~isscalar(data)
                    data = num2cell(data);
                elseif isstruct(data) && isscalar(data)
                    % Single element - wrap in cell
                    data = {data};
                end

                return;
            end
        catch
            % Fall through to more specialized paths below
        end

        % Fast path 4: try concatenating homogeneous DataFrames
        % When the list contains DataFrames with identical schemas,
        % concatenate them in Python and convert once (avoids N conversions
        % and N×M log messages for N DataFrames with M columns each).
        c = cell(py_obj);
        n = numel(c);

        [can_concat_dfs, concat_df] = try_concat_homogeneous_dataframes(c);
        if can_concat_dfs
            try
                scidb.Log.debug('from_python: trying DataFrame concat for list (%d DataFrames)', n);

                % Record original row counts before concatenation so we can
                % split back correctly even when sub-DataFrames have >1 row.
                row_counts = zeros(1, n, 'int64');
                for ri_tmp = 1:n
                    row_counts(ri_tmp) = int64(double(py.builtins.len(c{ri_tmp})));
                end

                % Convert concatenated DataFrame once (single set of log messages)
                concat_table = scidb.internal.from_python(concat_df);

                % Split back into individual tables using row counts.
                data = cell(1, n);
                pos = 1;
                for row_idx = 1:n
                    rc = row_counts(row_idx);
                    data{row_idx} = concat_table(pos:pos+rc-1, :);
                    pos = pos + rc;
                end

                scidb.Log.debug('from_python: DataFrame concat succeeded');
                return;
            catch concat_err
                scidb.Log.debug('from_python: DataFrame concat failed, falling back: %s', ...
                    concat_err.message);
                % Fall through to element-by-element conversion
            end
        end

        % Fallback: element-by-element conversion
        data = cell(1, n);
        all_str = n > 0;
        all_numeric_scalar = n > 0;
        all_logical_scalar = n > 0;
        for i = 1:n
            data{i} = scidb.internal.from_python(c{i});
            if all_str && ~isstring(data{i})
                all_str = false;
            end
            if all_numeric_scalar && ~(isnumeric(data{i}) && isscalar(data{i}))
                all_numeric_scalar = false;
            end
            if all_logical_scalar && ~(islogical(data{i}) && isscalar(data{i}))
                all_logical_scalar = false;
            end
        end
        if all_str
            % All-string list -> string array (round-trips string arrays)
            data = [data{:}];
        elseif all_numeric_scalar
            % All-scalar-numeric list -> numeric vector
            data = [data{:}];
        elseif all_logical_scalar
            % All-scalar-logical list -> logical vector
            data = [data{:}];
        end

    elseif isa(py_obj, 'py.pandas.core.frame.DataFrame') || isa(py_obj, 'py.pandas.DataFrame')
        data = convert_dataframe(py_obj);

    elseif isa(py_obj, 'py.dict')
        data = scidb.internal.pydict_to_struct(py_obj);

    else
        % Fallback: isa() can miss pandas DataFrames depending on MATLAB
        % version / class proxy resolution.  Use Python isinstance as a
        % robust secondary check before giving up.
        is_df = false;
        try
            is_df = logical(py.builtins.isinstance(py_obj, py.pandas.DataFrame));
        catch
        end

        if is_df
            data = convert_dataframe(py_obj);
        else
            % Last resort: try MATLAB's automatic conversion
            try
                data = double(py_obj);
            catch
                data = py_obj;  % Return raw Python object
            end
        end
    end
end


function data = convert_dataframe(py_obj)
%CONVERT_DATAFRAME  Convert a pandas DataFrame to a MATLAB table.
    try
        py_cols = py_obj.columns.tolist();
        scidb.Log.debug('convert_dataframe: got columns, converting to cell...');
        col_names = cell(py_cols);
        scidb.Log.debug('convert_dataframe: %d columns: %s', numel(col_names), strjoin(string(col_names(1:min(5,end))), ', '));
        args = cell(1, numel(col_names));

        scidb.Log.debug('convert_dataframe: getting row count...');
        py_len = py.builtins.len(py_obj);
        n_rows = int64(py_len);
        scidb.Log.debug('convert_dataframe: py_len type = %s, n_rows = %d', class(py_len), n_rows);
        scidb.Log.debug('convert_dataframe: converting DataFrame with %d rows, %d columns', ...
            n_rows, numel(col_names));
    catch init_err
        scidb.Log.err('convert_dataframe: initialization failed: %s', init_err.message);
        rethrow(init_err);
    end

    % Per-column timing. Every other message in this function is DEBUG, so
    % at INFO a slow DataFrame conversion is a silent gap. Keep quiet when
    % the conversion is quick (this runs for every result table too) and
    % name the expensive columns when it is not.
    col_secs = zeros(1, numel(col_names));
    df_t0 = tic;
    for i = 1:numel(col_names)
        col_t0 = tic;
        try
            col_key = col_names{i};
            scidb.Log.debug('convert_dataframe: column %d - getting column "%s"', i, string(col_key));
            col = py.operator.getitem(py_obj, col_key);
            scidb.Log.debug('convert_dataframe: column %d - got column, getting dtype...', i);

            % Break down dtype conversion into steps to find where it fails
            try
                py_dtype = col.dtype;
                scidb.Log.debug('convert_dataframe: got dtype object');
                py_dtype_str = py.str(py_dtype);
                scidb.Log.debug('convert_dataframe: converted dtype to py.str');
                char_dtype = char(py_dtype_str);
                scidb.Log.debug('convert_dataframe: converted py.str to char');
                dtype_str = string(char_dtype);
                scidb.Log.debug('convert_dataframe: converted char to string: %s', dtype_str);
            catch dtype_err
                scidb.Log.err('convert_dataframe: dtype conversion failed: %s', dtype_err.message);
                rethrow(dtype_err);
            end
            scidb.Log.debug('convert_dataframe: column %d/%d "%s" dtype=%s', ...
                i, numel(col_names), string(col_key), dtype_str);
        catch col_err
            scidb.Log.err('convert_dataframe: column %d "%s" failed: %s', i, string(col_key), col_err.message);
            rethrow(col_err);
        end
        if startsWith(dtype_str, "datetime")
            scidb.Log.debug('convert_dataframe: column %d - datetime branch', i);
            % datetime64 column -> MATLAB datetime via ISO strings
            iso_strs = cell(col.dt.strftime('%Y-%m-%dT%H:%M:%S.%f').tolist());
            args{i} = datetime(iso_strs, 'InputFormat', 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS');
        elseif dtype_str == "object"
            scidb.Log.debug('convert_dataframe: column %d "%s" - object branch', i, string(col_key));
            % Object column (e.g. dicts/structs, array columns) -> cell array via from_python
            py_list = col.tolist();

            % Try fast paths BEFORE converting to MATLAB cell array, in order of preference:
            % 1. Homogeneous DataFrames → concatenate & convert once
            % 2. Homogeneous numeric/logical → optimized py.list conversion
            % 3. Element-by-element fallback

            % Fast path 1: Concatenate homogeneous DataFrames
            c = cell(py_list);  % Need cell array to check DataFrame types
            [can_concat_dfs, concat_df] = try_concat_homogeneous_dataframes(c);
            if can_concat_dfs
                try
                    scidb.Log.debug('convert_dataframe: column %d - trying DataFrame concat (%d DataFrames)', i, numel(c));

                    % Record original row counts before concatenation so we can
                    % split back correctly even when sub-DataFrames have >1 row.
                    nc_tmp = numel(c);
                    row_counts_tmp = zeros(1, nc_tmp, 'int64');
                    for ri_tmp = 1:nc_tmp
                        row_counts_tmp(ri_tmp) = int64(double(py.builtins.len(c{ri_tmp})));
                    end

                    % Convert concatenated DataFrame once (single set of log messages)
                    concat_table = scidb.internal.from_python(concat_df);

                    % Split back into cell array using row counts.
                    col_data = cell(nc_tmp, 1);
                    pos = 1;
                    for row_idx = 1:nc_tmp
                        rc = row_counts_tmp(row_idx);
                        col_data{row_idx} = concat_table(pos:pos+rc-1, :);
                        pos = pos + rc;
                    end

                    scidb.Log.debug('convert_dataframe: column %d - DataFrame concat succeeded', i);
                    args{i} = col_data;
                    col_secs(i) = toc(col_t0);
                    continue;  % Skip to next column
                catch concat_err
                    scidb.Log.debug('convert_dataframe: column %d - DataFrame concat failed, falling back: %s', ...
                        i, concat_err.message);
                    % Fall through to next fast path
                end
            end

            % Fast path 2: Optimized py.list conversion for numeric/logical arrays
            % This handles homogeneous numeric/logical data efficiently by using
            % from_python(py_list) which can convert to numpy array in one call.
            try
                scidb.Log.debug('convert_dataframe: column %d - attempting optimized py.list conversion...', i);
                col_data = scidb.internal.from_python(py_list);
                scidb.Log.debug('convert_dataframe: column %d - SUCCESS: used optimized py.list conversion', i);

                if ~iscell(col_data) && size(col_data, 1) == n_rows
                    % from_python already produced the right shape (e.g., an
                    % N×M matrix for a column of equal-length row vectors).
                    % num2cell would flatten it into individual scalars, and
                    % try_stack_numeric would re-vertcat those into an (N*M)×1
                    % vector — 3× too many rows when M=3.  Assign directly.
                    if isvector(col_data)
                        col_data = col_data(:);
                    end
                    args{i} = col_data;
                    col_secs(i) = toc(col_t0);
                    continue;
                end
                if ~iscell(col_data)
                    col_data = num2cell(col_data);
                end

                % Try to stack into matrix if all same size
                col_data = try_stack_numeric(col_data);
                % Coalesce strings
                col_data = try_stack_strings(col_data);
                % Stack structs
                col_data = try_stack_structs(col_data);
                args{i} = col_data;
            catch opt_err
                % Fast path 3: Element-by-element fallback (for complex/heterogeneous types)
                scidb.Log.debug('convert_dataframe: column %d - optimized conversion failed, using element-by-element', i);
                scidb.Log.debug('convert_dataframe: column %d - error was: %s', i, opt_err.message);

                % c is already populated from DataFrame concat attempt above
                col_data = cell(numel(c), 1);
                scidb.Log.debug('convert_dataframe: column %d - converting %d elements individually...', i, numel(c));
                for k = 1:numel(c)
                    col_data{k} = scidb.internal.from_python(c{k});
                    % Parse stringified arrays (e.g. "[[false], [true], ...]")
                    cd = col_data{k};
                    if isstring(cd) && isscalar(cd) ...
                            && strlength(cd) > 1 && startsWith(cd, "[")
                        try
                            col_data{k} = jsondecode(char(cd));
                        catch
                        end
                    end
                end
                col_data = try_stack_numeric(col_data);
                col_data = try_stack_strings(col_data);
                args{i} = try_stack_structs(col_data);
            end
        else
            scidb.Log.debug('convert_dataframe: column %d - default branch, calling to_numpy()', i);
            try
                py_arr = col.to_numpy();
                scidb.Log.debug('convert_dataframe: column %d - to_numpy() succeeded, calling from_python', i);
                args{i} = scidb.internal.from_python(py_arr);
                scidb.Log.debug('convert_dataframe: column %d - from_python succeeded', i);
                % pandas 3.0+ returns StringDtype for text columns; from_python
                % converts these to cell arrays.  Stack into string arrays.
                if iscell(args{i})
                    args{i} = try_stack_strings(args{i});
                end
            catch convert_err
                scidb.Log.err('convert_dataframe: column %d conversion failed: %s', i, convert_err.message);
                rethrow(convert_err);
            end
        end
        % Ensure column vector — but only when the number of elements
        % matches the DataFrame row count.  Otherwise a 1-row DataFrame
        % with a 14-element array value would be reshaped from 1x14 to
        % 14x1, making the table think there are 14 rows.
        if isvector(args{i}) && numel(args{i}) == n_rows
            args{i} = args{i}(:);
        end
        col_secs(i) = toc(col_t0);
    end

    % Report only when the conversion actually cost something. The named
    % columns are what turns "the run hung" into "the RawEMG data column
    % took 11 minutes", which is the whole point of the instrumentation.
    df_secs = toc(df_t0);
    if df_secs >= 1.0
        scidb.Log.info('[timing] convert_dataframe: %dx%d, TOTAL=%.3fs (%s)', ...
            n_rows, numel(col_names), df_secs, ...
            format_slowest_columns(col_names, col_secs));
    end
    col_name_strs = cellfun(@string, col_names, 'UniformOutput', false);
    data = table;
    for i = 1:numel(args)
        % Special case: a 1-row DataFrame whose column carries a non-scalar
        % vector/matrix should stay cell-wrapped — scifor's
        % _nest_table_outputs=true convention treats each cell as one
        % per-row payload, and downstream MATLAB code expects to brace-
        % index back into it.  Without this guard, ``size(args{i},1) ==
        % n_rows`` would be true (1==1) and the data would land in the
        % table as a raw 1×N numeric column.
        if n_rows == 1 && ~iscell(args{i}) && ~isscalar(args{i})
            data.(col_name_strs{i}) = args(i);
        elseif size(args{i}, 1) == n_rows
            % Per-row values: assign directly.
            % size(·,1) handles both column vectors (Nx1) and matrix columns
            % (NxM) so that e.g. a 3×3 matrix is assigned as a 3-row column
            % rather than cell-wrapped.
            data.(col_name_strs{i}) = args{i};
        else
            % Per-row array values (e.g. time series stored in one cell per row):
            % cell-wrap so the table sees one cell per row, not one row per element.
            data.(col_name_strs{i}) = args(i);
        end
    end
end


function data = try_stack_numeric(data)
%TRY_STACK_NUMERIC  Stack a cell of same-size numeric vectors into a matrix.
%   If every element is numeric with identical size, vertcat them into a
%   matrix (round-trips multi-column table variables).  Otherwise return
%   the cell array unchanged.
    if ~iscell(data) || isempty(data) || ~isnumeric(data{1}), return; end
    ref_sz = size(data{1});
    for k = 2:numel(data)
        if ~isnumeric(data{k}) || ~isequal(size(data{k}), ref_sz)
            return;
        end
    end
    % When every element is an empty matrix (e.g. all-None object column
    % from a DataFrame), vertcat would collapse N empties into a single
    % [], losing the per-row count.  Keep the cell so convert_dataframe
    % assigns one empty per row instead of one empty for the whole column
    % (which would cause a row-count mismatch on table assembly).
    if numel(data) > 1 && any(ref_sz == 0)
        return;
    end
    % from_python converts 1-D numpy arrays to Nx1 column vectors.
    % When they represent rows of a matrix column (N cells, each Mx1),
    % transpose to row vectors so vertcat produces an N×M matrix matching
    % the MATLAB table convention (each row is a 1×M row vector).
    % For a single cell, there's nothing to stack — preserve the value's
    % original shape so a per-row vector payload round-trips faithfully.
    if numel(data) == 1
        data = data{1};
        if isnumeric(data) && isvector(data)
            data = data(:);
        end
    elseif iscolumn(data{1}) && ~isscalar(data{1})
        transposed = cellfun(@(v) v', data, 'UniformOutput', false);
        data = vertcat(transposed{:});
    else
        data = vertcat(data{:});
    end
end


function data = try_stack_strings(data)
%TRY_STACK_STRINGS  Convert a cell array of scalar strings to a string array.
%   If every element is a scalar MATLAB string, concatenate into a column
%   string vector (round-trips string columns stored as pandas object dtype).
%   Otherwise return the cell array unchanged.
    if ~iscell(data) || isempty(data) || ~isstring(data{1}) || ~isscalar(data{1}), return; end
    for k = 2:numel(data)
        if ~isstring(data{k}) || ~isscalar(data{k})
            return;
        end
    end
    data = vertcat(data{:});
end


function data = try_stack_structs(data)
%TRY_STACK_STRUCTS  Convert a cell array of structs to a struct array.
%   If every element is a struct with identical fields, vertcat them into
%   a struct array.  This allows table access as t.field.subfield instead
%   of t.field{1}.subfield.  Otherwise return the data unchanged.
    if ~iscell(data) || isempty(data) || ~isstruct(data{1}), return; end
    ref_fields = sort(fieldnames(data{1}));
    for k = 2:numel(data)
        if ~isstruct(data{k})
            return;
        end
        cur_fields = sort(fieldnames(data{k}));
        if ~isequal(cur_fields, ref_fields)
            return;
        end
    end
    % Check if vertcat will work (fields must have compatible sizes)
    try
        data = vertcat(data{:});
    catch
    end
end


function result = is_python_object(obj)
%IS_PYTHON_OBJECT  Check if an object is a Python object (class starts with 'py.')
    cl = class(obj);
    result = numel(cl) >= 3 && cl(1) == 'p' && cl(2) == 'y' && cl(3) == '.';
end


function [can_concat, concat_df] = try_concat_homogeneous_dataframes(c)
%TRY_CONCAT_HOMOGENEOUS_DATAFRAMES  Try to concatenate DataFrames in a list.
%
%   For lists where every element is a pandas DataFrame with identical
%   columns (same schema), concatenate all DataFrames into one using
%   pandas.concat.  This enables converting N DataFrames with a single
%   from_python call instead of N calls, reducing logging verbosity.
%
%   Returns:
%     can_concat - true if concatenation succeeded
%     concat_df  - Concatenated pandas DataFrame (all rows)

    can_concat = false;
    concat_df = py.None;

    n = numel(c);
    if n == 0
        return;
    end

    % Check if all elements are DataFrames
    for i = 1:n
        elem = c{i};
        if isempty(elem)
            % Empty elements not supported for this optimization
            return;
        end

        % Check if it's a DataFrame
        is_df = false;
        try
            is_df = isa(elem, 'py.pandas.core.frame.DataFrame') || ...
                    isa(elem, 'py.pandas.DataFrame') || ...
                    logical(py.builtins.isinstance(elem, py.pandas.DataFrame));
        catch
            return;
        end

        if ~is_df
            % Not all elements are DataFrames
            return;
        end
    end

    % Get column names from first DataFrame
    try
        first_cols = c{1}.columns.tolist();
        expected_cols_cell = cell(first_cols);
    catch
        return;
    end

    % Verify all DataFrames have matching column schemas
    for i = 2:n
        try
            cols = c{i}.columns.tolist();
            cols_cell = cell(cols);
            if ~isequal(cols_cell, expected_cols_cell)
                % Schema mismatch
                return;
            end
        catch
            return;
        end
    end

    % All checks passed — concatenate using pandas.concat
    try
        % Create Python list of DataFrames
        py_df_list = py.list(c);

        % Concatenate with pandas.concat, ignoring original index
        concat_df = py.pandas.concat(py_df_list, pyargs('ignore_index', true));

        can_concat = true;
    catch
        % Concatenation failed (e.g., incompatible dtypes)
        can_concat = false;
        concat_df = py.None;
    end
end


function [data, ok] = ndarray_via_buffer(py_c, dtype_kind, arr_shape, arr_ndim)
%NDARRAY_VIA_BUFFER  Convert a numpy array in ONE Python->MATLAB crossing.
%
%   The element-by-element route (``tolist()`` + ``cell()``) costs one
%   boundary crossing per element.  At the scale scidb loads -- a 419-record
%   EMG variable spread over 10 array columns is 1.74e8 samples -- that is
%   1.74e8 crossings plus a multi-GB transient MATLAB cell array, which
%   presents as a hung MATLAB rather than a slow one.
%
%   ``py.scimatlab.bridge.ndarray_to_buffer`` hands over the array's raw
%   bytes in Fortran (column-major) order instead.  MATLAB turns py.bytes
%   into uint8 in a single memcpy, typecasts back to the original class and
%   reshapes -- all independent of element count.
%
%   ok=false means "not applicable, use the element-by-element path":
%   object/string/datetime dtypes, a scimatlab without the bridge helper, or
%   a byte count the description disagrees with.  It is never an error --
%   the fallback produces the same values, just slowly.

    data = [];
    ok = false;

    % Only fixed-width numeric/bool dtypes have bytes worth reinterpreting.
    % Checked here as well as Python-side so the common decline (object
    % columns) costs no bridge crossing at all.
    if ~any(dtype_kind == ["f", "i", "u", "b"])
        return;
    end

    % The shape must be trustworthy before it can drive a reshape.
    %
    % arr_shape comes from int64(py_c.shape) in the caller, where it used to
    % feed nothing but a debug line -- an inaccurate value cost nothing. The
    % buffer path made it load-bearing, and a shape that does not describe the
    % array no longer fails loudly: it falls into the numel(arr_shape) <= 1
    % branch below and returns every element as ONE COLUMN, so a 4x3 matrix
    % silently becomes 12x1 with no error anywhere
    % (TestEndToEnd/test_matrix_through_pipeline, 2026-09-14).
    %
    % numpy's own ndim is the authority on how many dimensions the shape must
    % have. If the converted shape disagrees, decline: the element-by-element
    % fallback reconstructs the shape by stacking and does not depend on this
    % conversion at all.
    if numel(arr_shape) ~= double(arr_ndim)
        scidb.Log.warn(['from_python: shape [%s] has %d dimension(s) but the ' ...
            'array reports ndim=%d -- declining the buffer path so the shape ' ...
            'is rebuilt element-by-element rather than silently flattened'], ...
            strjoin(string(arr_shape), ' '), numel(arr_shape), double(arr_ndim));
        return;
    end

    try
        desc = py.scimatlab.bridge.ndarray_to_buffer(py_c);
    catch err
        % A scimatlab without the helper, or a Python-side failure.
        scidb.Log.debug('from_python: buffer path unavailable (%s)', err.message);
        return;
    end

    try
        if ~logical(desc{'ok'})
            scidb.Log.debug('from_python: buffer path declined: %s', ...
                char(desc{'reason'}));
            return;
        end

        ml_class = char(desc{'matlab_class'});
        n_expected = double(desc{'count'});

        if n_expected == 0
            % Match the element-by-element path's empty results exactly:
            % it returns logical([]) for bool arrays and [] otherwise.
            if strcmp(ml_class, 'logical')
                data = logical([]);
            else
                data = [];
            end
            ok = true;
            return;
        end

        raw = desc{'buffer'};
        if ~isa(raw, 'uint8')
            raw = uint8(raw);   % py.bytes -> uint8, one memcpy
        end
        raw = reshape(raw, 1, []);   % typecast requires a vector

        if strcmp(ml_class, 'logical')
            vec = logical(raw);
        else
            vec = typecast(raw, ml_class);
            % The element-by-element path returns DOUBLE for every numeric
            % dtype -- single and int*/uint* included. TestDataRoundTrip
            % pins that (test_int32_array, test_single_precision) with
            % verifyEqual, which is strict about class. Match it: this is a
            % transport optimization, not a type-contract change. For the
            % float64 data this exists for, the cast is a no-op.
            if ~isa(vec, 'double')
                vec = double(vec);
            end
        end

        % A size disagreement means the description and the buffer are out
        % of step (a mis-mapped dtype, a truncated read).  Refuse rather
        % than hand back silently wrong data -- the fallback is correct.
        if numel(vec) ~= n_expected
            scidb.Log.warn(['from_python: buffer path produced %d element(s) ' ...
                'but the array holds %d -- falling back to element-by-element'], ...
                numel(vec), n_expected);
            data = [];
            return;
        end

        if numel(arr_shape) <= 1
            % 0-D and 1-D both land as a column vector, matching the
            % trailing ndim==1 normalization the caller applies.
            data = vec(:);
        else
            % The bytes were written in Fortran order precisely so this
            % reshape needs no permute: MATLAB is column-major, numpy is not.
            data = reshape(vec, double(arr_shape));
        end
        ok = true;
    catch conv_err
        scidb.Log.debug(['from_python: buffer conversion failed (%s) -- ' ...
            'falling back to element-by-element'], conv_err.message);
        data = [];
        ok = false;
    end
end


function warn_if_large_slow_conversion(arr_shape, py_c)
%WARN_IF_LARGE_SLOW_CONVERSION  Say that a big element-wise conversion started.
%   The element-by-element path costs one Python->MATLAB crossing per
%   element.  Past roughly a million elements that is minutes, and every
%   other message on that path is DEBUG -- so at the INFO level the run
%   normally goes silent for the whole conversion and reads as a hang.
%   State it at WARN instead, with the element count, so a slow run is
%   diagnosable from an ordinary log.
    if isempty(arr_shape)
        n_elems = 1;
    else
        n_elems = prod(double(arr_shape));
    end
    if n_elems < 1e6
        return;
    end
    try
        dtype_str = string(char(py.str(py_c.dtype)));
    catch
        dtype_str = "unknown";
    end
    scidb.Log.warn(['from_python: converting a %d-element %s array ' ...
        'element-by-element (the bulk buffer path declined) -- this crosses ' ...
        'the Python bridge once per element and can take minutes'], ...
        n_elems, dtype_str);
end


function s = format_slowest_columns(col_names, col_secs)
%FORMAT_SLOWEST_COLUMNS  Name the columns that dominated a slow conversion.
%   Lists at most the three most expensive columns, each over 0.1s, so the
%   timing line stays one line regardless of how wide the DataFrame is.
    keep = col_secs > 0.1;
    if ~any(keep)
        s = 'no single column over 0.1s';
        return;
    end
    idx = find(keep);
    [~, order] = sort(col_secs(idx), 'descend');
    idx = idx(order);
    idx = idx(1:min(3, numel(idx)));
    parts = cell(1, numel(idx));
    for k = 1:numel(idx)
        parts{k} = sprintf('%s=%.3fs', string(col_names{idx(k)}), col_secs(idx(k)));
    end
    s = ['slowest: ' strjoin(parts, ', ')];
end
