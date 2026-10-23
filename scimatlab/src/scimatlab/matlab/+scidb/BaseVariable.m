classdef BaseVariable < dynamicprops
%SCIDB.BASEVARIABLE  Base class for all database-storable variable types.
%
%   Define variable types as empty subclasses:
%
%       classdef RawSignal < scidb.BaseVariable
%       end
%
%   Then use instance methods for all database operations:
%
%       RawSignal().save(data, subject=1, session="A")
%       var = RawSignal().load(subject=1, session="A")
%
%   The class name becomes the database table name automatically via
%   class(obj).  No additional properties or methods are needed.
%
%   Custom storage: to save a value of your own class (an object, not a
%   numeric/string/struct/table), override BOTH conversions. save() and
%   load() then call them automatically:
%
%       classdef MarkerTrajectory < scidb.BaseVariable
%           methods
%               function tbl = to_db(obj)       % obj.data -> table
%                   tbl = table(obj.data.t(:), 'VariableNames', {'t'});
%               end
%           end
%           methods (Static)
%               function data = from_db(tbl)    % table -> original value
%                   data = Trajectory(tbl.t);
%               end
%           end
%       end
%
%   Changing how a variable is stored is a change of type: raise its
%   schema_version (scidb.register_variable) when old records no longer
%   load. Python and the GUI read such a variable as its stored table.
%
%   Properties (populated after load):
%       data         - The loaded data (MATLAB native type)
%       record_id    - Unique record identifier (string)
%       metadata     - Struct of metadata key-value pairs
%       content_hash - Content hash of the data (string)
%       py_obj       - Python BaseVariable shadow (used internally)

    properties
        data                    % MATLAB data
        record_id    string     % Unique record ID
        metadata     struct     % Metadata key-value pairs
        branch_params struct    % Branch parameters (computational variant discriminator)
        content_hash string     % Content hash (16-char hex)
        py_obj                  % Python BaseVariable shadow (internal)
        selected_columns string % Column names to extract on load (empty = all columns)
        iterate logical = false % for_columns: iterate per-column and reassemble
    end

    methods
        function obj = BaseVariable(varargin)
        %BASEVARIABLE  Construct a BaseVariable, optionally with column selection.
        %
        %   OBJ = TypeClass()           % no column selection (full data)
        %   OBJ = TypeClass("col")      % single column selection
        %   OBJ = TypeClass(["c1","c2"]) % multiple column selection
        %
        %   When column selection is specified and the variable is used as
        %   input to scidb.for_each(), only the requested columns are
        %   extracted from the loaded table and passed to the function.
        %   Single column returns a numeric/cell array; multiple columns
        %   return a MATLAB subtable.
            obj.metadata = struct();
            obj.branch_params = struct();
            obj.selected_columns = string.empty;
            obj.iterate = false;
            if nargin >= 1
                cols = varargin{1};
                obj.selected_columns = string(cols);
            end
        end

        % -----------------------------------------------------------------
        % for_columns
        % -----------------------------------------------------------------
        function obj = for_columns(obj, cols)
        %FOR_COLUMNS  Iterate a for_each function over each column and reassemble.
        %
        %   OBJ = TypeClass().for_columns()          % all data columns
        %   OBJ = TypeClass().for_columns("col")     % one column
        %   OBJ = TypeClass().for_columns(["a","b"]) % a subset
        %
        %   When used as a scidb.for_each() input, the function runs once per
        %   column (each column passed as a single-column argument) and the
        %   per-column scalar results are reassembled into ONE output variable
        %   whose data, per schema combo, is a one-row table (1xN) with the
        %   same column names as the source. ``for_columns()`` with no argument
        %   means "all data columns", resolved at for_each time.
        %
        %   Multiple for_columns inputs in one call are zipped by column name
        %   and must resolve to the same column set.
            if nargin >= 2 && ~isempty(cols)
                obj.selected_columns = string(cols);
            else
                obj.selected_columns = string.empty;
            end
            obj.iterate = true;
        end

        % -----------------------------------------------------------------
        % clear
        % -----------------------------------------------------------------
        function [] = clear(obj, confirm)
        %CLEAR  Delete all data for this variable type and re-register.
        %
        %   TypeClass().clear()         % prompts for confirmation
        %   TypeClass().clear('y')      % skips prompt
        %
        %   Drops the data table and removes this type's entries from
        %   _variables, _record_save, and _record.

            type_name = class(obj);
            duck = py.getattr(scidb.get_database(), '_duck');

            if nargin < 2
                confirm = input(['This action will irreversibly delete all data for variable "' type_name '"! Type "y" (with quotes) to proceed (default n): ']);
                if isempty(confirm)
                    confirm = 'n';
                end
            end
            if ~startsWith(confirm, 'y')
                disp(['Aborted ' type_name '.clear()']);
                return;
            end

            % _record_save is keyed by record_id only (no variable_name
            % column post-migration); delete its rows for this type's records
            % via a subquery against _record (whose `type` column holds the
            % class name), then drop the _record entities themselves. Run the
            % _record_save delete before the _record delete so the subquery
            % still sees the rows. _variables stores the bare class name.
            duck.con.execute(['DROP TABLE IF EXISTS "' type_name '_data"']);
            duck.con.execute(['DELETE FROM _variables WHERE variable_name = ''' type_name '''']);
            duck.con.execute(['DELETE FROM _record_save WHERE record_id IN (SELECT record_id FROM _record WHERE type = ''' type_name ''')']);
            duck.con.execute(['DELETE FROM _record WHERE type = ''' type_name '''']);
            fprintf('Cleared all data for %s\n', type_name);
        end

        % -----------------------------------------------------------------
        % save
        % -----------------------------------------------------------------
        function record_id = save(obj, data, varargin)
        %SAVE  Save data to the database under this variable type.
        %
        %   RECORD_ID = TypeClass().save(DATA, Name, Value, ...)
        %
        %   DATA can be a numeric array, scalar, scidb.BaseVariable
        %   (re-save), or a MATLAB table. (Lineage is recorded automatically
        %   by scidb.for_each; there is no per-call lineage wrapper.)
        %
        %   Table auto-distribute: if DATA is a table whose column names
        %   include any configured dataset_schema_keys, save() automatically
        %   saves one record per row.  Schema-key columns become per-row
        %   metadata; remaining columns become the data.  If only one
        %   non-schema column exists the scalar value is saved directly;
        %   if multiple non-schema columns exist each row's sub-table is
        %   saved as the data.  Returns a string array of record_ids in
        %   this case.  Tables with no schema-key columns are saved as a
        %   single record (existing behaviour).
        %
        %   Name-Value Arguments:
        %       db - Optional DatabaseManager to use instead of the global
        %            database (returned by scidb.configure_database).
        %       Any other name-value pairs are metadata (e.g. subject=1).
        %       When auto-distributing a table, extra pairs become common
        %       metadata applied to every row.
        %
        %   Example:
        %       RawSignal().save(randn(100,3), subject=1, session="A");
        %
        %       % Auto-distribute a table (subject is a schema key)
        %       SubjectGrouping().save(groupingTbl);   % 1 record per row
        %
        %       % Save to a specific database
        %       RawSignal().save(data, db=db2, subject=1);

            type_name = class(obj);
            [py_class, codec] = scidb.internal.ensure_registered(type_name);

            % Extract db option and build kwargs (needed for all paths)
            [metadata_nv, db_val] = extract_db(varargin);
            py_kwargs = scidb.internal.metadata_to_pykwargs(metadata_nv{:});

            % Custom to_db/from_db: store the class's to_db table. It is one
            % record, never auto-distributed (Python's custom path never
            % distributes either). A re-saved BaseVariable is already stored.
            if codec.has_codec && ~isa(data, 'scidb.BaseVariable')
                encoded = codec.encode({data});
                data = encoded{1};
            end

            % Auto-detect table-distribute: if data is a table with columns
            % matching the configured schema keys, dispatch to save_from_table.
            if istable(data) && ~codec.has_codec
                if isempty(db_val)
                    py_db_check = py.scidb.database.get_database();
                else
                    py_db_check = db_val;
                end
                schema_keys = string(cell(py.list(py_db_check.dataset_schema_keys)));
                tbl_cols    = string(data.Properties.VariableNames);
                meta_cols   = intersect(schema_keys, tbl_cols, 'stable');
                if ~isempty(meta_cols)
                    data_cols = setdiff(tbl_cols, schema_keys, 'stable');
                    if isempty(data_cols)
                        error('scidb:SaveError', ...
                            'Table has no data columns (all columns are schema keys: %s).', ...
                            strjoin(meta_cols, ', '));
                    elseif numel(data_cols) == 1
                        scidb.Log.info(['[save] auto-distribute %s: single-col branch, ' ...
                            'meta=[%s], data_col=%s, rows=%d'], ...
                            type_name, strjoin(meta_cols, ','), data_cols(1), height(data));
                        record_id = obj.save_from_table(data, data_cols(1), meta_cols, varargin{:});
                    else
                        scidb.Log.info(['[save] auto-distribute %s: multi-col branch, ' ...
                            'meta=[%s], data_cols=[%s], rows=%d'], ...
                            type_name, strjoin(meta_cols, ','), strjoin(data_cols, ','), height(data));
                        % Multiple data columns: wrap each row's sub-table as a cell
                        sub_data  = data(:, data_cols);
                        meta_tbl  = data(:, meta_cols);
                        row_cells = cell(height(data), 1);
                        for ri = 1:height(data)
                            row_cells{ri} = sub_data(ri, :);
                        end
                        aug_tbl   = [meta_tbl, table(row_cells, 'VariableNames', {'scidb_row_data_'})];
                        record_id = obj.save_from_table(aug_tbl, 'scidb_row_data_', meta_cols, varargin{:});
                    end
                    return;
                end
                % No schema-key columns in table → fall through and save as one record.
                scidb.Log.info(['[save] auto-distribute %s: fall-through branch ' ...
                    '(no schema-key columns), cols=[%s], rows=%d'], ...
                    type_name, strjoin(tbl_cols, ','), height(data));
            end

            % Marshal data to Python for plain save
            if isa(data, 'scidb.BaseVariable')
                py_data = data.py_obj;
            else
                py_data = scidb.internal.to_python(data);
            end

            if isempty(db_val)
                py_db = py.scidb.database.get_database();
            else
                py_db = db_val;
            end
            py_record_id = py_db.save_variable(py_class, py_data, pyargs(py_kwargs{:}));
            record_id = string(py_record_id);


        end

        % -----------------------------------------------------------------
        % save_from_table
        % -----------------------------------------------------------------
        function record_ids = save_from_table(obj, tbl, data_column, metadata_columns, varargin)
        %SAVE_FROM_TABLE  Bulk-save each row of a MATLAB table as a separate record.
        %
        %   RECORD_IDS = TypeClass().save_from_table(TBL, DATA_COL, META_COLS, ...)
        %
        %   Uses a batched code path (~100x faster than looping save()).
        %
        %   Arguments:
        %       TBL              - MATLAB table where each row is a record
        %       DATA_COL         - Name of the column containing data values
        %                          (string or char)
        %       META_COLS        - Column names to use as per-row metadata
        %                          (string array or cell array of char)
        %
        %   Name-Value Arguments:
        %       db - Optional DatabaseManager to use instead of the global
        %            database (returned by scidb.configure_database).
        %       Any other name-value pairs are common metadata applied to
        %       every row (e.g. experiment="exp1").
        %
        %   Returns:
        %       String array of record_ids, one per row.
        %
        %   Example:
        %       % Table with 10 rows (2 subjects x 5 trials)
        %       %   Subject  Trial  MyVar
        %       %   1        1      0.5
        %       %   1        2      0.6
        %       %   ...
        %
        %       ids = ScalarValue().save_from_table( ...
        %           results_tbl, "MyVar", ["Subject", "Trial"], ...
        %           experiment="exp1");

            type_name = class(obj);
            [~, codec] = scidb.internal.ensure_registered(type_name);

            % Normalise inputs
            if isstring(data_column), data_column = char(data_column); end
            if isstring(metadata_columns)
                metadata_columns = cellstr(metadata_columns);
            end

            % Handle empty table
            if height(tbl) == 0
                record_ids = string.empty;
                return;
            end

            % Separate db option from common metadata
            [common_nv, db_val] = extract_db(varargin);

            % --- Convert data column to Python (numpy for numeric) ---
            data_col = tbl.(data_column);

            % Custom to_db/from_db: to_db on EVERY row first; the cell of
            % tables then crosses the bridge in one piece (Strategy A
            % vertcat + row heights) and Python saves the batch at once.
            if codec.has_codec
                if istable(data_col)
                    data_col = arrayfun(@(r) data_col(r, :), ...
                        (1:height(data_col))', 'UniformOutput', false);
                elseif ~iscell(data_col)
                    data_col = num2cell(data_col);
                end
                data_col = codec.encode(data_col(:));
            end

            % A datetime data column: one pd.Timestamp / NaT per record, in
            % the column's zone (datetime_to_python; it was ISO text, so the
            % zone and NaT were lost and the column stored as VARCHAR).
            is_datetime_col = isdatetime(data_col) && isvector(data_col);
            if iscategorical(data_col)
                if any(isnan(str2double(string(data_col))))
                    data_col = string(data_col);
                else
                    data_col = double(data_col);
                end
            end
            py_heights = py.None;  % overridden to a heights array by Strategy A
            if is_datetime_col
                py_data = scidb.internal.datetime_to_python(data_col, 'list');
            elseif isnumeric(data_col)
                py_data = py.numpy.array(data_col(:)');
            elseif isstring(data_col)
                py_data = py.list(cellfun(@char, num2cell(data_col(:)'), ...
                    'UniformOutput', false));
            elseif iscellstr(data_col) %#ok<ISCLSTR>
                py_data = py.list(data_col(:)');
            else
                t_conv = tic;
                bulk_mode = 'per_row';

                % Strategy A: vertcat — bulk-concatenate all records into one
                % MATLAB structure, then cross the bridge once.  Works for
                % cell of tables, cell of equal-length arrays, cell of structs.
                if iscell(data_col)
                    try
                        concat_data = vertcat(data_col{:});
                        heights_vec = int64(cellfun(@(x) size(x, 1), data_col(:)', 'UniformOutput', true));
                        py_data = scidb.internal.to_python(concat_data);
                        py_heights = py.numpy.array(heights_vec, pyargs('dtype', 'int64'));
                        bulk_mode = 'vertcat';
                    catch
                    end
                end

                % Strategy B: flatten — cell of variable-length numeric/logical
                % vectors.  try_flatten_cell_column already lives in to_python.m.
                if strcmp(bulk_mode, 'per_row') && iscell(data_col)
                    [can_flat, flat, lengths, flat_dtype] = ...
                        scidb.internal.try_flatten_cell_column(data_col);
                    if can_flat
                        py_flat    = py.numpy.array(flat,    pyargs('dtype', flat_dtype));
                        py_lengths = py.numpy.array(lengths, pyargs('dtype', 'int64'));
                        py_data    = py.scimatlab.bridge.split_flat_to_lists(py_flat, py_lengths);
                        bulk_mode  = 'flatten';
                    end
                end

                % Strategy C: per-row fallback for heterogeneous / non-
                % concatenable data.
                if strcmp(bulk_mode, 'per_row')
                    py_data = py.list();
                    for i = 1:height(tbl)
                        if iscell(data_col)
                            py_data.append(scidb.internal.to_python(data_col{i}));
                        else
                            py_data.append(scidb.internal.to_python(data_col(i)));
                        end
                    end
                end

                scidb.Log.info('[timing] save_from_table(%s): data_convert=%.3fs, mode=%s, n=%d', ...
                    type_name, toc(t_conv), bulk_mode, height(tbl));
            end

            % --- Convert metadata columns (numpy for numeric) ---
            py_meta_keys = py.list(metadata_columns);
            py_meta_cols = py.list();
            for j = 1:numel(metadata_columns)
                col = tbl.(metadata_columns{j});
                if iscategorical(col)
                    col = string(col); % Can't convert categorical to Python
                elseif isdatetime(col)
                    col = string(col, 'yyyy-MM-dd''T''HH:mm:ss.SSS');
                elseif isstruct(col)
                    % Struct metadata column -> JSON strings (one per row)
                    json_strs = strings(numel(col), 1);
                    for si = 1:numel(col)
                        json_strs(si) = string(jsonencode(col(si)));
                    end
                    col = json_strs;
                end
                if isnumeric(col)
                    % Same int-vs-float rule as save()'s metadata, so one
                    % location gets one record id on both routes.
                    py_meta_cols.append(scidb.internal.number_to_python(col(:)'));
                elseif isstring(col)
                    % Join into single string (1 boundary crossing vs N)
                    py_meta_cols.append(strjoin(col(:)', char(30)));
                elseif iscellstr(col) %#ok<ISCLSTR>
                    py_meta_cols.append(strjoin(string(col(:)'), char(30)));
                else
                    py_col = py.list();
                    for i = 1:height(tbl)
                        py_col.append(scidb.internal.to_python(col(i)));
                    end
                    py_meta_cols.append(py_col);
                end
            end

            % --- Build common metadata dict ---
            py_common = scidb.internal.metadata_to_pydict(common_nv{:});

            % --- Database ---
            if isempty(db_val)
                py_db = py.None;
            else
                py_db = db_val;
            end

            % --- Call Python bridge ---
            % py_heights is py.None except when Strategy A (vertcat) was used.
            py_result = py.scimatlab.bridge.save_batch_bridge( ...
                type_name, py_data, py_meta_keys, py_meta_cols, ...
                py_common, py_db, py_heights);

            % --- Convert result to MATLAB string array ---
            record_ids = splitlines(string(py_result));

        end

        % -----------------------------------------------------------------
        % load
        % -----------------------------------------------------------------
        function result = load(obj, varargin)
        %LOAD  Load variable(s) from the database.
        %
        %   RESULT = TypeClass().load(Name, Value, ...)
        %
        %   Returns a single BaseVariable when exactly one record matches
        %   (default as_table=false), a BaseVariable array when multiple
        %   records match, or a MATLAB table when as_table=true.
        %
        %   Name-Value Arguments:
        %       Any metadata key-value pairs (e.g. subject=1, session="A")
        %       version     - Which version(s) to load:
        %                     "latest" (default): latest per schema/version-key
        %                     "all"   : every stored version
        %                     string  : specific record_id (single result)
        %       as_table    - If true, always return a MATLAB table (even
        %                     for a single result). Default false.
        %       categorical - If true, convert metadata columns in the
        %                     result table to categorical (default false).
        %                     Only applies when as_table=true.
        %       db          - Optional DatabaseManager to use instead of the
        %                     global database
        %       introspect  - If true, attach call-context fields to each
        %                     returned BaseVariable (.where, .version_mode),
        %                     or append introspection columns to a table
        %                     result (record_id, branch_params, content_hash,
        %                     where, version_mode). Default false.
        %
        %   Example:
        %       % Load single record
        %       var = RawSignal().load(subject=1, session="A");
        %
        %       % Load all stored versions
        %       vars = RawSignal().load(version="all", subject=1);
        %
        %       % Load as table
        %       tbl = RawSignal().load(as_table=true, subject=1);
        %
        %       % Inspect internal metadata
        %       var = RawSignal().load(subject=1, session="A", introspect=true);
        %       disp(var.record_id); disp(var.branch_params); disp(var.where);

            type_name = class(obj);
            [py_class, codec] = scidb.internal.ensure_registered(type_name);

            [metadata_args, version, as_table, db_val, where, categorical_flag, introspect] = ...
                split_load_args(varargin{:});
            py_metadata = scidb.internal.metadata_to_pydict(metadata_args{:});

            if isempty(db_val)
                py_db = py.scidb.database.get_database();
            else
                py_db = db_val;
            end

            % All version modes ("latest", "all", or a specific record_id)
            % route through the bridge's load_and_extract, which drains the
            % generator returned by DatabaseManager.load() in Python. A
            % specific record_id resolves to a single record (n==1); passing
            % version_id straight through lets scidb's load() dispatch on it.
            if isempty(where)
                bulk = py.scimatlab.bridge.load_and_extract( ...
                    py_class, py_metadata, ...
                    pyargs('version_id', char(version), 'db', py_db));
            else
                bulk = py.scimatlab.bridge.load_and_extract( ...
                    py_class, py_metadata, ...
                    pyargs('version_id', char(version), 'db', py_db, ...
                           'where', where.py_filter));
            end
            n = int64(bulk{'n'});

            if n == 0
                error('scidb:NotFoundError', 'No %s found matching the given metadata.', type_name);
            end

            results_arr = scidb.BaseVariable.wrap_py_vars_batch(bulk);

            % Custom to_db/from_db: the batch arrives as stored tables;
            % from_db rebuilds every value in one pass.
            if codec.has_codec
                decoded = codec.decode({results_arr.data});
                for ii = 1:numel(results_arr)
                    results_arr(ii).data = decoded{ii};
                end
            end

            if as_table
                result = multi_result_to_table(results_arr, type_name, categorical_flag);
                if introspect
                    result = append_introspect_columns(result, results_arr, where, version);
                end
            else
                if introspect
                    for ii = 1:numel(results_arr)
                        results_arr(ii).addprop('where');
                        results_arr(ii).where = where;
                        results_arr(ii).addprop('version_mode');
                        results_arr(ii).version_mode = version;
                    end
                end
                if n == 1
                    result = results_arr(1);
                else
                    result = results_arr;
                end
            end

        end

        % -----------------------------------------------------------------
        % head
        % -----------------------------------------------------------------
        function tbl = head(obj, n, varargin)
        %HEAD  Peek at the first N records (latest version).
        %
        %   TBL = TypeClass().head()          % first 5 records
        %   TBL = TypeClass().head(10)        % first 10 records
        %   TBL = TypeClass().head(3, subject=1)  % first 3 for subject 1
        %
        %   Returns a MATLAB table with schema key columns and a 'data'
        %   column.  Returns an empty table if no records exist.
        %
        %   Arguments:
        %       n  - Number of records to return (default 5)
        %
        %   Name-Value Arguments:
        %       db - Optional DatabaseManager to use instead of the global
        %            database
        %       Any other name-value pairs are metadata filters.

            if nargin < 2
                n = 5;
            end

            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);

            [metadata_nv, db_val] = extract_db(varargin);
            py_kwargs = scidb.internal.metadata_to_pykwargs(metadata_nv{:});

            if isempty(db_val)
                py_result = py_class.head(pyargs('n', int64(n), py_kwargs{:}));
            else
                py_result = py_class.head(pyargs('n', int64(n), 'db', db_val, py_kwargs{:}));
            end

            if int64(py.builtins.len(py_result)) == 0
                tbl = table();
            else
                tbl = scidb.internal.from_python(py_result);
            end
        end

        % -----------------------------------------------------------------
        % to_csv
        % -----------------------------------------------------------------
        function to_csv(obj, filename, varargin)
        %TO_CSV  Export this variable to a CSV file in flat table format.
        %
        %   TypeClass().to_csv(FILENAME, Name, Value, ...)
        %
        %   Loads every record matching the metadata filters and writes one
        %   row per unique schema_id: one column per schema key (e.g.
        %   subject, trial) plus a single value column named after this
        %   variable type.  A trial-level variable produces one row per
        %   subject/trial combination; a subject-level variable produces one
        %   row per subject (no trial column).
        %
        %   Only SCALAR variables are supported (one value per schema_id).
        %   If any record holds a vector (DOUBLE[]/DOUBLE[][]), a multi-row
        %   table, or any other non-scalar value, an error is raised.
        %
        %   Arguments:
        %       FILENAME - Required output path.  Must end with '.csv' or an
        %                  error is raised.
        %
        %   A single-row table variable exports one column per table column;
        %   the constraint is one row per schema_id, not one column. Column
        %   selection is honored: GaitData("Speed").to_csv(...) exports only
        %   the selected column(s).
        %
        %   Name-Value Arguments (forwarded to load(), so every load()
        %   option is supported):
        %       version - "latest" (default), "all", or a specific record_id.
        %       where   - A scidb.Filter (e.g. Side() == "L").
        %       db      - Optional DatabaseManager instead of the global one.
        %       Any other name-value pairs are metadata filters (schema keys),
        %       or branch-param keys for scidb.Variant selection.
        %
        %   Example:
        %       StepLength().to_csv("step_lengths.csv", subject=1);
        %       StepLength().to_csv("clean.csv", where=Side() == "L", low_hz=20);
        %       GaitData("Speed").to_csv("speed.csv", subject=1);

            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);

            scidb.internal.validate_csv_filename(filename);
            scidb.internal.reject_csv_spec_args(type_name, varargin);

            [metadata_args, version, where, db_val] = ...
                scidb.internal.split_csv_args(varargin{:});
            py_kwargs = scidb.internal.build_csv_kwargs( ...
                metadata_args, version, where, db_val);

            scidb.Log.info('[to_csv] %s -> %s (version=%s)', ...
                type_name, string(filename), version);

            % Column selection (e.g. GaitData("Speed")) maps onto the Python
            % ColumnSelection wrapper, which has its own to_csv dispatched on an
            % INSTANCE (MATLAB resolves instance methods fine).
            %
            % The plain-class case must NOT call py_class.to_csv(...): MATLAB
            % cannot dispatch the inherited to_csv classmethod on the surrogate
            % CLASS object (methods(py_class) omits it though Python hasattr sees
            % it). Route it through the bridge instead — same pattern as
            % save/load.
            if isempty(obj.selected_columns)
                py.scimatlab.bridge.to_csv_bridge( ...
                    type_name, char(filename), pyargs(py_kwargs{:}));
            else
                cols = cellstr(obj.selected_columns(:)');
                py_spec = py.scidb.column_selection.ColumnSelection( ...
                    py_class, py.list(cols));
                py_spec.to_csv(char(filename), pyargs(py_kwargs{:}));
            end
        end

        % -----------------------------------------------------------------
        % list_versions
        % -----------------------------------------------------------------
        function versions = list_versions(obj, varargin)
        %LIST_VERSIONS  List all versions at a schema location.
        %
        %   VERSIONS = TypeClass().list_versions(Name, Value, ...)
        %
        %   Name-Value Arguments:
        %       db - Optional DatabaseManager to use instead of the global
        %            database
        %       Any other name-value pairs are metadata filters.
        %
        %   Returns a struct array with fields: record_id, schema,
        %   branch_params, timestamp.
        %
        %   Example:
        %       v = ProcessedSignal().list_versions(subject=1, session="A");

            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);

            [metadata_nv, db_val] = extract_db(varargin);
            py_kwargs = scidb.internal.metadata_to_pykwargs(metadata_nv{:});

            if isempty(db_val)
                py_db = py.scidb.database.get_database();
            else
                py_db = db_val;
            end
            py_list = py_db.list_versions(py_class, pyargs(py_kwargs{:}));

            n = int64(py.builtins.len(py_list));
            versions = struct('record_id', {}, 'schema', {}, 'branch_params', {}, 'timestamp', {});

            for i = 1:n
                py_dict = py_list{i};
                versions(i).record_id     = string(py_dict{'record_id'});
                versions(i).schema        = scidb.internal.pydict_to_struct(py_dict{'schema'});
                versions(i).branch_params = scidb.internal.pydict_to_struct(py_dict{'branch_params'});
                versions(i).timestamp     = string(py_dict{'timestamp'});
            end

            
        end

        % -----------------------------------------------------------------
        % provenance
        % -----------------------------------------------------------------
        function prov = provenance(obj, varargin)
        %PROVENANCE  Get the provenance (lineage) of a variable.
        %
        %   PROV = TypeClass().provenance(Name, Value, ...)
        %
        %   Name-Value Arguments:
        %       db - Optional DatabaseManager to use instead of the global
        %            database
        %       Any other name-value pairs are metadata filters.
        %
        %   Returns a struct with fields:
        %       function_name - producing function name (string)
        %       function_hash - producing function source hash (string)
        %       inputs        - cell array of structs, one per variable input,
        %                       each with record_id, param_name, variable_type
        %       constants     - struct mapping constant param name -> value
        %   All fields are derived from the bipartite provenance graph.
        %   Returns [] if the record has no producing invocation (raw save).
        %
        %   Example:
        %       p = ProcessedSignal().provenance(subject=1, session="A");

            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);

            [metadata_args, version, db_val] = scidb.internal.split_version_arg(varargin{:});
            py_kwargs = scidb.internal.metadata_to_pykwargs(metadata_args{:});

            if isempty(db_val)
                py_db = py.scidb.database.get_database();
            else
                py_db = db_val;
            end
            if version ~= "latest"
                py_result = py_db.get_provenance(py.None, pyargs('version', char(version)));
            else
                py_result = py_db.get_provenance(py_class, pyargs(py_kwargs{:}));
            end

            if isa(py_result, 'py.NoneType')
                prov = [];
            else
                prov.function_name = string(py_result{'function_name'});
                prov.function_hash = string(py_result{'function_hash'});
                prov.inputs        = scidb.internal.pylist_to_cell(py_result{'inputs'});
                % constants is a {param_name: value} dict in the graph model.
                prov.constants     = scidb.internal.pydict_to_struct(py_result{'constants'});
            end

            
        end

        % -----------------------------------------------------------------
        % Comparison operators (for where= filter syntax)
        % -----------------------------------------------------------------
        function filt = eq(obj, other)
        %EQ  Create a filter: TypeClass() == value or TypeClass("col") == value
        %
        %   FILT = TypeClass() == value
        %   FILT = TypeClass("column") == value
        %
        %   Returns a scidb.Filter that can be passed to load()
        %   via the 'where' key. Creates a ColumnFilter when column
        %   selection is active, otherwise creates a VariableFilter.
            if isa(other, 'scidb.BaseVariable')
                filt = builtin('eq', obj, other);
                return;
            end
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);

            % Check if column selection is active
            if ~isempty(obj.selected_columns)
                % Use ColumnFilter for column-specific filtering
                column = char(obj.selected_columns(1));  % Use first column
                py_filter = py.scidb.filters.ColumnFilter(py_class, column, '==', py_val);
            else
                % Use VariableFilter for whole-variable filtering
                py_filter = py.scidb.filters.VariableFilter(py_class, '==', py_val);
            end
            filt = scidb.Filter(py_filter);
        end

        function filt = ne(obj, other)
        %NE  Create a filter: TypeClass() ~= value or TypeClass("col") ~= value
            if isa(other, 'scidb.BaseVariable')
                filt = builtin('ne', obj, other);
                return;
            end
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);

            % Check if column selection is active
            if ~isempty(obj.selected_columns)
                % Use ColumnFilter for column-specific filtering
                column = char(obj.selected_columns(1));  % Use first column
                py_filter = py.scidb.filters.ColumnFilter(py_class, column, '!=', py_val);
            else
                % Use VariableFilter for whole-variable filtering
                py_filter = py.scidb.filters.VariableFilter(py_class, '!=', py_val);
            end
            filt = scidb.Filter(py_filter);
        end

        function filt = lt(obj, other)
        %LT  Create a filter: TypeClass() < value or TypeClass("col") < value
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);

            % Check if column selection is active
            if ~isempty(obj.selected_columns)
                % Use ColumnFilter for column-specific filtering
                column = char(obj.selected_columns(1));  % Use first column
                py_filter = py.scidb.filters.ColumnFilter(py_class, column, '<', py_val);
            else
                % Use VariableFilter for whole-variable filtering
                py_filter = py.scidb.filters.VariableFilter(py_class, '<', py_val);
            end
            filt = scidb.Filter(py_filter);
        end

        function filt = le(obj, other)
        %LE  Create a filter: TypeClass() <= value or TypeClass("col") <= value
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);

            % Check if column selection is active
            if ~isempty(obj.selected_columns)
                % Use ColumnFilter for column-specific filtering
                column = char(obj.selected_columns(1));  % Use first column
                py_filter = py.scidb.filters.ColumnFilter(py_class, column, '<=', py_val);
            else
                % Use VariableFilter for whole-variable filtering
                py_filter = py.scidb.filters.VariableFilter(py_class, '<=', py_val);
            end
            filt = scidb.Filter(py_filter);
        end

        function filt = gt(obj, other)
        %GT  Create a filter: TypeClass() > value or TypeClass("col") > value
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);

            % Check if column selection is active
            if ~isempty(obj.selected_columns)
                % Use ColumnFilter for column-specific filtering
                column = char(obj.selected_columns(1));  % Use first column
                py_filter = py.scidb.filters.ColumnFilter(py_class, column, '>', py_val);
            else
                % Use VariableFilter for whole-variable filtering
                py_filter = py.scidb.filters.VariableFilter(py_class, '>', py_val);
            end
            filt = scidb.Filter(py_filter);
        end

        function filt = ge(obj, other)
        %GE  Create a filter: TypeClass() >= value or TypeClass("col") >= value
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);

            % Check if column selection is active
            if ~isempty(obj.selected_columns)
                % Use ColumnFilter for column-specific filtering
                column = char(obj.selected_columns(1));  % Use first column
                py_filter = py.scidb.filters.ColumnFilter(py_class, column, '>=', py_val);
            else
                % Use VariableFilter for whole-variable filtering
                py_filter = py.scidb.filters.VariableFilter(py_class, '>=', py_val);
            end
            filt = scidb.Filter(py_filter);
        end        

        % -----------------------------------------------------------------
        % disp
        % -----------------------------------------------------------------
        function disp(obj)
        %DISP  Display the BaseVariable.
            if isempty(obj.data)
                fprintf('  %s (empty)\n', class(obj));
            else
                fprintf('  %s [%s]\n', class(obj), obj.record_id);
                fprintf('    data: %s\n', class(obj.data));
                if ~isempty(fieldnames(obj.metadata))
                    fprintf('    metadata: ');
                    disp(obj.metadata);
                end
            end
        end
    end

    methods (Static)
        function obj = str2var(s)
            %STR2VAR  Convert a string to a BaseVariable (for flexibility).
            %   This allows users to write code like:
            %      RawSignal = BaseVariable.str2var("RawSignal");
            if isstring(s), s = char(s); end
            var_folder = fullfile(pwd, 'src', 'vars');
            classFileName = fullfile(var_folder, [s '.m']);            
            if ~isfile(classFileName)
                classDefText = sprintf('classdef %s < scidb.BaseVariable\nend\n', s);
                % Write the text to the file
                fid = fopen(classFileName, 'w');
                if fid == -1
                    error('Could not create class definition file: %s', classFileName);
                end
                fprintf(fid, '%s', classDefText);
                fclose(fid);
                % Refresh the MATLAB path to recognize the new class
                rehash;
            end

            py_class = scidb.internal.ensure_registered(s);            

            obj = eval([s '()']); % Call the newly created class
            obj.py_obj = py_class([]);  % Create an empty Python BaseVariable shadow
        end

        function v = wrap_py_var(py_var)
        %WRAP_PY_VAR  Convert a Python BaseVariable to a MATLAB BaseVariable.
        %   This is used internally to convert results from the database into
        %   MATLAB objects.  The returned BaseVariable has the .py_obj property
        %   set to the original Python BaseVariable shadow, so that provenance
        %   is preserved if it's passed to scidb.for_each or re-saved.
        % Usage: v = scidb.BaseVariable.wrap_py_var(py_var)
            matlab_data = scidb.internal.from_python(py_var.data);
            matlab_data = scidb.internal.try_stack_numeric(matlab_data);
            v = scidb.BaseVariable();
            v.data = matlab_data;
            v.py_obj = py_var;
            v.record_id = string(py_var.record_id);
            v.content_hash = string(py_var.content_hash);

            py_meta = py_var.metadata;
            if ~isa(py_meta, 'py.NoneType')
                v.metadata = scidb.internal.pydict_to_struct(py_meta);
            end

            py_bp = py_var.branch_params;
            if ~isa(py_bp, 'py.NoneType') && ~isempty(py_bp)
                v.branch_params = scidb.internal.pydict_to_struct(py_bp);
            end

        end

        function results = wrap_py_vars_batch(bulk)
        %WRAP_PY_VARS_BATCH  Batch-convert Python BaseVariables to MATLAB BaseVariables.
        %   Accepts the bulk dict returned by bridge.load_and_extract() or
        %   bridge.wrap_batch_bridge().  Parses newline-joined strings and
        %   JSON metadata with minimal MATLAB-Python boundary crossings.
        %
        %   Optimizations:
        %   - py_vars are converted to a cell array in one call (not per-item)
        %   - Scalar data is transferred as a single numpy array when possible
        %   - str2double for version/parameter IDs is vectorized outside the loop
        %   - Results array is preallocated
        %
        %   results = scidb.BaseVariable.wrap_py_vars_batch(bulk)
        %
        %   Returns a BaseVariable array.
            n = int64(bulk{'n'});

            if n == 0
                results = scidb.BaseVariable.empty(0, 0);
                return;
            end

            % Extract scalar fields from newline-joined strings (1 crossing each)
            record_ids     = splitlines(string(bulk{'record_ids'}));
            content_hashes = splitlines(string(bulk{'content_hashes'}));
            branch_params_json = splitlines(string(bulk{'json_branch_params'}));

            % Parse all metadata at once via JSON (native C decoder, no crossings)
            json_str = char(bulk{'json_meta'});
            meta_arr = jsondecode(json_str);
            % jsondecode returns a cell array when objects have heterogeneous
            % fields; normalize to cell array for uniform handling.
            if iscell(meta_arr)
                meta_cell = meta_arr;
            else
                % struct array — wrap each element in a cell
                meta_cell = cell(n, 1);
                for mi = 1:n
                    meta_cell{mi} = meta_arr(mi);
                end
            end
            % Convert char fields to string for consistency with pydict_to_struct
            for mi = 1:n
                s = meta_cell{mi};
                flds = fieldnames(s);
                for fi = 1:numel(flds)
                    val = s.(flds{fi});
                    if ischar(val)
                        s.(flds{fi}) = string(val);
                    end
                end
                meta_cell{mi} = s;
            end

            % --- Optimization B: Scalar data batch transfer ---
            % When all data values are scalars, Python packs them as a
            % single numpy array — one double() call instead of N from_python.
            has_scalar_data = logical( ...
                py.operator.contains(bulk, 'scalar_data'));
            if has_scalar_data
                % Use from_python to properly convert scalar array
                all_scalar_data = scidb.internal.from_python(bulk{'scalar_data'});
                scidb.Log.debug('wrap_py_vars_batch: loaded %d scalar values', numel(all_scalar_data));
            end

            % --- Optimization B2: DataFrame batch transfer ---
            % When all data values are same-schema DataFrames, Python
            % concatenates them into one big DataFrame.  We convert it to
            % a MATLAB table once, then split by row counts.
            has_concat_df = logical( ...
                py.operator.contains(bulk, 'concat_df'));
            if has_concat_df
                scidb.Log.debug('wrap_py_vars_batch: using concat_df fast path...');
                concat_table = scidb.internal.from_python(bulk{'concat_df'});
                % Use from_python to properly convert row counts
                row_counts = scidb.internal.from_python(bulk{'concat_df_row_counts'});
                % Build cumulative row offsets for slicing
                cum_rows = cumsum([0; row_counts(:)]);
                scidb.Log.debug('wrap_py_vars_batch: concat_df has %d rows, split into %d tables', ...
                    height(concat_table), numel(row_counts));

                % Reconstruct flattened array columns if present
                has_flattened = logical(py.operator.contains(bulk, 'flattened_arrays'));
                if has_flattened
                    scidb.Log.debug('wrap_py_vars_batch: reconstructing flattened array columns...');
                    flat_dict = bulk{'flattened_arrays'};
                    flat_keys = cell(py.list(flat_dict.keys()));

                    for k = 1:numel(flat_keys)
                        col_name = string(flat_keys{k});
                        flat_info = flat_dict{flat_keys{k}};

                        % Convert flattened data and sizes
                        flat_data = scidb.internal.from_python(flat_info{'flat'});
                        sizes = scidb.internal.from_python(flat_info{'sizes'});

                        scidb.Log.debug('wrap_py_vars_batch: reconstructing column "%s": %d elements, %d arrays', ...
                            col_name, numel(flat_data), numel(sizes));

                        % Reconstruct variable-length arrays using cumulative offsets
                        offsets = [0; cumsum(sizes(:))];
                        reconstructed = cell(numel(sizes), 1);
                        for r = 1:numel(sizes)
                            start_idx = offsets(r) + 1;
                            end_idx = offsets(r+1);
                            reconstructed{r} = flat_data(start_idx:end_idx);
                        end

                        % Replace placeholder column with reconstructed data.
                        % For multi-row columns, stack same-size column vectors
                        % into a matrix (restores NxM matrix columns saved as
                        % per-row DOUBLE[] arrays).  Single-element stays as
                        % cell to preserve the per-row payload convention.
                        if numel(reconstructed) > 1
                            reconstructed = scidb.internal.try_stack_numeric(reconstructed);
                        end
                        concat_table.(col_name) = reconstructed;
                        scidb.Log.debug('wrap_py_vars_batch: column "%s" reconstructed successfully', col_name);
                    end

                    scidb.Log.info('wrap_py_vars_batch: reconstructed %d flattened array columns', numel(flat_keys));
                end

                % Parse JSON string columns back to structs
                % (JSON columns were serialized to strings in Python for fast transfer)
                has_json_cols = logical(py.operator.contains(bulk, 'json_columns'));
                if has_json_cols
                    json_col_list = bulk{'json_columns'};
                    % Convert Python list to MATLAB cell array
                    if isa(json_col_list, 'py.list')
                        json_cols = cell(json_col_list);
                    else
                        json_cols = cell(py.list(json_col_list));
                    end
                    scidb.Log.debug('wrap_py_vars_batch: parsing %d JSON columns to structs...', numel(json_cols));

                    for k = 1:numel(json_cols)
                        col_name = string(json_cols{k});
                        if ismember(col_name, concat_table.Properties.VariableNames)
                            % Convert string/cell array of JSON to cell array of structs
                            json_strings = concat_table.(col_name);
                            if isstring(json_strings)
                                json_strings = cellstr(json_strings);
                            end

                            structs = cell(size(json_strings));
                            for r = 1:numel(json_strings)
                                try
                                    structs{r} = jsondecode(json_strings{r});
                                catch
                                    structs{r} = struct();  % Empty struct on parse failure
                                end
                            end

                            concat_table.(col_name) = structs;
                            scidb.Log.debug('wrap_py_vars_batch: parsed JSON column "%s" to structs', col_name);
                        end
                    end

                    scidb.Log.info('wrap_py_vars_batch: parsed %d JSON columns to structs', numel(json_cols));
                end
            end

            % --- Optimization C: Bulk-convert py_vars to cell array ---
            % One cell() call instead of N get_batch_item Python calls.
            py_vars_cell = cell(bulk{'py_vars'});

            % Batch ID for non-scalar data fallback
            batch_id = int64(bulk{'batch_id'});

            % --- Optimization D: Preallocate results array ---
            results(1:n) = scidb.BaseVariable();

            for i = 1:n
                % Data: use scalar fast path, concat-df fast path,
                % or per-item fallback
                if has_scalar_data
                    matlab_data = all_scalar_data(i);
                elseif has_concat_df
                    r0 = cum_rows(i) + 1;
                    r1 = cum_rows(i + 1);
                    matlab_data = concat_table(r0:r1, :);
                else
                    py_data = py.scimatlab.bridge.get_batch_data_item( ...
                        batch_id, int64(i-1));
                    matlab_data = scidb.internal.from_python(py_data);
                    matlab_data = scidb.internal.try_stack_numeric(matlab_data);
                end

                v = scidb.BaseVariable();
                v.data = matlab_data;
                v.py_obj = py_vars_cell{i};
                v.record_id    = record_ids(i);
                v.content_hash = content_hashes(i);

                v.metadata = meta_cell{i};

                try
                    bp = jsondecode(branch_params_json{i});
                    if isstruct(bp)
                        v.branch_params = bp;
                    end
                catch
                    % leave branch_params as default empty struct on parse failure
                end

                results(i) = v;
            end

            % Release Python-side cache
            py.scimatlab.bridge.free_batch(batch_id);
        end
    end
end


% =========================================================================
% Local helper functions
% =========================================================================

function [metadata_args, version, as_table, db, where, categorical_flag, introspect] = split_load_args(varargin)
%SPLIT_LOAD_ARGS  Separate 'version', 'as_table', 'db', 'where', 'categorical',
%   and 'introspect' from metadata args.
    version = "latest";
    as_table = false;
    db = [];
    where = [];
    categorical_flag = false;
    introspect = false;
    metadata_args = {};

    i = 1;
    while i <= numel(varargin)
        key = varargin{i};
        if isstring(key), key = char(key); end

        if strcmpi(key, 'version') && i < numel(varargin)
            version = string(varargin{i+1});
            i = i + 2;
        elseif strcmpi(key, 'as_table') && i < numel(varargin)
            as_table = logical(varargin{i+1});
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
        elseif strcmpi(key, 'introspect') && i < numel(varargin)
            introspect = logical(varargin{i+1});
            i = i + 2;
        else
            metadata_args{end+1} = varargin{i};   %#ok<AGROW>
            metadata_args{end+1} = varargin{i+1};  %#ok<AGROW>
            i = i + 2;
        end
    end
end


function [remaining, db] = extract_db(args)
%EXTRACT_DB  Extract 'db' option from name-value pairs.
%   Returns remaining name-value pairs and the db value ([] if not found).
    db = [];
    remaining = {};

    i = 1;
    while i <= numel(args)
        key = args{i};
        if (ischar(key) || isstring(key)) && strcmpi(string(key), 'db') && i < numel(args)
            db = args{i+1};
            i = i + 2;
        else
            remaining{end+1} = args{i};   %#ok<AGROW>
            if i < numel(args)
                remaining{end+1} = args{i+1}; %#ok<AGROW>
                i = i + 2;
            else
                i = i + 1;
            end
        end
    end
end


function tbl = append_introspect_columns(tbl, results, where, version)
%APPEND_INTROSPECT_COLUMNS  Add record_id, branch_params, content_hash,
%   where, and version_mode columns to the right of an existing table.
    n = numel(results);
    tbl.record_id    = string({results.record_id}');
    tbl.content_hash = string({results.content_hash}');

    bp_col = cell(n, 1);
    for i = 1:n
        bp_col{i} = results(i).branch_params;
    end
    tbl.branch_params = bp_col;

    if isempty(where)
        tbl.where = repmat("", n, 1);
    else
        tbl.where = repmat(string(char(py.builtins.repr(where.py_filter))), n, 1);
    end
    tbl.version_mode = repmat(version, n, 1);
end


function tbl = multi_result_to_table(results, type_name, categorical_flag)
%MULTI_RESULT_TO_TABLE  Convert an array of BaseVariable to a MATLAB table.
    n = numel(results);

    % Build metadata columns
    meta_fields = fieldnames(results(1).metadata);
    tbl = table();
    for f = 1:numel(meta_fields)
        col_data = cell(n, 1);
        for i = 1:n
            if isfield(results(i).metadata, meta_fields{f})
                col_data{i} = results(i).metadata.(meta_fields{f});
            else
                col_data{i} = missing;
            end
        end
        % Convert to native type: scalar numeric → double array, string → string array
        all_scalar_numeric = true;
        all_string = true;
        for i = 1:n
            v = col_data{i};
            if ~(isnumeric(v) && isscalar(v) && ~ismissing(v))
                all_scalar_numeric = false;
            end
            if ~(isstring(v) || ischar(v))
                all_string = false;
            end
        end
        if all_scalar_numeric
            tbl.(meta_fields{f}) = cell2mat(col_data);
        elseif all_string
            tbl.(meta_fields{f}) = string(col_data);
        else
            tbl.(meta_fields{f}) = col_data;
        end
    end

    % Convert metadata columns to categorical if requested
    if categorical_flag
        for f = 1:numel(meta_fields)
            col = tbl.(meta_fields{f});
            str_col = string(col);
            unique_vals = unique(str_col, 'stable');
            tbl.(meta_fields{f}) = categorical(str_col, unique_vals);
        end
    end

    % Data column (named after the variable type)
    % Strip package prefix (e.g. "mypackage.StepLength" → "StepLength")
    parts = strsplit(type_name, '.');
    col_name = parts{end};
    data_col = cell(n, 1);
    for i = 1:n
        data_col{i} = results(i).data;
    end
    tbl.(col_name) = normalize_data_column(data_col);
end


function col = normalize_data_column(col_data)
%NORMALIZE_DATA_COLUMN  Convert a cell column to its native type.
%   All scalar numeric cells → numeric array, all scalar string/char cells →
%   string array, otherwise leave as cell array.
    n = numel(col_data);
    all_scalar_numeric = true;
    all_string = true;
    for i = 1:n
        v = col_data{i};
        if ~(isnumeric(v) && isscalar(v) && ~ismissing(v))
            all_scalar_numeric = false;
        end
        if ~(isstring(v) || ischar(v))
            all_string = false;
        end
    end
    if all_scalar_numeric
        col = cell2mat(col_data);
    elseif all_string
        col = string(col_data);
    else
        col = col_data;
    end
end
