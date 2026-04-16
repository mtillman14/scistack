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
%   Properties (populated after load):
%       data         - The loaded data (MATLAB native type)
%       record_id    - Unique record identifier (string)
%       metadata     - Struct of metadata key-value pairs
%       content_hash - Content hash of the data (string)
%       lineage_hash - Lineage hash, if computed by a LineageFcn (string)
%       py_obj       - Python BaseVariable shadow (used internally)

    properties
        data                    % MATLAB data
        record_id    string     % Unique record ID
        metadata     struct     % Metadata key-value pairs
        content_hash string     % Content hash (16-char hex)
        lineage_hash string     % Lineage hash (64-char hex), empty if raw
        py_obj                  % Python BaseVariable shadow (internal)
        selected_columns string % Column names to extract on load (empty = all columns)
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
            obj.selected_columns = string.empty;
            if nargin >= 1
                cols = varargin{1};
                obj.selected_columns = string(cols);
            end
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
        %   Drops the data table and removes all entries from _variables
        %   and _record_metadata for this variable type.

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

            duck.con.execute(['DROP TABLE IF EXISTS "' type_name '_data"']);
            duck.con.execute(['DELETE FROM _variables WHERE variable_name = ''' type_name '_data''']);
            duck.con.execute(['DELETE FROM _record_metadata WHERE variable_name = ''' type_name '_data''']);
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
        %   DATA can be a numeric array, scalar, scidb.LineageFcnResult
        %   (lineage is stored automatically), or scidb.BaseVariable (re-save).
        %
        %   Name-Value Arguments:
        %       db - Optional DatabaseManager to use instead of the global
        %            database (returned by scidb.configure_database).
        %       Any other name-value pairs are metadata (e.g. subject=1).
        %
        %   Example:
        %       RawSignal().save(randn(100,3), subject=1, session="A");
        %
        %       result = my_thunk(input_var, 2.5);
        %       Processed().save(result, subject=1, session="A");
        %
        %       % Save to a specific database
        %       RawSignal().save(data, db=db2, subject=1);

            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);

            % Extract db option and build kwargs (needed for all paths)
            [metadata_nv, db_val] = extract_db(varargin);
            py_kwargs = scidb.internal.metadata_to_pykwargs(metadata_nv{:});

            % LineageFcnResult: route to scihist's lineage-aware save
            if isa(data, 'scidb.LineageFcnResult')
                py_data = data.py_obj;
                py_record_id = py.scihist.foreach.save(py_class, py_data, pyargs(py_kwargs{:}));
                record_id = char(py_record_id);
                return;
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
            record_id = char(py_record_id);


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
            scidb.internal.ensure_registered(type_name);

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
            if isdatetime(data_col)
                data_col = string(data_col, 'yyyy-MM-dd''T''HH:mm:ss.SSS');
            end
            if isnumeric(data_col)
                py_data = py.numpy.array(data_col(:)');
            elseif isstring(data_col)
                py_data = py.list(cellfun(@char, num2cell(data_col(:)'), ...
                    'UniformOutput', false));
            elseif iscellstr(data_col) %#ok<ISCLSTR>
                py_data = py.list(data_col(:)');
            else
                % Generic fallback: convert each element individually
                py_data = py.list();
                for i = 1:height(tbl)
                    py_data.append(scidb.internal.to_python(data_col{i}));
                end
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
                    py_meta_cols.append(py.numpy.array(col(:)'));
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
            py_result = py.sci_matlab.bridge.save_batch_bridge( ...
                type_name, py_data, py_meta_keys, py_meta_cols, ...
                py_common, py_db);

            % --- Convert result to MATLAB string array ---
            record_ids = splitlines(string(py_result));

        end

        % -----------------------------------------------------------------
        % load
        % -----------------------------------------------------------------
        function result = load(obj, varargin)
        %LOAD  Load a variable from the database.
        %
        %   RESULT = TypeClass().load(Name, Value, ...)
        %
        %   When multiple records match, returns a MATLAB table by default
        %   (as_table=true). When as_table=false, returns only the raw data
        %   values (numeric array or cell array depending on data types).
        %   When exactly one record matches, returns a single result.
        %
        %   Name-Value Arguments:
        %       Any metadata key-value pairs (e.g. subject=1, session="A")
        %       version     - Specific record_id to load (default "latest")
        %       as_table    - If true, return a MATLAB table when multiple
        %                     results match. If false, return raw data
        %                     values only. (default true)
        %       categorical - If true, convert metadata columns in the
        %                     result table to categorical (default false).
        %                     Only applies when as_table=true.
        %       db          - Optional DatabaseManager to use instead of the
        %                     global database
        %
        %   Example:
        %       % Load as table (default)
        %       tbl = RawSignal().load(subject=1);
        %
        %       % Load as table with categorical metadata
        %       tbl = RawSignal().load(categorical=true, subject=1);
        %
        %       % Load raw data only (no table, no metadata)
        %       data = RawSignal().load(as_table=false, subject=1);
        %
        %       % Load from a specific database
        %       tbl = RawSignal().load(db=db2, subject=1);

            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);

            [metadata_args, version, as_table, db_val, where, categorical_flag] = split_load_args(varargin{:});
            py_metadata = scidb.internal.metadata_to_pydict(metadata_args{:});

            if isempty(db_val)
                py_db = py.scidb.database.get_database();
            else
                py_db = db_val;
            end

            % If loading by specific version, always return single
            if version ~= "latest"
                py_var = py_db.load(py_class, py_metadata, version=char(version));
                wrapped = scidb.BaseVariable.wrap_py_var(py_var);
                result = wrapped;
                return;
            end

            % Query all matching records (latest version per parameter set)
            % load_and_extract keeps generator materialization in Python
            if isempty(where)
                bulk = py.sci_matlab.bridge.load_and_extract( ...
                    py_class, py_metadata, ...
                    pyargs('version_id', 'latest', 'db', py_db));
            else
                bulk = py.sci_matlab.bridge.load_and_extract( ...
                    py_class, py_metadata, ...
                    pyargs('version_id', 'latest', 'db', py_db, ...
                           'where', where.py_filter));
            end
            n = int64(bulk{'n'});

            if n == 0
                error('scidb:NotFoundError', 'No %s found matching the given metadata.', type_name);
            else
                results_arr = scidb.BaseVariable.wrap_py_vars_batch(bulk);

                if n == 1
                    result = results_arr(1);
                elseif as_table
                    result = multi_result_to_table(results_arr, type_name, categorical_flag);
                else
                    % as_table=false: return array of BaseVariable wrappers
                    result = results_arr;
                end
            end


        end

        % -----------------------------------------------------------------
        % load_all
        % -----------------------------------------------------------------
        function results = load_all(obj, varargin)
        %LOAD_ALL  Load all variables matching the given metadata.
        %
        %   RESULTS = TypeClass().load_all(Name, Value, ...)
        %
        %   Returns an array of scidb.BaseVariable objects.
        %
        %   Name-Value Arguments:
        %       version_id - Which versions to return (default "all"):
        %           "all"    : return every version
        %           "latest" : return only the latest version per parameter set
        %           integer  : return only that specific version_id
        %       db         - Optional DatabaseManager to use instead of the
        %                    global database
        %       Any other name-value pairs are metadata filters.
        %       Non-scalar numeric or string arrays are treated as "match any".
        %
        %   Example:
        %       all_signals = RawSignal().load_all(subject=1);
        %       latest_only = RawSignal().load_all(subject=1, version_id="latest");
        %       all_from_db = RawSignal().load_all(db=db2, subject=1);

            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);

            [metadata_args, py_version_id, as_table, db_val, where, categorical_flag] = scidb.internal.split_load_all_args(varargin{:});
            py_metadata = scidb.internal.metadata_to_pydict(metadata_args{:});

            if isempty(db_val)
                py_db = py.scidb.database.get_database();
            else
                py_db = db_val;
            end
            % load_and_extract keeps generator materialization in Python
            if isempty(where)
                bulk = py.sci_matlab.bridge.load_and_extract( ...
                    py_class, py_metadata, ...
                    pyargs('version_id', py_version_id, 'db', py_db));
            else
                bulk = py.sci_matlab.bridge.load_and_extract( ...
                    py_class, py_metadata, ...
                    pyargs('version_id', py_version_id, 'db', py_db, ...
                           'where', where.py_filter));
            end
            results_arr = scidb.BaseVariable.wrap_py_vars_batch(bulk);

            if as_table && numel(results_arr) > 1
                results = multi_result_to_table(results_arr, type_name, categorical_flag);
            else
                results = results_arr;
            end

        end

        % -----------------------------------------------------------------
        % head
        % -----------------------------------------------------------------
        function tbl = head(obj, n, varargin)
        %HEAD  Peek at the first N records (latest version).
        %
        %   TBL = TypeClass().head()          % first record
        %   TBL = TypeClass().head(5)         % first 5 records
        %   TBL = TypeClass().head(3, subject=1)  % first 3 for subject 1
        %
        %   Returns a MATLAB table with schema key columns and a 'data'
        %   column.  Returns an empty table if no records exist.
        %
        %   Arguments:
        %       n  - Number of records to return (default 1)
        %
        %   Name-Value Arguments:
        %       db - Optional DatabaseManager to use instead of the global
        %            database
        %       Any other name-value pairs are metadata filters.

            if nargin < 2
                n = 1;
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
        %   Returns a struct with function_name, function_hash, inputs,
        %   constants.  Returns [] if no lineage recorded.
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
                prov.constants     = scidb.internal.pylist_to_cell(py_result{'constants'});
            end

            
        end

        % -----------------------------------------------------------------
        % Comparison operators (for where= filter syntax)
        % -----------------------------------------------------------------
        function filt = eq(obj, other)
        %EQ  Create a VariableFilter: TypeClass() == value
        %
        %   FILT = TypeClass() == value
        %
        %   Returns a scidb.Filter that can be passed to load/load_all
        %   via the 'where' key.
            if isa(other, 'scidb.BaseVariable')
                filt = builtin('eq', obj, other);
                return;
            end
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);
            py_filter = py.scidb.filters.VariableFilter(py_class, '==', py_val);
            filt = scidb.Filter(py_filter);
        end

        function filt = ne(obj, other)
        %NE  Create a VariableFilter: TypeClass() ~= value
            if isa(other, 'scidb.BaseVariable')
                filt = builtin('ne', obj, other);
                return;
            end
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);
            py_filter = py.scidb.filters.VariableFilter(py_class, '!=', py_val);
            filt = scidb.Filter(py_filter);
        end

        function filt = lt(obj, other)
        %LT  Create a VariableFilter: TypeClass() < value
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);
            py_filter = py.scidb.filters.VariableFilter(py_class, '<', py_val);
            filt = scidb.Filter(py_filter);
        end

        function filt = le(obj, other)
        %LE  Create a VariableFilter: TypeClass() <= value
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);
            py_filter = py.scidb.filters.VariableFilter(py_class, '<=', py_val);
            filt = scidb.Filter(py_filter);
        end

        function filt = gt(obj, other)
        %GT  Create a VariableFilter: TypeClass() > value
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);
            py_filter = py.scidb.filters.VariableFilter(py_class, '>', py_val);
            filt = scidb.Filter(py_filter);
        end

        function filt = ge(obj, other)
        %GE  Create a VariableFilter: TypeClass() >= value
            type_name = class(obj);
            py_class = scidb.internal.ensure_registered(type_name);
            py_val = scidb.internal.to_python(other);
            py_filter = py.scidb.filters.VariableFilter(py_class, '>=', py_val);
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
        %   set to the original Python BaseVariable shadow, so that lineage
        %   tracking works if it's passed to another LineageFcn or re-saved.
        % Usage: v = scidb.BaseVariable.wrap_py_var(py_var)
            matlab_data = scidb.internal.from_python(py_var.data);
            v = scidb.BaseVariable();
            v.data = matlab_data;
            v.py_obj = py_var;
            v.record_id = string(py_var.record_id);
            v.content_hash = string(py_var.content_hash);

            py_lh = py_var.lineage_hash;
            if ~isa(py_lh, 'py.NoneType')
                v.lineage_hash = string(py_lh);
            end

            py_meta = py_var.metadata;
            if ~isa(py_meta, 'py.NoneType')
                v.metadata = scidb.internal.pydict_to_struct(py_meta);
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
            lineage_hashes = splitlines(string(bulk{'lineage_hashes'}));

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
                all_scalar_data = double(py.numpy.ascontiguousarray(bulk{'scalar_data'}));
            end

            % --- Optimization B2: DataFrame batch transfer ---
            % When all data values are same-schema DataFrames, Python
            % concatenates them into one big DataFrame.  We convert it to
            % a MATLAB table once, then split by row counts.
            has_concat_df = logical( ...
                py.operator.contains(bulk, 'concat_df'));
            if has_concat_df
                concat_table = scidb.internal.from_python(bulk{'concat_df'});
                row_counts = double(py.numpy.ascontiguousarray(bulk{'concat_df_row_counts'}));
                % Build cumulative row offsets for slicing
                cum_rows = cumsum([0; row_counts(:)]);
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
                    py_data = py.sci_matlab.bridge.get_batch_data_item( ...
                        batch_id, int64(i-1));
                    matlab_data = scidb.internal.from_python(py_data);
                end

                v = scidb.BaseVariable();
                v.data = matlab_data;
                v.py_obj = py_vars_cell{i};
                v.record_id    = record_ids(i);
                v.content_hash = content_hashes(i);

                if lineage_hashes(i) ~= ""
                    v.lineage_hash = lineage_hashes(i);
                end

                v.metadata = meta_cell{i};

                results(i) = v;
            end

            % Release Python-side cache
            py.sci_matlab.bridge.free_batch(batch_id);
        end
    end
end


% =========================================================================
% Local helper functions
% =========================================================================

function [metadata_args, version, as_table, db, where, categorical_flag] = split_load_args(varargin)
%SPLIT_LOAD_ARGS  Separate 'version', 'as_table', 'db', 'where', and 'categorical' from metadata args.
    version = "latest";
    as_table = true;
    db = [];
    where = [];
    categorical_flag = false;
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
