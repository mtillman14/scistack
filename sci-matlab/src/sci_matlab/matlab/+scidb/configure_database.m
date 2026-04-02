function db = configure_database(dataset_db_path, dataset_schema_keys)
%SCIDB.CONFIGURE_DATABASE  Set up the SciStack database connection.
%
%   db = scidb.configure_database(DB_PATH, SCHEMA_KEYS)
%   configures the global database connection with DuckDB for data and
%   lineage storage.
%
%   Arguments:
%       dataset_db_path     - Path to the DuckDB database file (string)
%       dataset_schema_keys - Metadata keys defining the dataset schema
%                             (string array, e.g. ["subject", "session"])
%
%   Example:
%       scidb.configure_database( ...
%           "experiment.duckdb", ...
%           ["subject", "session"]);

    arguments
        dataset_db_path     string
        dataset_schema_keys string
    end

    % Convert keys to row vector
    if size(dataset_schema_keys,1) > 1
        dataset_schema_keys = dataset_schema_keys';
    end

    % Convert MATLAB string array to Python list of strings
    py_schema_keys = py.list(cellstr(dataset_schema_keys));

    dataset_db_path = char(dataset_db_path);
    if ~scidb.isabsolute(dataset_db_path)
        dataset_db_path = fullfile(pwd, dataset_db_path);
    end

    % Call Python's configure_database
    db = py.scidb.configure_database( ...
        char(dataset_db_path), ...
        py_schema_keys);

    % Verify the Python environment is working
    py_db = py.scidb.database.get_database();
    if isempty(py_db)
        error('scidb:ConfigFailed', ...
            'configure_database() did not produce a valid DatabaseManager.');
    end

    % Propagate schema keys to scifor so that table-input detection and
    % distribute=true work identically in DB-backed and standalone modes.
    scifor.set_schema(dataset_schema_keys);

    % Set log file path next to the database file
    [db_dir, ~, ~] = fileparts(dataset_db_path);
    scidb.Log.set_path(fullfile(db_dir, 'scidb.log'));

end
