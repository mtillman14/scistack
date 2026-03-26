function db = configure_database(dataset_db_path, dataset_schema_keys, options)
%SCIDB.CONFIGURE_DATABASE  Set up the SciStack database connection.
%
%   db = scidb.configure_database(DB_PATH, SCHEMA_KEYS)
%   configures the global database connection with DuckDB for data and
%   lineage storage.
%
%   db = scidb.configure_database(..., lineage_mode="ephemeral")
%   allows unsaved intermediate variables in lineage chains.
%
%   Arguments:
%       dataset_db_path     - Path to the DuckDB database file (string)
%       dataset_schema_keys - Metadata keys defining the dataset schema
%                             (string array, e.g. ["subject", "session"])
%
%   Name-Value Arguments:
%       lineage_mode - "strict" (default) or "ephemeral"
%
%   Example:
%       scidb.configure_database( ...
%           "experiment.duckdb", ...
%           ["subject", "session"]);

    arguments
        dataset_db_path     string
        dataset_schema_keys string
        options.lineage_mode string = "strict"
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
        py_schema_keys, ...
        pyargs('lineage_mode', char(options.lineage_mode)));

    % Verify the Python environment is working
    py_db = py.scidb.database.get_database();
    if isempty(py_db)
        error('scidb:ConfigFailed', ...
            'configure_database() did not produce a valid DatabaseManager.');
    end

    % Propagate schema keys to scifor so that table-input detection and
    % distribute=true work identically in DB-backed and standalone modes.
    scifor.set_schema(dataset_schema_keys);

end
