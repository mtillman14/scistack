function db = configure_database(dataset_db_path, dataset_schema_keys, options)
%SCIDB.CONFIGURE_DATABASE  Set up the SciStack database connection.
%
%   db = scidb.configure_database(DB_PATH, SCHEMA_KEYS)
%   db = scidb.configure_database(DB_PATH, SCHEMA_KEYS, schema_key_types=TYPES)
%   configures the global database connection with DuckDB for data and
%   lineage storage.
%
%   Arguments:
%       dataset_db_path     - Path to the DuckDB database file (string)
%       dataset_schema_keys - Metadata keys defining the dataset schema
%                             (string array, e.g. ["subject", "session"])
%       schema_key_types    - Optional struct declaring per-key types, e.g.
%                             struct('trial', 'numeric').  'numeric' keys
%                             canonicalize every spelling of the same number
%                             ("001", 1, 1.0 all store as "1"; zero-padded
%                             filenames still resolve via PathInput's
%                             numeric fallback).  'string' keys are verbatim
%                             — spelling IS identity, PathInput matches them
%                             exactly only.  Undeclared keys raise
%                             scidb:SchemaKeyTypeError the first time a
%                             PathInput resolution has to bridge spellings.
%                             The DATABASE stores the types: this records
%                             any key that has none, a matching stored type
%                             is a no-op, and a different one is an error.
%                             Omit it to use what is stored (the GUI's
%                             Numeric checkbox writes the same table).
%
%   Example:
%       scidb.configure_database( ...
%           "experiment.duckdb", ...
%           ["subject", "trial"], ...
%           schema_key_types=struct('trial', 'numeric'));

    arguments
        dataset_db_path     string
        dataset_schema_keys string
        options.schema_key_types struct = struct()
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

    % Call Python's configure_database. schema_key_types is recorded in the
    % database, which owns it (scidb.schema_types): omitting it uses what is
    % stored, and a different stored type raises SchemaKeyTypeConflictError.
    db = py.scidb.configure_database( ...
        char(dataset_db_path), ...
        py_schema_keys, ...
        pyargs('schema_key_types', scifor.key_types_to_py(options.schema_key_types)));

    % Verify the Python environment is working
    py_db = py.scidb.database.get_database();
    if isempty(py_db)
        error('scidb:ConfigFailed', ...
            'configure_database() did not produce a valid DatabaseManager.');
    end

    % Propagate schema keys to scifor so that table-input detection and
    % distribute=true work identically in DB-backed and standalone modes.
    scifor.set_schema(dataset_schema_keys);

    % Log file beside the database. scidb.log.log_path_for owns the file
    % name, so MATLAB and Python always write the same file.
    scidb.Log.set_path(char(py.str(py.scidb.log.log_path_for(dataset_db_path))));

    scidb.Log.info('configure_database (MATLAB host): %s', dataset_db_path);

end
