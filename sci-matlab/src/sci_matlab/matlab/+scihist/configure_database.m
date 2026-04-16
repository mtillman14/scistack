function db = configure_database(db_path, schema_keys, varargin)
%SCIHIST.CONFIGURE_DATABASE  Configure database with lineage backend.
%
%   DB = scihist.configure_database(DB_PATH, SCHEMA_KEYS, ...)
%
%   This is the scihist wrapper around scidb.configure_database(). It
%   opens the DuckDB-backed database AND registers the scilineage backend,
%   so that LineageFcn-based computations can look up previously computed
%   results (enabling cache hits).
%
%   Use this function (instead of scidb.configure_database) wherever
%   LineageFcn caching or provenance tracking is required.
%
%   Arguments:
%       db_path     - Path to the DuckDB database file (string or char)
%       schema_keys - String array of metadata keys that form the dataset
%                     schema (e.g. ["subject", "session"])
%       varargin    - Additional name-value pairs forwarded to
%                     scidb.configure_database
%
%   Returns:
%       DB - The configured DatabaseManager Python object.
%
%   Example:
%       db = scihist.configure_database("experiment.duckdb", ...
%           ["subject", "session"]);

    % Delegate to scidb.configure_database for all MATLAB-side setup:
    % string-array conversion, absolute-path resolution, scifor schema
    % propagation, and log-file path. This mirrors the Python
    % scihist.configure_database, which simply calls
    % scidb.configure_database() and then registers the lineage backend.
    db = scidb.configure_database(db_path, schema_keys, varargin{:});

    % Register the database as the scilineage cache backend so that
    % LineageFcn invocations can look up previously computed results.
    py.scilineage.configure_backend(db);
end
