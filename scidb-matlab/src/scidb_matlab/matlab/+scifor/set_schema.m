function set_schema(keys)
%SCIFOR.SET_SCHEMA  Set the global schema key list.
%
%   scifor.set_schema(KEYS)  stores KEYS as the schema key list used by
%   scifor.for_each() / scidb.for_each() to detect and filter table inputs.
%
%   Arguments:
%       keys - String array of schema key names,
%              e.g. ["subject", "session"]
%
%   This function is called automatically by scidb.configure_database().
%   Standalone users who do not use a database should call it once before
%   using scidb.for_each() with table inputs or distribute=true.
%
%   Example:
%       scifor.set_schema(["subject", "session"])

    arguments
        keys string
    end

    scifor.schema_store_(keys);

end
