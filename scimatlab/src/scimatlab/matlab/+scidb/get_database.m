function db = get_database()
%SCIDB.GET_DATABASE  Get the global database connection.
%
%   DB = scidb.get_database() returns the Python DatabaseManager
%   instance.  Use DB.close() to close the connection.
%
%   Example:
%       db = scidb.get_database();
%       db.close();

    db = py.scidb.store.database.get_database();
end
