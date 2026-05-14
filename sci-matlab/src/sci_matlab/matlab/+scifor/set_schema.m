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
%   Storage model:
%     - Python's scifor module global is the source of truth when the
%       py.scifor module is importable.
%     - A MATLAB-side fallback cache (kept via setappdata(0, ...)) holds
%       the same value so that standalone callers without Python still
%       work, and so scifor.get_schema() returns a consistent answer if
%       Python becomes unreachable mid-session.
%     - When Python is reachable, BOTH are written so they stay in sync.
%
%   Example:
%       scifor.set_schema(["subject", "session"])

    arguments
        keys string
    end

    % Normalize to a row vector of strings
    keys_row = keys(:)';
    if isempty(keys_row)
        keys_row = string.empty(1, 0);
    end

    % Always write the MATLAB-side fallback cache
    setappdata(0, 'scifor_schema', keys_row);

    % Forward to Python's scifor.set_schema; tolerate Python being unavailable
    try
        if isempty(keys_row)
            py_keys = py.list();
        else
            py_keys = py.list(cellstr(keys_row));
        end
        py.scifor.set_schema(py_keys);
    catch
        % Python not importable (standalone scifor use) — MATLAB cache is
        % sufficient. Nothing else to do.
    end

end
