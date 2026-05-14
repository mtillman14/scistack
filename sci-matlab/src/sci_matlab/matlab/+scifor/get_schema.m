function keys = get_schema()
%SCIFOR.GET_SCHEMA  Return the current global schema key list.
%
%   KEYS = scifor.get_schema()
%
%   Returns the schema key list set by scifor.set_schema() or
%   scidb.configure_database().  Returns string.empty(1, 0) if not set.
%
%   Storage model:
%     - Python's scifor module global is the source of truth when
%       py.scifor is importable; the result is converted to a MATLAB
%       string row vector.
%     - When Python isn't reachable, falls back to the MATLAB-side cache
%       written by scifor.set_schema (kept via setappdata(0, ...)).

    % Prefer Python as the source of truth
    try
        py_keys = py.scifor.get_schema();
        keys_cell = cell(py_keys);
        if isempty(keys_cell)
            keys = string.empty(1, 0);
        else
            keys = string(keys_cell);
            keys = keys(:)';
        end
        return
    catch
        % Python not importable — fall back to the MATLAB-side cache
    end

    cached = getappdata(0, 'scifor_schema');
    if isempty(cached)
        keys = string.empty(1, 0);
    else
        keys = cached(:)';
    end

end
