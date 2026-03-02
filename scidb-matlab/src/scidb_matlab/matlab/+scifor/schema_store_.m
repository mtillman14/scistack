function keys = schema_store_(new_keys)
%SCIFOR.SCHEMA_STORE_  Internal persistent store for schema keys.
%
%   This function is the single source of truth for the schema key list.
%   set_schema() and get_schema() both delegate to it.
%
%   With no argument: returns the stored schema keys.
%   With one argument: updates the stored schema keys and returns them.

    persistent stored_keys;

    if nargin > 0
        stored_keys = new_keys(:)';  % ensure row vector
    end

    if isempty(stored_keys)
        keys = string.empty(1, 0);
    else
        keys = stored_keys;
    end

end
