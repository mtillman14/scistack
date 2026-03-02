function keys = get_schema()
%SCIFOR.GET_SCHEMA  Return the current global schema key list.
%
%   KEYS = scifor.get_schema()
%
%   Returns the schema key list set by scifor.set_schema() or
%   scidb.configure_database().  Returns string.empty if not yet set.

    keys = scifor.schema_store_();

end
