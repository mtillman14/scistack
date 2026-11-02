classdef SchemaKey
%SCIDB.SCHEMAKEY  Builder for schema-key-based filters.
%
%   Create via scidb.schema_key():
%
%       scidb.schema_key("session")
%
%   Supports ismember and comparison operators:
%
%       ismember(scidb.schema_key("session"), ["BL", "POST"])
%       scidb.schema_key("session") == "BL"
%       scidb.schema_key("session") ~= "PRE"
%       scidb.schema_key("subject") > 2
%       scidb.schema_key("subject") >= 2
%       scidb.schema_key("subject") < 5
%       scidb.schema_key("subject") <= 5
%
%   All operators return a scidb.Filter that can be passed to load():
%
%       MyData().load(where=ismember(scidb.schema_key("session"), ["BL","POST"]))
%       MyData().load(where=scidb.schema_key("subject") > 2)
%
%   Filters can be combined with & / | / ~:
%
%       where=(scidb.schema_key("subject") > 1) & (scidb.schema_key("subject") < 4)

    properties
        key  string
    end

    methods
        function obj = SchemaKey(key)
            obj.key = string(key);
        end

        % -----------------------------------------------------------------
        % ismember — set membership filter
        % -----------------------------------------------------------------
        function filt = ismember(obj, values)
        %ISMEMBER  Filter where schema key is in a set of values.
        %
        %   FILT = ismember(scidb.schema_key(KEY), VALUES)
        %
        %   VALUES is a string array or numeric array.
        %
        %   Example:
        %       where=ismember(scidb.schema_key("session"), ["BL", "POST"])
            if isstring(values) || ischar(values)
                vals_cell = cellstr(string(values(:)));
                py_values = py.list(vals_cell');
            elseif isnumeric(values)
                py_values = py.list(num2cell(values(:)'));
            else
                py_values = py.list(values);
            end
            py_filter = py.scidb.model.filters.SchemaKeyInFilter(char(obj.key), py_values);
            filt = scidb.Filter(py_filter);
        end

        % -----------------------------------------------------------------
        % Comparison operators
        % -----------------------------------------------------------------
        function filt = eq(obj, value)
        %EQ  Filter where schema key equals value.
        %
        %   FILT = scidb.schema_key(KEY) == VALUE
            py_filter = py.scimatlab.bridge.schema_key_filter( ...
                char(obj.key), '==', scidb.internal.to_python(value));
            filt = scidb.Filter(py_filter);
        end

        function filt = ne(obj, value)
        %NE  Filter where schema key does not equal value.
        %
        %   FILT = scidb.schema_key(KEY) ~= VALUE
            py_filter = py.scimatlab.bridge.schema_key_filter( ...
                char(obj.key), '!=', scidb.internal.to_python(value));
            filt = scidb.Filter(py_filter);
        end

        function filt = lt(obj, value)
        %LT  Filter where schema key is less than value (numeric keys).
        %
        %   FILT = scidb.schema_key(KEY) < VALUE
            py_filter = py.scimatlab.bridge.schema_key_filter( ...
                char(obj.key), '<', scidb.internal.to_python(value));
            filt = scidb.Filter(py_filter);
        end

        function filt = le(obj, value)
        %LE  Filter where schema key is less than or equal to value.
        %
        %   FILT = scidb.schema_key(KEY) <= VALUE
            py_filter = py.scimatlab.bridge.schema_key_filter( ...
                char(obj.key), '<=', scidb.internal.to_python(value));
            filt = scidb.Filter(py_filter);
        end

        function filt = gt(obj, value)
        %GT  Filter where schema key is greater than value (numeric keys).
        %
        %   FILT = scidb.schema_key(KEY) > VALUE
            py_filter = py.scimatlab.bridge.schema_key_filter( ...
                char(obj.key), '>', scidb.internal.to_python(value));
            filt = scidb.Filter(py_filter);
        end

        function filt = ge(obj, value)
        %GE  Filter where schema key is greater than or equal to value.
        %
        %   FILT = scidb.schema_key(KEY) >= VALUE
            py_filter = py.scimatlab.bridge.schema_key_filter( ...
                char(obj.key), '>=', scidb.internal.to_python(value));
            filt = scidb.Filter(py_filter);
        end
    end
end
