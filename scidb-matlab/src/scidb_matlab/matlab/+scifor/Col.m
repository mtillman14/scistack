classdef Col
%SCIFOR.COL  Entry point for building MATLAB table column filters.
%
%   Create column filters using comparison operators:
%
%       scifor.Col("side") == "R"                   % ColFilter
%       (scifor.Col("side") == "R") & (scifor.Col("speed") > 1.2)
%       ~(scifor.Col("side") == "R")
%
%   The resulting filter objects have an apply(tbl) method that returns
%   a logical row index for the table.
%
%   Example:
%       f = scifor.Col("side") == "R";
%       sub_tbl = tbl(f.apply(tbl), :);

    properties
        column_name string
    end

    methods
        function obj = Col(column_name)
            arguments
                column_name string
            end
            obj.column_name = column_name;
        end

        function f = eq(obj, value)
            f = scifor.ColFilter(obj.column_name, "==", value);
        end

        function f = ne(obj, value)
            f = scifor.ColFilter(obj.column_name, "!=", value);
        end

        function f = lt(obj, value)
            f = scifor.ColFilter(obj.column_name, "<", value);
        end

        function f = le(obj, value)
            f = scifor.ColFilter(obj.column_name, "<=", value);
        end

        function f = gt(obj, value)
            f = scifor.ColFilter(obj.column_name, ">", value);
        end

        function f = ge(obj, value)
            f = scifor.ColFilter(obj.column_name, ">=", value);
        end
    end

end
