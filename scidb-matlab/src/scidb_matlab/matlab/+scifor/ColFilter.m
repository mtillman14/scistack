classdef ColFilter
%SCIFOR.COLFILTER  A filter comparing a table column to a value.
%
%   Created via scifor.Col("column") == value, etc.
%   Use apply(tbl) to get a logical row mask.

    properties
        column_name string
        op          string
        value
    end

    methods
        function obj = ColFilter(column_name, op, value)
            obj.column_name = column_name;
            obj.op          = op;
            obj.value       = value;
        end

        function mask = apply(obj, tbl)
            col = tbl.(obj.column_name);
            switch obj.op
                case "=="
                    mask = col == obj.value;
                case "!="
                    mask = col ~= obj.value;
                case "<"
                    mask = col < obj.value;
                case "<="
                    mask = col <= obj.value;
                case ">"
                    mask = col > obj.value;
                case ">="
                    mask = col >= obj.value;
                otherwise
                    error('scifor:ColFilter:unknownOp', ...
                          'Unknown operator: %s', obj.op);
            end
        end

        function f = and(obj, other)
            f = scifor.CompoundFilter("&", obj, other);
        end

        function f = or(obj, other)
            f = scifor.CompoundFilter("|", obj, other);
        end

        function f = not(obj)
            f = scifor.NotFilter(obj);
        end

        function key = to_key(obj)
            key = sprintf("Col('%s') %s %s", obj.column_name, obj.op, mat2str(obj.value));
        end
    end

end
