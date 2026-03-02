classdef CompoundFilter
%SCIFOR.COMPOUNDFILTER  Combines two filters with & or |.

    properties
        op    string
        left
        right
    end

    methods
        function obj = CompoundFilter(op, left, right)
            obj.op    = op;
            obj.left  = left;
            obj.right = right;
        end

        function mask = apply(obj, tbl)
            left_mask  = obj.left.apply(tbl);
            right_mask = obj.right.apply(tbl);
            switch obj.op
                case "&"
                    mask = left_mask & right_mask;
                case "|"
                    mask = left_mask | right_mask;
                otherwise
                    error('scifor:CompoundFilter:unknownOp', ...
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
            key = sprintf("(%s %s %s)", obj.left.to_key(), obj.op, obj.right.to_key());
        end
    end

end
