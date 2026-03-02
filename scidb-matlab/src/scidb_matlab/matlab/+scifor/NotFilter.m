classdef NotFilter
%SCIFOR.NOTFILTER  Negates a filter.

    properties
        inner
    end

    methods
        function obj = NotFilter(inner)
            obj.inner = inner;
        end

        function mask = apply(obj, tbl)
            mask = ~obj.inner.apply(tbl);
        end

        function f = and(obj, other)
            f = scifor.CompoundFilter("&", obj, other);
        end

        function f = or(obj, other)
            f = scifor.CompoundFilter("|", obj, other);
        end

        function f = not(obj)
            f = obj.inner;  % double-negation
        end

        function key = to_key(obj)
            key = sprintf("~(%s)", obj.inner.to_key());
        end
    end

end
