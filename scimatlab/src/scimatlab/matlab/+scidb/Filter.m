classdef Filter
%SCIDB.FILTER  Thin wrapper around a Python Filter object.
%
%   Created by comparing a BaseVariable instance to a value:
%
%       Side() == "L"         % VariableFilter
%       (Side() == "L") & (Speed() > 1.2)   % CompoundFilter (AND)
%       (Side() == "L") | (Speed() > 1.2)   % CompoundFilter (OR)
%       ~(Side() == "L")                     % NotFilter
%       scidb.raw_sql('"value" > 0.70')      % RawFilter
%
%   Filters are passed to load() via the 'where' key:
%
%       StepLength().load(where=Side() == "L", subject=1)
%       StepLength().load(where=(Side() == "L") & (Speed() > 1.2))

    properties
        py_filter   % Python Filter object (scidb.model.filters.Filter subclass)
    end

    methods
        function obj = Filter(py_filter)
        %FILTER  Construct a Filter wrapping a Python filter object.
        %
        %   OBJ = scidb.Filter(py_filter)
            obj.py_filter = py_filter;
        end

        function result = and(a, b)
        %AND  Combine two filters with logical AND.
        %
            % Overloads A & B
            py_and = py.getattr(a.py_filter, "__and__");
            new_py = py_and(b.py_filter);
            result = scidb.Filter(new_py);
        end

        function result = or(a, b)
        %OR  Combine two filters with logical OR.
        %
            % Overloads A | B
            py_or = py.getattr(a.py_filter, "__or__");
            new_py = py_or(b.py_filter);
            result = scidb.Filter(new_py);
        end

        function result = not(a)
        %NOT  Negate a filter.
        %
            % Overloads ~A
            py_inv = py.getattr(a.py_filter, "__invert__");
            new_py = py_inv();
            result = scidb.Filter(new_py);
        end
    end
end
