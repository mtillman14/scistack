classdef ColumnSelection
%SCIFOR.COLUMNSELECTION  Extract specific columns from a table input.
%
%   After filtering the table for the current combo, extracts only the
%   specified columns. Single column -> returns the column values (array).
%   Multiple columns -> returns a sub-table.
%
%   Properties:
%       data    - MATLAB table
%       columns - String array of column names to extract
%
%   Example:
%       scifor.for_each(@fn, ...
%           struct('speed', scifor.ColumnSelection(data_table, "speed")), ...
%           subject=[1 2 3])
%
%       scifor.for_each(@fn, ...
%           struct('data', scifor.ColumnSelection(data_table, ["speed", "force"])), ...
%           subject=[1 2 3])

    properties (SetAccess = private)
        data     % MATLAB table
        columns  string  % String array of column names to extract
    end

    methods
        function obj = ColumnSelection(data, columns)
        %COLUMNSELECTION  Construct a ColumnSelection wrapper.
        %
        %   CS = scifor.ColumnSelection(tbl, columns)
        %
        %   Arguments:
        %       data    - A MATLAB table
        %       columns - String or string array of column names

            obj.data = data;
            obj.columns = string(columns);
        end

        function disp(obj)
        %DISP  Display the ColumnSelection wrapper.
            fprintf('  scifor.ColumnSelection(<table>, [%s])\n', ...
                strjoin('"' + obj.columns + '"', ', '));
        end
    end
end
