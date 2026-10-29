classdef ColumnSelection
%SCIFOR.COLUMNSELECTION  Extract specific columns from a table input.
%
%   After filtering the table for the current combo, extracts only the
%   specified columns. Single column -> returns the column values (array).
%   Multiple columns -> returns a sub-table.
%
%   Properties:
%       data    - MATLAB table
%       columns - String array of column names to extract (with groups:
%                 the ordered union of every group's columns)
%       iterate - logical; when true the selection means "run fn once per
%                 column and reassemble the per-column results into one wide
%                 row" rather than "pass these columns as one argument".
%                 (default false)
%       groups  - Column groups (for_columns): a struct array with fields
%                 name (string; "" when unnamed) and columns (string array),
%                 or empty when every column is its own group. See
%                 PARSE_GROUPS.
%
%   Example:
%       scifor.for_each(@fn, ...
%           struct('speed', scifor.ColumnSelection(data_table, "speed")), ...
%           subject=[1 2 3])
%
%       scifor.for_each(@fn, ...
%           struct('data', scifor.ColumnSelection(data_table, ["speed", "force"])), ...
%           subject=[1 2 3])
%
%       % Column-wise iteration (for_columns): fn runs once per column and
%       % the per-column scalars are reassembled into a 1xN row.
%       scifor.for_each(@col_mean, ...
%           struct('value', scifor.ColumnSelection(data_table, ["speed", "force"], true)), ...
%           subject=[1 2 3])
%
%       % Column groups: each group is passed together as a sub-table.
%       % Several outputs -> one output per group, paired in order:
%       scifor.ColumnSelection(tbl, {["6MWT","6MWT_Date"], ["10MWT","10MWT_Date"]}, true)
%       % One output -> a dictionary names each group's column(s):
%       scifor.ColumnSelection(tbl, dictionary(["6MWT","10MWT"], ...
%           {["6MWT","6MWT_Date"], ["10MWT","10MWT_Date"]}), true)

    properties (SetAccess = private)
        data     % MATLAB table
        columns  string  % String array of column names to extract
        iterate  logical % Iterate per-column and reassemble (for_columns)
        groups   struct  % Column groups (name, columns); empty when flat
    end

    methods
        function obj = ColumnSelection(data, columns, iterate)
        %COLUMNSELECTION  Construct a ColumnSelection wrapper.
        %
        %   CS = scifor.ColumnSelection(tbl)                % all data columns
        %   CS = scifor.ColumnSelection(tbl, columns)
        %   CS = scifor.ColumnSelection(tbl, columns, iterate)
        %
        %   Arguments:
        %       data    - A MATLAB table
        %       columns - String or string array of column names. An empty
        %                 value ([] / string.empty, the default) means "all
        %                 data columns", resolved at for_each time. With
        %                 iterate, also column groups: a cell array of
        %                 string arrays, or a dictionary of named groups
        %                 (PARSE_GROUPS).
        %       iterate - (optional) logical; iterate per-column and
        %                 reassemble into one wide row. Default false.

            obj.data = data;
            if nargin < 2
                columns = [];
            end
            [obj.columns, obj.groups] = scifor.ColumnSelection.parse_groups(columns);
            if nargin >= 3 && ~isempty(iterate)
                obj.iterate = logical(iterate);
            else
                obj.iterate = false;
            end
            if ~isempty(obj.groups) && ~obj.iterate
                error('scifor:ColumnSelection:groupsNeedIterate', ...
                    ['Column groups run the function once per group, so they ' ...
                     'need for_columns (iterate=true). To pass several columns ' ...
                     'as one argument, use a string array.']);
            end
        end

        function obj = with_data(obj, data)
        %WITH_DATA  The same selection (columns, groups, iterate) over new data.
        %   Every rebuild over coerced or loaded data uses this, so the groups
        %   survive it.
            obj.data = data;
        end

        function spec = column_spec(obj)
        %COLUMN_SPEC  The columns as they would be passed in: a string array,
        %   a cell array of string arrays (unnamed groups), or a dictionary
        %   (named groups).
            if isempty(obj.groups)
                spec = obj.columns;
            elseif strlength(obj.groups(1).name) == 0
                spec = arrayfun(@(g) g.columns, obj.groups, 'UniformOutput', false);
            else
                spec = dictionary([obj.groups.name], ...
                    arrayfun(@(g) g.columns, obj.groups, 'UniformOutput', false));
            end
        end

        function disp(obj)
        %DISP  Display the ColumnSelection wrapper.
            if obj.iterate
                suffix = ', iterate';
            else
                suffix = '';
            end
            if isempty(obj.groups)
                body = strjoin('"' + obj.columns + '"', ', ');
            else
                parts = strings(1, numel(obj.groups));
                for i = 1:numel(obj.groups)
                    cols = "[" + strjoin('"' + obj.groups(i).columns + '"', ', ') + "]";
                    if strlength(obj.groups(i).name) > 0
                        parts(i) = '"' + obj.groups(i).name + '": ' + cols;
                    else
                        parts(i) = cols;
                    end
                end
                body = strjoin(parts, ', ');
            end
            fprintf('  scifor.ColumnSelection(<table>, [%s]%s)\n', body, suffix);
        end
    end

    methods (Static)
        function [columns, groups] = parse_groups(spec)
        %PARSE_GROUPS  What a column spec means (port of Python scifor's
        %   parse_column_groups; the two must agree).
        %
        %   [] / string.empty  -> all data columns, no groups
        %   "a" / ["a","b"]    -> flat, one column each
        %   {["a","a_d"], "b"} -> unnamed groups (split: paired with outputs)
        %   dictionary(names, {cols...}) -> named groups, key order (combine)
        %
        %   COLUMNS is the ordered union of every group's columns. Groups
        %   that add nothing normalize back to the flat list. containers.Map
        %   is refused: it sorts its keys, and group order is meaning.
            groups = struct('name', {}, 'columns', {});
            if isempty(spec) && ~isa(spec, 'dictionary')
                columns = string.empty;
                return;
            end
            if isa(spec, 'containers.Map')
                error('scifor:ColumnSelection:mapUnordered', ...
                    ['containers.Map sorts its keys, so it loses the group ' ...
                     'order (the output''s column order). Use a dictionary: ' ...
                     'dictionary(["A","B"], {["a","a_date"], ["b","b_date"]}).']);
            end
            if isa(spec, 'dictionary')
                names = string(keys(spec));
                vals = values(spec);
                for i = 1:numel(names)
                    if strlength(names(i)) == 0
                        error('scifor:ColumnSelection:badGroup', ...
                            'for_columns group names must be non-empty strings.');
                    end
                    v = vals(i);
                    if iscell(v)
                        v = v{1};
                    end
                    groups(end+1) = struct('name', names(i), ...
                        'columns', group_columns(v, names(i))); %#ok<AGROW>
                end
                flat = true;
                for i = 1:numel(groups)
                    flat = flat && isequal(groups(i).columns, groups(i).name);
                end
                if flat
                    columns = [groups.name];
                    groups = struct('name', {}, 'columns', {});
                else
                    columns = union_columns(groups);
                end
                return;
            end
            if iscell(spec)
                has_multi = false;
                for i = 1:numel(spec)
                    groups(end+1) = struct('name', "", ...
                        'columns', group_columns(spec{i}, "#" + i)); %#ok<AGROW>
                    has_multi = has_multi || numel(groups(end).columns) > 1;
                end
                if ~has_multi
                    columns = [groups.columns];
                    groups = struct('name', {}, 'columns', {});
                else
                    columns = union_columns(groups);
                end
                return;
            end
            columns = string(spec);
            columns = columns(:)';
        end
    end
end


function cols = group_columns(v, label)
%GROUP_COLUMNS  One group's columns, validated: non-empty, no repeats.
    if ~(ischar(v) || isstring(v) || iscellstr(v)) || isempty(v)
        error('scifor:ColumnSelection:badGroup', ...
            'for_columns group %s must list at least one column name.', label);
    end
    cols = string(v);
    cols = cols(:)';
    if any(strlength(cols) == 0)
        error('scifor:ColumnSelection:badGroup', ...
            'for_columns group %s: column names must be non-empty strings.', label);
    end
    if numel(unique(cols)) < numel(cols)
        error('scifor:ColumnSelection:badGroup', ...
            'for_columns group %s lists a column more than once.', label);
    end
end


function cols = union_columns(groups)
%UNION_COLUMNS  Every group's columns, first-seen order, once each.
    all_cols = [groups.columns];
    [~, first] = unique(all_cols, 'stable');
    cols = all_cols(sort(first));
end
