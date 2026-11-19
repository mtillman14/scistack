classdef Merge
%SCIFOR.MERGE  Combine multiple tables into a single table input for for_each.
%
%   Wraps 2+ MATLAB tables. For each combo, scifor.for_each filters
%   each table individually, then merges them column-wise (inner join
%   on common schema key columns, or simple horzcat for single-row results).
%
%   Constituents can be:
%   - MATLAB tables
%   - scifor.Fixed(table, ...) wrappers
%   - scifor.ColumnSelection(table, cols) wrappers
%
%   Properties:
%       tables - Cell array of table specs (tables, Fixed, or ColumnSelection)
%
%   Example:
%       scifor.for_each(@analyze, ...
%           struct('data', scifor.Merge(gait_table, force_table, name="gait_force")), ...
%           subject=[1 2 3])
%
%       % With Fixed override
%       scifor.for_each(@analyze, ...
%           struct('data', scifor.Merge( ...
%               gait_table, ...
%               scifor.Fixed(baseline_table, session="BL"), name="gait_bl")), ...
%           subject=[1 2 3], session=["A" "B"])

    properties (SetAccess = private)
        tables  cell  % Cell array of table specs
        name    string  % Identity of the Merge (node name), required
        how     string = "exact"  % Join method, one of scifor.merge_hows()
    end

    methods
        function obj = Merge(varargin)
        %MERGE  Construct a Merge wrapper.
        %
        %   M = scifor.Merge(table1, table2, ..., name="m")
        %   M = scifor.Merge(table1, table2, ..., name="m", how="left")
        %
        %   Arguments:
        %       2+ table specs: MATLAB tables, Fixed wrappers,
        %       or ColumnSelection wrappers.

            [obj.tables, obj.name, obj.how] = scifor.merge_args(varargin, 'scifor:Merge');
        end

        function disp(obj)
        %DISP  Display the Merge wrapper.
            parts = cell(1, numel(obj.tables));
            for i = 1:numel(obj.tables)
                spec = obj.tables{i};
                if isa(spec, 'scifor.Fixed')
                    fields = fieldnames(spec.fixed_metadata);
                    if isempty(fields)
                        parts{i} = 'Fixed(<table>)';
                    else
                        fp = cell(1, numel(fields));
                        for f = 1:numel(fields)
                            val = spec.fixed_metadata.(fields{f});
                            if isnumeric(val)
                                fp{f} = sprintf('%s=%g', fields{f}, val);
                            else
                                fp{f} = sprintf('%s="%s"', fields{f}, string(val));
                            end
                        end
                        parts{i} = sprintf('Fixed(<table>, %s)', strjoin(fp, ', '));
                    end
                elseif istable(spec)
                    parts{i} = sprintf('<table %dx%d>', height(spec), width(spec));
                else
                    parts{i} = class(spec);
                end
            end
            fprintf('  scifor.Merge(%s, name="%s", how="%s")\n', strjoin(parts, ', '), obj.name, obj.how);
        end
    end
end
