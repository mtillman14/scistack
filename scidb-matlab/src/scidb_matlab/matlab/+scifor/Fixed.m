classdef Fixed
%SCIFOR.FIXED  Specify fixed metadata overrides for a table input.
%
%   Wraps a MATLAB table with metadata overrides. When scifor.for_each
%   filters this input for a combo, it uses the fixed metadata values
%   instead of the iteration values for the specified keys.
%
%   Properties:
%       data            - MATLAB table (the actual data)
%       fixed_metadata  - Struct of metadata values to override
%
%   Example:
%       scifor.for_each(@compare, ...
%           struct('baseline', scifor.Fixed(data_table, session="BL"), ...
%                  'current',  data_table), ...
%           subject=[1 2 3], session=["A" "B"])

    properties (SetAccess = private)
        data            % MATLAB table (the actual data)
        fixed_metadata  struct  % Metadata overrides
    end

    methods
        function obj = Fixed(data, varargin)
        %FIXED  Construct a Fixed metadata wrapper.
        %
        %   F = scifor.Fixed(tbl, Name, Value, ...)
        %
        %   Arguments:
        %       data - A MATLAB table
        %
        %   Name-Value Arguments:
        %       Metadata keys and their fixed values

            obj.data = data;

            if mod(numel(varargin), 2) ~= 0
                error('scifor:Fixed', ...
                    'Fixed metadata must be name-value pairs.');
            end

            s = struct();
            for i = 1:2:numel(varargin)
                s.(string(varargin{i})) = varargin{i+1};
            end
            obj.fixed_metadata = s;
        end

        function disp(obj)
        %DISP  Display the Fixed wrapper.
            fields = fieldnames(obj.fixed_metadata);
            if isempty(fields)
                fprintf('  scifor.Fixed(<table>)\n');
            else
                parts = cell(1, numel(fields));
                for i = 1:numel(fields)
                    val = obj.fixed_metadata.(fields{i});
                    if isnumeric(val)
                        parts{i} = sprintf('%s=%g', fields{i}, val);
                    else
                        parts{i} = sprintf('%s="%s"', fields{i}, string(val));
                    end
                end
                fprintf('  scifor.Fixed(<table>, %s)\n', strjoin(parts, ', '));
            end
        end
    end
end
