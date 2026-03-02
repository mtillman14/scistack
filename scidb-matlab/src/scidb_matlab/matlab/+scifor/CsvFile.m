classdef CsvFile
%SCIFOR.CSVFILE  Load and save CSV files as MATLAB tables.
%
%   Can be used as an input or output to scifor.for_each / scidb.for_each.
%   Implements the .load(**metadata) / .save(data, **metadata) protocol.
%
%   Arguments (constructor):
%       path_template - Format string with {key} placeholders,
%                       e.g. "data/{subject}/{session}.csv"
%
%   Example:
%       f = scifor.CsvFile("data/{subject}/{session}.csv");
%       tbl = f.load("subject", 1, "session", "pre");
%       f.save(my_table, "subject", 1, "session", "pre");

    properties
        path_template string
    end

    methods
        function obj = CsvFile(path_template)
            arguments
                path_template string
            end
            obj.path_template = path_template;
        end

        function data = load(obj, varargin)
        %LOAD  Load a CSV file as a MATLAB table.
        %
        %   data = obj.load("key1", val1, "key2", val2, ...)
        %
        %   Extra keyword arguments (e.g. db=) are accepted and ignored.
            meta = scifor.CsvFile.parse_meta(varargin{:});
            path = scifor.CsvFile.resolve_path(obj.path_template, meta);
            data = readtable(path, 'TextType', 'string');
        end

        function save(obj, data, varargin)
        %SAVE  Save a MATLAB table (or array) to a CSV file.
        %
        %   obj.save(data, "key1", val1, "key2", val2, ...)
        %
        %   Extra keyword arguments (e.g. db=, __fn=) are accepted and ignored.
            meta = scifor.CsvFile.parse_meta(varargin{:});
            path = scifor.CsvFile.resolve_path(obj.path_template, meta);
            % Ensure parent directory exists
            parent = fileparts(path);
            if ~isempty(parent) && ~isfolder(parent)
                mkdir(parent);
            end
            if ~istable(data)
                data = array2table(data);
            end
            writetable(data, path);
        end
    end

    methods (Static, Access = private)
        function meta = parse_meta(varargin)
        %PARSE_META  Convert name-value pairs to a struct, ignoring __ keys.
            meta = struct();
            i = 1;
            while i <= numel(varargin) - 1
                key = varargin{i};
                val = varargin{i+1};
                if ischar(key) || isstring(key)
                    key = char(key);
                    if ~startsWith(key, '__')
                        meta.(key) = val;
                    end
                end
                i = i + 2;
            end
        end

        function path = resolve_path(template, meta)
        %RESOLVE_PATH  Substitute {key} placeholders in template.
            path = char(template);
            fields = fieldnames(meta);
            for k = 1:numel(fields)
                key = fields{k};
                val = meta.(key);
                if isnumeric(val)
                    val_str = num2str(val);
                else
                    val_str = char(val);
                end
                path = strrep(path, ['{' key '}'], val_str);
            end
        end
    end

end
