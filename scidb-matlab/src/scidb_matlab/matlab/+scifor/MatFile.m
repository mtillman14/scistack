classdef MatFile
%SCIFOR.MATFILE  Load and save MATLAB .mat files.
%
%   Can be used as an input or output to scifor.for_each / scidb.for_each.
%   Implements the .load(**metadata) / .save(data, **metadata) protocol.
%
%   Arguments (constructor):
%       path_template - Format string with {key} placeholders,
%                       e.g. "data/{subject}/{session}.mat"
%
%   Example:
%       f = scifor.MatFile("data/{subject}/{session}.mat");
%       data = f.load("subject", 1, "session", "pre");
%       f.save(my_data, "subject", 1, "session", "pre");

    properties
        path_template string
    end

    methods
        function obj = MatFile(path_template)
            arguments
                path_template string
            end
            obj.path_template = path_template;
        end

        function data = load(obj, varargin)
        %LOAD  Load data from a .mat file resolved from metadata.
        %
        %   data = obj.load("key1", val1, "key2", val2, ...)
        %
        %   Extra keyword arguments (e.g. db=) are accepted and ignored.
            meta = scifor.MatFile.parse_meta(varargin{:});
            path = scifor.MatFile.resolve_path(obj.path_template, meta);
            data = load(path);  %#ok<LOAD>
        end

        function save(obj, data, varargin)
        %SAVE  Save data to a .mat file resolved from metadata.
        %
        %   obj.save(data, "key1", val1, "key2", val2, ...)
        %
        %   Extra keyword arguments (e.g. db=, __fn=) are accepted and ignored.
            meta = scifor.MatFile.parse_meta(varargin{:});
            path = scifor.MatFile.resolve_path(obj.path_template, meta);
            % Ensure parent directory exists
            parent = fileparts(path);
            if ~isempty(parent) && ~isfolder(parent)
                mkdir(parent);
            end
            if isstruct(data)
                save(path, '-struct', 'data');
            else
                save(path, 'data');
            end
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
