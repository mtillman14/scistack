classdef PathInput < handle
%SCIFOR.PATHINPUT  Resolve a path template using iteration metadata.
%
%   Thin MATLAB handle around the Python ``scifor.pathinput.PathInput``.
%   All template parsing, filesystem walking, and regex matching is owned
%   by Python so MATLAB-driven and Python-driven pipelines stay byte-
%   identical.  This MATLAB class exists only so MATLAB callers can pass
%   a ``PathInput`` instance into ``scidb.for_each`` / ``scifor.for_each``
%   the same way they would in Python.
%
%   PI = scifor.PathInput(TEMPLATE)
%   PI = scifor.PathInput(TEMPLATE, root_folder=FOLDER)
%   PI = scifor.PathInput(TEMPLATE, root_folder=FOLDER, regex=true)
%
%   The template uses {key} placeholders that are replaced by the
%   metadata values supplied by for_each on each iteration.
%
%   When regex=true, the resolved last segment is treated as a regular
%   expression and matched against filenames in the parent directory.
%   Exactly one file must match; zero or multiple matches produce an
%   error.
%
%   Example:
%       scifor.for_each(@process_file, ...
%           struct('filepath', scifor.PathInput("{subject}/trial_{trial}.mat", ...
%                                              root_folder="/data"), ...
%                  'raw', data_table), ...
%           subject=[1 2 3], ...
%           trial=[0 1 2]);

    properties (SetAccess = private)
        path_template  string   % Format string with {key} placeholders
        root_folder    string   % Optional root directory
        regex          logical  % Whether to use regex matching on the last segment
        py_obj                  % Python scifor.pathinput.PathInput instance
    end

    methods
        function obj = PathInput(path_template, options)
        %PATHINPUT  Construct a PathInput.
        %
        %   PI = scifor.PathInput(TEMPLATE)
        %   PI = scifor.PathInput(TEMPLATE, root_folder=FOLDER)
        %   PI = scifor.PathInput(TEMPLATE, regex=true)

            arguments
                path_template  string
                options.root_folder  string = ""
                options.regex        logical = false
            end

            obj.path_template = path_template;
            obj.root_folder = options.root_folder;
            obj.regex = options.regex;

            % Construct the Python instance once and reuse for load/discover.
            if strlength(options.root_folder) > 0
                py_root = char(options.root_folder);
            else
                py_root = py.None;
            end
            obj.py_obj = py.scifor.pathinput.PathInput( ...
                char(path_template), ...
                pyargs('root_folder', py_root, ...
                       'regex', logical(options.regex)));
        end

        function filepath = load(obj, varargin)
        %LOAD  Resolve the template and return the absolute file path.
        %
        %   PATH = pi.load(Name, Value, ...)
        %
        %   Forwards to Python's PathInput.load(**metadata).  The 'db'
        %   key is accepted and ignored for compatibility with for_each's
        %   uniform db= passthrough.

            if mod(numel(varargin), 2) ~= 0
                error('scifor:PathInput', ...
                    'Metadata arguments must be name-value pairs.');
            end

            % Drop db=... and build a flat name-value cell array for pyargs.
            % pyargs takes variadic 'name', value pairs unpacked from a cell;
            % it does NOT accept a py.dict directly.
            kw_cell = {};
            for i = 1:2:numel(varargin)
                key = char(string(varargin{i}));
                if strcmpi(key, 'db')
                    continue;
                end
                val = varargin{i+1};
                if isnumeric(val) && isscalar(val)
                    val_str = num2str(val);
                else
                    val_str = char(string(val));
                end
                kw_cell{end+1} = key; %#ok<AGROW>
                kw_cell{end+1} = val_str; %#ok<AGROW>
            end

            try
                if isempty(kw_cell)
                    py_path = obj.py_obj.load();
                else
                    py_path = obj.py_obj.load(pyargs(kw_cell{:}));
                end
            catch err
                % Translate Python's no-match / multi-match exceptions
                % into the MATLAB error IDs the existing tests expect.
                msg = err.message;
                if contains(msg, 'matched no files')
                    error('scifor:PathInput:NoMatch', '%s', msg);
                elseif contains(msg, 'matched') && contains(msg, 'files')
                    error('scifor:PathInput:MultipleMatches', '%s', msg);
                else
                    rethrow(err);
                end
            end
            filepath = string(char(py.str(py_path)));
        end

        function keys = placeholder_keys(obj)
        %PLACEHOLDER_KEYS  Return cell array of unique placeholder keys in the template.
            py_keys = obj.py_obj.placeholder_keys();
            py_list = cell(py_keys);
            keys = cell(1, numel(py_list));
            for i = 1:numel(py_list)
                keys{i} = char(py_list{i});
            end
        end

        function combos = discover(obj)
        %DISCOVER  Walk filesystem and return all metadata combos matching template.
        %
        %   COMBOS = pi.discover()
        %
        %   Returns a cell array of structs, one per valid complete path.
        %   Each struct maps placeholder keys to their string values.
        %   Forwards to Python's PathInput.discover().

            py_combos = obj.py_obj.discover();
            n = int64(py.len(py_combos));
            combos = cell(1, n);
            for i = 1:n
                d = py_combos{i};
                s = struct();
                ks = cell(py.list(d.keys()));
                for ki = 1:numel(ks)
                    k = char(ks{ki});
                    s.(k) = char(d{k});
                end
                combos{i} = s;
            end
        end

        function disp(obj)
        %DISP  Display the PathInput.
            opts = "";
            if strlength(obj.root_folder) > 0
                opts = opts + sprintf(', root_folder="%s"', obj.root_folder);
            end
            if obj.regex
                opts = opts + ", regex=true";
            end
            fprintf('  scifor.PathInput("%s"%s)\n', obj.path_template, opts);
        end
    end
end
