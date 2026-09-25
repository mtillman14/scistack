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
%   PI = scifor.PathInput(TEMPLATE, aliases=ALIASES)
%   PI = scifor.PathInput(TEMPLATE, key_regex=KEY_REGEX)
%
%   The template uses {key} placeholders that are replaced by the
%   metadata values supplied by for_each on each iteration.
%
%   When regex=true, the resolved last segment is treated as a regular
%   expression and matched against filenames in the parent directory.
%   Exactly one file must match; zero or multiple matches produce an
%   error.
%
%   ALIASES lets a template key have several on-disk spellings that all
%   mean one canonical value (match-only — never affects what gets
%   written). Nested struct: ALIASES.(key).(canonical) = [spelling, ...].
%   E.g. a {session} folder spelled "Baseline" resolving as "BL":
%
%       scifor.PathInput("{subject}/{session}/data.mat", ...
%           aliases=struct('session', struct('BL', ["Baseline", "1. Baseline"])))
%
%   KEY_REGEX overrides the default greedy capture used for a {key}
%   placeholder when discover() builds its matching regex. Needed when
%   two placeholders are adjacent with no delimiter between them (e.g.
%   "{speed}{trial}") — without a literal to anchor the split, greedy
%   backtracking hands everything but the last character to the first
%   placeholder. Struct: KEY_REGEX.(key) = PATTERN (a raw regex fragment,
%   no capturing group). E.g. "SS01_EMG_SSV1.mat" where speed is letters
%   and trial is digits:
%
%       scifor.PathInput("{subject}_EMG_{speed}{trial}.mat", ...
%           key_regex=struct('speed', '[A-Za-z]+', 'trial', '\d+'))
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
        aliases        struct   % key -> canonical -> [spellings] (see class help)
        key_regex      struct   % key -> regex pattern (see class help)
        name           string   % REQUIRED identity of this PathInput (see class help)
        py_obj                  % Python scifor.pathinput.PathInput instance
    end

    methods
        function obj = PathInput(path_template, options)
        %PATHINPUT  Construct a PathInput.
        %
        %   PI = scifor.PathInput(TEMPLATE, name=NAME)
        %   PI = scifor.PathInput(TEMPLATE, root_folder=FOLDER, name=NAME)
        %   PI = scifor.PathInput(TEMPLATE, regex=true, name=NAME)
        %   PI = scifor.PathInput(TEMPLATE, aliases=ALIASES, name=NAME)
        %   PI = scifor.PathInput(TEMPLATE, key_regex=KEY_REGEX, name=NAME)
        %
        %   NAME is required: it is the PathInput's identity. The template
        %   and root folder only say where to look, so moving the data or
        %   running on another computer changes nothing recorded.

            arguments
                path_template  string
                options.root_folder  string = ""
                options.regex        logical = false
                options.aliases      struct = struct()
                options.key_regex    struct = struct()
                options.name         string = ""
            end

            if strlength(strtrim(options.name)) == 0
                error('scifor:PathInput:NameRequired', ...
                    ['PathInput requires a non-empty name=, e.g. ' ...
                     'scifor.PathInput("{subject}/data.csv", name="RawData"). ' ...
                     'The name is the PathInput''s identity; the template is only ' ...
                     'where to look.']);
            end
            obj.name = options.name;

            obj.path_template = path_template;
            obj.root_folder = options.root_folder;
            obj.regex = options.regex;
            obj.aliases = options.aliases;
            obj.key_regex = options.key_regex;

            % Construct the Python instance once and reuse for load/discover.
            if strlength(options.root_folder) > 0
                py_root = char(options.root_folder);
            else
                py_root = py.None;
            end
            obj.py_obj = py.scifor.pathinput.PathInput( ...
                char(path_template), ...
                pyargs('root_folder', py_root, ...
                       'regex', logical(options.regex), ...
                       'aliases', scifor.PathInput.aliases_to_py(options.aliases), ...
                       'key_regex', scifor.PathInput.key_regex_to_py(options.key_regex), ...
                       'name', char(options.name)));
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

        function [filepath, resolutions] = load_with_captures(obj, meta_nv, numeric_match)
        %LOAD_WITH_CAPTURES  Resolve like load() and report bridged spellings.
        %
        %   [PATH, RESOLUTIONS] = pi.load_with_captures(META_NV, NUMERIC_MATCH)
        %
        %   META_NV is a name-value cell array ({'subject', 1, 'trial', 1});
        %   NUMERIC_MATCH is a string array / cellstr of the keys eligible
        %   for the numeric-equivalence fallback (keys outside it resolve
        %   strictly literally — how scidb handles string-declared schema
        %   keys).  RESOLUTIONS is a struct mapping each key whose on-disk
        %   spelling differs from its given value (e.g. trial=1 matching
        %   6MWT-001.mat yields struct('trial', "001")); empty struct when
        %   the literal path resolved.
        %
        %   Forwards to Python's PathInput.load_with_captures with the same
        %   value marshaling and error translation as load().

            if mod(numel(meta_nv), 2) ~= 0
                error('scifor:PathInput', ...
                    'Metadata arguments must be name-value pairs.');
            end

            % Marshal metadata -> py.dict, same value rendering as load().
            py_meta = py.dict();
            for i = 1:2:numel(meta_nv)
                key = char(string(meta_nv{i}));
                if strcmpi(key, 'db')
                    continue;
                end
                val = meta_nv{i+1};
                if isnumeric(val) && isscalar(val)
                    val_str = num2str(val);
                else
                    val_str = char(string(val));
                end
                py_meta.update(pyargs(key, val_str));
            end

            py_eligible = py.list(cellstr(string(numeric_match)));

            try
                res = obj.py_obj.load_with_captures(py_meta, ...
                    pyargs('numeric_match', py_eligible));
            catch err
                msg = err.message;
                if contains(msg, 'matched no files')
                    error('scifor:PathInput:NoMatch', '%s', msg);
                elseif contains(msg, 'matched') && contains(msg, 'files')
                    error('scifor:PathInput:MultipleMatches', '%s', msg);
                else
                    rethrow(err);
                end
            end

            filepath = string(char(py.str(res{1})));
            resolutions = struct();
            py_res = res{2};
            ks = cell(py.list(py_res.keys()));
            for ki = 1:numel(ks)
                k = char(ks{ki});
                resolutions.(k) = string(char(py_res{k}));
            end
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

        function [iterables_out, combos] = apply_discovery(obj, metadata_iterables, user_explicit_keys, condense_numeric)
        %APPLY_DISCOVERY  Fill empty metadata iterables from filesystem discovery.
        %
        %   [ITERABLES, COMBOS] = pi.apply_discovery(METADATA_ITERABLES, USER_EXPLICIT_KEYS)
        %   [ITERABLES, COMBOS] = pi.apply_discovery(METADATA_ITERABLES, USER_EXPLICIT_KEYS, CONDENSE_NUMERIC)
        %
        %   Thin wrapper over Python's PathInput.apply_discovery so the scidb
        %   and scifor layers share one discovery-orchestration implementation.
        %
        %   METADATA_ITERABLES is a struct mapping each iterated key to a cell
        %   array of values (empty cell {} for keys to resolve from disk).
        %   USER_EXPLICIT_KEYS is a string array / cellstr of the keys the
        %   caller passed with explicit (non-empty) values.
        %   CONDENSE_NUMERIC (default false) mirrors Python's
        %   PathInput.apply_discovery flag of the same name: when true, a
        %   discovered value that is purely digits (e.g. a zero-padded
        %   filename token "001") collapses to a MATLAB double (1) instead
        %   of staying a zero-padded char. Off by default -- scidb's
        %   declared-only schema_key_types contract must opt in explicitly
        %   instead; this flag is for policy-free standalone scifor use
        %   only. See docs/claude/schema-key-types.md.
        %
        %   Returns ITERABLES (same struct with empty template keys filled
        %   from disk) and COMBOS — a cell array of structs to drive iteration
        %   directly, or [] when the Cartesian product of ITERABLES should be
        %   used instead.
            if nargin < 4
                condense_numeric = false;
            end

            % Marshal metadata_iterables -> py.dict of key -> py.list(str).
            py_iter = py.dict();
            fns = fieldnames(metadata_iterables);
            for i = 1:numel(fns)
                vals = metadata_iterables.(fns{i});
                py_list = py.list();
                if ~isempty(vals)
                    if ~iscell(vals)
                        vals = num2cell(vals);
                    end
                    for j = 1:numel(vals)
                        py_list.append(py.str(char(string(vals{j}))));
                    end
                end
                py_iter.update(pyargs(fns{i}, py_list));
            end

            % Marshal user_explicit_keys -> py.list(str).
            py_explicit = py.list();
            ek = string(user_explicit_keys);
            for i = 1:numel(ek)
                py_explicit.append(py.str(char(ek(i))));
            end

            res = obj.py_obj.apply_discovery(py_iter, py_explicit, ...
                pyargs('condense_numeric', condense_numeric));
            out_iter = res{1};
            py_combos = res{2};

            % Convert returned iterables dict back to a MATLAB struct of
            % values. Discovered values are strings ("001") unless
            % condense_numeric collapsed a digit-only value to a Python
            % int -- scifor.PathInput.condense_py_value() preserves that
            % as a MATLAB double rather than forcing it back to char.
            iterables_out = struct();
            ks = cell(py.list(out_iter.keys()));
            for ki = 1:numel(ks)
                k = char(ks{ki});
                v_list = cell(py.list(out_iter{k}));
                vals = cell(1, numel(v_list));
                for vi = 1:numel(v_list)
                    vals{vi} = scifor.PathInput.condense_py_value(v_list{vi});
                end
                iterables_out.(k) = vals;
            end

            % Convert discovered combos (None -> []; else cell of structs).
            if isa(py_combos, 'py.NoneType')
                combos = [];
            else
                n = int64(py.len(py_combos));
                combos = cell(1, n);
                for i = 1:n
                    d = py_combos{i};
                    s = struct();
                    cks = cell(py.list(d.keys()));
                    for cki = 1:numel(cks)
                        ck = char(cks{cki});
                        s.(ck) = scifor.PathInput.condense_py_value(d{ck});
                    end
                    combos{i} = s;
                end
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
            if ~isempty(fieldnames(obj.aliases))
                opts = opts + sprintf(', aliases=<%d key(s)>', numel(fieldnames(obj.aliases)));
            end
            if ~isempty(fieldnames(obj.key_regex))
                opts = opts + sprintf(', key_regex=<%d key(s)>', numel(fieldnames(obj.key_regex)));
            end
            fprintf('  scifor.PathInput("%s"%s, name="%s")\n', obj.path_template, opts, obj.name);
        end
    end

    methods (Static, Access = private)
        function py_aliases = aliases_to_py(aliases)
        %ALIASES_TO_PY  Marshal the nested ALIASES struct into a py.dict of
        %   py.dict of py.list(str), matching Python's
        %   ``{key: {canonical: [spelling, ...]}}`` shape.

            py_aliases = py.dict();
            keys = fieldnames(aliases);
            for i = 1:numel(keys)
                key = keys{i};
                canon_struct = aliases.(key);
                if ~isstruct(canon_struct)
                    error('scifor:PathInput', ...
                        'aliases.%s must be a struct mapping canonical -> spellings.', key);
                end
                py_canon = py.dict();
                canonicals = fieldnames(canon_struct);
                for j = 1:numel(canonicals)
                    canonical = canonicals{j};
                    spellings = cellstr(string(canon_struct.(canonical)));
                    py_canon.update(pyargs(canonical, py.list(spellings)));
                end
                py_aliases.update(pyargs(key, py_canon));
            end
        end

        function py_key_regex = key_regex_to_py(key_regex)
        %KEY_REGEX_TO_PY  Marshal the flat KEY_REGEX struct into a py.dict
        %   of key -> regex-pattern str, matching Python's
        %   ``{key: pattern}`` shape.

            py_key_regex = py.dict();
            keys = fieldnames(key_regex);
            for i = 1:numel(keys)
                key = keys{i};
                pattern = key_regex.(key);
                if ~(ischar(pattern) || isstring(pattern))
                    error('scifor:PathInput', ...
                        'key_regex.%s must be a char or string regex pattern.', key);
                end
                py_key_regex.update(pyargs(key, char(pattern)));
            end
        end

        function v = condense_py_value(py_val)
        %CONDENSE_PY_VALUE  Convert one discovered scalar back to MATLAB.
        %
        %   A plain py.str converts to char, exactly as before. A py.int
        %   or py.float only appears here when condense_numeric=true
        %   condensed a digit-only capture (e.g. "001" -> 1) on the
        %   Python side -- preserve that as a MATLAB double rather than
        %   re-stringifying it, so the schema column actually ends up
        %   numeric, not just a shorter string.
            if isa(py_val, 'py.int') || isa(py_val, 'py.float')
                v = double(py_val);
            else
                v = char(string(py_val));
            end
        end
    end
end
