classdef PathInput < handle
%SCIFOR.PATHINPUT  Resolve a path template using iteration metadata.
%
%   Works as an input to for_each: on each iteration, .load()
%   substitutes the current metadata values into the template and
%   returns the resolved file path as a string.  The user's function
%   receives the path and handles file reading itself.
%
%   PI = scifor.PathInput(TEMPLATE)
%   PI = scifor.PathInput(TEMPLATE, root_folder=FOLDER)
%   PI = scifor.PathInput(TEMPLATE, root_folder=FOLDER, regex=true)
%
%   The template uses {key} placeholders that are replaced by the
%   metadata values supplied by for_each on each iteration.
%
%   When regex=true, the resolved template is treated as a regular
%   expression and matched against filenames in the directory portion
%   of the path. Exactly one file must match; zero or multiple matches
%   produce an error.
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
        regex          logical  % Whether to use regex matching
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
        end

        function filepath = load(obj, varargin)
        %LOAD  Resolve the template with the given metadata.
        %
        %   PATH = pi.load(Name, Value, ...)
        %
        %   Substitutes {key} placeholders in the template with the
        %   supplied metadata values and returns the resolved absolute
        %   path as a string.  The 'db' key is accepted and ignored
        %   for compatibility with for_each's uniform db= passthrough.

            % Parse name-value pairs
            if mod(numel(varargin), 2) ~= 0
                error('scifor:PathInput', ...
                    'Metadata arguments must be name-value pairs.');
            end

            resolved = obj.path_template;
            for i = 1:2:numel(varargin)
                key = string(varargin{i});
                if strcmpi(key, 'db')
                    continue;  % Skip db parameter
                end
                val = varargin{i+1};
                if isnumeric(val)
                    val_str = num2str(val);
                else
                    val_str = string(val);
                end
                resolved = strrep(resolved, "{" + key + "}", val_str);
            end

            % Resolve to absolute path
            resolved_is_abs = startsWith(resolved, "/") || ...
                ~isempty(regexp(resolved, '^[A-Za-z]:', 'once'));
            if resolved_is_abs
                filepath = string(resolved);
            elseif strlength(obj.root_folder) > 0
                filepath = string(fullfile(obj.root_folder, resolved));
            else
                filepath = string(fullfile(pwd, resolved));
            end

            % Regex matching against directory contents
            if obj.regex
                % Split resolved template on '/' (not fileparts) to avoid
                % treating regex backslashes as Windows path separators.
                slash_idx = find(char(resolved) == '/', 1, 'last');
                if isempty(slash_idx)
                    dir_template = "";
                    pattern = resolved;
                else
                    dir_template = extractBefore(resolved, slash_idx);
                    pattern = extractAfter(resolved, slash_idx);
                end

                dir_is_abs = startsWith(dir_template, "/") || ...
                    ~isempty(regexp(dir_template, '^[A-Za-z]:', 'once'));
                if dir_is_abs
                    dir_path = string(dir_template);
                elseif strlength(obj.root_folder) > 0
                    dir_path = string(fullfile(obj.root_folder, dir_template));
                else
                    dir_path = string(fullfile(pwd, dir_template));
                end

                listing = dir(dir_path);
                listing = listing(~[listing.isdir]);
                names = string({listing.name});

                matches = false(size(names));
                for j = 1:numel(names)
                    tok = regexp(names(j), "^" + pattern + "$", 'once');
                    matches(j) = ~isempty(tok);
                end

                matched_names = names(matches);
                if numel(matched_names) == 0
                    error('scifor:PathInput:NoMatch', ...
                        'Regex pattern "%s" matched no files in "%s".', ...
                        pattern, dir_path);
                elseif numel(matched_names) > 1
                    error('scifor:PathInput:MultipleMatches', ...
                        'Regex pattern "%s" matched %d files in "%s": %s', ...
                        pattern, numel(matched_names), dir_path, ...
                        strjoin(matched_names, ', '));
                end

                filepath = string(fullfile(dir_path, matched_names(1)));
            end
        end

        function keys = placeholder_keys(obj)
        %PLACEHOLDER_KEYS  Return cell array of unique placeholder keys in the template.
            keys = {};
            seen = containers.Map('KeyType', 'char', 'ValueType', 'logical');
            tmpl = char(obj.path_template);
            idx = 1;
            while idx <= numel(tmpl)
                open_idx = find(tmpl(idx:end) == '{', 1, 'first');
                if isempty(open_idx)
                    break;
                end
                open_idx = open_idx + idx - 1;
                close_idx = find(tmpl(open_idx:end) == '}', 1, 'first');
                if isempty(close_idx)
                    break;
                end
                close_idx = close_idx + open_idx - 1;
                key = tmpl(open_idx+1 : close_idx-1);
                if ~isempty(key) && ~seen.isKey(key)
                    seen(key) = true;
                    keys{end+1} = key; %#ok<AGROW>
                end
                idx = close_idx + 1;
            end
        end

        function combos = discover(obj)
        %DISCOVER  Walk filesystem and return all metadata combos matching template.
        %
        %   COMBOS = pi.discover()
        %
        %   Returns a cell array of structs, one per valid complete path.
        %   Each struct maps placeholder keys to their string values.

            if strlength(obj.root_folder) > 0
                root = char(obj.root_folder);
            else
                root = pwd;
            end

            % Split template into segments
            tmpl = char(obj.path_template);
            tmpl = strrep(tmpl, '\', '/');
            segments = strsplit(tmpl, '/');
            % Remove empty segments
            segments = segments(~cellfun(@isempty, segments));

            combos = {};
            if isempty(segments)
                return;
            end

            combos = discover_walk(root, segments, 1, struct(), {});
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


function combos = discover_walk(current_dir, segments, seg_idx, bindings, combos)
%DISCOVER_WALK  Recursively descend through segments, matching filesystem entries.
    if seg_idx > numel(segments)
        return;
    end

    segment = segments{seg_idx};
    is_last = seg_idx == numel(segments);

    % Check if segment contains any placeholders
    has_placeholder = contains(segment, '{') && contains(segment, '}');

    if ~has_placeholder
        % Literal segment — must match exactly
        candidate = fullfile(current_dir, segment);
        if is_last
            if exist(candidate, 'file') || exist(candidate, 'dir')
                combos{end+1} = bindings;
            end
        else
            if isfolder(candidate)
                combos = discover_walk(candidate, segments, seg_idx + 1, bindings, combos);
            end
        end
        return;
    end

    % Segment has placeholder(s) — build a regex
    pattern = segment_to_regex(segment);

    try
        listing = dir(current_dir);
    catch
        return;
    end
    % Filter out . and ..
    listing = listing(~ismember({listing.name}, {'.', '..'}));
    names = sort({listing.name});
    is_dir_flags = [listing.isdir];
    % Sort is_dir_flags to match sorted names
    [~, sort_idx] = sort({listing.name});
    is_dir_flags = is_dir_flags(sort_idx);

    for i = 1:numel(names)
        entry = names{i};
        tok = regexp(entry, ['^' pattern '$'], 'names');
        if isempty(tok)
            continue;
        end

        % Validate captured values against existing bindings
        captured_fields = fieldnames(tok);
        clean_captured = struct();
        for f = 1:numel(captured_fields)
            raw_key = captured_fields{f};
            % Strip numbered suffixes (e.g. subject_2 -> subject)
            key = regexprep(raw_key, '_\d+$', '');
            clean_captured.(key) = tok.(raw_key);
        end

        consistent = true;
        clean_fields = fieldnames(clean_captured);
        for f = 1:numel(clean_fields)
            key = clean_fields{f};
            if isfield(bindings, key) && ~strcmp(bindings.(key), clean_captured.(key))
                consistent = false;
                break;
            end
        end
        if ~consistent
            continue;
        end

        new_bindings = bindings;
        for f = 1:numel(clean_fields)
            new_bindings.(clean_fields{f}) = clean_captured.(clean_fields{f});
        end

        if is_last
            combos{end+1} = new_bindings; %#ok<AGROW>
        else
            if is_dir_flags(i)
                combos = discover_walk(fullfile(current_dir, entry), ...
                    segments, seg_idx + 1, new_bindings, combos);
            end
        end
    end
end


function pattern = segment_to_regex(segment)
%SEGMENT_TO_REGEX  Convert a template segment to a regex with named capture groups.
    tmpl = char(segment);
    pattern = '';
    key_counts = containers.Map('KeyType', 'char', 'ValueType', 'double');
    idx = 1;
    while idx <= numel(tmpl)
        open_idx = find(tmpl(idx:end) == '{', 1, 'first');
        if isempty(open_idx)
            % Rest is literal
            pattern = [pattern, regexptranslate('escape', tmpl(idx:end))]; %#ok<AGROW>
            break;
        end
        open_idx = open_idx + idx - 1;
        % Add literal part before placeholder
        if open_idx > idx
            pattern = [pattern, regexptranslate('escape', tmpl(idx:open_idx-1))]; %#ok<AGROW>
        end
        close_idx = find(tmpl(open_idx:end) == '}', 1, 'first');
        if isempty(close_idx)
            % No closing brace — treat rest as literal
            pattern = [pattern, regexptranslate('escape', tmpl(open_idx:end))]; %#ok<AGROW>
            break;
        end
        close_idx = close_idx + open_idx - 1;
        key = tmpl(open_idx+1 : close_idx-1);

        % Handle duplicate keys by numbering
        if key_counts.isKey(key)
            key_counts(key) = key_counts(key) + 1;
            group_name = sprintf('%s_%d', key, key_counts(key));
        else
            key_counts(key) = 1;
            group_name = key;
        end
        pattern = [pattern, sprintf('(?<%s>[^/\\\\]+)', group_name)]; %#ok<AGROW>
        idx = close_idx + 1;
    end
end
