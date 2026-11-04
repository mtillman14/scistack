classdef PathOutput < handle
%SCIFOR.PATHOUTPUT  Output-path template for for_each.
%
%   Unlike scifor.PathInput — which locates EXISTING input files — a
%   PathOutput is a pure output-path template: for_each substitutes the
%   current combo's metadata into {key} placeholders and hands the
%   resolved path to the user function as a plain argument. There is no
%   discovery, no regex, and no file reading; the function decides what
%   to write where.
%
%   PO = scifor.PathOutput(TEMPLATE)
%
%   Substitution is a literal string replacement of each {key}:
%     - Combo metadata: {subject}, {session}, ... (schema keys).
%     - {ColName}: the current for_columns column (inside a for_columns
%       iteration).
%     - On the scidb.for_each path additionally branch_param placeholders
%       ({low_hz}, {bandpass.low_hz}, {variant}) — those are resolved
%       PYTHON-side during prepare (dotted names cannot be MATLAB struct
%       fields), and the finished per-combo paths cross the bridge; this
%       class's native resolve() covers only the pure-MATLAB scifor path
%       (schema keys + {ColName}).
%
%   Example:
%       scidb.for_each(@plot_timeseries, ...
%           struct('signal', RawSignal(), ...
%                  'filename', scifor.PathOutput("plots/{subject}_{trial}.png")), ...
%           {PlotFigure()}, ...
%           subject=["1" "2"], trial=["1" "2" "3"]);

    properties (SetAccess = private)
        template  string   % Path template with {key} placeholders
    end

    methods
        function obj = PathOutput(template)
        %PATHOUTPUT  Construct an output-path template.
            arguments
                template  string
            end
            if strlength(template) == 0
                error('scifor:PathOutput', 'PathOutput template must be non-empty.');
            end
            obj.template = template;
        end

        function resolved = resolve(obj, metadata, column)
        %RESOLVE  Substitute combo metadata (and optionally the current
        %   for_columns column) into the template. Literal strrep per key;
        %   keys missing from the metadata are left untouched (mirrors the
        %   Python scifor.PathOutput.resolve contract).
            arguments
                obj
                metadata  struct = struct()
                column    string = ""
            end
            resolved = char(obj.template);
            fields = fieldnames(metadata);
            for i = 1:numel(fields)
                key = fields{i};
                val = metadata.(key);
                if isnumeric(val)
                    val_str = char(string(val));
                else
                    val_str = char(string(val));
                end
                resolved = strrep(resolved, ['{' key '}'], val_str);
            end
            if strlength(column) > 0
                resolved = strrep(resolved, '{ColName}', char(column));
            end
        end

        function tf = has_column_token(obj)
        %HAS_COLUMN_TOKEN  True if the template references {ColName}.
            tf = contains(obj.template, "{ColName}");
        end
    end
end
