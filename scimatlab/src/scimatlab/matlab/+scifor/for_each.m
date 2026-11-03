function varargout = for_each(fn, inputs, varargin)
%SCIFOR.FOR_EACH  Execute a function for all combinations of metadata.
%
%   result = scifor.for_each(@FN, INPUTS, Name, Value, ...)
%   result = scifor.for_each(@FN, INPUTS, OUTPUTS, Name, Value, ...)
%   [r1, r2] = scifor.for_each(@FN, INPUTS, Name, Value, ...)
%
%   OUTPUTS may be given positionally (third argument, as in
%   scidb.for_each) or as outputs=. scidb-only options (save, db, ...; listed by
%   Python's scifor.options.matlab_discarded_options) are accepted, logged and ignored, so a call
%   written for scidb.for_each runs here unchanged.
%
%   Python decides, MATLAB applies (design review 05, M3). Python's
%   scifor.foreach plans the run -- the combinations, [] resolution,
%   PathInput discovery, locations=, the distribute target, as_table,
%   column groups, share_limits, every combination's PathInput/PathOutput
%   path, the banner and, at the end, the summary and the journal block --
%   through py.scifor.matlab_loop (plan / assemble / close). This file
%   describes its inputs once, then per combination slices each table by the
%   plan's row indices, calls FN, and stacks the results. It re-derives
%   nothing the plan decides.
%
%   Inputs can be:
%   - MATLAB tables        — filtered per combo by schema key columns
%   - scifor.Fixed(tbl)    — table filtered with overridden metadata
%   - scifor.Merge(t1, t2) — multiple tables merged column-wise per combo
%   - scifor.ColumnSelection(tbl, cols) — column extraction after filtering
%   - scifor.PathInput / scifor.PathOutput — a path per combination
%   - Constants            — passed directly to the function
%
%   Arguments:
%       fn      - Function handle
%       inputs  - Struct mapping parameter names to MATLAB tables,
%                 scifor.Fixed wrappers, scifor.Merge wrappers,
%                 scifor.ColumnSelection wrappers, or constant values.
%                 The field order determines argument order when calling fn.
%
%   Name-Value Arguments:
%       dry_run       - If true, preview without executing (default: false)
%       as_table      - If true, keep schema key columns when passing
%                       filtered tables. Can be a string array of specific
%                       input names. (default: false)
%       distribute    - If true, split each output by element/row and
%                       expand them into the result table at the schema
%                       level below the deepest iterated key (the top of the
%                       schema when nothing at a schema level is iterated).
%       where         - Optional scifor.ColFilter applied by MATLAB to each
%                       combination's table rows. (default: [])
%       categorical   - If true, convert metadata columns in result
%                       tables to categorical. (default: false)
%       outputs       - Result column names, one per output: a cell array
%                       of names and/or objects (see scifor.output_column_name),
%                       or a count N. Defaults to {'output'} for each.
%       locations     - JSON text: which schema locations to run, as
%                       scidb.for_each takes it. (default: none)
%       project_root  - Folder a PathInput without root_folder resolves its
%                       relative template against. Default: the pinned
%                       project root, else the cwd.
%       schema_key_types - struct: key -> 'numeric' | 'string'. Standalone
%                       only: scidb.for_each applies the database's types.
%       share_limits  - struct: input name -> schema keys held fixed; each
%                       group's [min max] arrives as a trailing argument.
%       _all_combos   - The combinations to run (py.list of dicts from
%                       scidb's prepare, or a cell of structs).
%       _row_selection - Aligned with _all_combos: per combo, which record
%                       ids each input reads (py.list of dicts). Needs
%                       _record_id_column, which is dropped before fn sees
%                       the table.
%       _resolved_path_outputs - py.dict {param -> [path per combo]} aligned
%                       with _all_combos (scidb resolves branch placeholders).
%       _mapping_inputs - struct: input name -> data column names of a
%                       struct-valued input (see extract_data).
%       (any other)   - Metadata iterables (numeric or string arrays)
%
%   Returns:
%       One table per function output. Returns [] for dry_run.
%
%   Example:
%       set_schema(["subject", "session"])
%       result = scifor.for_each(@filter_data, ...
%           struct('raw', data_table, 'smoothing', 0.2), ...
%           subject=[1 2 3], session=["A" "B"])

    % --- Fix struct-array inputs from struct() cell-array gotcha ---
    if ~isscalar(inputs)
        inputs = collapse_struct_array(inputs);
    end

    % --- Positional OUTPUTS (third argument, as in scidb.for_each). Name-value
    %     arguments come in pairs, so an odd count means the first is OUTPUTS;
    %     rewritten to outputs= so the EachOf recursion re-parses it. ---
    if mod(numel(varargin), 2) == 1
        varargin = [{'outputs', varargin{1}}, varargin(2:end)];
    end

    [meta_args, opts] = split_options(varargin{:});
    if ~isempty(opts.discarded)
        scifor.Log.info(['discard_db_only_kwargs: ignored %s (scifor has no ' ...
            'database; these only take effect in scidb.for_each)'], ...
            strjoin(opts.discarded, ', '));
    end
    if strlength(opts.project_root) > 0
        % The MATLAB PathInput objects a scidb loader receives are bound
        % here; Python binds its own copies to the same root.
        inputs = bind_path_inputs(inputs, opts.project_root);
    end

    if isa(fn, 'function_handle')
        fn_name = func2str(fn);
    else
        fn_name = 'unknown';
    end

    % --- EachOf: each alternative is an independent recursive call (its
    %     alternatives are MATLAB objects, so the recursion is MATLAB's; each
    %     branch is planned by Python like any other call). ---
    each_of_axes = each_of_axes_of(inputs, opts.where);
    if ~isempty(each_of_axes)
        varargout = run_each_of(fn, fn_name, inputs, varargin, each_of_axes, max(nargout, 1));
        return;
    end

    % --- Plan (Python) ---
    t_plan = tic;
    [plan, timing] = plan_with_python(fn, fn_name, inputs, meta_args, opts, nargout);
    scifor.Log.info('[timing] matlab_plan: %.3fs (describe %.3fs, python %.3fs, decode %.3fs)', ...
        toc(t_plan), timing.describe, timing.python, timing.decode);
    if plan.dry_run
        fprintf('%s', plan.dry_run_text);
        varargout = repmat({[]}, 1, max(nargout, 1));
        return;
    end
    cleanup = onCleanup(@() py.scifor.matlab_loop.discard(plan.token)); %#ok<NASGU>

    % --- The loop: slice, call, collect ---
    [state, stop_err] = run_combos(fn, fn_name, inputs, plan, opts);

    % --- Assemble (Python: the summary), then MATLAB stacks its values ---
    t_asm = tic;
    summary = assemble_run(plan, state, opts, stop_err);
    collected = state.collected;
    if ~isempty(summary.combine_columns) && ~isempty(collected{1})
        templates = missing_templates(collected{1});
        expected = string(summary.combine_columns(:)');
        collected{1} = cellfun(@(e) {e{1}, reindex_combine_row(e{2}, expected, templates)}, ...
            collected{1}, 'UniformOutput', false);
    end
    try
        varargout = output_tables(collected, plan, opts, fn_name, max(nargout, 1));
    catch build_err
        py.scifor.matlab_loop.close(plan.token, jsonencode( ...
            scifor.failure_from_mexception(build_err, fn_name, '', 'run', false)));
        rethrow(build_err);
    end
    py.scifor.matlab_loop.close(plan.token, '');
    scifor.Log.info('[timing] matlab_assemble: %.3fs', toc(t_asm));

    % on_error='stop': the ORIGINAL error, after the summary and outputs. A
    % caller that saves the completed combos first (scidb.for_each) passes
    % _defer_stop and rethrows it itself, from the run tally.
    if ~isempty(stop_err) && ~opts.defer_stop
        rethrow(stop_err);
    end
end


% =========================================================================
% EachOf
% =========================================================================

function axes_ = each_of_axes_of(inputs, where_filter)
%EACH_OF_AXES_OF  {kind, field, alternatives} per EachOf input / where=.
    axes_ = {};
    names = fieldnames(inputs);
    for p = 1:numel(names)
        v = inputs.(names{p});
        if isa(v, 'scifor.EachOf')
            scifor.require_alternatives(v, 'input', names{p});
            axes_{end+1} = {'input', names{p}, v.alternatives}; %#ok<AGROW>
        end
    end
    if isa(where_filter, 'scifor.EachOf')
        scifor.require_alternatives(where_filter, 'where');
        axes_{end+1} = {'where', '', where_filter.alternatives};
    end
end


function out = run_each_of(fn, fn_name, inputs, varargin_in, axes_, n_out)
%RUN_EACH_OF  One scifor.for_each per EachOf alternative; results are
%   concatenated per output (vertcat_each_of_results).
    scifor.Log.debug('EachOf expansion detected - %d axis(es), making recursive calls', ...
        numel(axes_));
    value_cells = cellfun(@(ax) ax{3}, axes_, 'UniformOutput', false);
    combos = scidb.internal.cartesian_product(value_cells);
    branch_out = cell(numel(combos), n_out);
    for ci = 1:numel(combos)
        combo = combos{ci};
        concrete_inputs = inputs;
        concrete_varargin = varargin_in;
        for a = 1:numel(axes_)
            ax = axes_{a};
            if strcmp(ax{1}, 'input')
                concrete_inputs.(ax{2}) = combo{a};
            else
                concrete_varargin = replace_name_value(concrete_varargin, 'where', combo{a});
            end
        end
        [branch_out{ci, 1:n_out}] = scifor.for_each(fn, concrete_inputs, concrete_varargin{:});
    end
    out = cell(1, n_out);
    for o = 1:n_out
        branch_col = branch_out(:, o);
        branch_col = branch_col(~cellfun(@isempty, branch_col));
        out{o} = vertcat_each_of_results(branch_col, fn_name);
    end
    scifor.Log.debug('EachOf expansion complete - concatenated %d branch result(s)', numel(combos));
end


% =========================================================================
% Plan: describe the inputs, ask Python, decode the answer
% =========================================================================

function [plan, timing] = plan_with_python(fn, fn_name, inputs, meta_args, opts, call_nargout)
%PLAN_WITH_PYTHON  The run's plan from py.scifor.matlab_loop.plan (the owner).
%   Crosses once each way: a JSON description plus ONE float64 buffer in;
%   a JSON plan plus int64/int32/float64 buffers out. Nothing per row.
    t0 = tic;
    [desc, numbers, path_inputs] = describe_run(fn, fn_name, inputs, meta_args, opts, call_nargout);
    desc_json = jsonencode(desc);
    timing.describe = toc(t0);

    t1 = tic;
    res = py.scifor.matlab_loop.plan(desc_json, to_py_bytes(numbers), pyargs( ...
        'prebuilt_combos', prebuilt_to_py(opts.all_combos), ...
        'row_selection', row_selection_to_py(opts.row_selection), ...
        'resolved_path_outputs', path_outputs_to_py(opts.resolved_path_outputs), ...
        'path_inputs', path_inputs));
    timing.python = toc(t1);

    t2 = tic;
    plan = jsondecode(char(res{'json'}));
    if isfield(plan, 'error_id')
        % A call that cannot run at all (no schema for distribute, [] with no
        % source, ColName / column-group misuse): Python names the error.
        error(plan.error_id, '%s', plan.error_message);
    end
    if isfield(plan, 'dry_run') && plan.dry_run
        timing.decode = toc(t2);
        return;
    end
    plan.dry_run = false;
    plan = decode_plan(plan, res);
    timing.decode = toc(t2);
    scifor.Log.debug(['[matlab-plan] crossed: description %d char(s) + %d numeric ' ...
        'value(s) in; %d combo(s), %d slice(s), %d row index(es) out'], ...
        numel(desc_json), numel(numbers), plan.n_combos, plan.n_slices, numel(plan.row_index));
end


function [desc, numbers, path_inputs] = describe_run(fn, fn_name, inputs, meta_args, opts, call_nargout)
%DESCRIBE_RUN  The run as Python's matlab_loop._Description reads it.
%   Numbers go to NUMBERS (one buffer), referenced as [offset count].
    numbers = zeros(0, 1);
    path_inputs = py.dict();
    try
        fn_nargin = nargin(fn);
    catch
        fn_nargin = -1;
    end
    try
        fn_nargout = nargout(fn);
    catch
        fn_nargout = 1;
    end

    metadata = {};
    meta_keys = string.empty;
    for i = 1:2:numel(meta_args)
        meta_keys(end+1) = string(meta_args{i}); %#ok<AGROW>
        [values, numbers] = describe_values(meta_args{i+1}, numbers);
        metadata{end+1} = struct('key', char(meta_args{i}), 'values', values); %#ok<AGROW>
    end

    names = fieldnames(inputs);
    candidates = key_candidates(inputs, meta_keys, opts);
    described = cell(1, numel(names));
    for p = 1:numel(names)
        wants_extents = isfield(opts.share_limits, names{p});
        [described{p}, numbers, path_inputs] = describe_input(names{p}, inputs.(names{p}), ...
            candidates, opts, wants_extents, numbers, path_inputs);
    end

    desc = struct( ...
        'fn_name', fn_name, ...
        'fn_nargin', fn_nargin, ...
        'fn_nargout', fn_nargout, ...
        'call_nargout', call_nargout, ...
        'outputs', describe_outputs(opts.outputs), ...
        'metadata', {metadata}, ...
        'schema_keys', [], ...
        'options', describe_options(opts), ...
        'inputs', {described});
    if ~isempty(opts.schema_keys)
        desc.schema_keys = cellstr(opts.schema_keys);
    end
end


function out = describe_outputs(outputs)
    if isempty(outputs)
        out = struct('kind', 'default');
    elseif isnumeric(outputs) && isscalar(outputs)
        out = struct('kind', 'count', 'n', outputs);
    else
        names = cellfun(@(o) char(scifor.output_column_name(o)), outputs, 'UniformOutput', false);
        out = struct('kind', 'names', 'names', {names});
    end
end


function o = describe_options(opts)
    where_text = [];
    if ~isempty(opts.where)
        try
            where_text = char(opts.where.to_key());
        catch
            where_text = class(opts.where);
        end
    end
    as_table = opts.as_table;
    if isstring(as_table)
        as_table = cellstr(as_table);
    end
    share_limits = struct();
    sl = fieldnames(opts.share_limits);
    for i = 1:numel(sl)
        share_limits.(sl{i}) = cellstr(string(opts.share_limits.(sl{i})));
    end
    mapping = struct();
    mi = fieldnames(opts.mapping_inputs);
    for i = 1:numel(mi)
        mapping.(mi{i}) = cellstr(string(opts.mapping_inputs.(mi{i})));
    end
    o = struct( ...
        'dry_run', opts.dry_run, ...
        'distribute', opts.distribute, ...
        'as_table', {as_table}, ...
        'where', where_text, ...
        'locations', char(string(opts.locations)), ...
        'on_error', opts.on_error, ...
        'schema_key_types', opts.schema_key_types, ...
        'project_root', char(opts.project_root), ...
        'share_limits', share_limits, ...
        'mapping_inputs', mapping, ...
        'record_id_column', char(opts.record_id_column), ...
        'resolve_pathinput', opts.resolve_pathinput, ...
        'has_pathinput_loader', ~isempty(opts.pathinput_loader));
end


function c = key_candidates(inputs, meta_keys, opts)
%KEY_CANDIDATES  The column names whose VALUES Python may need: the schema,
%   the iterated keys, schema_keys=, share_limits keys, Fixed keys and
%   PathInput placeholders. A transport limit, not a decision: every other
%   column crosses by name only.
    c = [as_row(scifor.get_schema()), as_row(meta_keys), as_row(opts.schema_keys)];
    sl = fieldnames(opts.share_limits);
    for i = 1:numel(sl)
        c = [c, as_row(opts.share_limits.(sl{i}))]; %#ok<AGROW>
    end
    names = fieldnames(inputs);
    for p = 1:numel(names)
        v = inputs.(names{p});
        if isa(v, 'scifor.Fixed')
            c = [c, as_row(fieldnames(v.fixed_metadata))]; %#ok<AGROW>
            v = v.data;
        end
        if isa(v, 'scifor.PathInput')
            c = [c, as_row(v.placeholder_keys())]; %#ok<AGROW>
        end
    end
    c = unique(c(strlength(c) > 0), 'stable');
end


function s = as_row(x)
%AS_ROW  Names (string, char, cellstr) as a 1xN string array.
    s = reshape(string(x), 1, []);
end


function [d, numbers, path_inputs] = describe_input(name, v, candidates, opts, wants_extents, numbers, path_inputs)
%DESCRIBE_INPUT  One input's kind and, for tables, its key columns.
    d = struct('name', name, 'kind', 'constant');
    if istable(v)
        d.kind = 'table';
        [d.table, numbers] = describe_table(v, candidates, opts, wants_extents, numbers);
    elseif isa(v, 'scifor.Fixed') && isa(v.data, 'scifor.PathInput')
        d.kind = 'fixed_path_input';
        d.fixed = v.fixed_metadata;
        path_inputs{name} = v.data.py_obj;
    elseif isa(v, 'scifor.Fixed')
        if isa(v.data, 'scifor.Merge')
            error('scifor:for_each', ['Fixed cannot wrap a Merge. Use Fixed on individual ' ...
                'constituents inside the Merge instead: Merge(Fixed(tbl1, ...), tbl2)']);
        end
        d.kind = 'fixed';
        d.fixed = v.fixed_metadata;
        [d.inner, numbers, path_inputs] = describe_input(name, v.data, candidates, opts, ...
            wants_extents, numbers, path_inputs);
    elseif isa(v, 'scifor.ColumnSelection')
        d.kind = 'column_selection';
        [d.table, numbers] = describe_table(v.data, candidates, opts, wants_extents, numbers);
        d.columns = cellstr(v.columns);
        d.iterate = v.iterate;
        groups = cell(1, numel(v.groups));
        for g = 1:numel(v.groups)
            groups{g} = struct('name', char(v.groups(g).name), ...
                'columns', {cellstr(v.groups(g).columns)});
        end
        d.groups = groups;
    elseif isa(v, 'scifor.Merge')
        d.kind = 'merge';
        parts = cell(1, numel(v.tables));
        for i = 1:numel(v.tables)
            [parts{i}, numbers, path_inputs] = describe_input(sprintf('%s[%d]', name, i), ...
                v.tables{i}, candidates, opts, false, numbers, path_inputs);
        end
        d.constituents = parts;
    elseif isa(v, 'scifor.PathInput')
        d.kind = 'path_input';
        d.text = char(v.path_template);
        path_inputs{name} = v.py_obj;
    elseif isa(v, 'scifor.PathOutput')
        d.kind = 'path_output';
        d.template = char(v.template);
    elseif isa(v, 'scifor.ColName') && v.is_deferred()
        d.kind = 'colname_deferred';
    elseif isa(v, 'scifor.ColName')
        if ~istable(v.data)
            error('scifor:ColName', 'ColName(%s) expected a table, got %s', name, class(v.data));
        end
        d.kind = 'colname_static';
        [d.table, numbers] = describe_table(v.data, candidates, opts, false, numbers);
    else
        d.text = format_value(v);
    end
end


function [t, numbers] = describe_table(tbl, candidates, opts, wants_extents, numbers)
%DESCRIBE_TABLE  Column names, row count, and the candidate key columns'
%   values (numbers into the buffer, text into the JSON); per-row [min max]
%   of every column for a share_limits input.
    cols = tbl.Properties.VariableNames;
    keys = struct();
    for c = 1:numel(cols)
        if ~ismember(string(cols{c}), candidates)
            continue;
        end
        [keys.(cols{c}), numbers] = describe_key_column(tbl.(cols{c}), numbers);
    end
    t = struct('columns', {cols}, 'n_rows', height(tbl), 'keys', keys, ...
        'record_ids', [], 'extents', []);
    rid = char(opts.record_id_column);
    if ~isempty(rid) && ismember(rid, cols)
        t.record_ids = cellstr(string(tbl.(rid)));
    end
    if wants_extents
        [ref, numbers] = push_numbers(numbers, row_extents(tbl));
        t.extents = struct('numbers', ref);
    end
end


function [k, numbers] = describe_key_column(col, numbers)
    k = struct('class', class(col));
    if (isnumeric(col) || islogical(col)) && size(col, 2) == 1
        [k.numbers, numbers] = push_numbers(numbers, double(col));
    elseif iscategorical(col)
        k.texts = cellstr_or_null(string(col));
        k.categories = categories(col);
        k.ordinal = isordinal(col);
    elseif isstring(col) || isdatetime(col) || isduration(col)
        k.texts = cellstr_or_null(string(col));
    elseif iscellstr(col)
        k.texts = col;
    elseif ischar(col)
        k.texts = cellstr(col);
    else
        try
            k.texts = cellstr_or_null(string(col));
        catch
            % No text form: the class still counts for round-trip types.
        end
    end
end


function c = cellstr_or_null(s)
%CELLSTR_OR_NULL  A string column as a cell, <missing> as [] (JSON null).
    s = s(:);
    c = cellstr(s);
    c(ismissing(s)) = {[]};
end


function E = row_extents(tbl)
%ROW_EXTENTS  n_rows x n_columns x 2: each cell's finite [min max] (NaN
%   when it holds no number). The per-value half of share_limits; Python
%   groups the rows and takes each group's extent.
    n = height(tbl);
    cols = tbl.Properties.VariableNames;
    E = NaN(n, numel(cols), 2);
    for c = 1:numel(cols)
        col = tbl.(cols{c});
        if (isnumeric(col) || islogical(col)) && size(col, 1) == n
            v = double(reshape(col, n, []));
            v(~isfinite(v)) = NaN;
            E(:, c, 1) = min(v, [], 2, 'omitnan');
            E(:, c, 2) = max(v, [], 2, 'omitnan');
        elseif iscell(col) && numel(col) == n
            for r = 1:n
                x = col{r};
                if (isnumeric(x) || islogical(x)) && ~isempty(x)
                    x = double(x(:));
                    x = x(isfinite(x));
                    if ~isempty(x)
                        E(r, c, 1) = min(x);
                        E(r, c, 2) = max(x);
                    end
                end
            end
        end
    end
end


function [values, numbers] = describe_values(v, numbers)
%DESCRIBE_VALUES  One metadata iterable: numbers, texts, or mixed items.
    if isnumeric(v) || islogical(v)
        [ref, numbers] = push_numbers(numbers, double(v(:)));
        values = struct('numbers', ref);
    elseif isstring(v)
        values = struct('texts', {cellstr(v(:)')});
    elseif ischar(v)
        values = struct('texts', {{v}});
    elseif iscell(v) && all(cellfun(@(x) (isnumeric(x) || islogical(x)) && isscalar(x), v))
        [ref, numbers] = push_numbers(numbers, cellfun(@double, v(:)));
        values = struct('numbers', ref);
    elseif iscell(v) && all(cellfun(@(x) ischar(x) || (isstring(x) && isscalar(x)), v))
        values = struct('texts', {cellfun(@char, v(:)', 'UniformOutput', false)});
    elseif iscell(v)
        values = struct('items', {v(:)'});
    else
        values = struct('texts', {{char(string(v))}});
    end
end


function [ref, numbers] = push_numbers(numbers, v)
%PUSH_NUMBERS  Append V to the numeric buffer; REF is [offset count].
    v = double(v(:));
    ref = [numel(numbers), numel(v)];
    numbers = [numbers; v];
end


function b = to_py_bytes(numbers)
%TO_PY_BYTES  The numeric buffer as one py.bytes (float64, one memcpy).
    if isempty(numbers)
        b = py.bytes(uint8(zeros(1, 0)));
    else
        b = py.bytes(typecast(double(numbers(:))', 'uint8'));
    end
end


function v = from_py_bytes(py_bytes, class_name)
%FROM_PY_BYTES  A py.bytes buffer as a MATLAB vector of CLASS_NAME.
    raw = uint8(py_bytes);
    if isempty(raw)
        v = zeros(0, 1, class_name);
    else
        v = typecast(raw, class_name);
        v = v(:);
    end
end


function out = prebuilt_to_py(all_combos)
%PREBUILT_TO_PY  _all_combos as Python's plan reads it. scidb passes its
%   py.list through untouched; a MATLAB cell of structs (a direct caller)
%   is converted once. Empty means "no pre-built combos".
    if isa(all_combos, 'py.list')
        out = all_combos;
    elseif iscell(all_combos) && ~isempty(all_combos)
        out = py.list();
        for c = 1:numel(all_combos)
            out.append(struct_to_py_dict(all_combos{c}));
        end
    else
        out = py.None;
    end
end


function out = row_selection_to_py(row_selection)
    if isa(row_selection, 'py.list')
        out = row_selection;
    elseif iscell(row_selection) && ~isempty(row_selection)
        out = py.list();
        for c = 1:numel(row_selection)
            d = py.dict();
            s = row_selection{c};
            if isstruct(s)
                f = fieldnames(s);
                for k = 1:numel(f)
                    d{f{k}} = py.list(cellstr(string(s.(f{k}))));
                end
            end
            out.append(d);
        end
    else
        out = py.None;
    end
end


function out = path_outputs_to_py(rpo)
    if isa(rpo, 'py.dict')
        out = rpo;
    elseif isstruct(rpo) && ~isempty(fieldnames(rpo))
        out = py.dict();
        f = fieldnames(rpo);
        for k = 1:numel(f)
            out{f{k}} = py.list(cellstr(string(rpo.(f{k}))));
        end
    else
        out = py.None;
    end
end


function d = struct_to_py_dict(s)
    d = py.dict();
    f = fieldnames(s);
    for k = 1:numel(f)
        val = s.(f{k});
        if (isnumeric(val) || islogical(val)) && isscalar(val)
            d{f{k}} = double(val);
        elseif isempty(val)
            d{f{k}} = py.None;
        else
            d{f{k}} = char(string(val));
        end
    end
end


function plan = decode_plan(plan, res)
%DECODE_PLAN  The plan's JSON plus its buffers, in the shapes the loop reads.
    n = plan.n_combos;
    plan.keys = as_cellstr(plan.keys);
    plan.labels = as_cellstr(plan.labels);
    plan.output_names = as_cellstr(plan.output_names);
    plan.filter_keys = string(as_cellstr(plan.filter_keys));
    plan.schema_keys = string(as_cellstr(plan.schema_keys));
    plan.inputs = as_cell(plan.inputs);
    plan.limits.names = as_cellstr(plan.limits.names);
    plan.fatal_ids = as_cellstr(plan.fatal_ids);
    plan.distribute_key = char(plan.distribute_key);
    if isempty(plan.column_types)
        plan.column_types = struct();
    end
    plan.combos = combos_from_plan(plan, from_py_bytes(res{'combo_index'}, 'int32'));
    plan.row_index = double(from_py_bytes(res{'row_index'}, 'int64'));
    plan.row_offsets = reshape(double(from_py_bytes(res{'row_offsets'}, 'int64')), ...
        plan.n_slices, n + 1);
    plan.limit_values = reshape(from_py_bytes(res{'limits'}, 'double'), ...
        n, numel(plan.limits.names), 2);
    plan.is_mark = false(1, n);
    marks = double(plan.progress.marks(:)');
    plan.is_mark(marks(marks < n) + 1) = true;
    if ~isempty(plan.for_columns)
        groups = plan.for_columns.groups;
        if iscell(groups)
            groups = [groups{:}];
        end
        for g = 1:numel(groups)
            groups(g).name = string(groups(g).name);
            groups(g).columns = string(as_cellstr(groups(g).columns));
        end
        plan.for_columns.groups = groups;
        plan.for_columns.params = as_cellstr(plan.for_columns.params);
    end
end


function combos = combos_from_plan(plan, index)
%COMBOS_FROM_PLAN  One metadata struct per combination, from the key
%   pools and the n_combos x n_keys index matrix (0: key absent).
    n = plan.n_combos;
    keys = plan.keys;
    pools = as_cell(plan.pools);
    index = reshape(double(index), n, numel(keys));
    values = cell(1, numel(keys));
    for k = 1:numel(keys)
        values{k} = pool_values(pools{k});
    end
    combos = cell(1, n);
    for c = 1:n
        s = struct();
        for k = 1:numel(keys)
            j = index(c, k);
            if j > 0
                s.(keys{k}) = values{k}{j};
            end
        end
        combos{c} = s;
    end
end


function vals = pool_values(pool)
%POOL_VALUES  A decoded key pool as a cell of MATLAB values (numbers as
%   double, text as char, null as []), as the element-wise conversion gave.
    if isstruct(pool) && isfield(pool, 'v')
        pool = pool.v;
    end
    if isnumeric(pool) || islogical(pool)
        vals = num2cell(pool(:)');
        return;
    end
    if ischar(pool)
        vals = {pool};
        return;
    end
    vals = pool(:)';
    for i = 1:numel(vals)
        if isstruct(vals{i}) && isfield(vals{i}, 'nonfinite')
            vals{i} = str2double(vals{i}.nonfinite);
        end
    end
end


function c = as_cell(x)
%AS_CELL  A decoded JSON array as a cell (jsondecode merges an array of
%   same-shaped objects into a struct array and gives [] for an empty one).
    if isempty(x)
        c = {};
    elseif iscell(x)
        c = x(:)';
    elseif isstruct(x)
        c = num2cell(x(:)');
    else
        c = {x};
    end
end


function c = as_cellstr(x)
%AS_CELLSTR  A decoded JSON array of strings as a row cellstr.
    if isempty(x)
        c = {};
    elseif ischar(x)
        c = {x};
    elseif isstring(x)
        c = cellstr(x(:)');
    else
        c = cellfun(@char, x(:)', 'UniformOutput', false);
    end
end


% =========================================================================
% The loop
% =========================================================================

function [state, stop_err] = run_combos(fn, fn_name, inputs, plan, opts)
%RUN_COMBOS  Per combination: slice the inputs by the plan, call FN, keep
%   its outputs. Failures are recorded by reason (record_iteration_failure);
%   nothing here decides what the summary says.
    input_names = fieldnames(inputs);
    n_inputs = numel(input_names);
    n_outputs = numel(plan.output_names);
    fc = plan.for_columns;
    iterate_pos = false(1, n_inputs);
    iter_as_table = false(1, n_inputs);
    for p = 1:n_inputs
        d = plan.inputs{p};
        if strcmp(d.kind, 'data') && strcmp(d.shape, 'iterate')
            iterate_pos(p) = true;
            iter_as_table(p) = d.as_table;
        end
    end

    state = struct('completed', 0, 'skipped', 0, 'no_data', 0, ...
        'collected', {repmat({{}}, 1, max(n_outputs, 1))}, ...
        'failures', new_failures(), 'output_skips', {{}}, ...
        'no_data_groups', containers.Map('KeyType', 'char', 'ValueType', 'any'), ...
        'group_columns', containers.Map('KeyType', 'char', 'ValueType', 'any'), ...
        'distribute_from_column', 0, 'stop_group', 0, 'elapsed', 0);
    stop_err = [];
    loop_t0 = tic;
    last_emit = 0;

    for c = 1:plan.n_combos
        metadata = plan.combos{c};
        metadata_str = plan.labels{c};
        if plan.is_mark(c)
            last_emit = maybe_progress(plan, c, state, toc(loop_t0), last_emit);
        end

        % --- Prepare every input for this combo ---
        loaded = cell(1, n_inputs);
        iterate_tables = cell(1, n_inputs);
        failed = false;
        for p = 1:n_inputs
            d = plan.inputs{p};
            try
                if iterate_pos(p)
                    iterate_tables{p} = prepare_data(inputs.(input_names{p}), d, plan, c, opts);
                else
                    loaded{p} = prepare_value(inputs.(input_names{p}), d, plan, c, metadata, opts);
                end
            catch err
                % An error the plan calls fatal (it would fail every combo
                % the same way, e.g. scidb's SchemaKeyTypeError from its
                % PathInput loader) ends the run, as in Python's loop.
                if ismember(err.identifier, plan.fatal_ids)
                    rethrow(err);
                end
                [state, k] = record_iteration_failure(state, err, c, metadata_str, ...
                    sprintf('failed to filter %s', input_names{p}), fn_name, 'prepare_inputs');
                [stop_err, state] = note_stop(opts.on_error, stop_err, err, state, k);
                failed = true;
                break;
            end
        end
        if failed
            state.skipped = state.skipped + 1;
            if ~isempty(stop_err)
                break;
            end
            continue;
        end

        if ~isempty(fc)
            scifor.Log.debug('[run] %s: %s x %d group(s) (%s)', metadata_str, fn_name, ...
                numel(fc.groups), strjoin(string(input_names'), ', '));
        else
            scifor.Log.debug('[run] %s: %s(%s)', metadata_str, fn_name, ...
                strjoin(string(input_names'), ', '));
        end
        call_args = [loaded, limit_args(plan, c)];

        % --- Call the function ---
        slot_skips = [];
        produced = [];
        try
            if ~isempty(fc)
                [result, slot_skips, produced] = run_column_iteration(fn, loaded, ...
                    iterate_pos, iterate_tables, fc.groups, fc.mode, ...
                    plan.schema_keys, iter_as_table, metadata_str);
            else
                result = call_fn(fn, call_args, n_outputs);
            end
        catch err
            % Errors the plan calls fatal (structural for_columns errors, a
            % key-type error) would fail every combo: surface, don't skip.
            if ismember(err.identifier, plan.fatal_ids)
                rethrow(err);
            end
            [state, k] = record_iteration_failure(state, err, c, metadata_str, ...
                sprintf('%s raised', fn_name), fn_name, 'call');
            state.skipped = state.skipped + 1;
            [stop_err, state] = note_stop(opts.on_error, stop_err, err, state, k);
            if ~isempty(stop_err)
                break;
            end
            continue;
        end

        state = collect_result(state, result, metadata, metadata_str, c, plan, opts, ...
            slot_skips, produced, n_outputs);
        state.completed = state.completed + 1;
    end
    state.elapsed = toc(loop_t0);
    scifor.Log.info('[timing] matlab_loop: %d combo(s) in %.3fs (completed=%d, skipped=%d)', ...
        plan.n_combos, state.elapsed, state.completed, state.skipped);
end


function last_emit = maybe_progress(plan, c, state, elapsed, last_emit)
%MAYBE_PROGRESS  At a plan mark, past the plan's time guards: ask Python
%   for the progress line (its text is Python's).
    if elapsed < plan.progress.start_delay || elapsed - last_emit < plan.progress.min_interval
        return;
    end
    last_emit = elapsed;
    py.scifor.matlab_loop.log_progress(plan.token, int64(c - 1), int64(state.completed), ...
        int64(state.skipped - state.no_data), int64(state.no_data), elapsed);
end


function args = limit_args(plan, c)
%LIMIT_ARGS  share_limits: each named input's [min max] at this combo as
%   trailing positional arguments ([] where its group has none), when the
%   plan says the function has room for them.
    args = {};
    n = numel(plan.limits.names);
    if n == 0 || ~plan.limits.inject
        return;
    end
    args = cell(1, n);
    for i = 1:n
        lo = plan.limit_values(c, i, 1);
        if isnan(lo)
            args{i} = [];
        else
            args{i} = [lo, plan.limit_values(c, i, 2)];
        end
    end
end


function result = call_fn(fn, call_args, n_outputs)
%CALL_FN  FN with as many outputs as the plan names.
    if n_outputs == 0
        fn(call_args{:});
        result = {};
    elseif n_outputs > 1
        fn_nargout = nargout(fn);
        if fn_nargout >= n_outputs || fn_nargout < 0
            result = cell(1, n_outputs);
            [result{1:n_outputs}] = fn(call_args{:});
        else
            % A single-output function returning a cell array to unpack.
            raw = fn(call_args{:});
            if iscell(raw) && numel(raw) >= n_outputs
                result = raw(1:n_outputs);
            else
                result = cell(1, n_outputs);
                result{1} = raw;
            end
        end
    else
        result = {fn(call_args{:})};
    end
end


function rows = slice_rows(plan, s, c)
%SLICE_ROWS  The rows (1-based) slice S reads at combo C.
    rows = plan.row_index(plan.row_offsets(s, c) + 1 : plan.row_offsets(s, c + 1));
end


function tbl = raw_table_of(v)
%RAW_TABLE_OF  The MATLAB table inside a table / Fixed / ColumnSelection spec.
    if istable(v)
        tbl = v;
    elseif isa(v, 'scifor.Fixed')
        tbl = raw_table_of(v.data);
    elseif isa(v, 'scifor.ColumnSelection')
        tbl = v.data;
    else
        error('scifor:for_each', 'expected a table input, got %s', class(v));
    end
end


function value = prepare_value(v, d, plan, c, metadata, opts)
%PREPARE_VALUE  One non-iterate input at combo C, as the plan delivers it.
    switch d.kind
        case 'data'
            if strcmp(d.shape, 'merge')
                value = prepare_merge(v, d, plan, c, metadata, opts);
            else
                value = prepare_data(v, d, plan, c, opts);
            end
        case 'path'
            if ~isempty(d.error_ids{c})
                error(d.error_ids{c}, '%s', d.error_messages{c});
            end
            if strcmp(d.value_class, 'string')
                value = string(d.paths{c});
            else
                value = char(d.paths{c});
            end
        case 'value'
            value = d.value;
        case 'pathinput_loader'
            value = load_with_loader(v, metadata, opts.pathinput_loader);
        otherwise
            value = v;
    end
end


function value = load_with_loader(v, metadata, loader)
%LOAD_WITH_LOADER  scidb's per-combo PathInput policy callback (the
%   database's key types; scifor stays policy-free). Fixed(PathInput) adds
%   its overrides to the combo's metadata first.
    meta = metadata;
    if isa(v, 'scifor.Fixed')
        f = fieldnames(v.fixed_metadata);
        for i = 1:numel(f)
            meta.(f{i}) = v.fixed_metadata.(f{i});
        end
        v = v.data;
    end
    meta_nv = reshape([fieldnames(meta)'; struct2cell(meta)'], 1, []);
    value = loader(v, meta_nv);
end


function result = prepare_data(v, d, plan, c, opts)
%PREPARE_DATA  A table input at combo C: the plan's rows, the bookkeeping
%   columns dropped, where=, then the delivery (selected columns, as_table,
%   or extraction). An iterate input stays a table.
    tbl = raw_table_of(v);
    if ~d.per_combo
        result = tbl;
        if ~strcmp(d.shape, 'iterate') && ~isempty(d.columns)
            result = apply_column_selection_on_table(tbl, string(as_cellstr(d.columns)));
        end
        return;
    end
    filtered = tbl(slice_rows(plan, d.slice, c), :);
    drop = as_cellstr(d.drop);
    if ~isempty(drop)
        filtered(:, drop) = [];
    end
    if ~isempty(opts.where)
        filtered = apply_where_filter(filtered, opts.where);
    end
    if height(filtered) == 0 && d.no_data_when_empty
        error('scifor:NoData', 'No data for this combo after filtering.');
    end
    if strcmp(d.shape, 'iterate')
        result = filtered;
        return;
    end
    if ~isempty(d.columns)
        cols = string(as_cellstr(d.columns));
        if d.as_table
            schema_cols = intersect(string(filtered.Properties.VariableNames), ...
                plan.filter_keys, 'stable');
            result = filtered(:, cellstr([schema_cols(:)', cols(:)']));
        else
            result = apply_column_selection_on_table(filtered, cols);
        end
        return;
    end
    result = extract_data(filtered, plan.filter_keys, d.as_table, string(as_cellstr(d.mapping)));
end


function result = prepare_merge(merge_spec, d, plan, c, metadata, opts)
%PREPARE_MERGE  Each constituent's rows (from the plan), then THE Merge rule
%   (scifor.merge_tables; Python owner scifor.merge, cases in
%   docs/claude/merge-cases.json): constant schema-key columns drop, shared
%   varying keys join, a shared data column or an unmatched key is an error.
%   Then where=, then the schema columns the merge lost are added back.
    schema_keys = plan.filter_keys;
    parts_d = as_cell(d.constituents);
    n = numel(merge_spec.tables);
    parts = cell(1, n);
    drop = false(1, n);
    columns = cell(1, n);
    for i = 1:n
        pd = parts_d{i};
        tbl = raw_table_of(merge_spec.tables{i});
        if pd.per_combo
            parts{i} = tbl(slice_rows(plan, pd.slice, c), :);
            if height(parts{i}) == 0
                % A sparse grid, as for any other per-combo input: an
                % expected skip (Python: NoDataError).
                error('scifor:NoData', 'No data for this combo in merge[%d].', i - 1);
            end
            drop(i) = true;
        else
            parts{i} = tbl;
        end
        columns{i} = string(as_cellstr(pd.columns));
    end

    [merged, dropped_const] = scifor.merge_tables(parts, schema_keys, ...
        'Drop', drop, 'Columns', columns);
    if ~isempty(opts.where)
        merged = apply_where_filter(merged, opts.where);
    end
    if height(merged) == 0
        error('scifor:NoData', 'No data for this combo after merge.');
    end

    nr = height(merged);
    merged_cols = string(merged.Properties.VariableNames);
    schema_tbl = table();
    for k = 1:numel(schema_keys)
        sk = char(schema_keys(k));
        if ismember(string(sk), merged_cols)
            continue;
        end
        if isfield(metadata, sk)
            val = metadata.(sk);
        elseif isfield(dropped_const, sk)
            val = dropped_const.(sk);
        else
            continue;
        end
        if isnumeric(val) && isscalar(val)
            schema_tbl.(sk) = repmat(val, nr, 1);
        elseif isstring(val) || ischar(val)
            schema_tbl.(sk) = repmat(string(val), nr, 1);
        else
            schema_tbl.(sk) = repmat({val}, nr, 1);
        end
    end
    result = [schema_tbl, merged];
end


function state = collect_result(state, result, metadata, metadata_str, c, plan, opts, slot_skips, produced, n_outputs)
%COLLECT_RESULT  Keep this combo's outputs: one row each, or (distribute)
%   one row per piece at the plan's distribute key. A for_columns group
%   skipped here is reported for the summary; a skipped split slot adds no
%   row to its output, which is how the gap reaches the bridge.
    if ~isempty(plan.distribute_key)
        state = collect_distributed(state, result, metadata, metadata_str, plan, opts, n_outputs);
        return;
    end
    skipped_slots = [];
    is_split = ~isempty(plan.for_columns) && strcmp(plan.for_columns.mode, 'split');
    for k = 1:numel(slot_skips)
        sk = slot_skips(k);
        if is_split
            state.output_skips{end+1} = struct('output', char(sk.name), 'combo', c, ...
                'reason', sk.reason, 'detail', sk.detail);
        else
            key = char(sk.name);
            if isKey(state.no_data_groups, key)
                state.no_data_groups(key) = [state.no_data_groups(key), c];
            else
                state.no_data_groups(key) = c;
            end
        end
        skipped_slots(end+1) = sk.slot; %#ok<AGROW>
    end
    for k = 1:numel(produced)
        g = char(produced(k).name);
        if isKey(state.group_columns, g)
            state.group_columns(g) = unique([state.group_columns(g), produced(k).columns], 'stable');
        else
            state.group_columns(g) = produced(k).columns;
        end
    end
    for o = 1:min(n_outputs, numel(result))
        if is_split && ismember(o, skipped_slots)
            continue
        end
        state.collected{o}{end+1} = {metadata, result{o}};
    end
end


function state = collect_distributed(state, result, metadata, metadata_str, plan, opts, n_outputs)
%COLLECT_DISTRIBUTED  Split each output into pieces (table rows, vector
%   elements, matrix rows); a returned distribute-key column addresses the
%   pieces, else 1-based ordinals.
    key = plan.distribute_key;
    for o = 1:min(n_outputs, numel(result))
        raw_value = result{o};
        try
            if istable(raw_value)
                if height(raw_value) == 1
                    raw_value = expand_single_row_for_distribute(raw_value);
                end
                data_tbl = raw_value;
                if ismember(key, raw_value.Properties.VariableNames)
                    dist_values = raw_value.(key);
                    data_tbl.(key) = [];
                    state.distribute_from_column = state.distribute_from_column + 1;
                else
                    dist_values = (1:height(raw_value))';
                end
                if ~opts.nest_table_outputs
                    overlap = intersect(fieldnames(metadata), data_tbl.Properties.VariableNames, 'stable');
                    if ~isempty(overlap)
                        data_tbl = removevars(data_tbl, overlap);
                    end
                end
                for r = 1:height(data_tbl)
                    dist_meta = metadata;
                    dist_meta.(key) = dist_values(r);
                    state.collected{o}{end+1} = {dist_meta, data_tbl(r, :)};
                end
            else
                pieces = split_for_distribute(raw_value);
                for k = 1:numel(pieces)
                    dist_meta = metadata;
                    dist_meta.(key) = k;
                    state.collected{o}{end+1} = {dist_meta, pieces{k}};
                end
            end
        catch err2
            scifor.Log.warn('%s: cannot distribute output %d: %s', metadata_str, o, err2.message);
        end
    end
end


% =========================================================================
% Failures (MATLAB keeps the MException; Python writes the summary)
% =========================================================================

function f = new_failures()
    f = struct('list', {{}}, ...
        'index', containers.Map('KeyType', 'char', 'ValueType', 'double'), ...
        'reported_ids', containers.Map('KeyType', 'char', 'ValueType', 'logical'));
end


function [state, k] = record_iteration_failure(state, err, c, metadata_str, context, fn_name, stage)
%RECORD_ITERATION_FAILURE  Keep one failure record per reason (the first,
%   from scifor.failure_from_mexception) and the combos that hit it; the
%   summary is Python's (assemble). Every failure logs a [skip] line at
%   DEBUG; the first of each reason a WARN, except scifor:NoData (an
%   expected outcome). The MATLAB error report rides only on the FIRST
%   reason of each identifier: messages usually embed a value (a path), so
%   every combo would be its own reason, and 533 missing files once wrote
%   ~11,200 report lines (2026-09-13).
    reason = sprintf('%s: %s', err.identifier, err.message);
    is_no_data = strcmp(err.identifier, 'scifor:NoData');
    if is_no_data
        state.no_data = state.no_data + 1;
    end
    failures = state.failures;
    if isKey(failures.index, reason)
        k = failures.index(reason);
        failures.list{k}.combos(end+1) = c;
    else
        record = scifor.failure_from_mexception(err, fn_name, metadata_str, stage, is_no_data);
        failures.list{end+1} = struct('record', record, 'combos', c);
        k = numel(failures.list);
        failures.index(reason) = k;
        if ~is_no_data
            where = describe_user_frame(record.user_frame);
            id_key = ['id:' err.identifier];
            if isKey(failures.reported_ids, id_key)
                scifor.Log.warn('iteration failed: %s — %s: %s [%s]', ...
                    metadata_str, context, err.message, where);
            else
                failures.reported_ids(id_key) = true;
                scifor.Log.warn(['iteration failed: %s — %s: %s [%s] ' ...
                    '(first %s; report follows)\n%s'], ...
                    metadata_str, context, err.message, where, err.identifier, ...
                    getReport(err, 'extended', 'hyperlinks', 'off'));
            end
        end
    end
    state.failures = failures;
    scifor.Log.debug('[skip] %s: %s: %s', metadata_str, context, err.message);
end


function [stop_err, state] = note_stop(on_error, stop_err, err, state, k)
%NOTE_STOP  on_error='stop': the first genuine failure ends the loop.
    was_empty = isempty(stop_err);
    stop_err = stop_error_after(on_error, stop_err, err);
    if was_empty && ~isempty(stop_err)
        state.stop_group = k;
    end
end


% =========================================================================
% Assemble (Python) and the result tables (MATLAB values)
% =========================================================================

function summary = assemble_run(plan, state, opts, stop_err)
%ASSEMBLE_RUN  Report what happened; Python logs the summary and returns
%   the summary_fn struct and the combine column list.
    outcome = struct( ...
        'completed', state.completed, ...
        'skipped', state.skipped, ...
        'elapsed', state.elapsed, ...
        'failures', {state.failures.list}, ...
        'stop', [], ...
        'output_skips', {state.output_skips}, ...
        'group_columns', {map_to_entries(state.group_columns, 'columns', true)}, ...
        'no_data_groups', {map_to_entries(state.no_data_groups, 'combos', false)}, ...
        'distribute_from_column', state.distribute_from_column);
    if state.stop_group > 0
        outcome.stop = struct('group', state.stop_group);
    end
    out = jsondecode(char(py.scifor.matlab_loop.assemble(plan.token, jsonencode(outcome))));
    summary.combine_columns = as_cellstr(out.combine_columns);
    if ~isempty(opts.summary_fn)
        s = out.summary;
        s.failures = as_cell(s.failures);
        s.stop_error = stop_err;
        opts.summary_fn(s);
    end
end


function entries = map_to_entries(m, field, as_text)
%MAP_TO_ENTRIES  containers.Map -> {struct(name, FIELD)} (group names need
%   not be valid struct fields).
    entries = {};
    ks = keys(m);
    for i = 1:numel(ks)
        v = m(ks{i});
        if as_text
            v = cellstr(string(v));
        end
        entries{end+1} = struct('name', ks{i}, field, {v}); %#ok<AGROW>
    end
end


function out = output_tables(collected, plan, opts, fn_name, n_return)
%OUTPUT_TABLES  One table per output (MATLAB values, the plan's types).
    n_outputs = numel(plan.output_names);
    tables = cell(1, n_outputs);
    for o = 1:n_outputs
        tables{o} = build_single_output_table(collected{o}, plan.output_names{o}, ...
            opts.categorical, plan.filter_keys, opts.nest_table_outputs, plan.column_types, ...
            plan.distribute_key, plan.distribute_top, fn_name);
    end
    out = repmat({table()}, 1, n_return);
    n_shared = min(n_outputs, n_return);
    out(1:n_shared) = tables(1:n_shared);
end


% =========================================================================
% Values: what only MATLAB can do with MATLAB values (extraction, joins,
% for_columns calls, distribute splitting, result tables, option types)
% =========================================================================

function [result, slot_skips, produced] = run_column_iteration(fn, base_args, iterate_pos, iterate_tables, column_groups, mode, schema_keys, iter_as_table, metadata_str)
%RUN_COLUMN_ITERATION  Run fn once per column group at one combination (port
%   of Python scifor's _run_column_iteration; the two must agree).
%
%   COLUMN_GROUPS is a struct array (name, columns) from the plan
%   (Python's resolve_column_groups); a plain for_columns list is one single-column
%   group per column, named after it. For each group, every iterate input is
%   sliced to the group's columns and passed positionally; non-iterate
%   inputs/constants in BASE_ARGS pass through unchanged. A one-column group
%   arrives as that column's values (unwrapped from a scalar cell); several
%   columns arrive as a sub-table. An iterate input with ITER_AS_TABLE(p) set
%   (the plan's as_table) arrives as a table of all schema key columns plus
%   the group's columns.
%   Deferred ColName() and {ColName} tokens resolve to the group's name.
%
%   No data: a group whose every value is missing in every iterate input
%   (GROUP_HAS_NO_DATA) is not called; it is listed in SLOT_SKIPS.
%
%   'combine' (one output): RESULT = {1-row table}, each group's return
%   expanded by EXPAND_COLUMN_RESULT under the group's name; PRODUCED lists
%   the names each group gave. A raising group fails the combination.
%   'split' (several outputs): RESULT has one slot per group; a group with
%   no data or one fn raised on is listed in SLOT_SKIPS (its slot is left
%   empty and the caller adds no row for it). Every group skipped -> the
%   first error is rethrown, or scifor:NoData when none raised.
    sep = '__';
    n = numel(column_groups);
    iter_positions = find(iterate_pos);
    is_split = strcmp(mode, 'split');
    % Constant positions holding a deferred ColName() marker resolve to the name
    % of the group currently being iterated (recomputed each pass below).
    deferred_colname_positions = [];
    % Char/string constants carrying a literal {ColName} token (pre-resolved
    % PathOutput paths keep the token for per-group substitution here).
    colname_token_positions = [];
    for k = 1:numel(base_args)
        if isa(base_args{k}, 'scifor.ColName') && base_args{k}.is_deferred()
            deferred_colname_positions(end+1) = k; %#ok<AGROW>
        elseif (ischar(base_args{k}) || isstring(base_args{k})) && ...
                contains(string(base_args{k}), "{ColName}")
            colname_token_positions(end+1) = k; %#ok<AGROW>
        end
    end
    out_names = {};
    out_values = {};
    slots = cell(1, n);
    slot_skips = struct('slot', {}, 'name', {}, 'reason', {}, 'detail', {});
    produced = struct('name', {}, 'columns', {});
    first_err = [];
    for gi = 1:n
        gname = char(column_groups(gi).name);
        gcols = column_groups(gi).columns;
        no_data = true;
        for qi = 1:numel(iter_positions)
            if ~scifor.has_no_data(iterate_tables{iter_positions(qi)}, gcols)
                no_data = false;
                break
            end
        end
        if no_data
            scifor.Log.debug(['[for_columns] %s: group %s has no data (all %d ' ...
                'column(s) missing) — not called'], metadata_str, gname, numel(gcols));
            slot_skips(end+1) = struct('slot', gi, 'name', string(gname), ...
                'reason', 'no_data', 'detail', ''); %#ok<AGROW>
            continue
        end
        call_args = base_args;
        for di = 1:numel(deferred_colname_positions)
            call_args{deferred_colname_positions(di)} = gname;
        end
        for di = 1:numel(colname_token_positions)
            k_tok = colname_token_positions(di);
            call_args{k_tok} = strrep( ...
                char(string(base_args{k_tok})), '{ColName}', gname);
        end
        for qi = 1:numel(iter_positions)
            p = iter_positions(qi);
            t = iterate_tables{p};
            if iter_as_table(p)
                % Keep all schema key columns alongside the group's columns.
                schema_cols = intersect( ...
                    string(t.Properties.VariableNames), schema_keys, 'stable');
                keep_cols = [schema_cols(:)', gcols(:)'];
                call_args{p} = t(:, cellstr(keep_cols));
            elseif isscalar(gcols)
                vals = t.(char(gcols));
                if iscell(vals) && isscalar(vals)
                    vals = vals{1};
                end
                call_args{p} = vals;
            else
                call_args{p} = t(:, cellstr(gcols));
            end
        end
        if is_split
            try
                slots{gi} = fn(call_args{:});
            catch err
                scifor.Log.warn('[for_columns] %s: %s not saved — %s', ...
                    metadata_str, gname, err.message);
                slot_skips(end+1) = struct('slot', gi, 'name', string(gname), ...
                    'reason', 'failed', 'detail', err.message); %#ok<AGROW>
                if isempty(first_err)
                    first_err = err;
                end
            end
            continue
        end
        res = fn(call_args{:});
        % for_columns: res is a plain value. Upstream provenance is recorded
        % at save time from input record_ids (no per-call lineage wrapper).
        [names_i, values_i] = expand_column_result(gname, res, sep);
        for k = 1:numel(names_i)
            if any(strcmp(names_i{k}, out_names))
                error('scifor:for_each:forColumnsDuplicate', ...
                    ['for_columns produced a duplicate output column ''%s''. ' ...
                     'Per-column output keys must be unique after prefixing ' ...
                     'with the source column name (separator ''%s'').'], ...
                    names_i{k}, sep);
            end
            out_names{end+1} = names_i{k}; %#ok<AGROW>
            out_values{end+1} = values_i{k}; %#ok<AGROW>
        end
        produced(end+1) = struct('name', string(gname), ...
            'columns', string(names_i)); %#ok<AGROW>
    end
    if numel(slot_skips) == n
        if ~isempty(first_err)
            rethrow(first_err);
        end
        error('scifor:NoData', ...
            'No data in any for_columns group (%s).', strjoin([slot_skips.name], ', '));
    end
    if is_split
        result = slots;
    else
        result = {cell2table(out_values, 'VariableNames', out_names)};
    end
end



function templates = missing_templates(rows)
%MISSING_TEMPLATES  Column name -> the missing value of that column's type,
%   from the first combination that produced it.
    templates = containers.Map('KeyType', 'char', 'ValueType', 'any');
    for i = 1:numel(rows)
        tbl = rows{i}{2};
        for v = string(tbl.Properties.VariableNames)
            if ~isKey(templates, char(v))
                templates(char(v)) = missing_like(tbl.(char(v)));
            end
        end
    end
end



function m = missing_like(v)
    if isstring(v)
        m = string(missing);
    elseif isdatetime(v)
        m = NaT;
    elseif iscategorical(v)
        m = categorical(missing);
    elseif iscell(v)
        m = {[]};
    else
        m = NaN;
    end
end



function tbl = reindex_combine_row(tbl, expected, templates)
%REINDEX_COMBINE_ROW  One combination's 1-row table with every expected
%   column, in order; a column it lacks is filled with its type's missing.
    have = string(tbl.Properties.VariableNames);
    for name = expected
        if ~any(have == name)
            if isKey(templates, char(name))
                tbl.(char(name)) = templates(char(name));
            else
                tbl.(char(name)) = NaN;
            end
        end
    end
    tbl = tbl(:, cellstr(expected));
end



function [names, values] = expand_column_result(col, res, sep)
%EXPAND_COLUMN_RESULT  Expand one source column's return into name/value pairs.
%   scalar (or any non-struct / non-table value) -> { col }
%   struct                -> one pair per field,    "<col><sep><field>"
%   1-row table           -> one pair per variable, "<col><sep><variable>"
%   A multi-row table is rejected: for_columns reassembles to one row per combo.
    if isstruct(res) && isscalar(res)
        fns = fieldnames(res);
        names = cell(1, numel(fns));
        values = cell(1, numel(fns));
        for i = 1:numel(fns)
            names{i} = sprintf('%s%s%s', col, sep, fns{i});
            values{i} = res.(fns{i});
        end
    elseif istable(res)
        if height(res) ~= 1
            error('scifor:for_each:forColumnsBadReturn', ...
                ['for_columns function returned a %d-row table for column ' ...
                 '''%s''; expected a single row (one value per output key). ' ...
                 'Return a scalar, struct, or 1-row table.'], height(res), col);
        end
        vns = string(res.Properties.VariableNames);
        names = cell(1, numel(vns));
        values = cell(1, numel(vns));
        for i = 1:numel(vns)
            names{i} = sprintf('%s%s%s', col, sep, char(vns(i)));
            v = res.(char(vns(i)));
            if iscell(v) && isscalar(v)
                v = v{1};
            end
            values{i} = v;
        end
    else
        names = {col};
        values = {res};
    end
end



function result = extract_data(tbl, schema_keys, as_table, mapping_cols)
%EXTRACT_DATA  Extract data from a filtered table.
%   If as_table: return full table (including schema columns).
%   Otherwise: drop schema key columns that are all-identical (constant
%   within this combo), keep schema keys that still vary. If the result
%   has one data column and one row, extract the scalar value. If multiple
%   columns remain, return as a table.
%
%   MAPPING_COLS (optional, see +scidb/for_each.m's mapping_inputs) says the
%   rows are struct records spread one column per field. A single row is then
%   rebuilt into that struct -- the value that was saved. Without it a
%   struct-valued variable came back as a 1xN table, because the one-row
%   unwrap below only fires for a SINGLE data column and a struct has as many
%   columns as fields; every field access then yielded a 1x1 cell.
    if nargin < 4
        mapping_cols = string.empty;
    end
    if as_table
        result = tbl;
        return;
    end

    if ~isempty(mapping_cols) && height(tbl) == 1
        present = mapping_cols(ismember(mapping_cols, ...
            string(tbl.Properties.VariableNames)));
        if ~isempty(present)
            result = struct();
            for mi = 1:numel(present)
                col = char(present(mi));
                val = tbl.(col);
                if iscell(val)
                    val = val{1};
                elseif isnumeric(val) && isrow(val) && ~isscalar(val)
                    % Same shape hazard the single-column branch below
                    % documents: when several records were loaded together,
                    % from_python's try_stack_numeric stacks their vectors as
                    % ROWS of a matrix, so filtering to one row yields 1xN
                    % where the saved value was Nx1. Coerce to a column so a
                    % field holds the same shape it was saved with,
                    % regardless of how many records shared the load.
                    val = val(:);
                end
                % The column name is already a valid MATLAB identifier (it
                % came from a struct field on the way in), but a Python-saved
                % dict key need not be -- makeValidName mirrors what
                % scidb.internal.pydict_to_struct does with the same keys.
                result.(matlab.lang.makeValidName(col)) = val;
            end
            return;
        end
        % Fall through: nothing upstream left any of the declared columns, so
        % treat it as an ordinary table rather than returning an empty struct.
    end

    % Drop schema key columns that are all-identical in the filtered rows
    col_names = string(tbl.Properties.VariableNames);
    keep_cols = setdiff(col_names, schema_keys, 'stable');

    if height(tbl) == 1 && numel(keep_cols) == 1
        % Extract scalar value
        val = tbl.(char(keep_cols(1)));
        if iscell(val)
            result = val{1};
        else
            result = val;
            % When the source DataFrame held multiple records of vector
            % data, from_python's try_stack_numeric stacks them as rows
            % of a matrix (M-record table, each row 1×N). Filtering to
            % one row yields a 1×N row vector — but the cell-extraction
            % path for a single-record case gives an N×1 column. Coerce
            % the row vector to a column so per-record extraction is
            % shape-consistent regardless of how many records were
            % loaded together.
            if isnumeric(result) && isrow(result) && ~isscalar(result)
                result = result(:);
            end
        end
    elseif ~isempty(keep_cols) && numel(keep_cols) < numel(col_names)
        sub_tbl = tbl(:, cellstr(keep_cols));
        if numel(keep_cols) == 1
            % Single data column: return as array (vector)
            if isnumeric(sub_tbl.(char(keep_cols(1))))
                result = table2array(sub_tbl);
            else
                result = sub_tbl;
            end
        else
            % Multiple columns remain: return as table
            result = sub_tbl;
        end
    else
        result = tbl;
    end
end



function result = apply_column_selection_on_table(tbl, cols)
%APPLY_COLUMN_SELECTION_ON_TABLE  Extract selected columns from a table.
%   Single column -> returns column values as array.
%   Multiple columns -> returns sub-table.
    for ci = 1:numel(cols)
        if ~ismember(cols(ci), tbl.Properties.VariableNames)
            error('scifor:for_each', ...
                'Column ''%s'' not found. Available columns: %s', ...
                cols(ci), strjoin(tbl.Properties.VariableNames, ', '));
        end
    end

    if numel(cols) == 1
        result = tbl.(char(cols(1)));
        % Scalar cell extraction (consistent with extract_data)
        if height(tbl) == 1 && iscell(result) && isscalar(result)
            result = result{1};
        end
    else
        result = tbl(:, cellstr(cols));
    end
end



function filtered = apply_where_filter(tbl, where_filter)
%APPLY_WHERE_FILTER  Apply a scifor.ColFilter to table rows.
    mask = where_filter.apply(tbl);
    filtered = tbl(mask, :);
end



function tbl = restore_schema_column_types(tbl, col_types)
%RESTORE_SCHEMA_COLUMN_TYPES  Cast metadata columns back to the input types
%   in the plan's column_types (Python: the first input table's class per
%   key, or a conflict). Keys without a captured type (explicit iterables
%   with no table column) keep the iterable's own type.
%   A cast that cannot be performed losslessly leaves the column unchanged
%   and warns, so identity errors are visible rather than silent.
    keys = fieldnames(col_types);
    tbl_cols = tbl.Properties.VariableNames;
    for i = 1:numel(keys)
        key = keys{i};
        if ~ismember(key, tbl_cols)
            continue;
        end
        info = col_types.(key);
        if info.conflict
            scifor.Log.warn(['schema key ''%s'': input tables disagree on ' ...
                'column type — leaving the output column as %s'], ...
                key, class(tbl.(key)));
            continue;
        end
        col = tbl.(key);
        if strcmp(class(col), info.class)
            continue;
        end
        try
            tbl.(key) = cast_schema_column(col, info);
            scifor.Log.debug(['schema key ''%s'': output column restored ' ...
                'to input type %s'], key, info.class);
        catch err
            scifor.Log.warn(['schema key ''%s'': cannot restore input ' ...
                'column type %s (%s) — leaving as %s'], ...
                key, info.class, err.message, class(col));
        end
    end
end



function out = cast_schema_column(col, info)
%CAST_SCHEMA_COLUMN  Cast one metadata column to a captured input type.
%   Errors when the cast would not be lossless; the caller warns and keeps
%   the column unchanged.
    target = info.class;
    numeric_classes = {'double', 'single', 'int8', 'int16', 'int32', ...
        'int64', 'uint8', 'uint16', 'uint32', 'uint64'};
    switch target
        case 'categorical'
            str_vals = string(col);
            if ~isempty(info.categories) && ...
                    all(ismember(str_vals, string(info.categories)))
                % All values belong to the input's category set: keep the
                % input's category order and ordinality.
                out = categorical(str_vals, info.categories, ...
                    'Ordinal', info.ordinal);
            else
                out = categorical(col);
            end
        case 'string'
            out = string(col);
        case 'cell'
            out = cellstr(string(col));
        case 'char'
            out = char(string(col));
        case 'logical'
            out = logical(col);
        case numeric_classes
            if isnumeric(col)
                out = cast(col, target);
            else
                str_vals = string(col);
                nums = str2double(str_vals);
                if any(isnan(nums)) || ~all(string(nums) == str_vals)
                    error('scifor:for_each', ...
                        'values are not canonical numeric spellings');
                end
                out = cast(nums, target);
            end
        otherwise
            error('scifor:for_each', 'unsupported column type');
    end
end



function s = collapse_struct_array(arr)
%COLLAPSE_STRUCT_ARRAY  Convert a non-scalar struct array back to a scalar struct.
%   When users write struct('a', tbl, 'b', {'x','y'}), MATLAB creates a
%   struct array instead of a scalar struct with cell fields. This undoes
%   that by collecting each field's values into a cell array, or unwrapping
%   if all elements are identical (i.e. the field was a replicated scalar).
    fnames = fieldnames(arr);
    s = struct();
    for i = 1:numel(fnames)
        vals = {arr.(fnames{i})};
        all_same = true;
        for j = 2:numel(vals)
            if ~isequal(vals{1}, vals{j})
                all_same = false;
                break;
            end
        end
        if all_same
            s.(fnames{i}) = vals{1};
        else
            s.(fnames{i}) = vals;
        end
    end
end



function tbl = build_single_output_table(collected, output_name, categorical_flag, schema_keys, nest_table_outputs, col_types, distribute_key, distribute_key_synthetic, fn_name)
%BUILD_SINGLE_OUTPUT_TABLE  Build one result table for a single output.
%
%   collected - cell array of {metadata_struct, value} pairs for one output
%   output_name - column name for non-table values (e.g., 'output')
%   categorical_flag - if true, convert metadata columns to categorical
%   schema_keys - string array of schema keys (for sort order)
%   nest_table_outputs - if true, force nested mode even for table outputs
%   col_types - the plan's column_types: per-key input column type to
%       restore on the output metadata columns
%   distribute_key - name of the distribute target key, or '' if not
%       distributing
%   distribute_key_synthetic - true when distribute_key was defaulted to
%       the top of the schema with no source table/iterable to carry a
%       type (the plan's distribute_top: nothing at a schema level is
%       iterated); the column is
%       cast to categorical rather than left as a bare double index
%
%   If all values are tables and nest_table_outputs is false →
%       flatten mode (metadata + data columns).
%   Otherwise → nested mode (metadata + single data column).

    if nargin < 5
        nest_table_outputs = false;
    end
    if nargin < 6
        col_types = struct();
    end
    if nargin < 7
        distribute_key = '';
    end
    if nargin < 8
        distribute_key_synthetic = false;
    end
    if nargin < 9
        fn_name = 'the function';
    end

    n_rows = numel(collected);

    if n_rows == 0
        tbl = table();
        return;
    end

    % Check whether all values for this output are tables (flatten mode)
    all_tables = true;
    for r = 1:n_rows
        if ~istable(collected{r}{2})
            all_tables = false;
            break;
        end
    end

    if all_tables && ~nest_table_outputs
        % Flatten mode: metadata columns + original table columns
        parts = cell(n_rows, 1);
        for r = 1:n_rows
            metadata = collected{r}{1};
            data_tbl = collected{r}{2};
            nr = height(data_tbl);

            % Validate no column name conflicts between metadata and data
            meta_fields = fieldnames(metadata);
            data_col_names = data_tbl.Properties.VariableNames;
            for f = 1:numel(meta_fields)
                if ismember(meta_fields{f}, data_col_names)
                    error('scifor:for_each', ...
                        'Table output has column ''%s'' which conflicts with metadata key ''%s''. Rename the column to avoid the conflict.', ...
                        meta_fields{f}, meta_fields{f});
                end
            end

            % Build metadata table with one replicated row per data row
            meta_tbl = table();
            for f = 1:numel(meta_fields)
                val = metadata.(meta_fields{f});
                if isnumeric(val) && isscalar(val)
                    meta_tbl.(meta_fields{f}) = repmat(val, nr, 1);
                elseif ischar(val) || (isstring(val) && isscalar(val))
                    meta_tbl.(meta_fields{f}) = repmat(string(val), nr, 1);
                else
                    meta_tbl.(meta_fields{f}) = repmat({val}, nr, 1);
                end
            end
            parts{r} = [meta_tbl, data_tbl];
        end
        % Stacking every combo's table is where a function that returns
        % different column types for different combos fails -- inside
        % SciStack, but fixable by the user. Say so (scistack:userValue),
        % naming the column and the combos, instead of a bare vertcat error.
        try
            tbl = vertcat(parts{:});
        catch vcat_err
            throw(user_value_error(vcat_err, 'combine_outputs', fn_name, ...
                output_name, describe_table_kinds(collected, fn_name, output_name)));
        end
    else
        % Nested mode: metadata columns + single output column
        tbl = table();
        meta_fields = fieldnames(collected{1}{1});

        % Metadata columns
        for f = 1:numel(meta_fields)
            col_data = cell(n_rows, 1);
            for r = 1:n_rows
                metadata = collected{r}{1};
                if isfield(metadata, meta_fields{f})
                    col_data{r} = metadata.(meta_fields{f});
                else
                    col_data{r} = {missing};
                end
            end
            tbl.(meta_fields{f}) = scidb.internal.normalize_cell_column(col_data);
        end

        % Single output column
        col_data = cell(n_rows, 1);
        for r = 1:n_rows
            col_data{r} = collected{r}{2};
        end
        tbl.(output_name) = scidb.internal.normalize_cell_column(col_data);
    end

    % Round-trip metadata column types: cast each metadata column back to
    % the exact type of the input table column it was resolved from.
    tbl = restore_schema_column_types(tbl, col_types);

    % A synthetic distribute key (no source table/iterable, so nothing was
    % captured above to restore) is cast to categorical here regardless of
    % categorical_flag — schema-key identity columns are categorical by
    % convention elsewhere in the framework, and a bare double index would
    % be the odd one out. Skipped when categorical_flag already converts
    % every metadata column below.
    if distribute_key_synthetic && ~categorical_flag && strlength(distribute_key) > 0
        dist_key_char = char(distribute_key);
        if ismember(dist_key_char, tbl.Properties.VariableNames)
            col = tbl.(dist_key_char);
            str_col = string(col);
            unique_vals = unique(str_col, 'stable');
            tbl.(dist_key_char) = categorical(str_col, unique_vals);
        end
    end

    % Sort by schema columns and convert to categorical if requested
    if categorical_flag
        tbl = scifor.categorize_metadata(tbl, meta_fields, schema_keys);
    end
end



function pieces = split_for_distribute(data)
%SPLIT_FOR_DISTRIBUTE  Split data into elements for distribute-style saving.
    if istable(data)
        pieces = cell(1, height(data));
        for i = 1:height(data)
            pieces{i} = data(i, :);
        end
    elseif isnumeric(data) || islogical(data)
        if isvector(data)
            pieces = cell(1, numel(data));
            for i = 1:numel(data)
                pieces{i} = data(i);
            end
        elseif ismatrix(data)
            pieces = cell(1, size(data, 1));
            for i = 1:size(data, 1)
                pieces{i} = data(i, :);
            end
        else
            error('scifor:for_each', ...
                'distribute does not support arrays with %d dimensions.', ndims(data));
        end
    elseif iscell(data)
        pieces = data(:)';
    elseif isstring(data)
        pieces = cellstr(data)';
    else
        error('scifor:for_each', ...
            'distribute does not support type %s.', class(data));
    end
end



function expanded = expand_single_row_for_distribute(tbl)
%EXPAND_SINGLE_ROW_FOR_DISTRIBUTE  Expand a height-1 table for distribute.
%   Cell columns containing multi-element arrays are expanded into rows.
%   The target expansion length is the most common length among expandable
%   columns. Columns that match the target length are expanded in-place.
%   All other columns (scalars, mismatched-length cells) are replicated
%   to preserve a consistent column set across iterations.

    col_names = tbl.Properties.VariableNames;
    n_cols = numel(col_names);

    expandable = false(1, n_cols);
    lengths = zeros(1, n_cols);

    for i = 1:n_cols
        val = tbl.(col_names{i});
        if iscell(val) && isscalar(val)
            inner = val{1};
            if numel(inner) > 1
                expandable(i) = true;
                lengths(i) = numel(inner);
            end
        end
    end

    if ~any(expandable)
        % Nothing to expand — return as-is
        expanded = tbl;
        return;
    end

    target_len = mode(lengths(expandable));

    expanded = table();
    for i = 1:n_cols
        if expandable(i) && lengths(i) == target_len
            % Expand matching cell-array columns into individual rows
            inner = tbl.(col_names{i}){1};
            expanded.(col_names{i}) = inner(:);
        else
            % Replicate scalar and mismatched-length columns
            expanded.(col_names{i}) = repmat(tbl.(col_names{i}), target_len, 1);
        end
    end
end



function [meta_args, opts] = split_options(varargin)
%SPLIT_OPTIONS  Separate known option flags from metadata name-value pairs.
    opts.dry_run = false;
    opts.as_table = string.empty;
    opts.distribute = false;
    opts.where = [];
    opts.categorical = false;
    opts.outputs = {};
    opts.all_combos = [];
    opts.row_selection = {};
    opts.record_id_column = "";
    opts.nest_table_outputs = false;
    opts.resolve_pathinput = true;
    opts.pathinput_loader = [];  % optional loader(pi, meta_nv) callback; the
                                 % scidb layer injects its schema-key-type
                                 % policy here so scifor stays policy-free
    opts.summary_fn = [];  % optional summary_fn(struct) called once with the
                           % end-of-loop counts; scidb injects its run tally
    opts.on_error = 'continue';  % 'continue' | 'stop' (Python owns the values)
    opts.defer_stop = false;     % scidb: return, don't rethrow; it saves first
    opts.resolved_path_outputs = struct();
    opts.share_limits = struct();
    opts.schema_keys = string.empty;
    opts.schema_key_types = struct();  % key -> 'numeric' | 'string' (standalone)
    opts.mapping_inputs = struct();  % {param -> string array of data column
                                     % names} for struct-valued records; see
                                     % extract_data
    opts.locations = '';   % JSON text; see the locations= doc above
    opts.project_root = "";  % "" = the pinned root (see project_root= above)
    opts.discarded = {};    % "name=<value>" of each scidb-only option seen

    % Which names are options (and which scidb options this layer discards)
    % is Python's call: scifor.option_pairs ->
    % scifor.options.classify_for_each_args. This function only turns each
    % value into the MATLAB type the loop reads.
    [names, values, roles] = scifor.option_pairs(varargin, 'scifor');
    meta_args = {};
    for k = 1:numel(names)
        val = values{k};
        switch roles{k}
            case 'axis'
                meta_args(end+1:end+2) = {names{k}, val}; %#ok<AGROW>
                continue;
            case 'discard'
                opts.discarded{end+1} = sprintf('%s=%s', names{k}, short_value(val));
                continue;
        end
        switch string(names{k})
            case "dry_run"
                opts.dry_run = logical(val);
            case "locations"
                if isstruct(val)
                    val = jsonencode(val);
                end
                opts.locations = char(string(val));
            case "as_table"
                if islogical(val) || isstring(val)
                    opts.as_table = val;
                elseif ischar(val) || iscell(val)
                    opts.as_table = string(val);
                end
            case "distribute"
                opts.distribute = logical(val);
            case "categorical"
                opts.categorical = logical(val);
            case "where"
                opts.where = val;
            case "project_root"
                opts.project_root = string(val);
            case "outputs"
                if isnumeric(val) && isscalar(val)
                    opts.outputs = val;
                elseif isstring(val) || ischar(val)
                    opts.outputs = cellstr(val);
                elseif iscell(val)
                    opts.outputs = val;
                elseif isobject(val)
                    % One object, or an object array: one output each.
                    opts.outputs = num2cell(val);
                end
            case "_all_combos"
                % scidb's py.list passes to the plan untouched (a cell
                % of structs from a direct caller is converted once).
                opts.all_combos = val;
            case "_row_selection"
                % Per-combo record ids (py.list of dicts, aligned with
                % _all_combos). The plan turns them into row indices --
                % the rule Python's own loop applies through the
                % LoopHooks.select_rows hook.
                opts.row_selection = val;
            case "_record_id_column"
                opts.record_id_column = string(val);
            case "_nest_table_outputs"
                opts.nest_table_outputs = logical(val);
            case "_resolve_pathinput"
                opts.resolve_pathinput = logical(val);
            case "_pathinput_loader"
                opts.pathinput_loader = val;
            case "on_error"
                opts.on_error = char(string(val));
                % The one validation: Python's scifor.validate_on_error.
                py.scifor.validate_on_error(opts.on_error);
            case "_defer_stop"
                opts.defer_stop = logical(val);
            case "_summary_fn"
                opts.summary_fn = val;
            case "_mapping_inputs"
                % {param -> string array of data column names} declaring
                % that the input's rows are struct records spread one
                % column per field, not the rows of a table. scifor
                % cannot infer this (a 1xN table looks identical), so
                % scidb states it -- see +scidb/for_each.m.
                opts.mapping_inputs = val;
            case "_resolved_path_outputs"
                % py.dict {param -> [path per combo]}, aligned with
                % _all_combos: scidb resolves PathOutput templates (incl.
                % branch_param placeholders whose dotted names cannot be
                % MATLAB struct fields); the plan keeps each kept combo's.
                opts.resolved_path_outputs = val;
            case "share_limits"
                % struct: input name -> string array of schema keys to
                % hold fixed. Python groups (_compute_shared_limits) the
                % per-row extents MATLAB measures (row_extents).
                opts.share_limits = val;
            case "schema_keys"
                % Schema key names to iterate — structural sugar,
                % expanded by Python (scifor.expand_schema_keys).
                opts.schema_keys = string(val);
            case "schema_key_types"
                % struct: key -> 'numeric' | 'string'. Validated by
                % Python (scifor.schema_types.validate_key_types).
                opts.schema_key_types = val;
                py.scifor.schema_types.validate_key_types( ...
                    scifor.key_types_to_py(opts.schema_key_types));
            otherwise
                if strcmp(roles{k}, 'option')
                    % Python declares an option this file cannot read yet.
                    error('scifor:for_each', ['for_each option "%s" has no ' ...
                        'MATLAB handling in +scifor/for_each.m split_options.'], names{k});
                end
                % An unknown leading-underscore name stays an iteration
                % axis, as it always has.
                meta_args(end+1:end+2) = {names{k}, val}; %#ok<AGROW>
        end
    end
end




function s = short_value(val)
%SHORT_VALUE  Compact text for a discarded option's value in the log line.
    if (isnumeric(val) || islogical(val)) && isscalar(val)
        s = char(string(val));
    elseif (ischar(val) || isstring(val)) && strlength(string(val)) <= 40
        s = char(string(val));
    else
        s = sprintf('<%s>', class(val));
    end
end



function s = format_value(val)
%FORMAT_VALUE  Format a constant value for display.
    if isnumeric(val)
        s = sprintf('%g', val);
    elseif islogical(val)
        if val
            s = 'true';
        else
            s = 'false';
        end
    elseif ischar(val) || isstring(val)
        s = sprintf('''%s''', string(val));
    elseif istable(val)
        s = sprintf('<table %dx%d>', height(val), width(val));
    else
        try
            s = mat2str(val);
        catch
            s = sprintf('<%s>', class(val));
        end
    end
end



function new_err = user_value_error(err, stage, fn_name, output, precise)
%USER_VALUE_ERROR  An error from a SciStack step that handles the user's
%   values, as the user-fixable error it most likely is. The MATLAB twin of
%   Python's scifor.failures.as_user_value_error:
%
%   PRECISE non-empty -- a check named the cause: scistack:userValue.
%   PRECISE empty     -- no check did: scistack:userValue:probable, hedged
%                        wording, and a [seam_fallback] WARN (give this cause
%                        a check, and a regression test).
%
%   The original error is attached as the cause, so its report survives.
    if ~isempty(precise)
        new_err = MException('scistack:userValue', '%s', precise);
    else
        new_err = MException('scistack:userValue:probable', ...
            ['SciStack couldn''t combine the output ''%s'' of %s: %s. This ' ...
             'usually means the function returns different kinds, shapes or ' ...
             'columns for different combos.'], char(output), char(fn_name), err.message);
        scifor.Log.warn('[seam_fallback] stage=%s fn=%s output=%s %s: %s — no check names this cause yet', ...
            stage, char(fn_name), char(output), err.identifier, err.message);
    end
    new_err = addCause(new_err, err);
end



function msg = describe_table_kinds(collected, fn_name, output_name)
%DESCRIBE_TABLE_KINDS  When combos returned tables whose columns differ (a
%   column missing from some, or of another type), the message naming the
%   first such column and the combos on each side; '' when none differ.
%   The diagnosis of the combine_outputs seam -- it runs only after vertcat
%   has already failed, so a run that works is never refused by it.
    msg = '';
    n = numel(collected);
    labels = cell(n, 1);
    kinds = cell(n, 1);   % per combo: containers.Map column -> class
    all_cols = {};
    for r = 1:n
        labels{r} = combo_label(collected{r}{1});
        t = collected{r}{2};
        m = containers.Map('KeyType', 'char', 'ValueType', 'char');
        names = t.Properties.VariableNames;
        for c = 1:numel(names)
            m(names{c}) = class(t.(names{c}));
        end
        kinds{r} = m;
        all_cols = union(all_cols, names, 'stable');
    end
    for c = 1:numel(all_cols)
        col = all_cols{c};
        by_kind = containers.Map('KeyType', 'char', 'ValueType', 'any');
        for r = 1:n
            if isKey(kinds{r}, col)
                k = kinds{r}(col);
            else
                k = 'missing';
            end
            if isKey(by_kind, k)
                by_kind(k) = [by_kind(k), labels(r)];
            else
                by_kind(k) = labels(r);
            end
        end
        if by_kind.Count < 2
            continue;
        end
        parts = {};
        ks = keys(by_kind);
        for i = 1:numel(ks)
            combos = by_kind(ks{i});
            shown = strjoin(combos(1:min(3, numel(combos))), '; ');
            if numel(combos) > 3
                shown = sprintf('%s; +%d more', shown, numel(combos) - 3);
            end
            parts{end+1} = sprintf('%s for %d combo(s) (%s)', ks{i}, numel(combos), shown); %#ok<AGROW>
        end
        msg = sprintf(['output ''%s'' of %s: column ''%s'' is %s. SciStack can ' ...
            'only stack tables whose columns match; make the function return ' ...
            'the same columns, of the same type, for every combo.'], ...
            char(output_name), char(fn_name), col, strjoin(parts, ', but '));
        return;
    end
end



function s = combo_label(metadata)
%COMBO_LABEL  'subject=1, session=A' for a combo's metadata struct.
    f = fieldnames(metadata);
    bits = cell(1, numel(f));
    for i = 1:numel(f)
        bits{i} = sprintf('%s=%s', f{i}, short_value(metadata.(f{i})));
    end
    s = strjoin(bits, ', ');
    if isempty(s)
        s = 'the only combo';
    end
end



function stop_err = stop_error_after(on_error, stop_err, err)
%STOP_ERROR_AFTER  on_error='stop': the first genuine failure ends the loop.
%   scifor:NoData (a combo with no data) never stops a run.
    if isempty(stop_err) && strcmp(on_error, 'stop') ...
            && ~strcmp(err.identifier, 'scifor:NoData')
        stop_err = err;
    end
end



function text = describe_user_frame(frame)
%DESCRIBE_USER_FRAME  'at FILE:LINE in FN' — the rendering Python's
%   scifor.failures.format_user_frame gives the same frame.
    if isempty(frame)
        text = 'no user frame (failed inside SciStack — see report)';
    else
        text = sprintf('at %s:%d in %s', frame.file, frame.line, frame.function);
    end
end



function out_varargin = replace_name_value(in_varargin, name, new_value)
%REPLACE_NAME_VALUE  Replace a name-value pair's value in a varargin cell
%   array (case-insensitive on the name). Used by Step 0's EachOf
%   expansion to substitute a concrete filter for a where=EachOf(...) axis
%   before the recursive scifor.for_each() call.
    out_varargin = in_varargin;
    for i = 1:2:numel(out_varargin)
        key = out_varargin{i};
        if (ischar(key) || isstring(key)) && strcmpi(string(key), name)
            out_varargin{i+1} = new_value;
            return;
        end
    end
    out_varargin{end+1} = name;
    out_varargin{end+1} = new_value;
end



function result_tbl = vertcat_each_of_results(branch_results, fn_name)
%VERTCAT_EACH_OF_RESULTS  Concatenate EachOf branch tables, with a clear
%   error (instead of a raw MATLAB vertcat failure) when branches disagree
%   on columns — which happens when the EachOf alternatives (e.g. two
%   scifor.PathInput templates) don't share the same placeholder/schema-key
%   names. MATLAB's table vertcat has no pandas-style NaN-union leniency,
%   so mismatched columns must be a hard, clearly-explained error.
    if isempty(branch_results)
        result_tbl = table();
        return;
    end
    first_vars = sort(string(branch_results{1}.Properties.VariableNames));
    for i = 2:numel(branch_results)
        these_vars = sort(string(branch_results{i}.Properties.VariableNames));
        if ~isequal(first_vars, these_vars)
            error('scifor:for_each:EachOfColumnMismatch', ...
                ['for_each(%s): EachOf branches produced result tables with ' ...
                 'different columns (branch 1: %s; branch %d: %s). Every ' ...
                 'EachOf alternative must resolve to the same schema-key/' ...
                 'metadata columns — e.g. two scifor.PathInput templates ' ...
                 'must use the same {placeholder} names even if root_folder ' ...
                 'differs.'], ...
                fn_name, strjoin(first_vars, ', '), i, strjoin(these_vars, ', '));
        end
    end
    result_tbl = vertcat(branch_results{:});
end



function inputs = bind_path_inputs(inputs, root)
%BIND_PATH_INPUTS  Every bare or Fixed-wrapped PathInput input bound to ROOT.
    names = fieldnames(inputs);
    for p = 1:numel(names)
        v = inputs.(names{p});
        if isa(v, 'scifor.PathInput')
            inputs.(names{p}) = v.bound_to(root);
        elseif isa(v, 'scifor.Fixed') && isa(v.data, 'scifor.PathInput')
            fm = v.fixed_metadata;
            nv = reshape([fieldnames(fm)'; struct2cell(fm)'], 1, []);
            inputs.(names{p}) = scifor.Fixed(v.data.bound_to(root), nv{:});
        end
    end
    scifor.Log.debug('project_root: PathInput inputs bound to %s', char(root));
end
