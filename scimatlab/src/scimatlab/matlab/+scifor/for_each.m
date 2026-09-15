function varargout = for_each(fn, inputs, varargin)
%SCIFOR.FOR_EACH  Execute a function for all combinations of metadata.
%
%   result = scifor.for_each(@FN, INPUTS, Name, Value, ...)
%   [r1, r2] = scifor.for_each(@FN, INPUTS, Name, Value, ...)
%
%   Pure loop orchestrator — works with MATLAB tables only, no I/O.
%   Iterates over every combination of the supplied metadata values.
%   For each combination it filters table inputs by metadata columns,
%   calls the function, and collects results.
%
%   Each function output becomes a separate result table:
%   - Non-table outputs produce a table with metadata columns + an
%     'output' data column (name customizable via output_names).
%   - Table outputs are preserved with metadata columns prepended.
%   Multiple outputs are returned via multiple return values.
%
%   Inputs can be:
%   - MATLAB tables        — filtered per combo by schema key columns
%   - scifor.Fixed(tbl)    — table filtered with overridden metadata
%   - scifor.Merge(t1, t2) — multiple tables merged column-wise per combo
%   - scifor.ColumnSelection(tbl, cols) — column extraction after filtering
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
%                       level below the deepest iterated key. If no
%                       metadata iterable is a schema key (e.g. a fully
%                       static PathInput with no {key} placeholders),
%                       distributes to the top of the schema instead.
%                       (default: false)
%       where         - Optional scifor.ColFilter to apply to table rows
%                       after combo filtering. (default: [])
%       categorical   - If true, convert metadata columns in result
%                       tables to categorical. (default: false)
%       output_names  - Cell array of strings for result column names
%                       (one per output). Defaults to {'output'} for each.
%       _all_combos   - Pre-built cell array of combo structs (from DB
%                       wrappers that pre-filter). Bypasses cartesian_product.
%       _mapping_inputs - struct: input name -> string array of data column
%                       names, declaring that the input's rows are STRUCT
%                       records spread one column per field rather than the
%                       rows of a table. A single row is then handed to fn as
%                       that struct. This cannot be inferred here -- "one row,
%                       N data columns" is also exactly what a genuine
%                       table-valued variable looks like -- so scidb, which
%                       knows the storage layout, states it (see
%                       +scidb/for_each.m). Ignored for an input passed
%                       through as_table or with a column selection.
%       (any other)   - Metadata iterables (numeric or string arrays)
%
%   Returns:
%       One table per function output. Non-table outputs produce a table
%       with metadata columns + a data column. Table outputs produce a
%       table with metadata columns + original data columns. Returns []
%       for dry_run.
%
%   Example:
%       set_schema(["subject", "session"])
%       result = scifor.for_each(@filter_data, ...
%           struct('raw', data_table, 'smoothing', 0.2), ...
%           subject=[1 2 3], session=["A" "B"])

    % --- Fix struct-array inputs from struct() cell-array gotcha ---
    %   struct('a', tbl, 'b', {'x','y'}) creates a 1x2 struct array
    %   instead of a scalar struct with a cell field. Collapse it.
    if ~isscalar(inputs)
        inputs = collapse_struct_array(inputs);
    end

    % --- Parse options vs metadata name-value pairs ---
    [meta_args, opts] = split_options(varargin{:});

    dry_run = opts.dry_run;
    as_table_raw = opts.as_table;
    distribute = opts.distribute;
    where_filter = opts.where;

    % Get function name for display
    if isa(fn, 'function_handle')
        fn_name = func2str(fn);
    else
        fn_name = 'unknown';
    end

    % --- Step 0: EachOf expansion — must be first, before any other logic.
    %     Mirrors Python scifor/src/scifor/foreach.py's own EachOf expansion
    %     (see docs/claude/each-of-variant-expansion.md): each alternative
    %     becomes an independent recursive scifor.for_each() call; results
    %     are concatenated per output index (this layer has no save/lineage
    %     to thread, unlike scidb.for_each's own, separate EachOf step).
    input_names_eo = fieldnames(inputs);
    each_of_axes = {};   % {kind, field_name, alternatives_cell}
    for p = 1:numel(input_names_eo)
        name = input_names_eo{p};
        if isa(inputs.(name), 'scifor.EachOf')
            scifor.require_alternatives(inputs.(name), 'input', name);
            each_of_axes{end+1} = {'input', name, inputs.(name).alternatives}; %#ok<AGROW>
        end
    end
    if isa(where_filter, 'scifor.EachOf')
        scifor.require_alternatives(where_filter, 'where');
        each_of_axes{end+1} = {'where', '', where_filter.alternatives}; %#ok<AGROW>
    end

    if ~isempty(each_of_axes)
        scifor.Log.debug('EachOf expansion detected - %d axis(es), making recursive calls', ...
            numel(each_of_axes));
        value_cells = cellfun(@(ax) ax{3}, each_of_axes, 'UniformOutput', false);
        combos = scidb.internal.cartesian_product(value_cells);
        n_out_request = max(nargout, 1);
        branch_out = cell(numel(combos), n_out_request);
        for ci = 1:numel(combos)
            combo = combos{ci};
            concrete_inputs = inputs;
            concrete_varargin = varargin;
            for a = 1:numel(each_of_axes)
                axis = each_of_axes{a};
                if strcmp(axis{1}, 'input')
                    concrete_inputs.(axis{2}) = combo{a};
                else
                    concrete_varargin = replace_name_value(concrete_varargin, ...
                        'where', combo{a});
                end
            end
            [branch_out{ci, 1:n_out_request}] = scifor.for_each(fn, concrete_inputs, concrete_varargin{:});
        end
        for o = 1:n_out_request
            branch_col = branch_out(:, o);
            branch_col = branch_col(~cellfun(@isempty, branch_col));
            varargout{o} = vertcat_each_of_results(branch_col, fn_name); %#ok<AGROW>
        end
        scifor.Log.debug('EachOf expansion complete - concatenated %d branch result(s)', ...
            numel(combos));
        return;
    end

    % Get schema keys
    full_schema_keys = scifor.get_schema();

    % --- Resolve output_names ---
    if isempty(opts.output_names)
        % Auto-detect number of outputs via nargout introspection
        try
            n_out = nargout(fn);
        catch
            n_out = 1;
        end
        if n_out < 0
            n_out = max(nargout, 1);
        end
        resolved_output_names = cell(1, n_out);
        for i = 1:n_out
            resolved_output_names{i} = 'output';
        end
    elseif isnumeric(opts.output_names) && isscalar(opts.output_names)
        n = opts.output_names;
        resolved_output_names = cell(1, n);
        for i = 1:n
            resolved_output_names{i} = 'output';
        end
    else
        resolved_output_names = opts.output_names;
    end
    n_outputs = numel(resolved_output_names);

    % Parse metadata iterables
    if mod(numel(meta_args), 2) ~= 0
        error('scifor:for_each', 'Metadata arguments must be name-value pairs.');
    end

    meta_keys = string.empty;
    meta_values = {};
    for i = 1:2:numel(meta_args)
        meta_keys(end+1) = string(meta_args{i}); %#ok<AGROW>
        v = meta_args{i+1};
        if isnumeric(v)
            meta_values{end+1} = num2cell(v); %#ok<AGROW>
        elseif isstring(v)
            meta_values{end+1} = cellstr(v); %#ok<AGROW>
        elseif iscell(v)
            meta_values{end+1} = v; %#ok<AGROW>
        else
            meta_values{end+1} = {v}; %#ok<AGROW>
        end
    end

    % --- schema_keys= structural sugar ---
    %   Seed meta_keys/meta_values with an empty entry per requested key —
    %   "iterate over these keys" without spelling out key=[] by hand. Each
    %   still auto-resolves via the existing empty-array resolution below.
    %   Mirrors Python scifor's expand_schema_keys(); mutually exclusive
    %   with explicit metadata name-value pairs.
    if ~isempty(opts.schema_keys)
        if ~isempty(meta_keys)
            error('scifor:for_each', ['Cannot use both schema_keys and ' ...
                'explicit metadata name-value pairs. Use schema_keys for ' ...
                'automatic iteration, or explicit key=value pairs for ' ...
                'manual control.']);
        end
        for i = 1:numel(opts.schema_keys)
            meta_keys(end+1) = string(opts.schema_keys(i)); %#ok<AGROW>
            meta_values{end+1} = {}; %#ok<AGROW>
        end
    end

    % --- Resolve empty arrays from inputs (standalone mode) ---
    %   Empty [] means "use all values". Resolve from table inputs first, then
    %   from PathInput filesystem discovery. Keys the user passed with explicit
    %   (non-empty) values assert intent and are never overwritten.
    if isempty(opts.all_combos)
        user_explicit_keys = string.empty;
        for i = 1:numel(meta_values)
            if ~isempty(meta_values{i})
                user_explicit_keys(end+1) = meta_keys(i); %#ok<AGROW>
            end
        end

        % Pass 1: table inputs (non-raising; returns {} when no column).
        for i = 1:numel(meta_values)
            if isempty(meta_values{i})
                meta_values{i} = distinct_values_from_inputs(inputs, meta_keys(i));
            end
        end

        % Pass 2: PathInput filesystem discovery. The Case A/B decision (and
        % whether discovered combos drive iteration directly) is owned by
        % PathInput so the scidb and scifor layers share one implementation.
        pi = find_pathinput(inputs);
        % Case A -- NO keys declared at all -- is as much a discovery trigger
        % as Case B. It has to be tested separately because `any([])` is
        % false: gating only on `any(cellfun(@isempty, meta_values))` meant a
        % bare `for_each(fn, struct('p', pathinput))` never ran discovery at
        % all, produced zero combos, and returned an empty table without
        % logging a word (2026-09-14). Python has always handled it --
        % pathinput.apply_discovery's "Case A: no metadata keys passed at all
        % -> adopt every discovered key" -- so this was a MATLAB-only gap.
        case_a = isempty(meta_keys);
        if ~isempty(pi) && (case_a || any(cellfun(@isempty, meta_values)))
            iter_struct = struct();
            for i = 1:numel(meta_keys)
                iter_struct.(char(meta_keys(i))) = meta_values{i};
            end
            % condense_numeric=true only here: this is scifor's own
            % standalone discovery call site (isempty(opts.all_combos)),
            % never reached when driven through scidb.for_each (which
            % always supplies opts.all_combos, mirroring the Python
            % isolation boundary -- see docs/claude/schema-key-types.md).
            [filled, discovered_combos] = pi.apply_discovery(iter_struct, user_explicit_keys, true);
            for i = 1:numel(meta_keys)
                if isempty(meta_values{i}) && isfield(filled, char(meta_keys(i)))
                    meta_values{i} = filled.(char(meta_keys(i)));
                end
            end

            placeholder_keys = string(pi.placeholder_keys());

            % Case A: the caller declared nothing, so the keys discovery found
            % ARE the iteration. Adopt them in TEMPLATE PLACEHOLDER order --
            % the order {subject}/{session}/data.txt names them -- because that
            % is the order Python adopts (combos[0].keys(), bound by the walk
            % in segment order) and the two layers must put the same columns in
            % the same places. The fill loop above cannot do this: it only
            % writes back keys already in meta_keys, and in Case A there are
            % none.
            if case_a
                for i = 1:numel(placeholder_keys)
                    k = char(placeholder_keys(i));
                    if isfield(filled, k)
                        meta_keys(end+1) = placeholder_keys(i); %#ok<AGROW>
                        meta_values{end+1} = filled.(k); %#ok<AGROW>
                    end
                end
                % Anything discovery returned that is not a template
                % placeholder. Not reachable today -- the walk only binds named
                % groups built from placeholders -- but dropping a discovered
                % key silently would be worse than the extra loop, and the
                % coverage test below turns any such key into a Cartesian
                % product rather than a wrong answer.
                extra = setdiff(string(fieldnames(filled))', meta_keys, 'stable');
                for i = 1:numel(extra)
                    meta_keys(end+1) = extra(i); %#ok<AGROW>
                    meta_values{end+1} = filled.(char(extra(i))); %#ok<AGROW>
                end
                if isempty(meta_keys)
                    % A PathInput was present, nothing was declared, and
                    % discovery found nothing to adopt: this for_each is about
                    % to iterate zero times and return an empty table. Say so
                    % -- silence here is exactly what made this look like a
                    % successful run with no matching data.
                    %
                    % root_folder is optional, so it can be an empty string;
                    % name the fallback rather than formatting a blank into
                    % the message.
                    root_desc = "<project root>";
                    if ~isempty(pi.root_folder) && strlength(pi.root_folder) > 0
                        root_desc = pi.root_folder;
                    end
                    scifor.Log.warn(['pathinput_discovery: no keys declared and ' ...
                        'nothing discovered under template ''%s'' (root_folder ' ...
                        '''%s'') -- 0 iterations'], ...
                        pi.path_template, root_desc);
                else
                    scifor.Log.info(['pathinput_discovery: adopted %d key(s) from ' ...
                        'the template (%s); %d combo(s) found on disk'], ...
                        numel(meta_keys), strjoin(meta_keys, ', '), ...
                        numel(discovered_combos));
                end
            end

            % Use discovered combos directly only when every iterated key is a
            % template placeholder (otherwise a Cartesian product with the
            % table-derived keys is still required).
            if ~isempty(discovered_combos) && all(ismember(meta_keys, placeholder_keys))
                opts.all_combos = project_combos(discovered_combos, meta_keys);
                scifor.Log.debug('pathinput_discovery: using %d disk combos', ...
                    numel(opts.all_combos));
            end
        end

        % Any key still unresolved: warn (0 iterations) when a source exists
        % but yields no values; drop the key entirely when the only
        % PathInput is fully static (no {key} placeholders anywhere in its
        % template) — a literal path can never drive iteration for ANY key,
        % so an unresolved key in that situation isn't a mistake, it just
        % means "this key doesn't apply here." Error only when a templated
        % PathInput (or no PathInput at all) leaves the key genuinely
        % unaccounted for.
        pi_is_static = ~isempty(pi) && isempty(pi.placeholder_keys());
        drop_idx = false(1, numel(meta_values));
        for i = 1:numel(meta_values)
            if isempty(meta_values{i})
                if key_has_source(inputs, meta_keys(i), pi)
                    scifor.Log.warn('no values found for ''%s'' in inputs, 0 iterations', ...
                        meta_keys(i));
                elseif pi_is_static
                    scifor.Log.debug(['''%s'' has no source and PathInput ' ...
                        '''%s'' has no template placeholders; ignoring ''%s'' ' ...
                        '(treating it as if it were never requested).'], ...
                        meta_keys(i), pi.path_template, meta_keys(i));
                    drop_idx(i) = true;
                else
                    error('scifor:for_each', ...
                        ['Empty list [] was passed for ''%s'', but no input table ' ...
                         'has that column and no PathInput template has a {%s} ' ...
                         'placeholder. Provide values explicitly, add a table ' ...
                         'input with a ''%s'' column, or use a PathInput with a ' ...
                         '{%s} placeholder.'], ...
                        meta_keys(i), meta_keys(i), meta_keys(i), meta_keys(i));
                end
            end
        end
        if any(drop_idx)
            meta_keys(drop_idx) = [];
            meta_values(drop_idx) = [];
        end
    end

    % --- Determine effective keys for filtering/extraction ---
    %   No schema set → iteration keys are the source of truth.
    %   Schema set    → schema keys are the source of truth.
    if isempty(full_schema_keys)
        effective_keys = meta_keys;
    else
        effective_keys = full_schema_keys;
    end

    % --- Validate distribute parameter and resolve target key ---
    % Internal discriminator keys (scidb's __rid_*/__vsig_* schema
    % extensions, arriving sanitized as x__rid_*/x__vsig_* through the
    % bridge) are not experimental LEVELS — hide them from distribute
    % resolution, or an aggregation over a variant-tracked input would see
    % the discriminator as the deepest key and refuse to distribute.
    %
    % The resolved target is logged at INFO, matching the wording of Python's
    % scifor (foreach.py resolve_distribute_target). A distribute that lands
    % on the wrong level fails INVISIBLY — the run still succeeds and still
    % saves, just at the wrong granularity — so 'options: distribute=true' on
    % its own is not enough to read a run: it says the flag arrived, never
    % which key it chose. A GUI node with every schema level deselected ran
    % the full grid and distributed to 'cycle' instead of 'subject' for
    % exactly this reason (2026-09-15); see
    % .claude/plan-schema-level-empty-distribute.md.
    distribute_key = '';
    % True only when distribute_key is synthesized by the "nothing
    % iterated" fallback below (no source table/iterable ever carries this
    % key), so it has no captured input type to round-trip to. Rather than
    % leave it as a bare double index, it's cast to categorical to match
    % how schema-key columns are represented elsewhere in the framework.
    distribute_key_synthetic = false;
    if distribute
        real_schema_keys = full_schema_keys( ...
            ~contains(full_schema_keys, "__rid_") & ~contains(full_schema_keys, "__vsig_"));
        if isempty(real_schema_keys)
            error('scifor:for_each', ...
                'distribute=true requires schema keys. Call set_schema() first.');
        end

        iter_keys_in_schema = real_schema_keys(ismember(real_schema_keys, meta_keys));
        if isempty(iter_keys_in_schema)
            % Nothing is being iterated at a schema level (e.g. a fully
            % static PathInput with no {key} placeholders, or no
            % metadata_iterables at all) — distribute to the top of the
            % schema rather than erroring.
            distribute_key = real_schema_keys(1);
            distribute_key_synthetic = true;
            scifor.Log.info('resolve_distribute_target: ''%s'' (top of schema; nothing iterated)', ...
                distribute_key);
        else
            deepest_iterated = iter_keys_in_schema(end);
            deepest_idx = find(real_schema_keys == deepest_iterated, 1);

            if deepest_idx >= numel(real_schema_keys)
                error('scifor:for_each', ...
                    'distribute=true but ''%s'' is the deepest schema key. There is no lower level to distribute to. Schema order: %s', ...
                    deepest_iterated, strjoin(real_schema_keys, ', '));
            end
            distribute_key = real_schema_keys(deepest_idx + 1);
            scifor.Log.info('resolve_distribute_target: ''%s'' (one level below ''%s'')', ...
                distribute_key, deepest_iterated);
        end
    end

    % --- Capture input schema-key column types for output round-trip ---
    %   Output metadata columns must come back as EXACTLY the type of the
    %   input column they were resolved from: double stays double, string
    %   stays string, categorical stays categorical (categories/ordinality
    %   preserved). Iteration itself uses canonical values (see
    %   decategorize_schema_column) so filtering and ordering stay correct;
    %   the cast back happens in build_single_output_table.
    type_keys = effective_keys;
    for tk = 1:numel(meta_keys)
        if ~ismember(meta_keys(tk), type_keys)
            type_keys(end+1) = meta_keys(tk); %#ok<AGROW>
        end
    end
    if strlength(distribute_key) > 0 && ~ismember(distribute_key, type_keys)
        type_keys(end+1) = distribute_key;
    end
    schema_col_types = capture_schema_column_types(inputs, type_keys);

    % --- Resolve static ColName(tbl) wrappers before the data/constant split.
    %     Deferred ColName() markers (no table) are left in place — they resolve
    %     per-column inside the for_columns iteration loop (validated below). ---
    input_names = fieldnames(inputs);
    for p = 1:numel(input_names)
        var_spec = inputs.(input_names{p});
        if isa(var_spec, 'scifor.ColName') && var_spec.is_deferred()
            % No-arg ColName() — leave in place; resolved per-column in
            % run_column_iteration.
            continue
        elseif isa(var_spec, 'scifor.ColName')
            inner_tbl = var_spec.data;
            if ~istable(inner_tbl)
                error('scifor:ColName', ...
                    'ColName(%s) expected a table, got %s', ...
                    input_names{p}, class(inner_tbl));
            end
            tbl_cols = string(inner_tbl.Properties.VariableNames);
            data_cols = setdiff(tbl_cols, full_schema_keys, 'stable');
            if numel(data_cols) == 1
                inputs.(input_names{p}) = char(data_cols(1));
            elseif isempty(data_cols)
                error('scifor:ColName', ...
                    'ColName(%s): table has no data columns (all columns are schema keys). Columns: %s, schema keys: %s', ...
                    input_names{p}, strjoin(tbl_cols, ', '), strjoin(full_schema_keys, ', '));
            else
                error('scifor:ColName', ...
                    'ColName(%s): table has %d data columns (%s), expected exactly 1. Schema keys: %s', ...
                    input_names{p}, numel(data_cols), strjoin(data_cols, ', '), strjoin(full_schema_keys, ', '));
            end
        end
    end

    % --- Parse inputs struct — separate data inputs from constants ---
    n_inputs = numel(input_names);

    data_idx = false(1, n_inputs);
    constant_names = {};
    constant_values = {};

    for p = 1:n_inputs
        var_spec = inputs.(input_names{p});
        if is_data_input(var_spec)
            data_idx(p) = true;
        else
            constant_names{end+1} = input_names{p}; %#ok<AGROW>
            constant_values{end+1} = var_spec; %#ok<AGROW>
        end
    end

    % Check distribute doesn't conflict with a constant input name
    if strlength(distribute_key) > 0 && ismember(distribute_key, string(constant_names))
        error('scifor:for_each', ...
            'distribute target ''%s'' conflicts with a constant input named ''%s''.', ...
            distribute_key, distribute_key);
    end

    % Resolve as_table: true → all data input names, false/empty → none
    if islogical(as_table_raw) && isscalar(as_table_raw) && as_table_raw
        as_table_set = string(input_names(data_idx)');
    elseif islogical(as_table_raw) && isscalar(as_table_raw) && ~as_table_raw
        as_table_set = string.empty;
    else
        as_table_set = as_table_raw;
    end

    % --- Step 6.5: Detect iterate-mode ColumnSelection inputs (for_columns) ---
    %   These fan out column-wise: fn runs once per column and the per-column
    %   results are reassembled into one wide row per combo. All iterate
    %   inputs share a single column axis (zipped by name).
    iterate_pos = false(1, n_inputs);
    for p = 1:n_inputs
        if data_idx(p)
            cs = unwrap_column_selection(inputs.(input_names{p}));
            if ~isempty(cs) && cs.iterate
                iterate_pos(p) = true;
            end
        end
    end
    has_iterate = any(iterate_pos);

    % Deferred ColName() markers resolve to the current iterated column, so they
    % require at least one for_columns input. (Static ColName(tbl) was already
    % resolved to a char above, so only no-arg markers remain here.)
    deferred_colname_names = {};
    for p = 1:n_inputs
        var_spec = inputs.(input_names{p});
        if isa(var_spec, 'scifor.ColName') && var_spec.is_deferred()
            deferred_colname_names{end+1} = input_names{p}; %#ok<AGROW>
        end
    end
    if ~isempty(deferred_colname_names) && ~has_iterate
        error('scifor:ColName', ...
            ['ColName() with no argument resolves to the current for_columns ' ...
             'column, so it requires at least one iterate input ' ...
             '(ColumnSelection(..., iterate=true)). Deferred ColName() ' ...
             'input(s): %s. Use ColName(tbl) for the static single-column ' ...
             'form instead.'], strjoin(string(deferred_colname_names), ', '));
    end

    iterate_columns = string.empty;
    if has_iterate
        first_set = true;
        for p = find(iterate_pos)
            cs = unwrap_column_selection(inputs.(input_names{p}));
            cols = string(cs.columns);
            if isempty(cols)
                % Empty selection (the all-columns sentinel) -> resolve to
                % every data column of the underlying table.
                cols = all_data_columns(cs.data, full_schema_keys);
                if isempty(cols)
                    error('scifor:for_each', ...
                        ['for_columns(): no data columns found to iterate over ' ...
                         '(columns: [%s], schema keys: [%s]).'], ...
                        strjoin(string(cs.data.Properties.VariableNames), ', '), ...
                        strjoin(full_schema_keys, ', '));
                end
            end
            if first_set
                iterate_columns = cols;
                first_set = false;
            elseif ~isequal(sort(cols), sort(iterate_columns))
                error('scifor:for_each', ...
                    ['for_columns inputs must iterate over the same columns ' ...
                     '(zipped by name). ''%s'' has [%s] but ''%s'' has [%s].'], ...
                    input_names{find(iterate_pos, 1)}, strjoin(iterate_columns, ', '), ...
                    input_names{p}, strjoin(cols, ', '));
            end
        end
        if n_outputs ~= 1
            error('scifor:for_each', ...
                'for_columns supports exactly one output; got %d.', n_outputs);
        end
        if strlength(distribute_key) > 0
            error('scifor:for_each', ...
                'for_columns cannot be combined with distribute=true.');
        end
    end

    % --- Build combo list ---
    if ~isempty(opts.all_combos)
        combos = opts.all_combos;
    elseif isempty(meta_values)
        combos = {{}};
    else
        combos = scidb.internal.cartesian_product(meta_values);
    end

    total = numel(combos);

    % --- Run banner: one INFO line with a truncated per-key value preview;
    %     the full value lists follow at DEBUG (mirrors Python scifor). ---
    meta_parts_banner = {};
    for mk = 1:numel(meta_keys)
        if ~startsWith(meta_keys(mk), "__")
            meta_parts_banner{end+1} = sprintf('%s=%s', meta_keys(mk), ...
                format_value_preview(meta_values{mk})); %#ok<AGROW>
        end
    end
    if isempty(meta_parts_banner)
        meta_summary = 'no metadata';
    else
        meta_summary = strjoin(meta_parts_banner, ', ');
    end
    if total == 1
        iter_str = sprintf('for_each(%s) — 1 iteration', fn_name);
    else
        iter_str = sprintf('for_each(%s) — %d iterations', fn_name, total);
    end
    scifor.Log.info('%s: %s', iter_str, meta_summary);

    % --- Detailed config: inputs ---
    inputs_str = format_inputs(inputs, input_names, data_idx);
    scifor.Log.info('inputs: %s', inputs_str);

    % --- Positional-binding preflight ---
    warn_on_arity_mismatch(fn, fn_name, input_names, ...
        numel(fieldnames(opts.share_limits)));

    % --- Detailed config: metadata actual values ---
    for mk2 = 1:numel(meta_keys)
        if ~startsWith(meta_keys(mk2), "__")
            scifor.Log.debug('%s=%s', meta_keys(mk2), format_meta_values(meta_values{mk2}));
        end
    end

    % --- Detailed config: non-default options ---
    opt_parts = {};
    if dry_run
        opt_parts{end+1} = 'dry_run=true';
    end
    if distribute
        opt_parts{end+1} = 'distribute=true';
    end
    if ~isempty(as_table_raw)
        if islogical(as_table_raw) && isscalar(as_table_raw) && as_table_raw
            opt_parts{end+1} = 'as_table=true';
        elseif isstring(as_table_raw)
            opt_parts{end+1} = sprintf('as_table=[%s]', strjoin(as_table_raw, ', '));
        end
    end
    if ~isempty(where_filter)
        opt_parts{end+1} = sprintf('where=%s', class(where_filter));
    end
    if ~isempty(opt_parts)
        scifor.Log.info('options: %s', strjoin(opt_parts, ', '));
    end

    % --- Dry-run header ---
    if dry_run
        fprintf('[dry-run] for_each(%s)\n', fn_name);
        fprintf('[dry-run] %d iterations over: %s\n', total, strjoin(meta_keys, ', '));
        fprintf('[dry-run] inputs: %s\n', format_inputs(inputs, input_names, data_idx));
        if strlength(distribute_key) > 0
            fprintf('[dry-run] distribute: ''%s'' (split outputs by element/row, 1-based)\n', distribute_key);
        end
        fprintf('\n');
    end

    completed = 0;
    skipped = 0;
    collected_per_output = cell(1, n_outputs);
    for o = 1:n_outputs
        collected_per_output{o} = {};
    end

    % Failure aggregation for the end-of-run summary (mirrors Python scifor):
    % reason -> cellstr of combo strings; first occurrence of each distinct
    % reason logs at WARN so the default (INFO) log answers "what failed".
    failure_reasons = containers.Map('KeyType', 'char', 'ValueType', 'any');
    failure_order = {};

    % Periodic progress: one line per outermost-key transition, but never at
    % the very first combo, never inside the start delay (fast runs stay
    % silent), and never more often than the minimum interval.
    PROGRESS_MIN_INTERVAL_S = 2.0;
    PROGRESS_START_DELAY_S = 5.0;
    progress_key = '';
    for mk3 = 1:numel(meta_keys)
        if ~startsWith(meta_keys(mk3), "__")
            progress_key = char(meta_keys(mk3));
            break;
        end
    end
    progress_last_value = [];
    progress_seen = 0;
    progress_total = count_distinct_combo_values(combos, meta_keys, progress_key);
    % Elapsed-seconds at the last progress emission (0 = loop start), so the
    % min-interval guard also spaces the FIRST line away from loop start.
    progress_last_emit = 0;
    loop_t0 = tic;

    % share_limits prepass (port of Python scifor's _compute_shared_limits):
    % per named input, group its table by the held-fixed schema keys and
    % compute each group's global numeric [min max] across all data columns.
    shared_limits_map = struct();
    if ~isempty(fieldnames(opts.share_limits))
        shared_limits_map = compute_shared_limits( ...
            opts.share_limits, inputs, input_names, data_idx, full_schema_keys);
    end

    for c = 1:numel(combos)
        combo = combos{c};

        % Build metadata struct for this iteration
        if isstruct(combo)
            % Pre-built combo from _all_combos
            metadata = combo;
            meta_fields = fieldnames(metadata);
            meta_nv = {};
            meta_parts = {};
            for k = 1:numel(meta_fields)
                val = metadata.(meta_fields{k});
                meta_nv{end+1} = meta_fields{k}; %#ok<AGROW>
                meta_nv{end+1} = val; %#ok<AGROW>
                if isnumeric(val)
                    meta_parts{end+1} = sprintf('%s=%g', meta_fields{k}, val); %#ok<AGROW>
                else
                    meta_parts{end+1} = sprintf('%s=%s', meta_fields{k}, string(val)); %#ok<AGROW>
                end
            end
        else
            % Cell array from cartesian_product
            metadata = struct();
            meta_nv = {};
            meta_parts = {};
            for k = 1:numel(meta_keys)
                val = combo{k};
                metadata.(meta_keys(k)) = val;
                meta_nv{end+1} = char(meta_keys(k)); %#ok<AGROW>
                meta_nv{end+1} = val; %#ok<AGROW>
                if isnumeric(val)
                    meta_parts{end+1} = sprintf('%s=%g', meta_keys(k), val); %#ok<AGROW>
                else
                    meta_parts{end+1} = sprintf('%s=%s', meta_keys(k), string(val)); %#ok<AGROW>
                end
            end
        end
        metadata_str = strjoin(meta_parts, ', ');

        % --- Periodic progress on outermost-key transitions ---
        if ~isempty(progress_key) && ~dry_run && isfield(metadata, progress_key)
            pv = metadata.(progress_key);
            if progress_seen == 0 || ~isequal(pv, progress_last_value)
                progress_last_value = pv;
                progress_seen = progress_seen + 1;
                elapsed_now = toc(loop_t0);
                if progress_seen > 1 ...
                        && elapsed_now >= PROGRESS_START_DELAY_S ...
                        && (elapsed_now - progress_last_emit) >= PROGRESS_MIN_INTERVAL_S
                    progress_last_emit = elapsed_now;
                    nd = 0;
                    for fr = 1:numel(failure_order)
                        if startsWith(failure_order{fr}, 'scifor:NoData:')
                            nd = nd + numel(failure_reasons(failure_order{fr}));
                        end
                    end
                    scifor.Log.info(['progress: %s=%s (%d/%d) — %d/%d combos ' ...
                        '(%.1f%%), completed=%d, failed=%d, no_data=%d, elapsed=%.1fs'], ...
                        progress_key, char(string(pv)), progress_seen, ...
                        progress_total, c - 1, total, ...
                        100.0 * (c - 1) / total, completed, skipped - nd, nd, elapsed_now);
                end
            end
        end

        % --- Dry-run iteration ---
        if dry_run
            print_dry_run_iteration(inputs, input_names, data_idx, ...
                metadata, metadata_str, distribute_key);
            completed = completed + 1;
            continue;
        end

        % --- Filter/prepare inputs for this combo ---
        loaded = cell(1, n_inputs);
        iterate_tables = cell(1, n_inputs);  % per-combo tables for iterate inputs
        filter_failed = false;

        for p = 1:n_inputs
            if ~data_idx(p)
                % Constant — pass value directly to user function,
                % or resolve PathInput if _resolve_pathinput is set
                val = inputs.(input_names{p});
                if isa(val, 'scifor.PathOutput')
                    % Output-path template. Prefer the Python-pre-resolved
                    % per-combo path (scidb bridge path: covers branch_param
                    % placeholders); fall back to native schema-key
                    % resolution for pure-MATLAB scifor calls. {ColName}
                    % stays in the string here; run_column_iteration
                    % substitutes it per column.
                    pname = input_names{p};
                    if isfield(opts.resolved_path_outputs, pname)
                        paths = opts.resolved_path_outputs.(pname);
                        loaded{p} = char(string(paths{c}));
                    else
                        loaded{p} = val.resolve(metadata);
                    end
                elseif opts.resolve_pathinput && isa(val, 'scifor.PathInput')
                    if isempty(opts.pathinput_loader)
                        loaded{p} = val.load(meta_nv{:});
                    else
                        loaded{p} = opts.pathinput_loader(val, meta_nv);
                    end
                elseif opts.resolve_pathinput && isa(val, 'scifor.Fixed') && isa(val.data, 'scifor.PathInput')
                    % Fixed(PathInput) — apply fixed overrides, then resolve
                    fixed_nv = meta_nv;
                    fixed_fields = fieldnames(val.fixed_metadata);
                    for f = 1:numel(fixed_fields)
                        key_name = fixed_fields{f};
                        key_val = val.fixed_metadata.(key_name);
                        replaced = false;
                        for nvi = 1:2:numel(fixed_nv)
                            if strcmp(fixed_nv{nvi}, key_name)
                                fixed_nv{nvi+1} = key_val;
                                replaced = true;
                                break;
                            end
                        end
                        if ~replaced
                            fixed_nv{end+1} = key_name; %#ok<AGROW>
                            fixed_nv{end+1} = key_val; %#ok<AGROW>
                        end
                    end
                    if isempty(opts.pathinput_loader)
                        loaded{p} = val.data.load(fixed_nv{:});
                    else
                        loaded{p} = opts.pathinput_loader(val.data, fixed_nv);
                    end
                else
                    loaded{p} = val;
                end
                continue;
            end

            var_spec = inputs.(input_names{p});

            if iterate_pos(p)
                % Keep the full per-combo table; slice per column below.
                try
                    iterate_tables{p} = prepare_iterate_table( ...
                        var_spec, metadata, effective_keys, where_filter);
                catch err
                    [failure_reasons, failure_order] = record_iteration_failure( ...
                        failure_reasons, failure_order, err, metadata_str, ...
                        sprintf('failed to filter %s', input_names{p}));
                    filter_failed = true;
                    break;
                end
                continue;
            end

            wants_table = ~isempty(as_table_set) && ismember(string(input_names{p}), as_table_set);

            if isfield(opts.mapping_inputs, input_names{p})
                mapping_cols = opts.mapping_inputs.(input_names{p});
            else
                mapping_cols = string.empty;
            end

            try
                loaded{p} = prepare_input(var_spec, metadata, effective_keys, wants_table, where_filter, mapping_cols);
            catch err
                [failure_reasons, failure_order] = record_iteration_failure( ...
                    failure_reasons, failure_order, err, metadata_str, ...
                    sprintf('failed to filter %s', input_names{p}));
                filter_failed = true;
                break;
            end
        end

        if filter_failed
            skipped = skipped + 1;
            continue;
        end

        % --- Column drift is a hard error (not a per-combo skip): the
        %     iterate column set is fixed up front, so a combo missing one of
        %     those columns means the stored data is inconsistent. ---
        if has_iterate
            for p = find(iterate_pos)
                t = iterate_tables{p};
                missing = setdiff(iterate_columns, ...
                    string(t.Properties.VariableNames), 'stable');
                if ~isempty(missing)
                    error('scifor:for_each', ...
                        ['for_columns column drift: column(s) [%s] missing from ' ...
                         'input ''%s'' for combo %s. The iterate column set [%s] ' ...
                         'must be present in every combo.'], ...
                        strjoin(missing, ', '), input_names{p}, metadata_str, ...
                        strjoin(iterate_columns, ', '));
                end
            end
        end

        % --- Call the function ---
        if has_iterate
            run_msg = sprintf('[run] %s: %s x %d column(s) (%s)', metadata_str, ...
                fn_name, numel(iterate_columns), ...
                strjoin(string(input_names'), ', '));
        else
            run_msg = sprintf('[run] %s: %s(%s)', metadata_str, fn_name, ...
                strjoin(string(input_names'), ', '));
        end
        scifor.Log.debug('%s', run_msg);

        % share_limits injection: append each named input's group limits as
        % trailing positional args (Python injects named kwargs; MATLAB has
        % no kwargs, so the fn declares trailing `<input>_limits` parameters
        % in share_limits field order). Skipped when the fn's declared arg
        % count has no capacity (varargin counts as capacity).
        call_args = loaded;
        if ~isempty(fieldnames(shared_limits_map))
            call_args = [loaded, build_limit_args( ...
                shared_limits_map, metadata, fn, n_inputs)];
        end

        try
            if has_iterate
                % for_columns: run fn once per column and reassemble into a
                % single 1xN-wide table (one output, validated at Step 6.5).
                result = {run_column_iteration(fn, loaded, iterate_pos, ...
                    iterate_tables, iterate_columns, full_schema_keys, ...
                    as_table_set, input_names)};
            elseif n_outputs == 0
                % Zero-output function (e.g. plotting side-effects only)
                fn(call_args{:});
                result = {};
            elseif n_outputs > 1
                fn_nargout = nargout(fn);
                if fn_nargout >= n_outputs || fn_nargout < 0
                    % True multi-output function
                    result = cell(1, n_outputs);
                    [result{1:n_outputs}] = fn(call_args{:});
                else
                    % Single-output function returning a cell array to unpack
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
        catch err
            % Structural for_columns errors are deterministic across combos and
            % indicate a return-contract bug — surface immediately, don't skip.
            if strcmp(err.identifier, 'scifor:for_each:forColumnsDuplicate') || ...
               strcmp(err.identifier, 'scifor:for_each:forColumnsBadReturn')
                rethrow(err);
            end
            [failure_reasons, failure_order] = record_iteration_failure( ...
                failure_reasons, failure_order, err, metadata_str, ...
                sprintf('%s raised', fn_name));
            skipped = skipped + 1;
            continue;
        end

        % Collect results per output
        if strlength(distribute_key) > 0
            % Distribute: expand each output into multiple rows
            for o = 1:min(n_outputs, numel(result))
                raw_value = result{o};
                try
                    dist_key_char = char(distribute_key);
                    if istable(raw_value)
                        % Expand single-row tables with cell-array columns
                        if height(raw_value) == 1
                            raw_value = expand_single_row_for_distribute(raw_value);
                        end
                        if ismember(dist_key_char, raw_value.Properties.VariableNames)
                            dist_values = raw_value.(dist_key_char);
                            data_tbl = raw_value;
                            data_tbl.(dist_key_char) = [];
                        else
                            dist_values = (1:height(raw_value))';
                            data_tbl = raw_value;
                        end
                        % Strip columns that overlap with metadata keys
                        % (only in flatten mode — nested mode preserves all columns)
                        if ~opts.nest_table_outputs
                            meta_field_names = fieldnames(metadata);
                            overlap = intersect(meta_field_names, data_tbl.Properties.VariableNames, 'stable');
                            if ~isempty(overlap)
                                data_tbl = removevars(data_tbl, overlap);
                            end
                        end
                        for rowIdx = 1:height(data_tbl)
                            dist_meta = metadata;
                            dist_meta.(dist_key_char) = dist_values(rowIdx);
                            collected_per_output{o}{end+1} = {dist_meta, data_tbl(rowIdx, :)}; %#ok<AGROW>
                        end
                    else
                        pieces = split_for_distribute(raw_value);
                        for k = 1:numel(pieces)
                            dist_meta = metadata;
                            dist_meta.(dist_key_char) = k;
                            collected_per_output{o}{end+1} = {dist_meta, pieces{k}}; %#ok<AGROW>
                        end
                    end
                catch err2
                    scifor.Log.warn('%s: cannot distribute output %d: %s', ...
                        metadata_str, o, err2.message);
                    continue;
                end
            end
        else
            % Normal: collect each output value separately
            for o = 1:min(n_outputs, numel(result))
                collected_per_output{o}{end+1} = {metadata, result{o}}; %#ok<AGROW>
            end
        end

        completed = completed + 1;
    end

    % --- End-of-run summary ---
    if dry_run
        fprintf('[dry-run] would process %d iterations\n', total);
        for o = 1:nargout
            varargout{o} = [];
        end
        if nargout == 0
            varargout{1} = [];
        end
    else
        % scifor:NoData means the combo simply has no backing data (expected
        % in a sparse schema-key cross-product) -- split it out from genuine
        % fn failures so the summary doesn't cry "failed" over missing data.
        no_data_count = 0;
        for fr = 1:numel(failure_order)
            if startsWith(failure_order{fr}, 'scifor:NoData:')
                no_data_count = no_data_count + numel(failure_reasons(failure_order{fr}));
            end
        end
        scifor.Log.info( ...
            'for_each(%s) done in %.1fs: completed=%d, failed=%d, no_data=%d, total=%d', ...
            fn_name, toc(loop_t0), completed, skipped - no_data_count, ...
            no_data_count, total);
        % One line per distinct failure reason (combos capped at 5), so the
        % default (INFO) log answers "what failed and why".
        SUMMARY_COMBOS_MAX = 5;
        for fr = 1:numel(failure_order)
            reason = failure_order{fr};
            combos_for_reason = failure_reasons(reason);
            n_shown = min(numel(combos_for_reason), SUMMARY_COMBOS_MAX);
            shown = strjoin(combos_for_reason(1:n_shown), '; ');
            if numel(combos_for_reason) > n_shown
                shown = sprintf('%s (+%d more)', shown, ...
                    numel(combos_for_reason) - n_shown);
            end
            if startsWith(reason, 'scifor:NoData:')
                label = 'no data';
            else
                label = 'failed';
            end
            scifor.Log.info('%s: %d × "%s" — %s', ...
                label, numel(combos_for_reason), reason, shown);
        end
        if n_outputs == 0
            % Zero-output function: nothing to collect
            if nargout > 0
                for o = 1:nargout
                    varargout{o} = table();
                end
            end
        else
            output_tables = cell(1, n_outputs);
            for o = 1:n_outputs
                output_tables{o} = build_single_output_table( ...
                    collected_per_output{o}, resolved_output_names{o}, opts.categorical, effective_keys, opts.nest_table_outputs, schema_col_types, ...
                    distribute_key, distribute_key_synthetic);
            end
            n_return = max(nargout, 1);
            for o = 1:n_return
                if o <= n_outputs
                    varargout{o} = output_tables{o};
                else
                    varargout{o} = table();
                end
            end
        end
    end
end


% =========================================================================
% Input classification
% =========================================================================

function tf = is_data_input(var_spec)
%IS_DATA_INPUT  Check if an input spec is a data input (table, Fixed, Merge, ColumnSelection).
%   Returns false for plain constants (numeric, string, logical, etc.).
    tf = istable(var_spec) ...
      || (isa(var_spec, 'scifor.Fixed') && ~isa(var_spec.data, 'scifor.PathInput')) ...
      || isa(var_spec, 'scifor.Merge') ...
      || isa(var_spec, 'scifor.ColumnSelection');
end


function tf = is_metadata_compatible(val)
%IS_METADATA_COMPATIBLE  Return true if val can be used as a save metadata key.
    tf = (isnumeric(val) && isscalar(val)) ...
      || (islogical(val) && isscalar(val)) ...
      || (isstring(val) && isscalar(val)) ...
      || ischar(val) ...
      || isstruct(val);
end


% =========================================================================
% Input preparation per combo
% =========================================================================

function result = prepare_input(var_spec, metadata, schema_keys, as_table, where_filter, mapping_cols)
%PREPARE_INPUT  Prepare a single data input for the current combo.
%
%   MAPPING_COLS (optional) is a string array of data column names declaring
%   that this input's rows are struct records spread one column per field.
%   It only reaches extract_data, so a column selection or as_table wins.

    if nargin < 6
        mapping_cols = string.empty;
    end

    % Merge
    if isa(var_spec, 'scifor.Merge')
        result = prepare_merge(var_spec, metadata, schema_keys, where_filter);
        return;
    end

    % Fixed wrapping Merge is not allowed
    if isa(var_spec, 'scifor.Fixed') && isa(var_spec.data, 'scifor.Merge')
        error('scifor:for_each', ...
            'Fixed cannot wrap a Merge. Use Fixed on individual constituents inside the Merge instead: Merge(Fixed(tbl1, ...), tbl2)');
    end

    % Resolve the raw table and effective metadata
    [tbl, effective_meta, col_sel] = resolve_data_spec(var_spec, metadata);

    % Check if this is a per-combo table or a constant table
    if ~is_per_combo_table(tbl, schema_keys)
        % Constant table — pass unchanged every iteration
        if ~isempty(col_sel)
            result = apply_column_selection_on_table(tbl, col_sel);
        else
            result = tbl;
        end
        return;
    end

    % Filter by combo metadata (always returns a table; extraction is done by extract_data)
    filtered = filter_table_for_combo(tbl, effective_meta, schema_keys);

    % Apply where filter (scifor.ColFilter on table rows)
    if ~isempty(where_filter)
        filtered = apply_where_filter(filtered, where_filter);
    end

    % No matching rows → skip this combo (unless as_table, where empty table is valid)
    if height(filtered) == 0 && ~as_table
        error('scifor:NoData', 'No data for this combo after filtering.');
    end

    % Column selection
    if ~isempty(col_sel)
        if as_table
            % Keep schema columns alongside selected data columns
            schema_cols = intersect(string(filtered.Properties.VariableNames), schema_keys, 'stable');
            keep_cols = [schema_cols(:)', col_sel(:)'];
            result = filtered(:, cellstr(keep_cols));
        else
            result = apply_column_selection_on_table(filtered, col_sel);
        end
        return;
    end

    % Extract data (drop schema cols, extract scalar if 1 row + 1 data col)
    result = extract_data(filtered, schema_keys, as_table, mapping_cols);
end


function cs = unwrap_column_selection(var_spec)
%UNWRAP_COLUMN_SELECTION  Return the scifor.ColumnSelection inside a spec
%   (bare or Fixed-wrapped), or [] if the spec is not a ColumnSelection.
    if isa(var_spec, 'scifor.ColumnSelection')
        cs = var_spec;
    elseif isa(var_spec, 'scifor.Fixed') && isa(var_spec.data, 'scifor.ColumnSelection')
        cs = var_spec.data;
    else
        cs = [];
    end
end


function result = prepare_iterate_table(var_spec, metadata, schema_keys, where_filter)
%PREPARE_ITERATE_TABLE  Prepare the per-combo table for an iterate-mode
%   ColumnSelection (for_columns). Returns the combo-filtered table retaining
%   all iterate columns so the caller can slice one column at a time. Unlike
%   prepare_input it does not collapse to a single column.
    [tbl, effective_meta, ~] = resolve_data_spec(var_spec, metadata);

    if ~is_per_combo_table(tbl, schema_keys)
        % Constant table — pass unchanged every iteration
        result = tbl;
        return;
    end

    filtered = filter_table_for_combo(tbl, effective_meta, schema_keys);

    if ~isempty(where_filter)
        filtered = apply_where_filter(filtered, where_filter);
    end

    if height(filtered) == 0
        error('scifor:NoData', 'No data for this combo after filtering.');
    end

    result = filtered;
end


function result_tbl = run_column_iteration(fn, base_args, iterate_pos, iterate_tables, iterate_columns, schema_keys, as_table_set, input_names)
%RUN_COLUMN_ITERATION  Run fn once per column and reassemble into a single
%   one-row wide table. For each column, every iterate input is sliced to that
%   column and passed positionally; non-iterate inputs/constants in BASE_ARGS
%   pass through unchanged. An iterate input named in AS_TABLE_SET is sliced to
%   a table holding all schema key columns plus that one column (mirroring the
%   non-iterate ColumnSelection as_table behavior in prepare_input); otherwise
%   it is sliced to that column's bare values (the default).
%
%   The per-column return is expanded into output columns by
%   EXPAND_COLUMN_RESULT: a scalar yields one column named after the source
%   column; a struct / 1-row table yields one column per field/variable, named
%   "<col><sep><key>". Different source columns may return different numbers
%   (and names) of outputs — the reassembled row is the ordered concatenation
%   of every produced column, so a single for_each call supports an arbitrary,
%   per-column-varying number of outputs.
    sep = '__';
    n = numel(iterate_columns);
    iter_positions = find(iterate_pos);
    % Constant positions holding a deferred ColName() marker resolve to the name
    % of the column currently being iterated (recomputed each pass below).
    deferred_colname_positions = [];
    % Char/string constants carrying a literal {ColName} token (pre-resolved
    % PathOutput paths keep the token for per-column substitution here).
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
    for ci = 1:n
        col = char(iterate_columns(ci));
        call_args = base_args;
        for di = 1:numel(deferred_colname_positions)
            call_args{deferred_colname_positions(di)} = col;
        end
        for di = 1:numel(colname_token_positions)
            k_tok = colname_token_positions(di);
            call_args{k_tok} = strrep( ...
                char(string(base_args{k_tok})), '{ColName}', col);
        end
        for pi = 1:numel(iter_positions)
            p = iter_positions(pi);
            t = iterate_tables{p};
            wants_table = ~isempty(as_table_set) && ...
                ismember(string(input_names{p}), as_table_set);
            if wants_table
                % Keep all schema key columns alongside the current column.
                schema_cols = intersect( ...
                    string(t.Properties.VariableNames), schema_keys, 'stable');
                keep_cols = [schema_cols(:)', string(col)];
                call_args{p} = t(:, cellstr(keep_cols));
                continue
            end
            vals = t.(col);
            if iscell(vals) && isscalar(vals)
                vals = vals{1};
            end
            call_args{p} = vals;
        end
        res = fn(call_args{:});
        % for_columns: res is a plain value. Upstream provenance is recorded
        % at save time from input record_ids (no per-call lineage wrapper).
        [names_i, values_i] = expand_column_result(col, res, sep);
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
    end
    result_tbl = cell2table(out_values, 'VariableNames', out_names);
end


function cols = all_data_columns(tbl, schema_keys)
%ALL_DATA_COLUMNS  A table's data columns: everything that is not a schema key
%   or an internal ``__*`` column. Used to expand an empty (all-columns)
%   ColumnSelection to a concrete list.
    vn = string(tbl.Properties.VariableNames);
    is_internal = startsWith(vn, "__");
    is_schema = ismember(vn, string(schema_keys));
    cols = vn(~is_schema & ~is_internal);
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


function [tbl, effective_meta, col_sel] = resolve_data_spec(var_spec, metadata)
%RESOLVE_DATA_SPEC  Resolve a var_spec into (table, effective_metadata, column_selection).
    col_sel = string.empty;

    if isa(var_spec, 'scifor.Fixed')
        % Apply fixed overrides to metadata
        effective_meta = metadata;
        fixed_fields = fieldnames(var_spec.fixed_metadata);
        for f = 1:numel(fixed_fields)
            effective_meta.(fixed_fields{f}) = var_spec.fixed_metadata.(fixed_fields{f});
        end
        inner = var_spec.data;
        if isa(inner, 'scifor.ColumnSelection')
            col_sel = inner.columns;
            tbl = inner.data;
        else
            tbl = inner;
        end
    elseif isa(var_spec, 'scifor.ColumnSelection')
        effective_meta = metadata;
        col_sel = var_spec.columns;
        tbl = var_spec.data;
    else
        % Plain MATLAB table
        effective_meta = metadata;
        tbl = var_spec;
    end
end


function tf = is_per_combo_table(tbl, schema_keys)
%IS_PER_COMBO_TABLE  True if table has at least one column that is a schema key.
    if isempty(schema_keys)
        tf = false;
        return;
    end
    col_names = string(tbl.Properties.VariableNames);
    tf = any(ismember(col_names, schema_keys));
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


function tf = all_identical(col_data)
%ALL_IDENTICAL  Return true if every element in col_data is the same.
    if isnumeric(col_data) || islogical(col_data)
        tf = all(col_data == col_data(1));
    else
        tf = all(string(col_data) == string(col_data(1)));
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


% =========================================================================
% Table filtering
% =========================================================================

function result = filter_table_for_combo(tbl, metadata, schema_keys)
%FILTER_TABLE_FOR_COMBO  Filter a MATLAB table to rows matching the combo metadata.
%
%   Always returns a filtered table. Extraction is done separately by extract_data.
%   If the table has schema key columns -> filter rows.
%   If not -> return as-is (constant table).

    col_names = string(tbl.Properties.VariableNames);
    schema_keys_in_tbl = intersect(col_names, schema_keys);

    if isempty(schema_keys_in_tbl)
        % Constant table — pass unchanged
        result = tbl;
        return;
    end

    % Build row mask
    mask = true(height(tbl), 1);
    for k = 1:numel(schema_keys_in_tbl)
        key = schema_keys_in_tbl(k);
        if isfield(metadata, char(key))
            val = metadata.(char(key));
            col_data = tbl.(char(key));
            if isnumeric(col_data)
                if isstring(val) || ischar(val)
                    val = str2double(string(val));
                end
                mask = mask & (col_data == val);
            else
                mask = mask & (string(col_data) == string(val));
            end
        end
    end

    result = tbl(mask, :);
end


% =========================================================================
% Merge handling
% =========================================================================

function result = prepare_merge(merge_spec, metadata, schema_keys, where_filter)
%PREPARE_MERGE  Filter each constituent of a Merge and combine into a single table.
    n = numel(merge_spec.tables);
    parts = cell(1, n);

    % Track the constant value of every schema-key column dropped from a
    % single-row / constant constituent.  A free (non-iterated) dimension —
    % e.g. session when iterating at the subject level and every constituent
    % has just one matching row (a Variant-pinned merge) — would otherwise be
    % dropped from all constituents and never restored, because the add-back
    % step below only knows the iterated keys in `metadata`.  Remembering the
    % dropped value lets us restore it.  A key whose constituents disagree on
    % the constant value is genuinely ambiguous (broadcast), so we mark it
    % conflicting and leave it out rather than pinning an arbitrary value.
    dropped_const = containers.Map('KeyType', 'char', 'ValueType', 'any');
    dropped_conflict = containers.Map('KeyType', 'char', 'ValueType', 'logical');

    for i = 1:n
        spec = merge_spec.tables{i};

        [tbl, effective_meta, col_sel] = resolve_data_spec(spec, metadata);

        if is_per_combo_table(tbl, schema_keys)
            filtered = filter_table_for_combo(tbl, effective_meta, schema_keys);
            % Drop only CONSTANT schema key columns (all-identical within
            % this combo).  Varying schema keys (e.g. session when iterating
            % at the subject level) are kept for the column-wise merge/join.
            col_names = string(filtered.Properties.VariableNames);
            cols_to_drop = string.empty;
            for sk_i = 1:numel(schema_keys)
                sk = schema_keys(sk_i);
                if ismember(sk, col_names)
                    col_data = filtered.(char(sk));
                    if height(filtered) <= 1 || all_identical(col_data)
                        cols_to_drop(end+1) = sk; %#ok<AGROW>
                        % Remember the constant value so a non-iterated schema
                        % key can be restored after the merge (see add-back).
                        ck = char(sk);
                        if iscell(col_data)
                            cv = col_data{1};
                        else
                            cv = col_data(1);
                        end
                        if isKey(dropped_const, ck)
                            if ~isequaln(dropped_const(ck), cv)
                                dropped_conflict(ck) = true;
                            end
                        else
                            dropped_const(ck) = cv;
                        end
                    end
                end
            end
            keep_cols = setdiff(col_names, cols_to_drop, 'stable');
            if ~isempty(keep_cols) && numel(keep_cols) < numel(col_names)
                part_tbl = filtered(:, cellstr(keep_cols));
            else
                part_tbl = filtered;
            end
        else
            part_tbl = tbl;
        end

        % Apply column selection
        if ~isempty(col_sel)
            for ci = 1:numel(col_sel)
                if ~ismember(col_sel(ci), part_tbl.Properties.VariableNames)
                    error('scifor:Merge', ...
                        'Column ''%s'' not found in merge constituent %d. Available: %s', ...
                        col_sel(ci), i, strjoin(part_tbl.Properties.VariableNames, ', '));
                end
            end
            if numel(col_sel) == 1
                col_data = part_tbl.(char(col_sel(1)));
                part_tbl = table(col_data, 'VariableNames', {char(col_sel(1))});
            else
                part_tbl = part_tbl(:, cellstr(col_sel));
            end
        end

        parts{i} = part_tbl;
    end

    merged = merge_parts_columnwise(parts);

    % Apply where filter to merged result
    if ~isempty(where_filter)
        merged = apply_where_filter(merged, where_filter);
    end

    % No rows after merge/filter → skip this combo
    if height(merged) == 0
        error('scifor:NoData', 'No data for this combo after merge.');
    end

    % Add back constant schema columns (those not already in the merged table)
    nr = height(merged);
    merged_cols = string(merged.Properties.VariableNames);
    if nr > 0
        schema_tbl = table();
        for k = 1:numel(schema_keys)
            sk = char(schema_keys(k));
            if ismember(string(sk), merged_cols)
                continue;  % already present in the merged result
            end
            % Prefer the iterated combo value; fall back to a constant value
            % dropped from the constituents (a free, non-iterated dimension).
            if isfield(metadata, sk)
                val = metadata.(sk);
            elseif isKey(dropped_const, sk) && ...
                    ~(isKey(dropped_conflict, sk) && dropped_conflict(sk))
                val = dropped_const(sk);
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
    else
        result = merged;
    end
end


function result = merge_parts_columnwise(parts)
%MERGE_PARTS_COLUMNWISE  Merge table fragments column-wise with broadcast.
%   When columns overlap between constituents, performs an inner join on the
%   shared columns.  Otherwise concatenates column-wise, broadcasting
%   single-row tables to match multi-row tables.

    if numel(parts) < 2
        if numel(parts) == 1
            result = parts{1};
        else
            result = table();
        end
        return;
    end

    % Build result iteratively by merging one part at a time
    result = parts{1};
    for i = 2:numel(parts)
        result = merge_two_tables(result, parts{i});
    end
end


function result = merge_two_tables(left, right)
%MERGE_TWO_TABLES  Merge two tables: join on shared columns, broadcast otherwise.
    if isempty(left) || isempty(right)
        error('scifor:Merge', 'Cannot merge: one or more constituents have no data.');
    end

    left_cols = string(left.Properties.VariableNames);
    right_cols = string(right.Properties.VariableNames);
    shared = intersect(left_cols, right_cols, 'stable');

    if ~isempty(shared)
        % Inner join on shared columns
        result = innerjoin(left, right, 'Keys', cellstr(shared));
    else
        % No shared columns — column-wise concatenation with broadcast
        lh = height(left);
        rh = height(right);
        if lh == 1 && rh > 1
            left = repmat(left, rh, 1);
        elseif rh == 1 && lh > 1
            right = repmat(right, lh, 1);
        elseif lh ~= rh && lh > 1 && rh > 1
            error('scifor:Merge', ...
                'Cannot merge constituents with different row counts (%d vs %d).', lh, rh);
        end
        result = [left, right];
    end
end


% =========================================================================
% Empty-list resolution from table inputs (standalone mode)
% =========================================================================

function values = distinct_values_from_inputs(inputs, key)
%DISTINCT_VALUES_FROM_INPUTS  Find distinct values for a key by scanning table inputs.
    all_values = {};
    input_names = fieldnames(inputs);
    for p = 1:numel(input_names)
        tbl = get_raw_table(inputs.(input_names{p}));
        if ~isempty(tbl) && ismember(char(key), tbl.Properties.VariableNames)
            col_data = tbl.(char(key));
            if iscategorical(col_data)
                col_data = decategorize_schema_column(col_data, key, input_names{p});
            end
            if isnumeric(col_data)
                unique_vals = unique(col_data);
                for vi = 1:numel(unique_vals)
                    all_values{end+1} = unique_vals(vi); %#ok<AGROW>
                end
            else
                unique_vals = unique(string(col_data));
                for vi = 1:numel(unique_vals)
                    all_values{end+1} = char(unique_vals(vi)); %#ok<AGROW>
                end
            end
        end
    end

    if isempty(all_values)
        % No table input has this column. Not an error here: a PathInput may
        % still provide the key via filesystem discovery (handled by the
        % caller), which decides whether an unresolved key warns or errors.
        values = {};
        return;
    end

    % Deduplicate
    if isnumeric(all_values{1})
        values = num2cell(unique(cell2mat(all_values)));
    else
        values = unique(all_values);
    end
end


function values = decategorize_schema_column(col_data, key, input_name)
%DECATEGORIZE_SCHEMA_COLUMN  Recover a categorical schema-key column's values.
%   MATLAB categorical stores category labels as text, erasing whether the
%   source column was numeric or string (categorical([1;2]) and
%   categorical(["1";"2"]) are indistinguishable). Schema-key identity must
%   not change type as a side effect of a categorical conversion (e.g.
%   categorical=true on an earlier for_each/load), so recover numerics only
%   when EVERY label round-trips losslessly through str2double
%   ("1" -> 1 -> "1"). Zero-padded labels ("01") fail the round-trip and stay
%   strings verbatim — spelling is identity for undeclared keys (see
%   docs/claude/schema-key-types.md).
    labels = string(col_data);
    nums = str2double(labels);
    lossless = ~isempty(labels) && ~any(ismissing(labels)) ...
        && all(~isnan(nums)) && all(string(nums) == labels);
    if lossless
        values = nums;
        scifor.Log.debug(['schema key ''%s'' (input ''%s''): categorical ' ...
            'column recovered as numeric'], key, input_name);
    else
        values = labels;
        scifor.Log.debug(['schema key ''%s'' (input ''%s''): categorical ' ...
            'column kept as strings (labels are not canonical numeric ' ...
            'spellings)'], key, input_name);
    end
end


function types = capture_schema_column_types(inputs, keys)
%CAPTURE_SCHEMA_COLUMN_TYPES  Record each schema-key input column's type.
%   Scans the raw table inputs and records, per key, the column class plus
%   (for categorical) the category list and ordinality, so output metadata
%   columns can round-trip as EXACTLY the input column type. A key whose
%   input tables disagree on class is marked conflicting and left at the
%   internal canonical type (warned at build time).
    types = struct();
    input_names = fieldnames(inputs);
    for p = 1:numel(input_names)
        tbl = get_raw_table(inputs.(input_names{p}));
        if isempty(tbl)
            continue;
        end
        for k = 1:numel(keys)
            key = char(keys(k));
            if ~ismember(key, tbl.Properties.VariableNames)
                continue;
            end
            col = tbl.(key);
            info = struct('class', class(col), 'conflict', false, ...
                'categories', {{}}, 'ordinal', false);
            if iscategorical(col)
                info.categories = categories(col);
                info.ordinal = isordinal(col);
            end
            if ~isfield(types, key)
                types.(key) = info;
                scifor.Log.debug(['schema key ''%s'' (input ''%s''): input ' ...
                    'column type %s'], key, input_names{p}, info.class);
            elseif ~strcmp(types.(key).class, info.class)
                types.(key).conflict = true;
            end
        end
    end
end


function tbl = restore_schema_column_types(tbl, col_types)
%RESTORE_SCHEMA_COLUMN_TYPES  Cast metadata columns back to the input types
%   captured by capture_schema_column_types. Keys without a captured type
%   (explicit iterables with no table column) keep the iterable's own type.
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


function pi = find_pathinput(inputs)
%FIND_PATHINPUT  Return the first PathInput in inputs (unwrapping Fixed), or [].
    pi = [];
    input_names = fieldnames(inputs);
    for p = 1:numel(input_names)
        v = inputs.(input_names{p});
        if isa(v, 'scifor.PathInput')
            pi = v;
            return;
        end
        if isa(v, 'scifor.Fixed') && isa(v.data, 'scifor.PathInput')
            pi = v.data;
            return;
        end
    end
end


function tf = key_has_source(inputs, key, pi)
%KEY_HAS_SOURCE  True if any table column or PathInput placeholder provides KEY.
    tf = false;
    input_names = fieldnames(inputs);
    for p = 1:numel(input_names)
        tbl = get_raw_table(inputs.(input_names{p}));
        if ~isempty(tbl) && ismember(char(key), tbl.Properties.VariableNames)
            tf = true;
            return;
        end
    end
    if ~isempty(pi) && any(string(pi.placeholder_keys()) == string(key))
        tf = true;
    end
end


function out = project_combos(combos, meta_keys)
%PROJECT_COMBOS  Reduce discovered combos to the iterated keys and dedupe.
%   Each combo is a struct; keep only the META_KEYS fields and drop duplicate
%   projections (e.g. when iterating fewer keys than the template has).
    keys = cellstr(meta_keys);
    seen = containers.Map('KeyType', 'char', 'ValueType', 'logical');
    out = {};
    for c = 1:numel(combos)
        s = struct();
        sig_parts = cell(1, numel(keys));
        for k = 1:numel(keys)
            if isfield(combos{c}, keys{k})
                s.(keys{k}) = combos{c}.(keys{k});
                sig_parts{k} = char(string(combos{c}.(keys{k})));
            else
                sig_parts{k} = '';
            end
        end
        sig = strjoin(sig_parts, '|');
        if ~isKey(seen, sig)
            seen(sig) = true;
            out{end+1} = s; %#ok<AGROW>
        end
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


function tbl = get_raw_table(var_spec)
%GET_RAW_TABLE  Extract the MATLAB table from a var_spec, if it contains one.
    if istable(var_spec)
        tbl = var_spec;
    elseif isa(var_spec, 'scifor.Fixed') && istable(var_spec.data)
        tbl = var_spec.data;
    elseif isa(var_spec, 'scifor.ColumnSelection') && istable(var_spec.data)
        tbl = var_spec.data;
    else
        tbl = [];
    end
end


% =========================================================================
% Return value helpers
% =========================================================================

function tbl = build_single_output_table(collected, output_name, categorical_flag, schema_keys, nest_table_outputs, col_types, distribute_key, distribute_key_synthetic)
%BUILD_SINGLE_OUTPUT_TABLE  Build one result table for a single output.
%
%   collected - cell array of {metadata_struct, value} pairs for one output
%   output_name - column name for non-table values (e.g., 'output')
%   categorical_flag - if true, convert metadata columns to categorical
%   schema_keys - string array of schema keys (for sort order)
%   nest_table_outputs - if true, force nested mode even for table outputs
%   col_types - struct from capture_schema_column_types: per-key input
%       column type to restore on the output metadata columns
%   distribute_key - name of the distribute target key, or '' if not
%       distributing
%   distribute_key_synthetic - true when distribute_key was defaulted to
%       the top of the schema with no source table/iterable to carry a
%       type (see for_each's "nothing iterated" fallback); the column is
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
        tbl = vertcat(parts{:});
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
        % Sort rows by schema columns in schema key order
        sort_cols = intersect(schema_keys, string(meta_fields'), 'stable');
        if ~isempty(sort_cols)
            tbl = sort_by_schema_columns(tbl, sort_cols);
        end

        % Convert to categorical with order matching sorted rows
        for f = 1:numel(meta_fields)
            col = tbl.(meta_fields{f});
            str_col = string(col);
            unique_vals = unique(str_col, 'stable');
            tbl.(meta_fields{f}) = categorical(str_col, unique_vals);
        end
    end
end


% =========================================================================
% Distribute
% =========================================================================

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


% =========================================================================
% Option parsing
% =========================================================================

function [meta_args, opts] = split_options(varargin)
%SPLIT_OPTIONS  Separate known option flags from metadata name-value pairs.
    opts.dry_run = false;
    opts.as_table = string.empty;
    opts.distribute = false;
    opts.where = [];
    opts.categorical = false;
    opts.output_names = {};
    opts.all_combos = [];
    opts.nest_table_outputs = false;
    opts.resolve_pathinput = true;
    opts.pathinput_loader = [];  % optional loader(pi, meta_nv) callback; the
                                 % scidb layer injects its schema-key-type
                                 % policy here so scifor stays policy-free
    opts.log_fn = [];  % deprecated, ignored (kept so parsing stays stable)
    opts.resolved_path_outputs = struct();
    opts.share_limits = struct();
    opts.schema_keys = string.empty;
    opts.mapping_inputs = struct();  % {param -> string array of data column
                                     % names} for struct-valued records; see
                                     % extract_data

    meta_args = {};
    i = 1;
    while i <= numel(varargin)
        key = varargin{i};
        if (ischar(key) || isstring(key))
            switch lower(string(key))
                case "dry_run"
                    opts.dry_run = logical(varargin{i+1});
                    i = i + 2;
                    continue;
                case "as_table"
                    val = varargin{i+1};
                    if islogical(val)
                        opts.as_table = val;
                    elseif isstring(val)
                        opts.as_table = val;
                    elseif ischar(val)
                        opts.as_table = string(val);
                    elseif iscell(val)
                        opts.as_table = string(val);
                    end
                    i = i + 2;
                    continue;
                case "distribute"
                    opts.distribute = logical(varargin{i+1});
                    i = i + 2;
                    continue;
                case "categorical"
                    opts.categorical = logical(varargin{i+1});
                    i = i + 2;
                    continue;
                case "where"
                    opts.where = varargin{i+1};
                    i = i + 2;
                    continue;
                case "output_names"
                    val = varargin{i+1};
                    if isnumeric(val) && isscalar(val)
                        opts.output_names = val;
                    elseif isstring(val)
                        opts.output_names = cellstr(val);
                    elseif iscell(val)
                        opts.output_names = val;
                    end
                    i = i + 2;
                    continue;
                case "_all_combos"
                    opts.all_combos = varargin{i+1};
                    i = i + 2;
                    continue;
                case "_nest_table_outputs"
                    opts.nest_table_outputs = logical(varargin{i+1});
                    i = i + 2;
                    continue;
                case "_resolve_pathinput"
                    opts.resolve_pathinput = logical(varargin{i+1});
                    i = i + 2;
                    continue;
                case "_pathinput_loader"
                    opts.pathinput_loader = varargin{i+1};
                    i = i + 2;
                    continue;
                case "_mapping_inputs"
                    % {param -> string array of data column names} declaring
                    % that the input's rows are struct records spread one
                    % column per field, not the rows of a table. scifor
                    % cannot infer this (a 1xN table looks identical), so
                    % scidb states it -- see +scidb/for_each.m.
                    opts.mapping_inputs = varargin{i+1};
                    i = i + 2;
                    continue;
                case "_resolved_path_outputs"
                    % {param -> cellstr of per-combo paths}, aligned with
                    % _all_combos. Python pre-resolves PathOutput templates
                    % (incl. branch_param placeholders whose dotted names
                    % cannot be MATLAB struct fields); MATLAB just applies.
                    opts.resolved_path_outputs = varargin{i+1};
                    i = i + 2;
                    continue;
                case "share_limits"
                    % struct: input name -> string array of schema keys to
                    % hold fixed (MATLAB port of Python scifor's
                    % _compute_shared_limits prepass).
                    opts.share_limits = varargin{i+1};
                    i = i + 2;
                    continue;
                case "schema_keys"
                    % Schema key names to iterate — structural sugar,
                    % expanded into meta_keys/meta_values below.
                    opts.schema_keys = string(varargin{i+1});
                    i = i + 2;
                    continue;
                case "_log_fn"
                    % Deprecated — ignored. scifor logs through scifor.Log
                    % (scistacklog facade) directly; accepted so existing
                    % call sites don't break.
                    i = i + 2;
                    continue;
            end
        end
        meta_args{end+1} = varargin{i}; %#ok<AGROW>
        i = i + 1;
    end
end


% =========================================================================
% Display helpers
% =========================================================================

function s = format_inputs(inputs, input_names, data_idx)
%FORMAT_INPUTS  Format the inputs struct for display.
    parts = cell(1, numel(input_names));
    for i = 1:numel(input_names)
        var_spec = inputs.(input_names{i});
        if isa(var_spec, 'scifor.Merge')
            sub_parts = cell(1, numel(var_spec.tables));
            for j = 1:numel(var_spec.tables)
                sub = var_spec.tables{j};
                if isa(sub, 'scifor.Fixed')
                    sub_parts{j} = sprintf('Fixed(<table>)');
                elseif istable(sub)
                    sub_parts{j} = sprintf('<table %dx%d>', height(sub), width(sub));
                else
                    sub_parts{j} = class(sub);
                end
            end
            parts{i} = sprintf('%s: Merge(%s)', input_names{i}, strjoin(sub_parts, ', '));
        elseif isa(var_spec, 'scifor.Fixed')
            fields = fieldnames(var_spec.fixed_metadata);
            fixed_parts = cell(1, numel(fields));
            for f = 1:numel(fields)
                val = var_spec.fixed_metadata.(fields{f});
                if isnumeric(val)
                    fixed_parts{f} = sprintf('%s=%g', fields{f}, val);
                else
                    fixed_parts{f} = sprintf('%s=%s', fields{f}, string(val));
                end
            end
            parts{i} = sprintf('%s: Fixed(<table>, %s)', input_names{i}, strjoin(fixed_parts, ', '));
        elseif isa(var_spec, 'scifor.ColumnSelection')
            parts{i} = sprintf('%s: ColumnSelection(<table>, [%s])', input_names{i}, ...
                strjoin('"' + var_spec.columns + '"', ', '));
        elseif data_idx(i)
            if istable(var_spec)
                parts{i} = sprintf('%s: <table %dx%d>', input_names{i}, height(var_spec), width(var_spec));
            else
                parts{i} = sprintf('%s: %s', input_names{i}, class(var_spec));
            end
        else
            parts{i} = sprintf('%s: %s', input_names{i}, format_value(var_spec));
        end
    end
    s = ['{' strjoin(parts, ', ') '}'];
end


function print_dry_run_iteration(inputs, input_names, data_idx, ...
    metadata, metadata_str, distribute_key)
%PRINT_DRY_RUN_ITERATION  Show what would happen for one iteration.
    fprintf('[dry-run] %s:\n', metadata_str);

    for p = 1:numel(input_names)
        var_spec = inputs.(input_names{p});
        if isa(var_spec, 'scifor.Merge')
            fprintf('  merge %s:\n', input_names{p});
            for mi = 1:numel(var_spec.tables)
                sub = var_spec.tables{mi};
                if isa(sub, 'scifor.Fixed')
                    fields = fieldnames(sub.fixed_metadata);
                    fparts = cell(1, numel(fields));
                    for fi = 1:numel(fields)
                        val = sub.fixed_metadata.(fields{fi});
                        if isnumeric(val)
                            fparts{fi} = sprintf('%s=%g', fields{fi}, val);
                        else
                            fparts{fi} = sprintf('%s=%s', fields{fi}, string(val));
                        end
                    end
                    fprintf('    [%d] filter with overrides: %s\n', mi-1, strjoin(fparts, ', '));
                else
                    fprintf('    [%d] filter with metadata: %s\n', mi-1, metadata_str);
                end
            end
        elseif isa(var_spec, 'scifor.Fixed')
            fields = fieldnames(var_spec.fixed_metadata);
            fparts = cell(1, numel(fields));
            for fi = 1:numel(fields)
                val = var_spec.fixed_metadata.(fields{fi});
                if isnumeric(val)
                    fparts{fi} = sprintf('%s=%g', fields{fi}, val);
                else
                    fparts{fi} = sprintf('%s=%s', fields{fi}, string(val));
                end
            end
            fprintf('  filter %s with overrides: %s\n', input_names{p}, strjoin(fparts, ', '));
        elseif isa(var_spec, 'scifor.ColumnSelection')
            fprintf('  filter %s with metadata -> columns: [%s]\n', input_names{p}, ...
                strjoin('"' + var_spec.columns + '"', ', '));
        elseif data_idx(p)
            fprintf('  filter %s with metadata: %s\n', input_names{p}, metadata_str);
        else
            fprintf('  constant %s = %s\n', input_names{p}, format_value(var_spec));
        end
    end

    if strlength(distribute_key) > 0
        fprintf('  distribute by ''%s'' (1-based indexing)\n', distribute_key);
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


function s = format_meta_values(vals)
%FORMAT_META_VALUES  Format a cell array of metadata values for display.
%   {1, 2, 3} -> "[1, 2, 3]"
%   {'pre', 'post'} -> "[pre, post]"
    if isempty(vals)
        s = '[]';
        return;
    end
    parts = cell(1, numel(vals));
    for i = 1:numel(vals)
        v = vals{i};
        if isnumeric(v)
            parts{i} = sprintf('%g', v);
        elseif ischar(v) || isstring(v)
            parts{i} = char(string(v));
        else
            parts{i} = char(string(v));
        end
    end
    s = ['[' strjoin(parts, ', ') ']'];
end


function s = format_value_preview(vals)
%FORMAT_VALUE_PREVIEW  "12 values [1, 2, 3, …, 12]" — truncated preview.
%   Mirrors Python scifor's _format_value_list for the run banner.
    PREVIEW_MAX = 4;
    n = numel(vals);
    full = format_meta_values(vals);
    inner = full(2:end-1);
    if n > PREVIEW_MAX
        head_vals = vals(1:PREVIEW_MAX - 1);
        head = format_meta_values(head_vals);
        tail = format_meta_values(vals(end));
        inner = sprintf('%s, …, %s', head(2:end-1), tail(2:end-1));
    end
    if n == 1
        s = sprintf('1 value [%s]', inner);
    else
        s = sprintf('%d values [%s]', n, inner);
    end
end


function n = count_distinct_combo_values(combos, meta_keys, progress_key)
%COUNT_DISTINCT_COMBO_VALUES  Distinct values of one key across the combos.
%   Used for the "(k/N)" part of the periodic progress line.
    n = 0;
    if isempty(progress_key)
        return;
    end
    seen = {};
    for i = 1:numel(combos)
        combo = combos{i};
        if isstruct(combo)
            if ~isfield(combo, progress_key)
                continue;
            end
            v = combo.(progress_key);
        else
            idx = find(meta_keys == string(progress_key), 1);
            if isempty(idx) || idx > numel(combo)
                continue;
            end
            v = combo{idx};
        end
        key = char(string(v));
        if ~any(strcmp(seen, key))
            seen{end+1} = key; %#ok<AGROW>
        end
    end
    n = numel(seen);
end


function [failure_reasons, failure_order] = record_iteration_failure( ...
        failure_reasons, failure_order, err, metadata_str, context)
%RECORD_ITERATION_FAILURE  Track a per-iteration failure for the summary.
%   Every failure logs a [skip] line at DEBUG; each distinct reason also logs
%   one WARN line, so the default (INFO) log still answers "what failed and
%   why" -- except scifor:NoData, which is an expected outcome (this combo has
%   no backing data) rather than a bug, so it never escalates to WARN.
%
%   The ~20-line MATLAB error report is attached only to the FIRST reason
%   carrying a given error identifier. The reason key is
%   'identifier: message', and messages usually embed the offending value --
%   a file path, say -- so every failing combo is a distinct reason and used to
%   get its own full report. 533 missing files produced ~11,200 log lines that
%   way (2026-09-13). The stack is identical across them; only the message
%   differs, and that is already in the WARN line and the end-of-run summary.
%
%   Whether an identifier has been reported is read back off failure_order
%   rather than kept in new state, so this stays a pure function of the
%   accumulators and a second for_each in the same MATLAB session still gets
%   its own first report.
    reason = sprintf('%s: %s', err.identifier, err.message);
    is_no_data = strcmp(err.identifier, 'scifor:NoData');
    if isKey(failure_reasons, reason)
        failure_reasons(reason) = [failure_reasons(reason), {metadata_str}];
    else
        failure_reasons(reason) = {metadata_str};
        failure_order{end+1} = reason;
        if ~is_no_data
            % Has this identifier already carried a report? Scan everything
            % except the reason just appended. Exits on the first hit, which
            % is the common case once an identifier repeats.
            id_prefix = sprintf('%s: ', err.identifier);
            id_already_reported = false;
            for fi = 1:(numel(failure_order) - 1)
                if startsWith(failure_order{fi}, id_prefix)
                    id_already_reported = true;
                    break;
                end
            end
            if id_already_reported
                scifor.Log.warn('iteration failed: %s — %s: %s', ...
                    metadata_str, context, err.message);
            else
                scifor.Log.warn(['iteration failed: %s — %s: %s ' ...
                    '(first %s; report follows)\n%s'], ...
                    metadata_str, context, err.message, err.identifier, ...
                    getReport(err, 'extended', 'hyperlinks', 'off'));
            end
        end
    end
    scifor.Log.debug('[skip] %s: %s: %s', metadata_str, context, err.message);
end


% =========================================================================
% Cartesian product
% =========================================================================


% =========================================================================
% Utility
% =========================================================================

function tbl = sort_by_schema_columns(tbl, sort_cols)
%SORT_BY_SCHEMA_COLUMNS  Sort table rows by schema columns, numeric-aware.
%   Numeric columns sort numerically. String columns that contain only
%   numeric-like values (e.g., "1", "10", "2") sort numerically rather
%   than alphabetically. Other string columns sort alphabetically.
    n = height(tbl);
    if n <= 1
        return;
    end
    sort_matrix = zeros(n, numel(sort_cols));
    for i = 1:numel(sort_cols)
        col = tbl.(char(sort_cols(i)));
        if isnumeric(col)
            sort_matrix(:, i) = col;
        else
            str_col = string(col);
            nums = str2double(str_col);
            if all(~isnan(nums))
                % All values are numeric-like strings — sort numerically
                sort_matrix(:, i) = nums;
            else
                % Non-numeric strings — sort alphabetically via rank
                [~, ~, sort_matrix(:, i)] = unique(str_col);
            end
        end
    end
    [~, order] = sortrows(sort_matrix);
    tbl = tbl(order, :);
end


% =========================================================================
% share_limits (MATLAB port of Python scifor's _compute_shared_limits)
% =========================================================================

function map = compute_shared_limits(share_limits, inputs, input_names, data_idx, schema_keys)
%COMPUTE_SHARED_LIMITS  Per named input: group its table by the held-fixed
%   keys and compute each group's global numeric [min max] across all data
%   columns (flattening array-valued cells). Mirrors the Python prepass.
    map = struct();
    sl_names = fieldnames(share_limits);
    for i = 1:numel(sl_names)
        name = sl_names{i};
        idx = find(strcmp(input_names, name), 1);
        if isempty(idx) || ~data_idx(idx)
            continue;
        end
        spec = inputs.(name);
        if isa(spec, 'scifor.Fixed')
            t = spec.data;
        elseif isa(spec, 'scifor.ColumnSelection')
            t = spec.data;
        else
            t = spec;
        end
        if ~istable(t)
            continue;
        end
        gkeys = cellstr(string(share_limits.(name)));
        present = gkeys(ismember(gkeys, t.Properties.VariableNames));
        data_cols = setdiff(t.Properties.VariableNames, ...
            cellstr(string(schema_keys)), 'stable');
        keep = ~startsWith(string(data_cols), "x__") & ...
               ~startsWith(string(data_cols), "__");
        data_cols = data_cols(keep);
        if isempty(data_cols)
            continue;
        end
        limits = containers.Map('KeyType', 'char', 'ValueType', 'any');
        if isempty(present)
            % containers.Map rejects '' as a char key; use a sentinel that
            % build_limit_args mirrors for the no-group case.
            [mn, mx] = numeric_extent(t, data_cols);
            if ~isempty(mn)
                limits('<all>') = [mn mx];
            end
        else
            keystrs = strings(height(t), 1);
            for r = 1:height(t)
                parts = strings(1, numel(present));
                for g = 1:numel(present)
                    v = t.(present{g});
                    parts(g) = string(v(r));
                end
                keystrs(r) = strjoin(parts, "|");
            end
            ukeys = unique(keystrs, 'stable');
            for u = 1:numel(ukeys)
                sub = t(keystrs == ukeys(u), :);
                [mn, mx] = numeric_extent(sub, data_cols);
                if ~isempty(mn)
                    limits(char(ukeys(u))) = [mn mx];
                end
            end
        end
        entry = struct();
        entry.group_keys = present;
        entry.limits = limits;
        map.(name) = entry;
    end
end


function [mn, mx] = numeric_extent(t, data_cols)
%NUMERIC_EXTENT  Global numeric min/max over the named columns of a table,
%   flattening numeric-array cells; [] when nothing numeric is found.
    mn = [];
    mx = [];
    for i = 1:numel(data_cols)
        col = t.(data_cols{i});
        if iscell(col)
            for r = 1:numel(col)
                v = col{r};
                if isnumeric(v) && ~isempty(v)
                    v = double(v(:));
                    v = v(~isnan(v));
                    if isempty(v); continue; end
                    if isempty(mn); mn = min(v); mx = max(v);
                    else; mn = min(mn, min(v)); mx = max(mx, max(v));
                    end
                end
            end
        elseif isnumeric(col) && ~isempty(col)
            v = double(col(:));
            v = v(~isnan(v));
            if isempty(v); continue; end
            if isempty(mn); mn = min(v); mx = max(v);
            else; mn = min(mn, min(v)); mx = max(mx, max(v));
            end
        end
    end
end


function warn_on_arity_mismatch(fn, fn_name, input_names, n_limit_args)
%WARN_ON_ARITY_MISMATCH  Warn when the inputs struct cannot fill FN's
%   arguments one-for-one.
%
%   MATLAB has no keyword arguments, so this function binds the inputs struct
%   to FN's arguments strictly BY FIELD ORDER (`call_args = loaded` below,
%   built from `fieldnames(inputs)`). The field NAMES never reach FN. A struct
%   with the wrong number of fields therefore does not error here and does not
%   error in MATLAB — it just calls FN with every argument after the first
%   discrepancy shifted onto the wrong parameter, and the user finds out via
%   whatever type error that eventually causes deep inside their own code (on
%   2026-09-02, `fieldnames()` on a sampling frequency that had landed in a
%   parameter expecting loaded data).
%
%   Advisory, never a refusal: a function may legitimately branch on `nargin`
%   and be called with fewer arguments than it declares. Skipped entirely for
%   varargin (`nargin(fn) < 0` => any count is valid) and for handles MATLAB
%   cannot introspect.
%
%   N_LIMIT_ARGS is the number of trailing `<input>_limits` arguments
%   share_limits will append (see BUILD_LIMIT_ARGS) — those are declared by FN
%   but never appear as struct fields, so they are not a discrepancy.
    try
        cap = nargin(fn);
    catch
        return;  % anonymous or unresolvable handle — nothing to compare to
    end
    if cap < 0 || (numel(input_names) + n_limit_args) == cap
        return;
    end
    scifor.Log.warn(['%s: the inputs struct has %d field(s) [%s] but the ' ...
        'function declares %d argument(s). Inputs bind to arguments BY ' ...
        'POSITION (field names are not used), so argument %d onward will ' ...
        'receive a value meant for a different parameter, or none at all. ' ...
        'Check that every parameter is wired and that the struct is built ' ...
        'in signature order.'], ...
        fn_name, numel(input_names), strjoin(string(input_names'), ', '), ...
        cap, min(numel(input_names), cap) + 1);
end


function args = build_limit_args(map, metadata, fn, n_inputs)
%BUILD_LIMIT_ARGS  Trailing positional limit args for this combo, in
%   share_limits field order. Empty when the fn's declared argument count
%   has no capacity for them (varargin => nargin(fn) < 0 => capacity).
    args = {};
    names = fieldnames(map);
    try
        cap = nargin(fn);
    catch
        cap = -1;
    end
    if cap >= 0 && cap < n_inputs + numel(names)
        return;
    end
    for i = 1:numel(names)
        entry = map.(names{i});
        present = entry.group_keys;
        if isempty(present)
            gkey = '<all>';  % sentinel mirrored from compute_shared_limits
        else
            parts = strings(1, numel(present));
            for g = 1:numel(present)
                gk = present{g};
                if isfield(metadata, gk)
                    parts(g) = string(metadata.(gk));
                else
                    parts(g) = "";
                end
            end
            gkey = char(strjoin(parts, "|"));
        end
        if ~isempty(gkey) && isKey(entry.limits, gkey)
            args{end+1} = entry.limits(gkey); %#ok<AGROW>
        else
            args{end+1} = []; %#ok<AGROW>
        end
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
