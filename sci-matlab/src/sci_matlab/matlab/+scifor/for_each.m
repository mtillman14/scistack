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
%                       level below the deepest iterated key. (default: false)
%       where         - Optional scifor.ColFilter to apply to table rows
%                       after combo filtering. (default: [])
%       categorical   - If true, convert metadata columns in result
%                       tables to categorical. (default: false)
%       output_names  - Cell array of strings for result column names
%                       (one per output). Defaults to {'output'} for each.
%       _all_combos   - Pre-built cell array of combo structs (from DB
%                       wrappers that pre-filter). Bypasses cartesian_product.
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

    % Get schema keys
    schema_keys = scifor.get_schema();

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

    % --- Resolve empty arrays from table inputs (standalone mode) ---
    if isempty(opts.all_combos)
        for i = 1:numel(meta_values)
            if isempty(meta_values{i})
                meta_values{i} = distinct_values_from_inputs(inputs, meta_keys(i));
                if isempty(meta_values{i})
                    fprintf('[warn] no values found for ''%s'' in input tables, 0 iterations\n', ...
                        meta_keys(i));
                end
            end
        end
    end

    % --- Determine effective keys for filtering/extraction ---
    %   No schema set → iteration keys are the source of truth.
    %   Schema set    → schema keys are the source of truth.
    if isempty(schema_keys)
        effective_keys = meta_keys;
    else
        effective_keys = schema_keys;
    end

    % --- Validate distribute parameter and resolve target key ---
    distribute_key = '';
    if distribute
        if isempty(schema_keys)
            error('scifor:for_each', ...
                'distribute=true requires schema keys. Call set_schema() first.');
        end

        iter_keys_in_schema = schema_keys(ismember(schema_keys, meta_keys));
        if isempty(iter_keys_in_schema)
            error('scifor:for_each', ...
                'distribute=true requires at least one metadata_iterable that is a schema key.');
        end
        deepest_iterated = iter_keys_in_schema(end);
        deepest_idx = find(schema_keys == deepest_iterated, 1);

        if deepest_idx >= numel(schema_keys)
            error('scifor:for_each', ...
                'distribute=true but ''%s'' is the deepest schema key. There is no lower level to distribute to. Schema order: %s', ...
                deepest_iterated, strjoin(schema_keys, ', '));
        end
        distribute_key = schema_keys(deepest_idx + 1);
    end

    % --- Resolve ColName wrappers before the data/constant split ---
    input_names = fieldnames(inputs);
    for p = 1:numel(input_names)
        var_spec = inputs.(input_names{p});
        if isa(var_spec, 'scifor.ColName')
            inner_tbl = var_spec.data;
            if ~istable(inner_tbl)
                error('scifor:ColName', ...
                    'ColName(%s) expected a table, got %s', ...
                    input_names{p}, class(inner_tbl));
            end
            tbl_cols = string(inner_tbl.Properties.VariableNames);
            data_cols = setdiff(tbl_cols, schema_keys, 'stable');
            if numel(data_cols) == 1
                inputs.(input_names{p}) = char(data_cols(1));
            elseif isempty(data_cols)
                error('scifor:ColName', ...
                    'ColName(%s): table has no data columns (all columns are schema keys). Columns: %s, schema keys: %s', ...
                    input_names{p}, strjoin(tbl_cols, ', '), strjoin(schema_keys, ', '));
            else
                error('scifor:ColName', ...
                    'ColName(%s): table has %d data columns (%s), expected exactly 1. Schema keys: %s', ...
                    input_names{p}, numel(data_cols), strjoin(data_cols, ', '), strjoin(schema_keys, ', '));
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

    % --- Build combo list ---
    if ~isempty(opts.all_combos)
        combos = opts.all_combos;
    elseif isempty(meta_values)
        combos = {{}};
    else
        combos = scidb.internal.cartesian_product(meta_values);
    end

    total = numel(combos);

    % --- Start banner ---
    meta_parts_banner = cell(1, numel(meta_keys));
    for mk = 1:numel(meta_keys)
        meta_parts_banner{mk} = sprintf('%s=[%d values]', meta_keys(mk), numel(meta_values{mk}));
    end
    if isempty(meta_parts_banner)
        meta_summary = 'no metadata';
    else
        meta_summary = strjoin(meta_parts_banner, ', ');
    end
    fprintf('\n%s\n', repmat('=', 1, 64));
    if total == 1
        fprintf('  for_each(%s) — 1 iteration\n', fn_name);
    else
        fprintf('  for_each(%s) — %d iterations\n', fn_name, total);
    end
    fprintf('  %s\n', meta_summary);
    fprintf('%s\n', repmat('=', 1, 64));

    % --- Detailed config: inputs ---
    fprintf('  inputs: %s\n', format_inputs(inputs, input_names, data_idx));

    % --- Detailed config: metadata actual values ---
    for mk2 = 1:numel(meta_keys)
        if ~startsWith(meta_keys(mk2), "__")
            fprintf('  %s=%s\n', meta_keys(mk2), format_meta_values(meta_values{mk2}));
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
        fprintf('  options: %s\n', strjoin(opt_parts, ', '));
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

        % --- Dry-run iteration ---
        if dry_run
            print_dry_run_iteration(inputs, input_names, data_idx, ...
                metadata, metadata_str, distribute_key);
            completed = completed + 1;
            continue;
        end

        % --- Filter/prepare inputs for this combo ---
        loaded = cell(1, n_inputs);
        filter_failed = false;

        for p = 1:n_inputs
            if ~data_idx(p)
                % Constant — pass value directly to user function,
                % or resolve PathInput if _resolve_pathinput is set
                val = inputs.(input_names{p});
                if opts.resolve_pathinput && isa(val, 'scifor.PathInput')
                    loaded{p} = val.load(meta_nv{:});
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
                    loaded{p} = val.data.load(fixed_nv{:});
                else
                    loaded{p} = val;
                end
                continue;
            end

            var_spec = inputs.(input_names{p});
            wants_table = ~isempty(as_table_set) && ismember(string(input_names{p}), as_table_set);

            try
                loaded{p} = prepare_input(var_spec, metadata, effective_keys, wants_table, where_filter);
            catch err
                if strcmp(err.identifier, 'scifor:NoData')
                    skip_msg = sprintf('[skip] %s: no data for %s', ...
                        metadata_str, input_names{p});
                else
                    skip_msg = sprintf('[skip] %s: failed to filter %s: %s', ...
                        metadata_str, input_names{p}, err.message);
                end
                fprintf('%s\n', skip_msg);
                if ~isempty(opts.log_fn)
                    opts.log_fn(skip_msg);
                end
                filter_failed = true;
                break;
            end
        end

        if filter_failed
            skipped = skipped + 1;
            continue;
        end

        % --- Call the function ---
        run_msg = sprintf('[run] %s: %s(%s)', metadata_str, fn_name, ...
            strjoin(string(input_names'), ', '));
        fprintf('%s\n', run_msg);
        if ~isempty(opts.log_fn)
            opts.log_fn(run_msg);
        end

        try
            if n_outputs == 0
                % Zero-output function (e.g. plotting side-effects only)
                fn(loaded{:});
                result = {};
            elseif n_outputs > 1
                fn_nargout = nargout(fn);
                if fn_nargout >= n_outputs || fn_nargout < 0
                    % True multi-output function
                    result = cell(1, n_outputs);
                    [result{1:n_outputs}] = fn(loaded{:});
                else
                    % Single-output function returning a cell array to unpack
                    raw = fn(loaded{:});
                    if iscell(raw) && numel(raw) >= n_outputs
                        result = raw(1:n_outputs);
                    else
                        result = cell(1, n_outputs);
                        result{1} = raw;
                    end
                end
            else
                result = {fn(loaded{:})};
            end
        catch err
            skip_msg = sprintf('[skip] %s: %s raised: %s', ...
                metadata_str, fn_name, err.message);
            fprintf('%s\n', skip_msg);
            if ~isempty(opts.log_fn)
                opts.log_fn(skip_msg);
            end
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
                    err_msg = sprintf('[error] %s: cannot distribute output %d: %s', ...
                        metadata_str, o, err2.message);
                    fprintf('%s\n', err_msg);
                    if ~isempty(opts.log_fn)
                        opts.log_fn(err_msg);
                    end
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

    % --- Summary ---
    fprintf('%s\n', repmat('-', 1, 64));
    if dry_run
        fprintf('  [dry-run] would process %d iterations\n', total);
        fprintf('%s\n\n', repmat('=', 1, 64));
        for o = 1:nargout
            varargout{o} = [];
        end
        if nargout == 0
            varargout{1} = [];
        end
    else
        fprintf('  done: completed=%d, skipped=%d, total=%d\n', ...
            completed, skipped, total);
        fprintf('%s\n\n', repmat('=', 1, 64));
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
                    collected_per_output{o}, resolved_output_names{o}, opts.categorical, effective_keys, opts.nest_table_outputs);
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

function result = prepare_input(var_spec, metadata, schema_keys, as_table, where_filter)
%PREPARE_INPUT  Prepare a single data input for the current combo.

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
    result = extract_data(filtered, schema_keys, as_table);
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


function result = extract_data(tbl, schema_keys, as_table)
%EXTRACT_DATA  Extract data from a filtered table.
%   If as_table: return full table (including schema columns).
%   Otherwise: drop schema key columns that are all-identical (constant
%   within this combo), keep schema keys that still vary. If the result
%   has one data column and one row, extract the scalar value. If multiple
%   columns remain, return as a table.
    if as_table
        result = tbl;
        return;
    end

    % Drop schema key columns that are all-identical in the filtered rows
    col_names = string(tbl.Properties.VariableNames);
    cols_to_drop = string.empty;
    for k = 1:numel(schema_keys)
        sk = schema_keys(k);
        if ismember(sk, col_names)
            col_data = tbl.(char(sk));
            if height(tbl) <= 1 || all_identical(col_data)
                cols_to_drop(end+1) = sk; %#ok<AGROW>
            end
        end
    end
    keep_cols = setdiff(col_names, cols_to_drop, 'stable');

    if height(tbl) == 1 && numel(keep_cols) == 1
        % Extract scalar value
        val = tbl.(char(keep_cols(1)));
        if iscell(val)
            result = val{1};
        else
            result = val;
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
            if isfield(metadata, sk) && ~ismember(string(sk), merged_cols)
                val = metadata.(sk);
                if isnumeric(val) && isscalar(val)
                    schema_tbl.(sk) = repmat(val, nr, 1);
                elseif isstring(val) || ischar(val)
                    schema_tbl.(sk) = repmat(string(val), nr, 1);
                else
                    schema_tbl.(sk) = repmat({val}, nr, 1);
                end
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
        error('scifor:for_each', ...
            'Empty list [] was passed for ''%s'', but no input DataFrame has that column.', key);
    end

    % Deduplicate
    if isnumeric(all_values{1})
        values = num2cell(unique(cell2mat(all_values)));
    else
        values = unique(all_values);
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

function tbl = build_single_output_table(collected, output_name, categorical_flag, schema_keys, nest_table_outputs)
%BUILD_SINGLE_OUTPUT_TABLE  Build one result table for a single output.
%
%   collected - cell array of {metadata_struct, value} pairs for one output
%   output_name - column name for non-table values (e.g., 'output')
%   categorical_flag - if true, convert metadata columns to categorical
%   schema_keys - string array of schema keys (for sort order)
%   nest_table_outputs - if true, force nested mode even for table outputs
%
%   If all values are tables and nest_table_outputs is false →
%       flatten mode (metadata + data columns).
%   Otherwise → nested mode (metadata + single data column).

    if nargin < 5
        nest_table_outputs = false;
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
    opts.log_fn = [];

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
                case "_log_fn"
                    opts.log_fn = varargin{i+1};
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
