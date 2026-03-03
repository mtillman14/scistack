function result_tbl = for_each(fn, inputs, varargin)
%SCIFOR.FOR_EACH  Execute a function for all combinations of metadata.
%
%   result_tbl = scifor.for_each(@FN, INPUTS, Name, Value, ...)
%
%   Pure loop orchestrator — works with MATLAB tables only, no I/O.
%   Iterates over every combination of the supplied metadata values.
%   For each combination it filters table inputs by metadata columns,
%   calls the function, and collects results.
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
%       pass_metadata - If true, pass metadata as trailing name-value
%                       arguments to fn (default: false)
%       as_table      - If true, keep schema key columns when passing
%                       filtered tables. Can be a string array of specific
%                       input names. (default: false)
%       distribute    - If true, split each output by element/row and
%                       expand them into the result table at the schema
%                       level below the deepest iterated key. (default: false)
%       where         - Optional scifor.ColFilter to apply to table rows
%                       after combo filtering. (default: [])
%       output_names  - Cell array of strings for result column names,
%                       or an integer N for auto-named ('output_1', etc.).
%                       Defaults to {'output'} for single output.
%       _all_combos   - Pre-built cell array of combo structs (from DB
%                       wrappers that pre-filter). Bypasses cartesian_product.
%       (any other)   - Metadata iterables (numeric or string arrays)
%
%   Returns:
%       result_tbl - MATLAB table with metadata columns and one column per
%                   output. When all outputs are tables, metadata is
%                   replicated per row and data columns are expanded inline
%                   (flatten mode). Otherwise each combination becomes one
%                   row with output data in a cell column (nested mode).
%                   Returns [] for dry_run.
%
%   Example:
%       set_schema(["subject", "session"])
%       result = scifor.for_each(@filter_data, ...
%           struct('raw', data_table, 'smoothing', 0.2), ...
%           subject=[1 2 3], session=["A" "B"])

    % Default return value
    result_tbl = [];

    % --- Parse options vs metadata name-value pairs ---
    [meta_args, opts] = split_options(varargin{:});

    dry_run = opts.dry_run;
    as_table_raw = opts.as_table;
    distribute = opts.distribute;
    where_filter = opts.where;

    % Auto-detect pass_metadata
    if isempty(opts.pass_metadata)
        should_pass_metadata = false;
    else
        should_pass_metadata = opts.pass_metadata;
    end

    % Get function name for display
    if isa(fn, 'function_handle')
        fn_name = func2str(fn);
    else
        fn_name = 'unknown';
    end

    disp(['Starting for_each(' fn_name ')'])

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
        if n_out > 1
            resolved_output_names = cell(1, n_out);
            for i = 1:n_out
                resolved_output_names{i} = sprintf('output_%d', i);
            end
        else
            resolved_output_names = {"output"};
        end
    elseif isnumeric(opts.output_names) && isscalar(opts.output_names)
        n = opts.output_names;
        resolved_output_names = cell(1, n);
        for i = 1:n
            resolved_output_names{i} = sprintf('output_%d', i);
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

    % --- Parse inputs struct — separate data inputs from constants ---
    input_names = fieldnames(inputs);
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
        combos = cartesian_product(meta_values);
    end

    total = numel(combos);

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
    collected_rows = {};

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
                metadata, metadata_str, should_pass_metadata, distribute_key);
            completed = completed + 1;
            continue;
        end

        % --- Filter/prepare inputs for this combo ---
        loaded = cell(1, n_inputs);
        filter_failed = false;

        for p = 1:n_inputs
            if ~data_idx(p)
                % Constant — use value directly
                loaded{p} = inputs.(input_names{p});
                continue;
            end

            var_spec = inputs.(input_names{p});
            wants_table = ~isempty(as_table_set) && ismember(string(input_names{p}), as_table_set);

            try
                loaded{p} = prepare_input(var_spec, metadata, schema_keys, wants_table, where_filter);
            catch err
                fprintf('[skip] %s: failed to filter %s: %s\n', ...
                    metadata_str, input_names{p}, err.message);
                filter_failed = true;
                break;
            end
        end

        if filter_failed
            skipped = skipped + 1;
            continue;
        end

        % --- Call the function ---
        fprintf('[run] %s: %s(%s)\n', metadata_str, fn_name, ...
            strjoin(string(input_names'), ', '));

        try
            if n_outputs > 1
                result = cell(1, n_outputs);
                if should_pass_metadata
                    [result{1:n_outputs}] = fn(loaded{:}, meta_nv{:});
                else
                    [result{1:n_outputs}] = fn(loaded{:});
                end
            else
                if should_pass_metadata
                    result = fn(loaded{:}, meta_nv{:});
                else
                    result = fn(loaded{:});
                end
                if ~iscell(result)
                    result = {result};
                end
            end
        catch err
            fprintf('[skip] %s: %s raised: %s\n', ...
                metadata_str, fn_name, err.message);
            skipped = skipped + 1;
            continue;
        end

        % Handle distribute: expand result into multiple rows
        if strlength(distribute_key) > 0
            for o = 1:min(n_outputs, numel(result))
                raw_value = result{o};
                try
                    dist_key_char = char(distribute_key);
                    if istable(raw_value)
                        if ismember(dist_key_char, raw_value.Properties.VariableNames)
                            dist_values = raw_value.(dist_key_char);
                            data_tbl = raw_value;
                            data_tbl.(dist_key_char) = [];
                        else
                            dist_values = (1:height(raw_value))';
                            data_tbl = raw_value;
                        end
                        for rowIdx = 1:height(data_tbl)
                            dist_meta = metadata;
                            dist_meta.(dist_key_char) = dist_values(rowIdx);
                            collected_rows{end+1} = {dist_meta, {data_tbl(rowIdx, :)}}; %#ok<AGROW>
                        end
                    else
                        pieces = split_for_distribute(raw_value);
                        for k = 1:numel(pieces)
                            dist_meta = metadata;
                            dist_meta.(dist_key_char) = k;
                            collected_rows{end+1} = {dist_meta, {pieces{k}}}; %#ok<AGROW>
                        end
                    end
                catch err2
                    fprintf('[error] %s: cannot distribute: %s\n', ...
                        metadata_str, err2.message);
                    continue;
                end
            end
        else
            collected_rows{end+1} = {metadata, result}; %#ok<AGROW>
        end

        completed = completed + 1;
    end

    % --- Summary ---
    fprintf('\n');
    if dry_run
        fprintf('[dry-run] would process %d iterations\n', total);
        result_tbl = [];
    else
        fprintf('[done] completed=%d, skipped=%d, total=%d\n', ...
            completed, skipped, total);
        result_tbl = results_to_output_table(collected_rows, resolved_output_names);
    end
end


% =========================================================================
% Input classification
% =========================================================================

function tf = is_data_input(var_spec)
%IS_DATA_INPUT  Check if an input spec is a data input (table, Fixed, Merge, ColumnSelection).
%   Returns false for plain constants (numeric, string, logical, etc.).
    tf = istable(var_spec) ...
      || isa(var_spec, 'scifor.Fixed') ...
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

    % Column selection
    if ~isempty(col_sel)
        result = apply_column_selection_on_table(filtered, col_sel);
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
%   Otherwise: drop schema key columns; if 1 row + 1 data col -> extract scalar.
    if as_table
        result = tbl;
        return;
    end

    col_names = string(tbl.Properties.VariableNames);
    data_cols = setdiff(col_names, schema_keys, 'stable');

    if height(tbl) == 1 && numel(data_cols) == 1
        % Extract scalar value
        val = tbl.(char(data_cols(1)));
        if iscell(val)
            result = val{1};
        else
            result = val;
        end
    elseif ~isempty(data_cols) && numel(data_cols) < numel(col_names)
        sub_tbl = tbl(:, cellstr(data_cols));
        % If all data columns are numeric, return as numeric array (vector or matrix)
        all_numeric = all(cellfun(@(c) isnumeric(sub_tbl.(c)), cellstr(data_cols)));
        if all_numeric
            result = table2array(sub_tbl);
        else
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
            if ~isempty(where_filter)
                filtered = apply_where_filter(filtered, where_filter);
            end
            % Drop schema key columns for merge
            col_names = string(filtered.Properties.VariableNames);
            data_cols = setdiff(col_names, schema_keys, 'stable');
            if ~isempty(data_cols) && numel(data_cols) < numel(col_names)
                part_tbl = filtered(:, cellstr(data_cols));
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

    result = merge_parts_columnwise(parts);
end


function result = merge_parts_columnwise(parts)
%MERGE_PARTS_COLUMNWISE  Merge table fragments column-wise with broadcast.
%   Validates no column conflicts and consistent row counts.
%   Broadcasts single-row tables to match multi-row tables.

    % Check column name conflicts
    seen = {};
    for i = 1:numel(parts)
        cols = parts{i}.Properties.VariableNames;
        for ci = 1:numel(cols)
            if ismember(cols{ci}, seen)
                error('scifor:Merge', ...
                    'Column name conflict in Merge: column ''%s'' appears in multiple constituents.', ...
                    cols{ci});
            end
            seen{end+1} = cols{ci}; %#ok<AGROW>
        end
    end

    % Determine target row count from multi-row parts
    row_counts = cellfun(@height, parts);
    multi_row = row_counts(row_counts > 1);

    if ~isempty(multi_row)
        unique_counts = unique(multi_row);
        if numel(unique_counts) > 1
            error('scifor:Merge', ...
                'Cannot merge constituents with different row counts. All multi-row constituents must have the same number of rows.');
        end
        target_len = unique_counts(1);
    else
        target_len = 1;
    end

    % Broadcast single-row parts and concatenate
    result = table();
    for i = 1:numel(parts)
        tbl_i = parts{i};
        if height(tbl_i) == 1 && target_len > 1
            tbl_i = repmat(tbl_i, target_len, 1);
        end
        result = [result, tbl_i]; %#ok<AGROW>
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

function tbl = results_to_output_table(collected_rows, output_names)
%RESULTS_TO_OUTPUT_TABLE  Build a combined MATLAB table from all for_each results.

    n_rows = numel(collected_rows);
    n_outputs = numel(output_names);

    if n_rows == 0
        tbl = table();
        return;
    end

    % Check whether all output values across all rows are tables (flatten mode)
    all_tables = true;
    for r = 1:n_rows
        if ~all_tables; break; end
        result = collected_rows{r}{2};
        for o = 1:min(n_outputs, numel(result))
            if ~istable(result{o})
                all_tables = false;
                break;
            end
        end
    end

    if all_tables
        % Flatten mode: replicate metadata per data row, expand output columns
        parts = cell(n_rows, 1);
        for r = 1:n_rows
            metadata = collected_rows{r}{1};
            result   = collected_rows{r}{2};

            % Horizontally concatenate all output tables
            combined_data = table();
            for o = 1:min(n_outputs, numel(result))
                combined_data = [combined_data, result{o}]; %#ok<AGROW>
            end
            nr = height(combined_data);

            % Build metadata table with one replicated row per data row
            meta_tbl = table();
            meta_fields = fieldnames(metadata);
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
            parts{r} = [meta_tbl, combined_data];
        end
        tbl = vertcat(parts{:});
    else
        % Nested mode: one row per combination
        tbl = table();
        meta_fields = fieldnames(collected_rows{1}{1});

        % Metadata columns
        for f = 1:numel(meta_fields)
            col_data = cell(n_rows, 1);
            for r = 1:n_rows
                metadata = collected_rows{r}{1};
                if isfield(metadata, meta_fields{f})
                    col_data{r} = metadata.(meta_fields{f});
                else
                    col_data{r} = {missing};
                end
            end
            tbl.(meta_fields{f}) = normalize_cell_column(col_data);
        end

        % Output columns (using output_names)
        for o = 1:n_outputs
            col_data = cell(n_rows, 1);
            for r = 1:n_rows
                result = collected_rows{r}{2};
                if o <= numel(result)
                    col_data{r} = result{o};
                else
                    col_data{r} = {missing};
                end
            end
            tbl.(output_names{o}) = normalize_cell_column(col_data);
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


% =========================================================================
% Option parsing
% =========================================================================

function [meta_args, opts] = split_options(varargin)
%SPLIT_OPTIONS  Separate known option flags from metadata name-value pairs.
    opts.dry_run = false;
    opts.pass_metadata = [];
    opts.as_table = string.empty;
    opts.distribute = false;
    opts.where = [];
    opts.output_names = {};
    opts.all_combos = [];

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
                case "pass_metadata"
                    opts.pass_metadata = logical(varargin{i+1});
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
    metadata, metadata_str, pass_metadata, distribute_key)
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

    if pass_metadata
        fprintf('  pass metadata: %s\n', metadata_str);
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


% =========================================================================
% Cartesian product
% =========================================================================

function combos = cartesian_product(value_cells)
%CARTESIAN_PRODUCT  Compute Cartesian product of cell arrays.
    n = numel(value_cells);
    if n == 0
        combos = {{}};
        return;
    end

    sizes = cellfun(@numel, value_cells);
    idx_args = arrayfun(@(s) 1:s, sizes, 'UniformOutput', false);
    grids = cell(1, n);
    [grids{:}] = ndgrid(idx_args{:});

    total = prod(sizes);
    combos = cell(1, total);
    for t = 1:total
        combo = cell(1, n);
        for d = 1:n
            combo{d} = value_cells{d}{grids{d}(t)};
        end
        combos{t} = combo;
    end
end


% =========================================================================
% Utility
% =========================================================================

function col = normalize_cell_column(col_data)
%NORMALIZE_CELL_COLUMN  Convert a cell column to its native type.
    n = numel(col_data);
    all_scalar_numeric = true;
    all_string = true;
    for i = 1:n
        v = col_data{i};
        if ~((isnumeric(v) || islogical(v)) && isscalar(v))
            all_scalar_numeric = false;
        end
        if ~(isstring(v) || ischar(v))
            all_string = false;
        end
    end
    if all_scalar_numeric
        col = cell2mat(col_data);
    elseif all_string
        col = string(col_data);
    else
        col = col_data;
    end
end
