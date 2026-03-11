function result_tbl = for_each(fn, inputs, outputs, varargin)
%SCIDB.FOR_EACH  DB-backed for_each — loads inputs, delegates loop to scifor, saves outputs.
%
%   scidb.for_each(@FN, INPUTS, OUTPUTS, Name, Value, ...)
%
%   This is the DB I/O layer. It:
%   1. Resolves empty lists [] via db.distinct_schema_values()
%   2. Pre-filters schema combos via db.distinct_schema_combinations()
%   3. Builds ForEachConfig version keys
%   4. Loads all input variables into MATLAB tables
%   5. Converts scidb wrappers -> scifor wrappers
%   6. Delegates the core loop to scifor.for_each()
%   7. Saves results from the returned table
%
%   Arguments:
%       fn      - Function handle (plain; use scihist.for_each for Thunk wrapping)
%       inputs  - Struct mapping parameter names to BaseVariable instances,
%                 scidb.Fixed wrappers, scidb.Merge wrappers,
%                 scifor.PathInput instances, or constant values.
%       outputs - Cell array of BaseVariable instances for output types
%
%   Name-Value Arguments:
%       dry_run       - If true, preview without executing (default: false)
%       save          - If true, save outputs (default: true)
%       preload       - If true, pre-load all inputs (default: true)
%       parallel      - If true, use 3-phase parallel execution (default: false)
%       distribute    - If true, split outputs by element/row (default: false)
%       db            - Optional DatabaseManager for load/save operations
%       where         - Optional scidb.Filter for input loading
%       as_table      - Controls which inputs are passed as full tables
%       (any other)   - Metadata iterables (numeric or string arrays)
%
%   Returns:
%       result_tbl - MATLAB table with metadata columns and output columns.
%                    Returns [] for dry_run or parallel mode.
%
%   Example:
%       scidb.for_each(@filter_data, ...
%           struct('step_length', StepLength(), 'smoothing', 0.2), ...
%           {FilteredStepLength()}, ...
%           subject=[1 2 3], session=["A" "B"]);

    % Default return value
    result_tbl = [];

    % --- Parse options vs metadata name-value pairs ---
    [meta_args, opts] = split_options(varargin{:});

    dry_run = opts.dry_run;
    do_save = opts.save;
    do_preload = opts.preload;
    as_table_raw = opts.as_table;

    % Build db name-value pair for passthrough to load/save
    if isempty(opts.db)
        db_nv = {};
    else
        db_nv = {'db', opts.db};
    end

    % Build where name-value pair for passthrough to load calls
    where_filter = opts.where;
    if isempty(where_filter)
        where_nv = {};
    else
        where_nv = {'where', where_filter};
    end

    % Get function name for display
    if isa(fn, 'function_handle')
        fn_name = func2str(fn);
    else
        fn_name = 'unknown';
    end

    % Parse metadata iterables
    if mod(numel(meta_args), 2) ~= 0
        error('scidb:for_each', 'Metadata arguments must be name-value pairs.');
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

    % --- Resolve empty arrays from database ---
    needs_resolve = false(1, numel(meta_keys));
    for i = 1:numel(meta_values)
        needs_resolve(i) = isempty(meta_values{i});
    end
    resolve_db = [];
    if any(needs_resolve)
        if isempty(opts.db)
            resolve_db = py.scidb.database.get_database();
        else
            resolve_db = opts.db;
        end
        for i = find(needs_resolve)
            py_vals = resolve_db.distinct_schema_values(char(meta_keys(i)));
            mat_vals = cell(py_vals);
            for j = 1:numel(mat_vals)
                mat_vals{j} = scidb.internal.from_python(mat_vals{j});
            end
            if isempty(mat_vals)
                fprintf('[warn] no values found for ''%s'' in database, 0 iterations\n', ...
                    meta_keys(i));
            end
            meta_values{i} = mat_vals;
        end
    end

    % --- Propagate schema keys to scifor ---
    propagate_schema(opts.db);

    % --- Parse inputs struct — separate loadable from constants ---
    input_names = fieldnames(inputs);
    n_inputs = numel(input_names);

    loadable_idx = false(1, n_inputs);
    constant_names = {};
    constant_values = {};
    constant_nv = {};

    for p = 1:n_inputs
        var_spec = inputs.(input_names{p});
        if is_loadable(var_spec)
            loadable_idx(p) = true;
        else
            constant_names{end+1} = input_names{p}; %#ok<AGROW>
            constant_values{end+1} = var_spec; %#ok<AGROW>
            if is_metadata_compatible(var_spec)
                constant_nv{end+1} = input_names{p}; %#ok<AGROW>
                constant_nv{end+1} = var_spec; %#ok<AGROW>
            end
        end
    end

    % Build ForEachConfig version keys
    config_nv = build_config_nv(fn_name, inputs, input_names, loadable_idx, ...
        where_filter, opts.distribute, as_table_raw);

    % Parse outputs cell array
    n_outputs = numel(outputs);
    output_names = cell(1, n_outputs);
    for o = 1:n_outputs
        parts = strsplit(class(outputs{o}), '.');
        output_names{o} = parts{end};
    end

    % --- Pre-filter to existing schema combinations ---
    all_combos = [];
    if any(needs_resolve) && ~has_pathinput(inputs)
        filter_db = resolve_db;
        db_schema_keys = cell(filter_db.dataset_schema_keys);
        for si = 1:numel(db_schema_keys)
            db_schema_keys{si} = string(db_schema_keys{si});
        end
        db_schema_keys_str = [db_schema_keys{:}];

        schema_indices = [];
        filter_keys = string.empty;
        for ki = 1:numel(meta_keys)
            if ismember(meta_keys(ki), db_schema_keys_str)
                schema_indices(end+1) = ki; %#ok<AGROW>
                filter_keys(end+1) = meta_keys(ki); %#ok<AGROW>
            end
        end

        if ~isempty(filter_keys)
            py_keys = py.list();
            for ki = 1:numel(filter_keys)
                py_keys.append(char(filter_keys(ki)));
            end
            py_existing = filter_db.distinct_schema_combinations(py_keys);
            n_existing = int64(py.len(py_existing));

            existing_set = containers.Map('KeyType', 'char', 'ValueType', 'logical');
            for ei = 1:n_existing
                py_tuple = py_existing{ei};
                combo_parts = cell(1, numel(filter_keys));
                for ki = 1:numel(filter_keys)
                    combo_parts{ki} = char(string(py_tuple{ki}));
                end
                existing_set(strjoin(combo_parts, '|')) = true;
            end

            % Build combos and filter
            if isempty(meta_values)
                raw_combos = {{}};
            else
                raw_combos = cartesian_product(meta_values);
            end
            original_count = numel(raw_combos);
            keep = true(1, original_count);
            for ci = 1:original_count
                combo = raw_combos{ci};
                combo_parts = cell(1, numel(schema_indices));
                for ki = 1:numel(schema_indices)
                    combo_parts{ki} = schema_str(combo{schema_indices(ki)});
                end
                combo_key = strjoin(combo_parts, '|');
                if ~existing_set.isKey(combo_key)
                    keep(ci) = false;
                end
            end

            % Convert to cell array of structs for _all_combos
            filtered_combos = raw_combos(keep);
            all_combos = cell(1, numel(filtered_combos));
            for ci = 1:numel(filtered_combos)
                combo = filtered_combos{ci};
                s = struct();
                for ki = 1:numel(meta_keys)
                    s.(char(meta_keys(ki))) = combo{ki};
                end
                all_combos{ci} = s;
            end

            removed = original_count - numel(filtered_combos);
            if removed > 0
                fprintf('[info] filtered %d non-existent schema combinations (from %d to %d)\n', ...
                    removed, original_count, numel(filtered_combos));
            end
        end
    end

    % --- Parallel branch ---
    if opts.parallel && ~dry_run
        % Parallel stays self-contained — does NOT delegate to scifor
        [completed, skipped, total] = run_parallel(fn, inputs, outputs, ...
            meta_keys, meta_values, input_names, loadable_idx, ...
            constant_names, constant_values, constant_nv, config_nv, ...
            as_table_raw, fn_name, do_save, ...
            db_nv, where_nv, opts);
        fprintf('\n[done] completed=%d, skipped=%d, total=%d\n', ...
            completed, skipped, total);
        result_tbl = [];
        return;
    end

    % --- Resolve database for loading ---
    if isempty(opts.db)
        py_db = py.scidb.database.get_database();
    else
        py_db = opts.db;
    end

    % --- Load all inputs into MATLAB tables ---
    scifor_inputs = struct();
    for p = 1:n_inputs
        param_name = input_names{p};
        var_spec = inputs.(param_name);

        if ~loadable_idx(p)
            % Constant — pass through
            scifor_inputs.(param_name) = var_spec;
            continue;
        end

        % Already a MATLAB table — pass through
        if istable(var_spec)
            scifor_inputs.(param_name) = var_spec;
            continue;
        end

        % Convert scidb wrappers to loaded tables with scifor wrappers
        try
            scifor_inputs.(param_name) = convert_input(var_spec, py_db, where_nv, db_nv);
        catch err
            fprintf('[error] failed to load input ''%s'': %s\n', param_name, err.message);
            result_tbl = [];
            return;
        end
    end

    % --- Build metadata NV args for scifor ---
    scifor_meta_nv = {};
    for k = 1:numel(meta_keys)
        scifor_meta_nv{end+1} = char(meta_keys(k)); %#ok<AGROW>
        vals = meta_values{k};
        if numel(vals) == 1
            scifor_meta_nv{end+1} = vals{1}; %#ok<AGROW>
        else
            if isnumeric(vals{1})
                scifor_meta_nv{end+1} = cell2mat(vals); %#ok<AGROW>
            else
                scifor_meta_nv{end+1} = string(vals); %#ok<AGROW>
            end
        end
    end

    % --- Build scifor options ---
    scifor_opts = {};
    scifor_opts{end+1} = 'dry_run';
    scifor_opts{end+1} = dry_run;
    scifor_opts{end+1} = 'distribute';
    scifor_opts{end+1} = opts.distribute;
    scifor_opts{end+1} = 'output_names';
    scifor_opts{end+1} = output_names;

    if ~isempty(as_table_raw)
        scifor_opts{end+1} = 'as_table';
        scifor_opts{end+1} = as_table_raw;
    end

    if ~isempty(all_combos)
        scifor_opts{end+1} = '_all_combos';
        scifor_opts{end+1} = all_combos;
    end

    % Note: scidb.Filter (where) is applied during loading, NOT passed to scifor.
    % scifor's where= is for scifor.ColFilter on tables.

    % --- Delegate to scifor.for_each ---
    result_tbl = scifor.for_each(fn, scifor_inputs, ...
        scifor_opts{:}, scifor_meta_nv{:});

    if isempty(result_tbl) || dry_run
        return;
    end

    % --- Save results ---
    if do_save && ~isempty(outputs) && ~isempty(result_tbl) && height(result_tbl) > 0
        save_results(result_tbl, outputs, output_names, config_nv, constant_nv, db_nv, py_db);
    end
end


% =========================================================================
% Input loading and conversion
% =========================================================================

function result = convert_input(var_spec, py_db, where_nv, db_nv)
%CONVERT_INPUT  Load a single input and return a scifor-compatible wrapper or table.

    % scidb.Merge -> load each constituent -> scifor.Merge of tables
    if isa(var_spec, 'scidb.Merge')
        loaded_tables = cell(1, numel(var_spec.var_specs));
        for i = 1:numel(var_spec.var_specs)
            loaded_tables{i} = convert_input(var_spec.var_specs{i}, py_db, where_nv, db_nv);
        end
        result = scifor.Merge(loaded_tables{:});
        return;
    end

    % scidb.Fixed -> load inner -> scifor.Fixed with loaded table
    if isa(var_spec, 'scidb.Fixed')
        inner_loaded = convert_input(var_spec.var_type, py_db, where_nv, db_nv);
        fixed_fields = fieldnames(var_spec.fixed_metadata);
        fixed_nv = {};
        for f = 1:numel(fixed_fields)
            fixed_nv{end+1} = fixed_fields{f}; %#ok<AGROW>
            fixed_nv{end+1} = var_spec.fixed_metadata.(fixed_fields{f}); %#ok<AGROW>
        end
        result = scifor.Fixed(inner_loaded, fixed_nv{:});
        return;
    end

    % scifor.PathInput -> return as constant (per-combo resolution happens via fn wrapper)
    if isa(var_spec, 'scifor.PathInput')
        result = var_spec;
        return;
    end

    % MATLAB table -> pass through
    if istable(var_spec)
        result = var_spec;
        return;
    end

    % BaseVariable instance -> bulk load all records into a MATLAB table
    if isa(var_spec, 'scidb.BaseVariable')
        var_inst = var_spec;
        type_name = class(var_inst);
        py_class = scidb.internal.ensure_registered(type_name);

        % Load all data
        if isempty(where_nv)
            bulk = py.sci_matlab.bridge.load_and_extract( ...
                py_class, py.dict(), ...
                pyargs('version_id', 'latest', 'db', py_db));
        else
            bulk = py.sci_matlab.bridge.load_and_extract( ...
                py_class, py.dict(), ...
                pyargs('version_id', 'latest', 'db', py_db, ...
                       'where', where_nv{2}.py_filter));
        end
        n_results = int64(bulk{'n'});

        if n_results == 0
            result = table();
            return;
        end

        % Batch-wrap all results
        results = scidb.BaseVariable.wrap_py_vars_batch(bulk);

        % Convert ThunkOutput array into a MATLAB table with metadata + data cols
        result = thunk_outputs_to_table(results, var_inst);

        % Handle column selection if specified
        if ~isempty(var_inst.selected_columns)
            cols = var_inst.selected_columns;
            result = scifor.ColumnSelection(result, cols);
        end
        return;
    end

    % Unknown -> pass through as constant
    result = var_spec;
end


function tbl = thunk_outputs_to_table(results, var_inst)
%THUNK_OUTPUTS_TO_TABLE  Convert an array of ThunkOutput/BaseVariable into a MATLAB table.
%   Produces a table with metadata columns + data columns, suitable for
%   scifor.for_each to filter per combo.
    n = numel(results);

    % Check if all data items are tables
    all_tables = true;
    for i = 1:n
        if ~istable(results(i).data)
            all_tables = false;
            break;
        end
    end

    if all_tables
        % Table data: metadata columns + data columns, flattened per record
        data_parts = cell(n, 1);
        row_counts = zeros(n, 1);
        for i = 1:n
            data_parts{i} = results(i).data;
            row_counts(i) = height(data_parts{i});
        end
        total_rows = sum(row_counts);
        data_tbl = vertcat(data_parts{:});

        meta_fields = fieldnames(results(1).metadata);
        meta_tbl = table();
        for f = 1:numel(meta_fields)
            val1 = results(1).metadata.(meta_fields{f});
            if isnumeric(val1)
                col = zeros(total_rows, 1);
            elseif isstring(val1) || ischar(val1)
                col = strings(total_rows, 1);
            else
                col = cell(total_rows, 1);
            end

            row_offset = 0;
            for i = 1:n
                nr = row_counts(i);
                idx = row_offset + (1:nr);
                if isfield(results(i).metadata, meta_fields{f})
                    val = results(i).metadata.(meta_fields{f});
                else
                    val = missing;
                end
                if isnumeric(val)
                    col(idx) = double(val);
                elseif isstring(val) || ischar(val)
                    col(idx) = string(val);
                else
                    col(idx) = repmat({val}, nr, 1);
                end
                row_offset = row_offset + nr;
            end
            meta_tbl.(meta_fields{f}) = col;
        end

        tbl = [meta_tbl, data_tbl];
    else
        % Non-table data: nest into a cell/numeric column
        type_parts = strsplit(class(var_inst), '.');
        view_name = type_parts{end};

        meta_fields = fieldnames(results(1).metadata);
        tbl = table();
        for f = 1:numel(meta_fields)
            col_data = cell(n, 1);
            for i = 1:n
                if isfield(results(i).metadata, meta_fields{f})
                    col_data{i} = results(i).metadata.(meta_fields{f});
                else
                    col_data{i} = missing;
                end
            end
            tbl.(meta_fields{f}) = normalize_cell_column(col_data);
        end

        data_col = cell(n, 1);
        for i = 1:n
            data_col{i} = results(i).data;
        end
        tbl.(view_name) = normalize_cell_column(data_col);
    end
end


% =========================================================================
% Saving results
% =========================================================================

function save_results(result_tbl, outputs, output_names, config_nv, constant_nv, db_nv, py_db)
%SAVE_RESULTS  Save results from the result table to output variable types.
    n_outputs = numel(outputs);

    % Determine which columns are metadata (not output names)
    meta_cols = setdiff(result_tbl.Properties.VariableNames, output_names, 'stable');

    % Batch save: accumulate data+metadata, flush once
    use_batch_save = true;

    if use_batch_save
        batch_accum = cell(1, n_outputs);
        for o = 1:n_outputs
            batch_accum{o}.py_data = py.list();
            batch_accum{o}.py_metas = py.list();
            batch_accum{o}.count = 0;
        end

        for ri = 1:height(result_tbl)
            row = result_tbl(ri, :);

            % Build save metadata from metadata columns + constants + config keys
            save_nv = {};
            for mc = 1:numel(meta_cols)
                save_nv{end+1} = meta_cols{mc}; %#ok<AGROW>
                val = row.(meta_cols{mc});
                if iscell(val)
                    save_nv{end+1} = val{1}; %#ok<AGROW>
                else
                    save_nv{end+1} = val; %#ok<AGROW>
                end
            end
            save_nv = [save_nv, constant_nv, config_nv]; %#ok<AGROW>

            for o = 1:n_outputs
                if ~ismember(output_names{o}, row.Properties.VariableNames)
                    continue;
                end
                output_value = row.(output_names{o});
                if iscell(output_value)
                    output_value = output_value{1};
                end

                try
                    batch_accum{o}.py_data.append(scidb.internal.to_python(output_value));
                    batch_accum{o}.py_metas.append(scidb.internal.metadata_to_pydict(save_nv{:}));
                    batch_accum{o}.count = batch_accum{o}.count + 1;
                catch err
                    meta_str = format_save_meta(save_nv);
                    fprintf('[error] %s: failed to convert for batch: %s\n', meta_str, err.message);
                end
            end
        end

        % Flush batch save
        for o = 1:n_outputs
            if batch_accum{o}.count > 0
                type_name = class(outputs{o});
                scidb.internal.ensure_registered(type_name);
                py.sci_matlab.bridge.for_each_batch_save( ...
                    type_name, batch_accum{o}.py_data, ...
                    batch_accum{o}.py_metas, py_db);
                fprintf('[save] %s: %d items (batch)\n', type_name, batch_accum{o}.count);
            end
        end
    end
end


function s = format_save_meta(save_nv)
%FORMAT_SAVE_META  Format save metadata NV pairs for display.
    parts = {};
    for i = 1:2:numel(save_nv)
        key = save_nv{i};
        if numel(key) >= 2 && key(1) == '_' && key(2) == '_'
            continue;  % Skip internal keys
        end
        val = save_nv{i+1};
        if isnumeric(val)
            parts{end+1} = sprintf('%s=%g', key, val); %#ok<AGROW>
        else
            parts{end+1} = sprintf('%s=%s', key, string(val)); %#ok<AGROW>
        end
    end
    s = strjoin(parts, ', ');
end


% =========================================================================
% Parallel execution (3-phase: pre-resolve -> parfor -> batch save)
% =========================================================================

function [completed, skipped, total] = run_parallel(fn, inputs, outputs, ...
    meta_keys, meta_values, input_names, loadable_idx, ...
    constant_names, constant_values, constant_nv, config_nv, ...
    as_table_raw, fn_name, do_save, db_nv, where_nv, opts)
%RUN_PARALLEL  Three-phase parallel execution for for_each.

    n_inputs = numel(input_names);
    n_outputs = numel(outputs);

    % Resolve database
    if isempty(opts.db)
        py_db = py.scidb.database.get_database();
    else
        py_db = opts.db;
    end

    % Build combos
    if isempty(meta_values)
        combos = {{}};
    else
        combos = cartesian_product(meta_values);
    end
    total = numel(combos);

    % Pre-load all inputs
    preloaded_results = cell(1, n_inputs);
    preloaded_maps    = cell(1, n_inputs);
    preloaded_keys    = cell(1, n_inputs);

    for p = 1:n_inputs
        if ~loadable_idx(p); continue; end

        var_spec = inputs.(input_names{p});
        if isa(var_spec, 'scifor.PathInput'); continue; end
        if isa(var_spec, 'scidb.Merge'); continue; end
        if istable(var_spec); continue; end
        if isa(var_spec, 'scidb.Fixed') && istable(var_spec.var_type); continue; end

        if isa(var_spec, 'scidb.Fixed')
            var_inst = var_spec.var_type;
            fixed_meta = var_spec.fixed_metadata;
        else
            var_inst = var_spec;
            fixed_meta = struct();
        end

        type_name = class(var_inst);
        py_class = scidb.internal.ensure_registered(type_name);

        query_nv = {};
        for k = 1:numel(meta_keys)
            query_nv{end+1} = char(meta_keys(k)); %#ok<AGROW>
            vals = meta_values{k};
            if numel(vals) == 1
                query_nv{end+1} = vals{1}; %#ok<AGROW>
            else
                if isnumeric(vals{1})
                    query_nv{end+1} = cell2mat(vals); %#ok<AGROW>
                else
                    query_nv{end+1} = string(vals); %#ok<AGROW>
                end
            end
        end

        fixed_fields = fieldnames(fixed_meta);
        for f = 1:numel(fixed_fields)
            fld_name = fixed_fields{f};
            fval = fixed_meta.(fld_name);
            replaced = false;
            for k = 1:2:numel(query_nv)
                if strcmp(query_nv{k}, fld_name)
                    query_nv{k+1} = fval;
                    replaced = true;
                    break;
                end
            end
            if ~replaced
                query_nv{end+1} = fld_name; %#ok<AGROW>
                query_nv{end+1} = fval; %#ok<AGROW>
            end
        end

        q_keys = string.empty;
        for k = 1:2:numel(query_nv)
            q_keys(end+1) = string(query_nv{k}); %#ok<AGROW>
        end
        preloaded_keys{p} = sort(q_keys);

        py_metadata = scidb.internal.metadata_to_pydict(query_nv{:});
        if isempty(where_nv)
            bulk = py.sci_matlab.bridge.load_and_extract( ...
                py_class, py_metadata, ...
                pyargs('version_id', 'latest', 'db', py_db));
        else
            bulk = py.sci_matlab.bridge.load_and_extract( ...
                py_class, py_metadata, ...
                pyargs('version_id', 'latest', 'db', py_db, ...
                       'where', where_nv{2}.py_filter));
        end
        n_results = int64(bulk{'n'});

        if n_results == 0
            preloaded_results{p} = scidb.ThunkOutput.empty();
            preloaded_maps{p} = containers.Map();
            continue;
        end

        results = scidb.BaseVariable.wrap_py_vars_batch(bulk);
        preloaded_results{p} = results;

        lookup = containers.Map('KeyType', 'char', 'ValueType', 'any');
        for i = 1:numel(results)
            key_str = result_meta_key(results(i).metadata, preloaded_keys{p});
            if lookup.isKey(key_str)
                lookup(key_str) = [lookup(key_str), i];
            else
                lookup(key_str) = i;
            end
        end
        preloaded_maps{p} = lookup;
    end

    % ---- Phase A: Pre-resolve all inputs (serial) ----
    fprintf('[parallel] Phase A: pre-resolving %d combinations...\n', total);

    all_inputs = cell(1, total);
    all_meta_nv = cell(1, total);
    all_save_nv = cell(1, total);
    resolve_ok = false(1, total);

    as_table_set = resolve_as_table_set(as_table_raw, input_names, loadable_idx);
    schema_keys = scifor.get_schema();

    for c = 1:total
        combo = combos{c};

        meta_nv = {};
        meta_parts = {};
        for k = 1:numel(meta_keys)
            val = combo{k};
            meta_nv{end+1} = char(meta_keys(k)); %#ok<AGROW>
            meta_nv{end+1} = val; %#ok<AGROW>
            if isnumeric(val)
                meta_parts{end+1} = sprintf('%s=%g', meta_keys(k), val); %#ok<AGROW>
            else
                meta_parts{end+1} = sprintf('%s=%s', meta_keys(k), string(val)); %#ok<AGROW>
            end
        end
        metadata_str = strjoin(meta_parts, ', ');
        all_meta_nv{c} = meta_nv;
        all_save_nv{c} = [meta_nv, constant_nv, config_nv];

        loaded = cell(1, n_inputs);
        load_failed = false;

        for p = 1:n_inputs
            if ~loadable_idx(p)
                loaded{p} = inputs.(input_names{p});
                continue;
            end

            var_spec = inputs.(input_names{p});

            if isa(var_spec, 'scifor.PathInput')
                error('scidb:for_each', ...
                    'parallel=true is not supported with PathInput.');
            end

            % Table inputs
            if istable(var_spec)
                metadata = struct();
                for k = 1:numel(meta_keys)
                    metadata.(char(meta_keys(k))) = combo{k};
                end
                wants_table = ~isempty(as_table_set) && ismember(string(input_names{p}), as_table_set);
                loaded{p} = filter_table_for_combo_simple(var_spec, metadata, schema_keys, wants_table);
                continue;
            end

            if isa(var_spec, 'scidb.Fixed')
                var_inst = var_spec.var_type;
            else
                var_inst = var_spec;
            end

            if ~isempty(preloaded_maps{p})
                fixed_meta = struct();
                if isa(var_spec, 'scidb.Fixed')
                    fixed_meta = var_spec.fixed_metadata;
                end
                key_str = combo_meta_key(meta_keys, combo, fixed_meta, preloaded_keys{p});

                if preloaded_maps{p}.isKey(key_str)
                    idx = preloaded_maps{p}(key_str);
                    loaded{p} = preloaded_results{p}(idx);
                else
                    fprintf('[skip] %s: no data found for %s (%s)\n', ...
                        metadata_str, input_names{p}, class(var_inst));
                    load_failed = true;
                    break;
                end
            else
                if isa(var_spec, 'scidb.Fixed')
                    load_nv = meta_nv;
                    fixed_fields = fieldnames(var_spec.fixed_metadata);
                    for f = 1:numel(fixed_fields)
                        load_nv{end+1} = fixed_fields{f}; %#ok<AGROW>
                        load_nv{end+1} = var_spec.fixed_metadata.(fixed_fields{f}); %#ok<AGROW>
                    end
                else
                    load_nv = meta_nv;
                end
                try
                    loaded{p} = var_inst.load(load_nv{:}, db_nv{:}, where_nv{:});
                catch err
                    fprintf('[skip] %s: failed to load %s: %s\n', ...
                        metadata_str, input_names{p}, err.message);
                    load_failed = true;
                    break;
                end
            end

            % Unwrap ThunkOutput/BaseVariable
            if ~istable(loaded{p}) && ~isnumeric(loaded{p})
                loaded{p} = scidb.internal.unwrap_input(loaded{p});
            end
        end

        if load_failed
            continue;
        end

        all_inputs{c} = loaded;
        resolve_ok(c) = true;
    end

    n_resolved = sum(resolve_ok);
    fprintf('[parallel] Phase A done: %d resolved, %d skipped\n', ...
        n_resolved, total - n_resolved);

    % ---- Phase B: parfor compute ----
    fprintf('[parallel] Phase B: computing %d items with parfor...\n', n_resolved);

    resolved_indices = find(resolve_ok);
    par_inputs = cell(1, n_resolved);
    par_meta_nv = cell(1, n_resolved);
    for j = 1:n_resolved
        par_inputs{j} = all_inputs{resolved_indices(j)};
        par_meta_nv{j} = all_meta_nv{resolved_indices(j)};
    end

    results_par = cell(1, n_resolved);
    compute_ok = true(1, n_resolved);
    compute_errors = cell(1, n_resolved);

    parfor j = 1:n_resolved
        try
            r = fn(par_inputs{j}{:});
            if ~iscell(r); r = {r}; end
            results_par{j} = r;
        catch err
            compute_ok(j) = false;
            compute_errors{j} = err.message;
            results_par{j} = {};
        end
    end

    for j = find(~compute_ok)
        c = resolved_indices(j);
        combo = combos{c};
        m_parts = {};
        for k = 1:numel(meta_keys)
            val = combo{k};
            if isnumeric(val)
                m_parts{end+1} = sprintf('%s=%g', meta_keys(k), val); %#ok<AGROW>
            else
                m_parts{end+1} = sprintf('%s=%s', meta_keys(k), string(val)); %#ok<AGROW>
            end
        end
        fprintf('[skip] %s: %s raised: %s\n', ...
            strjoin(m_parts, ', '), fn_name, compute_errors{j});
    end

    n_computed = sum(compute_ok);
    fprintf('[parallel] Phase B done: %d succeeded, %d failed\n', ...
        n_computed, n_resolved - n_computed);

    % ---- Phase C: Batch save ----
    if do_save && n_computed > 0
        fprintf('[parallel] Phase C: batch saving %d results...\n', n_computed);

        for o = 1:n_outputs
            type_name = class(outputs{o});
            scidb.internal.ensure_registered(type_name);

            py_data = py.list();
            py_metas = py.list();
            save_count = 0;

            for j = find(compute_ok)
                c = resolved_indices(j);
                if o <= numel(results_par{j})
                    raw_val = results_par{j}{o};
                    if isa(raw_val, 'scidb.ThunkOutput') || isa(raw_val, 'scidb.BaseVariable')
                        raw_val = raw_val.data;
                    end
                    py_data.append(scidb.internal.to_python(raw_val));
                    py_metas.append(scidb.internal.metadata_to_pydict(all_save_nv{c}{:}));
                    save_count = save_count + 1;
                end
            end

            if save_count > 0
                py.sci_matlab.bridge.for_each_batch_save( ...
                    type_name, py_data, py_metas, py_db);
                fprintf('[save] %s: %d items (batch)\n', type_name, save_count);
            end
        end
    end

    completed = n_computed;
    skipped = total - n_computed;
end


% =========================================================================
% Helpers
% =========================================================================

function tf = is_loadable(var_spec)
%IS_LOADABLE  Check if an input spec is loadable (var type, Fixed, Merge, etc.).
    tf = isa(var_spec, 'scidb.BaseVariable') ...
      || isa(var_spec, 'scidb.Fixed') ...
      || isa(var_spec, 'scifor.PathInput') ...
      || isa(var_spec, 'scidb.Merge') ...
      || istable(var_spec) ...
      || (isa(var_spec, 'scidb.Fixed') && istable(var_spec.var_type));
end


function tf = is_metadata_compatible(val)
%IS_METADATA_COMPATIBLE  Return true if val can be used as a save metadata key.
    tf = (isnumeric(val) && isscalar(val)) ...
      || (islogical(val) && isscalar(val)) ...
      || (isstring(val) && isscalar(val)) ...
      || ischar(val) ...
      || isstruct(val);
end


function tf = has_pathinput(inputs)
%HAS_PATHINPUT  Check if any input is a PathInput.
    tf = false;
    fnames = fieldnames(inputs);
    for i = 1:numel(fnames)
        v = inputs.(fnames{i});
        if isa(v, 'scifor.PathInput')
            tf = true; return;
        end
        if isa(v, 'scidb.Fixed') && isa(v.var_type, 'scifor.PathInput')
            tf = true; return;
        end
    end
end


function propagate_schema(db)
%PROPAGATE_SCHEMA  Propagate dataset_schema_keys from the db into scifor.set_schema().
    if ~isempty(db) && ~isa(db, 'double')
        if isprop(db, 'dataset_schema_keys') || isfield(db, 'dataset_schema_keys')
            sk = cell(db.dataset_schema_keys);
            keys = string.empty;
            for s = 1:numel(sk)
                keys(end+1) = string(sk{s}); %#ok<AGROW>
            end
            scifor.set_schema(keys);
            return;
        end
    end

    % Try global database
    try
        py_db = py.scidb.database.get_database();
        sk = cell(py_db.dataset_schema_keys);
        keys = string.empty;
        for s = 1:numel(sk)
            keys(end+1) = string(sk{s}); %#ok<AGROW>
        end
        scifor.set_schema(keys);
    catch
        % No database available — leave schema as-is
    end
end


function as_table_set = resolve_as_table_set(as_table_raw, input_names, loadable_idx)
%RESOLVE_AS_TABLE_SET  Resolve as_table option to a set of input names.
    if islogical(as_table_raw) && isscalar(as_table_raw) && as_table_raw
        as_table_set = string(input_names(loadable_idx)');
    elseif islogical(as_table_raw) && isscalar(as_table_raw) && ~as_table_raw
        as_table_set = string.empty;
    else
        as_table_set = as_table_raw;
    end
end


function result = filter_table_for_combo_simple(tbl, metadata, schema_keys, as_table)
%FILTER_TABLE_FOR_COMBO_SIMPLE  Filter a MATLAB table for parallel branch.
    col_names = string(tbl.Properties.VariableNames);
    schema_keys_in_tbl = intersect(col_names, schema_keys);

    if isempty(schema_keys_in_tbl)
        result = tbl;
        return;
    end

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

    sub = tbl(mask, :);

    if as_table
        result = sub;
        return;
    end

    data_cols = setdiff(col_names, schema_keys, 'stable');
    if height(sub) == 1 && numel(data_cols) == 1
        val = sub.(char(data_cols(1)));
        if iscell(val)
            result = val{1};
        else
            result = val;
        end
    else
        result = sub;
    end
end


% =========================================================================
% Preload lookup helpers
% =========================================================================

function key = build_meta_key(keys, vals)
%BUILD_META_KEY  Build a sorted lookup key from metadata key-value pairs.
    parts = cell(1, numel(keys));
    for k = 1:numel(keys)
        v = vals{k};
        if isnumeric(v)
            parts{k} = sprintf('%s=%g', keys(k), v);
        else
            parts{k} = sprintf('%s=%s', keys(k), string(v));
        end
    end
    key = char(strjoin(sort(string(parts)), '|'));
end


function key = result_meta_key(metadata_struct, query_keys)
%RESULT_META_KEY  Build lookup key from a loaded result's metadata struct.
    vals = cell(1, numel(query_keys));
    for k = 1:numel(query_keys)
        vals{k} = metadata_struct.(char(query_keys(k)));
    end
    key = build_meta_key(query_keys, vals);
end


function key = combo_meta_key(meta_keys, combo, fixed_meta, query_keys)
%COMBO_META_KEY  Build lookup key for a specific iteration combo.
    effective = containers.Map('KeyType', 'char', 'ValueType', 'any');
    for k = 1:numel(meta_keys)
        effective(char(meta_keys(k))) = combo{k};
    end
    ff = fieldnames(fixed_meta);
    for f = 1:numel(ff)
        effective(ff{f}) = fixed_meta.(ff{f});
    end
    vals = cell(1, numel(query_keys));
    for k = 1:numel(query_keys)
        vals{k} = effective(char(query_keys(k)));
    end
    key = build_meta_key(query_keys, vals);
end


% =========================================================================
% ForEachConfig version keys
% =========================================================================

function nv = build_config_nv(fn_name, inputs, input_names, loadable_idx, ...
    where_filter, distribute, as_table_raw)
%BUILD_CONFIG_NV  Build ForEachConfig version keys.
    nv = {};

    nv{end+1} = '__fn';
    nv{end+1} = fn_name;

    inputs_json = serialize_loadable_inputs(inputs, input_names, loadable_idx);
    if ~strcmp(inputs_json, '{}')
        nv{end+1} = '__inputs';
        nv{end+1} = inputs_json;
    end

    if ~isempty(where_filter)
        nv{end+1} = '__where';
        nv{end+1} = char(string(where_filter.py_filter.to_key()));
    end

    if distribute
        nv{end+1} = '__distribute';
        nv{end+1} = true;
    end

    if islogical(as_table_raw) && isscalar(as_table_raw) && as_table_raw
        nv{end+1} = '__as_table';
        nv{end+1} = true;
    elseif isstring(as_table_raw) && ~isempty(as_table_raw)
        nv{end+1} = '__as_table';
        nv{end+1} = strjoin(sort(as_table_raw), ',');
    end

end


function json_str = serialize_loadable_inputs(inputs, input_names, loadable_idx)
%SERIALIZE_LOADABLE_INPUTS  Serialize loadable inputs to a JSON string.
    sorted_names = sort(string(input_names(loadable_idx)'));
    parts = {};
    for i = 1:numel(sorted_names)
        name = sorted_names(i);
        spec = inputs.(char(name));
        key_str = input_spec_to_key(spec);
        parts{end+1} = sprintf('"%s": "%s"', name, strrep(key_str, '"', '\"')); %#ok<AGROW>
    end
    json_str = ['{' strjoin(parts, ', ') '}'];
end


function key = input_spec_to_key(spec)
%INPUT_SPEC_TO_KEY  Convert a single input spec to its canonical key string.
    if isa(spec, 'scidb.Merge')
        sub_parts = cell(1, numel(spec.var_specs));
        for i = 1:numel(spec.var_specs)
            sub_parts{i} = input_spec_to_key(spec.var_specs{i});
        end
        key = ['Merge(' strjoin(sub_parts, ', ') ')'];
    elseif isa(spec, 'scidb.Fixed')
        inner = spec.var_type;
        if isa(inner, 'scidb.BaseVariable') && ~isempty(inner.selected_columns)
            cols = inner.selected_columns;
            if numel(cols) == 1
                inner_key = sprintf('%s[''%s'']', class(inner), cols(1));
            else
                col_strs = arrayfun(@(c) sprintf('''%s''', c), cols, 'UniformOutput', false);
                inner_key = sprintf('%s[[%s]]', class(inner), strjoin(col_strs, ', '));
            end
        else
            inner_key = class(inner);
        end
        fields = sort(string(fieldnames(spec.fixed_metadata)));
        if isempty(fields)
            key = sprintf('Fixed(%s)', inner_key);
        else
            kv_parts = cell(1, numel(fields));
            for f = 1:numel(fields)
                val = spec.fixed_metadata.(char(fields(f)));
                kv_parts{f} = sprintf('%s=%s', fields(f), format_repr(val));
            end
            key = sprintf('Fixed(%s, %s)', inner_key, strjoin(kv_parts, ', '));
        end
    elseif isa(spec, 'scidb.BaseVariable') && ~isempty(spec.selected_columns)
        cols = spec.selected_columns;
        if numel(cols) == 1
            key = sprintf('%s[''%s'']', class(spec), cols(1));
        else
            col_strs = arrayfun(@(c) sprintf('''%s''', c), cols, 'UniformOutput', false);
            key = sprintf('%s[[%s]]', class(spec), strjoin(col_strs, ', '));
        end
    elseif isa(spec, 'scidb.BaseVariable')
        key = class(spec);
    elseif isa(spec, 'scifor.PathInput')
        if strlength(spec.root_folder) > 0
            key = sprintf('PathInput("%s", root_folder="%s")', ...
                spec.path_template, spec.root_folder);
        else
            key = sprintf('PathInput("%s")', spec.path_template);
        end
    elseif istable(spec)
        key = sprintf('<table %dx%d>', height(spec), width(spec));
    else
        key = char(string(spec));
    end
end


function s = format_repr(val)
%FORMAT_REPR  Format a value in Python repr() style for version key strings.
    if isnumeric(val) && isscalar(val)
        if val == floor(val)
            s = sprintf('%d', int64(val));
        else
            s = sprintf('%g', val);
        end
    elseif ischar(val) || (isstring(val) && isscalar(val))
        s = sprintf('''%s''', char(val));
    elseif islogical(val) && isscalar(val)
        if val
            s = 'True';
        else
            s = 'False';
        end
    else
        s = char(string(val));
    end
end


function s = schema_str(value)
%SCHEMA_STR  Stringify a schema key value for comparison with DB strings.
    cl = class(value);
    if numel(cl) >= 3 && cl(1) == 'p' && cl(2) == 'y' && cl(3) == '.'
        value = scidb.internal.from_python(value);
    end
    if isnumeric(value) && isscalar(value)
        if value == floor(value)
            s = sprintf('%d', int64(value));
        else
            s = sprintf('%g', value);
        end
    else
        s = char(string(value));
    end
end


% =========================================================================
% Option parsing
% =========================================================================

function [meta_args, opts] = split_options(varargin)
%SPLIT_OPTIONS  Separate known option flags from metadata name-value pairs.
    opts.dry_run = false;
    opts.save = true;
    opts.preload = true;
    opts.as_table = string.empty;
    opts.db = [];
    opts.parallel = false;
    opts.distribute = false;
    opts.where = [];

    meta_args = {};
    i = 1;
    while i <= numel(varargin)
        key = varargin{i};
        if (ischar(key) || isstring(key))
            switch lower(string(key))
                case "dry_run"
                    opts.dry_run = logical(varargin{i+1});
                    i = i + 2; continue;
                case "save"
                    opts.save = logical(varargin{i+1});
                    i = i + 2; continue;
                case "preload"
                    opts.preload = logical(varargin{i+1});
                    i = i + 2; continue;
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
                    i = i + 2; continue;
                case "db"
                    opts.db = varargin{i+1};
                    i = i + 2; continue;
                case "parallel"
                    opts.parallel = logical(varargin{i+1});
                    i = i + 2; continue;
                case "distribute"
                    opts.distribute = logical(varargin{i+1});
                    i = i + 2; continue;
                case "where"
                    opts.where = varargin{i+1};
                    i = i + 2; continue;
            end
        end
        meta_args{end+1} = varargin{i}; %#ok<AGROW>
        i = i + 1;
    end
end


% =========================================================================
% Utility
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


function col = normalize_cell_column(col_data)
%NORMALIZE_CELL_COLUMN  Convert a cell column to its native type.
    n = numel(col_data);
    all_scalar_numeric = true;
    all_string = true;
    for i = 1:n
        v = col_data{i};
        if ~(isnumeric(v) && isscalar(v))
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
