function result_tbl = for_each(fn, inputs, outputs, varargin)
%SCIDB.FOR_EACH  DB-backed for_each — delegates prepare + save to Python.
%
%   scidb.for_each(@FN, INPUTS, OUTPUTS, Name, Value, ...)
%
%   Two-pass design (see .claude/matlab-for-each-redesign-plan.md Phase 3):
%
%     1. Python ``sci_matlab.bridge.for_each_prepare`` does all pre-loop
%        work (DB-load inputs, __rid_* variant expansion, version-key
%        build, persist expected combos for the GUI).
%     2. MATLAB's existing ``+scifor/for_each.m`` runs the inner loop
%        with the prepared inputs and combo list, calling the user
%        function once per combo.
%     3. Python ``sci_matlab.bridge.for_each_save`` saves the results
%        with branch_params / ``__upstream`` / LineageFcnResult routing.
%
%   MATLAB owns only step 2 and the bridge plumbing. All correctness-
%   sensitive logic (variant tracking, lineage save, version keys) lives
%   in Python so MATLAB-driven and Python-driven pipelines stay in sync.
%
%   Arguments:
%       fn      - Function handle (plain; use scihist.for_each for LineageFcn wrapping)
%       inputs  - Struct mapping parameter names to BaseVariable instances,
%                 scidb.Fixed wrappers, scidb.Merge wrappers,
%                 scifor.PathInput instances, or constant values.
%       outputs - Cell array of BaseVariable instances for output types
%
%   Name-Value Arguments:
%       dry_run       - If true, preview without executing (default: false)
%       save          - If true, save outputs (default: true)
%       distribute    - If true, split outputs by element/row (default: false)
%       db            - Optional DatabaseManager for load/save operations
%       where         - Optional scidb.Filter for input loading
%       as_table      - Controls which inputs are passed as full tables
%       (any other)   - Metadata iterables (numeric or string arrays)
%
%   Returns:
%       result_tbl - MATLAB table with metadata + output columns.
%                    Returns [] for dry_run.

    % Default return value
    result_tbl = [];

    % --- Parse options vs metadata name-value pairs ---
    [meta_args, opts] = split_options(varargin{:});

    dry_run = opts.dry_run;
    do_save = opts.save;
    as_table_raw = opts.as_table;
    where_filter = opts.where;

    % --- Resolve function name + source hash ---
    if isa(fn, 'function_handle')
        fn_name = func2str(fn);
        hash_fn = fn;
    elseif isa(fn, 'scidb.LineageFcn')
        fn_name = func2str(fn.fcn);
        hash_fn = fn.fcn;
    else
        fn_name = 'unknown';
        hash_fn = [];
    end
    if ~isempty(opts.fn_name_override)
        fn_name = opts.fn_name_override;
    end

    if ~isempty(opts.fn_hash_override)
        fn_hash = opts.fn_hash_override;
    elseif ~isempty(hash_fn)
        try
            fn_hash = scidb.internal.hash_function(hash_fn);
        catch
            fn_hash = '';
        end
    else
        fn_hash = '';
    end

    scidb.Log.info('===== for_each(%s) start =====', fn_name);

    % Dry-run is handled below by passing dry_run=true through the bridge
    % so Python's _for_each_prepare can resolve empty [] iterables from
    % the database before invoking scifor.for_each(dry_run=true) itself.

    % --- Parse metadata iterables into a Python dict for the bridge ---
    if mod(numel(meta_args), 2) ~= 0
        error('scidb:for_each', 'Metadata arguments must be name-value pairs.');
    end
    % Track which metadata keys arrived numeric / logical so we can
    % coerce the result table's metadata columns back to MATLAB-native
    % types after save. Python's _for_each_prepare Step 5 stringifies
    % schema-key values for DataFrame-side filtering consistency, but
    % MATLAB callers expect numeric inputs to round-trip as numeric.
    py_meta = py.dict();
    meta_original_classes = containers.Map('KeyType', 'char', 'ValueType', 'char');
    for i = 1:2:numel(meta_args)
        key = char(string(meta_args{i}));
        val = meta_args{i+1};
        meta_original_classes(key) = class(val);
        py_meta{key} = scidb.internal.to_python(val);
    end

    % --- Build kind-tagged inputs spec for the bridge ---
    input_names = fieldnames(inputs);
    py_inputs_spec = py.dict();
    for p = 1:numel(input_names)
        name = input_names{p};
        py_inputs_spec{name} = describe_input_for_python(inputs.(name));
    end

    % --- Build output class names list ---
    n_outputs = numel(outputs);
    output_class_names = cell(1, n_outputs);
    for o = 1:n_outputs
        % outputs may be cell of instances or cell of classes; class()
        % handles both
        if iscell(outputs)
            output_class_names{o} = class(outputs{o});
        else
            output_class_names{o} = class(outputs(o));
        end
        % Ensure each output type is registered Python-side before prepare
        scidb.internal.ensure_registered(output_class_names{o});
    end
    py_output_classes = py.list(output_class_names);

    % --- where filter: ship the Python Filter object directly.
    %     ForEachConfig.to_version_keys handles the .to_key() stringification
    %     for __where; _load_input also expects the live Filter object.
    if isempty(where_filter)
        py_where = py.None;
    else
        py_where = where_filter.py_filter;
    end

    % --- as_table: pass through to bridge as bool / list / None ---
    if islogical(as_table_raw) && isscalar(as_table_raw) && as_table_raw
        py_as_table = true;
    elseif isstring(as_table_raw) && ~isempty(as_table_raw)
        py_as_table = py.list(cellstr(as_table_raw(:)'));
    else
        py_as_table = py.None;
    end

    % --- db: passthrough ---
    if isempty(opts.db)
        py_db = py.None;
    else
        py_db = opts.db;
    end

    % --- Call #1: Python prepare ---
    prep_t0 = tic;
    prep = py.sci_matlab.bridge.for_each_prepare( ...
        fn_name, fn_hash, py_inputs_spec, py_output_classes, py_meta, ...
        pyargs('where', py_where, ...
               'distribute', logical(opts.distribute), ...
               'as_table', py_as_table, ...
               'db', py_db, ...
               'dry_run', logical(dry_run)));
    scidb.Log.info('for_each_prepare returned in %.3fs', toc(prep_t0));

    % Dry-run: Python ran the scifor.for_each(dry_run=true) call itself
    % and returned a stub (handle=-1). Nothing else to do.
    if dry_run
        return;
    end

    handle = int64(prep{'handle'});

    % --- Convert prepared inputs to a MATLAB struct for scifor.
    %     Each loaded value may be a DataFrame, a Python scifor.Fixed /
    %     scifor.ColumnSelection / scifor.Merge wrapper, or a constant.
    %     The bridge describes it as a kind-tagged dict; MATLAB rebuilds
    %     the matching MATLAB classdef wrapper so MATLAB's scifor inner
    %     loop sees the same types a pure-MATLAB call would.
    py_loaded_inputs = prep{'loaded_inputs'};
    scifor_inputs = struct();
    loaded_keys = cell(py.list(py_loaded_inputs.keys()));
    for ki = 1:numel(loaded_keys)
        k = char(loaded_keys{ki});
        desc = py.sci_matlab.bridge.for_each_describe_loaded_input(py_loaded_inputs{k});
        scifor_inputs.(k) = build_scifor_input_from_desc(desc);
        % Python's Step 5 stringifies schema-key columns in loaded
        % DataFrames so DataFrame-side filtering can match user-supplied
        % string-form values. MATLAB user functions that receive the
        % table (as_table=true) expect the original metadata types.
        % Coerce schema-key columns back based on the originally-supplied
        % MATLAB classes tracked in meta_original_classes.
        scifor_inputs.(k) = coerce_meta_columns( ...
            scifor_inputs.(k), meta_original_classes);
    end

    % --- Convert extended_metadata_iterables to scifor name-value pairs ---
    py_meta_iters = prep{'extended_metadata_iterables'};
    scifor_meta_nv = {};
    meta_iter_keys = cell(py.list(py_meta_iters.keys()));
    for ki = 1:numel(meta_iter_keys)
        k = char(meta_iter_keys{ki});
        v_py = py_meta_iters{k};
        scifor_meta_nv{end+1} = k; %#ok<AGROW>
        scifor_meta_nv{end+1} = scidb.internal.from_python(v_py); %#ok<AGROW>
    end

    % --- Convert full_combos (py.list of dicts) to MATLAB cell of structs ---
    py_full_combos = prep{'full_combos'};
    n_combos = int64(py.len(py_full_combos));
    all_combos = cell(1, n_combos);
    for ci = 1:n_combos
        d = py_full_combos{ci};
        s = struct();
        ks = cell(py.list(d.keys()));
        for kii = 1:numel(ks)
            field_name = char(ks{kii});
            % Field names with leading __ become valid MATLAB fields via
            % the same x__ auto-sanitization MATLAB uses for jsondecode;
            % use dynamic field access to preserve original keys, and
            % rely on MATLAB's automatic prefixing for invalid names.
            s.(field_name) = scidb.internal.from_python(d{field_name});
        end
        all_combos{ci} = s;
    end

    % --- Output names returned by Python prepare ---
    py_output_names = prep{'output_names'};
    output_names_cell = cell(py.list(py_output_names));
    output_names = cell(1, numel(output_names_cell));
    for o = 1:numel(output_names_cell)
        output_names{o} = char(output_names_cell{o});
    end

    % --- Build scifor options for the inner loop ---
    scifor_opts = {};
    scifor_opts{end+1} = '_all_combos';
    scifor_opts{end+1} = all_combos;
    scifor_opts{end+1} = '_nest_table_outputs';
    scifor_opts{end+1} = true;
    scifor_opts{end+1} = 'output_names';
    scifor_opts{end+1} = output_names;
    if ~isempty(find_pathinput(inputs))
        scifor_opts{end+1} = '_resolve_pathinput';
        scifor_opts{end+1} = true;
    end
    if ~isempty(as_table_raw)
        scifor_opts{end+1} = 'as_table';
        scifor_opts{end+1} = as_table_raw;
    end
    if opts.distribute
        scifor_opts{end+1} = 'distribute';
        scifor_opts{end+1} = true;
    end
    scifor_opts{end+1} = '_log_fn';
    scifor_opts{end+1} = @(msg) scidb.Log.info('%s', msg);

    % --- MATLAB inner loop: scifor.for_each ---
    scidb.Log.debug('scifor.for_each: %d combo(s), %d input(s), %d output(s)', ...
        n_combos, numel(fieldnames(scifor_inputs)), n_outputs);
    n_out = max(n_outputs, 1);
    result_tables = cell(1, n_out);
    try
        [result_tables{1:n_out}] = scifor.for_each(fn, scifor_inputs, ...
            scifor_opts{:}, scifor_meta_nv{:});
    catch err
        % Surface the error but still attempt to free the prepare-side
        % cache so we don't leak state on partial failures.
        try
            py.sci_matlab.bridge.for_each_save(handle, py.list(), pyargs('save', false));
        catch
            % best-effort cleanup
        end
        % Re-tag scifor-layer configuration errors as scidb errors so
        % callers can catch them with a single identifier.  The scifor
        % message is preserved verbatim; only the identifier changes.
        if startsWith(err.identifier, 'scifor:')
            new_id = strrep(err.identifier, 'scifor:', 'scidb:');
            err_to_throw = MException(new_id, '%s', err.message);
            throw(err_to_throw);
        end
        rethrow(err);
    end

    % --- Convert each result table to a Python DataFrame for the save call.
    %     LineageFcnResult cells get their .py_obj substituted so Python's
    %     save path routes them through scihist's lineage-aware save. ---
    py_result_dfs = py.list();
    for o = 1:n_out
        tbl = result_tables{o};
        if isempty(tbl)
            scidb.Log.warn('scifor output %d (%s): empty table; nothing to save', ...
                o, output_names{o});
            py_result_dfs.append(py.None);
            continue;
        end
        scidb.Log.debug('scifor output %d (%s): table %dx%d cols=%s', ...
            o, output_names{o}, height(tbl), width(tbl), ...
            strjoin(string(tbl.Properties.VariableNames), ', '));
        % If any cell in the output column is a scidb.LineageFcnResult,
        % swap it for its py_obj so Python's save sees a real
        % LineageFcnResult (isinstance pass) and routes via save_lineage_result.
        oname = output_names{o};
        if any(strcmp(tbl.Properties.VariableNames, oname))
            col = tbl.(oname);
            if iscell(col)
                for r = 1:numel(col)
                    if isa(col{r}, 'scidb.LineageFcnResult')
                        col{r} = col{r}.py_obj;
                    end
                end
                tbl.(oname) = col;
            elseif isa(col, 'scidb.LineageFcnResult')
                % Should not happen (non-cell single-value column) but handle defensively
                tbl.(oname) = col.py_obj;
            end
        end
        py_result_dfs.append(scidb.internal.to_python(tbl));
    end

    % --- Call #2: Python save ---
    save_t0 = tic;
    py_result_df = py.sci_matlab.bridge.for_each_save( ...
        handle, py_result_dfs, pyargs('save', logical(do_save)));
    scidb.Log.info('for_each_save returned in %.3fs', toc(save_t0));

    % --- Convert returned DataFrame to MATLAB table ---
    if isa(py_result_df, 'py.NoneType')
        result_tbl = table();
    else
        result_tbl = scidb.internal.from_python(py_result_df);
    end

    scidb.Log.debug('post-save result: %dx%d cols=%s', ...
        height(result_tbl), width(result_tbl), ...
        strjoin(string(result_tbl.Properties.VariableNames), ', '));

    % Flatten any nested-table output columns: when scifor was called with
    % _nest_table_outputs=true, the returned table has one row per combo
    % with each output column carrying a cell containing the per-combo
    % inner table. Users expect a flat result where each row of an inner
    % table becomes its own row in the result, with metadata replicated.
    if ~isempty(result_tbl) && istable(result_tbl)
        result_tbl = flatten_nested_table_outputs(result_tbl, output_names);
        scidb.Log.debug('post-flatten result: %dx%d cols=%s', ...
            height(result_tbl), width(result_tbl), ...
            strjoin(string(result_tbl.Properties.VariableNames), ', '));
    end

    % --- Restore original MATLAB types for metadata columns. Python's
    %     Step 5 stringifies schema-key values so DataFrame-side filtering
    %     is consistent (numeric DB values vs. user-supplied strings); we
    %     reverse that here so numeric inputs round-trip as numeric.
    if ~isempty(result_tbl) && istable(result_tbl)
        meta_keys_tracked = keys(meta_original_classes);
        for mi = 1:numel(meta_keys_tracked)
            k = meta_keys_tracked{mi};
            if ~ismember(k, result_tbl.Properties.VariableNames)
                continue;
            end
            orig_class = meta_original_classes(k);
            col = result_tbl.(k);
            try
                switch orig_class
                    case {'double', 'single', 'int8', 'int16', 'int32', ...
                          'int64', 'uint8', 'uint16', 'uint32', 'uint64'}
                        if isstring(col) || iscellstr(col) || ischar(col)
                            num = str2double(string(col));
                            if all(~isnan(num) | ismissing(string(col)))
                                result_tbl.(k) = cast(num, orig_class);
                            end
                        end
                    case 'logical'
                        if isstring(col) || iscellstr(col)
                            s = lower(string(col));
                            result_tbl.(k) = s == "true" | s == "1";
                        end
                end
            catch
                % Best-effort coercion only; leave column as-is on failure.
            end
        end
    end

    scidb.Log.info('===== for_each(%s) done =====', fn_name);
end


% =========================================================================
% Helpers (kept):
%   - find_pathinput: used by the bridge spec builder and the
%     _resolve_pathinput option
%   - is_loadable / is_metadata_compatible: classification for spec building
%   - describe_input_for_python: kind-tagged spec serializer for Python
%   - split_options: name-value vs option splitter
% =========================================================================

function out = flatten_nested_table_outputs(result_tbl, output_names)
%FLATTEN_NESTED_TABLE_OUTPUTS  Expand nested-table output columns to flat rows.
%   For each row of result_tbl, if an output column's cell holds a table,
%   replicate that row's metadata across the inner table's rows and
%   concat the inner table's data columns. Non-table outputs pass
%   through unchanged. Output columns that contain a mix of tables and
%   non-tables are left as-is on a per-row basis.

    if isempty(result_tbl) || ~istable(result_tbl) || height(result_tbl) == 0
        out = result_tbl;
        return;
    end

    % Identify nested-table output columns (output_names columns that
    % contain at least one inner table).
    nested_cols = string.empty;
    for o = 1:numel(output_names)
        oname = output_names{o};
        if ~ismember(oname, result_tbl.Properties.VariableNames)
            continue;
        end
        col = result_tbl.(oname);
        if iscell(col)
            any_table = false;
            for r = 1:numel(col)
                if istable(col{r})
                    any_table = true;
                    break;
                end
            end
            if any_table
                nested_cols(end+1) = string(oname); %#ok<AGROW>
            end
        end
    end

    if isempty(nested_cols)
        out = result_tbl;
        return;
    end

    meta_cols = setdiff( ...
        string(result_tbl.Properties.VariableNames), ...
        [nested_cols, string(output_names)], 'stable');

    pieces = {};
    for r = 1:height(result_tbl)
        % Find the first non-empty nested table in this row to determine
        % how many rows this combo expands to. All nested columns are
        % expected to share the same height per combo.
        inner_h = 0;
        for nc = 1:numel(nested_cols)
            cell_val = result_tbl.(char(nested_cols(nc))){r};
            if istable(cell_val)
                inner_h = height(cell_val);
                break;
            end
        end
        if inner_h == 0
            % Pass the row through unchanged (no expansion needed).
            pieces{end+1} = result_tbl(r, :); %#ok<AGROW>
            continue;
        end

        % Build the metadata block (replicated)
        meta_row = result_tbl(r, cellstr(meta_cols));
        meta_block = repmat(meta_row, inner_h, 1);

        % For each nested output column, fold the inner table in.
        nested_block = table();
        for nc = 1:numel(nested_cols)
            nc_name = char(nested_cols(nc));
            cell_val = result_tbl.(nc_name){r};
            if istable(cell_val)
                inner_cols = string(cell_val.Properties.VariableNames);
                for ic = 1:numel(inner_cols)
                    icn = char(inner_cols(ic));
                    if ismember(icn, meta_block.Properties.VariableNames)
                        % Inner column matches a metadata column: prefer
                        % the inner value (it's the per-row data the user
                        % chose to include in their output table).
                        meta_block.(icn) = cell_val.(icn);
                    elseif ismember(icn, nested_block.Properties.VariableNames)
                        % Disambiguate name collisions across nested
                        % outputs by prefixing with the output name.
                        nested_block.(sprintf('%s_%s', nc_name, icn)) = cell_val.(icn);
                    else
                        nested_block.(icn) = cell_val.(icn);
                    end
                end
            else
                % Non-table cell — wrap in a cell column of inner_h
                % copies so widths line up.
                nested_block.(nc_name) = repmat({cell_val}, inner_h, 1);
            end
        end

        pieces{end+1} = [meta_block, nested_block]; %#ok<AGROW>
    end

    if isempty(pieces)
        out = result_tbl;
    else
        out = vertcat(pieces{:});
    end
end


function val = coerce_meta_columns(val, meta_original_classes)
%COERCE_META_COLUMNS  Restore original MATLAB types on schema-key columns
%   inside a loaded input.  Handles plain tables and scifor wrappers
%   recursively.  Returns a fresh wrapper because scifor's wrappers
%   have read-only properties.

    if istable(val)
        val = coerce_table_columns(val, meta_original_classes);
        return;
    end
    if isa(val, 'scifor.Fixed')
        if istable(val.data)
            new_data = coerce_table_columns(val.data, meta_original_classes);
        else
            new_data = val.data;
        end
        % Flatten fixed_metadata struct to name-value pairs
        fnames = fieldnames(val.fixed_metadata);
        nv = cell(1, 2 * numel(fnames));
        for i = 1:numel(fnames)
            nv{2*i - 1} = fnames{i};
            nv{2*i} = val.fixed_metadata.(fnames{i});
        end
        val = scifor.Fixed(new_data, nv{:});
        return;
    end
    if isa(val, 'scifor.ColumnSelection')
        if istable(val.data)
            new_data = coerce_table_columns(val.data, meta_original_classes);
        else
            new_data = val.data;
        end
        val = scifor.ColumnSelection(new_data, val.columns);
        return;
    end
    if isa(val, 'scifor.Merge')
        n = numel(val.tables);
        new_tables = cell(1, n);
        for i = 1:n
            inner = val.tables{i};
            if istable(inner)
                new_tables{i} = coerce_table_columns(inner, meta_original_classes);
            else
                % Recurse so nested wrappers also rebuild
                new_tables{i} = coerce_meta_columns(inner, meta_original_classes);
            end
        end
        val = scifor.Merge(new_tables{:});
        return;
    end
end


function tbl = coerce_table_columns(tbl, meta_original_classes)
%COERCE_TABLE_COLUMNS  For each column whose name matches a metadata key
%   we tracked the original MATLAB class for, convert string values back
%   to that numeric / logical type.  Strings the user originally passed
%   stay strings (the tracked class will be 'string' or 'char').
    keys_tracked = keys(meta_original_classes);
    for i = 1:numel(keys_tracked)
        k = keys_tracked{i};
        if ~ismember(k, tbl.Properties.VariableNames)
            continue;
        end
        orig_class = meta_original_classes(k);
        col = tbl.(k);
        try
            switch orig_class
                case {'double', 'single', 'int8', 'int16', 'int32', ...
                      'int64', 'uint8', 'uint16', 'uint32', 'uint64'}
                    if isstring(col) || iscellstr(col)
                        num = str2double(string(col));
                        if all(~isnan(num) | ismissing(string(col)))
                            tbl.(k) = cast(num, orig_class);
                        end
                    end
                case 'logical'
                    if isstring(col) || iscellstr(col)
                        s = lower(string(col));
                        tbl.(k) = s == "true" | s == "1";
                    end
            end
        catch
            % Leave column as-is on any failure
        end
    end
end


function val = build_scifor_input_from_desc(desc)
%BUILD_SCIFOR_INPUT_FROM_DESC  Rebuild a MATLAB scifor wrapper (or table)
%   from a kind-tagged description produced by
%   ``py.sci_matlab.bridge.for_each_describe_loaded_input``.
%
%   ``desc`` is a py.dict with a ``kind`` field. Cases:
%     'dataframe'         -> MATLAB table  (via from_python)
%     'fixed'             -> scifor.Fixed(inner, name, value, ...)
%     'column_selection'  -> scifor.ColumnSelection(inner_table, cols)
%     'merge'             -> scifor.Merge(inner1, inner2, ...)
%     'raw'               -> from_python(value) (constants, etc.)

    kind = char(desc{'kind'});
    switch kind
        case 'dataframe'
            val = scidb.internal.from_python(desc{'data'});

        case 'fixed'
            inner_val = build_scifor_input_from_desc(desc{'inner'});
            % fixed_metadata is a py.dict; flatten to name-value pairs
            py_meta = desc{'fixed_metadata'};
            keys_cell = cell(py.list(py_meta.keys()));
            nv = {};
            for ki = 1:numel(keys_cell)
                k = char(keys_cell{ki});
                nv{end+1} = k; %#ok<AGROW>
                nv{end+1} = scidb.internal.from_python(py_meta{k}); %#ok<AGROW>
            end
            val = scifor.Fixed(inner_val, nv{:});

        case 'column_selection'
            inner_val = build_scifor_input_from_desc(desc{'inner'});
            cols_py = cell(py.list(desc{'columns'}));
            cols = cellfun(@char, cols_py, 'UniformOutput', false);
            val = scifor.ColumnSelection(inner_val, cols);

        case 'merge'
            py_tables = desc{'tables'};
            n = int64(py.len(py_tables));
            tables_cell = cell(1, n);
            for ti = 1:n
                tables_cell{ti} = build_scifor_input_from_desc(py_tables{ti});
            end
            val = scifor.Merge(tables_cell{:});

        case 'pathinput'
            tmpl = char(desc{'template'});
            root = char(desc{'root_folder'});
            is_regex = logical(desc{'regex'});
            if isempty(root)
                val = scifor.PathInput(tmpl, 'regex', is_regex);
            else
                val = scifor.PathInput(tmpl, 'root_folder', root, 'regex', is_regex);
            end

        case 'raw'
            val = scidb.internal.from_python(desc{'value'});

        otherwise
            error('scidb:for_each:UnknownInputKind', ...
                'Unrecognized loaded-input kind from bridge: "%s"', kind);
    end
end


function pi = find_pathinput(inputs)
%FIND_PATHINPUT  Find the first PathInput in inputs, unwrapping Fixed if needed.
    pi = [];
    fnames = fieldnames(inputs);
    for i = 1:numel(fnames)
        v = inputs.(fnames{i});
        if isa(v, 'scifor.PathInput')
            pi = v; return;
        end
        if isa(v, 'scidb.Fixed') && isa(v.var_type, 'scifor.PathInput')
            pi = v.var_type; return;
        end
    end
end


function tf = is_loadable(var_spec) %#ok<DEFNU>
%IS_LOADABLE  Check if an input spec is loadable (var type, Fixed, Merge, etc.).
    tf = isa(var_spec, 'scidb.BaseVariable') ...
      || isa(var_spec, 'scidb.Fixed') ...
      || isa(var_spec, 'scifor.PathInput') ...
      || isa(var_spec, 'scidb.Merge') ...
      || istable(var_spec) ...
      || (isa(var_spec, 'scidb.Fixed') && istable(var_spec.var_type));
end


function tf = is_metadata_compatible(val) %#ok<DEFNU>
%IS_METADATA_COMPATIBLE  Return true if val can be used as a save metadata key.
    tf = (isnumeric(val) && isscalar(val)) ...
      || (islogical(val) && isscalar(val)) ...
      || (isstring(val) && isscalar(val)) ...
      || ischar(val) ...
      || isstruct(val);
end


function spec = describe_input_for_python(val)
%DESCRIBE_INPUT_FOR_PYTHON  Build a kind-tagged Python dict describing one
%   for_each input, for the for_each_prepare bridge.

    if isa(val, 'scidb.Merge')
        sub_specs = cell(1, numel(val.var_specs));
        for i = 1:numel(val.var_specs)
            sub_specs{i} = describe_input_for_python(val.var_specs{i});
        end
        spec = py.dict(pyargs('kind', 'merge', 'specs', py.list(sub_specs)));

    elseif isa(val, 'scidb.Fixed')
        inner_desc = describe_input_for_python(val.var_type);
        fmeta_py = py.dict();
        fnames = fieldnames(val.fixed_metadata);
        for i = 1:numel(fnames)
            fmeta_py{fnames{i}} = scidb.internal.to_python( ...
                val.fixed_metadata.(fnames{i}));
        end
        spec = py.dict(pyargs('kind', 'fixed', ...
            'inner', inner_desc, ...
            'fixed_metadata', fmeta_py));

    elseif isa(val, 'scidb.BaseVariable') && ~isempty(val.selected_columns)
        cols = val.selected_columns;
        scidb.internal.ensure_registered(class(val));
        spec = py.dict(pyargs('kind', 'column_selection', ...
            'type_name', class(val), ...
            'columns', py.list(cellstr(cols(:)'))));

    elseif isa(val, 'scidb.BaseVariable')
        scidb.internal.ensure_registered(class(val));
        spec = py.dict(pyargs('kind', 'var_type', 'type_name', class(val)));

    elseif isa(val, 'scifor.PathInput')
        if strlength(val.root_folder) > 0
            root_str = char(val.root_folder);
        else
            root_str = '';
        end
        spec = py.dict(pyargs('kind', 'pathinput', ...
            'template', char(val.path_template), ...
            'root_folder', root_str, ...
            'regex', logical(val.regex)));

    elseif istable(val)
        % A literal MATLAB table input: ship as a constant DataFrame so
        % Python's _is_loadable classifies it. Today's MATLAB scidb path
        % wraps tables in scifor.Fixed before reaching for_each; raw
        % tables are atypical here.
        spec = py.dict(pyargs('kind', 'constant', ...
            'value', scidb.internal.to_python(val)));

    else
        spec = py.dict(pyargs('kind', 'constant', ...
            'value', scidb.internal.to_python(val)));
    end
end


function [meta_args, opts] = split_options(varargin)
%SPLIT_OPTIONS  Separate known option flags from metadata name-value pairs.
    opts.dry_run = false;
    opts.save = true;
    opts.as_table = string.empty;
    opts.db = [];
    opts.distribute = false;
    opts.where = [];
    opts.fn_name_override = '';
    opts.fn_hash_override = '';

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
                    % Accepted but no longer used (Python owns prepare).
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
                    % Accepted but no longer supported (parallel branch deleted
                    % per redesign plan Phase 0).
                    if logical(varargin{i+1})
                        scidb.Log.warn('parallel=true ignored: parfor branch removed in redesign');
                    end
                    i = i + 2; continue;
                case "distribute"
                    opts.distribute = logical(varargin{i+1});
                    i = i + 2; continue;
                case "where"
                    opts.where = varargin{i+1};
                    i = i + 2; continue;
                case "_fn_name"
                    opts.fn_name_override = char(varargin{i+1});
                    i = i + 2; continue;
                case "_fn_hash"
                    opts.fn_hash_override = char(varargin{i+1});
                    i = i + 2; continue;
            end
        end
        meta_args{end+1} = varargin{i}; %#ok<AGROW>
        i = i + 1;
    end
end
