function result_tbl = for_each(fn, inputs, outputs, varargin)
%SCIDB.FOR_EACH  DB-backed for_each — delegates prepare + save to Python.
%
%   scidb.for_each(@FN, INPUTS, OUTPUTS, Name, Value, ...)
%
%   Two-pass design (see .claude/matlab-for-each-redesign-plan.md Phase 3):
%
%     1. Python ``scimatlab.bridge.for_each_prepare`` does all pre-loop
%        work (DB-load inputs, __rid_* variant expansion, version-key
%        build, persist expected combos for the GUI).
%     2. MATLAB's existing ``+scifor/for_each.m`` runs the inner loop
%        with the prepared inputs and combo list, calling the user
%        function once per combo.
%     3. Python ``scimatlab.bridge.for_each_save`` saves the results
%        (recording the bipartite provenance graph from each output's
%        save metadata) with branch_params / ``__upstream``.
%
%   MATLAB owns only step 2 and the bridge plumbing. All correctness-
%   sensitive logic (variant tracking, lineage save, version keys) lives
%   in Python so MATLAB-driven and Python-driven pipelines stay in sync.
%
%   Arguments:
%       fn      - Function handle (plain). Lineage is recorded automatically.
%       inputs  - Struct mapping parameter names to BaseVariable instances,
%                 scidb.Fixed wrappers, scidb.Variant wrappers,
%                 scidb.Merge wrappers, scifor.PathInput instances, or
%                 constant values.
%       outputs - Cell array of BaseVariable instances for output types
%
%   Name-Value Arguments:
%       dry_run       - If true, preview without executing (default: false)
%       save          - If true, save outputs (default: true)
%       distribute    - If true, split outputs by element/row (default: false)
%       db            - Optional DatabaseManager for load/save operations
%       where         - Optional scidb.Filter for input loading
%       as_table      - Controls which inputs are passed as full tables
%       finalized     - Endpoint (plot_/stat_ NAMED functions) draft/record
%                       flag. Default false = DRAFT: plot_ figures still
%                       render to their PathOutput path, stat_ results are
%                       printed, but NOTHING is recorded. Pass true to save
%                       records with full lineage (+ artifact stamping).
%       glue          - struct: input name -> a NAMED glue_ function handle
%                       (or a cell array of them, applied in order) that
%                       reshapes that input's loaded table in memory before
%                       fn sees it. The result is never saved; only a
%                       provenance node is recorded, so editing a glue body
%                       still invalidates downstream results. A glue node may
%                       change the column space but NOT the row set. Glue
%                       runs in the language of the run, so a MATLAB pipeline
%                       needs MATLAB glue. See
%                       docs/claude/free-code-glue-nodes.md.
%       parameter_names - struct: input name -> the DECLARED Parameter name
%                       that fed it (e.g. struct('gaitRiteConfig',
%                       'gaitrite_config')). Recorded on the constant's
%                       provenance edge so the GUI shows ONE Parameter node
%                       after a run; never part of identity. See
%                       scidb.parameter.declared_parameter_names (Python).
%       locations     - JSON text: the schema location selection
%                       {"include": [[[key, value], ...], ...],
%                        "exclude_levels": {key: [values]}} — ragged prefixes
%                       a per-key value list cannot say. Applied by the bridge
%                       to the combos it hands this loop (same filter as
%                       Python's for_each(locations=)).
%       share_limits  - struct: input name -> string array of schema keys to
%                       hold fixed; each group's [min max] is appended as a
%                       trailing positional arg (declare `<input>_limits`
%                       trailing parameters in share_limits field order).
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

    % --- Resolve function name + source hash (needed by Step 0's log
    %     lines and error messages, so this runs ahead of everything else,
    %     including the historical position further below) ---
    if isa(fn, 'function_handle')
        fn_name = func2str(fn);
        hash_fn = fn;
    else
        fn_name = 'unknown';
        hash_fn = [];
    end
    if ~isempty(opts.fn_name_override)
        fn_name = opts.fn_name_override;
    end

    % --- Step 0: EachOf expansion — must be first, before any other logic.
    %     Mirrors Python scidb/src/scidb/foreach.py's own EachOf expansion
    %     (see docs/claude/each-of-variant-expansion.md): each alternative
    %     becomes an independent recursive scidb.for_each() call (own
    %     prepare/save/lineage), and results are vertcat'd. This recursion
    %     cannot delegate to +scifor/for_each.m's own EachOf handling —
    %     each branch needs its own save/skip_computed/lineage treatment.
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
        scidb.Log.debug('EachOf expansion detected - %d axis(es), making recursive calls', ...
            numel(each_of_axes));
        value_cells = cellfun(@(ax) ax{3}, each_of_axes, 'UniformOutput', false);
        combos = scidb.internal.cartesian_product(value_cells);
        branch_results = {};
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
            branch_tbl = scidb.for_each(fn, concrete_inputs, outputs, concrete_varargin{:});
            if ~isempty(branch_tbl)
                branch_results{end+1} = branch_tbl; %#ok<AGROW>
            end
        end
        result_tbl = vertcat_each_of_results(branch_results, fn_name);
        scidb.Log.debug('EachOf expansion complete - concatenated %d branch result(s)', ...
            numel(branch_results));
        return;
    end

    % --- Resolve source hash (fn_name/hash_fn already resolved above,
    %     ahead of Step 0) ---
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

    % Endpoint (plot_/stat_) detection needs a NAMED function handle:
    % func2str(@(x)...) starts with '@' and can never match the prefix.
    if opts.finalized && startsWith(fn_name, '@')
        scidb.Log.warn(['finalized=true passed with an anonymous function ' ...
            'handle: endpoint detection requires a NAMED plot_*/stat_* ' ...
            'function, so the flag will be ignored.']);
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

    % --- schema_keys: string array -> py.list, or py.None ---
    % Mirrors scidb.for_each: NOT GIVEN -> py.None -> one call over the whole
    % dataset; GIVEN EMPTY ({} / string.empty) -> py.list() -> every schema
    % key, the same reading as subject=[] meaning every subject. isempty()
    % alone cannot tell those two apart, hence the schema_keys_given flag.
    if ~opts.schema_keys_given
        py_schema_keys = py.None;
    else
        py_schema_keys = py.list(cellstr(opts.schema_keys(:)'));
    end

    % --- schema_filter: struct (field -> value list) -> py.dict, or py.None ---
    schema_filter_fields = fieldnames(opts.schema_filter);
    if isempty(schema_filter_fields)
        py_schema_filter = py.None;
    else
        py_schema_filter = py.dict();
        for sf_i = 1:numel(schema_filter_fields)
            sf_key = schema_filter_fields{sf_i};
            py_schema_filter{sf_key} = scidb.internal.to_python(opts.schema_filter.(sf_key));
        end
    end

    % --- parameter_names: struct (input -> declared Parameter) -> py.dict ---
    py_parameter_names = parameter_names_to_python(opts.parameter_names);

    % --- locations: JSON text, parsed by the bridge (or py.None) ---
    if isempty(opts.locations) || strlength(string(opts.locations)) == 0
        py_locations = py.None;
    else
        py_locations = char(opts.locations);
    end

    % --- Pipeline registration seam (deferred execution, stage 4) ---
    % Reuses the marshalled py objects above; registration must have zero
    % side effects, so this runs BEFORE prepare (no loads, no DB writes).
    % Dry-run always stays eager (preview intent beats deferral).
    % Glue chains are normalized ahead of registration too: glue names are
    % part of the call-site identity, so a step registered without them would
    % be a different call site than the same step run eagerly.
    [glue_handles, py_glue] = build_glue_chains(opts.glue, inputs);

    if ~dry_run
        target_pipe = [];
        if isa(opts.pipeline, 'scidb.Pipeline')
            target_pipe = opts.pipeline;
        elseif ~(ischar(opts.pipeline) || isstring(opts.pipeline)) ...
                || ~strcmpi(char(string(opts.pipeline)), 'none')
            active_name = char(py.scimatlab.bridge.pipeline_active_name());
            if ~isempty(active_name)
                target_pipe = scidb.internal.pipeline_registry('get', active_name);
            end
        end
        if ~isempty(target_pipe)
            step_index = double(py.scimatlab.bridge.pipeline_register_step( ...
                target_pipe.py_handle, fn_name, fn_hash, py_inputs_spec, ...
                py_output_classes, py_meta, ...
                pyargs('where', py_where, ...
                       'distribute', logical(opts.distribute), ...
                       'as_table', py_as_table, ...
                       'save', logical(do_save), ...
                       'finalized', logical(opts.finalized), ...
                       'skip_computed', logical(opts.skip_computed), ...
                       'schema_keys', py_schema_keys, ...
                       'schema_filter', py_schema_filter, ...
                       'glue', py_glue, ...
                       'parameter_names', py_parameter_names, ...
                       'locations', py_locations)));
            target_pipe.store_step(step_index, fn, inputs, outputs, opts);
            scidb.Log.info(['pipeline_step_registered (MATLAB): %s -> ' ...
                'pipeline %s (deferred)'], fn_name, target_pipe.name);
            result_tbl = struct('deferred', true, ...
                                'pipeline', target_pipe.name, ...
                                'step_index', step_index, ...
                                'fn_name', fn_name);
            return;
        end
    end

    % --- Call #1: Python prepare ---
    % glue_handles / py_glue were built above (before pipeline registration,
    % which needs the same chain identity). Python hashes each body into the
    % consumer's identity here; the bodies themselves run below, after
    % prepare returns — a .m function cannot execute inside prepare.
    prep_t0 = tic;
    prep = py.scimatlab.bridge.for_each_prepare( ...
        fn_name, fn_hash, py_inputs_spec, py_output_classes, py_meta, ...
        pyargs('where', py_where, ...
               'distribute', logical(opts.distribute), ...
               'as_table', py_as_table, ...
               'db', py_db, ...
               'dry_run', logical(dry_run), ...
               'skip_computed', logical(opts.skip_computed), ...
               'finalized', logical(opts.finalized), ...
               'schema_keys', py_schema_keys, ...
               'schema_filter', py_schema_filter, ...
               'glue', py_glue, ...
               'parameter_names', py_parameter_names, ...
               'locations', py_locations));
    scidb.Log.info('for_each_prepare returned in %.3fs', toc(prep_t0));

    % Dry-run: Python ran the scifor.for_each(dry_run=true) call itself
    % and returned a stub (handle=-1). Nothing else to do.
    if dry_run
        return;
    end

    handle = int64(prep{'handle'});

    % --- Endpoint policy results (computed Python-side by _endpoint_policy,
    %     shared with the Python path). MATLAB does the fn WRAPPING below;
    %     draft save-suppression happens Python-side in for_each_save. ---
    endpoint_kind = char(prep{'endpoint_kind'});
    endpoint_path_param = char(prep{'path_param'});

    % as_table after endpoint policy (stat_ defaults it on): forward the
    % effective value to MATLAB's scifor loop, which does its own table
    % delivery independent of Python's.
    as_table_eff = as_table_raw;
    try
        v_at = prep{'as_table_effective'};
        if isa(v_at, 'py.bool') || islogical(v_at)
            as_table_eff = logical(v_at);
        elseif isa(v_at, 'py.list')
            at_cells = cell(v_at);
            at_strs = strings(1, numel(at_cells));
            for ati = 1:numel(at_cells)
                at_strs(ati) = string(char(at_cells{ati}));
            end
            as_table_eff = at_strs;
        end
    catch
        % leave as user-supplied on any conversion surprise
    end

    % Inputs whose records are structs, stored one DuckDB column per field
    % (sciduckdb multi_column mode). Python resolves this from the stored
    % dtype metadata -- MATLAB cannot tell a struct record apart from a
    % one-row table once the spread columns arrive, so scidb states it and
    % the scifor loop rebuilds the struct per combo. See
    % scidb._resolve_mapping_inputs / DatabaseManager.mapping_data_columns.
    mapping_inputs = struct();
    try
        py_mi = prep{'mapping_inputs'};
        mi_keys = cell(py.list(py_mi.keys()));
        for mi = 1:numel(mi_keys)
            k_mi = char(mi_keys{mi});
            col_cells = cell(py.list(py_mi{k_mi}));
            cols = strings(1, numel(col_cells));
            for ci_mi = 1:numel(col_cells)
                cols(ci_mi) = string(char(col_cells{ci_mi}));
            end
            mapping_inputs.(k_mi) = cols;
        end
    catch mi_err
        % An older bridge without the key, or a conversion surprise: fall
        % back to the previous (table) delivery rather than failing the run.
        scidb.Log.debug('for_each: mapping_inputs unavailable (%s)', mi_err.message);
        mapping_inputs = struct();
    end
    if ~isempty(fieldnames(mapping_inputs))
        mi_names = fieldnames(mapping_inputs);
        mi_parts = cell(1, numel(mi_names));
        for mi = 1:numel(mi_names)
            mi_parts{mi} = sprintf('%s (%d field(s))', mi_names{mi}, ...
                numel(mapping_inputs.(mi_names{mi})));
        end
        scidb.Log.info(sprintf( ...
            'struct-valued input(s) rebuilt per combo: %s', ...
            strjoin(mi_parts, ', ')));
    end

    % Everything from here to the scifor loop is pure Python->MATLAB
    % conversion, and it used to be silent: the last INFO before it is
    % 'struct-valued input(s) rebuilt per combo' and the next one comes from
    % +scifor/for_each.m once the loop is already running.  A slow
    % conversion -- a GB-scale array input crossing the bridge -- therefore
    % looked exactly like a hang, with no way to tell which phase owned the
    % time.  Time every phase, and report each converted input's SIZE next
    % to its duration so a slow conversion says how much it converted.
    convert_t0 = tic;

    % Pre-resolved PathOutput paths, aligned with full_combos (Python
    % resolves branch_param placeholders whose dotted names cannot cross
    % as MATLAB struct fields; {ColName} stays for the for_columns loop).
    t0_rpo = tic;
    resolved_paths = struct();
    try
        py_rpo = prep{'resolved_path_outputs'};
        rpo_keys = cell(py.list(py_rpo.keys()));
        for ri = 1:numel(rpo_keys)
            k_rpo = char(rpo_keys{ri});
            vals = cell(py.list(py_rpo{k_rpo}));
            cs = cell(1, numel(vals));
            for vi = 1:numel(vals)
                cs{vi} = char(vals{vi});
            end
            resolved_paths.(k_rpo) = cs;
        end
    catch rpo_err
        scidb.Log.warn('for_each: could not convert resolved_path_outputs: %s', ...
            rpo_err.message);
    end
    t_resolved_paths = toc(t0_rpo);

    % --- Convert prepared inputs to a MATLAB struct for scifor.
    %     Each loaded value may be a DataFrame, a Python scifor.Fixed /
    %     scifor.ColumnSelection / scifor.Merge wrapper, or a constant.
    %     The bridge describes it as a kind-tagged dict; MATLAB rebuilds
    %     the matching MATLAB classdef wrapper so MATLAB's scifor inner
    %     loop sees the same types a pure-MATLAB call would.
    t0_inputs = tic;
    py_loaded_inputs = prep{'loaded_inputs'};
    scifor_inputs = struct();
    loaded_keys = cell(py.list(py_loaded_inputs.keys()));
    for ki = 1:numel(loaded_keys)
        k = char(loaded_keys{ki});
        t0_one = tic;
        desc = py.scimatlab.bridge.for_each_describe_loaded_input(py_loaded_inputs{k});
        scifor_inputs.(k) = build_scifor_input_from_desc(desc);
        % Python's Step 5 stringifies schema-key columns in loaded
        % DataFrames so DataFrame-side filtering can match user-supplied
        % string-form values. MATLAB user functions that receive the
        % table (as_table=true) expect the original metadata types.
        % Coerce schema-key columns back based on the originally-supplied
        % MATLAB classes tracked in meta_original_classes.
        scifor_inputs.(k) = coerce_meta_columns( ...
            scifor_inputs.(k), meta_original_classes);
        % One line per input: this is the phase that hung on a 1.7e8-sample
        % EMG variable, and per-input granularity is what names the culprit.
        scidb.Log.info('for_each: converted input ''%s'' in %.3fs (%s)', ...
            k, toc(t0_one), describe_converted_input(scifor_inputs.(k)));
    end
    t_inputs = toc(t0_inputs);

    % --- Glue fusion (MATLAB side). Python already routed the consumer's
    %     provenance through virtual glue records during prepare; what is
    %     left is running the bodies. This is the one part of the pre-loop
    %     work MATLAB owns, because a .m function cannot execute inside
    %     Python's prepare step.
    %
    %     Same contract as scidb.glue: __-prefixed bookkeeping columns are
    %     hidden and re-attached, the row set must not change, and
    %     schema-key columns must come back value-identical. ---
    t0_glue = tic;
    scifor_inputs = apply_matlab_glue( ...
        scifor_inputs, prep{'glue_chains'}, glue_handles);
    t_glue = toc(t0_glue);

    % --- Convert extended_metadata_iterables to scifor name-value pairs ---
    t0_meta = tic;
    py_meta_iters = prep{'extended_metadata_iterables'};
    scifor_meta_nv = {};
    meta_iter_keys = cell(py.list(py_meta_iters.keys()));
    for ki = 1:numel(meta_iter_keys)
        k = char(meta_iter_keys{ki});
        v_py = py_meta_iters{k};
        scifor_meta_nv{end+1} = k; %#ok<AGROW>
        scifor_meta_nv{end+1} = scidb.internal.from_python(v_py); %#ok<AGROW>
    end
    t_meta = toc(t0_meta);

    % --- Convert full_combos (py.list of dicts) to MATLAB cell of structs ---
    t0_combos = tic;
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
    t_combos = toc(t0_combos);

    scidb.Log.info(['[timing] for_each_convert_inputs: %d input(s), ' ...
        '%d combo(s), TOTAL=%.3fs (resolved_paths=%.3fs, loaded_inputs=%.3fs, ' ...
        'glue=%.3fs, metadata_iterables=%.3fs, full_combos=%.3fs)'], ...
        numel(loaded_keys), n_combos, toc(convert_t0), t_resolved_paths, ...
        t_inputs, t_glue, t_meta, t_combos);

    % --- Output names returned by Python prepare ---
    py_output_names = prep{'output_names'};
    output_names_cell = cell(py.list(py_output_names));
    output_names = cell(1, numel(output_names_cell));
    for o = 1:numel(output_names_cell)
        output_names{o} = char(output_names_cell{o});
    end

    % --- No combos to run: short-circuit before the scifor loop. ---
    % prepare returns the authoritative combo set in full_combos; an empty set
    % means there is nothing to iterate (every combo was filtered out, e.g. by
    % skip_computed or non-existent-schema pruning). We must NOT fall through to
    % scifor.for_each here: it would treat an empty _all_combos as "unset" and
    % rebuild the full Cartesian product from the metadata iterables, re-running
    % exactly the combos prepare deliberately removed. Free the prepare-side
    % cache and return an empty result instead.
    if n_combos == 0
        scidb.Log.info(['for_each(%s): 0 combos to run (all filtered/skipped); ' ...
            'skipping scifor loop'], fn_name);
        try
            py.scimatlab.bridge.for_each_save(handle, py.list(), pyargs('save', false));
        catch free_err
            scidb.Log.warn('for_each: failed to free prepare cache: %s', free_err.message);
        end
        result_tbl = table();
        return;
    end

    % --- Row selection: which record ids each combination reads ---
    % Python decided (each combo's Selection, aligned with full_combos);
    % scifor.for_each applies it after its schema filter, by the sanitized
    % record-id column, and drops that column. The same rule Python's own
    % loop applies through scifor's _select_rows hook. Replaces the schema
    % extension with x__rid_*/x__vsig_* keys (2026-09-20).
    py_row_sel = prep{'row_selection'};
    n_sel = int64(py.len(py_row_sel));
    row_selection = cell(1, n_sel);
    for si = 1:n_sel
        d = py_row_sel{si};
        s = struct();
        ks = cell(py.list(d.keys()));
        for kii = 1:numel(ks)
            pname = char(ks{kii});
            s.(pname) = string(cellfun(@char, cell(py.list(d{pname})), ...
                'UniformOutput', false));
        end
        row_selection{si} = s;
    end
    record_id_column = string(prep{'record_id_column'});

    % --- Build scifor options for the inner loop ---
    scifor_opts = {};
    scifor_opts{end+1} = '_all_combos';
    scifor_opts{end+1} = all_combos;
    scifor_opts{end+1} = '_row_selection';
    scifor_opts{end+1} = row_selection;
    scifor_opts{end+1} = '_record_id_column';
    scifor_opts{end+1} = record_id_column;
    scifor_opts{end+1} = '_nest_table_outputs';
    scifor_opts{end+1} = true;
    scifor_opts{end+1} = 'output_names';
    scifor_opts{end+1} = output_names;
    if ~isempty(find_pathinput(inputs))
        scifor_opts{end+1} = '_resolve_pathinput';
        scifor_opts{end+1} = true;
        % Schema-key-type policy for per-combo PathInput loads (mirrors
        % Python's _load_pathinput_checked; the MATLAB loop resolves
        % PathInput itself, so the policy callback is injected here and
        % scifor stays policy-free). string keys: exact only; undeclared
        % keys needing a spelling bridge: scidb:SchemaKeyTypeError.
        if isempty(opts.db)
            py_db_kt = py.scidb.database.get_database();
        else
            py_db_kt = opts.db;
        end
        kt_struct = scidb.internal.pydict_to_struct( ...
            py_db_kt.dataset_schema_key_types);
        sk_strings = string(cellfun(@char, ...
            cell(py.list(py_db_kt.dataset_schema_keys)), ...
            'UniformOutput', false));
        scifor_opts{end+1} = '_pathinput_loader';
        scifor_opts{end+1} = @(pi_obj, meta_nv) ...
            scidb.internal.load_pathinput_checked( ...
                pi_obj, meta_nv, kt_struct, sk_strings);
    end
    if ~isempty(as_table_eff)
        scifor_opts{end+1} = 'as_table';
        scifor_opts{end+1} = as_table_eff;
    end
    if ~isempty(fieldnames(mapping_inputs))
        scifor_opts{end+1} = '_mapping_inputs';
        scifor_opts{end+1} = mapping_inputs;
    end
    if opts.distribute
        scifor_opts{end+1} = 'distribute';
        scifor_opts{end+1} = true;
    end
    if ~isempty(fieldnames(resolved_paths))
        scifor_opts{end+1} = '_resolved_path_outputs';
        scifor_opts{end+1} = resolved_paths;
    end
    if ~isempty(fieldnames(opts.share_limits))
        scifor_opts{end+1} = 'share_limits';
        scifor_opts{end+1} = opts.share_limits;
    end

    % --- Endpoint fn wrapping (MATLAB side of Step 1.55/1.56) ---
    % plot_: a returned graphics handle is exported to the combo's resolved
    % path and closed (memory bound across combos); the effective return is
    % the path char, which flows through the bridge save path exactly like
    % Python's _make_plot_wrapper output (record/lineage/stamping all happen
    % Python-side). stat_: the returned struct/JSON is canonicalized by the
    % bridge (normalize_stat_result) so MATLAB- and Python-run stats store
    % byte-identical payloads.
    if strcmp(endpoint_kind, 'plot') || strcmp(endpoint_kind, 'stat')
        endpoint_path_idx = [];
        if ~isempty(endpoint_path_param)
            endpoint_path_idx = find(strcmp(input_names, endpoint_path_param), 1);
        end
        user_fn = fn;
        if strcmp(endpoint_kind, 'plot')
            fn = @(varargin) plot_endpoint_call(user_fn, endpoint_path_idx, ...
                fn_name, varargin{:});
        else
            fn = @(varargin) stat_endpoint_call(user_fn, endpoint_path_idx, ...
                logical(opts.finalized), fn_name, varargin{:});
        end
        scidb.Log.info('for_each(%s): wrapped as %s endpoint (finalized=%d)', ...
            fn_name, endpoint_kind, opts.finalized);
    end

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
            py.scimatlab.bridge.for_each_save(handle, py.list(), pyargs('save', false));
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
    %     Outputs are plain values; the bipartite provenance graph is
    %     recorded by the bridge save path from each output's save metadata. ---
    py_result_dfs = py.list();
    for o = 1:n_out
        if n_outputs == 0
            scidb.Log.info('scifor output %d: Skipping storing output, 0 outputs specified', o)
            break; % Don't store outputs if 0 outputs are specified, e.g. {}
        end
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
        py_result_dfs.append(scidb.internal.to_python(tbl));
    end

    % --- Call #2: Python save ---
    save_t0 = tic;
    py_result_df = py.scimatlab.bridge.for_each_save( ...
        handle, py_result_dfs, pyargs('save', logical(do_save), ...
                                      'introspect', logical(opts.introspect)));
    scidb.Log.info('for_each_save returned in %.3fs', toc(save_t0));

    % --- Post-save result marshalling ---
    %
    % GATED ON nargout. Converting the returned DataFrame is the single most
    % expensive phase of a payload-heavy MATLAB run: from_python walks the
    % result element-by-element, and for a 419-row table carrying a RawEMG
    % struct per row it measured ~385s of a 641s run (2026-09-13,
    % .claude/matlab-run-timing-and-phantom-combos-plan.md). When the caller
    % discarded the result — the shape the GUI generates, a bare
    % `scidb.for_each(...)` statement — every second of that is spent on data
    % nothing reads. The records are already persisted by for_each_save above;
    % the result table is a convenience return, not part of saving.
    %
    % Everything skipped here is pure conversion (from_python, the nested-table
    % flatten, the metadata type restore, the introspect JSON parse). No DB
    % write, no lineage, no side effect depends on this block — which is what
    % makes skipping it safe.
    %
    % NOTE: nargout is the arity of the IMMEDIATE caller. A wrapper that writes
    % `result_tbl = scidb.for_each(...)` unconditionally reports nargout=1 here
    % and defeats the gate; +scihist/for_each.m forwards arity for that reason.
    post_t0 = tic;
    t_from_python = 0;
    t_flatten = 0;
    t_type_restore = 0;
    t_introspect = 0;
    result_bytes = 0;

    if nargout == 0
        % Caller wants nothing back. Assign the declared output explicitly
        % rather than leaving it unset — do not rely on MATLAB tolerating an
        % unassigned output on a statement call.
        result_tbl = table();
        scidb.Log.info(['for_each(%s): caller requested no output (nargout=0) — ' ...
            'skipping post-save result conversion (records are already saved)'], ...
            fn_name);
    elseif isa(py_result_df, 'py.NoneType')
        result_tbl = table();
    else
        t0_fp = tic;
        result_tbl = scidb.internal.from_python(py_result_df);
        t_from_python = toc(t0_fp);
        try
            w_info = whos('result_tbl');
            result_bytes = w_info.bytes;
        catch
            result_bytes = 0;  % size reporting is best-effort only
        end
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
        t0_fl = tic;
        result_tbl = flatten_nested_table_outputs(result_tbl, output_names);
        t_flatten = toc(t0_fl);
        scidb.Log.debug('post-flatten result: %dx%d cols=%s', ...
            height(result_tbl), width(result_tbl), ...
            strjoin(string(result_tbl.Properties.VariableNames), ', '));
    end

    % --- Restore original MATLAB types for metadata columns. Python's
    %     Step 5 stringifies schema-key values so DataFrame-side filtering
    %     is consistent (numeric DB values vs. user-supplied strings); we
    %     reverse that here so numeric inputs round-trip as numeric.
    t0_tr = tic;
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
    t_type_restore = toc(t0_tr);

    % --- Parse introspect dict columns from JSON strings to structs ---
    % Python's for_each_save serializes _branch_params_* as JSON strings for
    % MATLAB compatibility. Convert them back to structs here.
    t0_in = tic;
    if opts.introspect && ~isempty(result_tbl) && istable(result_tbl)
        col_names = string(result_tbl.Properties.VariableNames);
        bp_cols = col_names(startsWith(col_names, "_branch_params_"));
        for ci = 1:numel(bp_cols)
            col = char(bp_cols(ci));
            json_col = result_tbl.(col);
            struct_col = cell(height(result_tbl), 1);
            for ri = 1:height(result_tbl)
                val = json_col(ri);
                if isstring(val) || ischar(val)
                    try
                        struct_col{ri} = jsondecode(char(val));
                    catch
                        struct_col{ri} = struct();
                    end
                else
                    struct_col{ri} = struct();
                end
            end
            result_tbl.(col) = struct_col;
        end
        % _config_keys is also a JSON string — parse to struct.
        if ismember("_config_keys", col_names)
            ck_col = result_tbl.("_config_keys");
            ck_struct = cell(height(result_tbl), 1);
            for ri = 1:height(result_tbl)
                try
                    ck_struct{ri} = jsondecode(char(ck_col(ri)));
                catch
                    ck_struct{ri} = struct();
                end
            end
            result_tbl.("_config_keys") = ck_struct;
        end
    end
    t_introspect = toc(t0_in);

    % Named-operation timing for the phase that used to be a silent gap between
    % 'for_each_save returned' and 'for_each done'. bytes is the size of the
    % converted MATLAB table (0 when the conversion was skipped or unmeasurable),
    % so a slow conversion reports HOW MUCH it converted, not just how long.
    scidb.Log.info(['[timing] for_each_postsave: TOTAL=%.3fs (from_python=%.3fs, ' ...
        'flatten=%.3fs, type_restore=%.3fs, introspect_parse=%.3fs, bytes=%d)'], ...
        toc(post_t0), t_from_python, t_flatten, t_type_restore, t_introspect, ...
        result_bytes);

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

    % Metadata column names are constant across every row (meta_block is
    % always repmat of these), so resolve them once instead of re-reading
    % meta_block.Properties.VariableNames per inner column.
    meta_var_names = cellstr(meta_cols);

    % Accumulate expanded rows as parallel meta/nested blocks (concatenated
    % once at the end) and pass-through rows separately. Deferring the
    % horizontal concat avoids paying table metadata reconciliation per row.
    meta_pieces = {};
    nested_pieces = {};
    passthrough_pieces = {};

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
            passthrough_pieces{end+1} = result_tbl(r, :); %#ok<AGROW>
            continue;
        end

        % Build the metadata block (replicated)
        meta_row = result_tbl(r, meta_var_names);
        meta_block = repmat(meta_row, inner_h, 1);

        % Fold each nested output column in. Work with whole inner tables
        % rather than decomposing into individual columns: pop any
        % meta-override columns into meta_block, prefix name collisions,
        % then horizontally concat the (usually single) inner table.
        nested_block = [];
        seen_names = meta_var_names;  % names already in use (meta + nested)

        for nc = 1:numel(nested_cols)
            nc_name = char(nested_cols(nc));
            cell_val = result_tbl.(nc_name){r};
            if istable(cell_val)
                inner = cell_val;
                innames = inner.Properties.VariableNames;

                % Inner columns matching a metadata column: prefer the
                % inner value (per-row data the user put in their output)
                % and drop them from the inner block.
                meta_hit = ismember(innames, meta_var_names);
                if any(meta_hit)
                    hit_names = innames(meta_hit);
                    for k = 1:numel(hit_names)
                        meta_block.(hit_names{k}) = inner.(hit_names{k});
                    end
                    inner(:, meta_hit) = [];
                    innames = inner.Properties.VariableNames;
                end

                % Disambiguate name collisions across nested outputs by
                % prefixing with the output name.
                coll = ismember(innames, seen_names);
                if any(coll)
                    for k = find(coll)
                        innames{k} = sprintf('%s_%s', nc_name, innames{k});
                    end
                    inner.Properties.VariableNames = innames;
                end

                seen_names = [seen_names, innames]; %#ok<AGROW>
                if isempty(nested_block)
                    nested_block = inner;
                else
                    nested_block = [nested_block, inner]; %#ok<AGROW>
                end
            else
                % Non-table cell — wrap in a cell column of inner_h
                % copies so widths line up.
                col = table(repmat({cell_val}, inner_h, 1), 'VariableNames', {nc_name});
                seen_names = [seen_names, {nc_name}]; %#ok<AGROW>
                if isempty(nested_block)
                    nested_block = col;
                else
                    nested_block = [nested_block, col]; %#ok<AGROW>
                end
            end
        end

        meta_pieces{end+1} = meta_block; %#ok<AGROW>
        if isempty(nested_block)
            nested_pieces{end+1} = table(); %#ok<AGROW>
        else
            nested_pieces{end+1} = nested_block; %#ok<AGROW>
        end
    end

    % Assemble. Expanded rows share a uniform schema, so stack meta and
    % nested blocks independently then horzcat once. Pass-through rows have
    % a different (full-output) schema and cannot be vertcat'd with expanded
    % rows, matching the original behavior (datasets are all-expanded or
    % all-passthrough in practice).
    have_expanded = ~isempty(meta_pieces);
    have_passthrough = ~isempty(passthrough_pieces);
    if have_expanded
        expanded = [vertcat(meta_pieces{:}), vertcat(nested_pieces{:})];
        if have_passthrough
            out = vertcat(expanded, vertcat(passthrough_pieces{:}));
        else
            out = expanded;
        end
    elseif have_passthrough
        out = vertcat(passthrough_pieces{:});
    else
        out = result_tbl;
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
        val = scifor.ColumnSelection(new_data, val.columns, val.iterate);
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
%   ``py.scimatlab.bridge.for_each_describe_loaded_input``.
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
            iter_flag = false;
            desc_keys = cellfun(@char, cell(py.list(desc.keys())), 'UniformOutput', false);
            if any(strcmp('iterate', desc_keys))
                iter_flag = logical(desc{'iterate'});
            end
            val = scifor.ColumnSelection(inner_val, cols, iter_flag);

        case 'merge'
            py_tables = desc{'tables'};
            n = int64(py.len(py_tables));
            tables_cell = cell(1, n);
            for ti = 1:n
                inner = build_scifor_input_from_desc(py_tables{ti});
                % Drop schema key columns that are entirely {0×0 double} cells.
                % These arise when a constituent was saved at a coarser granularity
                % (e.g. subject-level only): the spread layout fills unused schema
                % key columns with NaN/None, which from_python converts to empty
                % doubles.  Removing them before the merge lets filter_table_for_combo
                % skip those dimensions naturally, giving correct broadcast semantics.
                if istable(inner)
                    inner = drop_all_empty_cell_columns(inner);
                end
                tables_cell{ti} = inner;
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

        case 'colname'
            % Deferred ColName() marker — rebuild the MATLAB-side scifor
            % wrapper so +scifor/for_each.m substitutes the current
            % for_columns column name per iteration.
            val = scifor.ColName();

        case 'path_output'
            % Output-path template. The actual per-combo values come from
            % prepare's resolved_path_outputs (Python pre-resolves so
            % branch_param placeholder keys never cross the bridge); the
            % MATLAB scifor.PathOutput is the native-resolution fallback.
            val = scifor.PathOutput(char(desc{'template'}));

        case 'raw'
            val = scidb.internal.from_python(desc{'value'});

        otherwise
            error('scidb:for_each:UnknownInputKind', ...
                'Unrecognized loaded-input kind from bridge: "%s"', kind);
    end
end


function tbl = drop_all_empty_cell_columns(tbl)
%DROP_ALL_EMPTY_CELL_COLUMNS  Remove columns whose every cell is an empty numeric.
%   Used to clean Merge constituent tables coming from the spread layout:
%   schema key columns for dimensions the variable was never saved with arrive
%   as cell arrays of {0×0 double} (NaN/None → [] via from_python).  Dropping
%   them lets filter_table_for_combo skip those dimensions automatically,
%   giving the correct broadcast semantics for coarse-level variables.
    col_names = tbl.Properties.VariableNames;
    drop = false(1, numel(col_names));
    for ci = 1:numel(col_names)
        col = tbl.(col_names{ci});
        if iscell(col) && ~isempty(col) && ...
                all(cellfun(@(x) isnumeric(x) && isempty(x), col))
            drop(ci) = true;
        end
    end
    if any(drop)
        tbl = tbl(:, col_names(~drop));
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
      || isa(var_spec, 'scidb.Variant') ...
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


function out = plot_endpoint_call(user_fn, path_idx, fn_name, varargin)
%PLOT_ENDPOINT_CALL  MATLAB side of the plot_ endpoint wrapper.
%   Mirrors Python's _make_plot_wrapper: a returned graphics handle is
%   exported to the combo's resolved PathOutput path and closed; a returned
%   char/string passes through (the fn saved it itself). The effective
%   return is always the path char — records, lineage, skip_computed, and
%   artifact stamping all happen Python-side from that string.
    r = user_fn(varargin{:});
    if ischar(r) || isstring(r)
        out = char(string(r));
        return;
    end
    if ~isempty(path_idx) && path_idx <= numel(varargin)
        out_path = char(string(varargin{path_idx}));
    else
        out_path = '';
    end
    if isempty(out_path)
        error('scidb:for_each', ...
            ['Plotting function ''%s'' returned a figure but no PathOutput ' ...
             'path was resolved for this combo.'], fn_name);
    end
    if ~isempty(r) && all(isgraphics(r))
        fig = ancestor(r(1), 'figure');
        export_endpoint_figure(fig, out_path);
        close(fig);
        out = out_path;
        return;
    end
    error('scidb:for_each', ...
        ['Plotting function ''%s'' must return a graphics handle (figure/' ...
         'axes) or a path char; got %s.'], fn_name, class(r));
end


function export_endpoint_figure(fig, out_path)
%EXPORT_ENDPOINT_FIGURE  Save a figure by target extension.
%   exportgraphics covers raster + PDF; it cannot write SVG, so .svg routes
%   through print -dsvg.
    [~, ~, ext] = fileparts(out_path);
    switch lower(ext)
        case '.svg'
            print(fig, '-dsvg', out_path);
        otherwise
            exportgraphics(fig, out_path);
    end
end


function out = stat_endpoint_call(user_fn, path_idx, finalized, fn_name, varargin)
%STAT_ENDPOINT_CALL  MATLAB side of the stat_ endpoint wrapper.
%   Mirrors Python's _make_stat_wrapper: in DRAFT mode the resolved
%   PathOutput arg is replaced with [] (report writers should skip their
%   artifact) and the result is pretty-printed; the result struct (or JSON
%   char) is canonicalized by the bridge (normalize_stat_payload) so MATLAB
%   and Python runs of the same stat store BYTE-IDENTICAL payloads —
%   skip_computed's cross-language identity depends on that single
%   normalization point.
    report_path = '';
    if ~isempty(path_idx) && path_idx <= numel(varargin)
        if finalized
            report_path = char(string(varargin{path_idx}));
        else
            varargin{path_idx} = [];
        end
    end
    r = user_fn(varargin{:});
    if isstruct(r) || isa(r, 'containers.Map')
        json_str = jsonencode(r);
    elseif ischar(r) || isstring(r)
        json_str = char(string(r));
    else
        error('scidb:for_each', ...
            ['Statistics function ''%s'' must return a struct (e.g. a stats ' ...
             'result) or a JSON char; got %s.'], fn_name, class(r));
    end
    out = char(py.scimatlab.bridge.normalize_stat_result( ...
        json_str, report_path, finalized, fn_name));
    if ~finalized
        try
            pretty = jsonencode(jsondecode(out), 'PrettyPrint', true);
        catch
            pretty = out;  % PrettyPrint requires R2021a+; fall back to compact
        end
        fprintf('[stat draft] %s:\n%s\n', fn_name, pretty);
    end
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

    elseif isa(val, 'scidb.AcrossVariants')
        % AcrossVariants is a prepare-time pooling marker (D1 opt-out):
        % ship the inner spec; the Python bridge rebuilds
        % scidb.AcrossVariants(inner), and all pooling/bp-column behavior
        % happens in Python's Step 12.
        inner_desc = describe_input_for_python(val.var_type);
        spec = py.dict(pyargs('kind', 'across_variants', 'inner', inner_desc));

    elseif isa(val, 'scifor.PathOutput')
        % Output-path template. Python prepare reconstructs a real
        % scifor.PathOutput so endpoint detection, branch_param placeholder
        % injection, and the collision guard all run; the finished per-combo
        % paths come back via resolved_path_outputs.
        spec = py.dict(pyargs('kind', 'path_output', ...
            'template', char(val.template)));

    elseif isa(val, 'scidb.Variant')
        % Variant is an orthogonal, load-time branch_params filter. Ship the
        % inner spec plus the pinned branch_params; the Python bridge rebuilds
        % scidb.Variant(inner, **branch_params), which injects the filter into
        % its subtree at load time.
        inner_desc = describe_input_for_python(val.var_type);
        bp_py = py.dict();
        bpnames = fieldnames(val.branch_params);
        for i = 1:numel(bpnames)
            bp_py{bpnames{i}} = scidb.internal.to_python( ...
                val.branch_params.(bpnames{i}));
        end
        spec = py.dict(pyargs('kind', 'variant', ...
            'inner', inner_desc, ...
            'branch_params', bp_py));

    elseif isa(val, 'scidb.BaseVariable') && ...
            (~isempty(val.selected_columns) || (isprop(val, 'iterate') && val.iterate))
        scidb.internal.ensure_registered(class(val));
        is_iter = isprop(val, 'iterate') && val.iterate;
        % An iterate (for_columns) selection may have no explicit columns,
        % meaning "all data columns" — ship None so Python resolves them.
        if isempty(val.selected_columns)
            cols_py = py.None;
        else
            cols = val.selected_columns;
            cols_py = py.list(cellstr(cols(:)'));
        end
        spec = py.dict(pyargs('kind', 'column_selection', ...
            'type_name', class(val), ...
            'columns', cols_py, ...
            'iterate', logical(is_iter)));

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

    elseif isa(val, 'scidb.ColName')
        % Deferred ColName() ships with no type_name; the Python bridge
        % resolves it to a scifor.ColName() marker (current for_columns
        % column). Static ColName(MyVar()) ships its type_name and is
        % resolved to a column-name string during Python prepare.
        if val.is_deferred()
            spec = py.dict(pyargs('kind', 'colname', 'deferred', true));
        else
            scidb.internal.ensure_registered(class(val.var_type));
            spec = py.dict(pyargs('kind', 'colname', 'deferred', false, ...
                'type_name', class(val.var_type)));
        end

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


function [handles, py_glue] = build_glue_chains(glue_opt, inputs)
%BUILD_GLUE_CHAINS  Normalize the glue= option for MATLAB and for Python.
%
%   Returns HANDLES, a struct mapping each glued input name to a cell array
%   of MATLAB function handles (applied in order), and PY_GLUE, the matching
%   Python dict of {name, language, source_text, source_file, per_schema_key}
%   entries.
%
%   Python needs the SOURCE TEXT, not the handle: the glue's content hash is
%   what the virtual record id — and therefore the consuming function's
%   invocation identity — is derived from. Without it an edited glue body
%   would leave every downstream record green and stale.
%
%   A chain element may be EITHER:
%     * a MATLAB function handle — the body runs here, in apply_matlab_glue;
%     * a struct with fields NAME, LANGUAGE ('python') and SOURCE_FILE — the
%       body runs in Python's for_each_prepare. That is the only site a
%       CONSTANT-fed glue can run (scidb.glue.apply_constant_glue applies it
%       while the version keys are being built, so the glued value is what
%       lands in __constants), and prepare is Python on both run paths. Such
%       an element contributes NO handle: MATLAB must not apply it again.

    handles = struct();
    py_glue = py.dict();
    if isempty(glue_opt) || ~isstruct(glue_opt)
        return;
    end

    params = fieldnames(glue_opt);
    for pi = 1:numel(params)
        param = params{pi};
        entry = glue_opt.(param);
        if isa(entry, 'function_handle') || isstruct(entry)
            entry = {entry};
        elseif ~iscell(entry)
            error('scidb:glue:badChain', ...
                ['glue.%s must be a function handle, a python-glue struct, ' ...
                 'or a cell array of those; got %s.'], param, class(entry));
        end
        if isempty(entry)
            continue;
        end
        if ~isfield(inputs, param)
            scidb.Log.warn(['[glue] ''%s'': chain declared but there is no ' ...
                'such input on this call — glue not applied'], param);
            continue;
        end

        chain_list = py.list();
        matlab_handles = {};
        labels = {};
        for ci = 1:numel(entry)
            h = entry{ci};
            if isstruct(h)
                % Python glue: named and located by the GUI, applied by
                % Python's prepare. No handle to collect.
                if ~isfield(h, 'name') || ~isfield(h, 'source_file')
                    error('scidb:glue:badChain', ...
                        ['glue.%s element %d is a struct but has no name/' ...
                         'source_file fields.'], param, ci);
                end
                chain_list.append(py.dict(pyargs( ...
                    'name', h.name, ...
                    'language', 'python', ...
                    'source_text', '', ...
                    'source_file', h.source_file, ...
                    'per_schema_key', false)));
                labels{end+1} = sprintf('%s (python)', h.name); %#ok<AGROW>
                continue;
            end
            if ~isa(h, 'function_handle')
                error('scidb:glue:badChain', ...
                    ['glue.%s element %d is a %s, not a function handle or ' ...
                     'a python-glue struct.'], param, ci, class(h));
            end
            name = func2str(h);
            if startsWith(name, '@')
                error('scidb:glue:badChain', ...
                    ['glue.%s element %d is an anonymous function. A glue ' ...
                     'node must be a NAMED function in its own file, so it ' ...
                     'can be hashed, exported and shown in the graph.'], ...
                    param, ci);
            end
            src = read_function_source(name);
            chain_list.append(py.dict(pyargs( ...
                'name', name, ...
                'language', 'matlab', ...
                'source_text', src, ...
                'source_file', which(name), ...
                'per_schema_key', false)));
            matlab_handles{end+1} = h; %#ok<AGROW>
            labels{end+1} = name; %#ok<AGROW>
        end
        py_glue{param} = chain_list;
        if ~isempty(matlab_handles)
            handles.(param) = matlab_handles;
        end
        scidb.Log.info('[glue] ''%s'': chain = [%s]', param, ...
            strjoin(labels, ', '));
    end
end


function src = read_function_source(name)
%READ_FUNCTION_SOURCE  Full .m source of a named function, for hashing.
%   Falls back to the bare name when the file cannot be read, so a glue node
%   still has a stable (if coarse) identity rather than failing the run.
    src = name;
    try
        path = which(name);
        if ~isempty(path) && exist(path, 'file') == 2
            src = fileread(path);
        else
            scidb.Log.warn(['[glue] could not locate source for ''%s'' on the ' ...
                'MATLAB path; hashing its name only, so edits to its body ' ...
                'will NOT invalidate downstream results.'], name);
        end
    catch err
        scidb.Log.warn('[glue] could not read source for ''%s'': %s', name, err.message);
    end
end


function scifor_inputs = apply_matlab_glue(scifor_inputs, py_chains, handles)
%APPLY_MATLAB_GLUE  Run the glue bodies on the loaded tables, in order.
%
%   Applied between prepare and the loop, on the tables MATLAB has already
%   rebuilt from for_each_describe_loaded_input. Enforces the same contract
%   scidb.glue enforces on the Python path:
%
%     * ``__``-prefixed columns are hidden from the glue and re-attached
%       afterwards — internal bookkeeping the user must never think about;
%     * the ROW SET must not change (columns may change freely);
%     * schema-key columns must come back value-identical, because they
%       drive the consuming function's per-combo slicing.

    if isempty(fieldnames(handles))
        return;
    end

    try
        chain_params = cell(py.list(py_chains.keys()));
    catch
        chain_params = {};
    end

    schema_keys = scifor.get_schema();

    for pi = 1:numel(chain_params)
        param = char(chain_params{pi});
        if ~isfield(handles, param) || ~isfield(scifor_inputs, param)
            continue;
        end
        for ci = 1:numel(handles.(param))
            h = handles.(param){ci};
            scifor_inputs.(param) = apply_one_glue( ...
                scifor_inputs.(param), h, param, schema_keys);
        end
    end
end


function value = apply_one_glue(value, h, param, schema_keys)
%APPLY_ONE_GLUE  One glue node against one loaded input value.
    name = func2str(h);

    % Wrappers (scifor.Fixed / scifor.ColumnSelection) carry the table on
    % .data — glue sees the table, never the wrapper.
    wrapper = [];
    tbl = value;
    if isobject(value) && isprop(value, 'data') && istable(value.data)
        wrapper = value;
        tbl = value.data;
    end

    if ~istable(tbl)
        % A scalar / array / struct: nothing to preserve, just apply.
        t0 = tic;
        out = h(tbl);
        scidb.Log.debug('[glue] ''%s'': applied %s (non-table) in %.3fs', ...
            param, name, toc(t0));
        if ~isempty(wrapper)
            wrapper.data = out;
            value = wrapper;
        else
            value = out;
        end
        return;
    end

    all_cols = string(tbl.Properties.VariableNames);
    % Both spellings of an internal column. Python's are ``__``-prefixed, but
    % the bridge renames every one to ``x__`` on the way over because MATLAB
    % struct fields cannot start with an underscore (see
    % scimatlab.bridge._sanitize_rid_key). Matching only ``__`` here would
    % hand the record-id discriminator to the glue as an ordinary column —
    % where dropping or reordering it silently severs per-row provenance.
    hidden_mask = startsWith(all_cols, "__") | startsWith(all_cols, "x__");
    hidden = tbl(:, hidden_mask);
    visible = tbl(:, ~hidden_mask);

    present_keys = string.empty;
    before = {};
    for si = 1:numel(schema_keys)
        key = string(schema_keys{si});
        if ismember(key, string(visible.Properties.VariableNames))
            present_keys(end+1) = key; %#ok<AGROW>
            before{end+1} = visible.(char(key)); %#ok<AGROW>
        end
    end
    n_before = height(visible);

    t0 = tic;
    out = h(visible);
    elapsed = toc(t0);

    if ~istable(out)
        error('scidb:glue:rowsChanged', ...
            ['glue ''%s'' on parameter ''%s'' was given a %d-row table and ' ...
             'returned a %s. A glue node may change the column space but not ' ...
             'the row set — use a function node with a saved output for ' ...
             'anything that reduces rows.'], name, param, n_before, class(out));
    end
    if height(out) ~= n_before
        error('scidb:glue:rowsChanged', ...
            ['glue ''%s'' on parameter ''%s'' changed the row set: %d row(s) ' ...
             'in, %d row(s) out. Filtering, aggregating and exploding must be ' ...
             'an ordinary function node with a saved output.'], ...
            name, param, n_before, height(out));
    end

    for si = 1:numel(present_keys)
        key = char(present_keys(si));
        if ~ismember(string(key), string(out.Properties.VariableNames))
            error('scidb:glue:schemaKeysAltered', ...
                ['glue ''%s'' on parameter ''%s'' dropped schema-key column ' ...
                 '''%s''. Schema-key columns are visible to glue so it can ' ...
                 'group by them, but they drive per-combo slicing and must ' ...
                 'come back unchanged.'], name, param, key);
        end
        if ~isequal(out.(key), before{si})
            error('scidb:glue:schemaKeysAltered', ...
                ['glue ''%s'' on parameter ''%s'' altered schema-key column ' ...
                 '''%s''. Schema values are matched as strings during ' ...
                 'per-combo slicing, so even a type change makes every combo ' ...
                 'filter miss.'], name, param, key);
        end
    end

    % Re-attach the hidden bookkeeping columns by row position (safe exactly
    % because the row set is preserved). A glue that invented a __-prefixed
    % column loses to scidb's own.
    if width(hidden) > 0
        collisions = intersect(string(out.Properties.VariableNames), ...
                               string(hidden.Properties.VariableNames));
        if ~isempty(collisions)
            scidb.Log.warn(['[glue] ''%s'': %s produced internal column(s) %s ' ...
                '— overwritten by scidb''s own bookkeeping'], ...
                param, name, strjoin(cellstr(collisions), ', '));
            out(:, cellstr(collisions)) = [];
        end
        out = [out hidden];
    end

    scidb.Log.debug('[glue] ''%s'': applied %s (%d -> %d cols) in %.3fs', ...
        param, name, width(visible), width(out) - width(hidden), elapsed);

    if ~isempty(wrapper)
        wrapper.data = out;
        value = wrapper;
    else
        value = out;
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
    opts.introspect = false;
    opts.skip_computed = false;
    opts.finalized = false;
    opts.share_limits = struct();
    opts.schema_keys = string.empty;
    opts.schema_keys_given = false;
    opts.schema_filter = struct();
    % Glue chains: struct mapping an INPUT name to a function handle, or a
    % cell array of handles applied in order, reshaping that input's loaded
    % table in memory before fn sees it. Never saved. See
    % docs/claude/free-code-glue-nodes.md.
    opts.glue = struct();
    % Input name -> declared Parameter name (see the parameter_names help).
    opts.parameter_names = struct();
    % Schema location selection as JSON text ('' = none). See the help.
    opts.locations = '';
    opts.fn_name_override = '';
    opts.fn_hash_override = '';
    % Deferred pipeline registration: '' = ambient (register into the
    % active pipeline if any), 'none' = force eager, or a scidb.Pipeline
    % to register into a non-ambient pipeline.
    opts.pipeline = '';

    % Reserved option names (normalized: lowercased, underscores removed) used
    % to warn when a metadata key looks like a misspelled option. Keep in sync
    % with the cases below.
    reserved_opts = ["dryrun", "save", "preload", "astable", "db", ...
                     "parallel", "distribute", "where", "introspect", ...
                     "skipcomputed", "finalized", "sharelimits", ...
                     "schemakeys", "schemafilter", "glue", "parameternames", "locations", ...
                     "fnname", "fnhash", "pipeline"];

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
                case "introspect"
                    opts.introspect = logical(varargin{i+1});
                    i = i + 2; continue;
                case "skip_computed"
                    opts.skip_computed = logical(varargin{i+1});
                    i = i + 2; continue;
                case "finalized"
                    opts.finalized = logical(varargin{i+1});
                    i = i + 2; continue;
                case "share_limits"
                    opts.share_limits = varargin{i+1};
                    i = i + 2; continue;
                case "schema_keys"
                    % Schema key names to iterate — structural sugar for
                    % key=[] on each. Forwarded to for_each_prepare, which
                    % seeds metadata_iterables via scifor.expand_schema_keys
                    % (same function scidb.for_each's Python path uses).
                    opts.schema_keys = string(varargin{i+1});
                    opts.schema_keys_given = true;
                    i = i + 2; continue;
                case "schema_filter"
                    % struct: schema key -> explicit value list, overriding
                    % auto-resolution (or constraining a non-iterated key
                    % via where=). See Python scidb.for_each's schema_filter.
                    opts.schema_filter = varargin{i+1};
                    i = i + 2; continue;
                case "glue"
                    % struct: input name -> glue function handle, or a cell
                    % array of handles applied in order.
                    opts.glue = varargin{i+1};
                    i = i + 2; continue;
                case "parameter_names"
                    opts.parameter_names = varargin{i+1};
                    i = i + 2; continue;
                case "locations"
                    opts.locations = varargin{i+1};
                    i = i + 2; continue;
                case "pipeline"
                    opts.pipeline = varargin{i+1};
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

    % Typo guard: an unrecognized option name (e.g. "skipComputed",
    % "dry_run" misspelled) silently becomes a metadata iteration axis, which
    % is almost never intended and produces confusing phantom iterations. Warn
    % when a metadata KEY normalizes to a reserved option name but didn't match
    % a case above.
    for j = 1:2:numel(meta_args)
        k = meta_args{j};
        if ischar(k) || isstring(k)
            nkey = erase(lower(string(k)), "_");
            if ismember(nkey, reserved_opts)
                scidb.Log.warn(['Metadata key "%s" looks like a for_each ' ...
                    'option but was not recognized — it is being treated as ' ...
                    'an iteration axis. Check spelling/case (e.g. ' ...
                    '"skip_computed").'], char(string(k)));
            end
        end
    end
end


function py_names = parameter_names_to_python(names)
%PARAMETER_NAMES_TO_PYTHON  struct (input -> declared Parameter name) ->
%   py.dict of str -> str, or py.None when empty. Values must be text: a
%   declared name is an identifier, and anything else is a caller bug worth
%   stopping on rather than recording a garbled name.
    if isempty(names) || ~isstruct(names) || isempty(fieldnames(names))
        py_names = py.None;
        return;
    end
    py_names = py.dict();
    fns = fieldnames(names);
    for k = 1:numel(fns)
        v = names.(fns{k});
        if ~(ischar(v) || (isstring(v) && isscalar(v)))
            error('scidb:for_each:parameterNames', ...
                ['parameter_names.%s must be the declared Parameter name ' ...
                 '(text), got a %s'], fns{k}, class(v));
        end
        py_names{fns{k}} = char(v);
    end
    scidb.Log.debug('parameter_names (input -> declared): %s', ...
        char(py.str(py_names)));
end


function out_varargin = replace_name_value(in_varargin, name, new_value)
%REPLACE_NAME_VALUE  Replace a name-value pair's value in a varargin cell
%   array (case-insensitive on the name). Used by Step 0's EachOf
%   expansion to substitute a concrete filter for a where=EachOf(...) axis
%   before the recursive scidb.for_each() call — otherwise the recursive
%   call would see the same EachOf object in its own opts.where and expand
%   forever.
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
            error('scidb:for_each:EachOfColumnMismatch', ...
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


function s = describe_converted_input(val)
%DESCRIBE_CONVERTED_INPUT  One-line size summary of a converted input.
%   Reported next to the conversion's duration so a slow conversion in the
%   log is attributable to data volume rather than guessed at: '6.4s' alone
%   says nothing, '6.4s (table 419x17, 1.30GB)' names the cause.
%   Size reporting is best-effort — never fail a run over a log line.
    try
        w = whos('val');
        bytes_str = format_input_bytes(w.bytes);
    catch
        bytes_str = 'size unknown';
    end
    try
        if istable(val)
            s = sprintf('table %dx%d, %s', height(val), width(val), bytes_str);
        elseif isobject(val)
            s = sprintf('%s, %s', class(val), bytes_str);
        else
            s = sprintf('%s [%s], %s', class(val), ...
                strjoin(string(size(val)), 'x'), bytes_str);
        end
    catch
        s = bytes_str;
    end
end


function s = format_input_bytes(n)
%FORMAT_INPUT_BYTES  Human-readable byte count for conversion log lines.
    if n >= 1024^3
        s = sprintf('%.2fGB', n / 1024^3);
    elseif n >= 1024^2
        s = sprintf('%.1fMB', n / 1024^2);
    else
        s = sprintf('%.1fKB', n / 1024);
    end
end
