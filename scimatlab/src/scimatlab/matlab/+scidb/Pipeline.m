classdef Pipeline < handle
%SCIDB.PIPELINE  MATLAB wrapper for a Python-side pipeline (stage 4 parity).
%
%   pipe = scidb.Pipeline("gait_analysis");                    % activates
%   pipe = scidb.Pipeline("gait", 'db', db, 'uses', {loading});
%   pipe = scidb.Pipeline("gait", 'uses', ...
%              {loading.bind('params', struct('low_hz', 30))});
%
%   While a pipeline is active, scidb.for_each calls REGISTER as deferred
%   steps instead of executing (pass 'pipeline','none' to force an eager
%   call). Execute with run_all() / run_until(target) / run_endpoints();
%   preview with plan().
%
%   The Python side owns the graph (StepSpecs with sentinel fns, type-edge
%   inference, topo order, bindings, plan); this wrapper stores what cannot
%   cross the bridge — each MATLAB step's function handle and raw call
%   arguments, keyed by the Python-assigned step index — and DRIVES
%   execution: it asks Python for the ordered run descriptors, replays
%   MATLAB steps through the normal two-pass scidb.for_each (with the
%   descriptor's post-binding iterables / constant overrides / path
%   templates applied), and runs Python-registered steps Python-side
%   (mixed-language pipelines).
%
%   Known v1 limitation: a key_map binding does NOT rewrite a MATLAB
%   step's where= filter on replay (Python rewrites its spec copy, but
%   MATLAB replays its stored filter object). Structured filters on
%   cross-project-bound MATLAB steps need manual adjustment.

    properties (SetAccess = private)
        name        % char — pipeline name (registry key)
        py_handle   % bridge _pipeline_cache handle
    end

    properties (Access = private)
        step_store  % containers.Map: step_index (double) -> stored call struct
    end

    methods
        function obj = Pipeline(name, varargin)
            %PIPELINE  Create + ACTIVATE (ambient registration target).
            %   Options: 'db' (DatabaseManager; defaults to the global
            %   database), 'uses' (cell of Pipelines / bind(...) structs).
            p_db = [];
            p_uses = {};
            for i = 1:2:numel(varargin)
                switch lower(string(varargin{i}))
                    case "db"
                        p_db = varargin{i+1};
                    case "uses"
                        p_uses = varargin{i+1};
                    otherwise
                        error('scidb:Pipeline', 'Unknown option "%s"', ...
                            char(string(varargin{i})));
                end
            end
            if isempty(p_db)
                py_db = py.None;
            else
                py_db = p_db;
            end

            obj.name = char(string(name));
            obj.py_handle = py.scimatlab.bridge.pipeline_create( ...
                obj.name, pyargs('db', py_db, 'activate', true));
            obj.step_store = containers.Map('KeyType', 'double', ...
                                            'ValueType', 'any');
            scidb.internal.pipeline_registry('put', obj.name, obj);

            for i = 1:numel(p_uses)
                obj.use(p_uses{i});
            end
        end

        function store_step(obj, step_index, fn, inputs, outputs, opts)
            %STORE_STEP  Keep the un-bridgeable raw call (registration seam).
            s = struct();
            s.fn = fn;
            s.inputs = inputs;
            s.outputs = outputs;
            s.opts = opts;
            obj.step_store(double(step_index)) = s;
        end

        function s = get_step(obj, step_index)
            s = obj.step_store(double(step_index));
        end

        function b = bind(obj, varargin)
            %BIND  Use-edge adaptation without touching pipeline source:
            %   b = pipe.bind('key_map', struct('session', 'subject'), ...
            %                 'params', struct('low_hz', 30), ...
            %                 'iterate', struct('subject', ["1" "2"]));
            %   Returns a binding struct for the 'uses' option / use().
            %   Param-target validation errors raise HERE (bind time).
            km = py.None; pr = py.None; it = py.None;
            for i = 1:2:numel(varargin)
                val = varargin{i+1};
                switch lower(string(varargin{i}))
                    case "key_map"
                        km = obj.struct_to_pydict(val);
                    case "params"
                        pr = obj.struct_to_pydict(val);
                    case "iterate"
                        it = obj.struct_to_pydict(val);
                    otherwise
                        error('scidb:Pipeline', 'Unknown bind option "%s"', ...
                            char(string(varargin{i})));
                end
            end
            bh = py.scimatlab.bridge.pipeline_bind(obj.py_handle, ...
                pyargs('key_map', km, 'params', pr, 'iterate', it));
            b = struct('is_pipeline_binding', true, ...
                       'binding_handle', bh, ...
                       'pipeline_name', obj.name);
        end

        function use(obj, other)
            %USE  Declare a dependency: a scidb.Pipeline or a bind(...) struct.
            if isa(other, 'scidb.Pipeline')
                py.scimatlab.bridge.pipeline_use(obj.py_handle, ...
                    pyargs('child_handle', other.py_handle));
            elseif isstruct(other) && isfield(other, 'is_pipeline_binding')
                py.scimatlab.bridge.pipeline_use(obj.py_handle, ...
                    pyargs('binding_handle', other.binding_handle));
            else
                error('scidb:Pipeline', ...
                    'use() takes a scidb.Pipeline or a bind(...) struct.');
            end
        end

        function deactivate(obj)
            %DEACTIVATE  Stop ambient registration without running.
            py.scimatlab.bridge.pipeline_deactivate(obj.py_handle);
        end

        function entries = plan(obj, target)
            %PLAN  Non-executing dry run over the composed graph: struct
            %   array with step, pipeline, endpoint, state
            %   ('green'/'red'/'unknown'), n_combos.
            if nargin < 2, target = ''; end
            py_entries = py.scimatlab.bridge.pipeline_plan(obj.py_handle, ...
                obj.target_name(target));
            entries = obj.dictlist_to_structs(py_entries);
        end

        function eps = endpoints(obj)
            %ENDPOINTS  The composed graph's plot_/stat_ steps.
            py_eps = py.scimatlab.bridge.pipeline_endpoints(obj.py_handle);
            eps = obj.dictlist_to_structs(py_eps);
        end

        function run_all(obj, varargin)
            %RUN_ALL  Run own steps + their ancestors in dependency order.
            %   Options: 'skip_computed' (default true).
            obj.drive('all', '', false, [], obj.opt_skip(varargin{:}));
        end

        function run_until(obj, target, varargin)
            %RUN_UNTIL  Run target (+ ancestors) only — resolved over the
            %   composed graph. Target: step/fn name string or function
            %   handle. Options: 'finalized' (applies to the target step(s)
            %   only), 'skip_computed' (default true).
            [fin, skip] = obj.opt_fin_skip(varargin{:});
            obj.drive('until', obj.target_name(target), false, fin, skip);
        end

        function run_endpoints(obj, varargin)
            %RUN_ENDPOINTS  Run every endpoint + ancestry ("make all my
            %   figures and stats"). Options: 'include_used' (default
            %   false: own endpoints only), 'finalized', 'skip_computed'.
            inc = false;
            for i = 1:2:numel(varargin)
                if lower(string(varargin{i})) == "include_used"
                    inc = logical(varargin{i+1});
                end
            end
            [fin, skip] = obj.opt_fin_skip(varargin{:});
            obj.drive('endpoints', '', inc, fin, skip);
        end
    end

    methods (Access = private)
        function nm = target_name(~, target)
            if isa(target, 'function_handle')
                nm = func2str(target);
            else
                nm = char(string(target));
            end
        end

        function skip = opt_skip(~, varargin)
            skip = true;
            for i = 1:2:numel(varargin)
                if lower(string(varargin{i})) == "skip_computed"
                    skip = logical(varargin{i+1});
                end
            end
        end

        function [fin, skip] = opt_fin_skip(obj, varargin)
            fin = [];
            for i = 1:2:numel(varargin)
                if lower(string(varargin{i})) == "finalized"
                    fin = logical(varargin{i+1});
                end
            end
            skip = obj.opt_skip(varargin{:});
        end

        function drive(obj, mode, target_nm, include_used, fin, skip)
            %DRIVE  The MATLAB run loop over Python's execution order.
            if isempty(fin)
                py_fin = py.None;
            else
                py_fin = logical(fin);
            end
            res = py.scimatlab.bridge.pipeline_execution_order( ...
                obj.py_handle, pyargs( ...
                    'mode', mode, ...
                    'target_name', target_nm, ...
                    'include_used', logical(include_used), ...
                    'finalized', py_fin, ...
                    'skip_computed', logical(skip)));
            run_handle = res{'run_handle'};
            steps_list = res{'steps'};
            n = double(py.len(steps_list));
            scidb.Log.info('pipeline_run_started (MATLAB-driven): %s, %d step(s)', ...
                obj.name, n);
            cleanup = onCleanup(@() ...
                py.scimatlab.bridge.pipeline_run_free(run_handle)); %#ok<NASGU>
            for pos = 0:(n - 1)
                d = steps_list{pos + 1};
                if logical(d{'is_matlab'})
                    obj.replay_matlab_step(d);
                else
                    py.scimatlab.bridge.pipeline_run_python_step( ...
                        run_handle, py.int(pos));
                end
            end
            scidb.Log.info('pipeline_run_finished (MATLAB-driven): %s', obj.name);
        end

        function replay_matlab_step(obj, d)
            %REPLAY_MATLAB_STEP  Run one descriptor through scidb.for_each.
            owner_name = char(d{'pipeline'});
            owner = scidb.internal.pipeline_registry('get', owner_name);
            if isempty(owner)
                error('scidb:Pipeline', ['Step "%s" belongs to pipeline ' ...
                    '"%s", whose MATLAB wrapper is not in this session''s ' ...
                    'registry — recreate it with scidb.Pipeline("%s").'], ...
                    char(d{'step'}), owner_name, owner_name);
            end
            stored = owner.get_step(double(d{'step_index'}));

            % Post-binding surface: constants + path templates into inputs.
            inputs = stored.inputs;
            ci = d{'constant_inputs'};
            ci_keys = cell(py.list(ci.keys()));
            for k = 1:numel(ci_keys)
                nm = char(ci_keys{k});
                inputs.(nm) = scidb.internal.from_python(ci{nm});
            end
            pt = d{'path_templates'};
            pt_keys = cell(py.list(pt.keys()));
            for k = 1:numel(pt_keys)
                nm = char(pt_keys{k});
                inputs.(nm) = scifor.PathOutput(char(pt{nm}));
            end

            % Post-binding metadata iterables.
            mi = d{'metadata_iterables'};
            mi_keys = cell(py.list(mi.keys()));
            meta_pairs = cell(1, 2 * numel(mi_keys));
            for k = 1:numel(mi_keys)
                nm = char(mi_keys{k});
                meta_pairs{2*k - 1} = nm;
                meta_pairs{2*k} = scidb.internal.from_python(mi{nm});
            end

            % finalized: descriptor override for targets, else as registered.
            fin_arg = stored.opts.finalized;
            af = d{'apply_finalized'};
            if ~isa(af, 'py.NoneType')
                fin_arg = logical(af);
            end

            scidb.Log.info('pipeline_step_run (MATLAB): %s (via %s)', ...
                char(d{'step'}), obj.name);
            % schema_keys/schema_filter were stored separately at
            % registration time (not folded into metadata_iterables, which
            % execution_order() passes through as raw stored data) — forward
            % them from stored.opts so replay resolves them just like an
            % eager call would.
            scidb.for_each(stored.fn, inputs, stored.outputs, ...
                'pipeline', 'none', ...
                'skip_computed', logical(d{'skip_computed'}), ...
                'finalized', fin_arg, ...
                'save', stored.opts.save, ...
                'distribute', stored.opts.distribute, ...
                'where', stored.opts.where, ...
                'as_table', stored.opts.as_table, ...
                'db', stored.opts.db, ...
                'share_limits', stored.opts.share_limits, ...
                'schema_keys', stored.opts.schema_keys, ...
                'schema_filter', stored.opts.schema_filter, ...
                'parameter_names', stored.opts.parameter_names, ...
                'glue', stored.opts.glue, ...
                'locations', stored.opts.locations, ...
                meta_pairs{:});
        end

        function d = struct_to_pydict(~, s)
            %STRUCT_TO_PYDICT  Field/value struct -> py.dict (values via
            %   to_python; scalar strings become str, arrays become lists).
            d = py.dict();
            fns = fieldnames(s);
            for f = 1:numel(fns)
                v = s.(fns{f});
                if (isstring(v) && isscalar(v)) || ischar(v)
                    d{fns{f}} = char(v);
                else
                    d{fns{f}} = scidb.internal.to_python(v);
                end
            end
        end

        function entries = dictlist_to_structs(~, py_entries)
            n = double(py.len(py_entries));
            if n == 0
                entries = struct([]);
                return;
            end
            cells = cell(1, n);
            for i = 1:n
                e = py_entries{i};
                s = struct();
                e_keys = cell(py.list(e.keys()));
                for k = 1:numel(e_keys)
                    nm = char(e_keys{k});
                    s.(nm) = scidb.internal.from_python(e{nm});
                end
                cells{i} = s;
            end
            entries = [cells{:}];
        end
    end
end
