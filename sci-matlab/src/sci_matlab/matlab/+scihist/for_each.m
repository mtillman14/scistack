function result_tbl = for_each(fn, inputs, outputs, varargin)
%SCIHIST.FOR_EACH  Lineage-tracked for_each — auto-wraps fn in LineageFcn.
%
%   scihist.for_each(@FN, INPUTS, OUTPUTS, Name, Value, ...)
%
%   This is Layer 3 (lineage tracking). It:
%   1. Auto-wraps the function handle in scidb.LineageFcn for lineage tracking
%   2. Wraps the LineageFcn in a plain function handle for scidb.for_each
%   3. Delegates to scidb.for_each for all DB I/O and loop orchestration
%
%   Arguments:
%       fn      - Function handle (auto-wrapped in LineageFcn if not already one)
%       inputs  - Struct mapping parameter names to BaseVariable instances,
%                 scidb.Fixed wrappers, scidb.Merge wrappers,
%                 scifor.PathInput instances, or constant values.
%       outputs - Cell array of BaseVariable instances for output types
%
%   Name-Value Arguments: same as scidb.for_each
%
%   Returns:
%       result_tbl - MATLAB table with metadata columns and output columns.
%
%   Example:
%       scihist.for_each(@filter_data, ...
%           struct('step_length', StepLength(), 'smoothing', 0.2), ...
%           {FilteredStepLength()}, ...
%           subject=[1 2 3], session=["A" "B"]);

    % Auto-wrap in LineageFcn if not already one
    if isa(fn, 'scidb.LineageFcn')
        lineage_obj = fn;
    else
        lineage_obj = scidb.LineageFcn(fn);
    end

    % Wrap LineageFcn in a plain function handle for scidb.for_each
    fn_plain = @(varargin) lineage_obj(varargin{:});

    % Pass the real function name and source hash so scidb.for_each can
    % persist expected combos with the correct name (not the anonymous
    % wrapper) and version_keys' __fn_hash identifies the wrapped function
    % source (not the wrapper closure, which is anonymous and unhashable).
    if isa(fn, 'function_handle')
        real_fn_name = func2str(fn);
        hash_fn = fn;
    elseif isa(fn, 'scidb.LineageFcn')
        real_fn_name = func2str(fn.fcn);
        hash_fn = fn.fcn;
    else
        real_fn_name = '';
        hash_fn = [];
    end

    real_fn_hash = '';
    if ~isempty(hash_fn)
        try
            real_fn_hash = scidb.internal.hash_function(hash_fn);
        catch
            % Anonymous or unresolvable; leave hash blank.
        end
    end

    % Delegate to scidb.for_each
    extra_nv = {};
    if ~isempty(real_fn_name)
        extra_nv = [extra_nv, {'_fn_name', real_fn_name}];
    end
    if ~isempty(real_fn_hash)
        extra_nv = [extra_nv, {'_fn_hash', real_fn_hash}];
    end
    result_tbl = scidb.for_each(fn_plain, inputs, outputs, extra_nv{:}, varargin{:});
end
