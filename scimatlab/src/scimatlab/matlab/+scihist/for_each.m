function result_tbl = for_each(fn, inputs, outputs, varargin)
%SCIHIST.FOR_EACH  Deprecated thin shim over scidb.for_each.
%
%   scihist.for_each(@FN, INPUTS, OUTPUTS, Name, Value, ...)
%
%   DEPRECATED: lineage is now tracked automatically by scidb.for_each
%   (the bipartite provenance graph is recorded on save). The former
%   per-call scidb.LineageFcn auto-wrap was removed; this function now
%   simply delegates to scidb.for_each, mirroring the deprecated Python
%   ``scihist.for_each`` shim. Prefer calling scidb.for_each directly.
%
%   Arguments and Name-Value options are identical to scidb.for_each.
%
%   Returns:
%       result_tbl - MATLAB table with metadata columns and output columns.
%
%   Example:
%       scidb.for_each(@filter_data, ...
%           struct('step_length', StepLength(), 'smoothing', 0.2), ...
%           {FilteredStepLength()}, ...
%           subject=[1 2 3], session=["A" "B"]);

    % --- Arity MUST be forwarded, not swallowed. ---
    % scidb.for_each skips the whole post-save result conversion when its
    % caller wants no output (from_python on a payload-carrying result table
    % measured ~385s of a 641s run on 2026-09-13). Writing this as an
    % unconditional `result_tbl = scidb.for_each(...)` makes nargout 1 inside
    % scidb.for_each for EVERY call through this shim, including bare-statement
    % ones, which silently disables that skip. Keep the branch.
    if nargout > 0
        result_tbl = scidb.for_each(fn, inputs, outputs, varargin{:});
    else
        scidb.for_each(fn, inputs, outputs, varargin{:});
    end
end
