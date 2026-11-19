classdef AcrossVariants < handle
%SCIDB.ACROSSVARIANTS  Deliberately pool ALL branch_param variants of an input.
%
%   AV = scidb.AcrossVariants(INNER)
%
%   Aggregation-mode for_each AUTO-SPLITS by upstream branch_param signature
%   (one call per variant group). AcrossVariants is the explicit opt-out for
%   multiverse / specification-curve analysis: the pooled rows keep their
%   variant identity — each namespaced branch_param key (e.g.
%   "bandpass.low_hz") becomes an ordinary table column so the function can
%   group by specification.
%
%   All pooling behavior lives in Python's prepare (Step 12); this MATLAB
%   class is only the builder that ships across the bridge (mirroring
%   scidb.Variant). Composition rules match the Python class:
%
%     - May wrap a BaseVariable, a scidb.Fixed, or a scidb.Variant
%       (pin some params, pool the rest).
%     - Cannot wrap scidb.Merge (pool per constituent instead), an EachOf,
%       or a column selection (it would drop the attached branch_param
%       columns).
%     - Nested AcrossVariants collapses (idempotent).
%
%   Example:
%       scidb.for_each(@robustness, ...
%           struct('df', scidb.AcrossVariants(Filtered())), ...
%           {Spec()}, subject=["S01" "S02"]);

    properties (SetAccess = private)
        var_type   % Inner spec: BaseVariable instance / Fixed / Variant
    end

    methods
        function obj = AcrossVariants(var_type)
            if isa(var_type, 'scidb.Merge')
                error('scidb:AcrossVariants', ...
                    ['AcrossVariants cannot wrap a Merge. branch_params are ' ...
                     'namespaced per producing function, so pooling must ' ...
                     'happen per constituent: Merge(AcrossVariants(A), B, name=...).']);
            end
            if isa(var_type, 'scidb.BaseVariable') && ...
                    ~isempty(var_type.selected_columns)
                error('scidb:AcrossVariants', ...
                    ['AcrossVariants cannot wrap a column selection: the ' ...
                     'selection would drop the attached branch_param ' ...
                     'columns. Select columns inside your function instead.']);
            end
            if isa(var_type, 'scidb.AcrossVariants')
                var_type = var_type.var_type;  % idempotent
            end
            obj.var_type = var_type;
        end
    end
end
