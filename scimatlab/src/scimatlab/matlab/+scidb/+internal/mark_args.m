function [py_keys, py_scope] = mark_args(fn_name, variable, varargin)
%MARK_ARGS  Arguments of scidb.mark_missing_expected / mark_present_expected.
%
%   [py_keys, py_scope] = scidb.internal.mark_args(FN, VARIABLE, 'subject', 3)
%
%   VARIABLE (a name or a BaseVariable instance) is the scope; the pairs are
%   schema keys, read by scidb.internal.exclusion_args (the one owner of the
%   pair rules). A 'variable', V pair as well is an error: the scope is
%   already the first argument.

    names = cell(scidb.internal.resolve_var_names(variable));
    if numel(names) ~= 1
        error(['scidb:' fn_name], 'Give one variable, got %d.', numel(names));
    end
    py_scope = py.str(char(names{1}));

    [py_keys, extra_scope] = scidb.internal.exclusion_args(fn_name, varargin{:});
    if ~isa(extra_scope, 'py.NoneType')
        error(['scidb:' fn_name], ...
              ['The variable is the first argument; do not also pass ' ...
               '''variable'', V.']);
    end
end
