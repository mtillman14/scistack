function written = mark_present_expected(variable, varargin)
%SCIDB.MARK_PRESENT_EXPECTED  A variable belongs at a location.
%
%   scidb.mark_present_expected("EMG", 'subject', 3)
%   written = scidb.mark_present_expected(EMG(), 'subject', 3);
%
%   "Mark present as expected" (GitHub #28): a preset of a scoped
%   scidb.include_schema with the reason "marked present as expected", the
%   undo of scidb.mark_missing_expected.
%
%   Arguments:
%       variable - The variable (a name or a BaseVariable instance)
%       varargin - Schema key/value pairs, e.g. 'subject', 3
%
%   Returns:
%       written - true if an include was stored; false (nothing written)
%                 when nothing excludes the location for VARIABLE, so it is
%                 already expected.

    [py_keys, py_scope] = scidb.internal.mark_args('mark_present_expected', variable, varargin{:});
    written = logical(py.scidb.store.exclusions.mark_present_expected_dict( ...
        py_keys, py_scope, py.None));

end
