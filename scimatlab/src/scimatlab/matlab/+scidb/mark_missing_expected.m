function mark_missing_expected(variable, varargin)
%SCIDB.MARK_MISSING_EXPECTED  A variable is absent at a location on purpose.
%
%   scidb.mark_missing_expected("EMG", 'subject', 3)
%   scidb.mark_missing_expected(EMG(), 'subject', 3, 'session', 'post')
%
%   "Mark missing as expected" (GitHub #28): a preset of a scoped
%   scidb.exclude_schema with the reason "marked missing as expected". Every
%   step that reads VARIABLE skips the location from now on; other steps
%   still run there. Undo with scidb.mark_present_expected(VARIABLE, ...).
%
%   Arguments:
%       variable - The variable (a name or a BaseVariable instance)
%       varargin - Schema key/value pairs, e.g. 'subject', 3. Omitted keys
%                  are wildcards.
%
%   Raises an error if a key is not in the dataset schema, or the location
%   is already excluded for VARIABLE.

    [py_keys, py_scope] = scidb.internal.mark_args('mark_missing_expected', variable, varargin{:});
    py.scidb.store.exclusions.mark_missing_expected_dict(py_keys, py_scope, py.None);

end
