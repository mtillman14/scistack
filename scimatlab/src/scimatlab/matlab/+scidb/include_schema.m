function include_schema(reason, varargin)
%SCIDB.INCLUDE_SCHEMA  Re-include a previously excluded schema-key combination.
%
%   scidb.include_schema(REASON, 'subject', 1, 'trial', 2)
%   scidb.include_schema(REASON, 'subject', 3)   % re-include all trials
%   scidb.include_schema(REASON, 'subject', 3, 'variable', "EMG")
%                                                % re-include for EMG only
%
%   Stores a re-inclusion record in the database (the original exclusion row
%   is NOT deleted — the full history is preserved).  The combination will
%   no longer be skipped by scidb.for_each.
%
%   Arguments:
%       reason   - Human-readable explanation (string)
%       varargin - Schema key/value pairs, e.g. 'subject', 1, 'trial', 2,
%                  and optionally 'variable', V: re-include for one variable
%                  only (a name or a BaseVariable instance), the counterpart
%                  of a scoped scidb.exclude_schema. In a dataset with a
%                  schema key named 'variable', the pair is that schema key
%                  instead (scidb.internal.exclusion_args).
%
%   The exclusion may be on a coarser keyset: after
%   scidb.exclude_schema(R, 'subject', 3), include_schema(R, 'subject', 3, 'trial', 2)
%   re-includes only trial 2 (the more specific row wins).
%
%   Raises an error if the keyset is already included in effect (no
%   exclusion covers it, or the row that decides it is an include).

    arguments
        reason (1,1) string
    end

    [py_keys, py_scope] = scidb.internal.exclusion_args('include_schema', varargin{:});
    py.scidb.store.exclusions.include_schema_dict( ...
        py_keys, char(reason), py.None, pyargs('variable', py_scope));

end
