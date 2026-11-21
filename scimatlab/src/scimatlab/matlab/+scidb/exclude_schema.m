function exclude_schema(reason, varargin)
%SCIDB.EXCLUDE_SCHEMA  Mark a schema-key combination as permanently excluded.
%
%   scidb.exclude_schema(REASON, 'subject', 1, 'trial', 2)
%   scidb.exclude_schema(REASON, 'subject', 3)   % wildcard — excludes all trials
%   scidb.exclude_schema(REASON, 'subject', 3, 'variable', "EMG")
%                                                % only EMG is bad there
%
%   Stores a persistent exclusion record in the database.  The combination
%   is automatically skipped by scidb.for_each without any per-call config.
%   Omitted schema keys act as wildcards (NULL in the table), so omitting
%   'trial' excludes every trial of the given subject.
%
%   Arguments:
%       reason   - Human-readable explanation (string)
%       varargin - Schema key/value pairs, e.g. 'subject', 1, 'trial', 2,
%                  and optionally 'variable', V: scope the exclusion to one
%                  variable (a name or a BaseVariable instance). Only that
%                  variable is bad or absent there; every step that reads it
%                  skips the location, and other steps still run. In a
%                  dataset with a schema key named 'variable', the pair is
%                  that schema key instead (scidb.internal.exclusion_args).
%
%   Raises an error if:
%       - No schema keys are specified
%       - A key is not in the dataset schema
%       - The exact keyset (and scope) is already excluded

    arguments
        reason (1,1) string
    end

    [py_keys, py_scope] = scidb.internal.exclusion_args('exclude_schema', varargin{:});
    py.scidb.store.exclusions.exclude_schema_dict( ...
        py_keys, char(reason), py.None, pyargs('variable', py_scope));

end
