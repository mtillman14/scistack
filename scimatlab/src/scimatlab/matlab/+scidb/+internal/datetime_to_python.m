function py_obj = datetime_to_python(dt, kind)
%DATETIME_TO_PYTHON  MATLAB datetime -> pandas datetime value(s).
%
%   py_obj = datetime_to_python(dt, kind)
%     kind = 'scalar'  one pd.Timestamp / pd.NaT        (dt must be scalar)
%            'column'  a datetime64[ns(, zone)] Series  (a table column)
%            'list'    a py.list of Timestamps          (one record per element)
%
%   The one MATLAB owner of sending a datetime to Python. Instants cross as
%   int64 nanoseconds from 1970-01-01 -- convertTo 'epochtime' with 1e9
%   ticks per second, exact (posixtime is a double: ~0.2 us resolution in
%   2026) -- plus the NaT mask and dt.TimeZone. Unzoned ('') = naive: the
%   ticks are the wall clock. Zoned: the ticks are the UTC instant and the
%   zone is kept (py.scimatlab.bridge.datetimes_from_matlab, which owns the
%   label). See docs/claude/datetime-roundtrip.md.

    if nargin < 2
        kind = 'scalar';
    end
    tz = dt.TimeZone;
    if strcmp(tz, 'UTCLeapSeconds')
        % pandas has no leap seconds: the same instants in plain UTC.
        scidb.Log.warn(['datetime_to_python: TimeZone ''UTCLeapSeconds'' sent ' ...
            'as ''UTC'' (a value AT a leap second becomes the next second)']);
        dt.TimeZone = 'UTC';
        tz = 'UTC';
    end

    vals = reshape(dt, 1, []);
    nat = isnat(vals);
    ns = zeros(1, numel(vals), 'int64');
    if any(~nat)
        if isempty(tz)
            epoch = datetime(1970, 1, 1);
        else
            epoch = datetime(1970, 1, 1, 'TimeZone', 'UTC');
        end
        ns(~nat) = convertTo(vals(~nat), 'epochtime', 'Epoch', epoch, ...
            'TicksPerSecond', 1e9);
    end

    scidb.Log.debug('datetime_to_python: %d value(s), TimeZone=''%s'', %d NaT, kind=%s', ...
        numel(vals), tz, sum(nat), kind);
    py_obj = py.scimatlab.bridge.datetimes_from_matlab( ...
        py.numpy.array(ns, pyargs('dtype', 'int64')), ...
        py.numpy.array(nat, pyargs('dtype', 'bool')), tz, kind);
end
