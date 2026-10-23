function data = datetime_from_python(py_obj)
%DATETIME_FROM_PYTHON  pandas / Python datetime value(s) -> MATLAB datetime.
%
%   data = datetime_from_python(py_obj)
%     py_obj: a datetime scalar (pd.Timestamp, NaT, datetime, np.datetime64)
%             -> a scalar datetime; a datetime64 Series -> an Nx1 datetime.
%
%   The one MATLAB owner of receiving a datetime from Python (the inverse
%   of datetime_to_python). py.scimatlab.bridge.datetimes_to_matlab hands
%   over int64 nanoseconds (wall clock when tz is '', else the UTC instant),
%   a NaT mask and ONE TimeZone; the result is rebuilt exactly from the
%   ticks. Values holding several zones (or naive and aware mixed) cannot
%   be one MATLAB datetime array: the caller converts them one at a time,
%   so this errors with 'scidb:DatetimeZones'.

    desc = py.scimatlab.bridge.datetimes_to_matlab(py_obj);
    if ~logical(desc{'ok'})
        error('scidb:DatetimeZones', 'datetime_from_python: %s', char(desc{'reason'}));
    end
    % Raw little-endian int64 bytes, not desc{'ns'}: a numeric array through
    % from_python becomes double, which cannot hold ~1.8e18 ns exactly.
    raw = uint8(desc{'ns_bytes'});
    if isempty(raw)
        ns = zeros(0, 1, 'int64');
    else
        ns = typecast(raw(:)', 'int64');
    end
    nat = logical(scidb.internal.from_python(desc{'nat'}));
    tz = char(desc{'tz'});
    ns = ns(:);
    nat = nat(:);

    if isempty(tz)
        data = datetime(ns, 'ConvertFrom', 'epochtime', 'Epoch', datetime(1970, 1, 1), ...
            'TicksPerSecond', 1e9);
    else
        data = datetime(ns, 'ConvertFrom', 'epochtime', ...
            'Epoch', datetime(1970, 1, 1, 'TimeZone', 'UTC'), ...
            'TicksPerSecond', 1e9, 'TimeZone', 'UTC');
        data.TimeZone = tz;  % same instants, shown in their own zone
    end
    data(nat) = NaT;
    data.Format = 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS';

    if logical(desc{'scalar'})
        data = data(1);
    end
    scidb.Log.debug('datetime_from_python: %d value(s), TimeZone=''%s'', %d NaT', ...
        numel(data), tz, sum(nat));
end
