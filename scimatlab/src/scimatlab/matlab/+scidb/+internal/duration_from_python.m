function data = duration_from_python(py_box)
%DURATION_FROM_PYTHON  A boxed Python timedelta -> MATLAB duration.
%
%   data = duration_from_python(py_box)
%     py_box: a py.scimatlab.bridge.crossing.DurationCrossing holding a
%             timedelta / pd.Timedelta / np.timedelta64 / NaT.
%
%   MATLAB R2024a converts a bare Python timedelta on arrival, so a
%   timedelta crosses boxed (scimatlab.bridge.crossing). This hands the box
%   back to Python (py.scimatlab.bridge.durations_to_matlab), which answers
%   with seconds (a double, which is what a MATLAB duration holds) and a
%   NaT flag. NaT -> a NaN duration.

    desc = py.scimatlab.bridge.durations_to_matlab(py_box);
    s = double(desc{'seconds'});
    if logical(desc{'nat'})
        data = seconds(NaN);
    else
        data = seconds(s);
    end
    scifor.Log.debug('duration_from_python: %s', char(string(data)));
end
