function [py_val, is_whole] = number_to_python(val)
%NUMBER_TO_PYTHON  A MATLAB metadata number as the Python value it is sent as.
%
%   [PY_VAL, IS_WHOLE] = scidb.internal.number_to_python(VAL)
%
%   The whole-number decision is scidb.internal.metadata_number's (the
%   owner); this only builds the Python object:
%
%     scalar VAL  -> py.int when whole, else py.float
%     array VAL   -> numpy int64 array when every element is whole, else a
%                    float64 array (one bridge crossing for a whole column)
%
%   Callers that need a py.list use PY_VAL.tolist() on the array form.

    [v, is_whole] = scidb.internal.metadata_number(val);
    if isscalar(val)
        if is_whole
            py_val = py.int(v);
        else
            py_val = py.float(v);
        end
    elseif is_whole
        py_val = py.numpy.array(v(:)', pyargs('dtype', 'int64'));
    else
        py_val = py.numpy.array(v(:)', pyargs('dtype', 'float64'));
    end
end
