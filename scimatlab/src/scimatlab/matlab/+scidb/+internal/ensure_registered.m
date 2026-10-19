function [py_class, codec] = ensure_registered(type_name)
%ENSURE_REGISTERED  Auto-register a MATLAB variable type with Python.
%
%   PY_CLASS = scidb.internal.ensure_registered(TYPE_NAME) ensures that
%   the given type name has a Python surrogate BaseVariable subclass.
%   Returns the surrogate class for use in database calls.
%
%   [PY_CLASS, CODEC] = ... also returns the class's custom to_db/from_db
%   (scidb.internal.codec). The surrogate is told whether the class has
%   one, so Python stores its to_db tables in the columnar "custom" form.
%
%   This is called automatically by BaseVariable instance methods (save,
%   load, etc.) — users do not need to call this directly.

    codec = scidb.internal.codec(type_name);
    py_class = py.scimatlab.bridge.register_matlab_variable( ...
        type_name, pyargs('codec', codec.has_codec));
end
