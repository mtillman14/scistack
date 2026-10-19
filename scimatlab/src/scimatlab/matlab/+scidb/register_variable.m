function register_variable(type_arg, options)
%SCIDB.REGISTER_VARIABLE  Register a variable type for database storage.
%
%   scidb.register_variable(TypeClass) registers a BaseVariable subclass.
%   The class name is extracted automatically.
%
%   scidb.register_variable(TypeClass, schema_version=2) sets a custom
%   schema version for the type.
%
%   This is only needed when you want to set a non-default schema_version.
%   Types are auto-registered with schema_version=1 on first save/load.
%
%   Arguments:
%       type_arg - A BaseVariable subclass instance (e.g. RawSignal())
%
%   Name-Value Arguments:
%       schema_version - Schema version number (default 1)
%
%   Example:
%       scidb.register_variable(RawSignal(), schema_version=2);

    arguments
        type_arg {mustBeA(type_arg, 'scidb.BaseVariable')}
        options.schema_version (1,1) double = 1
    end

    type_name = class(type_arg);
    codec = scidb.internal.codec(type_name);

    py.scimatlab.bridge.register_matlab_variable( ...
        type_name, ...
        int64(options.schema_version), ...
        pyargs('codec', codec.has_codec));
end
