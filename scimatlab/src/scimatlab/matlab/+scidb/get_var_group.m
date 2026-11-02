function variables = get_var_group(group_name)
%SCIDB.GET_VAR_GROUP  Get all variable classes in a variable group.
%
%   VARIABLES = scidb.get_var_group(GROUP_NAME)
%
%   Returns a cell array of BaseVariable instances, one per variable in
%   the group.  Each instance is constructed via its class name (e.g.
%   StepLength()), so the class must be on the MATLAB path.
%
%   Example:
%       vars = scidb.get_var_group("kinematics");
%       % {[StepLength], [StepTime], [StepWidth]}
%       for i = 1:numel(vars)
%           data = vars{i}.load(subject=1);
%       end

    db = py.scidb.store.database.get_database();
    duck = py.getattr(db, '_duck');
    py_names = duck.get_group(char(group_name));
    names = string(cell(py_names));

    variables = cell(1, numel(names));
    for i = 1:numel(names)
        variables{i} = eval(names(i) + "()");
    end
end
