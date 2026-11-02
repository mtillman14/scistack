function remove_from_var_group(group_name, variables)
%SCIDB.REMOVE_FROM_VAR_GROUP  Remove one or more variables from a variable group.
%
%   scidb.remove_from_var_group(GROUP_NAME, VARIABLES)
%
%   VARIABLES can be:
%       - A single BaseVariable instance   e.g. StepLength()
%       - A single char/string name        e.g. 'StepLength'
%       - A cell array of either           e.g. {StepLength(), StepWidth()}
%       - A string array of names          e.g. ["StepLength", "StepWidth"]
%
%   Example:
%       scidb.remove_from_var_group("kinematics", "StepTime")
%       scidb.remove_from_var_group("kinematics", {StepLength(), StepWidth()})

    py_names = scidb.internal.resolve_var_names(variables);
    db = py.scidb.store.database.get_database();
    db.remove_from_var_group(char(group_name), py_names);
end
