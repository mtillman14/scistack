function add_to_var_group(group_name, variables)
%SCIDB.ADD_TO_VAR_GROUP  Add one or more variables to a variable group.
%
%   scidb.add_to_var_group(GROUP_NAME, VARIABLES)
%
%   VARIABLES can be:
%       - A single BaseVariable instance   e.g. StepLength()
%       - A single char/string name        e.g. 'StepLength'
%       - A cell array of either           e.g. {StepLength(), StepWidth()}
%       - A string array of names          e.g. ["StepLength", "StepWidth"]
%
%   Adding the same variable to a group twice is idempotent (no duplicates).
%
%   Example:
%       scidb.add_to_var_group("kinematics", {StepLength(), StepWidth()})
%       scidb.add_to_var_group("kinematics", ["StepLength", "StepWidth"])
%       scidb.add_to_var_group("kinematics", 'StepLength')

    py_names = scidb.internal.resolve_var_names(variables);
    db = py.scidb.store.database.get_database();
    db.add_to_var_group(char(group_name), py_names);
end
