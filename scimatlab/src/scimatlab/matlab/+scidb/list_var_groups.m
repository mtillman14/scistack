function groups = list_var_groups()
%SCIDB.LIST_VAR_GROUPS  List all variable group names.
%
%   GROUPS = scidb.list_var_groups()
%
%   Returns a string array of group names, sorted alphabetically.
%
%   Example:
%       groups = scidb.list_var_groups()
%       % ["emg", "kinematics", "demographics"]

    db = py.scidb.store.database.get_database();
    py_list = db.list_var_groups();
    groups = string(cell(py_list));
end
