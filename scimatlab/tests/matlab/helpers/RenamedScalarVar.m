classdef RenamedScalarVar < scidb.BaseVariable
%RENAMEDSCALARVAR  Fixture: column_name renames its for_each result column.
    properties (Constant)
        column_name = "renamed_val"
    end
end
