function name = output_column_name(output)
%SCIFOR.OUTPUT_COLUMN_NAME  Result column name for one entry of outputs=.
%
%   NAME = scifor.output_column_name(OUTPUT)
%
%   First hit wins (same rule as Python's scifor.output_column_name):
%     1. char / string     -> the name itself
%     2. an object whose class defines a Constant property column_name set
%        to non-empty text -> that text
%     3. any other object  -> class(OUTPUT)
%
%   Define the override on a variable class like this:
%
%       classdef StepLength < scidb.BaseVariable
%           properties (Constant)
%               column_name = "step_len"
%           end
%       end
%
%   The name only applies when an output lands in ONE cell of the result
%   table; table outputs keep their own columns.
%
%   Cases shared with Python: docs/claude/output-column-name-cases.json.

    if ischar(output) || (isstring(output) && isscalar(output))
        name = char(output);
        return;
    end
    if isobject(output) && isprop(output, 'column_name')
        custom = output.column_name;
        if (ischar(custom) || (isstring(custom) && isscalar(custom))) ...
                && strlength(string(custom)) > 0
            name = char(custom);
            return;
        end
    end
    name = class(output);
end
