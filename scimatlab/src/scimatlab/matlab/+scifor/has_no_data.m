function tf = has_no_data(t, cols)
%SCIFOR.HAS_NO_DATA  Is every value of every column in COLS missing?
%
%   TF = scifor.has_no_data(T, COLS)
%
%   THE no-data rule for a for_columns group is owned by Python
%   (scifor.missing). MATLAB's loop applies it to MATLAB tables, so this file
%   runs the same rule natively and is held to the same cases:
%   docs/claude/missing-value-cases.json (TestMissingValueCases.m).
%
%   Missing: NaN / NaT / <missing> / undefined, an empty cell (MATLAB's
%   spelling of a null), and an empty or whitespace-only string. Placeholder
%   text such as "—" is data; so is a numeric array in a cell even when it
%   holds NaN. A table with no rows has no data.
    tf = true;
    if height(t) == 0
        return;
    end
    for c = 1:numel(cols)
        if ~all_missing(t.(char(cols(c))))
            tf = false;
            return;
        end
    end
end



function tf = all_missing(v)
    if isnumeric(v)
        tf = all(isnan(double(v(:))));
    elseif isdatetime(v)
        tf = all(isnat(v(:)));
    elseif isduration(v)
        tf = all(isnan(v(:)));
    elseif isstring(v)
        tf = all(ismissing(v(:)) | strlength(strtrim(v(:))) == 0);
    elseif ischar(v)
        tf = isempty(strtrim(v));
    elseif iscategorical(v)
        tf = all(isundefined(v(:)));
    elseif iscell(v)
        tf = all(cellfun(@cell_missing, v(:)));
    else
        tf = false;
    end
end



function tf = cell_missing(e)
    if isempty(e)
        tf = true;
    elseif ischar(e) || isstring(e)
        tf = all(ismissing(string(e))) || all(strlength(strtrim(string(e))) == 0);
    elseif isnumeric(e) && isscalar(e)
        tf = isnan(double(e));
    elseif isdatetime(e) && isscalar(e)
        tf = isnat(e);
    else
        tf = false;
    end
end
