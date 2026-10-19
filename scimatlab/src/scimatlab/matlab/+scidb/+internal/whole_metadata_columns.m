function [tbl, cast_cols] = whole_metadata_columns(tbl, data_cols)
%WHOLE_METADATA_COLUMNS  Whole-number metadata columns of a result table -> int64.
%
%   [TBL, CAST_COLS] = scidb.internal.whole_metadata_columns(TBL, DATA_COLS)
%
%   scidb.for_each hands Python one result table per output: metadata
%   columns (subject, session, ...) plus the output's data column. A MATLAB
%   numeric metadata column is double, so without this the saves were filed
%   under subject=1.0 while save() and Python's for_each use the int 1.
%   Applies scidb.internal.metadata_number (the owner) to every numeric
%   column EXCEPT DATA_COLS (outputs: data keeps its own type) and internal
%   bookkeeping columns (any name containing "__", e.g. x__combo).
%   CAST_COLS names the columns changed.

    data_cols = string(data_cols);
    cast_cols = string.empty;
    names = string(tbl.Properties.VariableNames);
    for c = names
        % contains, not startsWith: the bridge renames __combo to x__combo.
        if ismember(c, data_cols) || contains(c, "__")
            continue;
        end
        col = tbl.(c);
        if ~isnumeric(col) || isinteger(col) || isempty(col)
            continue;
        end
        [v, is_whole] = scidb.internal.metadata_number(col);
        if is_whole
            tbl.(c) = v;
            cast_cols(end + 1) = c; %#ok<AGROW>
        end
    end
end
