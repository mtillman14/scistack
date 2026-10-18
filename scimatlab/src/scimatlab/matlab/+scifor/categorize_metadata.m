function tbl = categorize_metadata(tbl, meta_fields, schema_keys)
%SCIFOR.CATEGORIZE_METADATA  The categorical=true conversion of a result table.
%
%   TBL = scifor.categorize_metadata(TBL, META_FIELDS, SCHEMA_KEYS) sorts
%   TBL's rows by its schema columns (in SCHEMA_KEYS order, numeric-aware)
%   and converts each metadata column in META_FIELDS to categorical, with
%   categories in first-seen (sorted) order.
%
%   One owner for both scifor.for_each(categorical=true) and
%   scidb.for_each(categorical=true); scidb applies it to the table it
%   returns, after saving, so categorical columns never cross to Python.
    meta_fields = cellstr(string(meta_fields));
    meta_fields = meta_fields(ismember(meta_fields, tbl.Properties.VariableNames));

    sort_cols = intersect(string(schema_keys), string(meta_fields), 'stable');
    if ~isempty(sort_cols)
        tbl = sort_by_schema_columns(tbl, sort_cols);
    end

    for f = 1:numel(meta_fields)
        col = tbl.(meta_fields{f});
        str_col = string(col);
        unique_vals = unique(str_col, 'stable');
        tbl.(meta_fields{f}) = categorical(str_col, unique_vals);
    end
end


function tbl = sort_by_schema_columns(tbl, sort_cols)
%SORT_BY_SCHEMA_COLUMNS  Sort table rows by schema columns, numeric-aware.
%   Numeric columns sort numerically. String columns that contain only
%   numeric-like values (e.g., "1", "10", "2") sort numerically rather
%   than alphabetically. Other string columns sort alphabetically.
    n = height(tbl);
    if n <= 1
        return;
    end
    sort_matrix = zeros(n, numel(sort_cols));
    for i = 1:numel(sort_cols)
        col = tbl.(char(sort_cols(i)));
        if isnumeric(col)
            sort_matrix(:, i) = col;
        else
            str_col = string(col);
            nums = str2double(str_col);
            if all(~isnan(nums))
                % All values are numeric-like strings — sort numerically
                sort_matrix(:, i) = nums;
            else
                % Non-numeric strings — sort alphabetically via rank
                [~, ~, sort_matrix(:, i)] = unique(str_col);
            end
        end
    end
    [~, order] = sortrows(sort_matrix);
    tbl = tbl(order, :);
end
