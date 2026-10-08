function tbl = list_exclusions()
%SCIDB.LIST_EXCLUSIONS  Return currently-excluded schema combinations.
%
%   tbl = scidb.list_exclusions()
%
%   Returns a MATLAB table of schema-key combinations whose effective
%   status is excluded.  Each row represents the latest record for a
%   given exact keyset where that keyset is still excluded.
%
%   Columns:
%       <schema key columns>  - Schema key values (strings; NULL if wildcard)
%       reason                - Human-readable exclusion reason
%       changed_at            - Timestamp of the exclusion
%
%   Returns an empty table if no exclusions are currently active.

    py_df = py.scidb.exclusions.list_exclusions(py.None);

    % Convert Python DataFrame → MATLAB table
    if py_df.empty
        % Build empty table with correct column names
        py_cols = py_df.columns.tolist();
        col_names = cellfun(@char, cell(py_cols), 'UniformOutput', false);
        tbl = array2table(zeros(0, numel(col_names)));
        tbl.Properties.VariableNames = col_names;
        return;
    end

    % Use the bridge helper for DataFrame conversion
    tbl = py2mat_df(py_df);

end


function tbl = py2mat_df(py_df)
%PY2MAT_DF  Convert a Python pandas DataFrame to a MATLAB table.
    col_names = cellfun(@char, cell(py_df.columns.tolist()), 'UniformOutput', false);
    n_rows = int32(py_df.shape{1});
    n_cols = numel(col_names);
    data = cell(n_rows, n_cols);
    for c = 1:n_cols
        col_vals = py_df{col_names{c}}.tolist();
        col_cell = cell(col_vals);
        for r = 1:n_rows
            v = col_cell{r};
            if isa(v, 'py.NoneType')
                data{r, c} = missing;
            elseif isa(v, 'py.str')
                data{r, c} = char(v);
            elseif isnumeric(v) || islogical(v)
                data{r, c} = double(v);
            else
                data{r, c} = char(string(v));
            end
        end
    end
    tbl = cell2table(data, 'VariableNames', col_names);
end
