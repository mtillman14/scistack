function out = latest_score(g)
%LATEST_SCORE  The score on a column group's last row (for_columns groups
%   tests): G is a sub-table whose first column is the score.
    out = g{end, 1};
end
