function tbl = encode_codec_output(tbl, col, codec)
%ENCODE_CODEC_OUTPUT  to_db on every value of a for_each result column.
%
%   TBL = scidb.internal.encode_codec_output(TBL, COL, CODEC)
%
%   scidb.for_each's result table holds each combination's return value in
%   column COL. For a class with custom storage (scidb.internal.codec) every
%   value is replaced by its to_db table, in one batch, before the table
%   crosses to Python; the cell of tables then crosses as one concatenated
%   DataFrame (to_python's table_concat strategy) and Python saves the batch
%   in the columnar "custom" form, never spreading it by schema key.

    vals = tbl.(col);
    if istable(vals)
        vals = arrayfun(@(r) vals(r, :), (1:height(vals))', 'UniformOutput', false);
    elseif ~iscell(vals)
        vals = num2cell(vals);
    end
    tbl.(col) = reshape(codec.encode(vals(:)), [], 1);
end
