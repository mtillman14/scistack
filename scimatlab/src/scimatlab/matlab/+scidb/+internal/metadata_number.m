function [v, is_whole] = metadata_number(val)
%METADATA_NUMBER  The one owner of "a whole MATLAB number is an integer" for metadata.
%
%   [V, IS_WHOLE] = scidb.internal.metadata_number(VAL)
%
%   MATLAB numbers are doubles, so subject=1 is 1.0. Metadata is hashed into
%   record ids exactly as given (canonical_hash encodes int 1 as "1" and
%   float 1.0 as "1.0"), and Python's own for_each carries integer schema
%   values as ints. So every MATLAB route sends a whole number as an integer:
%
%     V = int64(VAL) when every element is a finite whole number
%     V = double(VAL) otherwise (a fraction, NaN or Inf anywhere)
%
%   IS_WHOLE is true in the first case. Same shape as VAL. Consumers:
%   scidb.internal.number_to_python (scalar/array -> Python) and
%   scidb.internal.whole_metadata_columns (for_each result tables).

    d = double(val);
    is_whole = all(isfinite(d(:))) && all(d(:) == floor(d(:)));
    if is_whole
        v = int64(d);
    else
        v = d;
    end
end
