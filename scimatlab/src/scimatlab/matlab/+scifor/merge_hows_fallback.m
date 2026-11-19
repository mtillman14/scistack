function hows = merge_hows_fallback()
%SCIFOR.MERGE_HOWS_FALLBACK  Copy of scifor.merge.MERGE_HOWS for when Python
%   is unavailable. Guarded by TestMergeHows (must equal the Python tuple).
    hows = ["exact" "inner" "outer" "left" "right"];
end
