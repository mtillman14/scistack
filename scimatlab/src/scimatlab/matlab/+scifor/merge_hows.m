function hows = merge_hows()
%SCIFOR.MERGE_HOWS  The join methods a Merge accepts, as a string row.
%
%   The owner is Python's scifor.merge.MERGE_HOWS; this reads it through the
%   bridge. FALLBACK is used only when Python is unavailable, and
%   TestMergeHows checks that it equals the Python tuple.
    persistent cached
    if ~isempty(cached)
        hows = cached;
        return;
    end
    try
        hows = string(cell(py.scifor.merge.MERGE_HOWS));
    catch
        hows = scifor.merge_hows_fallback();
    end
    hows = reshape(hows, 1, []);
    cached = hows;
end
