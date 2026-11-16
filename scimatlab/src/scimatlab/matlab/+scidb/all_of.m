function result = all_of(filters)
%SCIDB.ALL_OF  Combine filters with AND; a where= list means all of them.
%
%   F = scidb.all_of({Age() > 80, Sex() == "M"})
%
%   where={A, B} is one run that every filter must pass, the same as
%   where=A & B. (Separate runs, one per filter, are where=scidb.EachOf(A, B).)
%   A value that is not a cell is returned unchanged, so callers can pass
%   whatever the user wrote. An empty cell is no filter ([]).
%
%   The MATLAB twin of Python's scidb.all_of. The AND itself is done by
%   scidb.Filter's & operator, which delegates to Python.

    if ~iscell(filters)
        result = filters;
        return;
    end
    result = [];
    for i = 1:numel(filters)
        item = filters{i};
        if iscell(item)
            item = scidb.all_of(item);
        end
        if isempty(item)
            continue;
        end
        if ~isa(item, 'scidb.Filter')
            error('scidb:WhereListEntry', ...
                'where= list entries must be filters (e.g. Age() > 80), got %s.', ...
                class(item));
        end
        if isempty(result)
            result = item;
        else
            result = result & item;
        end
    end
end
