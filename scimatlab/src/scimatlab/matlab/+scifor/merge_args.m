function [tables, name, how] = merge_args(args, err_id)
%SCIFOR.MERGE_ARGS  Split a Merge constructor's arguments into constituents
%   and the trailing name= / how= pairs, validated as Python's scifor.Merge
%   does (count, nesting, name, how). Used by scifor.Merge and scidb.Merge.
    name = "";
    how = "exact";
    while numel(args) >= 2 && (ischar(args{end - 1}) || isstring(args{end - 1})) ...
            && any(strcmpi(string(args{end - 1}), ["name" "how"]))
        key = lower(string(args{end - 1}));
        if key == "name"
            name = strtrim(string(args{end}));
        else
            how = string(args{end});
        end
        args(end - 1:end) = [];
    end
    tables = args;
    if numel(tables) < 2
        error(err_id, 'Merge requires at least 2 inputs, got %d.', numel(tables));
    end
    for i = 1:numel(tables)
        if isa(tables{i}, 'scifor.Merge') || isa(tables{i}, 'scidb.Merge')
            error(err_id, 'Cannot nest Merge inside another Merge.');
        end
    end
    if ~isscalar(name) || strlength(name) == 0
        error(err_id, ['Merge requires a non-empty name= (e.g. name="gait_force"); ' ...
            'it identifies the Merge node on the canvas and in history.']);
    end
    if ~any(how == scifor.merge_hows())
        error(err_id, 'Merge how="%s" is not one of %s.', how, ...
            strjoin(scifor.merge_hows(), ', '));
    end
end
