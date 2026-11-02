function [names, values, roles] = option_pairs(args, layer)
%SCIFOR.OPTION_PAIRS  A for_each call's name-value pairs, each with its role.
%
%   [NAMES, VALUES, ROLES] = scifor.option_pairs(ARGS, LAYER)
%
%   ARGS is the name-value part of a for_each call; LAYER is 'scidb' or
%   'scifor'. Python decides which names are options
%   (scifor.options.classify_for_each_args, from scifor's FOR_EACH_OPTIONS,
%   which also warns about a name that looks like a misspelled option), so
%   neither MATLAB split_options keeps its own option list. ROLES{k} is:
%
%     'option'   - a public option LAYER acts on
%     'discard'  - a scidb option MATLAB scifor accepts and ignores
%     'internal' - a leading-underscore name (the scidb -> scifor handshake)
%     'axis'     - a metadata iteration axis
%
%   Option, discard and internal names come back lowercased (they match
%   case-insensitively); an axis name is returned exactly as given. A name
%   that is not text is an axis (the metadata parser reports it).

    if mod(numel(args), 2) ~= 0
        error('scifor:for_each', 'Metadata arguments must be name-value pairs.');
    end
    names = args(1:2:end);
    values = args(2:2:end);
    roles = repmat({'axis'}, 1, numel(names));
    is_text = cellfun(@(k) ischar(k) || (isstring(k) && isscalar(k)), names);
    if ~any(is_text)
        return;
    end
    texts = cellfun(@char, names(is_text), 'UniformOutput', false);
    py_roles = cell(py.scifor.options.classify_for_each_args(py.list(texts), layer));
    text_roles = cellfun(@char, py_roles, 'UniformOutput', false);
    text_idx = find(is_text);
    for j = find(~strcmp(text_roles, 'axis'))
        names{text_idx(j)} = lower(texts{j});
    end
    roles(text_idx) = text_roles;
end
