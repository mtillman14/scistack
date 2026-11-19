function [result, dropped] = merge_tables(parts, schema_keys, varargin)
%SCIFOR.MERGE_TABLES  THE Merge rule on MATLAB tables.
%
%   [RESULT, DROPPED] = scifor.merge_tables(PARTS, SCHEMA_KEYS)
%   [...] = scifor.merge_tables(PARTS, SCHEMA_KEYS, 'Drop', TF, 'Columns', C, ...
%                               'Labels', LABELS, 'Positional', TF, 'How', HOW)
%
%   The rule is owned by Python (scifor.merge: drop_constant_keys +
%   merge_frames). The join must run on MATLAB tables, so this file runs the
%   same rule natively and is held to the same cases:
%   docs/claude/merge-cases.json (TestMergeCases.m).
%
%     1. a part flagged in 'Drop' loses its schema-key columns that hold one
%        value (unless that would leave no column); varying keys stay
%     2. parts join on the schema-key columns they share; a shared non-key
%        column is an error (scifor:Merge)
%     3. no row is dropped silently: a key on one side only, or a
%        many-to-many pairing, is an error; rows follow the side whose keys
%        repeat (else the left); columns are left's, then right's minus keys
%     4. no shared key: column-wise, a 1-row part broadcasts, other height
%        mismatches are an error ('Positional', false: always an error)
%     5. a part with no rows is an error
%
%   'How' (default "exact", the rules above) is one of scifor.merge_hows():
%   inner/outer/left/right relax ONLY rule 3's one-side-only error and rule 5
%   per side, dropping or NaN-padding rows like a SQL join (logged at WARN).
%   Rows follow the right side for "right", else the left; "outer" appends
%   right-only rows. Clash and many-to-many stay errors.
%
%   DROPPED is a struct: each dropped key -> its value, for keys whose
%   dropped values agree across parts (MATLAB's loop adds them back).

    p = inputParser;
    p.addParameter('Drop', false(1, numel(parts)));
    p.addParameter('Labels', strings(0));
    p.addParameter('Positional', true);
    p.addParameter('Columns', {});
    p.addParameter('How', "exact");
    p.parse(varargin{:});
    how = string(p.Results.How);
    if ~any(how == scifor.merge_hows())
        error('scifor:Merge', 'Merge how="%s" is not one of %s.', how, ...
            strjoin(scifor.merge_hows(), ', '));
    end
    drop = logical(p.Results.Drop);
    labels = string(p.Results.Labels);
    if isempty(labels)
        labels = "merge[" + string(0:numel(parts) - 1) + "]";
    end
    % A ROW of keys: `for x = array` loops over COLUMNS, so a column of keys
    % (what string(cellstr(jsondecode(...))) gives) would be one iteration
    % holding every key.
    schema_keys = reshape(string(schema_keys), 1, []);

    if isempty(parts)
        error('scifor:Merge', 'Merge has no constituents.');
    end

    dropped = struct();
    conflicted = string.empty;
    for i = 1:numel(parts)
        if numel(drop) >= i && drop(i)
            [parts{i}, d] = drop_constant(parts{i}, schema_keys);
            names = reshape(string(fieldnames(d)), 1, []);
            for k = names
                if isfield(dropped, char(k))
                    if ~isequaln(dropped.(char(k)), d.(char(k)))
                        conflicted(end + 1) = k; %#ok<AGROW>
                    end
                else
                    dropped.(char(k)) = d.(char(k));
                end
            end
        end
        cols = p.Results.Columns;
        if numel(cols) >= i && ~isempty(cols{i})
            parts{i} = select_columns(parts{i}, string(cols{i}), labels(i));
        end
    end
    % reshape: unique() of an empty string array is 0x1, which a `for` loop
    % runs ONCE (one empty column), and rmfield(dropped, '') then fails.
    for k = reshape(unique(conflicted), 1, [])
        dropped = rmfield(dropped, char(k));
    end

    result = parts{1};
    acc_label = labels(1);
    if numel(parts) == 1 && height(result) == 0
        error('scifor:Merge', 'Cannot merge: %s has no rows.', acc_label);
    end
    for i = 2:numel(parts)
        result = merge_two(result, parts{i}, schema_keys, acc_label, ...
            labels(i), p.Results.Positional, how);
        acc_label = acc_label + "+" + labels(i);
    end
end


function [tbl, dropped] = drop_constant(tbl, schema_keys)
    dropped = struct();
    cols = string(tbl.Properties.VariableNames);
    to_drop = string.empty;
    for sk = reshape(schema_keys(ismember(schema_keys, cols)), 1, [])
        col = tbl.(char(sk));
        txt = key_text(col);
        if height(tbl) <= 1 || all(txt == txt(1))
            to_drop(end + 1) = sk; %#ok<AGROW>
            if height(tbl) == 0
                dropped.(char(sk)) = [];
            elseif iscell(col)
                dropped.(char(sk)) = col{1};
            else
                dropped.(char(sk)) = col(1);
            end
        end
    end
    keep = setdiff(cols, to_drop, 'stable');
    if isempty(keep) || numel(keep) == numel(cols)
        dropped = struct();
        return;
    end
    tbl = tbl(:, cellstr(keep));
end


function result = merge_two(left, right, schema_keys, l_label, r_label, positional, how)
    lcols = string(left.Properties.VariableNames);
    rcols = string(right.Properties.VariableNames);
    shared = intersect(lcols, rcols, 'stable');
    keys = shared(ismember(shared, schema_keys));
    clash = shared(~ismember(shared, schema_keys));
    if ~isempty(clash)
        error('scifor:Merge', ['Column name conflict in Merge: column(s) %s ' ...
            'appear in both %s and %s. Merge joins only on schema keys (%s); ' ...
            'select the column from one constituent with ColumnSelection, ' ...
            'or rename it.'], strjoin(clash, ', '), l_label, r_label, ...
            strjoin(schema_keys, ', '));
    end
    if how ~= "exact" && (height(left) == 0 || height(right) == 0)
        result = join_empty(left, right, how, l_label, r_label);
        return;
    end
    if height(left) == 0
        error('scifor:Merge', 'Cannot merge: %s has no rows.', l_label);
    end
    if height(right) == 0
        error('scifor:Merge', 'Cannot merge: %s has no rows.', r_label);
    end

    if isempty(keys)
        if ~positional
            error('scifor:Merge', ['Cannot join %s and %s: no shared schema ' ...
                'column to join on.'], l_label, r_label);
        end
        lh = height(left);
        rh = height(right);
        if lh == 1 && rh > 1
            left = repmat(left, rh, 1);
        elseif rh == 1 && lh > 1
            right = repmat(right, lh, 1);
        elseif lh ~= rh
            error('scifor:Merge', ['Cannot merge constituents with different ' ...
                'row counts: %s has %d, %s has %d.'], l_label, lh, r_label, rh);
        end
        result = [left, right];
        scifor.Log.debug('[merge] %s + %s: no shared key, column-wise (%d row(s))', ...
            l_label, r_label, height(result));
        return;
    end

    lk = row_keys(left, keys);
    rk = row_keys(right, keys);
    l_dup = numel(unique(lk)) < numel(lk);
    r_dup = numel(unique(rk)) < numel(rk);
    if l_dup && r_dup
        error('scifor:Merge', ['Cannot merge %s and %s on %s: both repeat key ' ...
            'values, so rows cannot be paired.'], l_label, r_label, strjoin(keys, ', '));
    end
    only_l = setdiff(lk, rk, 'stable');
    only_r = setdiff(rk, lk, 'stable');
    if how ~= "exact"
        result = join_how(left, right, keys, lk, rk, how, l_label, r_label, only_l, only_r);
        return;
    end
    if ~isempty(only_l) || ~isempty(only_r)
        error('scifor:Merge', ['Merge would drop rows: %s and %s do not cover ' ...
            'the same %s. %d key value(s) only in %s, %d only in %s. Filter ' ...
            'both constituents to the same locations first (where= or metadata).'], ...
            l_label, r_label, strjoin(keys, ', '), numel(only_l), l_label, ...
            numel(only_r), r_label);
    end
    if r_dup
        [~, l_idx] = ismember(rk, lk);
        r_idx = (1:numel(rk))';
    else
        [~, r_idx] = ismember(lk, rk);
        l_idx = (1:numel(lk))';
    end
    rest = right(:, ~ismember(rcols, keys));
    result = [left(l_idx, :), rest(r_idx, :)];
    scifor.Log.debug('[merge] %s + %s: joined on %s (%d x %d -> %d row(s))', ...
        l_label, r_label, strjoin(keys, ', '), height(left), height(right), height(result));
end


function k = row_keys(tbl, keys)
%ROW_KEYS  One text per row encoding its key values. Numbers, text and
%   datetimes never equal each other (Python: 1 != "1"); NaN equals NaN.
    k = strings(height(tbl), 1);
    for j = 1:numel(keys)
        k = k + key_text(tbl.(char(keys(j)))) + char(31);
    end
end


function s = key_text(col)
    if iscell(col)
        s = strings(numel(col), 1);
        for i = 1:numel(col)
            s(i) = key_text(col{i});
        end
    elseif isnumeric(col) || islogical(col)
        s = "n:" + compose("%.17g", double(col(:)));
    elseif isdatetime(col)
        s = "d:" + compose("%.17g", posixtime(col(:)));
    else
        t = string(col(:));
        m = ismissing(t);
        s = "s:" + t;
        s(m) = "s:<missing>";
    end
end


function tbl = select_columns(tbl, cols, label)
%SELECT_COLUMNS  A ColumnSelection constituent, after its key drop (Python:
%   foreach._prepare_merge selects after drop_constant_keys too).
    missing_cols = cols(~ismember(cols, tbl.Properties.VariableNames));
    if ~isempty(missing_cols)
        error('scifor:Merge', 'Column ''%s'' not found in %s. Available: %s', ...
            missing_cols(1), label, strjoin(tbl.Properties.VariableNames, ', '));
    end
    tbl = tbl(:, cellstr(cols));
end


function result = join_how(left, right, keys, lk, rk, how, l_label, r_label, only_l, only_r)
%JOIN_HOW  Rule 3 relaxed by an explicit How (Python: merge._join_how).
    l_idx = zeros(0, 1);
    r_idx = zeros(0, 1);
    if how == "right"
        for j = 1:numel(rk)
            hits = find(lk == rk(j));
            if isempty(hits)
                hits = 0;
            end
            l_idx = [l_idx; hits(:)]; %#ok<AGROW>
            r_idx = [r_idx; repmat(j, numel(hits), 1)]; %#ok<AGROW>
        end
    else
        for i = 1:numel(lk)
            hits = find(rk == lk(i));
            if ~isempty(hits)
                l_idx = [l_idx; repmat(i, numel(hits), 1)]; %#ok<AGROW>
                r_idx = [r_idx; hits(:)]; %#ok<AGROW>
            elseif how == "left" || how == "outer"
                l_idx(end + 1, 1) = i; %#ok<AGROW>
                r_idx(end + 1, 1) = 0; %#ok<AGROW>
            end
        end
        if how == "outer"
            extra = find(~ismember(rk, lk));
            l_idx = [l_idx; zeros(numel(extra), 1)];
            r_idx = [r_idx; extra(:)];
        end
    end

    out_left = take_rows(left, l_idx);
    from_right = l_idx == 0;
    for k = reshape(keys, 1, [])
        name = char(k);
        kc = left.(name);
        kc = kc(max(l_idx, 1), :);
        if any(from_right)
            rc = right.(name);
            kc(from_right, :) = rc(r_idx(from_right), :);
        end
        out_left.(name) = kc;
    end
    rcols = string(right.Properties.VariableNames);
    rest = right(:, ~ismember(rcols, keys));
    result = [out_left, take_rows(rest, r_idx)];

    msgs = string.empty;
    if any(how == ["inner" "right"]) && ~isempty(only_l)
        msgs(end + 1) = sprintf('dropped %d %s-only key value(s)', numel(only_l), l_label);
    end
    if any(how == ["inner" "left"]) && ~isempty(only_r)
        msgs(end + 1) = sprintf('dropped %d %s-only key value(s)', numel(only_r), r_label);
    end
    if any(how == ["right" "outer"]) && ~isempty(only_r)
        msgs(end + 1) = sprintf('NaN-padded %s columns at %d key value(s)', l_label, numel(only_r));
    end
    if any(how == ["left" "outer"]) && ~isempty(only_l)
        msgs(end + 1) = sprintf('NaN-padded %s columns at %d key value(s)', r_label, numel(only_l));
    end
    if ~isempty(msgs)
        scifor.Log.warn('[merge] %s + %s how=%s: %s', l_label, r_label, how, strjoin(msgs, '; '));
    end
    scifor.Log.debug('[merge] %s + %s: how=%s on %s (%d x %d -> %d row(s))', ...
        l_label, r_label, how, strjoin(keys, ', '), height(left), height(right), height(result));
end


function result = join_empty(left, right, how, l_label, r_label)
%JOIN_EMPTY  A non-exact join with one side empty (Python: merge._join_empty).
    rcols = string(right.Properties.VariableNames);
    extra = right(:, ~ismember(rcols, string(left.Properties.VariableNames)));
    if height(right) == 0 && height(left) > 0 && any(how == ["left" "outer"])
        result = [left, take_rows(extra, zeros(height(left), 1))];
        scifor.Log.warn('[merge] %s + %s how=%s: %s has no rows; kept %d %s row(s) with %s columns NaN-padded', ...
            l_label, r_label, how, r_label, height(left), l_label, r_label);
    elseif height(left) == 0 && height(right) > 0 && any(how == ["right" "outer"])
        lcols = string(left.Properties.VariableNames);
        shared = ismember(lcols, rcols);
        result = take_rows(left, zeros(height(right), 1));
        for name = lcols(shared)
            result.(char(name)) = right.(char(name));
        end
        result = [result, extra];
        scifor.Log.warn('[merge] %s + %s how=%s: %s has no rows; kept %d %s row(s) with %s columns NaN-padded', ...
            l_label, r_label, how, l_label, height(right), r_label, l_label);
    else
        result = [left([], :), extra([], :)];
        scifor.Log.warn('[merge] %s + %s how=%s: one side has no rows -> no rows', ...
            l_label, r_label, how);
    end
end


function out = take_rows(tbl, idx)
%TAKE_ROWS  tbl(idx, :) where idx == 0 is a row of missing values (NaN,
%   <missing>, NaT, <undefined>; numbers become double, as pandas does).
    n = numel(idx);
    mask = idx(:) == 0;
    names = tbl.Properties.VariableNames;
    if height(tbl) == 0
        out = table();
        for i = 1:numel(names)
            out.(names{i}) = blank_column(tbl.(names{i}), n);
        end
        if isempty(names)
            out = table('Size', [n 0], 'VariableTypes', {}, 'VariableNames', {});
        end
        return;
    end
    safe = idx(:);
    safe(mask) = 1;
    out = tbl(safe, :);
    if ~any(mask)
        return;
    end
    for i = 1:numel(names)
        col = out.(names{i});
        if (isnumeric(col) && ~isfloat(col)) || islogical(col)
            col = double(col);
        end
        if isfloat(col)
            col(mask, :) = NaN;
        elseif iscell(col)
            col(mask, :) = {NaN};
        else
            col(mask, :) = missing;
        end
        out.(names{i}) = col;
    end
end


function col = blank_column(like, n)
    w = max(size(like, 2), 1);
    if isnumeric(like) || islogical(like)
        col = NaN(n, w);
    elseif iscell(like)
        col = repmat({NaN}, n, w);
    elseif isstring(like)
        col = strings(n, w);
        col(:) = missing;
    elseif isdatetime(like)
        col = NaT(n, w);
    elseif iscategorical(like)
        col = repmat(categorical(missing), n, w);
    else
        col = repmat(missing, n, w);
    end
end
