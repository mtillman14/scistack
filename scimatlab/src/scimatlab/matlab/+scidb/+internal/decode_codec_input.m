function value = decode_codec_input(value, codec, var_name, data_cols, rid_col)
%DECODE_CODEC_INPUT  Rebuild a loaded for_each input of a to_db/from_db class.
%
%   VALUE = scidb.internal.decode_codec_input(VALUE, CODEC, VAR_NAME,
%                                              DATA_COLS, RID_COL)
%
%   scidb.for_each loads an input as ONE table for every combination. For a
%   class with custom storage (scidb.internal.codec) that table has one row
%   per STORED row: each record's to_db table, row by row, with the
%   metadata and record-id column repeated. This groups the rows by record
%   id (RID_COL, e.g. "x__record_id"; inside a scifor.Fixed, where the
%   bridge strips it, by the metadata columns), runs from_db on each record's stored
%   columns (DATA_COLS, from scimatlab.bridge.codec_storage_columns) in one
%   batch, and returns one row per record: the metadata columns plus a cell
%   column VAR_NAME holding the rebuilt value — the shape of any
%   single-value variable, so the loop hands each call the object.
%
%   VALUE may be a table or a scifor.Fixed wrapping one; anything else is
%   returned unchanged.

    if istable(value)
        value = decode_table(value, codec, var_name, data_cols, rid_col);
    elseif isa(value, 'scifor.Fixed') && istable(value.data)
        meta = value.fixed_metadata;
        nv = {};
        for f = reshape(fieldnames(meta), 1, [])
            nv(end + 1:end + 2) = {f{1}, meta.(f{1})}; %#ok<AGROW>
        end
        value = scifor.Fixed( ...
            decode_table(value.data, codec, var_name, data_cols, rid_col), nv{:});
    end
end


function out = decode_table(tbl, codec, var_name, data_cols, rid_col)
    names = string(tbl.Properties.VariableNames);
    data_cols = string(data_cols);
    missing_cols = setdiff(data_cols, names, 'stable');
    if ~isempty(missing_cols)
        error('scidb:CodecError', ...
            '%s: stored column(s) %s missing from the loaded input.', ...
            var_name, strjoin(missing_cols, ', '));
    end
    meta_cols = names(~ismember(names, data_cols));

    % Group rows into records: by record id when the input carries it (a
    % plain variable input), else by the metadata columns (scifor.Fixed:
    % the bridge strips __record_id inside wrappers). The load returns the
    % latest record per location, so one metadata combination = one record.
    if ismember(rid_col, names)
        group_key = string(tbl.(rid_col));
        grouped_by = rid_col;
    elseif ~isempty(meta_cols)
        parts = strings(height(tbl), numel(meta_cols));
        for c = 1:numel(meta_cols)
            parts(:, c) = string(tbl.(char(meta_cols(c))));
        end
        parts(ismissing(parts)) = "";  % unique() treats each <missing> as distinct
        group_key = join(parts, char(30), 2);
        grouped_by = "metadata " + strjoin(meta_cols, ",");
    else
        group_key = repmat("", height(tbl), 1);  % no metadata: one record
        grouped_by = "(single record)";
    end
    [~, first_idx, grp] = unique(group_key, 'stable');
    n = numel(first_idx);
    stored = cell(n, 1);
    for r = 1:n
        stored{r} = tbl(grp == r, cellstr(data_cols));
    end
    values = codec.decode(stored);

    out = tbl(first_idx, cellstr(meta_cols));
    out.(var_name) = values(:);
    scidb.Log.debug('[codec] %s: %d stored row(s) -> %d record(s), grouped by %s', ...
        var_name, height(tbl), n, grouped_by);
end
