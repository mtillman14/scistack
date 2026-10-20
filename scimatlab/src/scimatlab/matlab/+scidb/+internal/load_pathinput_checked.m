function filepath = load_pathinput_checked(pi_obj, meta_nv, key_types, schema_keys)
%LOAD_PATHINPUT_CHECKED  Per-combo PathInput load enforcing declared key types.
%
%   FILEPATH = load_pathinput_checked(PI, META_NV, KEY_TYPES, SCHEMA_KEYS)
%
%   MATLAB mirror of Python scidb.foreach._load_pathinput_checked — the
%   policy half of the zero-padded-filename contract (the reporting half
%   lives in scifor.PathInput.load_with_captures, which stays policy-free):
%
%   - 'string'-declared keys are excluded from the numeric-equivalence
%     fallback entirely: spelling is identity, "1" never matches "001".
%   - 'numeric'-declared keys may resolve freely (their stored identity is
%     already canonical; only the filename lookup bridges spellings).
%   - An UNDECLARED schema key whose spelling had to be bridged raises
%     scidb:SchemaKeyTypeError: the dataset has proven the spelling
%     ambiguous, so the user must declare the key's type once.  Non-schema
%     keys keep the silent fallback.
%
%   KEY_TYPES is a struct (key -> 'numeric'|'string'); SCHEMA_KEYS a string
%   array.  META_NV is the combo's name-value cell.

    kt_fields = fieldnames(key_types);
    string_keys = strings(0);
    numeric_keys = strings(0);
    for i = 1:numel(kt_fields)
        t = string(key_types.(kt_fields{i}));
        if t == "string"
            string_keys(end+1) = string(kt_fields{i}); %#ok<AGROW>
        elseif t == "numeric"
            numeric_keys(end+1) = string(kt_fields{i}); %#ok<AGROW>
        end
    end

    meta_keys = strings(1, numel(meta_nv) / 2);
    for i = 1:2:numel(meta_nv)
        meta_keys((i + 1) / 2) = string(meta_nv{i});
    end
    eligible = setdiff(meta_keys, string_keys, 'stable');

    % Numeric bridges only: an alias spelling ("Baseline" for canonical
    % "BL") is a declared equivalence. scifor owns which kind a bridge was.
    [filepath, resolutions] = pi_obj.load_with_captures(meta_nv, eligible, "numeric");

    res_fields = fieldnames(resolutions);
    for i = 1:numel(res_fields)
        key = string(res_fields{i});
        if ismember(key, string(schema_keys)) && ~ismember(key, numeric_keys)
            % Recover the given value for the error message.
            given = "";
            for j = 1:2:numel(meta_nv)
                if string(meta_nv{j}) == key
                    given = string(meta_nv{j+1});
                    break;
                end
            end
            error('scidb:SchemaKeyTypeError', ...
                ['PathInput resolved %s=%s to ''%s'' on disk (template ' ...
                 '''%s'') — the spelling of schema key ''%s'' is ambiguous ' ...
                 '(zero-padded filenames). Declare its type once to fix ' ...
                 'its identity: scidb.configure_database(..., ' ...
                 'schema_key_types=struct(''%s'', ''numeric'')) to treat ' ...
                 'values as numbers (canonical, no leading zeros), or ' ...
                 '''string'' to make spelling significant (exact matches ' ...
                 'only).'], ...
                key, given, resolutions.(res_fields{i}), ...
                pi_obj.path_template, key, key);
        end
    end

    if ~isempty(res_fields)
        scidb.Log.debug('pathinput resolved spellings (declared numeric): %s', ...
            strjoin(string(res_fields), ', '));
    end
end
