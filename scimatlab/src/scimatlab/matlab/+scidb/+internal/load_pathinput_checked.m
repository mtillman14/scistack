function filepath = load_pathinput_checked(pi_obj, meta_nv, key_types, schema_keys)
%LOAD_PATHINPUT_CHECKED  Per-combo PathInput load enforcing declared key types.
%
%   FILEPATH = load_pathinput_checked(PI, META_NV, KEY_TYPES, SCHEMA_KEYS)
%
%   The policy is Python's (scidb.run.foreach.pathinput_numeric_eligible and
%   pathinput_key_type_offender, also used by _load_pathinput_checked); this
%   file only calls it and raises the MATLAB error. The reporting half lives
%   in scifor.PathInput.load_with_captures, which stays policy-free:
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

    py_kt = py.dict();
    kt_fields = fieldnames(key_types);
    for i = 1:numel(kt_fields)
        py_kt{kt_fields{i}} = char(string(key_types.(kt_fields{i})));
    end
    meta_keys = strings(1, numel(meta_nv) / 2);
    for i = 1:2:numel(meta_nv)
        meta_keys((i + 1) / 2) = string(meta_nv{i});
    end
    policy = py.importlib.import_module('scidb.run.foreach');
    eligible = string(cellfun(@char, cell(policy.pathinput_numeric_eligible( ...
        py.list(cellstr(meta_keys)), py_kt)), 'UniformOutput', false));

    % Numeric bridges only: an alias spelling ("Baseline" for canonical
    % "BL") is a declared equivalence. scifor owns which kind a bridge was.
    [filepath, resolutions] = pi_obj.load_with_captures(meta_nv, eligible, "numeric");

    res_fields = fieldnames(resolutions);
    py_res = py.dict();
    for i = 1:numel(res_fields)
        py_res{res_fields{i}} = char(resolutions.(res_fields{i}));
    end
    offender = policy.pathinput_key_type_offender(py_res, py_kt, ...
        py.list(cellstr(string(schema_keys))));
    if ~isa(offender, 'py.NoneType')
        key = string(char(offender{1}));
        spelling = string(char(offender{2}));
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
             'its identity (it is stored in the database): tick ' ...
             '''Numeric'' for ''%s'' in the GUI, or call ' ...
             'scidb.configure_database(..., ' ...
             'schema_key_types=struct(''%s'', ''numeric'')) once, to treat ' ...
             'values as numbers (canonical, no leading zeros), or ' ...
             '''string'' to make spelling significant (exact matches ' ...
             'only).'], ...
            key, given, spelling, pi_obj.path_template, key, key, key);
    end

    if ~isempty(res_fields)
        scifor.Log.debug('pathinput resolved spellings (declared numeric): %s', ...
            strjoin(string(res_fields), ', '));
    end
end
