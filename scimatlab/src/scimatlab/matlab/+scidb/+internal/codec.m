function c = codec(type_name)
%CODEC  The one owner of a MATLAB variable class's custom to_db/from_db.
%
%   C = scidb.internal.codec(TYPE_NAME) inspects the class TYPE_NAME and
%   returns a struct:
%
%       C.type_name  - the class name (char)
%       C.has_codec  - true when the class overrides to_db/from_db
%       C.encode     - @(values) cell of values -> cell of tables (to_db)
%       C.decode     - @(tables) cell of tables -> cell of values (from_db)
%
%   encode/decode take and return CELL arrays so a batch converts with one
%   object construction and one log line; wrap a single value as {value}.
%   They are empty when has_codec is false.
%
%   A class with custom storage defines both methods:
%
%       classdef MarkerTrajectory < scidb.BaseVariable
%           methods
%               function tbl = to_db(obj)        % obj.data -> table
%                   ...
%               end
%           end
%           methods (Static)
%               function data = from_db(tbl)     % table -> original value
%                   ...
%               end
%           end
%       end
%
%   Overriding only one of them, a Static to_db or a non-Static from_db
%   raises scidb:CodecError. Every save and load site gets its codec from
%   here (through scidb.internal.ensure_registered); none inspects the class
%   itself. Not cached: the class is re-read on every call, so an edited
%   classdef takes effect without clearing anything.

    if isstring(type_name), type_name = char(type_name); end
    c = struct('type_name', type_name, 'has_codec', false, ...
               'encode', [], 'decode', []);

    mc = meta.class.fromName(type_name);
    if isempty(mc)
        return;  % not a class on the path (e.g. a type known only to the database)
    end
    ml = mc.MethodList;
    names = {ml.Name};
    to_m = ml(strcmp(names, 'to_db'));
    from_m = ml(strcmp(names, 'from_db'));
    has_to = ~isempty(to_m);
    has_from = ~isempty(from_m);

    if ~has_to && ~has_from
        return;
    end
    if has_to ~= has_from
        if has_to
            missing_name = 'from_db';
        else
            missing_name = 'to_db';
        end
        error('scidb:CodecError', ...
            ['%s overrides only one of to_db/from_db (%s is missing). ' ...
             'Override both or neither.'], type_name, missing_name);
    end
    if to_m(1).Static
        error('scidb:CodecError', ...
            ['%s.to_db must be an ordinary method, not Static: ' ...
             'function tbl = to_db(obj), reading obj.data.'], type_name);
    end
    if ~from_m(1).Static
        error('scidb:CodecError', ...
            ['%s.from_db must be Static (methods (Static)): ' ...
             'function data = from_db(tbl).'], type_name);
    end

    c.has_codec = true;
    c.encode = @(values) encode_values(type_name, values);
    c.decode = @(tables) decode_values(type_name, tables);
    scidb.Log.debug('[codec] %s: custom to_db/from_db', type_name);
end


function tbls = encode_values(type_name, values)
%ENCODE_VALUES  to_db on every value, one object reused for the batch.
    t0 = tic;
    obj = feval(type_name);
    tbls = cell(size(values));
    for i = 1:numel(values)
        obj.data = values{i};
        tbl = obj.to_db();
        if ~istable(tbl)
            error('scidb:CodecError', ...
                '%s.to_db() must return a table, got %s (value %d of %d).', ...
                type_name, class(tbl), i, numel(values));
        end
        tbls{i} = tbl;
    end
    log_timing('to_db', type_name, numel(values), toc(t0));
end


function values = decode_values(type_name, tables)
%DECODE_VALUES  from_db on every stored table.
    t0 = tic;
    from_db_name = [type_name '.from_db'];
    values = cell(size(tables));
    for i = 1:numel(tables)
        values{i} = feval(from_db_name, tables{i});
    end
    log_timing('from_db', type_name, numel(tables), toc(t0));
end


function log_timing(op, type_name, n, elapsed)
%LOG_TIMING  One line per batch; single-value calls stay at DEBUG.
    if n > 1
        scidb.Log.info('[codec] %s: %s on %d value(s) in %.3fs', ...
            type_name, op, n, elapsed);
    else
        scidb.Log.debug('[codec] %s: %s on %d value(s) in %.3fs', ...
            type_name, op, n, elapsed);
    end
end
