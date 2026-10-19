classdef CodecOnlyToDbVar < scidb.BaseVariable
%CODECONLYTODBVAR  Overrides to_db without from_db: must raise scidb:CodecError.
    methods
        function tbl = to_db(obj)
            tbl = table(obj.data, 'VariableNames', {'v'});
        end
    end
end
