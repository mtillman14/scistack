classdef CodecStaticToDbVar < scidb.BaseVariable
%CODECSTATICTODBVAR  Static to_db: must raise scidb:CodecError.
    methods (Static)
        function tbl = to_db(data)
            tbl = table(data, 'VariableNames', {'v'});
        end
        function data = from_db(tbl)
            data = tbl.v;
        end
    end
end
