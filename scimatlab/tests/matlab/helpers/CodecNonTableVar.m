classdef CodecNonTableVar < scidb.BaseVariable
%CODECNONTABLEVAR  to_db returns a struct, not a table: must raise scidb:CodecError.
    methods
        function s = to_db(obj)
            s = struct('v', obj.data);
        end
    end
    methods (Static)
        function data = from_db(tbl)
            data = tbl.v;
        end
    end
end
