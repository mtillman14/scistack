classdef CodecTraj < scidb.BaseVariable
%CODECTRAJ  Variable with custom storage: a CodecTrajectory <-> a (t, label) table.
    methods
        function tbl = to_db(obj)
            traj = obj.data;
            n = numel(traj.t);
            tbl = table(traj.t, repmat(traj.label, n, 1), ...
                'VariableNames', {'t', 'label'});
        end
    end
    methods (Static)
        function data = from_db(tbl)
            data = CodecTrajectory(tbl.t, string(tbl.label(1)));
        end
    end
end
