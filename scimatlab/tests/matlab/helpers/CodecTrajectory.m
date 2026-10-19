classdef CodecTrajectory
%CODECTRAJECTORY  A user's own value class, saved through CodecTraj's to_db/from_db.
    properties
        t
        label
    end
    methods
        function obj = CodecTrajectory(t, label)
            if nargin > 0
                obj.t = double(t(:));
                obj.label = string(label);
            end
        end
    end
end
