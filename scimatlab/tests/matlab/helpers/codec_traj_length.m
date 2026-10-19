function n = codec_traj_length(traj)
%CODEC_TRAJ_LENGTH  Test function: must RECEIVE a CodecTrajectory (a decoded codec input).
    if ~isa(traj, 'CodecTrajectory')
        error('codec_traj_length:notDecoded', ...
            'expected a CodecTrajectory, got %s', class(traj));
    end
    n = numel(traj.t);
end
