function traj = make_codec_traj(raw)
%MAKE_CODEC_TRAJ  Test function: wrap samples in a CodecTrajectory (a codec output).
    traj = CodecTrajectory(raw, "heel");
end
