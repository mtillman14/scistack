function [gaitSymmetry] = loadGaitSymmetryOneCycle(csv_file_path)

%% PURPOSE: LOAD ONE GAIT CYCLE'S SYMMETRY VALUES
% Inputs:
% csv_file_path: Full path to one cycle's CSV file
%
% Outputs:
% gaitSymmetry: Struct with one field per joint (ankle, knee, hip)
%
% NOTE: Each file holds one row: left vs. right symmetry per joint, where 0
% is perfectly symmetric and larger values are more asymmetric. Returning a
% struct rather than three outputs stores the joints as one variable with one
% column each, so the Plot Studio offers them as a ColName factor.

data = readtable(csv_file_path);

gaitSymmetry = struct( ...
    'ankle', data.ankle(1), ...
    'knee', data.knee(1), ...
    'hip', data.hip(1) ...
);
end
