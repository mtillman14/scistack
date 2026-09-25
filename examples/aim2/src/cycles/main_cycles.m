% Load the per-cycle gait symmetry example dataset.
% Run this file from the examples/aim2 project folder.

%% Add code to path (points to src/ folder)
addpath(genpath(fileparts(fileparts(matlab.desktop.editor.getActiveFilename))));

%% Dataset setup
projectRoot = fileparts(fileparts(fileparts(matlab.desktop.editor.getActiveFilename)));
root_load = fullfile(projectRoot, 'data');

schema = ["subject", "session", "speed", "trial", "cycle"];
scidb.configure_database(fullfile(projectRoot, 'aim2.duckdb'), schema);

%% Load one file per cycle
% Every key is delimited in the file name, so no key_regex is needed; the
% empty key lists let PathInput discover what is actually on disk.
symmetryTemplate = '{subject}/{session}/t{trial}/{subject}_{session}_{speed}_t{trial}_c{cycle}.csv';
symmetryPath = scifor.PathInput(symmetryTemplate, 'root_folder', root_load, 'name', 'symmetryPath');

scidb.for_each(@loadGaitSymmetryOneCycle, ...
    struct('csv_file_path', symmetryPath), ...
    {GaitSymmetryLoaded()}, ...
    subject=[], session=[], speed=[], trial=[], cycle=[] ...
);

% 3 subjects x 4 sessions x 2 speeds x 3 trials x 10 cycles = 720 records.
% In the Plot Studio: group `session`, separate figures by `speed`, and
% collapse `subject`, `trial` and `cycle` — then "Save data (CSV)" writes
% one row per subject/session/speed, one column per joint.
