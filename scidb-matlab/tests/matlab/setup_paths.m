function setup_paths()
%SETUP_PATHS  Add MATLAB and Python paths for integration tests.
%
%   Call this once before running any tests. It adds:
%   - The scidb MATLAB source (+scidb package) to the MATLAB path
%   - The test helpers directory to the MATLAB path
%   - All monorepo Python package source directories to py.sys.path

    this_dir = fileparts(mfilename('fullpath'));
    matlab_root = fullfile(this_dir, '..', '..', 'src', 'scidb_matlab', 'matlab');
    helpers_dir = fullfile(this_dir, 'helpers');
    workspace_root = fullfile(this_dir, '..', '..', '..');

    % Add MATLAB paths
    addpath(matlab_root);
    addpath(helpers_dir);

    % Add Python paths for all monorepo packages
    py_paths = {
        fullfile(workspace_root, 'src')
        fullfile(workspace_root, 'thunk-lib', 'src')
        fullfile(workspace_root, 'canonical-hash', 'src')
        fullfile(workspace_root, 'sciduck', 'src')
        fullfile(workspace_root, 'pipelinedb-lib', 'src')
        fullfile(workspace_root, 'path-gen', 'src')
        fullfile(workspace_root, 'scifor', 'src')
        fullfile(workspace_root, 'scirun-lib', 'src')
        fullfile(workspace_root, 'scidb-matlab', 'src')
    };

    for i = 1:numel(py_paths)
        p = py_paths{i};
        if count(py.sys.path, p) == 0
            py.sys.path().insert(int64(0), p);
        end
    end
end
