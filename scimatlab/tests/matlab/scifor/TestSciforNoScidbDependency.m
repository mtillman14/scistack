classdef TestSciforNoScidbDependency < matlab.unittest.TestCase
%TESTSCIFORNOSCIDBDEPENDENCY  scifor's MATLAB layer must stay scidb-free.
%
%   Mirrors Python's test_scifor_does_not_import_scidb (scifor/tests/
%   test_logging.py): standalone scifor must not require the Python
%   scidb package. scifor.Log delegates to py.scistacklog.Log directly
%   (not py.scidb.model.log.Log) so that "no database configured" MATLAB usage
%   doesn't pull in scidb just to log.
%
%   A source-text guard (rather than a py.sys.modules check) is used
%   because MATLAB tests share one long-lived pyenv session, so
%   sys.modules can already contain 'scidb' from an earlier test in the
%   same run regardless of what +scifor itself does.

    methods (Test)
        function test_scifor_sources_never_reference_py_scidb(tc)
            scifor_dir = fullfile(scimatlab_root(), 'src', 'scimatlab', ...
                'matlab', '+scifor');
            files = dir(fullfile(scifor_dir, '*.m'));
            tc.assertNotEmpty(files, 'could not locate +scifor source files');

            for i = 1:numel(files)
                path = fullfile(files(i).folder, files(i).name);
                text = fileread(path);
                tc.verifyEqual(count(text, 'py.scidb'), 0, ...
                    sprintf(['%s references py.scidb — scifor must stay ' ...
                    'scidb-free and delegate logging via scifor.Log ' ...
                    '(py.scistacklog.Log) instead'], files(i).name));
            end
        end

        function test_scifor_log_delegates_to_scistacklog(tc)
            log_src = fileread(fullfile(scimatlab_root(), 'src', ...
                'scimatlab', 'matlab', '+scifor', 'Log.m'));
            tc.verifySubstring(log_src, 'py.scistacklog.Log');
            tc.verifyEqual(count(log_src, 'py.scidb'), 0);
        end
    end

end

function root = scimatlab_root()
%SCIMATLAB_ROOT  Path to the scimatlab package root (two levels above
%   tests/matlab/scifor).
    root = fullfile(fileparts(mfilename('fullpath')), '..', '..', '..');
end
