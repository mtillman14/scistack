classdef TestGluePathInput < matlab.unittest.TestCase
%TESTGLUEPATHINPUT  Glue on the MATLAB path: PathInput heads and any language.
%
%   Issue #21 (docs/claude/free-code-glue-nodes.md §1):
%     * a PathInput may feed a glue node; the glue receives the resolved
%       path and runs ONCE PER DISTINCT PATH, not once per function combo;
%     * glue language does not have to match the run: Python glue runs in a
%       MATLAB run (here: on a PathInput, through the bridge), and MATLAB
%       glue runs on a constant (before prepare).
%   The Python half is scidb/tests/test_pathinput_glue.py.

    properties
        test_dir
    end

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function setupDatabase(testCase)
            testCase.test_dir = tempname;
            mkdir(testCase.test_dir);
            scidb.configure_database( ...
                fullfile(testCase.test_dir, 'test.duckdb'), ...
                ["subject", "session"]);
            global GLUE_TEST_CALLS CFG_FACTOR_CALLS %#ok<GVMIS>
            GLUE_TEST_CALLS = 0;
            CFG_FACTOR_CALLS = 0;
            % sub1.json -> factor 2, sub2.json -> factor 3: a {subject}-level
            % PathInput under a [subject, session] function.
            write_json(fullfile(testCase.test_dir, 'sub1.json'), struct('factor', 2));
            write_json(fullfile(testCase.test_dir, 'sub2.json'), struct('factor', 3));
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            try
                scidb.get_database().close();
            catch
            end
            if contains(path, testCase.test_dir)
                rmpath(testCase.test_dir);
            end
            if isfolder(testCase.test_dir)
                rmdir(testCase.test_dir, 's');
            end
        end
    end

    methods (Test)

        function test_matlab_glue_on_a_pathinput_runs_once_per_file(testCase)
            global GLUE_TEST_CALLS CFG_FACTOR_CALLS %#ok<GVMIS>
            run_cfg(testCase, struct('cfg', @glue_test_read_factor), false);

            testCase.verifyEqual(CFG_FACTOR_CALLS, 4, ...
                'the function runs at [subject, session]: 4 combos');
            testCase.verifyEqual(GLUE_TEST_CALLS, 2, ...
                'the glue must run once per distinct file (2), not per combo (4)');
            verify_factors(testCase);
        end

        function test_python_glue_on_a_pathinput_runs_in_a_matlab_run(testCase)
            py_file = fullfile(testCase.test_dir, 'glue_py_read_factor.py');
            fid = fopen(py_file, 'w');
            fprintf(fid, ['import json\n\n' ...
                'def glue_py_read_factor(path):\n' ...
                '    with open(path) as f:\n' ...
                '        return json.load(f)\n']);
            fclose(fid);
            glue = struct('cfg', struct('name', 'glue_py_read_factor', ...
                'language', 'python', 'source_file', py_file));

            run_cfg(testCase, glue, false);
            verify_factors(testCase);
        end

        function test_an_unchanged_rerun_skips(testCase)
            global CFG_FACTOR_CALLS %#ok<GVMIS>
            run_cfg(testCase, struct('cfg', @glue_test_read_factor), true);
            testCase.verifyEqual(CFG_FACTOR_CALLS, 4);

            run_cfg(testCase, struct('cfg', @glue_test_read_factor), true);
            testCase.verifyEqual(CFG_FACTOR_CALLS, 4, ...
                'an unchanged rerun must skip every combo');
        end

        function test_editing_the_glue_body_recomputes(testCase)
            % The glue lives in the test folder so its body can be rewritten.
            global CFG_FACTOR_CALLS %#ok<GVMIS>
            write_glue(testCase.test_dir, 1);
            addpath(testCase.test_dir);
            run_cfg(testCase, struct('cfg', @glue_test_edited), true);
            testCase.verifyEqual(CFG_FACTOR_CALLS, 4);

            write_glue(testCase.test_dir, 10);
            clear('glue_test_edited');
            rehash;
            run_cfg(testCase, struct('cfg', @glue_test_edited), true);
            testCase.verifyEqual(CFG_FACTOR_CALLS, 8, ...
                'an edited glue body must recompute every combo');
            r = ScalarVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(double(r.data), 20, 'AbsTol', 1e-10);
        end

        function test_matlab_glue_on_a_constant_runs_before_prepare(testCase)
            % Until 2026-11-17 this was refused: constant glue runs in prepare,
            % which is Python. MATLAB now runs it first and hands the glued
            % value over; identity records the raw value.
            global CFG_FACTOR_CALLS %#ok<GVMIS>
            args = {'subject', 1, 'session', "A", 'skip_computed', true, ...
                'glue', struct('k', @glue_test_double_value)};
            scidb.for_each(@cfg_factor, struct('k', 3), {ScalarVar()}, args{:});
            r = ScalarVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(double(r.data), 6, 'AbsTol', 1e-10);
            testCase.verifyEqual(CFG_FACTOR_CALLS, 1);

            scidb.for_each(@cfg_factor, struct('k', 3), {ScalarVar()}, args{:});
            testCase.verifyEqual(CFG_FACTOR_CALLS, 1, ...
                'an unchanged rerun with MATLAB constant glue must skip');
        end
    end
end


function run_cfg(testCase, glue, skip)
    scidb.for_each(@cfg_factor, ...
        struct('cfg', scifor.PathInput("sub{subject}.json", ...
            'root_folder', testCase.test_dir, 'name', "subject_cfg")), ...
        {ScalarVar()}, ...
        'subject', [1 2], 'session', ["A", "B"], ...
        'skip_computed', skip, ...
        'glue', glue);
end


function verify_factors(testCase)
    for subj = [1 2]
        for sess = ["A", "B"]
            r = ScalarVar().load('subject', subj, 'session', sess);
            testCase.verifyEqual(double(r.data), double(subj + 1), 'AbsTol', 1e-10, ...
                sprintf('subject %d session %s', subj, sess));
        end
    end
end


function write_json(file, s)
    fid = fopen(file, 'w');
    fprintf(fid, '%s', jsonencode(s));
    fclose(fid);
end


function write_glue(folder, multiplier)
    fid = fopen(fullfile(folder, 'glue_test_edited.m'), 'w');
    fprintf(fid, ['function cfg = glue_test_edited(path)\n' ...
        '    cfg = jsondecode(fileread(char(path)));\n' ...
        '    cfg.factor = cfg.factor * %d;\n' ...
        'end\n'], multiplier);
    fclose(fid);
end
